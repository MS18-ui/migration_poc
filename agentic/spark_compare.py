"""Spark 2 vs Spark 3 comparison of one job. Deterministic: no LLM, no Spark needed.

Both files are read with `ast`, so layout and comments do not affect the result. The report has:
  * steps:       the sections of each file (SETUP, SOURCES, T1..Tn, TARGET) matched by name, with the result for
                 each (same logic / rewritten / only in one file) and the calls that appear on one side only
  * evidence:    counts of the constructs that changed between Spark 2.4 and Spark 3.x
  * differences: what each version reads, writes, configures and hard-codes; these can change the result

Outputs go into one folder under the output folder, named `<spark2 name>_vs_<spark3 name>_<date>_<time>`:
    compare_<stamp>.md    summary tables
    compare_<stamp>.html  side-by-side diff of the two files
    compare_<stamp>.json  the same report as data

CLI:  python -m agentic.spark_compare Claude_op/daily_txn_risk_job.py Claude_op/daily_txn_risk_job_spark3.py
"""
import argparse
import ast
import difflib
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .config import ui_settings
from .outputs import new_run_dir, out_name, safe_stem, stamp_now

HOF = {"filter", "transform", "exists", "forall", "aggregate", "zip_with",
       "transform_keys", "transform_values", "map_filter"}
SQL_HOF = re.compile(r"\b(?:%s)\s*\(" % "|".join(sorted(HOF)))
SECTION = re.compile(r"^\s*#\s*-*\s*(SOURCES|TARGET|T\d+)\b:?\s*(.*?)[\s-]*$")
ADDRESS = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b|\b(?:hdfs|thrift|jdbc|s3a?)://[^\s\"']+")
READERS = {"parquet", "orc", "csv", "json", "text", "avro"}
WRITERS = {"insertInto", "saveAsTable"}


@dataclass
class Source:
    label: str
    code: str


def load_source(code: Optional[str] = None, path: Optional[str] = None, name: Optional[str] = None) -> Source:
    if path:
        with open(path, encoding="utf-8") as fh:
            return Source(name or os.path.basename(path), fh.read())
    if code is None:
        raise ValueError("give either code or path")
    return Source(name or "job", code)


# ---------- facts: what the file reads, writes, configures and calls ----------

@dataclass
class Facts:
    loc: int = 0                                   # lines of code (no blanks, no comments)
    tables_read: List[str] = field(default_factory=list)
    files_read: List[str] = field(default_factory=list)
    targets: List[str] = field(default_factory=list)
    conf: Dict[str, str] = field(default_factory=dict)
    master: str = ""
    hive_support: bool = False
    env_reads: Dict[str, str] = field(default_factory=dict)
    functions: List[str] = field(default_factory=list)   # distinct F.<name> calls
    sql_exprs: List[str] = field(default_factory=list)   # F.expr(...) strings
    sql_hof: int = 0                                     # filter()/transform()... inside SQL text
    native_hof: int = 0                                  # F.filter / F.transform ... calls
    lambdas: int = 0
    get_item: int = 0                                    # .getItem("key")
    bracket_access: int = 0                              # ["key"]
    addresses: List[str] = field(default_factory=list)   # hard-coded IPs and URIs


def _loc(code: str) -> int:
    return sum(1 for line in code.splitlines() if line.strip() and not line.strip().startswith("#"))


def _is_env_get(node: ast.Call) -> bool:
    if not isinstance(node.func, ast.Attribute):
        return False
    return (node.func.attr == "get" and "environ" in ast.unparse(node.func.value)) or node.func.attr == "getenv"


def _text(node: ast.AST, consts: Dict[str, str]) -> Optional[str]:
    """Text of a string expression (literal, module constant, f-string, +) or None when it is not static."""
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) else None
    if isinstance(node, ast.Name):
        return consts.get(node.id)
    if isinstance(node, ast.JoinedStr):
        parts = []
        for part in node.values:
            if isinstance(part, ast.FormattedValue):
                inner = _text(part.value, consts)
                if inner is None:
                    return None
                parts.append(inner)
            else:
                parts.append(part.value)
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _text(node.left, consts), _text(node.right, consts)
        return left + right if left is not None and right is not None else None
    if isinstance(node, ast.Call) and _is_env_get(node) and len(node.args) > 1:
        return _text(node.args[1], consts)  # os.environ.get(NAME, default): the default is the static value
    return None


def _value(node: ast.AST, consts: Dict[str, str]) -> str:
    text = _text(node, consts)
    return text if text is not None else ast.unparse(node)


def _constants(tree: ast.Module) -> Dict[str, str]:
    consts: Dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            value = _text(node.value, consts)
            if value is not None:
                consts[node.targets[0].id] = value
    return consts


def _record_call(node: ast.Call, f: Facts, functions: set, consts: Dict[str, str]) -> None:
    attr = node.func.attr
    recv = ast.unparse(node.func.value)
    args = node.args
    first = _value(args[0], consts) if args else None
    if recv == "F":
        functions.add(attr)
        if attr == "expr" and first is not None:
            f.sql_exprs.append(first)
        if attr in HOF:
            f.native_hof += 1
    elif attr == "table" and first is not None:
        f.tables_read.append(first)
    elif attr in READERS and "read" in recv.split(".") and first is not None:
        f.files_read.append(first)
    elif attr in WRITERS and first is not None:
        f.targets.append(first)
    elif attr == "config" and len(args) == 2 and first is not None:
        f.conf[first] = _value(args[1], consts)
    elif attr == "set" and recv.endswith("conf") and len(args) == 2 and first is not None:
        f.conf[first] = _value(args[1], consts)
    elif attr == "master" and first is not None:
        f.master = first
    elif attr == "enableHiveSupport":
        f.hive_support = True
    elif attr == "getItem":
        f.get_item += 1
    elif _is_env_get(node) and first is not None:
        f.env_reads[first] = _value(args[1], consts) if len(args) > 1 else ""


def facts_of(code: str) -> Facts:
    tree = ast.parse(code)
    consts = _constants(tree)
    f = Facts(loc=_loc(code))
    functions: set = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Lambda):
            f.lambdas += 1
        elif isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant) \
                and isinstance(node.slice.value, str):
            f.bracket_access += 1
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            _record_call(node, f, functions, consts)
    f.functions = sorted(functions)
    f.sql_hof = sum(len(SQL_HOF.findall(sql)) for sql in f.sql_exprs)
    f.addresses = sorted(set(ADDRESS.findall(code)))
    return f


# ---------- sections: the step-by-step view ----------

@dataclass
class Section:
    key: str
    title: str
    lines: int
    dump: Optional[str]     # AST dump; equal dumps mean the same logic
    apis: List[str]        # attribute names used; F.<name> for pyspark.sql.functions


def _section(key: str, title: str, text: str) -> Section:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return Section(key, title, _loc(text), None, [])
    apis = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            is_f = isinstance(node.value, ast.Name) and node.value.id == "F"
            apis.add("F." + node.attr if is_f else node.attr)
    return Section(key, title, _loc(text), ast.dump(tree), sorted(apis))


def sections_of(code: str) -> Dict[str, Section]:
    lines = code.splitlines()
    marks = [(i, m.group(1), m.group(2)) for i, line in enumerate(lines) if (m := SECTION.match(line))]
    starts = [(0, "SETUP", "imports and session")] + marks
    out: Dict[str, Section] = {}
    for n, (start, key, title) in enumerate(starts):
        end = starts[n + 1][0] if n + 1 < len(starts) else len(lines)
        text = "\n".join(lines[start:end])
        if text.strip():
            out[key] = _section(key, title, text)
    return out


def _order(key: str):
    if key == "SETUP":
        return (0, 0)
    if key == "SOURCES":
        return (1, 0)
    if key == "TARGET":
        return (9, 0)
    return (2, int(key[1:]))


def steps_of(a: Dict[str, Section], b: Dict[str, Section]) -> List[dict]:
    rows = []
    for key in sorted(set(a) | set(b), key=_order):
        x, y = a.get(key), b.get(key)
        if x and y:
            same = x.dump is not None and x.dump == y.dump
            rows.append({"section": key, "title": y.title or x.title,
                         "spark2_loc": x.lines, "spark3_loc": y.lines,
                         "result": "same logic" if same else "rewritten",
                         "only_spark2": sorted(set(x.apis) - set(y.apis)),
                         "only_spark3": sorted(set(y.apis) - set(x.apis))})
        else:
            rows.append({"section": key, "title": (x or y).title,
                         "spark2_loc": x.lines if x else 0, "spark3_loc": y.lines if y else 0,
                         "result": "only in spark2" if x else "only in spark3",
                         "only_spark2": x.apis if x else [], "only_spark3": y.apis if y else []})
    return rows


# ---------- evidence and differences ----------

EVIDENCE = [
    ("SQL expression strings (F.expr)", lambda f: len(f.sql_exprs),
     "Logic written as SQL text. Spark checks it only when the job runs."),
    ("Higher-order functions inside SQL text", lambda f: f.sql_hof,
     "filter(), transform() and similar, written inside SQL strings (Spark 2.4 style)."),
    ("Native higher-order functions (F.filter, F.transform, ...)", lambda f: f.native_hof,
     "Python API from Spark 3.1. Python and the editor check the call."),
    ("Python lambda expressions", lambda f: f.lambdas,
     "Passed to the native higher-order functions."),
    ('Field access with getItem("key")', lambda f: f.get_item,
     "Map and struct access, Spark 2 style."),
    ('Field access with ["key"]', lambda f: f.bracket_access,
     "Map and struct access, Spark 3 style."),
    ("Tables read by name (spark.table)", lambda f: len(f.tables_read),
     "Read through the Hive metastore catalog."),
    ("Files read by path (spark.read.*)", lambda f: len(f.files_read),
     "Read straight from storage, bypassing the catalog."),
    ("Tables written (insertInto / saveAsTable)", lambda f: len(f.targets),
     "Output tables."),
    ("Session settings set in code", lambda f: len(f.conf),
     "Spark and Hive settings applied when the job starts."),
    ("Environment variables read", lambda f: len(f.env_reads),
     "Values that can change per run without editing the job."),
    ("Hard-coded addresses (IP or URI)", lambda f: len(f.addresses),
     "Ties the job to one cluster."),
    ("Distinct pyspark.sql.functions used", lambda f: len(f.functions),
     "Breadth of the F.* API."),
]

DIFF_NOTE = {
    "source": "Confirm both versions read the same data for the same run date.",
    "target": "Confirm both versions write to the same table.",
    "setting": "Changes how Spark runs the job. Check the cluster and metastore settings.",
    "master": "Sets where the job runs.",
    "hive": "Switches Hive support on or off.",
    "env": "The value can change per run without editing the job.",
    "address": "Ties the job to one environment. Move it to configuration.",
}


def differences_of(a: Facts, b: Facts) -> List[dict]:
    rows: List[dict] = []

    def add(area: str, note: str, x: str, y: str) -> None:
        rows.append({"area": area, "spark2": x, "spark3": y, "note": note})

    src_a = set(a.tables_read) | {f"file {p}" for p in a.files_read}
    src_b = set(b.tables_read) | {f"file {p}" for p in b.files_read}
    for item in sorted(src_a - src_b):
        add("Source read only in Spark 2", DIFF_NOTE["source"], item, "-")
    for item in sorted(src_b - src_a):
        add("Source read only in Spark 3", DIFF_NOTE["source"], "-", item)

    tgt_a, tgt_b = set(a.targets), set(b.targets)
    for item in sorted(tgt_a - tgt_b):
        add("Target written only in Spark 2", DIFF_NOTE["target"], item, "-")
    for item in sorted(tgt_b - tgt_a):
        add("Target written only in Spark 3", DIFF_NOTE["target"], "-", item)

    for key in sorted(set(a.conf) | set(b.conf)):
        x, y = a.conf.get(key, "(not set)"), b.conf.get(key, "(not set)")
        if x != y:
            add(f"Session setting {key}", DIFF_NOTE["setting"], x, y)
    if a.master != b.master:
        add("Spark master", DIFF_NOTE["master"], a.master or "(not set)", b.master or "(not set)")
    if a.hive_support != b.hive_support:
        add("Hive support", DIFF_NOTE["hive"], str(a.hive_support), str(b.hive_support))

    for key in sorted(set(a.env_reads) ^ set(b.env_reads)):
        x = f"{key} (default {a.env_reads[key]!r})" if key in a.env_reads else "(not read)"
        y = f"{key} (default {b.env_reads[key]!r})" if key in b.env_reads else "(not read)"
        add(f"Environment variable {key}", DIFF_NOTE["env"], x, y)

    for item in sorted(set(a.addresses) - set(b.addresses)):
        add("Hard-coded address only in Spark 2", DIFF_NOTE["address"], item, "-")
    for item in sorted(set(b.addresses) - set(a.addresses)):
        add("Hard-coded address only in Spark 3", DIFF_NOTE["address"], "-", item)
    return rows


def build_report(a: Source, b: Source) -> dict:
    fa, fb = facts_of(a.code), facts_of(b.code)
    steps = steps_of(sections_of(a.code), sections_of(b.code))
    counts = {"same logic": 0, "rewritten": 0, "only in one file": 0}
    for st in steps:
        key = "same logic" if st["result"] == "same logic" else \
              "rewritten" if st["result"] == "rewritten" else "only in one file"
        counts[key] += 1
    return {
        "spark2": a.label,
        "spark3": b.label,
        "summary": {"sections": len(steps), **counts, "loc": {"spark2": fa.loc, "spark3": fb.loc}},
        "steps": steps,
        "evidence": [{"construct": label, "spark2": get(fa), "spark3": get(fb), "meaning": meaning}
                     for label, get, meaning in EVIDENCE],
        "differences": differences_of(fa, fb),
    }


# ---------- output ----------

def _cell(text: str) -> str:
    return str(text).replace("|", "\\|")


def _names(names: List[str]) -> str:
    return ", ".join(f"`{n}`" for n in names) if names else "-"


def to_markdown(r: dict, html_name: str) -> str:
    s = r["summary"]
    out = [f"# Spark 2 vs Spark 3: {r['spark2']} and {r['spark3']}", "",
           f"Lines of code: **{s['loc']['spark2']}** in Spark 2, **{s['loc']['spark3']}** in Spark 3.", "",
           f"Sections: {s['sections']} ({s['rewritten']} rewritten, {s['same logic']} same logic, "
           f"{s['only in one file']} in one file only).", "",
           "## Step by step", "",
           "| Section | Title | Spark 2 LOC | Spark 3 LOC | Result | Calls only in Spark 2 | Calls only in Spark 3 |",
           "|---|---|---:|---:|---|---|---|"]
    for st in r["steps"]:
        out.append(f"| {st['section']} | {_cell(st['title'])} | {st['spark2_loc']} | {st['spark3_loc']} "
                   f"| {st['result']} | {_names(st['only_spark2'])} | {_names(st['only_spark3'])} |")
    out += ["", "## API evidence", "",
            "| Construct | Spark 2 | Spark 3 | What it means |", "|---|---:|---:|---|"]
    for e in r["evidence"]:
        out.append(f"| {_cell(e['construct'])} | {e['spark2']} | {e['spark3']} | {_cell(e['meaning'])} |")
    out += ["", "## Differences that can change the result", ""]
    if not r["differences"]:
        out.append("None found by static comparison. Still run both versions on the same data before relying on it.")
    else:
        out += ["| Area | Spark 2 | Spark 3 | Note |", "|---|---|---|---|"]
        for d in r["differences"]:
            out.append(f"| {_cell(d['area'])} | {_cell(d['spark2'])} | {_cell(d['spark3'])} | {_cell(d['note'])} |")
    out += ["", f"Side-by-side diff: `{html_name}`", ""]
    return "\n".join(out)


def render(a: Source, b: Source, stamp: str) -> dict:
    """The report and the contents of its three files, named for `stamp`. Nothing is written."""
    report = build_report(a, b)
    report["stamp"] = stamp
    names = {"markdown": out_name("compare_report", stamp, "md"), "html": out_name("compare_diff", stamp, "html"),
             "json": out_name("compare_record", stamp, "json")}
    return {
        "report": report,
        "names": names,
        "markdown": to_markdown(report, names["html"]),
        "html": difflib.HtmlDiff(tabsize=4, wrapcolumn=100).make_file(
            a.code.splitlines(), b.code.splitlines(), a.label, b.label, context=False),
        "json": json.dumps(report, indent=2, default=str),
    }


def compare(a: Source, b: Source, out_base: Optional[str] = None, now=None) -> dict:
    """Write the comparison into a new run folder under the output folder (used by the CLI and the API)."""
    folder, stamp = new_run_dir(out_base or ui_settings()["out_dir"], stamp_now(now),
                                name=f"{safe_stem(a.label)}_vs_{safe_stem(b.label)}")
    out = render(a, b, stamp)
    paths = {}
    for key, fname in out["names"].items():
        paths[key] = os.path.join(folder, fname)
        with open(paths[key], "w", encoding="utf-8") as fh:
            fh.write(out[key])
    return {"folder": folder, "files": paths, "report": out["report"], "markdown": out["markdown"]}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="agentic.spark_compare", description="Compare a Spark 2 job with its Spark 3 version")
    ap.add_argument("spark2", help="Spark 2 job file")
    ap.add_argument("spark3", help="Spark 3 job file")
    ap.add_argument("--out", default=None, help="output folder (default: ui.out_dir in config.yaml)")
    a = ap.parse_args(argv)
    try:
        res = compare(load_source(path=a.spark2), load_source(path=a.spark3), a.out)
    except (OSError, SyntaxError, ValueError) as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    print(res["markdown"])
    print(f"Files written to {res['folder']}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
