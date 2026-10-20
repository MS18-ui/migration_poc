"""Layer 1: everything that can be decided without test data.

Most checks are pure Python. The column/type flow check builds the graph on empty tables in Spark; if no Spark
is available here it is reported as INCOMPLETE (never as passed) and the rest still runs.
"""
import os
from typing import Dict, List, Optional, Tuple

from ..blueprint_checks import check_rich_blueprint
from ..schemas import PipelineBlueprint
from ..testdata import field_type_str
from .compat import scan_python, scan_sql_text
from .coverage import check_coverage
from .engine import Engine, shape_problems
from .findings import Finding
from .spark_session import spark_unavailable_reason
from .values import is_numeric_type, norm_type

_DEFINITE = ("cannot resolve", "unresolved_column", "ambiguous", "does not exist", "not found", "not produced",
             "missing from", "cannot be resolved", "no such")


def detect_lang(path: str, text: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext in (".hql", ".sql", ".hive"):
        return "hiveql"
    return "pyspark2"


def engine_findings(bp: PipelineBlueprint, engine: Engine) -> List[Finding]:
    out = []
    for node, err in engine.errors.items():
        if err.kind == "blocked":
            continue
        executability = err.kind == "unsupported"
        definite = any(k in err.msg.lower() for k in _DEFINITE)
        out.append(Finding(
            "executability" if executability else "schema", "error" if (executability or definite) else "warning",
            err.msg, node=node, confidence="high" if (executability or definite) else "medium",
            impact="The deterministic runtime cannot execute this node, so everything downstream is unverified.",
            resolution=("Make the operation fully structured (see the contract for this node type)." if executability
                        else "Fix the column reference / types; a dropped rename or derivation is the usual cause.")))
    blocked = sorted(n for n, e in engine.errors.items() if e.kind == "blocked")
    if blocked:
        out.append(Finding("executability", "info", f"{len(blocked)} node(s) are blocked by upstream failures: {blocked}",
                           confidence="high"))
    return out


def shape_findings(bp: PipelineBlueprint) -> List[Finding]:
    """Operation specs that cannot be executed deterministically (pure Python: no Spark needed)."""
    return [Finding("executability", "error", msg, node=nid, confidence="high",
                    impact="The deterministic runtime cannot execute this node, so everything downstream is unverified.",
                    resolution="Make the operation fully structured (see the contract for this node type).")
            for nid, msg in shape_problems(bp).items()]


def target_type_findings(bp: PipelineBlueprint, engine: Engine) -> List[Finding]:
    out = []
    for tg in bp.targets:
        if tg.input not in engine.cols:
            continue
        produced = {c.lower(): t for c, t in engine.cols[tg.input]}
        for fld in tg.schema_:
            want, got = field_type_str(fld), produced.get(fld.name.lower())
            if not got or norm_type(got) == norm_type(want):
                continue
            both_plain_numeric = (is_numeric_type(got) and is_numeric_type(want)
                                  and "decimal" not in got.lower() and "decimal" not in want.lower())
            if not both_plain_numeric:  # int->bigint / float->double widening is routine; the rest changes semantics
                out.append(Finding("schema", "warning", f"target '{tg.id}' column '{fld.name}': produced {got}, target type {want} "
                                   "(a silent cast will be applied)", node=tg.input, confidence="medium",
                                   resolution="Confirm the cast is intended (precision/semantics may change)."))
    return out


def contract_findings(bp: PipelineBlueprint) -> List[Finding]:
    try:
        check_rich_blueprint(bp)
        return []
    except ValueError as e:
        return [Finding("executability", "error", line.lstrip("- ").strip(), confidence="high",
                        resolution="Fix the blueprint structure.")
                for line in str(e).splitlines()[1:] if line.strip().startswith("-")]


def ignored_field_findings(bp: PipelineBlueprint) -> List[Finding]:
    out = []
    for t in bp.transformations:
        if (t.model_extra or {}).get("post_filter"):
            out.append(Finding("executability", "warning",
                               f"'post_filter' is declared but not executed by the runtime (null elements are always dropped); "
                               f"it must not carry logic", node=t.id, confidence="high",
                               resolution="Remove it or express the logic as a separate filter node."))
    return out


def run_static(bp: PipelineBlueprint, source_text: str, source_path: str, lang: str = None,
               engine: Optional[Engine] = None) -> Tuple[List[Finding], List[Dict], Optional[Engine]]:
    lang = lang or detect_lang(source_path, source_text)
    findings: List[Finding] = []
    findings += contract_findings(bp)
    findings += shape_findings(bp)
    why = None if engine is not None else spark_unavailable_reason()
    if engine is None and why is None:
        try:
            engine = Engine(bp).run({})  # empty tables: Spark's analyzer reports unresolved columns per node
        except Exception as e:  # session could not start (misconfigured Java, port clash...)
            why, engine = f"Spark session failed to start: {str(e).splitlines()[0][:200]}", None
    if engine is not None:
        findings += engine_findings(bp, engine)
        findings += target_type_findings(bp, engine)
    else:
        findings.append(Finding("test-gap", "warning", f"column/type flow check was NOT run: {why}", confidence="high",
                                resolution="Run the validator on a host with Spark (e.g. the cluster host)."))
    findings += ignored_field_findings(bp)
    matrix: List[Dict] = []
    try:
        cov, matrix = check_coverage(bp, source_text, source_path, lang)
        findings += cov
    except SyntaxError as e:
        findings.append(Finding("executability", "error", f"source cannot be parsed ({e.msg})", source_location=f"{source_path}:{e.lineno}"))
    except Exception as e:  # coverage must never crash the run; report the failed check as incomplete
        findings.append(Finding("coverage", "warning", f"coverage check incomplete: {type(e).__name__}: {e}", confidence="low"))
    findings += scan_python(source_text, source_path) if lang == "pyspark2" else scan_sql_text(source_text, source_path)
    return findings, matrix, engine
