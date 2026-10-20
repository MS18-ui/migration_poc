"""Node-level test scenarios: crafted input tables for one operation, independent oracles, and branch coverage.

A scenario feeds crafted rows to ONE transformation (its input tables only) and checks the output:
  * it executes (no engine error)
  * independent invariants hold (cardinalities computed in plain Python, not by the engine)
  * source-derived boundary expectations hold (comparison semantics taken from the legacy source)
Input data comes from per-column value pools built from the node's own literals (L-d, L, L+d, NULL, empty
arrays, missing map keys, frame-edge timestamps...), so boundaries of every constant are exercised.
"""
import random
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp

from ..schemas import PipelineBlueprint, Transformation
from .coverage import _EXPR_KEYS, _NUM_RE, _STR_RE, _walk
from .engine import Engine, EngineError, canonical
from .values import RowFactory, parse_type

BASE_TS = datetime(2026, 10, 18, 12, 0, 0)
Schema = List[Tuple[str, str]]


@dataclass
class Scenario:
    id: str
    node: str
    kind: str
    description: str
    inputs: Dict[str, List[dict]]
    expect: List[Dict[str, Any]] = field(default_factory=list)  # declarative source-derived expectations
    origin: str = "generated"

    def to_dict(self):
        return {"id": self.id, "node": self.node, "kind": self.kind, "description": self.description,
                "inputs": self.inputs, "expect": self.expect, "origin": self.origin}


@dataclass
class NodeInfo:
    refs: Set[str] = field(default_factory=set)
    nums: List[float] = field(default_factory=list)
    strs: List[str] = field(default_factory=list)
    keys: Set[str] = field(default_factory=set)
    frame: List[float] = field(default_factory=list)
    map_keys: Dict[str, List[str]] = field(default_factory=dict)
    pairs: List[Tuple[str, str]] = field(default_factory=list)


def node_info(t: Transformation) -> NodeInfo:
    d = t.model_dump(by_alias=True, exclude_none=True)
    info = NodeInfo()
    texts: List[Tuple[str, str]] = []
    _walk(t.id, d, texts)
    for r in d.get("rules") or []:
        for k in ("when", "condition"):
            if isinstance(r, dict) and r.get(k):
                texts.append((t.id, str(r[k])))
    for _, text in texts:
        for m in _STR_RE.finditer(text):
            info.strs.append(m.group(1))
        stripped = _STR_RE.sub("''", text)
        info.nums += [float(x) for x in _NUM_RE.findall(stripped)]
        try:
            tree = sqlglot.parse_one(re.sub(r":(\w+)", r"'p'", text), read="spark")
        except Exception:
            continue
        for c in tree.find_all(exp.Column):
            info.refs.add(c.name.lower())
        for b in tree.find_all(exp.Bracket):
            if isinstance(b.this, exp.Column) and b.expressions and isinstance(b.expressions[0], exp.Literal):
                info.map_keys.setdefault(b.this.name.lower(), []).append(b.expressions[0].this)
        for eq in tree.find_all(exp.EQ):
            if isinstance(eq.left, exp.Column) and isinstance(eq.right, exp.Column):
                info.pairs.append((eq.left.name.lower(), eq.right.name.lower()))
    for k in (t.dedupe_keys or []):
        info.keys.add(k.lower()); info.refs.add(k.lower())
    for g in (t.group_by or []):
        m = re.match(r"\s*([\w.]+)", g)
        if m and "." not in m.group(1):
            info.keys.add(m.group(1).lower()); info.refs.add(m.group(1).lower())
    for s in d.get("steps") or []:
        if isinstance(s, dict):
            for g in s.get("group_by") or []:
                info.keys.add(str(g).lower()); info.refs.add(str(g).lower())
    for p in (t.window or {}).get("partition_by") or []:
        info.keys.add(str(p).lower()); info.refs.add(str(p).lower())
    for j in t.joins or []:
        on = j.get("on") if isinstance(j, dict) else None
        if isinstance(on, list):
            info.keys.update(c.lower() for c in on); info.refs.update(c.lower() for c in on)
    for a, b in info.pairs:
        info.keys.update((a, b))
    ex = d.get("explode")
    if isinstance(ex, dict) and ex.get("column"):
        info.refs.add(ex["column"].lower())
    fr = (t.window or {}).get("frame")
    if fr:
        info.frame = [float(x) for x in re.findall(r"\d+", str(fr))]
    return info


# ---- value pools ----------------------------------------------------------------------------
def _uniq(vals: List[Any]) -> List[Any]:
    out, seen = [], set()
    for v in vals:
        k = repr(v)
        if k not in seen:
            seen.add(k)
            out.append(v)
    return out


def pool(tree, col: str, info: NodeInfo, rf: RowFactory, cap: int = 14) -> List[Any]:
    kind = tree[0]
    typical = rf.gen.value(tree, col, info.map_keys.get(col.lower()))
    if kind == "decimal" or (kind == "prim" and tree[1] in ("INT", "BIGINT", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT")):
        is_int = kind == "prim" and tree[1] in ("INT", "BIGINT", "SMALLINT", "TINYINT")
        d = 1 if is_int else 0.01
        vals = [typical, 0, 1, -1] + ([] if is_int else [0.5, 2.0])
        for L in info.nums:
            vals += [L - d, L, L + d, L * 2]
        if kind == "decimal":
            vals = [round(float(v), tree[2]) for v in vals]
        elif is_int:
            vals = [int(round(v)) for v in vals]
        else:
            vals = [float(v) for v in vals]
        return _uniq(vals)[:cap] + [None]
    if kind == "prim":
        t = tree[1]
        if t == "BOOLEAN":
            return [True, False, None]
        if t in ("TIMESTAMP", "TIMESTAMP_NTZ"):
            offs = [0, 1, 60, 3600, 86400, -1, -3600]
            for f in info.frame:
                offs += [f - 1, f, f + 1, -f]
            return [(BASE_TS + timedelta(seconds=o)).strftime("%Y-%m-%dT%H:%M:%S") for o in _uniq(offs)][:cap] + [None]
        if t == "DATE":
            return [(BASE_TS + timedelta(days=o)).strftime("%Y-%m-%d") for o in (0, 1, -1, 30)] + [None]
        if t in ("STRING", "VARCHAR", "CHAR"):
            return _uniq([typical, ""] + info.strs + ["other"])[:cap] + [None]
        return [typical, None]
    if kind == "array":
        el = pool(tree[1], col, info, rf, 6)
        els = [e for e in el if e is not None]
        arrs: List[Any] = [[], None]
        arrs += [[e] for e in els[:4]]
        if len(els) >= 2:
            arrs += [[els[0], els[1]], [els[1], els[1]]]
        if tree[1][0] == "struct":  # flag-bearing structs (e.g. is_primary): none / one / two flagged
            flags = [n for n, ft in tree[1][1] if ft == ("prim", "BOOLEAN")]
            for fl in flags:
                base = rf.gen.value(tree[1], col)
                mk = lambda v: {**base, fl: v}
                arrs += [[mk(False)], [mk(True)], [mk(True), mk(True)], [mk(False), mk(True)]]
        return _uniq(arrs)[:cap + 6]
    if kind == "map":
        keys = info.map_keys.get(col.lower()) or list(typical.keys())[:3] or ["k"]
        vals = ["Y", "N", "", "0", "1000", "x"] + [str(int(n)) for n in info.nums[:2]]
        out = [{}, None, typical]
        for k in keys[:4]:
            out.append({kk: v for kk, v in typical.items() if kk != k})
            for v in vals[:5]:
                out.append({**typical, k: v})
            out.append({**typical, k: None})
        return _uniq(out)[:cap + 10]
    if kind == "struct":
        base = typical
        out = [base, None]
        for fn, ft in tree[1]:
            for alt in pool(ft, fn, info, rf, 4)[:5]:
                out.append({**base, fn: alt})
        return _uniq(out)[:cap + 6]
    return [typical, None]


# ---- scenario generation --------------------------------------------------------------------
def _typical_rows(schema: Schema, rf: RowFactory, n: int = 3) -> List[dict]:
    return [rf.row(schema) for _ in range(n)]


def _is_rowwise(t: Transformation) -> bool:
    return t.type in ("derive_columns", "nested_derive", "filter", "select", "build_array_of_struct") or (
        t.type == "filter_dedupe_flatten" and not t.dedupe_keys)


def gen_scenarios(t: Transformation, in_schemas: Dict[str, Schema], seed: int = 1, n_random: int = 24,
                  source_expect: Optional[List[dict]] = None, cap_rows: int = 400) -> List[Scenario]:
    info = node_info(t)
    rf = RowFactory(seed)
    rng = random.Random(f"{seed}:{t.id}")
    out: List[Scenario] = []
    sid = lambda kind, n=0: f"{t.id}:{kind}{n if n else ''}"
    expect = source_expect or []

    out.append(Scenario(sid("typical"), t.id, "typical", "typical valid rows",
                        {i: _typical_rows(s, rf) for i, s in in_schemas.items()}, expect))
    out.append(Scenario(sid("empty"), t.id, "empty", "all inputs empty", {i: [] for i in in_schemas}))
    if len(in_schemas) > 1:
        for k, i in enumerate(in_schemas):
            out.append(Scenario(sid("empty_input", k), t.id, "empty", f"input '{i}' empty, others typical",
                                {j: ([] if j == i else _typical_rows(s, rf)) for j, s in in_schemas.items()}))

    # null in each referenced column
    for i, schema in in_schemas.items():
        refs = [c for c, _ in schema if c.lower() in info.refs]
        rows = []
        for c in refs:
            r = rf.row(schema)
            r[c] = None
            rows.append(r)
        if rows:
            inputs = {j: (rows if j == i else _typical_rows(s, rf)) for j, s in in_schemas.items()}
            out.append(Scenario(sid("nulls", list(in_schemas).index(i) or 0), t.id, "nulls",
                                f"one row per referenced column of '{i}' with that column NULL", inputs, expect))

    pools = {i: {c: pool(parse_type(ty), c, info, rf) for c, ty in schema if c.lower() in info.refs}
             for i, schema in in_schemas.items()}
    if _is_rowwise(t) and len(in_schemas) == 1:
        i, schema = next(iter(in_schemas.items()))
        base = rf.row(schema)
        rows = [dict(base)]
        for c, vals in pools[i].items():  # one-at-a-time around the typical row
            for v in vals:
                rows.append({**base, c: v})
        cols = list(pools[i])
        while len(rows) < cap_rows and cols:  # then random combinations
            rows.append({**base, **{c: rng.choice(pools[i][c]) for c in cols}})
        out.append(Scenario(sid("boundary"), t.id, "boundary", "value-pool boundaries for every referenced column "
                            "(L-d, L, L+d, NULL, empty/duplicate collections, missing map keys)", {i: rows}, expect))
    else:
        for n in range(n_random):
            out.append(random_multi(t, in_schemas, info, pools, rf, random.Random(f"{seed}:{t.id}:{n}"), sid("random", n + 1), expect))
    return out


def random_multi(t, in_schemas, info, pools, rf, rng, scenario_id, expect=None, kind="random", hot=False) -> Scenario:
    """Several rows per input; key columns drawn from a tiny pool so rows collide into groups/matches.
    hot=True (mutant-guided synthesis) makes rows more numerous and biases flags towards TRUE and keys
    towards a single group, so rare conditions (e.g. 'three hits in one window') actually occur."""
    keypool = ["k1", "k2", None]
    intkeys = [1, 2, None]
    inputs = {}
    for i, schema in in_schemas.items():
        rows = []
        lo, hi = (4, 10) if hot else (2, 6)
        if len(in_schemas) > 1:  # colliding keys multiply across inputs: keep joins small (hi^n rows)
            lo, hi = 2, (4 if len(in_schemas) == 2 else 3)
        for _ in range(rng.randint(lo, hi)):
            r = rf.row(schema)
            for c, ty in schema:
                lc = c.lower()
                tree = parse_type(ty)
                if lc in info.keys:
                    kv = intkeys if tree[0] == "prim" and tree[1] in ("INT", "BIGINT", "SMALLINT") else keypool
                    if hot:
                        r[c] = kv[0] if rng.random() < 0.8 else rng.choice(kv)
                    else:
                        r[c] = kv[min(int(rng.random() ** 2 * len(kv)), len(kv) - 1)] if rng.random() < 0.9 else rng.choice(kv)
                elif lc in pools[i]:
                    if hot and tree == ("prim", "BOOLEAN"):
                        r[c] = True if rng.random() < 0.75 else rng.choice(pools[i][c])
                    else:
                        r[c] = rng.choice(pools[i][c])
            rows.append(r)
        inputs[i] = rows
    return Scenario(scenario_id, t.id, kind, "random multi-row scenario (keys collide)" if not hot
                    else "mutant-guided synthesized scenario", inputs, expect or [], origin="synthesized" if hot else "generated")


# ---- execution ------------------------------------------------------------------------------
Result = Tuple[str, Any]  # ("ok", rows) | ("error", message)


def run_node(bp: PipelineBlueprint, t: Transformation, in_schemas: Dict[str, Schema],
             inputs: Dict[str, List[dict]], params: dict = None) -> Result:
    eng = Engine(bp, params)
    try:
        for i, schema in in_schemas.items():
            eng.register_input(i, schema, inputs.get(i, []))
    except EngineError as e:
        return ("error", f"input load: {e.msg}")
    eng.build_transformation(t)
    if t.id in eng.errors:
        return ("error", eng.errors[t.id].msg)
    try:
        return ("ok", eng.fetch(t.id))
    except Exception as e:  # pragma: no cover - defensive
        return ("error", f"fetch: {e}")


def run_variants(bp: PipelineBlueprint, variants: List[Transformation], in_schemas: Dict[str, Schema],
                 inputs: Dict[str, List[dict]], params: dict = None) -> List[Result]:
    """Load the inputs ONCE, then run each variant (original + mutants) of the node on them."""
    eng = Engine(bp, params)
    try:
        for i, schema in in_schemas.items():
            eng.register_input(i, schema, inputs.get(i, []))
    except EngineError as e:
        return [("error", f"input load: {e.msg}")] * len(variants)
    out: List[Result] = []
    for t in variants:
        eng.errors.pop(t.id, None)
        eng.df.pop(t.id, None)
        eng.build_transformation(t)
        if t.id in eng.errors:
            out.append(("error", eng.errors[t.id].msg))
        else:
            try:
                out.append(("ok", eng.fetch(t.id)))
            except Exception as e:  # pragma: no cover - defensive
                out.append(("error", f"fetch: {e}"))
    return out


def result_key(r: Result):
    return (r[0], tuple(canonical(r[1])) if r[0] == "ok" else r[1][:80])


# ---- oracles ---------------------------------------------------------------------------------
def _equi_pairs(j: dict) -> Optional[List[Tuple[str, str]]]:
    on = j.get("on")
    if isinstance(on, list):
        return [(c, c) for c in on]
    if isinstance(on, str):
        try:
            tree = sqlglot.parse_one(on, read="spark")
        except Exception:
            return None
        eqs = list(tree.find_all(exp.EQ))
        if eqs and all(isinstance(e.left, exp.Column) and isinstance(e.right, exp.Column) for e in eqs) \
                and len(eqs) == len(list(tree.find_all(exp.Binary))) - len(list(tree.find_all(exp.And))):
            return [(e.left.name, e.right.name) for e in eqs]
    return None


def _py_join_count(t: Transformation, inputs: Dict[str, List[dict]]) -> Optional[Tuple[int, List[Tuple[str, int, int]]]]:
    """Independent hash-join of the whole chain: (expected output rows, [(right id, rows before, rows after) per join])."""
    cur, first, steps = None, None, []
    for j in t.joins or []:
        pairs = _equi_pairs(j)
        how = str(j.get("type", "inner")).lower()
        if pairs is None or how not in ("inner", "left", "left_outer"):
            return None
        left = cur if (j.get("left") == "*" and cur is not None) else inputs.get(j.get("left"), [])
        right = inputs.get(j.get("right"), [])
        if first is None:
            first = len(left)
        idx: Dict[tuple, List[dict]] = {}
        for r in right:
            k = tuple(r.get(b) for _, b in pairs)
            if None not in k:
                idx.setdefault(k, []).append(r)
        nxt = []
        for l in left:
            k = tuple(l.get(a) for a, _ in pairs)
            ms = idx.get(k, []) if None not in k else []
            if ms:
                nxt += [{**r, **l} for r in ms]
            elif how != "inner":
                nxt.append(dict(l))
        steps.append((j.get("right"), len(left), len(nxt), how))
        cur = nxt
    return (len(cur), steps) if cur is not None else None


def invariants(t: Transformation, inputs: Dict[str, List[dict]], out: List[dict],
               source_ids: Optional[Set[str]] = None) -> Tuple[List[str], List[str]]:
    """-> (violations, observations). Violations are engine/YAML disagreements with an independent oracle;
    observations are legitimate behaviours worth reporting (e.g. join fan-out)."""
    bad, obs = [], []
    n_in = sum(len(v) for v in inputs.values()) if len(inputs) == 1 else None
    rows_in = next(iter(inputs.values()), []) if len(inputs) == 1 else None
    if t.type in ("derive_columns", "nested_derive", "window", "build_array_of_struct", "select") and not t.filter \
            and rows_in is not None and len(out) != len(rows_in):
        bad.append(f"row-wise operation changed row count {len(rows_in)} -> {len(out)}")
    if t.type == "filter" and rows_in is not None and len(out) > len(rows_in):
        bad.append(f"filter increased row count {len(rows_in)} -> {len(out)}")
    if t.type == "filter_dedupe_flatten" and t.dedupe_keys and rows_in is not None:
        keys = {tuple(json_key(r.get(k)) for k in t.dedupe_keys) for r in rows_in}
        if not t.filter and len(out) != len(keys):
            bad.append(f"dedupe on {t.dedupe_keys} should leave {len(keys)} row(s), got {len(out)}")
        if t.filter and len(out) > len(keys):
            bad.append(f"dedupe on {t.dedupe_keys} left more rows ({len(out)}) than distinct keys ({len(keys)})")
        if len({tuple(json_key(r.get(k)) for k in t.dedupe_keys) for r in out}) != len(out):
            bad.append("duplicate keys remain after dedupe")
    if t.type in ("aggregate", "collect_to_array_of_struct") and rows_in is not None and t.group_by \
            and all(re.fullmatch(r"\w+", g) for g in t.group_by):
        groups = {tuple(json_key(r.get(g)) for g in t.group_by) for r in rows_in}
        if len(out) != len(groups):
            bad.append(f"group by {t.group_by}: expected {len(groups)} group(s), got {len(out)}")
    if t.type in ("join", "join_derive") and t.joins:
        exp_n = _py_join_count(t, inputs)
        if exp_n is not None:
            expected, steps = exp_n
            if t.type == "join" and len(out) != expected and not t.select:
                bad.append(f"join cardinality: independent hash-join gives {expected} row(s), engine {len(out)}")
            # Fan-out is only a real risk for REFERENCE tables read straight from a source; computed inputs
            # (group-bys etc.) are unique on their keys by construction.
            for right, before, after, how in steps:
                if how != "inner" and after > before and (source_ids is None or right in source_ids):
                    obs.append(f"join fan-out: the {how} join with source table '{right}' multiplies rows when that "
                               "source has duplicate join keys")
    return bad, obs


def json_key(v):
    import json
    return json.dumps(v, sort_keys=True, default=str)


# ---- source-derived expectations --------------------------------------------------------------
_CMP = {">": lambda a, b: a > b, ">=": lambda a, b: a >= b, "<": lambda a, b: a < b, "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b, "!=": lambda a, b: a != b}


def source_expectations(t: Transformation, src_preds) -> List[dict]:
    """Derived boolean columns of this node that are exactly a comparison from the legacy source:
    expectation = what the SOURCE operator says for the row's own values."""
    out = []
    from .coverage import predicates_from_tree
    for c in t.columns or []:
        if not isinstance(c, dict) or "expr" not in c:
            continue
        try:
            tree = sqlglot.parse_one(re.sub(r":(\w+)", "'p'", str(c["expr"])), read="spark")
        except Exception:
            continue
        if not isinstance(tree, (exp.Binary, exp.Between)) or isinstance(tree, (exp.And, exp.Or)):
            continue
        for lhs, op, rhs in predicates_from_tree(tree):
            for sl, sop, srhs, line in src_preds:
                if sl == lhs and srhs == rhs and sop != "between" and sop != "in":
                    out.append({"column": c["name"], "lhs": lhs, "op": sop, "rhs": rhs[0], "source_line": line})
    return out


def _lhs_value(lhs: str, row: dict):
    m = re.fullmatch(r"size\((\w+)\)", lhs)
    if m:
        v = row.get(m.group(1))
        return None if v is None else len(v)
    m = re.fullmatch(r"(\w+)\['(.+)'\]", lhs)
    if m:
        v = row.get(m.group(1))
        return None if v is None else v.get(m.group(2))
    return row.get(lhs)


def check_expectations(expect: List[dict], out: List[dict]) -> List[str]:
    bad = []
    for e in expect:
        for r in out:
            lv = _lhs_value(e["lhs"], r)
            got = r.get(e["column"])
            if lv is None:
                if got not in (None, False):  # SQL: comparison with NULL is NULL (falsy)
                    bad.append(f"{e['column']}: lhs NULL but result {got!r}")
                continue
            try:
                want = _CMP[e["op"]](lv, e["rhs"])
            except TypeError:
                continue
            if got != want:
                bad.append(f"{e['column']}: source says ({e['lhs']} {e['op']} {e['rhs']}) for {e['lhs']}={lv!r} is {want}, "
                           f"YAML gave {got!r} (source line {e['source_line']})")
                break
    return bad


# ---- branch coverage --------------------------------------------------------------------------
def branch_observations(t: Transformation, outs: List[List[dict]], in_counts: List[int], out_schema: Schema) -> Dict[str, Set[Any]]:
    """What the suite actually exercised: derived booleans (T/F/NULL), rules (fired/not fired), filters (kept/dropped),
    joins (matched/unmatched are covered by invariants), arrays (empty/non-empty)."""
    seen: Dict[str, Set[Any]] = {}
    bool_cols = {c for c, ty in out_schema if ty.upper() == "BOOLEAN" and any(isinstance(x, dict) and x.get("name") == c for x in (t.columns or []))}
    for rows in outs:
        for r in rows:
            for c in bool_cols:
                seen.setdefault(f"boolean {c}", set()).add(r.get(c))
    if t.type == "build_array_of_struct":
        ids = [x.get("rule_id") for x in ((t.model_extra or {}).get("rules") or []) if isinstance(x, dict)]
        col = (t.model_extra or {}).get("output_column")
        for rid in ids:
            seen.setdefault(f"rule {rid}", set())
        for rows in outs:
            for r in rows:
                fired = {e.get("rule_id") for e in (r.get(col) or []) if isinstance(e, dict)}
                for rid in ids:
                    seen[f"rule {rid}"].add(rid in fired)
    if t.filter or (t.type == "filter"):
        seen["filter"] = set()
        for rows, n in zip(outs, in_counts):
            if n:
                seen["filter"].add("kept" if rows else None)
                if len(rows) < n:
                    seen["filter"].add("dropped")
    return seen


def expected_branch_values(label: str) -> Set[Any]:
    if label.startswith("boolean"):
        return {True, False}
    if label.startswith("rule"):
        return {True, False}
    if label == "filter":
        return {"kept", "dropped"}
    return set()

