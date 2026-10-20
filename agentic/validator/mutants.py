"""Mutation operators over a blueprint node. A mutant is a plausible mistake in the conversion (a dropped
filter, a flipped comparison, a changed threshold, a wrong join type...). A good test suite kills every
non-equivalent mutant; survivors point at logic the tests do not actually check."""
import copy
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

import sqlglot
from sqlglot import exp

from ..schemas import Transformation

Path = Tuple[Any, ...]


@dataclass
class Mutant:
    node: str
    operator: str
    description: str
    transformation: Transformation

    def to_dict(self):
        return {"node": self.node, "operator": self.operator, "description": self.description}


_CMP_FLIP = {exp.GT: exp.GTE, exp.GTE: exp.GT, exp.LT: exp.LTE, exp.LTE: exp.LT, exp.EQ: exp.NEQ, exp.NEQ: exp.EQ}
_AGG_SWAP = {exp.Sum: exp.Max, exp.Max: exp.Min, exp.Min: exp.Max, exp.Avg: exp.Sum}


def _sites(tree) -> List[Tuple[str, Callable]]:
    """Mutation sites in an expression tree: (description, fn(tree) -> new tree). Each fn mutates a COPY."""
    sites = []
    nodes = list(tree.walk())
    for k, n in enumerate(nodes):
        def at(fn, k=k):
            def apply(root):
                root = root.copy()
                target = list(root.walk())[k]
                return fn(root, target)
            return apply
        if type(n) in _CMP_FLIP:
            sites.append((f"comparison {n.sql(dialect='spark')} -> {_CMP_FLIP[type(n)].__name__}",
                          at(lambda root, t: _replace(root, t, _CMP_FLIP[type(t)](this=t.left.copy(), expression=t.right.copy())))))
        elif isinstance(n, exp.And):
            sites.append(("AND -> OR", at(lambda root, t: _replace(root, t, exp.Or(this=t.left.copy(), expression=t.right.copy())))))
        elif isinstance(n, exp.Or):
            sites.append(("OR -> AND", at(lambda root, t: _replace(root, t, exp.And(this=t.left.copy(), expression=t.right.copy())))))
        elif isinstance(n, exp.Literal) and not n.is_string and not isinstance(n.parent, (exp.Bracket, exp.Limit)):
            def tweak(root, t):
                v = t.this
                new = str(int(v) + 1) if re.fullmatch(r"\d+", v) else repr(round(float(v) + 0.01, 6)) if float(v) < 1 else str(round(float(v) + 1, 6))
                return _replace(root, t, exp.Literal.number(new))
            sites.append((f"constant {n.this} changed", at(tweak)))
        elif isinstance(n, exp.Is) and isinstance(n.expression, exp.Null):
            if isinstance(n.parent, exp.Not):
                sites.append(("IS NOT NULL -> IS NULL", at(lambda root, t: _replace(root, t.parent, t.copy()))))
            else:
                sites.append(("IS NULL -> IS NOT NULL", at(lambda root, t: _replace(root, t, exp.Not(this=t.copy())))))
        elif type(n) in _AGG_SWAP:
            sites.append((f"aggregate {type(n).__name__} -> {_AGG_SWAP[type(n)].__name__}",
                          at(lambda root, t: _replace(root, t, _AGG_SWAP[type(t)](this=t.this.copy())))))
        elif isinstance(n, exp.Not) and not isinstance(n.this, exp.Is):
            sites.append(("NOT removed", at(lambda root, t: _replace(root, t, t.this.copy()))))
        elif isinstance(n, exp.In) and len(n.expressions) > 1:
            sites.append(("IN list drops its first value", at(lambda root, t: _replace(root, t, exp.In(this=t.this.copy(), expressions=[e.copy() for e in t.expressions[1:]])))))
        elif isinstance(n, exp.Coalesce) and n.expressions:
            sites.append(("COALESCE default dropped", at(lambda root, t: _replace(root, t, t.this.copy()))))
    return sites


def _replace(root, target, new):
    if target is root:
        return new
    target.replace(new)
    return root


def _expr_paths(d: dict) -> List[Tuple[Path, str]]:
    """Paths to expression strings inside a node dict."""
    out = []
    def walk(o, path):
        if isinstance(o, dict):
            for k, v in o.items():
                if k in ("expr", "filter", "condition", "when", "sql") and isinstance(v, str):
                    out.append((path + (k,), v))
                elif k == "on" and isinstance(v, str):
                    out.append((path + (k,), v))
                elif k == "agg" and isinstance(v, dict):
                    for kk, vv in v.items():
                        if isinstance(vv, str):
                            out.append((path + (k, kk), vv))
                elif k in ("order_by",) and isinstance(v, list):
                    pass
                else:
                    walk(v, path + (k,))
        elif isinstance(o, list):
            for i, v in enumerate(o):
                walk(v, path + (i,))
    walk(d, ())
    return out


def _set(d: dict, path: Path, value) -> dict:
    d = copy.deepcopy(d)
    cur = d
    for p in path[:-1]:
        cur = cur[p]
    cur[path[-1]] = value
    return d


def _scalar_paths(d: dict, keys=("score",)) -> List[Tuple[Path, Any]]:
    out = []
    for i, r in enumerate(d.get("rules") or []):
        if isinstance(r, dict):
            for k, v in r.items():
                if k in keys and isinstance(v, (int, float)):
                    out.append((("rules", i, k), v))
    return out


def mutants_for(t: Transformation, limit: int = 60) -> List[Mutant]:
    d = t.model_dump(by_alias=True, exclude_none=True)
    muts: List[Mutant] = []

    def add(op, desc, nd):
        try:
            muts.append(Mutant(t.id, op, desc, Transformation(**nd)))
        except Exception:
            pass

    # expression-level
    for path, text in _expr_paths(d):
        try:
            tree = sqlglot.parse_one(re.sub(r":(\w+)", r"__P_\1__", text), read="spark")
        except Exception:
            continue
        for desc, fn in _sites(tree):
            try:
                new = re.sub(r"__P_(\w+)__", r":\1", fn(tree).sql(dialect="spark"))
            except Exception:
                continue
            if new.replace(" ", "").lower() != text.replace(" ", "").lower():
                label = ".".join(str(p) for p in path)
                add("expr", f"{label}: {desc}  [{text[:50]} -> {new[:50]}]", _set(d, path, new))
    # structural
    if d.get("filter") and t.type != "filter":
        nd = copy.deepcopy(d); nd.pop("filter"); add("drop-filter", "filter removed", nd)
    if d.get("dedupe_keys"):
        nd = copy.deepcopy(d); nd.pop("dedupe_keys"); add("drop-dedupe", "deduplication removed", nd)
        if len(d["dedupe_keys"]) > 1:
            nd = copy.deepcopy(d); nd["dedupe_keys"] = d["dedupe_keys"][:-1]; add("dedupe-key", f"dedupe key '{d['dedupe_keys'][-1]}' dropped", nd)
    for i, j in enumerate(d.get("joins") or []):
        ty = str(j.get("type", "inner")).lower()
        for new in ({"inner": ["left"], "left": ["inner"]}.get(ty, [])):
            nd = copy.deepcopy(d); nd["joins"][i]["type"] = new; add("join-type", f"join {i} ({j.get('right')}): {ty} -> {new}", nd)
        if isinstance(j.get("on"), list) and len(j["on"]) > 1:
            nd = copy.deepcopy(d); nd["joins"][i]["on"] = j["on"][:-1]; add("join-key", f"join {i}: key '{j['on'][-1]}' dropped", nd)
    w = d.get("window")
    if w:
        if len(w.get("partition_by") or []) >= 1:
            nd = copy.deepcopy(d); nd["window"]["partition_by"] = (w["partition_by"] or [])[:-1]; add("window-partition", f"window partition '{w['partition_by'][-1]}' dropped", nd)
        fr = str(w.get("frame", ""))
        m = re.search(r"(\d+)\s+PRECEDING", fr)
        if m:
            nd = copy.deepcopy(d); nd["window"]["frame"] = fr.replace(m.group(0), f"{int(m.group(1)) + 1} PRECEDING", 1); add("window-frame", f"window frame {m.group(1)} -> {int(m.group(1)) + 1}", nd)
        if fr.upper().startswith("RANGE"):
            nd = copy.deepcopy(d); nd["window"]["frame"] = re.sub(r"(?i)^RANGE", "ROWS", fr); add("window-type", "window RANGE -> ROWS", nd)
    if len(d.get("group_by") or []) > 1:
        nd = copy.deepcopy(d); nd["group_by"] = d["group_by"][:-1]; add("group-key", f"group-by key '{d['group_by'][-1]}' dropped", nd)
    if len(d.get("aggregations") or []) > 1:
        nd = copy.deepcopy(d); nd["aggregations"] = d["aggregations"][:-1]; add("drop-agg", f"aggregation '{d['aggregations'][-1].get('name')}' removed", nd)
    for path, v in _scalar_paths(d):
        add("rule-score", f"rule {d['rules'][path[1]].get('rule_id')} score {v} -> {v + 1}", _set(d, path, v + 1))
    if len(d.get("rules") or []) > 1:
        nd = copy.deepcopy(d); removed = nd["rules"].pop(); add("drop-rule", f"rule {removed.get('rule_id')} removed", nd)
    # keep a spread across operator kinds if there are too many
    if len(muts) > limit:
        by_op: Dict[str, List[Mutant]] = {}
        for m in muts:
            by_op.setdefault(m.operator, []).append(m)
        picked, k = [], 0
        while len(picked) < limit:
            added = False
            for ms in by_op.values():
                if k < len(ms) and len(picked) < limit:
                    picked.append(ms[k]); added = True
            if not added:
                break
            k += 1
        muts = picked
    return muts
