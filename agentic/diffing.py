"""Pure-Python comparison of two target outputs (rows as dicts + Spark dtypes)."""
import json
import math
from typing import Any, Dict, List, Optional, Sequence

REL_TOL = 1e-6
ABS_TOL = 1e-9


def _norm_type(t: str) -> str:
    return "".join(t.lower().split())


def values_equal(a: Any, b: Any) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return math.isclose(a, b, rel_tol=REL_TOL, abs_tol=ABS_TOL)
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(values_equal(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(values_equal(x, y) for x, y in zip(a, b))
    return a == b


def _canon(row: dict) -> str:
    return json.dumps(row, sort_keys=True, default=str)


def compare_outputs(cand_rows: List[dict], base_rows: List[dict],
                    cand_dtypes: Sequence[Sequence[str]], base_dtypes: Sequence[Sequence[str]],
                    keys: Optional[List[str]] = None, max_examples: int = 3) -> List[str]:
    """Return a list of human-readable differences (empty list == outputs match)."""
    diffs: List[str] = []
    ct = {n.lower(): _norm_type(t) for n, t in cand_dtypes}
    bt = {n.lower(): _norm_type(t) for n, t in base_dtypes}
    if set(ct) != set(bt):
        diffs.append(f"columns differ: only in candidate {sorted(set(ct) - set(bt))}, "
                     f"only in baseline {sorted(set(bt) - set(ct))}")
    for c in sorted(set(ct) & set(bt)):
        if ct[c] != bt[c]:
            diffs.append(f"type of '{c}': candidate {ct[c]} vs baseline {bt[c]}")
    if [n.lower() for n, _ in cand_dtypes] != [n.lower() for n, _ in base_dtypes] and set(ct) == set(bt):
        diffs.append("column ORDER differs (matters for insertInto, which writes by position): "
                     f"candidate {[n for n, _ in cand_dtypes]} vs baseline {[n for n, _ in base_dtypes]}")
    if len(cand_rows) != len(base_rows):
        diffs.append(f"row count: candidate {len(cand_rows)} vs baseline {len(base_rows)}")

    keys = [k for k in (keys or []) if k in ct and k in bt]

    def index(rows):
        if not keys:
            return None
        idx = {}
        for r in rows:
            k = tuple(json.dumps(r.get(c), default=str) for c in keys)
            if k in idx:
                return None  # keys not unique
            idx[k] = r
        return idx

    ci, bi = index(cand_rows), index(base_rows)
    if ci is not None and bi is not None:
        def show(ks):
            return [[json.loads(x) for x in k] for k in ks[:max_examples]]
        missing, extra = sorted(set(bi) - set(ci)), sorted(set(ci) - set(bi))
        if missing:
            diffs.append(f"{len(missing)} key(s) missing from candidate, e.g. {show(missing)}")
        if extra:
            diffs.append(f"{len(extra)} extra key(s) in candidate, e.g. {show(extra)}")
        per_col: Dict[str, List[tuple]] = {}
        for k in set(ci) & set(bi):
            for c in set(ct) & set(bt):
                a, b = ci[k].get(c), bi[k].get(c)
                if not values_equal(a, b):
                    per_col.setdefault(c, []).append((k, a, b))
        for c, items in sorted(per_col.items(), key=lambda kv: -len(kv[1])):
            ex = "; ".join(f"key={[json.loads(x) for x in k]} cand={a!r} base={b!r}"
                           for k, a, b in items[:max_examples])
            diffs.append(f"column '{c}' differs in {len(items)} row(s): {ex}")
    else:  # no usable unique key: multiset comparison on whole rows
        cs, bs = sorted(map(_canon, cand_rows)), sorted(map(_canon, base_rows))
        if cs != bs:
            only_c = [r for r in cs if r not in set(bs)]
            only_b = [r for r in bs if r not in set(cs)]
            diffs.append(f"{len(only_c)} candidate row(s) / {len(only_b)} baseline row(s) have no match "
                         f"(no unique key to align on). e.g. cand={only_c[:1]} base={only_b[:1]}")
    return diffs
