"""Synthetic, deterministic test data generated from a rich blueprint's source schemas.

Heuristics keep joins productive: the first `*_id` column of a source is its primary key
(unique), other `*_id` columns are foreign keys drawn from the first n/10 keys of the same
name, dimension tables (no id column) get one row per distinct key, and columns named in a
`pre_filter` (`rate_date = :run_date`) are pinned to the parameter value. Override entirely
with `validation.data_script` (a .py file defining `generate(blueprint, params) -> dict`).
"""
import random
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from .schemas import PipelineBlueprint, SchemaField

_POOLS = {
    "currency": ["USD", "EUR", "GBP", "INR", "AED"],
    "country": ["IN", "US", "GB", "DE", "SG", "AE", "CA"],
    "status": ["POSTED"] * 8 + ["PENDING", "REVERSED"],
    "txn_type": ["CASH_DEP", "CASH_WDL", "CASH_WDL", "TRANSFER", "CARD", "ACH"],
    "type": ["HOME", "WORK", "MAIL"],
    "segment": ["RETAIL", "PREMIUM", "SME", "CORP"],
    "city": ["Mumbai", "London", "Berlin", "Austin", "Dubai"],
    "risk_level": ["HIGH", "MEDIUM"],
    "channel": ["ATM", "ONLINE", "BRANCH"],
}
_MAP_VALUES = {"joint_holder": ["Y", "N"], "product_code": ["SAV", "CHK", "CC", "LOAN"],
               "interest_tier": ["T1", "T2", "T3"]}
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


# ---- types --------------------------------------------------------------------------------
def field_type_str(f: SchemaField) -> str:
    """Canonical Spark DDL type string for a (possibly structured) schema field."""
    k = f.type.strip().upper()
    if k == "STRUCT" and f.fields:
        return "STRUCT<%s>" % ",".join(f"{c.name}:{field_type_str(c)}" for c in f.fields)
    if k == "ARRAY":
        if f.fields:
            el = "STRUCT<%s>" % ",".join(f"{c.name}:{field_type_str(c)}" for c in f.fields)
        else:
            el = f.element_type or "STRING"
        return f"ARRAY<{el}>"
    if k == "MAP":
        return f"MAP<{f.key_type or 'STRING'},{f.value_type or 'STRING'}>"
    return f.type.strip()


def _split_top(s: str) -> List[str]:
    out, depth, cur = [], 0, ""
    for ch in s:
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        if ch == "," and depth == 0:
            out.append(cur)
            cur = ""
        else:
            cur += ch
    return out + [cur] if cur.strip() else out


def parse_type(t: str):
    t = t.strip()
    u = t.upper()
    if u.startswith("ARRAY<"):
        return ("array", parse_type(t[6:-1]))
    if u.startswith("MAP<"):
        k, v = _split_top(t[4:-1])
        return ("map", parse_type(k), parse_type(v))
    if u.startswith("STRUCT<"):
        fields = []
        for item in _split_top(t[7:-1]):
            n, ty = item.split(":", 1)
            fields.append((n.strip().strip("`"), parse_type(ty)))
        return ("struct", fields)
    m = re.match(r"DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", u)
    if m:
        return ("decimal", int(m.group(1)), int(m.group(2)))
    return ("prim", re.sub(r"\(.*\)", "", u).strip())


# ---- values -------------------------------------------------------------------------------
class _Gen:
    def __init__(self, seed: int, run_date: str, n_keys: int):
        self.rng = random.Random(seed)
        self.run_date, self.n_keys = run_date, n_keys

    def key(self, name: str, i: int) -> str:
        return f"{name[0].upper()}{i:07d}"

    def string(self, name: str) -> str:
        n = name.lower()
        for k in sorted(_POOLS, key=len, reverse=True):
            if n == k or n.endswith("_" + k) or n.endswith(k):
                return self.rng.choice(_POOLS[k])
        if n.endswith("_id") or n == "id":
            return self.key(n, self.rng.randrange(self.n_keys))
        if "bic" in n:
            return self.rng.choice(["HDFCINBB", "CHASUS33", "DEUTDEFF", "BARCGB22"])
        if "account_no" in n:
            return "AC%08d" % self.rng.randrange(10 ** 8)
        if n.endswith("name"):
            return self.rng.choice(["Acme Ltd", "Globex", "Initech", "Self", "Umbrella"])
        return f"{n}_{self.rng.randint(1, 20)}"

    def number(self, name: str) -> float:
        r = self.rng.random()
        if "amount" in name.lower():
            if r < 0.12:
                return round(self.rng.uniform(8000, 9999.99), 2)  # near-threshold band
            if r < 0.17:
                return round(self.rng.uniform(10000, 50000), 2)
            return round(self.rng.uniform(5, 4000), 2)
        if "rate" in name.lower():
            return round(self.rng.uniform(0.01, 2.0), 6)
        return round(self.rng.uniform(0, 1000), 2)

    def value(self, node, name: str, known_keys=None):
        kind = node[0]
        if kind == "decimal":
            return round(self.number(name), node[2])
        if kind == "array":
            n = self.rng.randint(1, 3)
            vals = [self.value(node[1], name) for _ in range(n)]
            if node[1][0] == "struct" and any(f == "is_primary" for f, _ in node[1][1]):
                for j, v in enumerate(vals):
                    v["is_primary"] = j == 0
            return vals
        if kind == "map":
            keys = known_keys or [f"{name}_k{j}" for j in range(3)]
            return {k: self._map_value(k) for k in keys if self.rng.random() < 0.85}
        if kind == "struct":
            return {fn: self.value(ft, fn) for fn, ft in node[1]}
        t = node[1]
        if t in ("STRING", "VARCHAR", "CHAR"):
            return self.string(name)
        if t == "BOOLEAN":
            return self.rng.random() < 0.2
        if t in ("INT", "INTEGER", "BIGINT", "LONG", "SMALLINT", "TINYINT", "SHORT"):
            return self.rng.randint(0, 1000)
        if t in ("DOUBLE", "FLOAT", "REAL"):
            return self.number(name)
        if t == "DATE":
            return self.run_date
        if t in ("TIMESTAMP", "TIMESTAMP_NTZ"):
            base = datetime.strptime(self.run_date, "%Y-%m-%d")
            return (base + timedelta(seconds=self.rng.randrange(86400))).strftime("%Y-%m-%dT%H:%M:%S")
        return self.string(name)

    def _map_value(self, key: str) -> str:
        if key in _MAP_VALUES:
            return self.rng.choice(_MAP_VALUES[key])
        if "limit" in key:
            return str(self.rng.choice([0, 500, 1000, 5000]))
        return str(self.rng.randint(1, 9))


def _run_date(params: Dict[str, Any]) -> str:
    for v in params.values():
        if isinstance(v, str) and _DATE_RE.fullmatch(v):
            return v
    return datetime.now().strftime("%Y-%m-%d")


def _map_keys_used(bp: PipelineBlueprint) -> Dict[str, List[str]]:
    """Map keys the job actually reads, found as col['key'] in transformation expressions."""
    found: Dict[str, List[str]] = {}
    texts = []
    for t in bp.transformations:
        texts += [str(c.get("expr", "")) for c in (t.columns or []) if isinstance(c, dict)]
        texts += [str(a.get("expr", "")) for a in (t.aggregations or []) if isinstance(a, dict)]
        texts += [t.filter or "", t.sql or ""]
    for text in texts:
        for col, key in re.findall(r"\b(\w+)\s*\[\s*['\"](\w+)['\"]\s*\]", text):
            if key not in found.setdefault(col, []):
                found[col].append(key)
    return found


# ---- public -------------------------------------------------------------------------------
def generate(bp: PipelineBlueprint, params: Dict[str, Any] = None, n_rows: int = 2000,
             seed: int = 7) -> Dict[str, Dict[str, Any]]:
    """-> {source_id: {"ddl": "`a` STRING, ...", "rows": [dict, ...]}}"""
    params = {**bp.parameters, **(params or {})}
    g = _Gen(seed, _run_date(params), max(10, n_rows // 10))
    out: Dict[str, Dict[str, Any]] = {}
    used_keys = _map_keys_used(bp)
    for s in bp.sources:
        if not s.schema_:  # no declared / catalog schema: there is nothing to generate rows from
            out[s.id] = {"ddl": "", "rows": [], "skipped": "source has no schema"}
            continue
        cols: List[Tuple[str, str, Any, List[str]]] = []  # name, ddl type, parsed, known_keys
        for f in s.schema_:
            ts = field_type_str(f)
            cols.append((f.name, ts, parse_type(ts), f.known_keys or used_keys.get(f.name, [])))
        pinned: Dict[str, Any] = {}
        for col, p in re.findall(r"(\w+)\s*=\s*:(\w+)", s.pre_filter or ""):
            if p in params:
                pinned[col] = params[p]
                if col not in {c[0] for c in cols}:
                    cols.append((col, "DATE", ("prim", "DATE"), []))
        id_cols = [c[0] for c in cols if c[0].endswith("_id")]
        pk = id_cols[0] if id_cols else None
        n = n_rows
        rows = []
        if pk is None:  # dimension / reference table: one row per distinct first-column value
            seen = set()
            for _ in range(60):
                row = {c[0]: g.value(c[2], c[0], c[3]) for c in cols}
                row.update(pinned)
                k = json_key(row[cols[0][0]])
                if k not in seen:
                    seen.add(k)
                    rows.append(row)
        else:
            for i in range(n):
                row = {}
                for name, _, node, kk in cols:
                    row[name] = g.key(name, i) if name == pk else g.value(node, name, kk)
                row.update(pinned)
                rows.append(row)
        out[s.id] = {"ddl": ", ".join(f"`{c[0]}` {c[1]}" for c in cols), "rows": rows}
    return out


def json_key(v) -> str:
    import json
    return json.dumps(v, sort_keys=True, default=str)
