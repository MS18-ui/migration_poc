"""Structural and semantic checks for the rich blueprint (sources -> transformations -> targets)."""
import re
from typing import Iterable, List

from .schemas import (SOURCE_TYPES, TARGET_TYPES, TRANSFORM_TYPES, PipelineBlueprint,
                      SchemaField)

_FILE_TYPES = {"parquet", "orc", "csv", "json", "avro", "delta"}
_PARAM_RE = re.compile(r"(?<![\w:]):([A-Za-z_]\w*)")
_BARE_COMPLEX = {"STRUCT", "ARRAY", "MAP"}

# transformation type -> keys that must be present and non-empty
_REQUIRED = {
    "filter": ["filter"],
    "select": ["select"],
    "derive_columns": ["columns"],
    "nested_derive": ["columns"],
    "filter_dedupe_flatten": [],
    "join": ["joins"],
    "join_derive": ["joins", "columns"],
    "window": ["window", "columns"],
    "aggregate": ["group_by", "aggregations"],
    "explode_aggregate": ["explode", "group_by", "aggregations"],
    "build_array_of_struct": ["output_column", "rules"],
    "collect_to_array_of_struct": ["group_by", "output"],
    "collect_to_map": ["steps"],
    "spark_sql": ["sql"],
    "union": [],
    "custom": [],
}


def _check_type_string(owner: str, t: str, errors: List[str]) -> None:
    if t.count("<") != t.count(">") or t.count("(") != t.count(")"):
        errors.append(f"{owner}: unbalanced brackets in type '{t}'.")


def _check_schema(owner: str, fields: Iterable[SchemaField], errors: List[str]) -> None:
    seen = set()
    for f in fields:
        where = f"{owner}.{f.name}"
        if f.name in seen:
            errors.append(f"{owner}: duplicate column '{f.name}'.")
        seen.add(f.name)
        _check_type_string(where, f.type, errors)
        kind = f.type.strip().upper()
        if kind == "STRUCT" and not f.fields:
            errors.append(f"{where}: bare STRUCT needs 'fields' (or use STRUCT<a:T,...>).")
        if kind == "ARRAY" and not (f.element_type or f.fields):
            errors.append(f"{where}: bare ARRAY needs 'element_type' (and 'fields' for STRUCT elements).")
        if kind == "MAP" and not (f.key_type and f.value_type):
            errors.append(f"{where}: bare MAP needs 'key_type' and 'value_type'.")
        if f.fields:
            _check_schema(where, f.fields, errors)


def _exprs_of(t) -> List[str]:
    out = []
    for c in (t.columns or []):
        if isinstance(c, dict) and c.get("expr"):
            out.append(str(c["expr"]))
    for a in (t.aggregations or []):
        if isinstance(a, dict) and a.get("expr"):
            out.append(str(a["expr"]))
    if t.filter:
        out.append(t.filter)
    return out


def _check_expr_syntax(owner: str, expr: str, errors: List[str]) -> None:
    try:
        import sqlglot
    except ImportError:
        return
    if re.search(r"\bOVER\s+\w+\b(?!\s*\()", expr, re.I):
        return  # named-window reference; the WINDOW clause lives in the transformation's `window`
    probe = _PARAM_RE.sub("'0'", expr)
    try:
        sqlglot.parse_one(probe, read="spark")
    except Exception as e:  # sqlglot ParseError / TokenError
        errors.append(f"{owner}: expression does not parse as Spark SQL: {expr!r} ({str(e).splitlines()[0]})")


def check_rich_blueprint(bp: PipelineBlueprint) -> None:
    """Raise ValueError listing every problem found (so the agent can fix them in one pass)."""
    errors: List[str] = []
    params = set(bp.parameters)
    if not bp.name:
        errors.append("Missing job name (set `job.name` or `name`).")

    ids = {}  # id -> kind
    def register(i: str, kind: str):
        if i in ids:
            errors.append(f"Duplicate id '{i}' ({ids[i]} and {kind}); ids must be unique across "
                          "sources, transformations and targets.")
        ids[i] = kind

    for s in bp.sources:
        register(s.id, "source")
        if s.type not in SOURCE_TYPES:
            errors.append(f"source '{s.id}': unknown type '{s.type}' (use one of {list(SOURCE_TYPES)}).")
        elif s.type in _FILE_TYPES and not s.path:
            errors.append(f"source '{s.id}': {s.type} source needs 'path'.")
        elif s.type not in _FILE_TYPES and not (s.object or s.path):
            errors.append(f"source '{s.id}': needs 'object' (db.table).")
        _check_schema(f"source '{s.id}'", s.schema_, errors)
        if s.pre_filter:
            _check_expr_syntax(f"source '{s.id}'.pre_filter", s.pre_filter, errors)

    defined = {s.id for s in bp.sources}
    for t in bp.transformations:
        register(t.id, "transformation")
        if t.type not in TRANSFORM_TYPES:
            errors.append(f"transformation '{t.id}': unknown type '{t.type}' (use one of "
                          f"{list(TRANSFORM_TYPES)}; use 'custom' with `sql` or `notes` if none fit).")
        if not t.inputs:
            errors.append(f"transformation '{t.id}': needs at least one input.")
        for i in t.inputs:
            if i not in defined:
                errors.append(f"transformation '{t.id}': input '{i}' is not a source or an EARLIER transformation.")
        for key in _REQUIRED.get(t.type, []):
            val = getattr(t, key, None)
            if val is None and t.model_extra:
                val = t.model_extra.get(key)
            if not val:
                errors.append(f"transformation '{t.id}' (type {t.type}): missing required '{key}'.")
        for j in (t.joins or []):
            if not isinstance(j, dict):
                errors.append(f"transformation '{t.id}': each join must be a mapping.")
                continue
            if j.get("right") not in t.inputs:
                errors.append(f"transformation '{t.id}': join right '{j.get('right')}' is not in inputs.")
            if j.get("left") not in (*t.inputs, "*"):
                errors.append(f"transformation '{t.id}': join left '{j.get('left')}' must be an input or '*'.")
            if not j.get("on"):
                errors.append(f"transformation '{t.id}': join with '{j.get('right')}' has no 'on'.")
        win = t.window or {}
        if t.window and not (win.get("partition_by") or win.get("order_by")):
            errors.append(f"transformation '{t.id}': window needs partition_by and/or order_by.")
        for e in _exprs_of(t):
            _check_expr_syntax(f"transformation '{t.id}'", e, errors)
            for p in _PARAM_RE.findall(e):
                if p not in params:
                    errors.append(f"transformation '{t.id}': uses :{p} but job.parameters has no '{p}'.")
        defined.add(t.id)

    for tg in bp.targets:
        register(tg.id, "target")
        if tg.type not in TARGET_TYPES:
            errors.append(f"target '{tg.id}': unknown type '{tg.type}' (use one of {list(TARGET_TYPES)}).")
        elif tg.type in ("hive_table", "iceberg_table") and not tg.object:
            errors.append(f"target '{tg.id}': table target needs 'object' (db.table).")
        elif tg.type in _FILE_TYPES and not tg.path:
            errors.append(f"target '{tg.id}': file target needs 'path'.")
        if tg.input not in defined:
            errors.append(f"target '{tg.id}': input '{tg.input}' is not a source or transformation.")
        _check_schema(f"target '{tg.id}'", tg.schema_, errors)
        cols = {f.name for f in tg.schema_}
        producer = next((t for t in bp.transformations if t.id == tg.input), None)
        if producer:  # partition columns may be declared outside `schema` but must be produced upstream
            cols |= {c["name"] for c in (producer.columns or []) if isinstance(c, dict) and "name" in c}
            cols |= set(producer.select or [])
        if tg.schema_:
            for p in tg.partition_by:
                if p not in cols:
                    errors.append(f"target '{tg.id}': partition column '{p}' is neither in its schema "
                                  f"nor produced by '{tg.input}'.")

    if bp.transformations and not bp.targets:
        errors.append("A rich blueprint with transformations needs at least one target.")

    transform_ids = {t.id for t in bp.transformations}
    for ln in bp.lineage:
        for v in ln.via:
            if v not in transform_ids:
                errors.append(f"lineage '{ln.target_column}': via '{v}' is not a transformation id.")

    if errors:
        shown = errors[:25]
        extra = f"\n... and {len(errors) - 25} more." if len(errors) > 25 else ""
        raise ValueError("Blueprint has %d problem(s):\n- %s%s" % (len(errors), "\n- ".join(shown), extra))
