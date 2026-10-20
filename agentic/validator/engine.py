"""Deterministic executor for the rich blueprint's operation graph, on Spark.

Every operation type has a fixed implementation built from the DataFrame API and Spark SQL expressions (the
same constructs the generated job uses), so semantics are Spark's own. No LLM, no Python callbacks, no
arbitrary code: anything that cannot be executed raises a node-attributed EngineError.

Uses:
  * analysis - build the graph on EMPTY tables: Spark's analyzer reports unresolved columns per node
  * testing  - run a single node on crafted inputs (scenarios) or the whole graph on fixtures
  * mutation - run a modified copy of a node and compare outputs

Execution is lazy: structural problems surface when a node is built, data-dependent failures (e.g. a NULL
map key) when its result is collected; `diagnose()` pins a runtime failure on the node that caused it.
`check_shape()` validates an operation's specification in pure Python (no Spark needed).
"""
import json
import re
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from sqlglot import exp

from ..schemas import PipelineBlueprint, Source, Target, Transformation
from ..testdata import field_type_str
from .spark_session import get_spark
from .values import Schema, ddl_of, new_tempdir, write_ndjson

_PARAM_RE = re.compile(r"(?<![\w:]):([A-Za-z_]\w*)")
_OVER_W = re.compile(r"\bOVER\s+w\b", re.I)
_ALIAS_RE = re.compile(r"^(.*?)\s+AS\s+(\w+)\s*$", re.I | re.S)
_JOIN_TYPES = {"inner": "inner", "left": "left", "left_outer": "left", "right": "right", "right_outer": "right",
               "full": "full", "outer": "full", "full_outer": "full", "cross": "cross", "left_semi": "left_semi",
               "semi": "left_semi", "left_anti": "left_anti", "anti": "left_anti"}


def _F():
    from pyspark.sql import functions as F
    return F


class EngineError(Exception):
    def __init__(self, node: str, msg: str, kind: str = "error"):
        super().__init__(msg)
        self.node, self.msg, self.kind = node, msg, kind  # kind: error | unsupported | blocked

    def __str__(self):
        return f"{self.node}: {self.msg}"


def _lit(v) -> str:
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _bt(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def spark_msg(e: Exception) -> str:
    """First meaningful line of a Spark/Py4J error (drops the Java stack)."""
    text = getattr(e, "desc", None) or str(e)
    for line in str(text).splitlines():
        line = line.strip()
        if line and not line.startswith(("at ", "Traceback", "JVM stacktrace", "py4j")):
            return line[:400]
    return type(e).__name__


# ---- operation specs: pure-Python shape validation -------------------------------------------
_REQUIRED = {  # type -> (keys that must be present and non-empty)
    "select": ["select"], "filter": ["filter"], "derive_columns": ["columns"], "nested_derive": ["columns"],
    "filter_dedupe_flatten": [], "join": ["joins"], "join_derive": ["joins", "columns"], "window": ["window", "columns"],
    "aggregate": [], "explode_aggregate": ["explode"], "collect_to_array_of_struct": ["group_by", "output"],
    "collect_to_map": ["steps"], "build_array_of_struct": ["output_column", "rules"], "union": [], "spark_sql": ["sql"],
}


def _get(t: Transformation, key: str):
    v = getattr(t, key, None)
    if v is None and t.model_extra:
        v = t.model_extra.get(key)
    return v


def check_shape(t: Transformation) -> Optional[str]:
    """Why this node's specification cannot be executed deterministically (None = fine). Needs no Spark."""
    if t.type == "custom":
        return "'custom' operations have no deterministic implementation"
    if t.type not in _REQUIRED:
        return f"operation type '{t.type}' has no deterministic implementation"
    for k in _REQUIRED[t.type]:
        if not _get(t, k):
            return f"{t.type} node is missing required '{k}'"
    for c in t.columns or []:
        if not (isinstance(c, dict) and c.get("name") and c.get("expr")):
            return f"each column must be a mapping with 'name' and 'expr', got {c!r}"
    for a in t.aggregations or []:
        if not (isinstance(a, dict) and a.get("name") and a.get("expr")):
            return f"each aggregation must be a mapping with 'name' and 'expr', got {a!r}"
    if t.type == "aggregate" and not (t.group_by or t.aggregations):
        return "aggregate has neither group_by nor aggregations"
    if t.type == "explode_aggregate":
        ex = _get(t, "explode")
        if not (isinstance(ex, dict) and ex.get("column")):
            return "explode must be a mapping {column, alias}"
    if t.type == "collect_to_array_of_struct":
        out = _get(t, "output")
        if not (isinstance(out, dict) and out.get("name") and out.get("expr")):
            return "output must be a mapping {name, expr}"
    if t.type == "collect_to_map":
        steps = _get(t, "steps")
        if not (isinstance(steps, list) and all(isinstance(s, dict) and s.get("group_by") is not None
                                                and isinstance(s.get("agg"), dict) and s["agg"] for s in steps)):
            return (f"collect_to_map.steps must be a list of {{group_by: [...], agg: {{name: expr}}}}; got {steps!r} "
                    "(free text cannot be executed deterministically)")
    if t.type == "build_array_of_struct":
        for r in _get(t, "rules") or []:
            if not (isinstance(r, dict) and (r.get("when") or r.get("condition"))):
                return f"rule {r!r} has no 'when'/'condition'"
    if t.type in ("join", "join_derive"):
        for j in t.joins or []:
            if not isinstance(j, dict) or not j.get("right"):
                return f"each join must be a mapping with 'left', 'right', 'type', 'on'; got {j!r}"
            if str(j.get("type", "inner")).lower() not in _JOIN_TYPES:
                return f"unknown join type {j.get('type')!r}"
            if str(j.get("type", "inner")).lower() != "cross" and not (j.get("on") and isinstance(j["on"], (list, str))):
                return f"join with '{j.get('right')}' has no usable 'on'"
    if t.type == "window" and not (t.window or {}).get("partition_by") and not (t.window or {}).get("order_by"):
        return "window needs partition_by and/or order_by"
    return None


def shape_problems(bp: PipelineBlueprint) -> Dict[str, str]:
    return {t.id: msg for t in bp.transformations if (msg := check_shape(t))}


# ---- engine ----------------------------------------------------------------------------------
class Engine:
    def __init__(self, bp: PipelineBlueprint, params: Dict[str, Any] = None, spark=None):
        self.bp = bp
        self.params = {**bp.parameters, **(params or {})}
        self.spark = spark or get_spark()
        self.dir = new_tempdir()
        self.df: Dict[str, Any] = {}                     # node id -> DataFrame
        self.cols: Dict[str, List[Tuple[str, str]]] = {}  # node id -> [(name, spark type)]
        self.errors: Dict[str, EngineError] = {}

    # ---- plumbing -------------------------------------------------------------------------
    def _set(self, node: str, df) -> None:
        self.df[node] = df
        self.cols[node] = [(n, t) for n, t in df.dtypes]

    def load_rows(self, schema: Schema, rows: List[dict], node: str = "?"):
        ddl = ddl_of(schema)
        try:
            if not rows:
                return self.spark.createDataFrame([], ddl)
            path = write_ndjson(rows, self.dir, "rows")
            return (self.spark.read.schema(ddl).option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss").json(path))
        except Exception as e:
            raise EngineError(node, f"cannot load test data: {spark_msg(e)}")

    def register_input(self, node_id: str, schema: Schema, rows: List[dict]) -> None:
        self._set(node_id, self.load_rows(schema, rows, node_id))

    # ---- expressions ----------------------------------------------------------------------
    def _subst(self, text: str) -> str:
        return _PARAM_RE.sub(lambda m: _lit(self.params[m.group(1)]) if m.group(1) in self.params else m.group(0), text)

    def window_sql(self, window: dict) -> str:
        parts = []
        if window.get("partition_by"):
            parts.append("PARTITION BY " + ", ".join(str(x) for x in window["partition_by"]))
        if window.get("order_by"):
            parts.append("ORDER BY " + ", ".join(str(x) for x in window["order_by"]))
        if window.get("frame"):
            parts.append(str(window["frame"]))
        return " ".join(parts)

    def expr_text(self, text: str, node: str, window: Optional[dict] = None) -> str:
        """Spark SQL text for an expression: parameters substituted, `OVER w` expanded, and (for a window node)
        bare aggregates wrapped in the node's window."""
        s = self._subst(str(text))
        if window:
            s = _OVER_W.sub(f"OVER ({self.window_sql(window)})", s)
            try:
                tree = sqlglot.parse_one(s, read="spark")
            except Exception as e:
                raise EngineError(node, f"cannot parse expression {text!r}: {str(e).splitlines()[0]}", "unsupported")
            if not list(tree.find_all(exp.Window)) and list(tree.find_all(exp.AggFunc)):
                spec = sqlglot.parse_one(f"SUM(x) OVER ({self.window_sql(window)})", read="spark")

                def wrap(n):
                    if isinstance(n, exp.AggFunc) and not isinstance(n.parent, exp.Window):
                        return exp.Window(this=n, partition_by=spec.args.get("partition_by"),
                                          order=spec.args.get("order"), spec=spec.args.get("spec"))
                    return n
                s = tree.transform(wrap).sql(dialect="spark")
        return s

    def E(self, text: str, node: str, window: Optional[dict] = None):
        return _F().expr(self.expr_text(text, node, window))

    # ---- sources / targets ----------------------------------------------------------------
    def build_source(self, s: Source, rows: List[dict]) -> None:
        try:
            if not s.schema_:
                raise EngineError(s.id, "source has no schema; cannot be executed", "unsupported")
            schema = [(f.name, field_type_str(f)) for f in s.schema_]
            for col, _p in re.findall(r"(\w+)\s*=\s*:(\w+)", s.pre_filter or ""):
                if col not in {c for c, _ in schema}:
                    schema.append((col, "date"))
            df = self.load_rows(schema, rows, s.id)
            df = df.selectExpr(*[f"{_bt(f.name)} AS {_bt(f.rename_to or f.name)}" for f in s.schema_])
            if s.pre_filter:
                df = df.filter(self.E(s.pre_filter, s.id))
            self._set(s.id, df)
        except EngineError as e:
            self.errors[s.id] = e
        except Exception as e:
            self.errors[s.id] = EngineError(s.id, spark_msg(e))

    def build_target(self, tg: Target) -> None:
        try:
            if tg.input not in self.df:
                raise EngineError(tg.id, f"input '{tg.input}' did not build", "blocked")
            have = {c.lower(): c for c, _ in self.cols[tg.input]}
            df = self.df[tg.input]
            if tg.schema_:
                F = _F()
                sel = []
                for f in tg.schema_:
                    if f.name.lower() not in have:
                        raise EngineError(tg.id, f"target column '{f.name}' is not produced by '{tg.input}' "
                                                 f"(available: {sorted(have.values())})")
                    sel.append(F.col(_bt(have[f.name.lower()])).cast(field_type_str(f)).alias(f.name))
                for p in tg.partition_by:
                    if p.lower() not in {f.name.lower() for f in tg.schema_}:
                        if p.lower() not in have:
                            raise EngineError(tg.id, f"partition column '{p}' is not produced by '{tg.input}'")
                        sel.append(F.col(_bt(have[p.lower()])))
                df = df.select(*sel)
            self._set(tg.id, df)
        except EngineError as e:
            self.errors[tg.id] = e
        except Exception as e:
            self.errors[tg.id] = EngineError(tg.id, spark_msg(e))

    # ---- transformations ------------------------------------------------------------------
    def build_transformation(self, t: Transformation) -> None:
        try:
            msg = check_shape(t)
            if msg:
                raise EngineError(t.id, msg, "unsupported")
            for i in t.inputs:
                if i not in self.df:
                    raise EngineError(t.id, f"input '{i}' did not build", "blocked")
            self._set(t.id, getattr(self, "_op_" + t.type)(t))
        except EngineError as e:
            self.errors[t.id] = e
        except Exception as e:
            kind = "unsupported" if "ParseException" in type(e).__name__ or "ParseException" in str(e)[:200] else "error"
            self.errors[t.id] = EngineError(t.id, spark_msg(e), kind)

    def _in(self, t: Transformation, k: int = 0):
        return self.df[t.inputs[k]]

    def _columns(self, df, columns, node: str, window: dict = None):
        for c in columns or []:
            df = df.withColumn(c["name"], self.E(c["expr"], node, window))
        return df

    def _project(self, df, select, node: str):
        if not select:
            return df
        F = _F()
        return df.select(*[F.col(_bt(s)) if re.fullmatch(r"\w+", s) else self.E(s, node) for s in select])

    def _group_col(self, g: str, node: str):
        m = _ALIAS_RE.match(g)
        return self.E(m.group(1), node).alias(m.group(2)) if m else self.E(g, node)

    def _aggregate(self, df, group_by, aggs, node: str):
        keys = [self._group_col(g, node) for g in (group_by or [])]
        cols = [self.E(a["expr"], node).alias(a["name"]) for a in (aggs or [])]
        if not cols:
            return df.select(*keys).distinct()
        return (df.groupBy(*keys) if keys else df.groupBy()).agg(*cols)

    def _op_select(self, t):
        return self._project(self._in(t), t.select, t.id)

    def _op_filter(self, t):
        return self._in(t).filter(self.E(t.filter, t.id))

    def _op_derive_columns(self, t):
        df = self._in(t)
        if t.filter:
            df = df.filter(self.E(t.filter, t.id))
        return self._project(self._columns(df, t.columns, t.id), t.select, t.id)

    _op_nested_derive = _op_derive_columns

    def _op_filter_dedupe_flatten(self, t):
        df = self._in(t)
        if t.filter:
            df = df.filter(self.E(t.filter, t.id))
        if t.dedupe_keys:
            df = df.dropDuplicates(list(t.dedupe_keys))
        return self._project(self._columns(df, t.columns, t.id), t.select, t.id)

    def _join_chain(self, t):
        F = _F()
        cur = None
        for j in t.joins:
            left = cur if (j.get("left") == "*" and cur is not None) else self.df.get(j.get("left"))
            right = self.df.get(j.get("right"))
            if left is None or right is None:
                raise EngineError(t.id, f"join references unknown side {j.get('left')!r}/{j.get('right')!r}", "unsupported")
            if "broadcast" in str(j.get("hint", "")).lower():
                right = F.broadcast(right)
            how = _JOIN_TYPES[str(j.get("type", "inner")).lower()]
            on = j.get("on")
            if how == "cross":
                cur = left.crossJoin(right)
            elif isinstance(on, list):
                cur = left.join(right, on=list(on), how=how)
            else:
                cur = left.join(right, self.E(on, t.id), how)
        return cur

    def _op_join(self, t):
        return self._project(self._join_chain(t), t.select, t.id)

    def _op_join_derive(self, t):
        return self._project(self._columns(self._join_chain(t), t.columns, t.id), t.select, t.id)

    def _op_window(self, t):
        return self._project(self._columns(self._in(t), t.columns, t.id, t.window), t.select, t.id)

    def _op_aggregate(self, t):
        return self._aggregate(self._in(t), t.group_by, t.aggregations, t.id)

    def _op_explode_aggregate(self, t):
        F = _F()
        ex = _get(t, "explode")
        alias = ex.get("alias") or ex["column"]
        df = self._in(t).select("*", F.explode(F.col(_bt(ex["column"]))).alias(alias))
        return self._aggregate(df, t.group_by, t.aggregations, t.id) if (t.group_by or t.aggregations) else df

    def _op_collect_to_array_of_struct(self, t):
        out = _get(t, "output")
        return self._aggregate(self._in(t), t.group_by, [{"name": out["name"], "expr": out["expr"]}], t.id)

    def _op_collect_to_map(self, t):
        df = self._in(t)
        for s in _get(t, "steps"):
            df = self._aggregate(df, s["group_by"], [{"name": k, "expr": v} for k, v in s["agg"].items()], t.id)
        return df

    def _op_build_array_of_struct(self, t):
        rules, out = _get(t, "rules"), _get(t, "output_column")
        elements = []
        for r in rules:
            cond = r.get("when") or r.get("condition")
            fields = {k: v for k, v in r.items() if k not in ("when", "condition")}
            struct = "named_struct(" + ", ".join(f"'{k}', {_lit(v)}" for k, v in fields.items()) + ")"
            elements.append(f"CASE WHEN ({self.expr_text(cond, t.id)}) THEN {struct} END")
        return self._in(t).withColumn(out, _F().expr(f"filter(array({', '.join(elements)}), r -> r IS NOT NULL)"))

    def _op_union(self, t):
        df = self._in(t)
        for k in range(1, len(t.inputs)):
            df = df.unionByName(self._in(t, k))
        return df

    def _op_spark_sql(self, t):
        for i in t.inputs:
            self.df[i].createOrReplaceTempView(i)
        return self.spark.sql(self._subst(t.sql))

    # ---- whole graph / results ------------------------------------------------------------
    def run(self, data: Dict[str, List[dict]]) -> "Engine":
        for s in self.bp.sources:
            self.build_source(s, data.get(s.id, []))
        for t in self.bp.transformations:
            self.build_transformation(t)
        for tg in self.bp.targets:
            self.build_target(tg)
        return self

    def fetch(self, node_id: str) -> List[dict]:
        return [json.loads(r) for r in self.df[node_id].toJSON().collect()]

    def diagnose(self) -> Optional[Tuple[str, str]]:
        """Materialize nodes in order; returns (node id, message) of the first one that fails at run time."""
        order = [s.id for s in self.bp.sources] + [t.id for t in self.bp.transformations] + [t.id for t in self.bp.targets]
        for n in order:
            if n in self.df:
                try:
                    self.df[n].collect()
                except Exception as e:
                    return n, spark_msg(e)
        return None


def canonical(rows: List[dict], places: int = 9) -> List[str]:
    """Order-insensitive canonical form of a result set (for equality / mutation comparison)."""
    def rnd(x):
        if isinstance(x, float):
            return round(x, places)
        if isinstance(x, dict):
            return {k: rnd(v) for k, v in x.items()}
        if isinstance(x, list):
            return [rnd(v) for v in x]
        return x
    return sorted(json.dumps(rnd(r), sort_keys=True, default=str) for r in rows)
