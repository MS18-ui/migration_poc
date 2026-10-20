"""Two-way coverage between the legacy source and the blueprint YAML.

  source -> YAML : every read/write, constant, comparison (column, operator, threshold), join (type,
                   count), dedupe key set, window (partition, frame), config key and created column in the
                   source must be represented in the YAML (dropped logic)
  YAML -> source : constants and functions in YAML expressions with no basis in the source (invented logic)

Static and heuristic: a 'missing' result means "not found in this form" - findings carry confidence
and the node/line evidence a reviewer needs. Python sources are parsed with `ast` (never executed);
HiveQL sources with sqlglot.
"""
import ast
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp

from ..schemas import PipelineBlueprint
from .findings import Finding

_NUM_RE = re.compile(r"(?<![\w.'])\d+(?:\.\d+)?(?![\w'])")
_STR_RE = re.compile(r"'((?:[^'\\]|\\.)*)'")
_FUNC_RE = re.compile(r"\b([A-Za-z_]\w*)\s*\(")
_NOT_FUNCS = {"and", "or", "not", "in", "over", "as", "when", "then", "else", "end", "case", "is", "null", "by",
              "partition", "order", "between", "select", "from", "where", "like", "exists", "if", "using"}
_TRIVIAL_NUMS = {0.0, 1.0, -1.0}
_INFRA_CONF = ("hadoop", "hive", "warehouse", "master", "metastore", "fs.", "jars", "yarn", "executor", "driver",
               "app.name", "ui.")
_NOTHING = object()
_JOIN_NORM = {"inner": "inner", "left": "left", "left_outer": "left", "leftouter": "left", "right": "right",
              "right_outer": "right", "rightouter": "right", "full": "full", "outer": "full", "full_outer": "full",
              "fullouter": "full", "cross": "cross", "left_semi": "semi", "leftsemi": "semi", "semi": "semi",
              "left_anti": "anti", "leftanti": "anti", "anti": "anti"}
# source function / method name -> token that must appear in the YAML (None = not a logic construct)
_FUNC_TOKEN = {"lit": None, "col": None, "expr": None, "alias": None, "broadcast": "broadcast", "asc": None, "desc": None,
               "isnotnull": "is not null", "isnull": "is null", "isin": "in", "between": "between", "when": "case",
               "otherwise": "case", "getitem": None, "cast": "cast", "countdistinct": "count", "mean": "avg",
               "dropduplicates": "dedupe", "distinct": "distinct"}
_SIGNIFICANT = {"explode", "explode_outer", "collect_list", "collect_set", "sort_array", "map_from_entries",
                "array_distinct", "transform", "filter", "element_at", "size", "array_max", "array_min", "first",
                "last", "sum", "max", "min", "count", "avg", "round", "coalesce", "concat", "lower", "upper", "trim",
                "substring", "split", "datediff", "date_add", "date_sub", "to_date", "to_timestamp", "row_number",
                "rank", "dense_rank", "lag", "lead", "abs", "floor", "ceil", "array_contains", "struct", "array",
                "greatest", "least", "broadcast", "when", "isin", "between", "isnotnull", "isnull", "cast",
                "countdistinct", "stddev", "variance", "mean", "months_between", "date_format", "unix_timestamp",
                "from_unixtime", "regexp_replace", "regexp_extract", "map_keys", "map_values", "flatten", "slice",
                "array_union", "array_intersect", "named_struct", "concat_ws", "nvl", "ifnull", "pow", "sqrt"}


# ---- facts ---------------------------------------------------------------------------------
@dataclass
class Facts:
    reads: List[Tuple[str, str, int]] = field(default_factory=list)         # kind, name/path, line
    writes: List[Tuple[str, str, int]] = field(default_factory=list)        # name, mode, line
    created: List[Tuple[str, int]] = field(default_factory=list)
    nums: List[Tuple[float, int]] = field(default_factory=list)
    strs: List[Tuple[str, int]] = field(default_factory=list)
    funcs: List[Tuple[str, int]] = field(default_factory=list)
    joins: List[Tuple[str, List[str], int]] = field(default_factory=list)  # how, key columns, line
    dedupe: List[Tuple[Optional[Tuple[str, ...]], int]] = field(default_factory=list)
    group_by: List[Tuple[Tuple[str, ...], int]] = field(default_factory=list)
    windows: List[Dict[str, Any]] = field(default_factory=list)
    preds: List[Tuple[str, str, tuple, int]] = field(default_factory=list)  # lhs, op, rhs, line
    conf: List[Tuple[str, Any, int]] = field(default_factory=list)


_TYPE_NAMES = {"long", "int", "integer", "double", "float", "string", "boolean", "bigint", "date", "timestamp",
               "short", "byte", "decimal", "binary", "utf-8", "r", "w"}


def _sql_like(s: str) -> bool:
    """Whole-expression strings (parsed separately), format templates and type names are not data literals."""
    return bool(re.search(r"[()]|->|%s|\{\}|\s{2,}", s)) or s.lower() in _TYPE_NAMES


def _norm_num(x) -> Optional[float]:
    return float(x) if isinstance(x, (int, float)) and not isinstance(x, bool) else None


def _lhs_from_sqlglot(node) -> Optional[str]:
    if isinstance(node, (exp.Column, exp.Func, exp.Bracket, exp.Dot)):
        return re.sub(r"\s+", "", node.sql(dialect="spark")).lower()
    return None


def _lit_value(node):
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        try:
            return float(node.this)
        except ValueError:
            return None
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal) and not node.this.is_string:
        return -float(node.this.this)
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    return None


_OPS = {exp.GT: ">", exp.GTE: ">=", exp.LT: "<", exp.LTE: "<=", exp.EQ: "==", exp.NEQ: "!="}
_FLIP = {">": "<", ">=": "<=", "<": ">", "<=": ">=", "==": "==", "!=": "!="}


def predicates_from_tree(tree) -> List[Tuple[str, str, tuple]]:
    out = []
    for n in tree.find_all(exp.Binary, exp.Between, exp.In):
        if isinstance(n, exp.Between):
            lhs, lo, hi = _lhs_from_sqlglot(n.this), _lit_value(n.args.get("low")), _lit_value(n.args.get("high"))
            if lhs and lo is not None and hi is not None:
                out.append((lhs, "between", (lo, hi)))
        elif isinstance(n, exp.In):
            vals = [_lit_value(e) for e in n.expressions]
            lhs = _lhs_from_sqlglot(n.this)
            if lhs and vals and all(v is not None for v in vals):
                out.append((lhs, "in", tuple(sorted(vals, key=str))))
        elif type(n) in _OPS:
            op = _OPS[type(n)]
            l, r = n.left, n.right
            if _lit_value(r) is not None and _lhs_from_sqlglot(l):
                out.append((_lhs_from_sqlglot(l), op, (_lit_value(r),)))
            elif _lit_value(l) is not None and _lhs_from_sqlglot(r):
                out.append((_lhs_from_sqlglot(r), _FLIP[op], (_lit_value(l),)))
    return out


def _expr_text_facts(f: Facts, text: str, line: int, dialect: str = "spark") -> None:
    """Literals / functions / predicates inside a SQL expression string."""
    for m in _STR_RE.finditer(text):
        f.strs.append((m.group(1), line))
    stripped = _STR_RE.sub("''", text)
    for m in _NUM_RE.finditer(stripped):
        f.nums.append((float(m.group(0)), line))
    for m in _FUNC_RE.finditer(stripped):
        if m.group(1).lower() not in _NOT_FUNCS:
            f.funcs.append((m.group(1).lower(), line))
    for kw, tok in (("case", "case"), ("between", "between"), (" in ", "in"), ("is not null", "is not null"),
                    ("is null", "is null")):
        if re.search(rf"\b{kw.strip()}\b" if kw != " in " else r"\bin\s*\(", text, re.I):
            f.funcs.append((tok, line))
    try:
        for lhs, op, rhs in predicates_from_tree(sqlglot.parse_one(re.sub(r":(\w+)", r"'__p_\1__'", text), read=dialect)):
            f.preds.append((lhs, op, rhs, line))
    except Exception:
        pass


# ---- python source -------------------------------------------------------------------------
def _call_name(node: ast.Call) -> Optional[str]:
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    if isinstance(node.func, ast.Name):
        return node.func.id
    return None


def _const(node):
    """Constant-fold numbers/strings; returns _NOTHING if not a pure constant."""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        v = _const(node.operand)
        return _NOTHING if v is _NOTHING or not isinstance(v, (int, float)) else (-v if isinstance(node.op, ast.USub) else v)
    if isinstance(node, ast.BinOp):
        a, b = _const(node.left), _const(node.right)
        if a is _NOTHING or b is _NOTHING or not all(isinstance(x, (int, float)) for x in (a, b)):
            return _NOTHING
        try:
            return {ast.Add: a + b, ast.Sub: a - b, ast.Mult: a * b, ast.Div: a / b if b else _NOTHING,
                    ast.FloorDiv: a // b if b else _NOTHING}.get(type(node.op), _NOTHING)
        except Exception:
            return _NOTHING
    return _NOTHING


def _chain(node) -> List[ast.Call]:
    """All Call nodes down a method chain: a.b(x).c(y) -> [c(y), b(x)]."""
    calls = []
    while True:
        if isinstance(node, ast.Call):
            calls.append(node)
            node = node.func.value if isinstance(node.func, ast.Attribute) else None
        elif isinstance(node, ast.Attribute):
            node = node.value
        else:
            break
        if node is None:
            break
    return calls


def _chain_attrs(node) -> Set[str]:
    attrs = set()
    while node is not None:
        if isinstance(node, ast.Attribute):
            attrs.add(node.attr)
            node = node.value
        elif isinstance(node, ast.Call):
            node = node.func
        else:
            break
    return attrs


def _col_name(node) -> Optional[str]:
    """F.col("x") / "x" / F.col("x").alias("y") -> 'x' (alias -> 'y' handled by caller)."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Call) and _call_name(node) in ("col", "column") and node.args:
        return _const(node.args[0]) if isinstance(_const(node.args[0]), str) else None
    return None


def _lhs_py(node) -> Optional[str]:
    n = _col_name(node)
    if n:
        return n.lower()
    if isinstance(node, ast.Call) and _call_name(node) in ("size", "length", "abs", "lower", "upper", "trim") and node.args:
        inner = _lhs_py(node.args[0])
        return f"{_call_name(node)}({inner})" if inner else None
    if isinstance(node, ast.Subscript):
        base, key = _lhs_py(node.value), _const(node.slice)
        return f"{base}['{key}']" if base and isinstance(key, str) else None
    if isinstance(node, ast.Call) and _call_name(node) == "getItem" and isinstance(node.func, ast.Attribute) and node.args:
        base, key = _lhs_py(node.func.value), _const(node.args[0])
        return f"{base}['{key}']" if base and isinstance(key, str) else None
    return None


class _PyVisitor(ast.NodeVisitor):
    def __init__(self):
        self.f = Facts()
        self.consts: Dict[str, str] = {}

    def _str(self, node) -> Optional[str]:
        v = _const(node)
        if isinstance(v, str):
            return v
        if isinstance(node, ast.Name):
            return self.consts.get(node.id)
        if isinstance(node, ast.JoinedStr):
            return "".join(str(self.consts.get(getattr(p.value, "id", ""), "{?}")) if isinstance(p, ast.FormattedValue)
                           else str(p.value) for p in node.values)
        return None

    def visit_Assign(self, node):
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Constant) \
                and isinstance(node.value.value, str):
            self.consts[node.targets[0].id] = node.value.value
        self.generic_visit(node)

    def visit_Expr(self, node):  # skip bare docstring-like strings
        if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            return
        self.generic_visit(node)

    def _record_const(self, node) -> bool:
        v = _const(node)
        if v is _NOTHING:
            return False
        if isinstance(v, bool):
            return True
        if isinstance(v, (int, float)):
            self.f.nums.append((float(v), node.lineno))
        elif isinstance(v, str) and not _sql_like(v):
            self.f.strs.append((v, node.lineno))
        return True

    def visit_BinOp(self, node):
        if not self._record_const(node):
            self.generic_visit(node)

    def visit_UnaryOp(self, node):
        if not self._record_const(node):
            self.generic_visit(node)

    def visit_Constant(self, node):
        self._record_const(node)

    def visit_JoinedStr(self, node):
        pass  # f-strings carry table names / formatting, handled where used

    def visit_Compare(self, node):
        if len(node.ops) == 1:
            op = {ast.Gt: ">", ast.GtE: ">=", ast.Lt: "<", ast.LtE: "<=", ast.Eq: "==", ast.NotEq: "!="}.get(type(node.ops[0]))
            l, r = node.left, node.comparators[0]
            if op:
                lv, rv = _const(l), _const(r)
                if rv is not _NOTHING and _lhs_py(l) and isinstance(rv, (int, float, str)):
                    self.f.preds.append((_lhs_py(l), op, (float(rv) if isinstance(rv, (int, float)) else rv,), node.lineno))
                elif lv is not _NOTHING and _lhs_py(r) and isinstance(lv, (int, float, str)):
                    self.f.preds.append((_lhs_py(r), _FLIP[op], (float(lv) if isinstance(lv, (int, float)) else lv,), node.lineno))
        self.generic_visit(node)

    def visit_Call(self, node):
        name, line, a = _call_name(node), node.lineno, node.args
        kw = {k.arg: k.value for k in node.keywords if k.arg}
        low = (name or "").lower()
        is_f = isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and node.func.value.id in ("F", "functions")
        if is_f or low in ("isin", "between", "isnotnull", "isnull", "getitem", "cast", "otherwise", "alias"):
            if low in _SIGNIFICANT or low in _FUNC_TOKEN:
                self.f.funcs.append((low, line))
        if name == "table" and a and self._str(a[0]):
            self.f.reads.append(("table", self._str(a[0]), line))
        elif name in ("parquet", "orc", "csv", "json", "load") and a and "read" in _chain_attrs(node.func) and self._str(a[0]):
            self.f.reads.append(("path", self._str(a[0]), line))
        elif name in ("insertInto", "saveAsTable") and a and self._str(a[0]):
            mode = next((self._str(c.args[0]) for c in _chain(node) if _call_name(c) == "mode" and c.args), None)
            self.f.writes.append((self._str(a[0]), mode or "", line))
        elif name == "withColumn" and a and isinstance(_const(a[0]), str):
            self.f.created.append((_const(a[0]), line))
        elif name == "alias" and a and isinstance(_const(a[0]), str):
            self.f.created.append((_const(a[0]), line))
        elif name == "withColumnRenamed" and len(a) > 1 and isinstance(_const(a[1]), str):
            self.f.created.append((_const(a[1]), line))
        elif name == "join" and len(a) >= 1:
            how_node = a[2] if len(a) > 2 else kw.get("how")
            how = _const(how_node) if how_node is not None else "inner"
            on = a[1] if len(a) > 1 else kw.get("on")
            keys = []
            if isinstance(on, ast.Constant) and isinstance(on.value, str):
                keys = [on.value]
            elif isinstance(on, (ast.List, ast.Tuple)):
                keys = [e.value for e in on.elts if isinstance(e, ast.Constant)]
            elif on is not None:
                keys = sorted({c for n in ast.walk(on) for c in [_col_name(n)] if c})
            self.f.joins.append((_JOIN_NORM.get(str(how).lower(), str(how).lower()) if isinstance(how, str) else "?", keys, line))
        elif name in ("dropDuplicates", "drop_duplicates"):
            keys = None
            if a and isinstance(a[0], (ast.List, ast.Tuple)):
                keys = tuple(e.value for e in a[0].elts if isinstance(e, ast.Constant))
            elif a:
                keys = tuple(_const(x) for x in a if isinstance(_const(x), str))
            self.f.dedupe.append((keys, line))
        elif name in ("groupBy", "groupby") and a:
            names = []
            for x in a:
                al = next((c for c in _chain(x) if _call_name(c) == "alias" and c.args), None) if isinstance(x, ast.Call) else None
                names.append(_const(al.args[0]) if al else (_col_name(x) or "").split(".")[-1])
            self.f.group_by.append((tuple(n for n in names if n), line))
        elif name in ("rangeBetween", "rowsBetween"):
            self._window(node)
        elif name == "set" and len(a) == 2 and "conf" in _chain_attrs(node.func) and isinstance(_const(a[0]), str):
            self.f.conf.append((_const(a[0]), _const(a[1]), line))
        elif name == "config" and len(a) == 2 and isinstance(_const(a[0]), str):
            self.f.conf.append((_const(a[0]), _const(a[1]), line))
        elif name in ("expr", "selectExpr", "filter", "where") and a and isinstance(_const(a[0]), str):
            _expr_text_facts(self.f, _const(a[0]), line)
        self.generic_visit(node)

    def _window(self, node):
        chain = _chain(node)
        part = [c for c in chain if _call_name(c) == "partitionBy"]
        order = [c for c in chain if _call_name(c) == "orderBy"]
        lo, hi = (_const(node.args[0]), _const(node.args[1])) if len(node.args) >= 2 else (_NOTHING, _NOTHING)
        if lo is _NOTHING or hi is _NOTHING:
            return
        self.f.windows.append({
            "type": "range" if _call_name(node) == "rangeBetween" else "rows", "lo": lo, "hi": hi, "line": node.lineno,
            "partition": sorted((_col_name(x) or "").lower() for c in part for x in c.args if _col_name(x)),
            "order": [ast.unparse(x) for c in order for x in c.args]})


def python_facts(source: str) -> Facts:
    v = _PyVisitor()
    v.visit(ast.parse(source))
    return v.f


# ---- HiveQL source -------------------------------------------------------------------------
def sql_facts(source: str, dialect: str = "hive") -> Facts:
    f = Facts()
    text = re.sub(r"\$\{hivevar:(\w+)\}", r":\1", source)
    for stmt in sqlglot.parse(text, read=dialect):
        if stmt is None:
            continue
        line = 1
        for t in stmt.find_all(exp.Table):
            if t.name:
                f.reads.append(("table", ".".join(p for p in (t.db, t.name) if p), line))
        if isinstance(stmt, exp.Insert) and stmt.this is not None:
            tbl = stmt.this.find(exp.Table)
            if tbl is not None:
                f.writes.append((".".join(p for p in (tbl.db, tbl.name) if p), "overwrite" if stmt.args.get("overwrite") else "append", line))
        for j in stmt.find_all(exp.Join):
            side = (j.side or "").lower()
            f.joins.append((_JOIN_NORM.get(side or (j.kind or "inner").lower(), side or "inner"), [], line))
        for lit in stmt.find_all(exp.Literal):
            if lit.is_string:
                f.strs.append((lit.this, line))
            else:
                f.nums.append((float(lit.this), line))
        for fn in stmt.find_all(exp.Func):
            f.funcs.append((re.sub(r"\W", "", fn.sql_name()).lower(), line))
        for m in _FUNC_RE.finditer(stmt.sql(dialect=dialect)):
            if m.group(1).lower() not in _NOT_FUNCS:
                f.funcs.append((m.group(1).lower(), line))
        for lhs, op, rhs in predicates_from_tree(stmt):
            f.preds.append((lhs, op, rhs, line))
        for a in stmt.find_all(exp.Alias):
            f.created.append((a.alias, line))
        for w in stmt.find_all(exp.Window):
            spec = w.args.get("spec")
            if spec is not None:
                f.windows.append({"type": (spec.args.get("kind") or "range").lower(), "lo": None, "hi": None, "line": line,
                                  "partition": sorted(p.name.lower() for p in w.args.get("partition_by") or [] if isinstance(p, exp.Column)),
                                  "order": [], "spec_sql": spec.sql(dialect=dialect)})
    return f


# ---- YAML side ------------------------------------------------------------------------------
_EXPR_KEYS = {"expr", "filter", "pre_filter", "frame", "condition", "when", "post_filter", "sql", "on"}


def _walk(node_id: str, obj, out: List[Tuple[str, str]], key: str = "") -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if key == "agg" and isinstance(v, str):  # {output_name: expression}
                out.append((node_id, v))
            else:
                _walk(node_id, v, out, k)
    elif isinstance(obj, list):
        for v in obj:
            _walk(node_id, v, out, key)
    elif isinstance(obj, str) and (key in _EXPR_KEYS or key in ("order_by", "partition_by", "group_by", "agg")):
        out.append((node_id, obj))


def yaml_facts(bp: PipelineBlueprint) -> Tuple[Facts, Dict[str, Set[str]]]:
    """Facts of the YAML expressed in the same shape as source facts, plus literal -> nodes index."""
    f, index = Facts(), {}
    texts: List[Tuple[str, str]] = []
    for s in bp.sources:
        _walk(s.id, s.model_dump(by_alias=True, exclude_none=True), texts)
        f.reads.append(("table" if s.object else "path", s.object or s.path or "", 0))
        for fld in s.schema_:
            if fld.rename_to:
                f.created.append((fld.rename_to, 0))
    for t in bp.transformations:
        d = t.model_dump(by_alias=True, exclude_none=True)
        _walk(t.id, d, texts)
        for c in t.columns or []:
            if isinstance(c, dict) and "name" in c:
                f.created.append((c["name"], 0))
        for a in t.aggregations or []:
            if isinstance(a, dict) and "name" in a:
                f.created.append((a["name"], 0))
        for g in t.group_by or []:
            m = re.search(r"\bas\s+(\w+)\s*$", g, re.I)
            f.created.append((m.group(1) if m else g.split(".")[-1], 0))
        ex = d.get("explode")
        if isinstance(ex, dict):
            f.funcs.append(("explode", 0))
            if ex.get("alias"):
                f.created.append((ex["alias"], 0))
        if d.get("output_column"):
            f.created.append((d["output_column"], 0))
        if isinstance(d.get("output"), dict) and d["output"].get("name"):
            f.created.append((d["output"]["name"], 0))
        for s in d.get("steps") or []:
            if isinstance(s, dict):
                for k in (s.get("agg") or {}):
                    f.created.append((k, 0))
                if s.get("group_by"):
                    f.group_by.append((tuple(re.sub(r".*\bas\s+", "", g, flags=re.I).split(".")[-1] for g in s["group_by"]), 0))
        for es in d.get("element_schema") or []:
            if isinstance(es, dict) and "name" in es:
                f.created.append((es["name"], 0))
        for r in d.get("rules") or []:
            if isinstance(r, dict):
                f.created += [(k, 0) for k in r if k not in ("when", "condition")]
        if t.dedupe_keys:
            f.dedupe.append((tuple(t.dedupe_keys), 0))
            f.funcs.append(("dedupe", 0))
        if t.group_by:
            f.group_by.append((tuple(re.sub(r".*\bas\s+", "", g, flags=re.I).split(".")[-1] for g in t.group_by), 0))
        for j in t.joins or []:
            if isinstance(j, dict):
                on = j.get("on")
                keys = on if isinstance(on, list) else sorted(set(re.findall(r"[A-Za-z_]\w*", str(on)))) if on else []
                f.joins.append((_JOIN_NORM.get(str(j.get("type", "inner")).lower(), "?"), keys, 0))
                if j.get("hint"):
                    f.funcs.append(("broadcast", 0))
        if t.window:
            w = t.window
            m = re.search(r"(RANGE|ROWS)\s+BETWEEN\s+(UNBOUNDED|\d+)\s+PRECEDING\s+AND\s+(CURRENT\s+ROW|\d+\s+FOLLOWING)", str(w.get("frame", "")), re.I)
            f.windows.append({"type": m.group(1).lower() if m else None,
                              "lo": -float(m.group(2)) if m and m.group(2).isdigit() else None,
                              "hi": 0.0 if m and "CURRENT" in m.group(3).upper() else None, "line": 0,
                              "partition": sorted(str(p).lower() for p in w.get("partition_by") or []),
                              "order": list(w.get("order_by") or []), "node": t.id})
        for r in d.get("rules") or []:
            if isinstance(r, dict):
                for k, v in r.items():
                    if k in ("when", "condition"):
                        texts.append((t.id, str(v)))
                    elif isinstance(v, str):
                        f.strs.append((v, 0)); index.setdefault(v, set()).add(t.id)
                    elif isinstance(v, (int, float)):
                        f.nums.append((float(v), 0)); index.setdefault(float(v), set()).add(t.id)
                f.funcs += [("case", 0), ("struct", 0), ("array", 0)]
    for tg in bp.targets:
        f.writes.append((tg.object or tg.path or "", tg.mode or "", 0))
    for node_id, text in texts:
        before = (len(f.nums), len(f.strs))
        _expr_text_facts(f, text, 0)
        for n, _ in f.nums[before[0]:]:
            index.setdefault(n, set()).add(node_id)
        for s, _ in f.strs[before[1]:]:
            index.setdefault(s, set()).add(node_id)
    return f, index


# ---- comparison -----------------------------------------------------------------------------
def _blob(bp: PipelineBlueprint) -> str:
    """Lower-cased YAML text used for "is X mentioned" checks. `post_filter` is excluded: the runtime never
    executes it (reported separately), so names that only appear there are not real references."""
    d = bp.model_dump(by_alias=True, exclude_none=True)
    for t in d.get("transformations", []):
        t.pop("post_filter", None)
    return json.dumps(d, default=str).lower()


def _last(name: str) -> str:
    return name.replace("`", "").split(".")[-1].lower()


def check_coverage(bp: PipelineBlueprint, source_text: str, source_path: str, lang: str):
    """-> (findings, coverage_matrix_rows)"""
    src = sql_facts(source_text) if lang == "hiveql" else python_facts(source_text)
    yf, yidx = yaml_facts(bp)
    blob = _blob(bp)
    loc = lambda line: f"{source_path}:{line}" if line else source_path
    findings: List[Finding] = []
    rows: List[Dict[str, Any]] = []

    def row(construct, ok, detail=""):
        rows.append({"construct": construct, "status": "covered" if ok else "MISSING", "detail": detail})

    def miss(sev, msg, line, node=None, res="", conf="high", cat="coverage"):
        findings.append(Finding(cat, sev, msg, node=node, source_location=loc(line), confidence=conf,
                                impact="Behaviour of the source may not be preserved by the conversion.", resolution=res))

    # reads / writes
    y_reads = {_last(n) for _, n, _ in yf.reads if n}
    y_paths = {n.rstrip("/").lower() for _, n, _ in yf.reads if n}
    for kind, name, line in sorted(set(src.reads), key=lambda r: r[2]):
        ok = (_last(name) in y_reads) if kind == "table" else (name.rstrip("/").lower() in y_paths or _last(name.rstrip("/")) in y_reads)
        row(f"read {kind} {name}", ok)
        if not ok and "{" not in name:
            miss("error", f"source reads {kind} '{name}' but the YAML has no matching source", line,
                 res="Add a source (explicit table/path mapping).")
    y_writes = {_last(n) for n, _, _ in yf.writes if n}
    for name, mode, line in src.writes:
        ok = _last(name) in y_writes
        row(f"write {name} (mode={mode or 'n/a'})", ok)
        if not ok:
            miss("error", f"source writes '{name}' but the YAML has no matching target", line, res="Add a target.")
        elif mode:
            ymodes = {m for n, m, _ in yf.writes if _last(n) == _last(name)}
            if ymodes and mode.lower() not in " ".join(ymodes).lower() and mode.lower() != "append":
                miss("warning", f"source write mode '{mode}' vs YAML target mode {sorted(ymodes)} for '{name}'", line,
                     conf="medium", res="Confirm the overwrite scope (partition-dynamic vs full).")

    # created columns
    for name, line in sorted(set(src.created), key=lambda r: r[1]):
        ok = name.lower() in {c.lower() for c, _ in yf.created}
        mentioned = re.search(rf"\b{re.escape(name.lower())}\b", blob) is not None
        row(f"column created: {name}", ok, "" if ok else ("referenced in the YAML but never defined" if mentioned else ""))
        if not ok and mentioned:
            miss("error", f"the YAML references column '{name}' but no node defines it; the source creates it "
                          f"(a dropped rename/derivation)", line,
                 res="Define the column (e.g. rename_to / a derive step) exactly as the source does.")
        elif not ok:
            miss("warning", f"column '{name}' is created in the source but never appears in the YAML", line, conf="medium",
                 res="Add it, or confirm it is an intentionally dropped intermediate.")

    # numeric constants (source -> YAML), with YAML -> source in the reverse direction
    ynums = {round(abs(n), 9) for n, _ in yf.nums}
    snums = {round(abs(n), 9) for n, _ in src.nums}
    for n, line in sorted({(abs(n), l) for n, l in src.nums if abs(n) not in _TRIVIAL_NUMS}, key=lambda x: x[1]):
        ok = round(n, 9) in ynums
        row(f"constant {n:g}", ok)
        if not ok:
            miss("error", f"constant {n:g} used in the source does not appear in the YAML", line,
                 res="Restore the threshold/constant, or document why it was removed.")
    for n in sorted(ynums - snums - {abs(x) for x in _TRIVIAL_NUMS}):
        nodes = sorted(yidx.get(n, yidx.get(float(n), set())) or yidx.get(-n, set()))
        findings.append(Finding("coverage", "warning", f"constant {n:g} in the YAML has no basis in the source",
                                node=nodes[0] if nodes else None, confidence="medium", origin="deterministic",
                                impact="Possible invented logic.", resolution="Trace it to the source or remove it."))
    src_text_l = source_text.lower()
    for s, line in sorted(set(src.strs), key=lambda r: r[1]):
        if not s or len(s) < 2:
            continue
        ok = s.lower() in blob or s in {c for c, _ in src.created}  # created names are reported separately
        if not ok:
            row(f"string literal '{s}'", False)
            miss("warning", f"string literal '{s}' from the source does not appear in the YAML", line, conf="medium",
                 res="Check whether a filter value / label was dropped.")
    for s in sorted({s for s, _ in yf.strs if s}):
        if s.lower() not in src_text_l and not s.startswith("__p_"):
            findings.append(Finding("coverage", "warning", f"string literal '{s}' in the YAML does not occur in the source",
                                    node=(sorted(yidx.get(s, [])) or [None])[0], confidence="medium",
                                    impact="Possible invented logic.", resolution="Trace it to the source or remove it."))

    # comparison predicates (column, operator, threshold)
    ypreds = {(l, o, r) for l, o, r, _ in yf.preds}
    for lhs, op, rhs, line in sorted(set(src.preds), key=lambda p: p[3]):
        key = (lhs, op, rhs)
        if key in ypreds:
            row(f"predicate {lhs} {op} {rhs[0] if len(rhs) == 1 else rhs}", True)
            continue
        same_rhs = [p for p in ypreds if p[0] == lhs and p[2] == rhs]
        same_op = [p for p in ypreds if p[0] == lhs and p[1] == op]
        if same_rhs:
            row(f"predicate {lhs} {op} {rhs}", False, f"YAML uses operator {sorted({p[1] for p in same_rhs})}")
            miss("error", f"comparison operator changed for '{lhs}' against {rhs[0] if len(rhs) == 1 else rhs}: source '{op}', "
                          f"YAML {sorted({p[1] for p in same_rhs})}", line, res="Fix the operator (boundary values behave differently).")
        elif same_op:
            row(f"predicate {lhs} {op} {rhs}", False, f"YAML thresholds {[p[2] for p in same_op]}")
            miss("error", f"threshold changed for '{lhs} {op}': source {rhs}, YAML {[p[2] for p in same_op]}", line,
                 res="Restore the threshold.")
        else:
            row(f"predicate {lhs} {op} {rhs}", False)
            miss("warning", f"comparison '{lhs} {op} {rhs}' from the source was not found in the YAML in this form", line,
                 conf="medium", res="Confirm the condition is implemented (possibly under a different expression).")

    # functions
    yfuncs = {n for n, _ in yf.funcs}
    ytext = " ".join(yfuncs)
    for name, line in sorted({(n, l) for n, l in src.funcs}, key=lambda x: x[1]):
        tok = _FUNC_TOKEN.get(name, name)
        if tok is None or name not in _SIGNIFICANT and name not in _FUNC_TOKEN:
            continue
        ok = tok in yfuncs or (tok == "dedupe" and "dedupe" in yfuncs) or (tok == "avg" and "mean" in yfuncs)
        if tok in ("in", "between", "case", "cast", "is null", "is not null", "distinct") and not ok:
            ok = re.search(rf"\b{re.escape(tok)}\b", blob) is not None
        if not ok and name in ("filter", "where"):
            ok = True
        row(f"function {name}", ok)
        if not ok:
            miss("warning", f"function/operator '{name}' used in the source is not used anywhere in the YAML", line, conf="medium",
                 res="Check the logic that depended on it is represented.")

    # joins
    sj, yj = Counter(h for h, _, _ in src.joins), Counter(h for h, _, _ in yf.joins)
    if lang != "hiveql" or src.joins:
        ok = sj == yj
        row(f"joins ({sum(sj.values())}: {dict(sj)})", ok, f"YAML has {dict(yj)}" if not ok else "")
        if not ok:
            miss("error", f"join structure differs: source {dict(sj)} vs YAML {dict(yj)}", (src.joins[0][2] if src.joins else 0),
                 res="Every join and its type (inner/left/...) must be preserved.")
    for how, keys, line in src.joins:
        if keys and not any(set(map(str.lower, keys)) <= {k.lower() for k in yk} or {k.lower() for k in yk} <= set(map(str.lower, keys))
                            for h, yk, _ in yf.joins):
            miss("warning", f"join keys {keys} from the source were not found in any YAML join", line, conf="medium",
                 res="Confirm the join condition.")

    # dedupe / group by / windows
    for keys, line in src.dedupe:
        ok = any((keys is not None and set(k.lower() for k in keys) == set(k.lower() for k in yk)) for yk, _ in yf.dedupe) \
            or (keys is None and bool(yf.dedupe))
        row(f"dedupe on {list(keys) if keys else 'all columns'}", ok)
        if not ok:
            miss("error", f"dropDuplicates({list(keys) if keys else ''}) in the source has no matching dedupe_keys in the YAML", line,
                 res="Preserve the deduplication (and its key set).")
    ygroups = [set(g) for g, _ in yf.group_by]
    for keys, line in src.group_by:
        ok = any(set(k.lower() for k in keys) <= {x.lower() for x in g} for g in ygroups)
        row(f"group by {list(keys)}", ok)
        if not ok:
            miss("warning", f"groupBy{list(keys)} in the source has no YAML group_by covering those keys", line, conf="medium")
    for w in src.windows:
        yw = [x for x in yf.windows if x["partition"] == w["partition"]]
        ok = bool(yw) and (w.get("lo") is None or any(x["lo"] == w["lo"] and x["hi"] == w["hi"] and x["type"] == w["type"] for x in yw))
        row(f"window {w['type']} partition={w['partition']} frame=({w.get('lo')},{w.get('hi')})", ok,
            "" if ok else f"YAML windows: {[(x['partition'], x['type'], x['lo'], x['hi']) for x in yf.windows]}")
        if not ok:
            miss("error", f"window definition differs: source {w['type']} frame ({w.get('lo')}, {w.get('hi')}) partition {w['partition']}; "
                          f"YAML {[(x['partition'], x['type'], x['lo'], x['hi']) for x in yf.windows] or 'none'}", w["line"],
                 res="Window partition, type and frame all change results.")

    # spark config
    yconf = {str(k).lower() for k in (bp.job.spark_conf if bp.job else {})} | {str(k).lower() for k in bp.global_configs}
    for k, v, line in src.conf:
        if any(t in k.lower() for t in _INFRA_CONF):
            continue
        ok = k.lower() in yconf
        row(f"spark conf {k}={v}", ok)
        if not ok:
            miss("warning", f"Spark setting {k}={v} from the source is not in the YAML spark_conf", line, conf="medium",
                 res="Behaviour-relevant settings (e.g. partition overwrite mode) must be carried over.")
    return findings, rows
