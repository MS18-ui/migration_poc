"""Rule-based compatibility, security and unsupported-construct detection (PLAN.md section 4 / 6).

Source is parsed, never executed. Each rule yields a Finding with a source location; static analysis is
reported as evidence for review, never as proof that a workload is safe or equivalent."""
import ast
import re
from typing import List, Tuple

from .findings import Finding

_UNSUPPORTED = {
    "rdd": "RDD API", "udf": "Python UDF", "pandas_udf": "pandas UDF", "mappartitions": "mapPartitions",
    "foreach": "foreach", "foreachpartition": "foreachPartition", "readstream": "Structured Streaming read",
    "writestream": "Structured Streaming write", "foreachbatch": "foreachBatch", "parallelize": "sc.parallelize (RDD)",
    "hivecontext": "HiveContext (removed in Spark 3)", "sqlcontext": "SQLContext (use SparkSession)",
}
_EXPOSURE = {"show": "prints rows to the driver log", "collect": "collects all rows to the driver (unbounded)",
             "topandas": "collects all rows to the driver as pandas (unbounded)", "take": "pulls rows to the driver",
             "head": "pulls rows to the driver"}
_UNSAFE_CALLS = {"eval", "exec", "compile", "__import__"}
_UNSAFE_ATTRS = {"system", "popen", "check_output", "check_call", "loads", "load", "import_module"}
_DATE_FUNCS = {"to_date", "to_timestamp", "unix_timestamp", "from_unixtime", "date_format"}
_DEPRECATED = {"registertemptable": "registerTempTable is deprecated (use createOrReplaceTempView)",
               "unionall": "unionAll is deprecated (use union)"}
# \w* on both sides so compound names (DB_PASSWORD, client_secret_value, aws_access_key_id) are caught
_SECRET_RE = re.compile(r"(?i)\w*(?:pass(?:word|wd|phrase)|secret|token|api[_-]?key|access[_-]?key|private[_-]?key)\w*"
                        r"\s*[=:]\s*['\"][^'\"]{3,}['\"]")
_JDBC_CRED_RE = re.compile(r"(?i)jdbc:[a-z]+://[^'\"\s]*(?:password|pwd)=[^&'\"\s]+|://\w+:[^@/'\"\s]+@")
_MONEY_RE = re.compile(r"(?i)amount|price|balance|total|cost|fee|salary")


def redact_secrets(text: str) -> Tuple[str, int]:
    """Local scan before ANY model submission (PLAN.md section 4): mask credential-looking values."""
    n = 0
    def mask(m):
        nonlocal n
        n += 1
        return re.sub(r"""(['"])[^'"]{3,}\1""", '"<REDACTED>"', m.group(0))
    text = _SECRET_RE.sub(mask, text)
    def mask_url(m):
        nonlocal n
        n += 1
        return re.sub(r"((?:password|pwd)=)[^&'\"\s]+", r"\1<REDACTED>", re.sub(r"(://\w+:)[^@/'\"\s]+@", r"\1<REDACTED>@", m.group(0)), flags=re.I)
    return _JDBC_CRED_RE.sub(mask_url, text), n


def _name(n: ast.Call) -> str:
    return (n.func.attr if isinstance(n.func, ast.Attribute) else n.func.id if isinstance(n.func, ast.Name) else "") or ""


def _attrs(node) -> set:
    out = set()
    while node is not None:
        if isinstance(node, ast.Attribute):
            out.add(node.attr.lower())
            node = node.value
        elif isinstance(node, ast.Call):
            node = node.func
        else:
            break
    return out


def scan_python(source: str, path: str) -> List[Finding]:
    f: List[Finding] = []
    loc = lambda n: f"{path}:{getattr(n, 'lineno', 0)}"

    def add(cat, sev, msg, node, conf="high", res="", impact=""):
        f.append(Finding(cat, sev, msg, source_location=loc(node), confidence=conf, resolution=res, impact=impact))

    for i, line in enumerate(source.splitlines(), 1):
        if _SECRET_RE.search(line) or _JDBC_CRED_RE.search(line):
            f.append(Finding("security", "error", "possible embedded secret / credential in source",
                             source_location=f"{path}:{i}", confidence="medium",
                             resolution="Replace with a secret reference; never copy it into YAML or logs."))
    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        return f + [Finding("executability", "error", f"source is not valid Python 3 ({e.msg}); Python 2 syntax?",
                            source_location=f"{path}:{e.lineno}", resolution="Run 2to3 / fix syntax before analysis.")]
    dyn_overwrite = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            nm, low = _name(node), _name(node).lower()
            if nm in _UNSAFE_CALLS or (isinstance(node.func, ast.Attribute) and node.func.attr in _UNSAFE_ATTRS
                                       and isinstance(node.func.value, ast.Name)
                                       and node.func.value.id in ("os", "subprocess", "pickle", "importlib")):
                add("security", "error", f"unsafe execution: {nm}()", node, res="Remove or replace; it must not run in the framework.")
            if low in _UNSUPPORTED:
                add("executability", "error", f"unsupported construct: {_UNSUPPORTED[low]}", node,
                    res="Rewrite with DataFrame/SQL operations or exclude this job explicitly.",
                    impact="Cannot be converted to a deterministic operation.")
            if low in _EXPOSURE and low in ("show", "collect", "topandas") or (low in ("take", "head") and node.args):
                add("security", "warning", f"data exposure / resource risk: {nm}() {_EXPOSURE.get(low, '')}", node, conf="medium",
                    res="Remove from production code or bound and mask the output.")
            if low in _DEPRECATED:
                add("compat", "warning", _DEPRECATED[low], node, res="Use the Spark 3 API.")
            if low in _DATE_FUNCS and len(node.args) < 2 and not node.keywords:
                add("compat", "warning", f"{nm}() without an explicit format: Spark 3 parsing differs from Spark 2", node,
                    conf="medium", res="Set an explicit format and test boundary dates.")
            if low in ("dropduplicates", "drop_duplicates"):
                add("compat", "warning", "dropDuplicates keeps an arbitrary row among duplicates (non-deterministic)", node,
                    conf="medium", res="Define which duplicate wins (ordering / window row_number) or confirm duplicates are identical.")
            if low in ("first", "last") and isinstance(node.func, ast.Attribute):
                add("compat", "warning", f"{nm}() aggregate is non-deterministic without ordering", node, conf="medium",
                    res="Make the choice explicit (min/max or ordered window).")
            if low in ("collect_list", "collect_set"):
                add("compat", "info", f"{nm}() element order is not guaranteed", node, conf="medium",
                    res="Sort before comparing, or wrap with sort_array when order matters.")
            if low == "size":
                add("compat", "info", "size(null) semantics differ across Spark versions/ANSI mode (-1 vs NULL)", node, conf="medium",
                    res="Handle NULL arrays explicitly (coalesce) and test it.")
            if low == "crossjoin":
                add("security", "warning", "crossJoin can explode row counts (resource risk)", node, conf="medium")
            if low == "cast" and node.args and isinstance(node.args[0], ast.Constant) and str(node.args[0].value).lower() in ("double", "float"):
                try:
                    inner = ast.unparse(node.func.value)
                except Exception:
                    inner = ""
                if _MONEY_RE.search(inner):
                    add("compat", "warning", f"monetary value cast to floating point: {inner[:60]}", node, conf="medium",
                        res="Prefer DECIMAL for money; floating point changes cents.")
            if low in ("set", "config") and node.args and isinstance(node.args[0], ast.Constant):
                k = str(node.args[0].value)
                if k.startswith("spark.sql.legacy"):
                    add("compat", "warning", f"legacy compatibility flag {k}: understand the difference instead of enabling it globally", node)
                if k == "spark.sql.sources.partitionOverwriteMode" and len(node.args) > 1 and getattr(node.args[1], "value", "") == "dynamic":
                    dyn_overwrite = True
            if low == "mode" and node.args and isinstance(node.args[0], ast.Constant) and node.args[0].value == "overwrite":
                add("security", "info", "overwrite write mode: scope depends on partitionOverwriteMode and the target's partitioning", node,
                    conf="medium", res="Record an explicit overwrite predicate or approved full-table scope.")
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [a.name for a in node.names] + ([node.module] if isinstance(node, ast.ImportFrom) and node.module else [])
            for n in names:
                if n and n.split(".")[0] in ("subprocess", "pickle", "marshal", "ctypes"):
                    add("security", "warning", f"import of '{n}' (unsafe execution / deserialization surface)", node, conf="medium")
    if any(x.message.startswith("overwrite write mode") for x in f) and dyn_overwrite:
        for x in f:
            if x.message.startswith("overwrite write mode"):
                x.message += " (dynamic partition overwrite is configured: only partitions present in the data are replaced)"
    return f


def scan_sql_text(source: str, path: str) -> List[Finding]:
    f = []
    for i, line in enumerate(source.splitlines(), 1):
        if _SECRET_RE.search(line):
            f.append(Finding("security", "error", "possible embedded secret in SQL", source_location=f"{path}:{i}", confidence="medium"))
        if re.search(r"(?i)\b(drop\s+table|truncate\s+table|delete\s+from)\b", line):
            f.append(Finding("security", "warning", "destructive statement in source SQL", source_location=f"{path}:{i}", confidence="medium",
                             resolution="Keep it out of automated execution or scope it explicitly."))
        if re.search(r"(?i)\btransform\s*\(.*\busing\b", line) or re.search(r"(?i)\bADD\s+(JAR|FILE)\b", line):
            f.append(Finding("executability", "error", "Hive TRANSFORM / ADD JAR is not convertible", source_location=f"{path}:{i}"))
    return f
