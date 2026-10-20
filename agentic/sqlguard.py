"""Write-protection for cluster-mode validation. Pure Python (no Spark import) so it is unit-tested.

Rules: a generated script may READ real tables, but every write/DDL must target the scratch
database. Table names are first rewritten (read_map: real source names; write_map: the script's
target -> scratch table), then every statement is checked.

This is a best-effort guard against mistakes and LLM-written code, NOT a security boundary:
for real isolation also run with a Hive/Ranger user that has read-only access to the real
databases and write access only to the scratch prefix.
"""
import ast
import re
from typing import Dict, List

_IDENT = r"`?[\w$]+`?(?:\.`?[\w$]+`?)?"
_READ_OK = {"SELECT", "WITH", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "SET", "USE", "VALUES",
            "CACHE", "UNCACHE", "REFRESH", "FROM", "TABLE"}
# verb -> regex capturing the object being modified
_WRITE_PATTERNS = [
    ("INSERT", re.compile(rf"^INSERT\s+(?:OVERWRITE|INTO)\s+(?:TABLE\s+)?({_IDENT})", re.I)),
    ("CREATE", re.compile(rf"^CREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?({_IDENT})", re.I)),
    ("CREATE_DB", re.compile(rf"^CREATE\s+(?:DATABASE|SCHEMA)\s+(?:IF\s+NOT\s+EXISTS\s+)?({_IDENT})", re.I)),
    ("DROP", re.compile(rf"^DROP\s+(?:TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?({_IDENT})", re.I)),
    ("DROP_DB", re.compile(rf"^DROP\s+(?:DATABASE|SCHEMA)\s+(?:IF\s+EXISTS\s+)?({_IDENT})", re.I)),
    ("ALTER", re.compile(rf"^ALTER\s+(?:TABLE|VIEW)\s+({_IDENT})", re.I)),
    ("TRUNCATE", re.compile(rf"^TRUNCATE\s+TABLE\s+({_IDENT})", re.I)),
    ("MERGE", re.compile(rf"^MERGE\s+INTO\s+({_IDENT})", re.I)),
    ("UPDATE", re.compile(rf"^UPDATE\s+({_IDENT})", re.I)),
    ("DELETE", re.compile(rf"^DELETE\s+FROM\s+({_IDENT})", re.I)),
    ("LOAD", re.compile(rf"^LOAD\s+DATA\s+.*?INTO\s+TABLE\s+({_IDENT})", re.I | re.S)),
    ("MSCK", re.compile(rf"^MSCK\s+REPAIR\s+TABLE\s+({_IDENT})", re.I)),
    ("ANALYZE", re.compile(rf"^ANALYZE\s+TABLE\s+({_IDENT})", re.I)),
]
_TEMP = re.compile(r"^CREATE\s+(?:OR\s+REPLACE\s+)?(?:GLOBAL\s+)?TEMP(?:ORARY)?\s+VIEW", re.I)


def rewrite_names(sql: str, mapping: Dict[str, str]) -> str:
    """Replace table names (case-insensitive, whole identifiers only) per mapping."""
    for old, new in sorted(mapping.items(), key=lambda kv: -len(kv[0])):
        if "/" in old or ":" in old:  # paths are not SQL identifiers
            continue
        sql = re.sub(rf"(?<![\w.`]){re.escape(old)}(?![\w`])", new, sql, flags=re.I)
    return sql


def _clean(name: str) -> str:
    return name.replace("`", "").lower()


def assert_scratch_table(name: str, scratch_db: str, what: str = "write") -> None:
    n = _clean(name)
    if "." not in n or n.split(".", 1)[0] != scratch_db.lower():
        raise PermissionError(
            f"BLOCKED {what} to '{name}': cluster-mode validation may only write inside the scratch "
            f"database '{scratch_db}'. Write to the blueprint target table (it is redirected there "
            f"automatically) and use fully qualified names.")


def strip_comments(sql: str) -> str:
    """Remove `-- ...` and `/* ... */` comments that are not inside a quoted string. Comments must go BEFORE
    statement splitting: an apostrophe in a comment ("-- it's") would otherwise look like an open quote and hide
    every following ';' (and so a following DROP) inside one harmless-looking statement."""
    out, i, n, quote = [], 0, len(sql), None
    while i < n:
        ch = sql[i]
        if quote:
            out.append(ch)
            if ch == quote:
                quote = None
            i += 1
        elif ch in ("'", '"', "`"):
            quote = ch
            out.append(ch)
            i += 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j == -1 else j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j == -1 else j + 2
            out.append(" ")
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def split_statements(sql: str) -> List[str]:
    """Split on ';' that is not inside a quoted string or backticks (comments are removed first)."""
    sql = strip_comments(sql)
    out, cur, quote = [], [], None
    for ch in sql:
        if quote:
            if ch == quote:
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
        elif ch == ";":
            out.append("".join(cur))
            cur = []
            continue
        cur.append(ch)
    out.append("".join(cur))
    return out


def check_sql(sql: str, scratch_db: str) -> None:
    """Raise PermissionError unless every statement in `sql` is read-only or scratch-only."""
    for stmt in [s.strip() for s in split_statements(sql) if s.strip()]:
        stmt = re.sub(r"^(?:--[^\n]*\n|/\*.*?\*/|\s)+", "", stmt, flags=re.S)  # leading comments
        if not stmt:
            continue
        if re.match(r"^INSERT\s+OVERWRITE\s+(?:LOCAL\s+)?DIRECTORY", stmt, re.I):
            raise PermissionError("BLOCKED: INSERT OVERWRITE DIRECTORY writes outside the scratch database.")
        if _TEMP.match(stmt):
            continue  # session-scoped, touches no catalog object
        for verb, rx in _WRITE_PATTERNS:
            m = rx.match(stmt)
            if not m:
                continue
            target = _clean(m.group(1))
            if verb in ("CREATE_DB", "DROP_DB"):
                if target != scratch_db.lower():
                    raise PermissionError(f"BLOCKED {verb.lower().replace('_', ' ')} '{m.group(1)}': only the "
                                          f"scratch database '{scratch_db}' may be created/dropped.")
            else:
                assert_scratch_table(m.group(1), scratch_db, verb.lower())
            if verb == "CREATE" and re.search(r"\bLOCATION\b", stmt, re.I):
                raise PermissionError("BLOCKED: CREATE ... LOCATION could point at real data.")
            break
        else:
            first = re.match(r"\w+", stmt)
            if not first or first.group(0).upper() not in _READ_OK:
                raise PermissionError(f"BLOCKED statement starting with "
                                      f"'{first.group(0) if first else stmt[:20]}': not recognised as read-only.")


# ---- static scan of generated source ------------------------------------------------------
_BAD_IMPORTS = {"subprocess", "shutil", "socket", "requests", "urllib", "http", "ftplib", "paramiko",
                "psycopg2", "pyhive", "jaydebeapi", "pymysql", "hdfs", "pyodbc", "ctypes"}
_BAD_ATTRS = {"_jvm", "_jsc", "_jdf", "_jsparkSession", "system", "popen", "rmtree", "remove", "unlink"}


def scan_cluster_safety(code: str) -> List[str]:
    """Constructs the runtime guard cannot see (raw JVM/Hadoop/JDBC/shell access)."""
    problems = []
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return []  # syntax errors are reported by check_code
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name.split(".")[0] in _BAD_IMPORTS:
                    problems.append(f"line {node.lineno}: import of '{a.name}'")
        elif isinstance(node, ast.ImportFrom) and node.module and node.module.split(".")[0] in _BAD_IMPORTS:
            problems.append(f"line {node.lineno}: import from '{node.module}'")
        elif isinstance(node, ast.Attribute) and node.attr in _BAD_ATTRS:
            problems.append(f"line {node.lineno}: use of '.{node.attr}'")
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "jdbc":
            problems.append(f"line {node.lineno}: JDBC access ('.jdbc(')")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.strip().lower() == "jdbc":
            problems.append(f"line {node.lineno}: JDBC format")
    return problems
