"""Read-only guard for ad-hoc SQL typed into the UI.

Allowed: exactly ONE statement that is SELECT / WITH (query) / VALUES / SHOW / DESCRIBE / DESC / EXPLAIN of a read.
Everything else is rejected before it reaches the server: DDL/DML, CALL/EXECUTE, SET, LOAD/EXPORT, multiple statements,
write statements hidden behind a WITH, EXPLAIN ANALYZE of a write, and Hive's script/Java escapes (TRANSFORM, reflect).

This is a defence against mistakes, not a security boundary: also connect with an account that has read-only rights.
"""
import re

from ..sqlguard import split_statements, strip_comments

_ALLOWED_FIRST = {"SELECT", "WITH", "VALUES", "SHOW", "DESCRIBE", "DESC", "EXPLAIN"}
_QUERY_FIRST = {"SELECT", "WITH", "VALUES"}
# Words that can turn a statement that STARTS like a query into a write (WITH x AS (...) INSERT ..., FROM a INSERT ...).
# Deliberately short: names like comment/set/add/load are common columns and cannot make a SELECT write anything.
_WRITE_WORDS = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|GRANT|REVOKE|CALL|EXECUTE)\b", re.I)
_ESCAPES = re.compile(r"\b(reflect|reflect2|java_method)\s*\(|\bTRANSFORM\s*\(", re.I)
_EXPLAIN_OPTS = re.compile(
    r"(?is)^EXPLAIN\s+(?:(?:ANALYZE|VERBOSE|EXTENDED|FORMATTED|DEPENDENCY|AUTHORIZATION|LOCKS|CBO|COST|AST|VECTORIZATION|"
    r"REOPTIMIZATION|DDL|LOGICAL|DISTRIBUTED|VALIDATE|IO)\b\s*|\([^)]*\)\s*)*")


class QueryRejected(ValueError):
    """The statement is not a single read-only statement."""


def _mask_literals(sql: str) -> str:
    sql = re.sub(r"'(?:[^'\\]|\\.|'')*'", "''", sql)
    sql = re.sub(r'"(?:[^"]|"")*"', '""', sql)
    return re.sub(r"`[^`]*`", "``", sql)


def _check_one(stmt: str) -> None:
    m = re.match(r"(?s)^[\s(]*(\w+)", stmt)
    if not m:
        raise QueryRejected("Could not read the statement.")
    kw = m.group(1).upper()
    if kw == "EXPLAIN":
        inner = _EXPLAIN_OPTS.sub("", stmt, count=1).strip()
        if not inner:
            raise QueryRejected("EXPLAIN needs a statement to explain.")
        return _check_one(inner)  # EXPLAIN ANALYZE actually runs the inner statement, so it must itself be a read
    if kw not in _ALLOWED_FIRST:
        raise QueryRejected(f"Only read-only statements are allowed (SELECT, WITH, VALUES, SHOW, DESCRIBE, EXPLAIN); "
                            f"this one starts with {kw}.")
    if kw in _QUERY_FIRST:
        masked = _mask_literals(stmt)
        bad = _WRITE_WORDS.search(masked)
        if bad:
            raise QueryRejected(f"The query contains {bad.group(1).upper()}, which is not allowed in a read-only query "
                                "(quote the name if it is a column or table called that).")
        esc = _ESCAPES.search(masked)
        if esc:
            raise QueryRejected("Hive scripts and Java reflection (TRANSFORM, reflect, java_method) are not allowed.")


def check_readonly(sql: str) -> str:
    """Return the single cleaned statement (no comments, no trailing ';') or raise QueryRejected."""
    cleaned = strip_comments(sql or "").strip()
    stmts = [s.strip() for s in split_statements(cleaned) if s.strip()]
    if len(stmts) != 1:
        raise QueryRejected("Enter exactly one statement." if stmts else "Enter a statement.")
    _check_one(stmts[0])
    return stmts[0].rstrip(";").strip()


_IDENT = re.compile(r"^[A-Za-z0-9_.$-]{1,200}$")


def safe_identifier(name: str) -> str:
    """For names the UI composes into SHOW/DESCRIBE statements (database, schema, table, catalog)."""
    if not _IDENT.match(name or ""):
        raise QueryRejected(f"'{name}' is not a plain identifier (letters, digits, _ . $ - only).")
    return name
