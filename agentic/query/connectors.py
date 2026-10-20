"""Hive (HiveServer2 via PyHive) and Trino (trino client) connectors for read-only queries.

Imports are lazy so the rest of the framework runs without these drivers. Every query goes through
guard.check_readonly first, is capped to `row_limit` rows (the cursor is cancelled once the cap is reached) and to a
timeout, and is appended to a query log WITHOUT its results. Passwords come from the caller (a UI field or an
environment variable) and are never logged or stored.
"""
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .guard import QueryRejected, check_readonly, safe_identifier


class QueryError(Exception):
    """Connection or execution problem (message is safe to show: it never contains the password)."""


@dataclass
class QueryResult:
    engine: str
    sql: str
    columns: List[str]
    rows: List[List[Any]]
    truncated: bool
    seconds: float
    row_limit: int


def _plain(v: Any) -> Any:
    return v if v is None or isinstance(v, (str, int, float, bool)) else str(v)


def _scrub(msg: str, secret: Optional[str]) -> str:
    msg = str(msg)
    return (msg.replace(secret, "<password>") if secret else msg)[:600]


# ---- Hive ---------------------------------------------------------------------------------------
def _run_hive(stmt: str, conn: Dict[str, Any], row_limit: int, timeout: int) -> QueryResult:
    try:
        from pyhive import hive
    except ImportError:
        raise QueryError('PyHive is not installed. Install it with:  pip install "pyhive[hive_pure_sasl]"')
    password = conn.get("password") or None
    auth = (conn.get("auth") or "NONE").upper()
    kwargs: Dict[str, Any] = dict(host=conn.get("host"), port=int(conn.get("port") or 10000),
                                  username=conn.get("username") or None, database=conn.get("database") or "default", auth=auth)
    if not kwargs["host"]:
        raise QueryError("Enter the HiveServer2 host.")
    if auth in ("LDAP", "CUSTOM"):
        kwargs["password"] = password
    if auth == "KERBEROS":
        kwargs["kerberos_service_name"] = conn.get("kerberos_service_name") or "hive"
    t0 = time.time()
    c = None
    try:
        c = hive.Connection(**kwargs)
        cur = c.cursor()
        cur.execute(stmt, async_=True)
        try:  # poll so a long query can be cancelled at the timeout
            from TCLIService.ttypes import TOperationState
            active = {getattr(TOperationState, n) for n in ("INITIALIZED_STATE", "RUNNING_STATE", "PENDING_STATE")
                      if hasattr(TOperationState, n)}
            while cur.poll().operationState in active:
                if time.time() - t0 > timeout:
                    cur.cancel()
                    raise QueryError(f"Query cancelled after {timeout}s (timeout).")
                time.sleep(0.25)
        except ImportError:
            pass  # old PyHive: no polling; fetch blocks until the query finishes
        rows = cur.fetchmany(row_limit + 1)
        cols = [d[0] for d in (cur.description or [])]
        truncated = len(rows) > row_limit
        if truncated:
            try:
                cur.cancel()
            except Exception:
                pass
        return QueryResult("hive", stmt, cols, [[_plain(v) for v in r] for r in rows[:row_limit]], truncated,
                           round(time.time() - t0, 2), row_limit)
    except QueryError:
        raise
    except Exception as e:
        raise QueryError(_scrub(f"{type(e).__name__}: {e}", password))
    finally:
        try:
            if c is not None:
                c.close()
        except Exception:
            pass


# ---- Trino --------------------------------------------------------------------------------------
def _run_trino(stmt: str, conn: Dict[str, Any], row_limit: int, timeout: int) -> QueryResult:
    try:
        import trino
        from trino.auth import BasicAuthentication
    except ImportError:
        raise QueryError("The Trino client is not installed. Install it with:  pip install trino")
    password = conn.get("password") or None
    scheme = (conn.get("http_scheme") or "http").lower()
    if not conn.get("host"):
        raise QueryError("Enter the Trino host.")
    if password and scheme != "https":
        raise QueryError("Trino only accepts a password over https; set the scheme to https (or clear the password).")
    verify: Any = conn.get("verify", True)
    t0 = time.time()
    c = None
    try:
        c = trino.dbapi.connect(
            host=conn["host"], port=int(conn.get("port") or 8080), user=conn.get("user") or "migration-ui",
            catalog=conn.get("catalog") or None, schema=conn.get("schema") or None, http_scheme=scheme,
            auth=BasicAuthentication(conn.get("user") or "", password) if password else None, verify=verify,
            session_properties={"query_max_run_time": f"{int(timeout)}s"}, request_timeout=float(timeout) + 30)
        cur = c.cursor()
        cur.execute(stmt)
        rows = cur.fetchmany(row_limit + 1)
        cols = [d[0] for d in (cur.description or [])]
        truncated = len(rows) > row_limit
        if truncated:
            try:
                cur.cancel()
            except Exception:
                pass
        return QueryResult("trino", stmt, cols, [[_plain(v) for v in r] for r in rows[:row_limit]], truncated,
                           round(time.time() - t0, 2), row_limit)
    except QueryError:
        raise
    except Exception as e:
        raise QueryError(_scrub(f"{type(e).__name__}: {e}", password))
    finally:
        try:
            if c is not None:
                c.close()
        except Exception:
            pass


# ---- public API ---------------------------------------------------------------------------------
def log_query(path: Optional[str], engine: str, user: str, sql: str, rows: Optional[int], seconds: float, error: Optional[str]) -> None:
    """Append one line per query (statement text, size, timing, error) - never results, never passwords."""
    if not path:
        return
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "engine": engine, "user": user,
                                "sql": sql[:500], "rows": rows, "seconds": seconds, "error": (error or "")[:300] or None}) + "\n")
    except OSError:
        pass


def run_query(engine: str, conn: Dict[str, Any], sql: str, row_limit: int = 1000, timeout: int = 120,
              max_row_limit: int = 10000, log_path: Optional[str] = None) -> QueryResult:
    """Validate (read-only, one statement), run, cap, log. Raises QueryRejected or QueryError."""
    engine = (engine or "").lower()
    if engine not in ("hive", "trino"):
        raise QueryError(f"Unknown engine '{engine}'.")
    t0 = time.time()
    user = str(conn.get("username") or conn.get("user") or "")
    try:
        stmt = check_readonly(sql)
    except QueryRejected as e:
        log_query(log_path, engine, user, sql, None, 0.0, f"rejected: {e}")
        raise
    row_limit = max(1, min(int(row_limit), int(max_row_limit)))
    timeout = max(5, int(timeout))
    try:
        res = (_run_hive if engine == "hive" else _run_trino)(stmt, conn, row_limit, timeout)
    except QueryError as e:
        log_query(log_path, engine, user, stmt, None, round(time.time() - t0, 2), str(e))
        raise
    log_query(log_path, engine, user, stmt, len(res.rows), res.seconds, None)
    return res


def list_databases(engine: str, conn: Dict[str, Any], **kw) -> List[str]:
    """Hive: databases. Trino: schemas of the connection's catalog."""
    if engine == "hive":
        res = run_query("hive", conn, "SHOW DATABASES", **kw)
    else:
        res = run_query("trino", conn, f"SHOW SCHEMAS FROM {safe_identifier(conn.get('catalog') or 'hive')}", **kw)
    return [str(r[0]) for r in res.rows]


def list_tables(engine: str, conn: Dict[str, Any], database: str, **kw) -> List[str]:
    database = safe_identifier(database)
    if engine == "hive":
        res = run_query("hive", conn, f"SHOW TABLES IN {database}", **kw)
        return [str(r[1] if len(r) > 1 else r[0]) for r in res.rows]   # HiveServer2 may return (database, tableName)
    res = run_query("trino", conn, f"SHOW TABLES FROM {safe_identifier(conn.get('catalog') or 'hive')}.{database}", **kw)
    return [str(r[0]) for r in res.rows]


def describe_table(engine: str, conn: Dict[str, Any], database: str, table: str, **kw) -> QueryResult:
    qualified = f"{safe_identifier(database)}.{safe_identifier(table)}"
    if engine == "hive":
        return run_query("hive", conn, f"DESCRIBE FORMATTED {qualified}", **kw)
    return run_query("trino", conn, f"DESCRIBE {safe_identifier(conn.get('catalog') or 'hive')}.{qualified}", **kw)
