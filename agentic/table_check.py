"""Check that the tables in a DDL file exist in the Hive metastore, and create the missing ones.

    python -m agentic.table_check Claude_op/aahhd01q_pnc_poc_ddl.sql            # report only (default)
    python -m agentic.table_check Claude_op/aahhd01q_pnc_poc_ddl.sql --apply    # also create the missing tables

Runs where pyspark and Java exist (the cluster host). Connection settings come from config.yaml, section
validation.cluster (master and spark_conf), the same settings run-and-diff uses.

Safety: only CREATE TABLE statements that contain IF NOT EXISTS are run, and only for tables named in the file.
Without --apply nothing is created. Missing databases are created only with --apply.
"""
import argparse
import re
import sys
from typing import Dict, List, Optional, Tuple

from .config import validation_settings

CREATE_RE = re.compile(r"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w.]+)", re.I)


def parse_ddl(text: str) -> List[Tuple[str, str]]:
    """(qualified table name, statement) for each CREATE TABLE statement, in file order. `--` comments are ignored."""
    body = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    tables = []
    for stmt in body.split(";"):
        m = CREATE_RE.search(stmt)
        if m:
            tables.append((m.group(1), stmt.strip()))
    return tables


def spark_session():
    from pyspark.sql import SparkSession
    cluster = validation_settings().get("cluster") or {}
    builder = SparkSession.builder.appName("ensure_tables")
    if cluster.get("master"):
        builder = builder.master(cluster["master"])
    for key, value in (cluster.get("spark_conf") or {}).items():
        builder = builder.config(key, str(value))
    return builder.enableHiveSupport().getOrCreate()


def _tables_in(spark, db: str) -> Optional[set]:
    """Lower-case table names in a database, or None when the database does not exist."""
    try:
        return {str(r["tableName"]).lower() for r in spark.sql(f"SHOW TABLES IN {db}").collect()}
    except Exception:  # AnalysisException: no such database
        return None


def ensure_tables(spark, ddl_text: str, apply: bool = False) -> List[dict]:
    """One row per table: status is exists, created, missing (report only) or failed."""
    existing: Dict[str, Optional[set]] = {}
    rows = []
    for name, stmt in parse_ddl(ddl_text):
        db, table = name.split(".", 1) if "." in name else ("default", name)
        if db not in existing:
            existing[db] = _tables_in(spark, db)
        row = {"table": name, "status": "", "error": ""}
        if existing[db] is not None and table.lower() in existing[db]:
            row["status"] = "exists"
        elif not apply:
            row["status"] = "missing (report only)"
        elif "IF NOT EXISTS" not in stmt.upper():
            row["status"], row["error"] = "failed", "statement has no IF NOT EXISTS; not run"
        else:
            try:
                if existing[db] is None:
                    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
                    existing[db] = set()
                spark.sql(stmt)
                existing[db].add(table.lower())
                row["status"] = "created"
            except Exception as e:
                row["status"], row["error"] = "failed", str(e).splitlines()[0][:300]
        rows.append(row)
    return rows


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="agentic.table_check", description="Check and create the tables in a DDL file")
    ap.add_argument("ddl", help="DDL file with CREATE TABLE statements")
    ap.add_argument("--apply", action="store_true", help="create the missing tables (default: report only)")
    a = ap.parse_args(argv)
    try:
        with open(a.ddl, encoding="utf-8") as fh:
            text = fh.read()
    except OSError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    spark = spark_session()
    try:
        rows = ensure_tables(spark, text, apply=a.apply)
    finally:
        spark.stop()
    width = max(len(r["table"]) for r in rows) if rows else 10
    for r in rows:
        print(f"{r['table']:<{width}}  {r['status']}" + (f"  ({r['error']})" if r["error"] else ""))
    failed = [r for r in rows if r["status"] == "failed"]
    if not a.apply and any(r["status"].startswith("missing") for r in rows):
        print("\nNothing was created. Run again with --apply to create the missing tables.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
