"""Helpers shared by the validator: Spark type trees, typical-row generation, and scratch files for test data."""
import json
import os
import re
import tempfile
from typing import Any, Dict, List, Tuple

from ..testdata import _Gen, _split_top, parse_type  # noqa: F401  (parse_type re-exported)

Schema = List[Tuple[str, str]]  # [(column, spark type string)], e.g. ("tags", "array<string>")


def is_numeric_type(spark_type: str) -> bool:
    return bool(re.match(r"(?i)(decimal|int|integer|bigint|smallint|tinyint|double|float|long|short)", spark_type.strip()))


def norm_type(spark_type: str) -> str:
    """Comparable form of a Spark type string (case, spaces and quotes removed)."""
    return re.sub(r'[`"\s]', "", spark_type).lower()


def ddl_of(schema: Schema) -> str:
    """Spark DDL schema string: `a` string, `b` array<struct<x:int>>"""
    return ", ".join(f"`{c}` {t}" for c, t in schema)


class RowFactory:
    """Typical rows for a schema, deterministic per seed."""

    def __init__(self, seed: int = 11, run_date: str = "2026-10-18"):
        self.gen = _Gen(seed, run_date, 10)
        self.run_date = run_date

    def value(self, col: str, spark_type: str):
        return self.gen.value(parse_type(spark_type), col)

    def row(self, schema: Schema, **overrides) -> Dict[str, Any]:
        r = {c: self.value(c, t) for c, t in schema}
        r.update(overrides)
        return r


_SHARED_DIR = None
_COUNTER = 0


def new_tempdir() -> str:
    """One scratch directory per process (removed at exit); file names are made unique by write_ndjson."""
    global _SHARED_DIR
    if _SHARED_DIR is None:
        import atexit
        import shutil
        _SHARED_DIR = tempfile.mkdtemp(prefix="validator_")
        atexit.register(shutil.rmtree, _SHARED_DIR, True)
    return _SHARED_DIR


def write_ndjson(rows: List[dict], directory: str, name: str) -> str:
    global _COUNTER
    _COUNTER += 1
    path = os.path.join(directory, f"{name}_{os.getpid()}_{_COUNTER}.json")
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")
    return path
