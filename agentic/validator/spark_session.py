"""The one SparkSession used by the validator (created lazily, reused for thousands of tiny runs).

Imports of pyspark happen inside functions so the static checks, the SQL guard and the unit tests that use a
fake Spark work on machines without Java.
"""
import importlib.util
import os
import shutil
from typing import Optional

_SESSION = None


def spark_unavailable_reason() -> Optional[str]:
    """None if a Spark session can be created here, else why not."""
    if _SESSION is not None:  # already created or injected
        return None
    if importlib.util.find_spec("pyspark") is None:
        return "pyspark is not installed (run the validator on the cluster host, or pip install pyspark)"
    if not (shutil.which("java") or os.getenv("JAVA_HOME")):
        return "no Java runtime found (set JAVA_HOME to the JDK Spark uses)"
    return None


def get_spark():
    global _SESSION
    if _SESSION is not None:
        return _SESSION
    from pyspark.sql import SparkSession
    from ..config import validator_settings
    cfg = validator_settings()["spark"]
    b = (SparkSession.builder.master(cfg["master"]).appName("blueprint-validator")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.default.parallelism", "2")
         .config("spark.sql.adaptive.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         # keep NULL-valued fields in toJSON() output so results compare faithfully
         .config("spark.sql.jsonGenerator.ignoreNullFields", "false"))
    for k, v in cfg["conf"].items():
        b = b.config(k, str(v))
    _SESSION = b.getOrCreate()
    _SESSION.sparkContext.setLogLevel("ERROR")
    return _SESSION


def set_spark(session) -> None:
    """Inject a session (tests use a recording fake)."""
    global _SESSION
    _SESSION = session
