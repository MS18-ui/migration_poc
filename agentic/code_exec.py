"""Check or run a job's source text in a fresh temporary folder, with a timeout.

mode="check"  compiles the code only (default; nothing runs)
mode="run"    executes it with `python -I job.py`, or `spark-submit job.py` when runner="spark-submit"

Running code executes it with the privileges of this process. It is NOT a sandbox. The HTTP API therefore
refuses execution unless the FRAMEWORK_API_TOKEN environment variable is set (see agentic/api.py).
"""
import os
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Dict, Optional

DEFAULT_TIMEOUT = 120
MAX_TIMEOUT = 1800
OUTPUT_LIMIT = 200_000          # characters kept of stdout and of stderr
MODES = ("check", "run")
RUNNERS = ("python", "spark-submit")


def _clip(text) -> str:
    if text is None:
        return ""
    if isinstance(text, bytes):
        text = text.decode("utf-8", "replace")
    return text if len(text) <= OUTPUT_LIMIT else text[:OUTPUT_LIMIT] + "\n... (output truncated)"


def check(code: str) -> dict:
    try:
        compile(code, "job.py", "exec")
    except SyntaxError as e:
        return {"status": "failed", "mode": "check", "error": f"SyntaxError at line {e.lineno}: {e.msg}"}
    except ValueError as e:  # e.g. source contains null bytes
        return {"status": "failed", "mode": "check", "error": f"ValueError: {e}"}
    return {"status": "passed", "mode": "check", "error": ""}


def _conf_flags(conf: Optional[Dict[str, str]]) -> list:
    flags: list = []
    for key, value in (conf or {}).items():
        key = str(key)
        if not key.startswith("spark."):
            key = "spark.hadoop." + key   # spark-submit ignores bare Hadoop/Hive keys such as hive.metastore.uris
        flags += ["--conf", f"{key}={value}"]
    return flags


def run(code: str, runner: str = "python", timeout: int = DEFAULT_TIMEOUT,
        env: Optional[Dict[str, str]] = None, args: Optional[list] = None,
        master: Optional[str] = None, spark_conf: Optional[Dict[str, str]] = None) -> dict:
    """`args` are passed to the script; `master` and `spark_conf` apply to the spark-submit runner only."""
    if runner == "python":
        cmd_head = [sys.executable, "-I"]
    else:
        exe = shutil.which("spark-submit") or os.environ.get("SPARK_SUBMIT")
        if not exe:
            return {"status": "error", "mode": "run", "runner": runner,
                    "error": "spark-submit not found on PATH (set SPARK_SUBMIT to its full path)"}
        cmd_head = [exe] + (["--master", master] if master else []) + _conf_flags(spark_conf)
    timeout = min(max(int(timeout), 1), MAX_TIMEOUT)
    child_env = dict(os.environ, PYTHONIOENCODING="utf-8", **(env or {}))
    with tempfile.TemporaryDirectory(prefix="exec_") as tmp:
        with open(os.path.join(tmp, "job.py"), "w", encoding="utf-8") as fh:
            fh.write(code)
        started = time.time()
        try:
            p = subprocess.run(cmd_head + ["job.py"] + [str(a) for a in (args or [])], cwd=tmp, capture_output=True, text=True,
                               encoding="utf-8", errors="replace", timeout=timeout, env=child_env)
        except subprocess.TimeoutExpired as e:
            return {"status": "timeout", "mode": "run", "runner": runner, "exit_code": None,
                    "duration_s": round(time.time() - started, 2), "stdout": _clip(e.stdout),
                    "stderr": _clip(e.stderr), "error": f"stopped after {timeout} seconds"}
    return {"status": "passed" if p.returncode == 0 else "failed", "mode": "run", "runner": runner,
            "exit_code": p.returncode, "duration_s": round(time.time() - started, 2),
            "stdout": _clip(p.stdout), "stderr": _clip(p.stderr), "error": ""}


def execute(code: str, mode: str = "check", runner: str = "python", timeout: int = DEFAULT_TIMEOUT,
            env: Optional[Dict[str, str]] = None, args: Optional[list] = None,
            master: Optional[str] = None, spark_conf: Optional[Dict[str, str]] = None) -> dict:
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}")
    if runner not in RUNNERS:
        raise ValueError(f"runner must be one of {RUNNERS}")
    if not code or not code.strip():
        raise ValueError("code is empty")
    if mode == "check":
        return check(code)
    return run(code, runner=runner, timeout=timeout, env=env, args=args, master=master, spark_conf=spark_conf)
