"""Tools the agent can call: compare a Spark 2 job with its Spark 3 version, and check or run job code.

The same functions back the CLIs (`agentic.spark_compare`, `agentic.code_exec`) and the HTTP API (`agentic.api`).
Usage with the tool layer:  ToolRegistry(TOOLS).dispatch("compare_spark_jobs", {...})
"""
from .code_exec import execute
from .spark_compare import compare, load_source
from .tools import ToolSpec

COMPARE_SCHEMA = {
    "type": "object",
    "properties": {
        "spark2_path": {"type": "string", "description": "Path to the Spark 2 job file"},
        "spark3_path": {"type": "string", "description": "Path to the Spark 3 job file"},
    },
    "required": ["spark2_path", "spark3_path"],
}

EXECUTE_SCHEMA = {
    "type": "object",
    "properties": {
        "code": {"type": "string", "description": "Full Python source, no markdown fences"},
        "mode": {"type": "string", "enum": ["check", "run"],
                 "description": "check = compile only (default); run = execute the code"},
        "runner": {"type": "string", "enum": ["python", "spark-submit"], "description": "default: python"},
        "timeout_seconds": {"type": "integer", "description": "1 to 1800, default 120"},
    },
    "required": ["code"],
}


def _compare_tool(args: dict) -> str:
    res = compare(load_source(path=args["spark2_path"]), load_source(path=args["spark3_path"]))
    s = res["report"]["summary"]
    return (f"OK: compared {s['sections']} sections: {s['rewritten']} rewritten, {s['same logic']} same logic, "
            f"{s['only in one file']} in one file only. Report: {res['files']['markdown']}")


def _execute_tool(args: dict) -> str:
    r = execute(args["code"], mode=args.get("mode", "check"), runner=args.get("runner", "python"),
                timeout=args.get("timeout_seconds", 120))
    if r["status"] == "passed":
        if r["mode"] == "check":
            return "OK: syntax check passed"
        return f"OK: exit 0 in {r['duration_s']}s\n{r['stdout'][-2000:]}"
    detail = r.get("error") or (r.get("stderr") or "")[-2000:]
    return f"ERROR: {r['status']}: {detail}"


TOOLS = [
    ToolSpec("compare_spark_jobs",
             "Compare a Spark 2 job file with its Spark 3 rewrite step by step. Writes a Markdown report, a "
             "side-by-side HTML diff and a JSON record. Deterministic; no LLM.",
             _compare_tool, COMPARE_SCHEMA),
    ToolSpec("execute_job_code",
             "Check (compile) or run the Python or spark-submit code of a job and return its output.",
             _execute_tool, EXECUTE_SCHEMA),
]
