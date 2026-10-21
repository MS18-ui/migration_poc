"""HTTP API for the migration tools.

    python -m agentic.api [--host 127.0.0.1] [--port 8000]
    Interactive docs: http://127.0.0.1:8000/docs

    GET  /api/health    liveness, and whether execution is enabled
    POST /api/compare   compare a Spark 2 job with its Spark 3 version (code in the body, or a path in the project)
    POST /api/execute   check (compile) or run job code. Needs the header  Authorization: Bearer <token>
                        where <token> is the FRAMEWORK_API_TOKEN environment variable. Without that variable,
                        execution is disabled (503).

Keep the default host (127.0.0.1) unless the API must be reachable from other machines. Execution runs code with
this process's privileges, so only expose it with a token and on a network you trust.
"""
import hmac
import os
from pathlib import Path
from typing import Dict, Literal, Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field, model_validator

from .code_exec import execute
from .spark_compare import compare, load_source

ROOT = Path(__file__).resolve().parent.parent     # the project folder; path inputs must stay inside it

app = FastAPI(
    title="Migration framework API",
    version="1.0",
    description="Compare Spark 2 and Spark 3 jobs, and check or run job code.",
)


class JobInput(BaseModel):
    name: Optional[str] = Field(None, description="Label used in the report. Defaults to the file name.")
    code: Optional[str] = Field(None, description="Full source text of the job.")
    path: Optional[str] = Field(None, description="File path inside the project folder.")

    @model_validator(mode="after")
    def one_source(self):
        if bool(self.code) == bool(self.path):
            raise ValueError("give exactly one of 'code' or 'path'")
        return self


class CompareRequest(BaseModel):
    spark2: JobInput
    spark3: JobInput


class ExecuteRequest(BaseModel):
    code: str = Field(..., min_length=1, description="Full Python source of the job.")
    mode: Literal["check", "run"] = Field("check", description="check = compile only; run = execute.")
    runner: Literal["python", "spark-submit"] = "python"
    timeout_seconds: int = Field(120, ge=1, le=1800)
    env: Dict[str, str] = Field(default_factory=dict, description="Extra environment variables for the run.")


def _job(job: JobInput):
    if job.code is not None and job.code != "":
        return load_source(code=job.code, name=job.name or "job")
    p = Path(job.path)
    p = (p if p.is_absolute() else ROOT / p).resolve()
    try:
        p.relative_to(ROOT)
    except ValueError:
        raise HTTPException(400, "path must be inside the project folder")
    if not p.is_file():
        raise HTTPException(404, f"no such file: {job.path}")
    return load_source(path=str(p), name=job.name)


def require_token(authorization: Optional[str] = Header(None)) -> None:
    expected = os.environ.get("FRAMEWORK_API_TOKEN", "")
    if not expected:
        raise HTTPException(503, "Code execution is disabled. Set FRAMEWORK_API_TOKEN and restart the API.")
    supplied = authorization[len("Bearer "):] if authorization and authorization.startswith("Bearer ") else ""
    if not hmac.compare_digest(supplied.encode("utf-8"), expected.encode("utf-8")):
        raise HTTPException(401, "missing or invalid bearer token")


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok", "execution_enabled": bool(os.environ.get("FRAMEWORK_API_TOKEN"))}


@app.post("/api/compare")
def compare_jobs(req: CompareRequest) -> dict:
    a, b = _job(req.spark2), _job(req.spark3)
    try:
        res = compare(a, b)
    except SyntaxError as e:
        raise HTTPException(422, f"could not parse a job: {e}")
    return {"summary": res["report"]["summary"], "steps": res["report"]["steps"],
            "evidence": res["report"]["evidence"], "differences": res["report"]["differences"],
            "folder": res["folder"], "files": res["files"]}


@app.post("/api/execute", dependencies=[Depends(require_token)])
def execute_code(req: ExecuteRequest) -> dict:
    try:
        return execute(req.code, mode=req.mode, runner=req.runner, timeout=req.timeout_seconds, env=req.env)
    except ValueError as e:
        raise HTTPException(422, str(e))


if __name__ == "__main__":
    import argparse

    import uvicorn

    ap = argparse.ArgumentParser(prog="agentic.api", description="Run the migration tools as an HTTP API")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    a = ap.parse_args()
    uvicorn.run(app, host=a.host, port=a.port)
