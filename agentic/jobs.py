"""Framework-independent job layer behind the UI: run migrations/validations on background threads, follow their
events, and read finished runs back from disk. No UI code here, so a CLI, an API or another front end can reuse it."""
import io
import json
import os
import re
import threading
import time
import uuid
import zipfile
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from . import events as ev
from .events import RunEvents, load_events
from .outputs import is_run_dir

MAX_UPLOAD_BYTES = 5 * 1024 * 1024
UPLOAD_EXTS = (".py", ".xml", ".sh", ".hql", ".sql", ".hive", ".xlsx", ".xls")


# ---- uploads --------------------------------------------------------------------------------
def save_upload(out_dir: str, filename: str, data: bytes) -> str:
    """Store an uploaded legacy job under <out_dir>/_uploads/<id>/ (basename only, extension checked, size capped)."""
    name = os.path.basename((filename or "").replace("\\", "/"))
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name) or "upload"
    if not name.lower().endswith(UPLOAD_EXTS):
        raise ValueError(f"unsupported file type: {name} (allowed: {', '.join(UPLOAD_EXTS)})")
    if len(data) > MAX_UPLOAD_BYTES:
        raise ValueError(f"{name} is larger than {MAX_UPLOAD_BYTES // (1024 * 1024)} MB")
    folder = os.path.join(out_dir, "_uploads", uuid.uuid4().hex[:8])
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, name)
    with open(path, "wb") as f:
        f.write(data)
    return path


# ---- finished runs on disk --------------------------------------------------------------------
def _meta_file(run_dir: str) -> Optional[str]:
    try:
        names = sorted(f for f in os.listdir(run_dir) if f.startswith("run_meta_") and f.endswith(".json"))
    except OSError:
        return None
    return os.path.join(run_dir, names[-1]) if names else None


def read_run_meta(run_dir: str) -> Dict[str, Any]:
    p = _meta_file(run_dir)
    if p:
        try:
            with open(p, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            pass
    return {"status": "incomplete", "input": {}, "files": []}


def list_runs(out_dir: str) -> List[Dict[str, Any]]:
    """Run folders under out_dir (any folder that holds run files, e.g. `daily_job_20261020_153045`), newest first,
    each with its run_meta summary. Housekeeping folders such as `_uploads` are skipped."""
    if not os.path.isdir(out_dir):
        return []
    runs = []
    for name in os.listdir(out_dir):
        p = os.path.join(out_dir, name)
        if not name.startswith("_") and is_run_dir(p):
            meta = read_run_meta(p)
            runs.append({"name": name, "path": p, "_t": meta.get("started") or time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(p))),
                         **meta})
    runs.sort(key=lambda r: (r["_t"], r["name"]), reverse=True)
    return runs


def run_files(run_dir: str) -> List[Dict[str, Any]]:
    """Files in a run folder, oldest first (the order they were produced)."""
    out = []
    try:
        for f in os.listdir(run_dir):
            full = os.path.join(run_dir, f)
            if os.path.isfile(full):
                st = os.stat(full)
                out.append({"name": f, "path": full, "size": st.st_size, "mtime": st.st_mtime})
    except OSError:
        pass
    return sorted(out, key=lambda d: (d["mtime"], d["name"]))


def find_file(run_dir: str, prefix: str) -> Optional[str]:
    names = sorted(f["name"] for f in run_files(run_dir) if f["name"].startswith(prefix))
    return os.path.join(run_dir, names[-1]) if names else None


def run_events(run_dir: str) -> List[Dict[str, Any]]:
    p = find_file(run_dir, "events_")
    return load_events(p) if p else []


def zip_run(run_dir: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in run_files(run_dir):
            z.write(f["path"], arcname=f["name"])
    return buf.getvalue()


# ---- jobs --------------------------------------------------------------------------------------
@dataclass
class Job:
    id: str
    kind: str                       # migration | validation
    label: str
    events: RunEvents = field(default_factory=RunEvents)
    status: str = "queued"          # queued | running | done | failed
    error: Optional[str] = None
    started: float = field(default_factory=time.time)
    finished: Optional[float] = None
    framework: Any = None           # the AgenticFramework (migration jobs)
    run_dir_hint: str = ""          # for validation jobs: the run folder being validated
    result: Optional[Dict[str, Any]] = None

    @property
    def run_dir(self) -> str:
        if self.framework is not None and self.framework.run_dir:
            return self.framework.run_dir
        return self.run_dir_hint

    @property
    def active(self) -> bool:
        return self.status in ("queued", "running")

    @property
    def seconds(self) -> float:
        return round((self.finished or time.time()) - self.started, 1)


class JobManager:
    def __init__(self, max_parallel: int = 2):
        self._jobs: Dict[str, Job] = {}
        self._lock = threading.Lock()
        self._slots = threading.Semaphore(max_parallel)

    def get(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    def all(self) -> List[Job]:
        with self._lock:
            return sorted(self._jobs.values(), key=lambda j: j.started, reverse=True)

    def _spawn(self, job: Job, target) -> Job:
        with self._lock:
            self._jobs[job.id] = job
        t = threading.Thread(target=target, args=(job,), daemon=True, name=f"job-{job.id}")
        t.start()
        return job

    # ---- migration ------------------------------------------------------------------------
    def submit_migration(self, input_path: str, label: str, out_dir: str, source_type: str = "auto",
                         provider: Optional[str] = None, snapshot=None, aliases: Optional[Dict[str, str]] = None) -> Job:
        job = Job(id=uuid.uuid4().hex[:8], kind="migration", label=label)

        def target(j: Job):
            token = ev.bind(j.events)
            try:
                with self._slots:
                    j.status = "running"
                    from .llm import create_llm
                    from .orchestrator import AgenticFramework
                    llm = create_llm(provider)
                    j.framework = AgenticFramework(input_path, llm, out_dir=out_dir, source_type=source_type,
                                                   snapshot=snapshot, aliases=aliases, input_label=label)
                    j.framework.run()
                j.status = "done"
            except Exception as e:  # the framework already recorded the stage error and run_meta
                j.status, j.error = "failed", f"{type(e).__name__}: {e}"[:800]
                if j.framework is None:
                    j.events.emit("run_failed", error=j.error, run_dir="")
            finally:
                j.finished = time.time()
                ev.unbind(token)

        return self._spawn(job, target)

    # ---- validation of an existing run -------------------------------------------------------
    def submit_validation(self, run_dir: str, snapshot=None, aliases: Optional[Dict[str, str]] = None) -> Job:
        job = Job(id=uuid.uuid4().hex[:8], kind="validation", label=f"validate {os.path.basename(run_dir)}", run_dir_hint=run_dir)

        def target(j: Job):
            token = ev.bind(j.events)
            try:
                with self._slots:
                    j.status = "running"
                    spec = find_file(run_dir, "pipeline_spec_")
                    src = find_file(run_dir, "input_screened_")
                    if not spec or not src:
                        raise RuntimeError("this run has no pipeline_spec / input_screened file to validate")
                    from .validation import ValidationEngine
                    from .validator.pipeline import run_validation
                    with open(spec, encoding="utf-8") as f:
                        bp = ValidationEngine.validate_yaml_structure(f.read())
                    if not bp.is_rich:
                        raise RuntimeError("the validator needs a rich blueprint (sources / transformations / targets); "
                                           "this run produced a flat step list")
                    with open(src, encoding="utf-8") as f:
                        text = f.read()
                    j.events.emit("note", message=f"validating {os.path.basename(spec)} against {os.path.basename(src)}")
                    report, _suite, sess = run_validation(
                        bp, text, src, out_dir=run_dir, progress=lambda m: j.events.emit("note", message=m),
                        snapshot=snapshot, aliases=aliases or {},
                        report_meta={"spec_file": os.path.basename(spec), "source_file": os.path.basename(src)})
                    j.result = {"result": report.result, "markdown": report.to_markdown(),
                                "files": [os.path.basename(p) for p in sess.written]}
                    for p in sess.written:
                        j.events.emit("file", label="Validation output", name=os.path.basename(p), path=p)
                j.status = "done"
            except Exception as e:
                j.status, j.error = "failed", f"{type(e).__name__}: {e}"[:800]
                j.events.emit("run_failed", error=j.error, run_dir=run_dir)
            finally:
                j.finished = time.time()
                ev.unbind(token)

        return self._spawn(job, target)
