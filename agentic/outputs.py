"""One folder per run, named after the input file, inside the output folder; timestamped file names.

A run writes ALL its files into a single subfolder of the output folder (default `output/`), named
`<input file name>_<YYYYMMDD>_<HHMMSS>`:

    output/daily_txn_risk_job_20261020_153045/
        pipeline_spec_20261020_153045.yaml   generated_pipeline_20261020_153045.py   airflow_dag_20261020_153045.py
        validation_report_20261020_160210.md ...   (validator reports are added to the folder of the spec they check)

File names are `<name>_<YYYYMMDD>_<HHMMSS>.<ext>`. A rerun gets its own folder, so nothing is ever overwritten.
"""
import os
import re
from datetime import datetime
from typing import Optional, Tuple

# files that mark a folder as the output of a run (used to recognise run folders whatever they are called)
_RUN_MARKERS = ("run_meta_", "events_", "input_screened_", "pipeline_spec_")


def stamp_now(now: Optional[datetime] = None) -> str:
    return (now or datetime.now()).strftime("%Y%m%d_%H%M%S")


def unique_stamp(out_dir: str, stamp: str) -> str:
    """`stamp`, or `stamp_2`, `stamp_3`... if entries with that stamp already exist (two runs in the same second)."""
    if not os.path.isdir(out_dir):
        return stamp
    names = os.listdir(out_dir)
    cand, n = stamp, 1
    while any(re.search(rf"_{re.escape(cand)}(?:\.|$)", f) for f in names):
        n += 1
        cand = f"{stamp}_{n}"
    return cand


def out_name(base: str, stamp: str, ext: str) -> str:
    return f"{base}_{stamp}.{ext.lstrip('.')}"


def safe_stem(name: Optional[str], default: str = "run") -> str:
    """File-system-safe base name of an input file: `/x/Daily Job.py` -> `Daily_Job`."""
    stem = os.path.splitext(os.path.basename((name or "").replace("\\", "/")))[0]
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._-")[:60]
    return stem or default


def is_run_dir(path: str) -> bool:
    try:
        return os.path.isdir(path) and any(f.startswith(_RUN_MARKERS) for f in os.listdir(path))
    except OSError:
        return False


def new_run_dir(base_dir: str, stamp: str, name: Optional[str] = None) -> Tuple[str, str]:
    """Create `base_dir/<name>_<stamp>` (the stamp is made unique among existing entries) and return (path, stamp).
    `name` is normally the input file name; without it the folder is `run_<stamp>`."""
    os.makedirs(base_dir, exist_ok=True)
    stamp = unique_stamp(base_dir, stamp)
    path = os.path.join(base_dir, f"{safe_stem(name)}_{stamp}")
    os.makedirs(path, exist_ok=True)
    return path, stamp


def validation_dir(spec_path: str, base_dir: str = "output") -> str:
    """Where a validator run should write: the run folder that already holds the spec it checks; for a spec
    that lives elsewhere, a fresh folder named after that spec under `base_dir`."""
    parent = os.path.dirname(os.path.abspath(spec_path))
    if is_run_dir(parent):
        return parent
    return new_run_dir(base_dir, stamp_now(), name=spec_path)[0]
