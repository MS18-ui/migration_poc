"""One folder per run, inside the output folder; timestamped file names.

A run writes ALL its files into a single subfolder of the output folder (default `output/`):

    output/run_20261020_153045/
        pipeline_spec_20261020_153045.yaml   generated_pipeline_20261020_153045.py   airflow_dag_20261020_153045.py
        validation_report_20261020_160210.md ...   (validator reports are added to the folder of the spec they check)

File names are `<name>_<YYYYMMDD>_<HHMMSS>.<ext>`. A rerun gets its own folder, so nothing is ever overwritten.
"""
import os
import re
from datetime import datetime
from typing import Optional, Tuple


def stamp_now(now: Optional[datetime] = None) -> str:
    return (now or datetime.now()).strftime("%Y%m%d_%H%M%S")


def unique_stamp(out_dir: str, stamp: str) -> str:
    """`stamp`, or `stamp_2`, `stamp_3`... if files with that stamp already exist (two runs in the same second)."""
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


def new_run_dir(base_dir: str, stamp: str) -> Tuple[str, str]:
    """Create `base_dir/run_<stamp>` (the stamp is made unique among existing entries) and return (path, stamp)."""
    os.makedirs(base_dir, exist_ok=True)
    stamp = unique_stamp(base_dir, stamp)
    path = os.path.join(base_dir, f"run_{stamp}")
    os.makedirs(path, exist_ok=True)
    return path, stamp


def validation_dir(spec_path: str, base_dir: str = "output") -> str:
    """Where a validator run should write: the run folder that already holds the spec it checks; for a spec
    that lives elsewhere, a fresh run folder under `base_dir`."""
    parent = os.path.dirname(os.path.abspath(spec_path))
    if os.path.basename(parent).startswith("run_"):
        return parent
    return new_run_dir(base_dir, stamp_now())[0]
