"""One output folder, timestamped file names.

Every run writes into the same folder (default `output/`) and names each file `<name>_<YYYYMMDD>_<HHMMSS>.<ext>`,
e.g. `pipeline_spec_20261020_153045.yaml`. Files from one run share one stamp, so they can be matched, and a rerun
never overwrites an earlier one.
"""
import os
import re
from datetime import datetime
from typing import Optional


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
