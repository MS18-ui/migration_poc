import argparse

from agentic import AgenticFramework, create_llm
from agentic.config import metadata_settings
from agentic.metadata import load_snapshot

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Legacy job -> YAML -> PySpark 3 + Airflow")
    p.add_argument("input", nargs="?", default="stm_input.xlsx",
                   help="Oozie .xml, PySpark2 .py, Sqoop .sh, Hive .hql, or Excel .xlsx")
    p.add_argument("--type", default="auto",
                   choices=["auto", "oozie", "pyspark2", "sqoop", "hiveql", "excel"])
    p.add_argument("--provider", default=None, choices=["anthropic", "azure"],
                   help="LLM provider (default: $LLM_PROVIDER or anthropic)")
    p.add_argument("--metadata", nargs="*", default=None, metavar="PATH",
                   help="catalog metadata snapshot: Hive DDL (.hql/.sql), canonical YAML/JSON, or folders of those "
                        "(default: metadata.snapshot in config.yaml; pass no value to run without one)")
    p.add_argument("--metadata-alias", action="append", default=[], metavar="JOB_NAME=CATALOG_NAME",
                   help="explicit table alias, used only when the job's name differs from the catalog's (repeatable)")
    p.add_argument("--out", default="output",
                   help="output folder for all files (default: output); names are <name>_<YYYYMMDD>_<HHMMSS>.<ext>")
    a = p.parse_args()

    cfg = metadata_settings()
    paths = cfg["snapshot"] if a.metadata is None else a.metadata
    paths = [paths] if isinstance(paths, str) else [x for x in (paths or []) if x]
    snapshot = load_snapshot(*paths) if paths else None
    aliases = {**cfg["aliases"], **dict(x.split("=", 1) for x in a.metadata_alias)}
    if snapshot:
        print(f"[metadata] snapshot {snapshot.id}: {len(snapshot.tables)} table(s) from {', '.join(snapshot.files)}")
        for w in snapshot.warnings:
            print("[metadata] warning:", w)
    AgenticFramework(a.input, create_llm(a.provider), out_dir=a.out, source_type=a.type,
                     snapshot=snapshot, aliases=aliases).run()
