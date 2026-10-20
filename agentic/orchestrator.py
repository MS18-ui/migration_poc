import hashlib
import json
import os
import time
from typing import Dict, List, Optional

import pandas as pd
import yaml

from . import events
from .agents import (build_airflow_agent, build_hive_to_spark_agent,
                     build_ingestion_agent, build_pyspark_agent)
from .llm import BaseLLM
from .schemas import PipelineBlueprint
from .metadata import Snapshot, apply_snapshot
from .outputs import new_run_dir, out_name, stamp_now
from .secrets import redact_secrets
from .skills_loader import skill_provenance
from .validation import ValidationEngine

_EXT_MAP = {
    ".xlsx": "excel", ".xls": "excel",
    ".xml": "oozie",
    ".py": "pyspark2",
    ".sh": "sqoop",
    ".hql": "hiveql", ".hive": "hiveql", ".sql": "hiveql",
}


def detect_source_type(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext in _EXT_MAP:
        return _EXT_MAP[ext]
    with open(path, encoding="utf-8", errors="ignore") as f:
        head = f.read(4000).lower()
    if "<workflow-app" in head:
        return "oozie"
    if "sqoop" in head:
        return "sqoop"
    if "import pyspark" in head or "sparkcontext" in head:
        return "pyspark2"
    return "hiveql"


STAGES = [("load", "Load and screen input"), ("hive", "Hive QL to Spark SQL"), ("ingest", "Analyse source to YAML blueprint"),
          ("generate", "Generate PySpark and Airflow"), ("export", "Finish")]


class AgenticFramework:
    """Router + pipeline:  input -> [Hive->Spark] -> Ingestion -> YAML -> PySpark + Airflow.

    Every artifact is written into this run's folder (<out_dir>/run_<stamp>/) AS SOON AS IT EXISTS, and progress is
    reported through `agentic.events`, so a UI can show the intermediate steps and files while the run is going."""

    def __init__(self, input_path: str, llm: BaseLLM, out_dir: str = "output",
                 source_type: str = "auto", snapshot: Optional[Snapshot] = None,
                 aliases: Optional[Dict[str, str]] = None, input_label: Optional[str] = None):
        self.input_path, self.llm, self.out_dir = input_path, llm, out_dir
        self.input_label = input_label or input_path   # e.g. "git: repo @ ref : path" when the file came from a clone
        self.source_type = source_type
        self.snapshot, self.aliases = snapshot, aliases or {}   # catalog metadata (optional)
        self.stamp = stamp_now()    # run start time; names every output file of this run
        self.output_files: list = []
        self.run_dir = ""           # <out_dir>/run_<stamp>, created when the run starts
        self.status = "new"         # new | running | done | failed
        self.error: Optional[str] = None
        self.raw_input = ""
        self.model_text = ""        # what the models see: raw_input after local secret screening
        self.redactions = 0
        self.source_sha256 = ""
        self.spark_sql = ""
        self.blueprint: Optional[PipelineBlueprint] = None
        self.pyspark_code = ""
        self.airflow_code = ""
        self._written: Dict[str, str] = {}   # base name -> path, so each artifact is written once
        self._t0 = time.time()

    # ---- run folder, files, stages ---------------------------------------------------------
    def _start_run(self) -> None:
        if self.run_dir:
            return
        self.run_dir, self.stamp = new_run_dir(self.out_dir, self.stamp)
        ev = events.current()
        if ev is not None:
            ev.set_path(os.path.join(self.run_dir, out_name("events", self.stamp, "jsonl")))
        self._t0 = time.time()
        self.status = "running"
        events.emit("run_start", run_dir=self.run_dir, stamp=self.stamp, input=self.input_label)

    def _write(self, label: str, base: str, ext: str, content: str) -> str:
        """Write one artifact into the run folder now (once per `base`) and announce it."""
        self._start_run()
        if base in self._written:
            return self._written[base]
        name = out_name(base, self.stamp, ext)
        path = os.path.join(self.run_dir, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        self._written[base] = path
        self.output_files.append(path)
        events.emit("file", label=label, name=name, path=path)
        return path

    def _stage(self, key: str, title: str, fn) -> None:
        events.emit("stage", stage=key, title=title, status="start")
        t0 = time.time()
        try:
            result = fn()
        except Exception as e:
            events.emit("stage", stage=key, title=title, status="error", error=str(e)[:600], seconds=round(time.time() - t0, 1))
            raise
        events.emit("stage", stage=key, title=title, status="skipped" if result == "skipped" else "done",
                    seconds=round(time.time() - t0, 1))

    # 1 -------------------------------------------------------------------------------
    def load_input(self):
        if self.source_type == "auto":
            if not os.path.exists(self.input_path):
                self.source_type = "excel"
            else:
                self.source_type = detect_source_type(self.input_path)
        print(f"[1/5 Load] {self.input_path} (source type: {self.source_type})")
        events.emit("note", message=f"source type: {self.source_type}")
        if self.source_type == "excel":
            if not os.path.exists(self.input_path):
                print("   (file not found - creating sample spreadsheet)")
                pd.DataFrame({
                    "Phase": ["source", "transformation"],
                    "Step_Name": ["extract_sales", "sanitize_dates"],
                    "Action": ["read_parquet", "cast_column"],
                    "Input_Source": ["s3://bucket/raw/", "extract_sales"],
                    "Target_Type": ["DataFrame", "DataFrame"],
                }).to_excel(self.input_path, index=False)
            self.raw_input = pd.read_excel(self.input_path).to_string(index=False)
        else:
            with open(self.input_path, encoding="utf-8") as f:
                self.raw_input = f.read()
        # Local secret screening BEFORE any model sees the source (skills: migration-analysis step 2)
        self.source_sha256 = hashlib.sha256(self.raw_input.encode("utf-8")).hexdigest()
        self.model_text, self.redactions = redact_secrets(self.raw_input)
        if self.redactions:
            print(f"   (masked {self.redactions} credential-looking value(s) locally before model submission)")
            events.emit("note", message=f"{self.redactions} credential-like value(s) were masked before analysis")
        ext = os.path.splitext(self.input_path)[1].lstrip(".").lower()
        self._write("Input submitted for analysis (credentials masked)", "input_screened",
                    ext if ext in ("py", "xml", "sh", "hql", "sql", "hive") else "txt", self.model_text)

    # 2 -------------------------------------------------------------------------------
    def convert_hive_if_needed(self):
        if self.source_type != "hiveql":
            return "skipped"
        print("[2/5 Hive->Spark] Hive QL to Spark SQL agent working...")
        self.spark_sql = build_hive_to_spark_agent(self.llm).run(
            f"Hive QL:\n{self.model_text}\n\nConvert to Spark 3 SQL."
        )
        self.raw_input = self.model_text = self.spark_sql
        self._write("Hive QL converted to Spark SQL", "converted_spark", "sql", self.spark_sql + "\n")
        print(">> Spark SQL accepted.")

    # 3 -------------------------------------------------------------------------------
    def run_ingestion_agent(self):
        print(f"[3/5 YAML] {self.source_type} ingestion agent working...")
        meta_block = (self.snapshot.prompt_block(self.model_text, self.aliases) if self.snapshot else
                      "No catalog metadata snapshot was supplied: no schemas are available. Do not invent them; "
                      "report the gap as a metadata finding.")
        self.blueprint = build_ingestion_agent(self.llm, self.source_type).run(
            f"Source system: {self.source_type}\nTarget engine: pyspark_3.x\n\n"
            f"Input:\n{self.model_text}\n\n{meta_block}\n\nProduce the YAML blueprint."
        )
        self.blueprint.source_system = self.blueprint.source_system or self.source_type  # type: ignore
        self._record_provenance()
        if self.snapshot:
            # show what the agent produced BEFORE the catalog is applied, then the catalog itself, then the result
            self._write("Blueprint draft (before catalog metadata is applied)", "blueprint_agent", "yaml",
                        yaml.dump(self.blueprint.to_dict(), sort_keys=False))
            self._write("Catalog metadata snapshot used", "catalog_metadata", "yaml",
                        yaml.dump(self.snapshot.to_dict(), sort_keys=False))
            self._apply_metadata()
        self._write("YAML blueprint (final)", "pipeline_spec", "yaml", yaml.dump(self.blueprint.to_dict(), sort_keys=False))
        print(">> Blueprint accepted.")

    def _apply_metadata(self):
        """Deterministic: the catalog, not the model, decides physical schemas (fills, corrects, records findings)."""
        bp = self.blueprint
        s = apply_snapshot(bp, self.snapshot, self.aliases, source_ref=self.input_path)
        print(f"   [metadata] snapshot {s['id']}: matched {len(s['matched'])}, unmatched {len(s['unmatched'])}, "
              f"{len(s['changes'])} change(s)")
        events.emit("note", message=f"catalog snapshot {s['id']}: matched {len(s['matched'])}, "
                                    f"unmatched {len(s['unmatched'])}, {len(s['changes'])} change(s)")
        try:
            ValidationEngine.validate_logical_dag(bp)
        except ValueError as e:  # keep going, but never hide it
            bp.findings.append({
                "id": "F-META-REVALIDATE", "category": "metadata", "severity": "high", "confidence": "high",
                "source_ref": self.input_path, "affected_refs": [], "blocks": ["generation", "execution"], "status": "open",
                "evidence": "after applying catalog metadata the blueprint no longer validates: " + str(e).replace("\n", " ")[:400],
                "resolution": "Reconcile the blueprint with the catalog schemas."})

    def _record_provenance(self):
        """Framework-written traceability (never by the agent): source hash, skill hashes, model, local screening."""
        bp = self.blueprint
        bp.provenance = {
            "source": {"path": self.input_label, "sha256": self.source_sha256, "type": self.source_type},
            "skills": skill_provenance(self.source_type),
            "model": getattr(self.llm, "model", None) or getattr(self.llm, "deployment", None),
            "local_secret_screening": {"values_masked": self.redactions},
        }
        if self.redactions and not any(f.get("id") == "F-REDACT" for f in bp.findings):
            bp.findings.append({
                "id": "F-REDACT", "category": "security", "severity": "medium", "confidence": "high",
                "source_ref": self.input_label, "affected_refs": [], "blocks": ["execution"], "status": "open",
                "evidence": f"{self.redactions} credential-looking value(s) were masked locally before the source was sent to "
                            "the model; the YAML cannot carry them.",
                "resolution": "Provide target credentials only as secret references in the deployment environment."})

    # 4 -------------------------------------------------------------------------------
    def run_generation_agents(self):
        yaml_text = yaml.dump(self.blueprint.to_dict(), sort_keys=False)
        print("[4/5 Generate] PySpark 3.x agent working...")
        self.pyspark_code = build_pyspark_agent(self.llm, self.blueprint).run(
            f"YAML blueprint:\n{yaml_text}\n\nGenerate the PySpark script."
        )
        spark_path = self._write("Generated PySpark 3 script", "generated_pipeline", "py", self.pyspark_code + "\n")
        print("[4/5 Generate] Airflow agent working...")
        self.airflow_code = build_airflow_agent(self.llm, self.blueprint).run(
            f"YAML blueprint:\n{yaml_text}\n\nGenerate the Airflow DAG file."
        )
        # the DAG is told to submit `generated_pipeline.py`; point it at the file actually written
        self._write("Airflow DAG", "airflow_dag", "py",
                    self.airflow_code.replace("generated_pipeline.py", os.path.basename(spark_path)) + "\n")
        print(">> Code accepted.")

    # 5 -------------------------------------------------------------------------------
    def export_production_artifacts(self):
        """Finish: make sure every artifact is on disk (normally all were written as they were produced) and record the run."""
        print("[5/5 Export] Finalising outputs...")
        self._start_run()
        if self.blueprint is not None:
            self._write("YAML blueprint (final)", "pipeline_spec", "yaml", yaml.dump(self.blueprint.to_dict(), sort_keys=False))
        if self.pyspark_code:
            spark_path = self._write("Generated PySpark 3 script", "generated_pipeline", "py", self.pyspark_code + "\n")
            if self.airflow_code:
                self._write("Airflow DAG", "airflow_dag", "py",
                            self.airflow_code.replace("generated_pipeline.py", os.path.basename(spark_path)) + "\n")
        if self.spark_sql:
            self._write("Hive QL converted to Spark SQL", "converted_spark", "sql", self.spark_sql + "\n")
        self.status = "done"
        self._write_run_meta()
        print(f">> Outputs are in {self.run_dir}/: {', '.join(os.path.basename(p) for p in self.output_files)}")

    def _write_run_meta(self) -> None:
        """run_meta_<stamp>.json: what a UI needs to list and reopen this run later."""
        bp = self.blueprint
        meta = {
            "status": self.status, "error": self.error, "stamp": self.stamp, "run_dir": self.run_dir,
            "started": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._t0)),
            "seconds": round(time.time() - self._t0, 1),
            "input": {"label": self.input_label, "sha256": self.source_sha256, "source_type": self.source_type},
            "model": getattr(self.llm, "model", None) or getattr(self.llm, "deployment", None),
            "metadata_snapshot": (bp.provenance.get("metadata_snapshot", {}).get("id") if bp is not None else None),
            "findings": len(bp.findings) if bp is not None else 0,
            "files": [os.path.basename(p) for p in self.output_files],
        }
        path = os.path.join(self.run_dir, out_name("run_meta", self.stamp, "json"))
        with open(path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=1, default=str)
        if path not in self.output_files:
            self.output_files.append(path)

    def run(self):
        self._start_run()
        try:
            self._stage("load", "Load and screen input", self.load_input)
            self._stage("hive", "Hive QL to Spark SQL", self.convert_hive_if_needed)
            self._stage("ingest", "Analyse source to YAML blueprint", self.run_ingestion_agent)
            self._stage("generate", "Generate PySpark and Airflow", self.run_generation_agents)
            self._stage("export", "Finish", self.export_production_artifacts)
            events.emit("run_done", run_dir=self.run_dir, files=[os.path.basename(p) for p in self.output_files])
        except Exception as e:
            self.status, self.error = "failed", str(e)[:800]
            try:
                self._write_run_meta()   # keep what was produced and why it stopped
            except Exception:
                pass
            events.emit("run_failed", error=self.error, run_dir=self.run_dir)
            raise
