import hashlib
import os
from typing import Dict, Optional

import pandas as pd
import yaml

from .agents import (build_airflow_agent, build_hive_to_spark_agent,
                     build_ingestion_agent, build_pyspark_agent)
from .llm import BaseLLM
from .schemas import PipelineBlueprint
from .metadata import Snapshot, apply_snapshot
from .outputs import out_name, stamp_now, unique_stamp
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


class AgenticFramework:
    """Router + pipeline:  input -> [Hive->Spark] -> Ingestion -> YAML -> PySpark + Airflow."""

    def __init__(self, input_path: str, llm: BaseLLM, out_dir: str = "output",
                 source_type: str = "auto", snapshot: Optional[Snapshot] = None,
                 aliases: Optional[Dict[str, str]] = None):
        self.input_path, self.llm, self.out_dir = input_path, llm, out_dir
        self.source_type = source_type
        self.snapshot, self.aliases = snapshot, aliases or {}   # catalog metadata (optional)
        self.stamp = stamp_now()    # run start time; names every output file of this run
        self.output_files: list = []
        self.raw_input = ""
        self.model_text = ""        # what the models see: raw_input after local secret screening
        self.redactions = 0
        self.source_sha256 = ""
        self.spark_sql = ""
        self.blueprint: Optional[PipelineBlueprint] = None
        self.pyspark_code = ""
        self.airflow_code = ""

    # 1 -------------------------------------------------------------------------------
    def load_input(self):
        if self.source_type == "auto":
            if not os.path.exists(self.input_path):
                self.source_type = "excel"
            else:
                self.source_type = detect_source_type(self.input_path)
        print(f"[1/5 Load] {self.input_path} (source type: {self.source_type})")
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

    # 2 -------------------------------------------------------------------------------
    def convert_hive_if_needed(self):
        if self.source_type != "hiveql":
            return
        print("[2/5 Hive->Spark] Hive QL to Spark SQL agent working...")
        self.spark_sql = build_hive_to_spark_agent(self.llm).run(
            f"Hive QL:\n{self.model_text}\n\nConvert to Spark 3 SQL."
        )
        self.raw_input = self.model_text = self.spark_sql
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
            self._apply_metadata()
        print(">> Blueprint accepted.")

    def _apply_metadata(self):
        """Deterministic: the catalog, not the model, decides physical schemas (fills, corrects, records findings)."""
        bp = self.blueprint
        s = apply_snapshot(bp, self.snapshot, self.aliases, source_ref=self.input_path)
        print(f"   [metadata] snapshot {s['id']}: matched {len(s['matched'])}, unmatched {len(s['unmatched'])}, "
              f"{len(s['changes'])} change(s)")
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
            "source": {"path": self.input_path, "sha256": self.source_sha256, "type": self.source_type},
            "skills": skill_provenance(self.source_type),
            "model": getattr(self.llm, "model", None) or getattr(self.llm, "deployment", None),
            "local_secret_screening": {"values_masked": self.redactions},
        }
        if self.redactions and not any(f.get("id") == "F-REDACT" for f in bp.findings):
            bp.findings.append({
                "id": "F-REDACT", "category": "security", "severity": "medium", "confidence": "high",
                "source_ref": self.input_path, "affected_refs": [], "blocks": ["execution"], "status": "open",
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
        print("[4/5 Generate] Airflow agent working...")
        self.airflow_code = build_airflow_agent(self.llm, self.blueprint).run(
            f"YAML blueprint:\n{yaml_text}\n\nGenerate the Airflow DAG file."
        )
        print(">> Code accepted.")

    # 5 -------------------------------------------------------------------------------
    def export_production_artifacts(self):
        print("[5/5 Export] Writing outputs...")
        os.makedirs(self.out_dir, exist_ok=True)
        stamp = unique_stamp(self.out_dir, self.stamp)  # one folder, `<name>_<date>_<time>`; never overwrites a prior run
        spark_name = out_name("generated_pipeline", stamp, "py")
        files = {
            out_name("pipeline_spec", stamp, "yaml"): yaml.dump(self.blueprint.to_dict(), sort_keys=False),
            spark_name: self.pyspark_code + "\n",
            # the DAG is told to submit `generated_pipeline.py`; point it at the file actually written
            out_name("airflow_dag", stamp, "py"): self.airflow_code.replace("generated_pipeline.py", spark_name) + "\n",
        }
        if self.spark_sql:
            files[out_name("converted_spark", stamp, "sql")] = self.spark_sql + "\n"
        for name, content in files.items():
            with open(os.path.join(self.out_dir, name), "w", encoding="utf-8") as f:
                f.write(content)
        self.output_files = [os.path.join(self.out_dir, n) for n in files]
        print(f">> Wrote {', '.join(files)} to {self.out_dir}/")

    def run(self):
        self.load_input()
        self.convert_hive_if_needed()
        self.run_ingestion_agent()
        self.run_generation_agents()
        self.export_production_artifacts()



