import os
from typing import Optional

import pandas as pd
import yaml

from .agents import (build_airflow_agent, build_hive_to_spark_agent,
                     build_ingestion_agent, build_pyspark_agent)
from .llm import BaseLLM
from .schemas import PipelineBlueprint

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
                 source_type: str = "auto"):
        self.input_path, self.llm, self.out_dir = input_path, llm, out_dir
        self.source_type = source_type
        self.raw_input = ""
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

    # 2 -------------------------------------------------------------------------------
    def convert_hive_if_needed(self):
        if self.source_type != "hiveql":
            return
        print("[2/5 Hive->Spark] Hive QL to Spark SQL agent working...")
        self.spark_sql = build_hive_to_spark_agent(self.llm).run(
            f"Hive QL:\n{self.raw_input}\n\nConvert to Spark 3 SQL."
        )
        self.raw_input = self.spark_sql
        print(">> Spark SQL accepted.")

    # 3 -------------------------------------------------------------------------------
    def run_ingestion_agent(self):
        print(f"[3/5 YAML] {self.source_type} ingestion agent working...")
        self.blueprint = build_ingestion_agent(self.llm, self.source_type).run(
            f"Source system: {self.source_type}\nTarget engine: pyspark_3.x\n\n"
            f"Input:\n{self.raw_input}\n\nProduce the YAML blueprint."
        )
        self.blueprint.source_system = self.blueprint.source_system or self.source_type  # type: ignore
        print(">> Blueprint accepted.")

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
        files = {
            "pipeline_spec.yaml": yaml.dump(self.blueprint.to_dict(), sort_keys=False),
            "generated_pipeline.py": self.pyspark_code + "\n",
            "airflow_dag.py": self.airflow_code + "\n",
        }
        if self.spark_sql:
            files["converted_spark.sql"] = self.spark_sql + "\n"
        for name, content in files.items():
            with open(os.path.join(self.out_dir, name), "w", encoding="utf-8") as f:
                f.write(content)
        print(f">> Wrote {', '.join(files)} to {self.out_dir}/")

    def run(self):
        self.load_input()
        self.convert_hive_if_needed()
        self.run_ingestion_agent()
        self.run_generation_agents()
        self.export_production_artifacts()

