import argparse

from agentic import AgenticFramework, create_llm

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Legacy job -> YAML -> PySpark 3 + Airflow")
    p.add_argument("input", nargs="?", default="stm_input.xlsx",
                   help="Oozie .xml, PySpark2 .py, Sqoop .sh, Hive .hql, or Excel .xlsx")
    p.add_argument("--type", default="auto",
                   choices=["auto", "oozie", "pyspark2", "sqoop", "hiveql", "excel"])
    p.add_argument("--provider", default=None, choices=["anthropic", "azure"],
                   help="LLM provider (default: $LLM_PROVIDER or anthropic)")
    p.add_argument("--out", default="output")
    a = p.parse_args()
    AgenticFramework(a.input, create_llm(a.provider), out_dir=a.out, source_type=a.type).run()
