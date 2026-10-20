# Agentic migration framework

Legacy jobs → YAML blueprint → PySpark 3.x + Airflow DAG, using tool-calling agents.

```
Oozie XML ─┐
PySpark 2  ├─ ingestion agent (per source) ─┐
Sqoop      │                                ├─► YAML ─► Agentic Framework ─┬─► PySpark 3.x
Excel     ─┘                                │                              └─► Airflow DAG
Hive QL ─► Hive→Spark agent ─► Spark SQL ───┘
```

## Layout

```
main.py                     CLI
samples/                    sample Oozie / Sqoop / PySpark2 / Hive inputs
agentic/
  schemas.py                common YAML contract (PipelineBlueprint)
  parsers.py                deterministic tools: Oozie XML, Sqoop flags, PySpark2 scanner
  validation.py             YAML/DAG, Python, Airflow DAG, SQL validators
  llm.py                    BaseLLM + AnthropicLLM + create_llm()
  azure_openai.py           AzureOpenAILLM (function calling, key or Entra ID auth)
  tools.py                  ToolSpec + ToolRegistry
  agent.py                  ToolAgent loop (observe -> act, message memory)
  agents.py                 ingestion (per source), hive_to_spark, pyspark, airflow agents
  orchestrator.py           router + 5-stage pipeline + export
```

## Run

```
pip install -r requirements.txt
$env:ANTHROPIC_API_KEY="..."            # PowerShell
python main.py samples/workflow.xml     # type auto-detected
python main.py samples/report.hql
python main.py samples/import.sh --type sqoop
```

Outputs in `output/`: `pipeline_spec.yaml`, `generated_pipeline.py`, `airflow_dag.py`
(plus `converted_spark.sql` for Hive input). `ANTHROPIC_MODEL` overrides the model.

## LLM providers

`agentic/llm.py` defines `BaseLLM`; each provider adapts its own message/tool format.
Select with `--provider anthropic|azure` or `LLM_PROVIDER`.

Azure OpenAI (`agentic/azure_openai.py`) env vars:

```
AZURE_OPENAI_ENDPOINT=https://<resource>.openai.azure.com/
AZURE_OPENAI_DEPLOYMENT=<deployment name>     # must support tool/function calling
AZURE_OPENAI_API_KEY=...                      # omit to use Entra ID (azure-identity)
AZURE_OPENAI_API_VERSION=2024-10-21           # optional
```

```
python main.py samples/workflow.xml --provider azure
```

## Tool calling

Each agent has a `ToolRegistry`. Ingestion agents get a parser tool (`parse_oozie`,
`parse_sqoop`, `scan_pyspark2`) plus `validate_yaml`; generation agents get a checker
(`check_code`, `check_dag`, `check_sql`). `submit_final` only accepts validated output.
Add a source: add a profile in `agents.py::_SOURCE_PROFILES` and an extension in `orchestrator.py`.
