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

## Validation agent (does the YAML capture all the source logic?)

```
python -m agentic.validator --spec output/pipeline_spec.yaml --source samples/legacy_job.py --out validation_out
python -m agentic.validator ... --agent offline     # scripted review loop, no network
python -m agentic.validator ... --agent llm         # LLM review (provider from config.yaml)
```

Writes `validation_report.md/.json` and `test_suite.json` (the minimized scenarios, with what each one kills).
Result is **PASSED** / **FAILED** (error findings) / **INCOMPLETE** (a layer did not run); exit codes 0 / 1 / 3.

| Layer | Needs Spark? | What it checks |
|---|---|---|
| Static (`coverage.py`, `compat.py`, `static_checks.py`) | no | source <-> YAML: reads/writes, constants, comparison operators and thresholds, join types, dedupe keys, window frames, Spark settings, created columns; executability of every operation; compatibility and security rules |
| Column/type flow | yes | the graph is built on empty tables; Spark's analyzer reports unresolved columns per node |
| Scenarios + oracles | yes | per node: boundary values of every constant, NULLs, empty/duplicate/missing collections, join key collisions; independent invariants (cardinalities) and source-derived boundary expectations |
| Mutation testing + synthesis | yes | the YAML is perturbed (operator, threshold, join type, filter, dedupe, window...); every perturbation must change some test's result; survivors are searched for killing data |
| Review agent | no | bounded tool loop; its findings are capped at "warning", flagged `origin: llm`, and never decide pass/fail |

**Where it runs.** The Spark-backed layers execute the blueprint's operations (DataFrame API + Spark SQL
expressions, exactly like the generated job) on small synthetic tables, so they need `pyspark` and Java: run it on the
cluster host. `validator.spark.master` in `config.yaml` is `local[2]` (driver-only, fast for thousands of tiny runs);
point it at `spark://spark-master:7077` to use the cluster itself (slower per run). It reads no Hive tables and writes
nothing but a temp directory. Without Spark the static layer still runs and the report is INCOMPLETE.

Tests: `python -m unittest discover -s tests -v`. Offline tests need no Spark; the Spark-backed ones are skipped
(not passed) where Spark/Java are missing.

## Skills (Type Exchange analysis skills as ingestion-agent prompts)

`agentic/skills/` holds the Type Exchange skills unchanged (one coordinator + one skill per conversion pair).
`agentic/skills_loader.py` builds each ingestion agent's system prompt from them:

| Source type | Skill used |
|---|---|
| oozie | oozie-to-airflow |
| sqoop | sqoop-to-spark |
| pyspark2 | pyspark2-to-spark3 |
| hiveql | hiveql-to-spark-sql (also the Hive->Spark SQL agent, with an SQL deliverable) |
| excel | none (built-in short prompt) |
| hive-to-iceberg, yarn-to-ocp | no ingestion profile yet |

The prompt is: coordinator skill + pair skill + security/privacy rules + findings contract, then an **Environment**
section (written last, takes precedence) that says what this run can and cannot do (the skills assume file tools and a
package writer; our agents have a parser tool and validate/submit only), then the blueprint format.
Switch off with `skills.enabled: false` in `config.yaml`.

What changes in a run:
- **Findings.** The agent records what the skill says to report (unsupported logic, missing metadata/schema,
  uniqueness and tie assumptions, privacy, write scope...) in the blueprint's `findings` list (checked by
  `blueprint_checks.check_findings`: allowed categories/severities, `blocks`, source_ref for high/critical, no
  credentials; status must be `open`, because only a human reviewer may resolve a finding or accept a risk).
  The validator shows them next to its own findings.
- **Local secret screening** (`agentic/secrets.py`) runs before any source reaches a model; a `F-REDACT` finding is
  added if anything was masked.
- **Bounded repair loop:** the first draft + `skills.max_corrections` (2) corrections, then the agent stops and the
  unresolved problems are reported.
- **Provenance** (written by the framework): source sha256, hashes of the skill files used, model, screening count.
- **Metadata:** the skills expect a catalog metadata snapshot and do not invent schemas, so without one sources may come
  back without schemas (reported as a finding) and the validator's Spark-backed layers cannot execute them.
