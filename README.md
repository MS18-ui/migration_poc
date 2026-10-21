# Agentic migration framework

Legacy jobs â†’ YAML blueprint â†’ PySpark 3.x + Airflow DAG, using tool-calling agents.

```
Oozie XML â”€â”
PySpark 2  â”œâ”€ ingestion agent (per source) â”€â”
Sqoop      â”‚                                â”œâ”€â–º YAML â”€â–º Agentic Framework â”€â”¬â”€â–º PySpark 3.x
Excel     â”€â”˜                                â”‚                              â””â”€â–º Airflow DAG
Hive QL â”€â–º Hiveâ†’Spark agent â”€â–º Spark SQL â”€â”€â”€â”˜
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
  orchestrator.py           router + 5-stage pipeline; writes each artifact as it is produced
  events.py                 structured step events (UI / events_<stamp>.jsonl)
  jobs.py                   background job runner + run history (used by the UI)
  gitsource.py              read-only git input (shallow https clone, safe paths)
  query/                    read-only Hive / Trino: guard.py, connectors.py
ui/app.py                   Streamlit workbench (Migration / Run History / Data Query)
```

## Run

```
pip install -r requirements.txt
$env:ANTHROPIC_API_KEY="..."            # PowerShell
python main.py samples/workflow.xml     # type auto-detected
python main.py samples/report.hql
python main.py samples/import.sh --type sqoop
```

### Configuration

All settings live in `config.yaml` (LLM provider and endpoint, metadata snapshot, skills, validation, validator Spark).
`config.yaml` is **local and git-ignored** because it can hold keys. The repo tracks `config.example.yaml` (every setting,
no secrets); with no `config.yaml` present it is used automatically. To customise: `cp config.example.yaml config.yaml`
and edit it. Put keys in the environment (`AZURE_OPENAI_API_KEY`, `ANTHROPIC_API_KEY`), not in the file.

### Outputs: one folder per run, inside `output/`

A pipeline run writes ALL of its files into a single subfolder of the output folder (`output/` by default, `--out DIR` to
change the base). The folder is named after the input file: `<input file name>_<YYYYMMDD>_<HHMMSS>`, for example
`daily_txn_risk_job_20261020_153045`. Files inside are named `<name>_<YYYYMMDD>_<HHMMSS>.<ext>`. A rerun gets its own folder,
so nothing is overwritten (two runs in the same second get `_2`, `_3`...).

Files are written AS EACH STEP PRODUCES THEM (not only at the end), so a failed run still leaves what it got that far:

```
output/
  daily_txn_risk_job_20261020_153045/
    input_screened_<stamp>.<ext>        the input as submitted for analysis (credentials masked)
    converted_spark_<stamp>.sql         Hive QL -> Spark SQL                (Hive input only)
    blueprint_agent_<stamp>.yaml        the blueprint draft before the catalog is applied     (when a catalog is used)
    catalog_metadata_<stamp>.yaml       the catalog tables this pipeline reads or writes      (when a catalog is used)
    pipeline_spec_<stamp>.yaml          the final specification: sources, transformations, targets, findings
    generated_pipeline_<stamp>.py       airflow_dag_<stamp>.py
    provenance_<stamp>.yaml             audit record: source hash, skill versions, catalog application
    run_meta_<stamp>.json               status, input, timings, file list (lets the UI reopen the run)
    events_<stamp>.jsonl                the step-by-step history
    validation_report_<stamp>.json/.md  test_suite_<stamp>.json               (added by the validator)
```

**`pipeline_spec` is written to be read by people.** It contains only what describes the pipeline: empty values are not written,
keys follow a reading order (identifier and type first, notes last; findings: severity before evidence), long SQL
expressions are kept on one line or in a literal block instead of being wrapped mid-expression, source schemas list only the
columns the pipeline uses, and audit data (hashes, skill versions, the catalog application log) is kept in `provenance_<stamp>.yaml`
instead. It still loads with `agentic.validation.load_yaml`, which reads `on` as a plain key.

The validator adds its report to the run folder that holds the spec it checks (so one job's results stay together); for a
spec that lives elsewhere it creates a new folder named after the spec under `output/`. The Airflow DAG submits the timestamped
`generated_pipeline_<stamp>.py` that was actually written, and a validation report records `spec_file` and `source_file`
in its summary. `ANTHROPIC_MODEL` overrides the model.

## Web UI

```
pip install -r requirements.txt                       # streamlit>=1.37, trino  (+ pyhive for Hive, see requirements.txt)
python -m streamlit run ui/app.py --server.port 8501 --server.address 0.0.0.0     # run from the project root
```

Open `http://<host>:8501`. To require an access password, set the environment variable `AGENTIC_UI_PASSWORD` before
starting the application. Service credentials are read from the environment or from `config.yaml`; they are never entered
on the page, and the interface does not expose provider or model settings.

- **Migration**: upload one or more files, or enter a git repository (HTTPS URL, optional branch or tag, folder and, for
  private repositories, an access token), select *Retrieve Files* and choose the files to migrate. Advanced options: source
  type, catalog metadata (configured default, uploaded DDL or YAML, or none) and table aliases. *Start Migration* runs each
  file as a separate job. The progress panel refreshes every two seconds and shows the five stages, each analysis step with
  its outcome, each file at the moment it is created (input, blueprint draft, catalog metadata, final YAML, SQL, PySpark
  script, DAG), the findings recorded in the blueprint, a ZIP download, and a *Validate YAML Blueprint* action (the static
  checks always; the Spark-backed checks where Spark and Java are available).
  The *Stages and artifacts* selector shows the input code, each YAML stage, the generated Spark 3 code, the two side by
  side, the Spark 2 vs Spark 3 comparison, and *Run and Trino output*: after an explicit confirmation, the generated script
  is run with `spark-submit` on the UI host (master and Hive settings from `validation.cluster`; it writes to its target
  table), the result is saved in the run folder as `run_output_<stamp>.json`, and the target table is queried through Trino.
- **Run History**: reopens any earlier run folder under `output/` with its step history, files and findings.
- **Data Query (Hive / Trino)**: a connection form (defaults from the `query:` section of `config.yaml`; passwords are
  entered on the page or read from `HIVE_PASSWORD` / `TRINO_PASSWORD` and are never stored), a schema browser (databases or
  schemas, tables, table description, 100-row sample), an SQL editor with a row limit and time limit, and CSV download.
  Access is read-only by construction: exactly one SELECT, WITH, VALUES, SHOW, DESCRIBE or EXPLAIN statement is accepted per
  execution; write statements, multiple statements, `CALL`, and Hive `TRANSFORM` and `reflect` are rejected before they reach
  the server. Each query (text, row count, timing and errors, never the results) is appended to `output/query_log.jsonl`.
  Results are displayed on the page only. A read-only database account should also be used.

The UI is a thin layer over `agentic/jobs.py` (job runner), `agentic/events.py` (step events), `agentic/gitsource.py`
(read-only git input) and `agentic/query/` (guard + connectors), so a CLI or API can reuse them.

## Spark 2 vs Spark 3 comparison, and code execution

Deterministic tools (no LLM). Both are also registered in `agentic/code_tools.py` (`TOOLS`) for the tool-calling layer.

```
python -m agentic.spark_compare Claude_op/daily_txn_risk_job.py Claude_op/daily_txn_risk_job_spark3.py
```

Writes `output/daily_txn_risk_job_vs_daily_txn_risk_job_spark3_<date>_<time>/` with `compare_<stamp>.md` (step table,
API evidence, differences that can change the result), `compare_<stamp>.html` (side-by-side diff) and `compare_<stamp>.json`.

HTTP API:

```
python -m agentic.api --host 127.0.0.1 --port 8000        # docs at http://127.0.0.1:8000/docs
```

- `GET /api/health`: liveness and whether execution is enabled.
- `POST /api/compare`: body `{"spark2": {"code": "..."}, "spark3": {"path": "Claude_op/daily_txn_risk_job_spark3.py"}}`.
  A `path` must be inside the project folder.
- `POST /api/execute`: body `{"code": "...", "mode": "check" | "run", "runner": "python" | "spark-submit", "timeout_seconds": 120, "env": {}}`.
  Needs `Authorization: Bearer <token>`, where the token is the `FRAMEWORK_API_TOKEN` environment variable. Without it, the
  endpoint returns 503. Execution is not a sandbox: it runs the code with the API process's privileges.

Streamlit page for the same API (`ui/compare_app.py`). It calls the API over HTTP, so start the API first:

```
python -m streamlit run ui/compare_app.py --server.port 8502
```

Set the API address and the token in the sidebar, or with `MIGRATION_API_URL` and `FRAMEWORK_API_TOKEN`.

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
python -m agentic.validator --spec output/<input file>_<stamp>/pipeline_spec_<stamp>.yaml --source samples/legacy_job.py   # report goes into that run folder
python -m agentic.validator ... --agent offline     # scripted review loop, no network
python -m agentic.validator ... --agent llm         # LLM review (provider from config.yaml)
```

Writes `validation_report_<stamp>.md/.json` and `test_suite_<stamp>.json` (the minimized scenarios, with what each one kills) into the spec's run folder.
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

Tests (YAML verification only): `tests/test_validator.py` (injected conversion mistakes must be caught; Spark-backed
checks skip themselves where Spark/Java are missing) and `tests/test_engine_plans.py` (the engine's translation of the
YAML, using a Spark fake). Run: `python -m unittest discover -s tests -t . -v`. The other test files are parked in
`../agentic_framework_tests_parked/` and are not part of the project.

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

