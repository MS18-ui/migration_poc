"""
Agent factories. Every agent = system prompt + ToolRegistry (parsers/validators as
tools + submit_final). Stages follow the architecture diagram:

  Oozie / PySpark2 / Sqoop / Excel --(ingestion agent)--> YAML
  Hive QL --(hive_to_spark agent)--> Spark QL --(ingestion agent)--> YAML
  YAML --(pyspark agent)--> PySpark 3.x     YAML --(airflow agent)--> Airflow DAG
"""
from typing import Callable, Dict, List

import yaml
from pydantic import ValidationError

from .agent import ToolAgent
from .config import skills_settings, validation_settings
from .llm import BaseLLM
from .parsers import parse_oozie_workflow, parse_sqoop_command, scan_pyspark2
from .runcheck import code_is_verified, make_runcheck_tools, spark_available
from .schemas import PipelineBlueprint
from .skills_loader import compose_prompt
from .tools import ToolRegistry, ToolSpec
from .validation import ValidationEngine, strip_fences

BLUEPRINT_SPEC = (
    "YAML keys: name, source_system (oozie|pyspark2|sqoop|hiveql|excel), "
    "target_engine (pyspark_3.x|pyspark_2.x), schedule (cron/preset or omit), global_configs, "
    "steps[]. Each step: step_id (unique int), name (unique), type (source|transformation|sink), "
    "action (e.g. read_parquet, sqoop_import, cast_column, spark_sql, write_table), "
    "path, source (name of an EARLIER step for data lineage), depends_on (list of EARLIER step "
    "names for orchestration-only ordering), details{column,target_type,date_format,condition}, "
    "params (string->string map for source-specific settings such as jdbc url, table, queries). "
    "Every non-source step needs source or depends_on. Output the whole YAML in the tool call."
)

RICH_SPEC = (
    "Use the RICH blueprint (do NOT use `steps`). YAML keys: "
    "job{name, domain, use_case, engine, engine_version, source_file, parameters{name: value}, spark_conf}; "
    "sources[]{id, type (hive_table|parquet|orc|csv|json|avro|jdbc|iceberg_table|delta), object (db.table) "
    "or path, pre_filter (may use :param), schema[]}; "
    "transformations[]{id, type, inputs[] (ids of sources or EARLIER transformations), ...}; "
    "targets[]{id, input, type (hive_table|iceberg_table|parquet|...), object, format, mode "
    "(append|overwrite|overwrite_dynamic_partition|merge), method, partition_by[], schema[], properties{}}; "
    "lineage[]{target_column, from[], via[] (transformation ids)}. "
    "Schema fields: {name, type} where type is a Spark type string ('DECIMAL(18,2)', 'ARRAY<STRING>', "
    "'MAP<STRING,DOUBLE>', 'STRUCT<a:INT,b:STRING>') or bare STRUCT (+fields[]) / ARRAY (+element_type, "
    "+fields[] for struct elements) / MAP (+key_type, value_type, known_keys[]); optional rename_to. "
    "Transformation types and their keys: select{select}; filter{filter}; derive_columns / nested_derive"
    "{columns[{name, expr, op?}], select?}; filter_dedupe_flatten{filter?, dedupe_keys?, columns?}; "
    "join{joins[{left (input id or '*' for the running result), right, type, on (list of columns or a "
    "condition string), hint?}]}; join_derive{joins, columns}; window{window{partition_by, order_by, frame}, "
    "columns}; aggregate{group_by, aggregations[{name, expr}]}; explode_aggregate{explode{column, alias}, "
    "group_by, aggregations}; build_array_of_struct{output_column, element_schema, rules[], post_filter}; "
    "collect_to_array_of_struct{group_by, output{name, expr, type}}; collect_to_map{steps[], output_type}; "
    "spark_sql{sql}; custom{sql or notes}. Write every `expr`/`filter` as a Spark 3 SQL expression "
    "(higher-order functions allowed), reference job parameters as :name, and preserve ALL logic - "
    "thresholds, window frames, join hints, null handling, casts. Put migration caveats in `notes`. "
    "QUOTE any YAML scalar containing [ ] { } : # or starting with * (e.g. \"curated.customers.addresses[].country\")."
)

# Per-source extra instructions + deterministic parser tools -----------------------------
_SOURCE_PROFILES: Dict[str, Dict] = {
    "excel": {
        "hint": "Input is an Excel structural-metadata map rendered as text.",
        "tools": [],
    },
    "oozie": {
        "hint": (
            "Input is an Oozie workflow.xml. Call parse_oozie to extract nodes, then map each "
            "action to a step. Preserve ordering via depends_on (ok transitions), represent "
            "fork/join as parallel steps sharing the same dependency, and capture the "
            "coordinator schedule if present."
        ),
        "tools": [("parse_oozie", "Parse Oozie workflow.xml into nodes/transitions (pass raw XML).",
                   lambda a: parse_oozie_workflow(a["content"]))],
    },
    "sqoop": {
        "hint": (
            "Input is one or more Sqoop commands. Call parse_sqoop to extract flags. Map "
            "`sqoop import` to a source step (action sqoop_import; put connect, table/query, "
            "target-dir, num-mappers, split-by in params) and `sqoop export` to a sink step."
        ),
        "tools": [("parse_sqoop", "Parse sqoop import/export commands into flag dicts (pass raw text).",
                   lambda a: parse_sqoop_command(a["content"]))],
    },
    "pyspark2": {
        "hint": (
            "Input is legacy PySpark 2.x code. Call scan_pyspark2 to list constructs needing "
            "migration. Model the job with the rich blueprint: one source per spark.table/read, one "
            "transformation per logical stage (keep nested-type logic: struct/map/array access, "
            "higher-order functions, windows, explode/collect), one target per write, plus column "
            "lineage for derived output columns. Record migration issues in `notes`. Set "
            "target_engine to pyspark_3.x."
        ),
        "rich": True,
        "tools": [("scan_pyspark2", "Scan PySpark 2.x / Python 2 code for Spark 3 migration issues.",
                   lambda a: scan_pyspark2(a["content"]))],
    },
    "hiveql": {
        "hint": (
            "Input is Spark SQL already converted from Hive QL. Model it with the rich blueprint: "
            "tables read -> sources (schema from CREATE TABLE if present), each CTE/stage -> a "
            "transformation (typed where it fits, otherwise spark_sql with the SQL in `sql`), "
            "INSERT OVERWRITE/CREATE TABLE targets -> targets (partition_by from PARTITIONED BY). "
            "Turn ${hivevar:x} / SET hivevar values into job.parameters used as :x."
        ),
        "rich": True,
        "tools": [],
    },
}


def _count_failure(holder: Dict, error: str) -> str:
    """Enforce 'initial draft + at most N correction attempts' (PLAN.md section 1; skills: bounded repair loop).
    After the limit the agent loop stops with the unresolved problems, which then need manual review."""
    max_c = holder.get("max_corrections")
    holder["failures"] = holder.get("failures", 0) + 1
    holder["last_error"] = error
    if max_c is not None and holder["failures"] > max_c:
        holder["agent"].abort = (f"validation still failing after the first draft and {max_c} correction attempt(s); "
                                 f"unresolved: {error[:700]}")
        return error + f"\n\nCORRECTION LIMIT REACHED ({max_c}); no further attempts are allowed."
    left = None if max_c is None else max_c - holder["failures"] + 1
    return error + ("" if left is None else f"\n\n({left} correction attempt(s) left.)")


def _structured_tools(holder: Dict) -> List[ToolSpec]:
    def validate(args: dict) -> str:
        try:
            bp = ValidationEngine.validate_yaml_structure(args["content"])
            ValidationEngine.validate_logical_dag(bp)
            return f"OK: valid blueprint '{bp.name}' with {len(bp.node_names())} nodes."
        except (ValidationError, ValueError, yaml.YAMLError) as e:
            return _count_failure(holder, f"ERROR: {e}")

    def submit(args: dict) -> str:
        result = validate(args)
        if result.startswith("OK"):
            holder["agent"].final = ValidationEngine.validate_yaml_structure(args["content"])
        return result

    return [
        ToolSpec("validate_yaml", "Validate YAML against the schema and DAG rules.", validate),
        ToolSpec("submit_final", "Submit the final YAML. Only accepted if valid.", submit),
    ]


def build_ingestion_agent(llm: BaseLLM, source_type: str = "excel", verbose: bool = True) -> ToolAgent:
    """Source-specific ingestion agent: legacy artefact -> validated YAML blueprint."""
    profile = _SOURCE_PROFILES[source_type]
    cfg = skills_settings()
    holder: Dict = {"max_corrections": cfg["max_corrections"] if cfg["enabled"] else None}
    tools = [ToolSpec(n, d, f) for n, d, f in profile["tools"]] + _structured_tools(holder)
    role_spec = (f"Convert the input into a pipeline blueprint. {profile['hint']} "
                 f"{RICH_SPEC if profile.get('rich') else BLUEPRINT_SPEC} "
                 "Always call validate_yaml before submit_final and fix every error it reports.")
    skill_prompt = (compose_prompt(source_type, role_spec, [t.name for t in tools], cfg["max_corrections"])
                    if cfg["enabled"] else None)
    system = skill_prompt or f"You are the {source_type.upper()} Ingestion Agent. {role_spec}"
    agent = ToolAgent(f"Ingestion:{source_type}", llm, system, ToolRegistry(tools), verbose=verbose)
    agent.skill_prompt_used = skill_prompt is not None
    holder["agent"] = agent
    return agent


def build_hive_to_spark_agent(llm: BaseLLM, verbose: bool = True) -> ToolAgent:
    """Hive QL -> Spark SQL translation agent (validated before reaching the YAML stage)."""
    cfg = skills_settings()
    holder: Dict = {"max_corrections": cfg["max_corrections"] if cfg["enabled"] else None}

    def check(args: dict) -> str:
        try:
            ValidationEngine.validate_sql(args["content"], "spark")
            return "OK: valid Spark SQL."
        except ValueError as e:
            return _count_failure(holder, f"ERROR: {e}")

    def submit(args: dict) -> str:
        r = check(args)
        if r.startswith("OK"):
            holder["agent"].final = strip_fences(args["content"])
        return r

    role_spec = (
        "Translate Hive QL to Spark 3 SQL: replace Hive-only syntax (e.g. hivevar/hiveconf substitution, TRANSFORM "
        "scripts, deprecated functions, MAPJOIN hints -> BROADCAST), keep semantics identical, keep statements "
        "separated by semicolons. Always call check_sql before submit_final and fix errors.")
    tools = [
        ToolSpec("check_sql", "Validate Spark SQL syntax.", check),
        ToolSpec("submit_final", "Submit final Spark SQL. Only accepted if valid.", submit),
    ]
    skill_prompt = (compose_prompt("hiveql", role_spec, [t.name for t in tools], cfg["max_corrections"], deliverable="sql")
                    if cfg["enabled"] else None)
    system = skill_prompt or f"You are the Hive QL -> Spark SQL Agent. {role_spec}"
    agent = ToolAgent("HiveToSpark", llm, system, ToolRegistry(tools), verbose=verbose)
    agent.auto_submit = True
    agent.skill_prompt_used = skill_prompt is not None
    holder["agent"] = agent
    return agent


def build_pyspark_agent(llm: BaseLLM, blueprint: PipelineBlueprint, verbose: bool = True) -> ToolAgent:
    """YAML -> PySpark 3.x script."""
    holder: Dict = {}

    def check(args: dict) -> str:
        try:
            ValidationEngine.validate_code_syntax(args["content"])
            ValidationEngine.validate_step_coverage(args["content"], blueprint)
            return "OK: syntax valid and all blueprint steps covered."
        except SyntaxError as e:
            return f"ERROR: SyntaxError: {e.msg} at line {e.lineno}"
        except ValueError as e:
            return f"ERROR: {e}"

    rc_tools = _runcheck_tools(blueprint, holder, verbose)
    require_pass = bool(rc_tools) and validation_settings().get("require_pass", True)

    def submit(args: dict) -> str:
        r = check(args)
        if r.startswith("OK") and require_pass and not holder.get("runcheck_unavailable") \
                and not code_is_verified(holder, args["content"]):
            return ("ERROR: this exact code has not passed " +
                    "/".join(t.name for t in rc_tools) + " yet. Run it, fix any problems, then submit.")
        if r.startswith("OK"):
            holder["agent"].final = strip_fences(args["content"])
        return r

    system = (
        "You are the PySpark Generation Agent. Translate the YAML blueprint into a complete, "
        "runnable PySpark 3.x script. Name each node's DataFrame df_<id> (flat steps: df_<step_name>), apply "
        "global_configs on the SparkSession builder, implement sqoop_import steps with "
        "spark.read.jdbc, spark_sql steps with spark.sql, and include run_pipeline() plus a "
        "__main__ guard. For a rich blueprint: read each source with its schema and pre_filter, "
        "implement each transformation exactly from its expr/columns/joins/window/aggregations "
        "(F.expr for SQL expressions), bind job.parameters to a run_date-style variable that can be "
        "overridden from the command line, and write each target honoring format, mode, method and "
        "partition_by (iceberg_table targets: df.writeTo(object) / MERGE INTO). "
        "Accept every job parameter as a command-line flag named --<param with _ replaced by ->, "
        "e.g. run_date -> --run-date. The script file is named generated_pipeline.py. "
        "Mark the sections of the module-level code with these comment lines, in this order, so a reviewer can match "
        "each section with the source: '# ---------- SOURCES ----------' before the sources are read; "
        "'# ---------- T1: <short title> ----------', '# ---------- T2: <short title> ----------' and so on, one per "
        "transformation of the blueprint in order (T1 first); '# ---------- TARGET ----------' before the write. "
        "Do not indent these lines and do not put them inside functions. "
        "Always call check_code before submit_final and fix errors." +
        (" Then call " + " and ".join(t.name for t in rc_tools) + " (they execute the script on test data "
         "and compare it with a trusted baseline); fix every reported error or difference, and "
         "submit_final only the exact code that passed." if rc_tools else "")
    )
    tools = [
        ToolSpec("check_code", "Check Python syntax and that every blueprint step is implemented.", check),
        *rc_tools,
        ToolSpec("submit_final", "Submit the final script. Only accepted if it passes checks.", submit),
    ]
    agent = ToolAgent("PySparkGen", llm, system, ToolRegistry(tools), verbose=verbose,
                      max_turns=16 if rc_tools else 10)
    agent.auto_submit = True
    holder["agent"] = agent
    return agent


def _runcheck_tools(blueprint: PipelineBlueprint, holder: Dict, verbose: bool) -> List[ToolSpec]:
    """Run/diff tools for rich blueprints when enabled and Spark + Java are present."""
    cfg = validation_settings()
    if not (cfg.get("enabled") and blueprint.is_rich):
        return []
    why = spark_available()
    if why:
        if verbose:
            print(f"   [PySparkGen] run-and-diff validation skipped: {why}")
        return []
    tools = make_runcheck_tools(blueprint, cfg, holder)
    if verbose:
        for note in holder.get("runcheck_notes", []):
            print(f"   [PySparkGen] {note}")
    return tools


# backwards-compatible alias
build_generation_agent = build_pyspark_agent


def build_airflow_agent(llm: BaseLLM, blueprint: PipelineBlueprint, verbose: bool = True) -> ToolAgent:
    """YAML -> Airflow DAG (one task per step, task_id == step name)."""
    holder: Dict = {}

    def check(args: dict) -> str:
        try:
            ValidationEngine.validate_airflow_dag(args["content"], blueprint)
            return "OK: valid Airflow DAG covering all steps."
        except SyntaxError as e:
            return f"ERROR: SyntaxError: {e.msg} at line {e.lineno}"
        except ValueError as e:
            return f"ERROR: {e}"

    def submit(args: dict) -> str:
        r = check(args)
        if r.startswith("OK"):
            holder["agent"].final = strip_fences(args["content"])
        return r

    system = (
        "You are the Airflow Generation Agent. Produce an Apache Airflow 2.x DAG file from the "
        "YAML blueprint: one task per flat step with task_id exactly equal to the step name "
        "(a rich blueprint is ONE Spark task with task_id equal to its job name, "
        "application='generated_pipeline.py', passing each job parameter as a flag named "
        "--<param with _ replaced by ->, e.g. application_args=['--run-date', '{{ ds }}']), "
        "dependencies from `source` and `depends_on` using >>, schedule from the blueprint "
        "(schedule=None if absent), catchup=False. Use SparkSubmitOperator (or BashOperator "
        "invoking generated_pipeline.py) for spark steps. Always call check_dag before "
        "submit_final and fix errors."
    )
    tools = [
        ToolSpec("check_dag", "Validate the Airflow DAG file against the blueprint.", check),
        ToolSpec("submit_final", "Submit the final DAG file. Only accepted if valid.", submit),
    ]
    agent = ToolAgent("AirflowGen", llm, system, ToolRegistry(tools), verbose=verbose)
    agent.auto_submit = True
    holder["agent"] = agent
    return agent


