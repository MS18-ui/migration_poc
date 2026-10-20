import ast
import re

import yaml

from .blueprint_checks import check_findings, check_rich_blueprint
from .schemas import PipelineBlueprint


def strip_fences(text: str) -> str:
    """Remove ```yaml / ```python / ```sql / ``` fences if the model added them."""
    return re.sub(r"```(?:yaml|python|py|sql)?\s*\n?|```", "", text).strip()


class _BlueprintLoader(yaml.SafeLoader):
    """SafeLoader that keeps on/off/yes/no as strings (YAML 1.1 would turn a join's
    `on:` key into boolean True). Only true/false remain booleans."""


_BlueprintLoader.yaml_implicit_resolvers = {
    ch: [(tag, rx) for tag, rx in rs if tag != "tag:yaml.org,2002:bool"]
    for ch, rs in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
_BlueprintLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool", re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"), list("tTfF"))


def load_yaml(text: str):
    return yaml.load(text, Loader=_BlueprintLoader)


class ValidationEngine:
    # ---------- YAML blueprint ----------
    @staticmethod
    def validate_yaml_structure(yaml_string: str) -> PipelineBlueprint:
        raw = load_yaml(strip_fences(yaml_string))
        if not isinstance(raw, dict):
            raise ValueError("YAML root must be a mapping.")
        return PipelineBlueprint(**raw)

    @staticmethod
    def validate_logical_dag(blueprint: PipelineBlueprint) -> None:
        if not blueprint.steps and not blueprint.is_rich:
            raise ValueError("Pipeline has no steps (or sources/transformations/targets).")
        problems = check_findings(blueprint)
        if problems:
            raise ValueError("Invalid findings:\n- " + "\n- ".join(problems[:15]))
        if blueprint.is_rich:
            check_rich_blueprint(blueprint)
        seen, ids = set(), set()
        for step in blueprint.steps:
            if step.name in seen:
                raise ValueError(f"Duplicate step name '{step.name}'.")
            if step.step_id in ids:
                raise ValueError(f"Duplicate step_id {step.step_id}.")
            if step.type != "source" and not (step.source or step.depends_on):
                raise ValueError(f"Step '{step.name}' is a {step.type} but has no 'source' or 'depends_on'.")
            if step.source and step.source not in seen:
                raise ValueError(
                    f"Step '{step.name}' references upstream source '{step.source}' "
                    f"which does not exist in preceding steps."
                )
            for dep in step.depends_on:
                if dep not in seen:
                    raise ValueError(f"Step '{step.name}' depends_on unknown/later step '{dep}'.")
            seen.add(step.name)
            ids.add(step.step_id)

    # ---------- Python ----------
    @staticmethod
    def validate_code_syntax(code: str) -> None:
        ast.parse(strip_fences(code))

    @staticmethod
    def validate_step_coverage(code: str, blueprint: PipelineBlueprint) -> None:
        missing = [n for n in blueprint.node_names() if n not in code]
        if missing:
            raise ValueError(
                f"Code does not reference these blueprint steps "
                f"(use df_<step_name> variables): {missing}"
            )

    @staticmethod
    def validate_airflow_dag(code: str, blueprint: PipelineBlueprint) -> None:
        code = strip_fences(code)
        tree = ast.parse(code)
        if "airflow" not in code:
            raise ValueError("No airflow imports found.")
        if "DAG(" not in code:
            raise ValueError("No DAG(...) definition found.")
        missing = [n for n in blueprint.task_names() if f"'{n}'" not in code and f'"{n}"' not in code]
        if missing:
            raise ValueError(f"These steps have no task (task_id must equal step name): {missing}")
        has_deps = any(s.source or s.depends_on for s in blueprint.steps)
        if has_deps and ">>" not in code and "set_downstream" not in code and "set_upstream" not in code:
            raise ValueError("Steps have dependencies but no task dependencies (>>) are defined.")
        _ = tree

    # ---------- SQL ----------
    @staticmethod
    def validate_sql(sql: str, dialect: str = "spark") -> None:
        sql = strip_fences(sql)
        if not sql.strip():
            raise ValueError("Empty SQL.")
        try:
            import sqlglot  # optional but preferred
        except ImportError:
            sqlglot = None
        if sqlglot is not None:
            try:
                sqlglot.parse(sql, read=dialect)
            except Exception as e:  # sqlglot.errors.ParseError / TokenError
                raise ValueError(f"SQL parse error: {e}")
            return
        # Fallback heuristics when sqlglot is not installed
        if sql.count("(") != sql.count(")"):
            raise ValueError("Unbalanced parentheses.")
        first = re.match(r"\s*(\w+)", sql)
        if not first or first.group(1).upper() not in {
            "SELECT", "WITH", "INSERT", "CREATE", "DROP", "ALTER", "SET", "USE", "MSCK", "LOAD", "WITH",
        }:
            raise ValueError("SQL does not start with a recognised statement keyword.")
