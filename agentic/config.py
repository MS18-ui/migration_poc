"""Loads config.yaml (path override: AGENTIC_CONFIG). Env vars take precedence over file values.

config.yaml is local and git-ignored (it can hold keys). The repo tracks `config.example.yaml` with every setting and no
secrets; when there is no config.yaml, that example is used, so a fresh clone works. To customise:
copy config.example.yaml to config.yaml and edit it."""
import os
from typing import Any, Dict

import yaml

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_PATH = os.path.join(_ROOT, "config.yaml")
_EXAMPLE_PATH = os.path.join(_ROOT, "config.example.yaml")


def load_config() -> Dict[str, Any]:
    path = os.getenv("AGENTIC_CONFIG") or (_DEFAULT_PATH if os.path.exists(_DEFAULT_PATH) else _EXAMPLE_PATH)
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


SECRET_WORDS = ("api_key", "password", "token", "secret")


def _is_secret_key(key: str) -> bool:
    k = key.lower()
    return not k.endswith("_env") and any(w in k for w in SECRET_WORDS)   # `password_env` names a variable: not secret


def _mask(node: Any, key: str = "") -> Any:
    if isinstance(node, dict):
        return {k: _mask(v, str(k)) for k, v in node.items()}
    if isinstance(node, list):
        return [_mask(v, key) for v in node]
    if _is_secret_key(key) and node not in (None, ""):
        return "********"
    return node


def masked_config() -> Dict[str, Any]:
    """The effective configuration file with every secret value replaced by ******** (for display only)."""
    return _mask(load_config())


def anthropic_settings() -> Dict[str, Any]:
    """Resolved Anthropic settings (config.yaml > env, since ANTHROPIC_* vars are often set
    ambiently by other tools); works for Azure Foundry via base_url."""
    cfg = load_config()
    a = cfg.get("anthropic", {}) or {}
    return {
        "base_url": a.get("base_url") or os.getenv("ANTHROPIC_BASE_URL") or None,
        "model": a.get("model") or os.getenv("ANTHROPIC_MODEL") or None,
        "api_key": (a.get("api_key") or os.getenv("ANTHROPIC_API_KEY")
                    or (cfg.get("azure_openai", {}) or {}).get("api_key") or None),
        "max_tokens": int(a.get("max_tokens", 4096)),
    }


def metadata_settings() -> Dict[str, Any]:
    """`metadata:` section of config.yaml: the catalog snapshot (file/folder) and explicit table aliases.
    (Unrelated to validation.baseline_table_map, which names the BASELINE script's tables.)"""
    m = dict(load_config().get("metadata", {}) or {})
    m.setdefault("snapshot", None)
    m["aliases"] = dict(m.get("aliases") or {})
    return m


def skills_settings() -> Dict[str, Any]:
    """`skills:` section of config.yaml (Type Exchange skills used as ingestion-agent prompts)."""
    cfg = dict(load_config().get("skills", {}) or {})
    cfg.setdefault("enabled", True)
    cfg.setdefault("max_corrections", 2)  # PLAN.md: at most two correction attempts after the first draft
    return cfg


def validator_settings() -> Dict[str, Any]:
    """`validator:` section of config.yaml (Spark session used by the validation agent)."""
    cfg = dict(load_config().get("validator", {}) or {})
    spark = dict(cfg.get("spark", {}) or {})
    spark.setdefault("master", "local[2]")
    spark["conf"] = dict(spark.get("conf") or {})
    cfg["spark"] = spark
    return cfg


def validation_settings() -> Dict[str, Any]:
    """`validation:` section of config.yaml (run-and-diff of generated code)."""
    return dict(load_config().get("validation", {}) or {})


def ui_settings() -> Dict[str, Any]:
    """`ui:` section of config.yaml (the Streamlit app)."""
    u = dict(load_config().get("ui", {}) or {})
    u.setdefault("out_dir", os.getenv("AGENTIC_OUT_DIR", "output"))
    u.setdefault("max_parallel_jobs", 2)
    return u


def query_settings() -> Dict[str, Any]:
    """`query:` section of config.yaml: defaults for the UI's read-only Hive / Trino query tab. Passwords are never read
    from the file: `password_env` names the environment variable that holds one."""
    q = dict(load_config().get("query", {}) or {})
    q.setdefault("row_limit", 1000)
    q.setdefault("max_row_limit", 10000)
    q.setdefault("timeout_seconds", 120)
    q.setdefault("log", os.path.join(ui_settings()["out_dir"], "query_log.jsonl"))
    q["hive"] = {"host": "", "port": 10000, "username": "", "auth": "NONE", "database": "default",
                 "password_env": "HIVE_PASSWORD", **(q.get("hive") or {})}
    q["trino"] = {"host": "", "port": 8080, "user": "", "catalog": "hive", "schema": "default", "http_scheme": "http",
                  "verify": True, "password_env": "TRINO_PASSWORD", **(q.get("trino") or {})}
    return q


def azure_settings() -> Dict[str, Any]:
    """Resolved Azure OpenAI settings (env > config.yaml). Empty values are treated as unset."""
    cfg = load_config().get("azure_openai", {}) or {}
    pick = lambda env, key: os.getenv(env) or cfg.get(key) or None
    return {
        "endpoint": pick("AZURE_OPENAI_ENDPOINT", "endpoint"),
        "deployment": pick("AZURE_OPENAI_DEPLOYMENT", "deployment"),
        "api_key": pick("AZURE_OPENAI_API_KEY", "api_key"),
        "api_version": str(v) if (v := pick("AZURE_OPENAI_API_VERSION", "api_version")) else None,
        "max_tokens": int(cfg.get("max_tokens", 4096)),
    }
