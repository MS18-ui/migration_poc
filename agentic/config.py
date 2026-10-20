"""Loads config.yaml (path override: AGENTIC_CONFIG). Env vars take precedence over file values."""
import os
from typing import Any, Dict

import yaml

_DEFAULT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.yaml")


def load_config() -> Dict[str, Any]:
    path = os.getenv("AGENTIC_CONFIG", _DEFAULT_PATH)
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


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
