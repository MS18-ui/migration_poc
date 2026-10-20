"""Human-readable YAML for the files a person is meant to read (pipeline specification, blueprint draft, traceability).

What it does to the machine-oriented dictionaries before writing them:
  * drops empty values (None, "", [], {}) and constants that carry no information (a finding's status when it is 'open')
  * puts keys in a logical reading order (identifier and type first, notes last; findings: severity before evidence)
  * never folds a SQL/Spark expression across lines; long expressions become a literal block, long prose a folded block
  * writes the join key `on:` plainly instead of the quoted YAML-1.1 workaround `'on':`

The result loads back to the same data with agentic.validation.load_yaml (which treats on/off/yes/no as strings), and
`dump_yaml` verifies that round trip itself, falling back to a plain dump if it ever differs.
"""
import re
from typing import Any, Dict, List

import yaml

TOP_ORDER = ["name", "source_system", "target_engine", "schedule", "job", "sources", "transformations", "targets",
             "findings", "lineage", "steps", "global_configs"]
ITEM_ORDER = {
    "sources": ["id", "type", "object", "path", "pre_filter", "schema", "catalog"],
    "transformations": ["id", "type", "inputs", "filter", "dedupe_keys", "joins", "columns", "window", "group_by",
                        "aggregations", "select"],
    "targets": ["id", "input", "type", "object", "format", "mode", "method", "partition_by", "schema", "catalog"],
    "findings": ["id", "severity", "category", "blocks", "source_ref", "affected_refs", "evidence", "resolution", "confidence"],
}
_LAST = ("notes",)
_EXPR_KEYS = {"expr", "filter", "on", "sql", "condition", "when", "post_filter", "pre_filter", "frame"}
_PROSE_KEYS = {"evidence", "resolution", "notes", "use_case", "description", "summary"}
_LONG = 110


class _Lit(str):
    """Written as a literal block (|-) so an expression is never folded."""


class _Fold(str):
    """Written as a folded block (>-) so long prose wraps at word boundaries."""


class HumanDumper(yaml.SafeDumper):
    pass


HumanDumper.yaml_implicit_resolvers = {
    ch: [(tag, rx) for tag, rx in rs if tag != "tag:yaml.org,2002:bool"]
    for ch, rs in yaml.SafeDumper.yaml_implicit_resolvers.items()
}
HumanDumper.add_implicit_resolver("tag:yaml.org,2002:bool", re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"), list("tTfF"))
HumanDumper.add_representer(_Lit, lambda d, s: d.represent_scalar("tag:yaml.org,2002:str", str(s), style="|"))
HumanDumper.add_representer(_Fold, lambda d, s: d.represent_scalar("tag:yaml.org,2002:str", str(s), style=">"))


def prune(obj: Any) -> Any:
    """Recursively remove None / "" / [] / {} (False and 0 are real values and are kept)."""
    if isinstance(obj, dict):
        out = {k: prune(v) for k, v in obj.items()}
        return {k: v for k, v in out.items() if v is not None and v != "" and v != [] and v != {}}
    if isinstance(obj, list):
        return [prune(v) for v in obj]
    return obj


def _reorder(d: Dict[str, Any], first: List[str], last: tuple = _LAST) -> Dict[str, Any]:
    keys = [k for k in first if k in d]
    keys += [k for k in d if k not in first and k not in last]
    keys += [k for k in last if k in d]
    return {k: d[k] for k in keys}


def _drop_constants(f: Dict[str, Any]) -> Dict[str, Any]:
    f = dict(f)
    if f.get("status") == "open":      # every finding is open until a human reviewer decides otherwise
        f.pop("status")
    return f


def _prepare(obj: Any, key: str = "") -> Any:
    if isinstance(obj, dict):
        return {k: _prepare(v, k) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_prepare(v, key) for v in obj]
    if isinstance(obj, str):
        if "\n" in obj:
            return _Lit(obj)
        if len(obj) > _LONG and key in _EXPR_KEYS:
            return _Lit(obj)
        if len(obj) > _LONG and key in _PROSE_KEYS:
            return _Fold(obj)
    return obj


def humanize(d: Dict[str, Any]) -> Dict[str, Any]:
    """The cleaned, reordered dictionary (no YAML styling yet)."""
    d = prune(d)
    d = _reorder(d, TOP_ORDER, last=())
    for sect, order in ITEM_ORDER.items():
        if isinstance(d.get(sect), list):
            items = [x if not isinstance(x, dict) else _reorder(x, order) for x in d[sect]]
            if sect == "findings":
                items = [_drop_constants(x) for x in items]
            d[sect] = items
    return d


def dump_yaml(d: Dict[str, Any]) -> str:
    """Readable YAML text for `d`; verified to load back to the same data (else a plain dump is returned)."""
    clean = humanize(d)
    text = yaml.dump(_prepare(clean), Dumper=HumanDumper, sort_keys=False, allow_unicode=True,
                     default_flow_style=False, width=100)
    try:
        from .validation import load_yaml
        if load_yaml(text) == clean:
            return text
    except Exception:
        pass
    return yaml.safe_dump(clean, sort_keys=False, allow_unicode=True, default_flow_style=False, width=1000)
