"""Catalog metadata snapshot: authoritative physical schemas for the tables a job reads and writes.

The Type Exchange skills expect a supplied metadata snapshot and forbid inventing schemas or inferring
destinations from naming. This module:
  * loads a snapshot from Hive DDL (.hql/.sql), canonical YAML/JSON, or a folder of those
  * matches a job's tables/paths to catalog tables (exact name, explicit alias, or a table's LOCATION - never a guess)
  * resolves a blueprint against it DETERMINISTICALLY (no model): fills missing source schemas, corrects declared
    types to the catalog's, and records findings for unknown tables/columns, type conflicts and target drift
  * renders the relevant tables as a prompt block so the model starts from the real schemas

Inspect or export:  python -m agentic.metadata <file-or-folder> [...] [--out snapshot.yaml]
"""
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
import yaml
from sqlglot import exp

from .schemas import PipelineBlueprint, SchemaField
from .sqlguard import split_statements
from .testdata import field_type_str

_EXTS = (".hql", ".sql", ".ddl", ".yaml", ".yml", ".json")


def _norm(t: str) -> str:
    return re.sub(r'[`"\s]', "", t).lower()


def _clean_type(t: str) -> str:
    return re.sub(r"\s*([<>:,()])\s*", r"\1", t.strip())


def _loc_keys(loc: Optional[str]) -> List[str]:
    if not loc:
        return []
    full = loc.strip().rstrip("/").lower()
    return list({full, re.sub(r"^\w+://[^/]*", "", full)})


@dataclass
class ColumnMeta:
    name: str
    type: str
    partition: bool = False
    comment: str = ""


@dataclass
class TableMeta:
    name: str                      # db.table, lower case
    columns: List[ColumnMeta] = field(default_factory=list)   # includes partition columns, flagged
    kind: str = "table"            # table | view
    format: Optional[str] = None
    location: Optional[str] = None
    external: bool = False
    properties: Dict[str, str] = field(default_factory=dict)
    source: str = ""               # file it came from

    @property
    def partition_columns(self) -> List[str]:
        return [c.name for c in self.columns if c.partition]

    def column(self, name: str) -> Optional[ColumnMeta]:
        return next((c for c in self.columns if c.name.lower() == name.lower()), None)


class Snapshot:
    def __init__(self, tables: Dict[str, TableMeta], files: List[str], digest: str, warnings: List[str]):
        self.tables, self.files, self.id, self.warnings = tables, files, digest, warnings
        self._by_loc: Dict[str, TableMeta] = {}
        for t in tables.values():
            for k in _loc_keys(t.location):
                self._by_loc[k] = t

    # ---- lookup -------------------------------------------------------------------------
    def lookup(self, key: str, aliases: Optional[Dict[str, str]] = None) -> Tuple[Optional[TableMeta], List[str]]:
        """Exact name, explicit alias, or a table's LOCATION. Never guesses: near misses are returned as candidates."""
        k = (key or "").strip().lower()
        al = {a.lower(): b.lower() for a, b in (aliases or {}).items()}
        if k in self.tables:  # an identical name is unambiguous and always wins
            return self.tables[k], []
        if k in al:
            t = self.tables.get(al[k])
            return (t, []) if t else (None, [f"the alias maps '{key}' to '{al[k]}', which is not in the snapshot"])
        if "/" in k:
            for lk in _loc_keys(k):
                if lk in self._by_loc:
                    return self._by_loc[lk], []
        last = k.rstrip("/").split(".")[-1].split("/")[-1]
        return None, sorted(n for n in self.tables if n.split(".")[-1] == last)

    # ---- model prompt block ---------------------------------------------------------------
    def prompt_block(self, source_text: str, aliases: Optional[Dict[str, str]] = None, max_chars: int = 9000) -> str:
        """The tables the source mentions (by name, alias or location), with full column types."""
        text = source_text.lower()
        al = {a.lower(): b.lower() for a, b in (aliases or {}).items()}
        rev: Dict[str, List[str]] = {}
        for a, b in al.items():
            rev.setdefault(b, []).append(a)
        lines, used = [], 0
        for name in sorted(self.tables):
            t = self.tables[name]
            names = [name] + rev.get(name, []) + [k for k in _loc_keys(t.location) if "/" in k]
            hits = [n for n in names if n in text]
            if not hits:
                continue
            via = "" if name in hits else f"  (the job calls it '{hits[0]}')"
            part = f" partitioned by ({', '.join(t.partition_columns)})" if t.partition_columns else ""
            fmt = f" [{t.kind}{', ' + t.format if t.format else ''}{', external' if t.external else ''}]"
            loc = f" location {t.location}" if t.location else ""
            cols = ", ".join(f"{c.name} {c.type}{' (partition)' if c.partition else ''}" for c in t.columns)
            entry = f"table {name}{fmt}{part}{loc}{via}\n  columns: {cols}"
            if used + len(entry) > max_chars:
                lines.append(f"... (catalog block truncated at {max_chars} characters; remaining tables omitted)")
                break
            lines.append(entry)
            used += len(entry)
        if not lines:
            return (f"CATALOG METADATA (snapshot {self.id}): none of the snapshot's tables is mentioned in this source. "
                    "No schemas are available for it; do not invent them.")
        return (f"CATALOG METADATA (snapshot {self.id}; authoritative for physical schemas). Use these names, types and "
                "partition columns EXACTLY for every table listed; never invent or alter columns. Declare only the columns "
                "the job uses (keep `rename_to` where the source renames a column). Report any column the source uses "
                "that is missing here as a metadata finding.\n" + "\n".join(lines))

    # ---- export ---------------------------------------------------------------------------
    def to_dict(self, only: Optional[set] = None) -> Dict[str, Any]:
        """Canonical dictionary; `only` (a set of table names) restricts it to those tables."""
        return {"snapshot": {"id": self.id, "files": self.files},
                "tables": {n: {k: v for k, v in {
                    "kind": t.kind, "format": t.format, "location": t.location, "external": t.external or None,
                    "partition_columns": t.partition_columns or None, "properties": t.properties or None,
                    "columns": [{k2: v2 for k2, v2 in {"name": c.name, "type": c.type, "partition": c.partition or None,
                                                       "comment": c.comment or None}.items() if v2 is not None}
                                for c in t.columns]}.items() if v is not None}
                           for n, t in sorted(self.tables.items()) if only is None or n in only}}


# ---- loading -------------------------------------------------------------------------------
def parse_ddl(text: str, source: str = "", default_db: Optional[str] = None) -> Tuple[List[TableMeta], List[str]]:
    tables, warns, cur_db = [], [], default_db
    for stmt in split_statements(re.sub(r"\$\{hivevar:(\w+)\}", r"\1", text)):
        s = stmt.strip()
        if not s:
            continue
        m = re.match(r"(?i)USE\s+`?(\w+)`?", s)
        if m:
            cur_db = m.group(1).lower()
            continue
        if not re.match(r"(?is)CREATE\s+(?:EXTERNAL\s+)?(?:TEMPORARY\s+)?(?:TABLE|VIEW)\b", s):
            continue
        try:
            tree = sqlglot.parse_one(s, read="hive")
        except Exception as e:
            warns.append(f"{source}: could not parse '{s[:50]}...' ({str(e).splitlines()[0][:80]})")
            continue
        if not isinstance(tree, exp.Create):
            continue
        kind = str(tree.args.get("kind") or "").upper()
        target = tree.this
        tbl = target.this if isinstance(target, exp.Schema) else target
        if not isinstance(tbl, exp.Table) or not tbl.name:
            continue
        name = (f"{(tbl.db or cur_db or '').lower()}.{tbl.name.lower()}").lstrip(".")
        if kind == "VIEW":
            tables.append(TableMeta(name, kind="view", source=source))
            continue
        if not isinstance(target, exp.Schema):
            continue  # CREATE TABLE AS SELECT: no declared columns
        cols = [ColumnMeta(c.name, _clean_type(c.args["kind"].sql(dialect="spark")),
                           comment=_comment_of(c)) for c in target.expressions if isinstance(c, exp.ColumnDef)]
        meta = TableMeta(name, cols, source=source)
        for p in (tree.args.get("properties").expressions if tree.args.get("properties") else []):
            ptxt = p.sql(dialect="hive")
            if isinstance(p, exp.PartitionedByProperty) and isinstance(p.this, exp.Schema):
                meta.columns += [ColumnMeta(c.name, _clean_type(c.args["kind"].sql(dialect="spark")), True, _comment_of(c))
                                 for c in p.this.expressions if isinstance(c, exp.ColumnDef)]
            elif isinstance(p, exp.FileFormatProperty):
                m2 = re.search(r"(?i)STORED\s+AS\s+(\w+)", ptxt)
                meta.format = (m2.group(1) if m2 else ptxt).lower()
            elif isinstance(p, exp.LocationProperty):
                m2 = re.search(r"(?i)LOCATION\s+'([^']*)'", ptxt)
                meta.location = m2.group(1) if m2 else None
            elif isinstance(p, exp.ExternalProperty):
                meta.external = True
            elif isinstance(p, exp.Property):
                m2 = re.match(r"'([^']*)'\s*=\s*'([^']*)'", ptxt)
                if m2:
                    meta.properties[m2.group(1)] = m2.group(2)
        tables.append(meta)
    return tables, warns


def _comment_of(c) -> str:
    for k in c.args.get("constraints") or []:
        if isinstance(k.args.get("kind"), exp.CommentColumnConstraint):
            return str(k.args["kind"].this.name if hasattr(k.args["kind"].this, "name") else "")
    return ""


def _from_canonical(d: Dict[str, Any], source: str) -> List[TableMeta]:
    out = []
    for name, t in (d.get("tables") or {}).items():
        cols = []
        for c in t.get("columns") or []:
            if isinstance(c, str):
                n, _, ty = c.partition(" ")
                cols.append(ColumnMeta(n, _clean_type(ty)))
            else:
                cols.append(ColumnMeta(str(c["name"]), _clean_type(str(c["type"])), bool(c.get("partition")), str(c.get("comment") or "")))
        pc = {p.lower() for p in t.get("partition_columns") or []}
        for c in cols:
            if c.name.lower() in pc:
                c.partition = True
        out.append(TableMeta(str(name).lower(), cols, t.get("kind", "table"), t.get("format"), t.get("location"),
                             bool(t.get("external")), {str(k): str(v) for k, v in (t.get("properties") or {}).items()}, source))
    return out


def load_snapshot(*paths: str) -> Snapshot:
    """Files and/or folders (non-recursive): .hql/.sql/.ddl are parsed as Hive DDL; .yaml/.yml/.json must hold a
    `tables:` mapping (other YAML/JSON files, e.g. blueprints, are ignored)."""
    files: List[str] = []
    for p in paths:
        if os.path.isdir(p):
            files += sorted(os.path.join(p, f) for f in os.listdir(p) if f.lower().endswith(_EXTS))
        elif os.path.isfile(p):
            files.append(p)
        else:
            raise FileNotFoundError(f"metadata path not found: {p}")
    tables: Dict[str, TableMeta] = {}
    used, warns, h = [], [], hashlib.sha256()
    for f in files:
        raw = open(f, "rb").read()
        text = raw.decode("utf-8", errors="replace")
        ext = os.path.splitext(f)[1].lower()
        if ext in (".yaml", ".yml", ".json"):
            try:
                d = json.loads(text) if ext == ".json" else yaml.safe_load(text)
            except Exception as e:
                warns.append(f"{os.path.basename(f)}: unreadable ({str(e).splitlines()[0][:80]})")
                continue
            found = _from_canonical(d, os.path.basename(f)) if isinstance(d, dict) and isinstance(d.get("tables"), dict) else []
            parsed_warns: List[str] = []
        else:
            found, parsed_warns = parse_ddl(text, os.path.basename(f))
        warns += parsed_warns
        if found:
            used.append(os.path.basename(f))
            h.update(os.path.basename(f).encode() + raw)
            for t in found:
                if t.name in tables and [(c.name, c.type) for c in tables[t.name].columns] != [(c.name, c.type) for c in t.columns]:
                    warns.append(f"{t.name} is defined differently in {tables[t.name].source} and {t.source}; the later file wins")
                tables[t.name] = t
    return Snapshot(tables, used, h.hexdigest()[:12], warns)


# ---- resolving a blueprint against the snapshot --------------------------------------------
_TOKEN = re.compile(r"[A-Za-z_]\w*")


def referenced_names(bp: PipelineBlueprint) -> set:
    """Lower-case identifiers that appear anywhere in the pipeline logic (expressions, selects, keys, windows, rules, filters,
    target columns). Used to keep a filled-in source schema to the columns the pipeline actually uses."""
    out: set = set()

    def walk(o: Any) -> None:
        if isinstance(o, str):
            out.update(m.lower() for m in _TOKEN.findall(o))
        elif isinstance(o, dict):
            for v in o.values():
                walk(v)
        elif isinstance(o, (list, tuple)):
            for v in o:
                walk(v)

    for t in bp.transformations:
        walk(t.model_dump(by_alias=True, exclude_none=True))
    for s in bp.sources:
        walk(s.pre_filter)
    for tg in bp.targets:
        walk([f.name for f in tg.schema_])
        walk(tg.partition_by)
    return out


def apply_snapshot(bp: PipelineBlueprint, snap: Snapshot, aliases: Optional[Dict[str, str]] = None,
                   source_ref: str = "") -> Dict[str, Any]:
    """Make the blueprint agree with the catalog (deterministically). Idempotent: running it again on its own
    output changes nothing and adds no findings.

    A source whose schema is empty is filled with the catalog columns the pipeline USES (not every column of the table), so
    the specification stays readable."""
    aliases = aliases or {}
    changes: List[str] = []
    matched: List[str] = []
    unmatched: List[str] = []
    tables_used: set = set()
    target_cols: set = set()   # a target that will be filled from the catalog needs all its columns, which may pass through sources
    for tg in bp.targets:
        if tg.object and not tg.schema_:
            m, _c = snap.lookup(tg.object, aliases)
            if m is not None:
                target_cols |= {c.name.lower() for c in m.columns}
    used_names = referenced_names(bp) | target_cols
    ids = {f.get("id") for f in bp.findings if isinstance(f, dict)}
    n = [0]

    def add(severity, evidence, blocks, affected, resolution):
        if any(isinstance(f, dict) and f.get("evidence") == evidence for f in bp.findings):
            return
        n[0] += 1
        while f"F-META-{n[0]:03d}" in ids:
            n[0] += 1
        fid = f"F-META-{n[0]:03d}"
        ids.add(fid)
        bp.findings.append({"id": fid, "category": "metadata", "severity": severity, "confidence": "high",
                            "source_ref": source_ref or f"snapshot:{snap.id}", "evidence": evidence, "affected_refs": affected,
                            "blocks": blocks, "status": "open", "resolution": resolution})

    def catalog_note(meta: TableMeta) -> Dict[str, Any]:
        tables_used.add(meta.name)
        return {k: v for k, v in {"table": meta.name, "kind": meta.kind if meta.kind != "table" else None,
                                  "format": meta.format, "location": meta.location, "external": meta.external or None,
                                  "partition_columns": meta.partition_columns or None}.items() if v is not None}

    for s in bp.sources:
        key = s.object or s.path
        meta, cands = snap.lookup(key, aliases) if key else (None, [])
        if meta is None:
            unmatched.append(f"{s.id} ({key})")
            hint = (f" Same-named tables elsewhere in the snapshot: {cands}. Add an explicit alias if one of them is the "
                    "source (the framework does not guess)." if cands else "")
            add("high", f"source '{s.id}' ({key}) is not in the metadata snapshot {snap.id}; its schema cannot be verified.{hint}",
                ["generation", "execution"], [s.id], "Add the table to the snapshot, or provide an alias to the catalog table.")
            continue
        matched.append(f"{s.id} -> {meta.name}")
        if meta.kind == "view":
            add("medium", f"source '{s.id}' reads '{meta.name}', a VIEW; a view is not a physical table and carries no "
                          "column metadata in the snapshot.", ["generation"], [s.id], "Treat the view separately (its defining query).")
            s.catalog = catalog_note(meta)
            continue
        if not s.schema_:
            cols = [c for c in meta.columns if c.name.lower() in used_names] or list(meta.columns)
            s.schema_ = [SchemaField(name=c.name, type=c.type) for c in cols]
            changes.append(f"{s.id}: schema filled from {meta.name} ({len(cols)} of {len(meta.columns)} columns, those the pipeline uses)")
        else:
            for f in s.schema_:
                c = meta.column(f.name)
                if c is None:
                    add("high", f"column '{f.name}' of source '{s.id}' does not exist in catalog table {meta.name}.",
                        ["execution"], [s.id], "Fix the column name or confirm the snapshot is current.")
                    continue
                declared = field_type_str(f)
                if _norm(declared) != _norm(c.type):
                    add("medium", f"source '{s.id}' column '{f.name}' was declared as {declared} but the catalog type is "
                                  f"{c.type}; the catalog type was used.", ["execution"], [s.id], "Confirm the catalog type is current.")
                    f.type, f.fields, f.element_type, f.key_type, f.value_type = c.type, None, None, None, None
                    changes.append(f"{s.id}.{f.name}: type set to {c.type}")
        declared_names = {f.name.lower() for f in s.schema_}
        for tok in dict.fromkeys(re.findall(r"\b[A-Za-z_]\w*\b", s.pre_filter or "")):
            c = meta.column(tok)
            if c is not None and tok.lower() not in declared_names:
                s.schema_.append(SchemaField(name=c.name, type=c.type))
                declared_names.add(tok.lower())
                changes.append(f"{s.id}: added '{c.name}' (used by pre_filter) from the catalog")
        s.catalog = catalog_note(meta)

    for tg in bp.targets:
        if not tg.object:
            continue
        meta, cands = snap.lookup(tg.object, aliases)
        if meta is None:
            unmatched.append(f"{tg.id} ({tg.object})")
            add("low", f"target '{tg.id}' ({tg.object}) is not in the metadata snapshot: the table does not exist yet or the "
                       "snapshot is incomplete; a reviewed DDL artifact is needed before execution.", ["execution"], [tg.id],
                "Create the table through a reviewed DDL artifact, or add it to the snapshot.")
            continue
        matched.append(f"{tg.id} -> {meta.name}")
        tg.catalog = catalog_note(meta)
        if not tg.schema_:
            tg.schema_ = [SchemaField(name=c.name, type=c.type) for c in meta.columns if not c.partition]
            changes.append(f"{tg.id}: schema filled from {meta.name}")
        else:
            have = {f.name.lower(): f for f in tg.schema_}
            for c in meta.columns:
                if c.partition:
                    continue
                f = have.get(c.name.lower())
                if f is None:
                    add("medium", f"target '{tg.id}' does not declare catalog column '{c.name}' of {meta.name}.", ["execution"], [tg.id],
                        "Declare it, or confirm the table is meant to change.")
                elif _norm(field_type_str(f)) != _norm(c.type):
                    add("medium", f"target '{tg.id}' column '{c.name}' is declared {field_type_str(f)} but the existing table has {c.type}.",
                        ["execution"], [tg.id], "Align the declaration with the table, or plan a schema change.")
            pcols = {c.name.lower() for c in meta.columns if c.partition}
            for f in tg.schema_:
                if f.name.lower() not in {c.name.lower() for c in meta.columns} and f.name.lower() not in pcols:
                    add("medium", f"target '{tg.id}' declares column '{f.name}' that does not exist in {meta.name}.", ["execution"], [tg.id],
                        "Remove it or plan the schema change.")
        if meta.partition_columns:
            if not tg.partition_by:
                tg.partition_by = list(meta.partition_columns)
                changes.append(f"{tg.id}: partition_by set to {meta.partition_columns} from the catalog")
            elif [p.lower() for p in tg.partition_by] != [p.lower() for p in meta.partition_columns]:
                add("medium", f"target '{tg.id}' partitions by {tg.partition_by} but {meta.name} is partitioned by {meta.partition_columns}.",
                    ["execution"], [tg.id], "Align the write with the table's partitioning.")
        elif tg.partition_by:
            add("medium", f"target '{tg.id}' partitions by {tg.partition_by} but {meta.name} is not partitioned.", ["execution"], [tg.id],
                "Align the write with the table.")

    summary = {"id": snap.id, "files": snap.files, "tables_in_snapshot": len(snap.tables), "matched": matched,
               "unmatched": unmatched, "tables": sorted(tables_used), "changes": changes[:60], "warnings": snap.warnings[:10]}
    bp.provenance["metadata_snapshot"] = summary
    return summary


# ---- CLI -----------------------------------------------------------------------------------
def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(prog="agentic.metadata", description="Inspect or export a catalog metadata snapshot")
    ap.add_argument("paths", nargs="+", help="DDL (.hql/.sql), canonical YAML/JSON, or folders of those")
    ap.add_argument("--out", help="write the canonical YAML snapshot here")
    a = ap.parse_args(argv)
    try:
        snap = load_snapshot(*a.paths)
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    print(f"snapshot {snap.id}: {len(snap.tables)} table(s) from {', '.join(snap.files) or 'no usable files'}")
    for n, t in sorted(snap.tables.items()):
        print(f"  {n:34} {t.kind:5} {len(t.columns):2} cols  partition={t.partition_columns or '-'}  "
              f"{t.format or ''}{' external' if t.external else ''}")
    for w in snap.warnings:
        print("  warning:", w)
    if a.out:
        with open(a.out, "w", encoding="utf-8") as f:
            yaml.dump(snap.to_dict(), f, sort_keys=False)
        print("wrote", a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
