"""Sqoop jobs -> intermediate YAML, one file per job (contract family `sqoop_to_spark`). Deterministic: no LLM, no Sqoop.

Input : a text file of Sqoop commands (one per line; `\\` line continuations and `sqoop job --create NAME -- import ...`
        saved jobs work). Optional source table metadata, used for the column mappings: the source database's
        CREATE TABLE statements (Postgres style: primary keys and NOT NULL are kept), or a Trino lookup.
Output: the YAML of every Sqoop command, in the shape PLAN.md section 3 asks for: common fields (ids, revision, status,
        source files + hashes, target, parameters, dependencies, provenance, findings, versions, artifacts, review)
        plus the Sqoop fields (JDBC connection by secret reference, table or query, column mappings, split key,
        load mode and watermark state, destination).

No executable sqoop_to_spark schema is installed in the repository yet, so every file is a PROPOSED DRAFT with
`execution_eligible: false` and a blocking finding that says so. Same inputs give the same YAML (no timestamps inside a
job). Passwords are never written: the command is screened with agentic.secrets, and a literal --password is a
critical finding.

Outputs go into one folder under the output folder, named `<sqoop file name>_<date>_<time>`:
    <job_id>_<stamp>.yaml   one per Sqoop command
    sqoop_yaml_<stamp>.md   summary tables: jobs, findings, coverage of every Sqoop option
    sqoop_yaml_<stamp>.json the same summary as data

CLI:  python -m agentic.sqoop_yaml samples/sqoop_jobs.txt --ddl 01_schema.sql --project-id sqoop_poc \\
          --target-catalog ice --target-namespace sqoop_poc_iceberg --target-storage hdfs://.../sqoop_poc_iceberg.db
"""
import argparse
import hashlib
import json
import os
import re
import shlex
import sys
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import yaml

from .config import query_settings, ui_settings
from .outputs import new_run_dir, out_name, safe_stem, stamp_now
from .secrets import redact_secrets
from .skills_loader import SKILLS_DIR, SOURCE_SKILL
from .spark_compare import Source, load_source

RULES_VERSION = "sqoop_rules 0.1"

# Sqoop options and how they are represented ---------------------------------------------
REPRESENTED = {  # option -> where it lives in the YAML
    "--connect": "connection.jdbc_url", "--username": "connection.username",
    "--password-file": "connection.authentication", "--table": "ingestion.read.table",
    "--query": "ingestion.read.query", "--where": "ingestion.read.where", "--columns": "ingestion.read.columns",
    "--split-by": "ingestion.split.split_by", "--num-mappers": "ingestion.split.parallelism",
    "--incremental": "ingestion.load.mode", "--check-column": "ingestion.load.watermark.column",
    "--last-value": "parameters.p_last_value", "--merge-key": "ingestion.load.merge_keys",
    "--target-dir": "ingestion.destination.sqoop_landing", "--warehouse-dir": "ingestion.destination.sqoop_landing",
    "--hive-table": "ingestion.destination.sqoop_landing", "--hive-database": "ingestion.destination.sqoop_landing",
    "--map-column-hive": "ingestion.column_mappings", "--map-column-java": "ingestion.column_mappings",
    "--fetch-size": "ingestion.read.fetch_size", "--boundary-query": "ingestion.read.boundary_query"}
IRRELEVANT = {   # Sqoop text-file mechanics with no meaning for a Spark -> Iceberg write
    "--fields-terminated-by": "Iceberg/Parquet stores typed columns; no field delimiter",
    "--lines-terminated-by": "Iceberg/Parquet has no line delimiter",
    "--null-string": "Iceberg stores real NULLs; no null marker string",
    "--null-non-string": "Iceberg stores real NULLs; no null marker string",
    "--as-textfile": "output file format is replaced by the Iceberg table format",
    "--as-parquetfile": "output file format is replaced by the Iceberg table format",
    "--as-avrodatafile": "output file format is replaced by the Iceberg table format",
    "--delete-target-dir": "landing directory is not used by the Spark job",
    "--verbose": "logging only",
}
FLAGGED = {   # represented, but their behavior needs a decision
    "--hive-drop-import-delims": "Sqoop removed \\n, \\r and \\001 from text values; Spark JDBC does not. "
                                 "Decide whether the target must keep that cleaning.",
    "--autoreset-to-one-mapper": "Sqoop silently fell back to one mapper when no split key existed.",
    "--hive-import": "Sqoop created/loaded a Hive table; the target is now an Iceberg table.",
    "--hive-overwrite": "Sqoop replaced the whole Hive table; overwrite scope must be approved for Iceberg.",
    "--append": "Sqoop appended files to the landing directory; append intent must be explicit.",
}
UNSUPPORTED = {"--direct": "database-specific fast path (e.g. COPY) with no Spark JDBC equivalent",
               "--staging-table": "export staging semantics", "--update-key": "export update semantics",
               "--update-mode": "export update semantics", "--batch": "export batching semantics",
               "--call": "stored procedure export", "--export-dir": "export source directory",
               "--compress": "codec of landing files", "--compression-codec": "codec of landing files",
               "--driver": "custom JDBC driver class", "--connection-manager": "custom connection manager"}
BOOL_FLAGS = {"--hive-drop-import-delims", "--hive-import", "--append", "--direct", "--as-parquetfile",
              "--as-avrodatafile", "--as-textfile", "--autoreset-to-one-mapper", "--delete-target-dir",
              "--create-hive-table", "--hive-overwrite", "--verbose", "-P", "--batch", "--compress"}
SECRET_OPTS = {"--password", "--connection-param-file"}
PII = [r"email", r"phone", r"mobile", r"ssn", r"birth", r"(^|_)dob$", r"(^|_)(first|last|full|middle)_name$",
       r"^name$", r"address", r"(^|_)zip", r"postal", r"passport", r"tax_id", r"national_id", r"card",
       r"iban", r"account_no", r"account_number"]


def default_skill_ref() -> str:
    """`agentic/skills/sqoop-to-spark@<hash>`: the version of the pair skill these rules implement."""
    name = SOURCE_SKILL["sqoop"]
    path = os.path.join(SKILLS_DIR, name, "SKILL.md")
    try:
        digest = hashlib.sha256(open(path, "rb").read()).hexdigest()[:7]
    except OSError:
        return "agentic/skills/%s" % name
    return "agentic/skills/%s@%s" % (name, digest)


@dataclass
class Options:
    """Target settings. The Iceberg destination is never guessed: a missing catalog/namespace/storage is a finding."""
    project_id: str = "project_01"
    sqoop_version: str = "1.4.7"
    target_runtime: str = "Spark 3.5.x"
    target_catalog: Optional[str] = None
    target_namespace: Optional[str] = None
    target_storage: Optional[str] = None
    environment_ref: Optional[str] = None
    skill_ref: Optional[str] = None


# ---------------------------------------------------------------------------- reading inputs
def read_commands(text: str):
    """Return [((first_line, last_line), command_text)]."""
    cmds, buf, start = [], "", None
    for n, line in enumerate(text.splitlines(), 1):
        line = line.rstrip("\r")
        if not buf and (not line.strip() or line.strip().startswith("#")):
            continue
        if start is None:
            start = n
        if line.endswith("\\"):
            buf += line[:-1] + " "
            continue
        cmds.append(((start, n), buf + line))
        buf, start = "", None
    if buf:
        cmds.append(((start, start), buf))
    return cmds


def parse(cmd):
    toks = shlex.split(cmd)
    out = {"saved_job": None, "tool": None, "opts": {}, "connector_args": {}, "order": []}
    if "job" in toks and "--create" in toks:
        out["saved_job"] = toks[toks.index("--create") + 1]
    for tool in ("import", "export", "import-all-tables", "eval", "merge"):
        if tool in toks:
            out["tool"] = tool
            toks = toks[toks.index(tool) + 1:]
            break
    i = 0
    while i < len(toks):
        t = toks[i]
        if t == "--":
            rest = toks[i + 1:]
            for k in range(0, len(rest) - 1, 2):
                out["connector_args"][rest[k].lstrip("-")] = rest[k + 1]
            break
        if t in BOOL_FLAGS:
            out["opts"][t] = True
            out["order"].append(t)
            i += 1
        elif t.startswith("-") and i + 1 < len(toks):
            out["opts"][t] = toks[i + 1]
            out["order"].append(t)
            i += 2
        else:
            i += 1
    if "-m" in out["opts"]:
        out["opts"]["--num-mappers"] = out["opts"].pop("-m")
        out["order"] = ["--num-mappers" if o == "-m" else o for o in out["order"]]
    return out


def to_type(raw):
    """Source type -> Spark SQL type for the target, plus a note when the mapping is not exact."""
    t = " ".join(raw.lower().split())
    m = re.match(r"^(character varying|varchar)\s*\(\d+\)$", t)
    if m or t in ("text", "varchar", "character varying", "string", "clob"):
        return "string", None
    m = re.match(r"^(character|char|bpchar)\s*\((\d+)\)$", t)
    if m:
        return "string", "fixed-length char padding is not preserved"
    m = re.match(r"^(numeric|decimal)\s*\((\d+)\s*,\s*(\d+)\)$", t)
    if m:
        return "decimal(%s,%s)" % (m.group(2), m.group(3)), None
    simple = {"bigserial": "bigint", "bigint": "bigint", "int8": "bigint", "serial": "int", "integer": "int",
              "int": "int", "int4": "int", "smallint": "int", "int2": "int", "boolean": "boolean", "bool": "boolean",
              "date": "date", "timestamp": "timestamp_ntz", "timestamp without time zone": "timestamp_ntz",
              "timestamptz": "timestamp", "timestamp with time zone": "timestamp", "double precision": "double",
              "float8": "double", "real": "float", "float4": "float", "bytea": "binary", "uuid": "string"}
    if t in simple:
        note = None
        if t in ("smallint", "int2"):
            note = "widened to int (Iceberg has no smallint)"
        if t in ("timestamptz", "timestamp with time zone"):
            note = "time zone aware; Spark stores UTC instant, session time zone must be fixed"
        return simple[t], note
    if t in ("json", "jsonb"):
        return "string", "%s read as text; must be cast in the extraction query" % t
    if t.endswith("[]"):
        inner, _ = to_type(t[:-2])
        return "array<%s>" % inner, "Postgres array; Spark JDBC support must be verified"
    if t in ("numeric", "decimal"):
        return "decimal(38,10)", "unbounded numeric: scale must be confirmed"
    return None, "type '%s' has no registered mapping (enum/user-defined?)" % raw.strip()


def source_ddl_tables(text: str, origin: str) -> Dict[str, dict]:
    """CREATE TABLE statements of the SOURCE database -> {schema.table: {columns, primary_key, origin}}.

    Kept separate from agentic.metadata.parse_ddl, which reads Hive DDL and normalises types to Spark: here the source
    types (Postgres NUMERIC, JSONB, arrays, enums...), primary keys and NOT NULL are needed as written."""
    text = re.sub(r"--[^\n]*", "", text)
    tables = {}
    for m in re.finditer(r"create\s+table\s+(?:if\s+not\s+exists\s+)?([\w\.]+)\s*\(", text, re.I):
        i, depth, j = m.end(), 1, m.end()
        while j < len(text) and depth:
            depth += {"(": 1, ")": -1}.get(text[j], 0)
            j += 1
        body, cols, pk, cur, d = text[i:j - 1], [], [], "", 0
        parts = []
        for ch in body:
            d += {"(": 1, ")": -1}.get(ch, 0)
            if ch == "," and d == 0:
                parts.append(cur)
                cur = ""
            else:
                cur += ch
        parts.append(cur)
        for p in parts:
            p = " ".join(p.split())
            km = re.match(r"^primary key\s*\((.+)\)", p, re.I)
            if km:
                pk = [c.strip() for c in km.group(1).split(",")]
                continue
            if not p or re.match(r"^(constraint|unique|foreign|check)\b", p, re.I):
                continue
            name, rest = p.split(" ", 1)
            stop = re.search(r"\b(not null|null|primary key|references|default|check|unique)\b", rest, re.I)
            src_type = (rest[:stop.start()] if stop else rest).strip()
            nullable = False if re.search(r"\bnot null\b|\bprimary key\b", rest, re.I) else None
            if re.search(r"\bprimary key\b", rest, re.I):
                pk = [name]
            cols.append({"name": name.lower(), "source_type": src_type, "nullable": nullable})
        tables[m.group(1).lower()] = {"columns": cols, "primary_key": pk, "origin": "ddl:%s" % origin}
    return tables


def trino_table(url: str, fq: str) -> Optional[dict]:
    """Columns of catalog.schema.table from Trino's information_schema, through the read-only query connector."""
    from .query.connectors import run_query
    from .query.guard import safe_identifier

    m = re.match(r"^(https?)://([^:/]+)(?::(\d+))?", url)
    if not m:
        raise ValueError("not an http(s) URL: %s" % url)
    cat, sch, tbl = (safe_identifier(p) for p in fq.split("."))
    conn = {"host": m.group(2), "port": int(m.group(3) or 8080), "http_scheme": m.group(1), "user": "sqoop_to_yaml"}
    q = query_settings()
    res = run_query("trino", conn, "SELECT column_name, data_type, is_nullable FROM %s.information_schema.columns "
                    "WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position" % (cat, sch, tbl),
                    timeout=q["timeout_seconds"], log_path=q["log"])
    rows = res.rows
    return {"columns": [{"name": r[0], "source_type": r[1], "nullable": (False if r[2] == "NO" else None)} for r in rows],
            "primary_key": [], "origin": "trino:%s" % fq} if rows else None


# ------------------------------------------------------------------------------- building
class Builder(object):
    def __init__(self, job_id):
        self.job_id = job_id
        self.findings, self.coverage, self.prov = [], [], []

    def provenance(self, pid, locator, method):
        self.prov.append({"id": pid, "file_ref": "file_sqoop", "locator": locator, "extraction_method": method})
        return pid

    def finding(self, category, severity, text, blocks, affected, resolution, confidence="high",
                source_ref=None, rule=None):
        fid = "F%02d" % (len(self.findings) + 1)
        self.findings.append({
            "id": "%s.%s" % (self.job_id, fid), "category": category, "severity": severity,
            "confidence": confidence, "source_ref": source_ref, "evidence": text,
            "affected_refs": affected, "blocks": blocks, "status": "open",
            "resolution": resolution, "rule_ref": rule and "%s/%s" % (RULES_VERSION, rule)})
        return "%s.%s" % (self.job_id, fid)

    def cover(self, construct, status, refs=None, finding=None, reason=None):
        c = {"construct": construct, "status": status}
        if refs:
            c["refs"] = refs
        if finding:
            c["finding_refs"] = [finding]
        if reason:
            c["reason"] = reason
        self.coverage.append(c)


def build(job, lines, raw_cmd, sqoop_label: str, opt: Options, meta_lookup: Callable, file_sha: str,
          query_by_name: bool = False) -> dict:
    """One job's spec. `query_by_name`: look up a free-form query's columns by the landed table's name (Trino)."""
    o = job["opts"]
    name = (o.get("--hive-table") or (o.get("--target-dir") or "").rstrip("/").split("/")[-1]
            or (o.get("--table") or "query")).split(".")[-1].lower()
    job_id = re.sub(r"[^a-z0-9_]", "_", job["saved_job"] or name)
    b = Builder(job_id)
    src_ref = "%s:%d-%d" % (os.path.basename(sqoop_label), lines[0], lines[1])
    b.provenance("prov_cmd", "lines %d-%d" % lines, "shlex_parse_%s" % RULES_VERSION.replace(" ", "_"))

    # ---- direction
    direction = {"import": "import", "export": "export", "import-all-tables": "import_all_tables"}.get(job["tool"], job["tool"])
    if direction != "import":
        b.finding("unsupported_logic", "critical", "Sqoop tool '%s' is not a single-table import." % job["tool"],
                  ["generation", "execution"], [job_id], "Only full imports are in the POC subset; treat separately.",
                  source_ref=src_ref, rule="direction")

    # ---- connection (never secrets)
    url = o.get("--connect", "")
    m = re.match(r"jdbc:(\w+)://([^/:]+)(?::(\d+))?/([^?;]+)", url)
    system = {"postgresql": "postgres"}.get(m.group(1), m.group(1)) if m else None
    auth = {"method": None, "secret_ref": None}
    if o.get("--password-file"):
        auth = {"method": "password_file", "secret_ref": o["--password-file"]}
    elif o.get("-P"):
        auth = {"method": "interactive_prompt", "secret_ref": None}
    if o.get("--password"):
        auth = {"method": "inline_password", "secret_ref": None}
        b.finding("security", "critical", "The command contains a literal --password value (redacted in this file).",
                  ["execution"], [job_id], "Rotate the password and use a secret reference.", source_ref=src_ref, rule="inline_secret")
    if not auth["secret_ref"]:
        b.finding("configuration", "high", "No secret reference for the JDBC login.", ["execution"], ["conn_source"],
                  "Supply a secret reference (vault / Kubernetes secret / password file).", source_ref=src_ref, rule="secret_ref")
    connection = {"id": "conn_source", "type": "jdbc", "system": system,
                  "jdbc_url": redact_secrets(url)[0] if url else None,
                  "host": m.group(2) if m else None, "port": int(m.group(3)) if m and m.group(3) else None,
                  "database": m.group(4) if m else None, "username": o.get("--username"),
                  "authentication": auth, "properties": dict(job["connector_args"]) or {}, "evidence_refs": ["prov_cmd"]}

    # ---- read
    schema = job["connector_args"].get("schema")
    table = o.get("--table")
    query = o.get("--query")
    obj = (table if table and "." in table else "%s.%s" % (schema, table) if table and schema else table)
    read = {"mode": "query" if query else "table", "table": obj, "query": None, "columns": None,
            "where": o.get("--where"), "fetch_size": int(o["--fetch-size"]) if o.get("--fetch-size") else None,
            "boundary_query": o.get("--boundary-query"), "consistency": "unknown"}
    if query:
        read["query"] = " ".join(query.replace("$CONDITIONS", "1=1").split())
        b.finding("compatibility", "medium",
                  "Free-form query in the source SQL dialect; Sqoop injected $CONDITIONS per mapper.",
                  ["execution"], ["ingestion.read"], "Confirm the query is pushed down unchanged as a JDBC subquery "
                  "and that its source-dialect functions are valid there.", source_ref=src_ref, rule="free_form_query")
    if o.get("--columns"):
        read["columns"] = [c.strip().lower() for c in o["--columns"].split(",")]
    b.finding("operational", "low", "Read consistency is unknown: Sqoop mappers read in separate transactions.",
              [], ["ingestion.read"], "Accept, or require a snapshot/consistent read.", confidence="medium", rule="read_consistency")

    # ---- split
    mappers = int(o["--num-mappers"]) if o.get("--num-mappers") else 4
    split = {"split_by": (o.get("--split-by") or "").split(".")[-1].lower() or None, "parallelism": mappers,
             "lower_bound": None, "upper_bound": None, "bounds_source": "min/max query at run time"}
    if mappers > 1 and not split["split_by"]:
        pk = (meta_lookup(obj) or {}).get("primary_key") if obj else None
        if pk and len(pk) == 1:
            split["split_by"] = pk[0]
            split["bounds_source"] = "primary key (Sqoop default)"
        else:
            b.finding("configuration", "high", "Parallel read requested without a usable split key.",
                      ["execution"], ["ingestion.split"], "Set split_by or parallelism 1.", source_ref=src_ref, rule="split_key")

    # ---- load mode and state
    inc = o.get("--incremental")
    load = {"mode": "full", "watermark": None, "merge_keys": None, "write_intent": "overwrite_scope_unapproved"}
    params = []
    if inc:
        load["mode"] = "incremental_" + inc
        load["watermark"] = {"column": (o.get("--check-column") or "").lower() or None,
                             "last_value_param": "p_last_value", "state_store": "sqoop_saved_job" if job["saved_job"] else "command_line",
                             "update_rule": "set to the max value read, after a successful write"}
        params.append({"id": "p_last_value", "name": "last_value", "type": "string", "required": True,
                       "default": o.get("--last-value"), "binding": "sqoop_saved_job_state" if job["saved_job"] else "command_argument",
                       "evidence_refs": ["prov_cmd"]})
        load["write_intent"] = "merge" if inc == "lastmodified" else "append"
        if o.get("--merge-key"):
            load["merge_keys"] = [k.strip().lower() for k in o["--merge-key"].split(",")]
        b.finding("unsupported_logic", "high",
                  "Incremental '%s' load: the POC supports full imports only, and the current watermark lives in Sqoop's job store." % inc,
                  ["execution"], ["ingestion.load"], "Keep as a draft; export the saved job's last value and register incremental support.",
                  source_ref=src_ref, rule="incremental_mode")
        if inc == "lastmodified" and not load["merge_keys"]:
            b.finding("configuration", "high", "lastmodified without --merge-key would duplicate changed rows.",
                      ["execution"], ["ingestion.load"], "Supply merge keys.", source_ref=src_ref, rule="merge_key")
    else:
        b.finding("operational", "medium", "Full import: overwrite scope of the target table is not approved.",
                  ["execution"], ["ingestion.destination"], "Approve full-table replacement of the isolated test table.",
                  source_ref=src_ref, rule="overwrite_scope")

    # ---- columns from metadata
    meta = meta_lookup(obj) if obj else (meta_lookup(name) if query_by_name else None)   # query jobs: landed table via Trino
    columns = []
    if meta:
        keep = read["columns"]
        overrides = {}
        for opt_name in ("--map-column-hive", "--map-column-java"):
            for pair in (o.get(opt_name) or "").split(","):
                if "=" in pair:
                    c, t = pair.split("=")
                    overrides[c.strip().lower()] = (t.strip(), opt_name)
        for c in meta["columns"]:
            if keep and c["name"] not in keep:
                continue
            tgt, note = to_type(c["source_type"])
            entry = {"source_column": c["name"], "source_type": c["source_type"], "nullable": c["nullable"],
                     "target_column": c["name"], "target_type": tgt, "mapping_rule": "type_map" if tgt else None}
            if c["name"] in overrides:
                entry["target_type"], entry["mapping_rule"] = overrides[c["name"]][0].lower(), overrides[c["name"]][1]
            if note:
                entry["note"] = note
            if any(re.search(p, c["name"]) for p in PII):
                entry["sensitivity"] = "possible_personal_data"
            columns.append(entry)
            if entry["target_type"] is None:
                b.finding("compatibility", "high", "Column %s: %s." % (c["name"], note), ["execution"],
                          ["column:%s" % c["name"]], "Cast it in the extraction query or supply a type mapping.", rule="type_map")
        sens = [c["source_column"] for c in columns if c.get("sensitivity")]
        if sens:
            b.finding("privacy", "high", "Column names suggest personal data: %s (names are evidence, not proof)." % ", ".join(sens),
                      ["execution"], ["column:%s" % s for s in sens],
                      "Approve a handling policy (destinations, logging, access, required transformations). No masking is applied automatically.",
                      confidence="medium", rule="pii_names")
        if any(c["nullable"] is None for c in columns) and meta["origin"].startswith("trino"):
            b.finding("metadata", "low", "Nullability unknown for some columns.", [], [obj], "Accept as unknown.", rule="nullability")
    else:
        b.finding("metadata", "high", "No source metadata for %s: column mappings are unknown." % (obj or "the query result"),
                  ["generation", "execution"], ["ingestion.column_mappings"],
                  "Supply the source table definition or the query's result schema.", source_ref=src_ref, rule="metadata_missing")

    # ---- destination (explicit only; never inferred from HDFS paths)
    landing = {"target_dir": o.get("--target-dir"), "warehouse_dir": o.get("--warehouse-dir"),
               "hive_database": o.get("--hive-database"), "hive_table": o.get("--hive-table")}
    destination = {"sqoop_landing": landing, "target_table": {"catalog": opt.target_catalog, "namespace": opt.target_namespace,
                   "name": name if opt.target_namespace else None, "format": "iceberg", "storage": opt.target_storage}}
    missing = [k for k in ("catalog", "namespace", "storage") if not destination["target_table"][k]]
    if missing:
        b.finding("configuration", "high", "Target %s not supplied (an HDFS landing path does not establish the target)." % ", ".join(missing),
                  ["generation", "execution"], ["ingestion.destination"], "Supply explicit target settings.", rule="target_explicit")

    # ---- text handling
    text = {"drop_import_delims": bool(o.get("--hive-drop-import-delims")),
            "null_string": o.get("--null-string"), "null_non_string": o.get("--null-non-string")}

    # ---- coverage of every option
    for opt_name in job["order"]:
        if opt_name in SECRET_OPTS:
            b.cover(opt_name, "represented", ["connection.authentication"], reason="value redacted")
        elif opt_name in REPRESENTED:
            b.cover(opt_name, "represented", [REPRESENTED[opt_name]])
        elif opt_name in IRRELEVANT:
            b.cover(opt_name, "irrelevant", reason=IRRELEVANT[opt_name])
        elif opt_name in FLAGGED:
            f = b.finding("business_assumption" if opt_name == "--hive-drop-import-delims" else "compatibility", "medium",
                          FLAGGED[opt_name], ["execution"], ["ingestion"], "Decide and record the target behavior.",
                          source_ref=src_ref, rule="flag%s" % opt_name.replace("--", "_"))
            b.cover(opt_name, "unresolved", finding=f)
        elif opt_name in UNSUPPORTED:
            f = b.finding("unsupported_logic", "high", "%s: %s." % (opt_name, UNSUPPORTED[opt_name]), ["execution"], ["ingestion"],
                          "Remove, replace with a supported setting, or exclude the job.", source_ref=src_ref, rule="unsupported_option")
            b.cover(opt_name, "unsupported", finding=f)
        else:
            f = b.finding("unsupported_logic", "medium", "Option %s is not recognised by %s." % (opt_name, RULES_VERSION),
                          ["execution"], ["ingestion"], "Review manually.", confidence="low", source_ref=src_ref, rule="unknown_option")
            b.cover(opt_name, "unresolved", finding=f)
    for k, v in job["connector_args"].items():
        b.cover("-- --%s" % k, "represented", ["connection.properties"])

    b.finding("configuration", "critical", "No executable sqoop_to_spark schema or capability registry is installed; "
              "this file is a proposed draft.", ["execution"], [job_id], "Install the contract and registry, then re-validate.",
              rule="missing_executable_contract")

    return {
        "contract_family": "sqoop_to_spark",
        "contract_version": None,
        "status": "draft",
        "execution_eligible": False,
        "project_id": opt.project_id,
        "job_id": job_id,
        "revision": 1,
        "description": "Sqoop %s of %s into an Iceberg table via Spark JDBC." % (direction, obj or "a free-form query"),
        "source": {
            "tool": "sqoop", "version": opt.sqoop_version,
            "files": [{"id": "file_sqoop", "path": sqoop_label, "sha256": file_sha, "locator": "lines %d-%d" % lines}],
            "saved_job": job["saved_job"], "command_redacted": redact_secrets(" ".join(raw_cmd.split()))[0],
            "metadata_snapshot": meta["origin"] if meta else None,
        },
        "target": {"runtime_family": opt.target_runtime, "table_format": "iceberg", "environment_ref": opt.environment_ref},
        "connection": connection,
        "ingestion": {"direction": direction, "read": read, "split": split, "load": load,
                      "column_mappings": columns, "text_handling": text, "destination": destination},
        "parameters": params,
        "dependencies": [{"id": "dep_db", "type": "database", "ref": "conn_source", "objects": [obj] if obj else []}] +
                        ([{"id": "dep_landing", "type": "hdfs_path", "ref": landing["target_dir"],
                           "note": "Sqoop landing path; not a target mapping"}] if landing["target_dir"] else []),
        "provenance": b.prov,
        "findings": b.findings,
        "coverage": b.coverage,
        "versions": {"rules": RULES_VERSION, "skill": opt.skill_ref or default_skill_ref(), "analysis": None, "framework": None},
        "artifacts": [],
        "review": {"status": "pending", "decisions": []},
    }


def spec_yaml(spec: dict) -> str:
    """The YAML text of one job, with a header that says it is a draft and where it came from."""
    head = ("# Proposed draft, NOT an executable contract (no sqoop_to_spark schema installed yet).\n"
            "# Generated by agentic.sqoop_yaml (%s) from %s %s.\n" % (RULES_VERSION, os.path.basename(spec["source"]["files"][0]["path"]),
                                                                     spec["source"]["files"][0]["locator"]))
    return head + yaml.safe_dump(spec, sort_keys=False, allow_unicode=True, width=110, default_flow_style=False)


def build_specs(sqoop: Source, ddl: Optional[Source] = None, options: Optional[Options] = None,
                trino_url: Optional[str] = None, trino_schema: Optional[str] = None) -> dict:
    """All job specs of a Sqoop file, without writing anything. -> {"specs": [...], "skipped": [...], "warnings": [...]}"""
    if ddl and trino_url:
        raise ValueError("give either DDL or a Trino URL for the source metadata, not both")
    opt = options or Options()
    tables = source_ddl_tables(ddl.code, os.path.basename(ddl.label)) if ddl else {}
    warnings: List[str] = []

    def lookup(obj):
        if not obj:
            return None
        if trino_url:
            if not trino_schema and obj.count(".") != 2:
                warnings.append("Trino needs a schema (catalog.schema) to find %s; mappings left unknown" % obj)
                return None
            fq = obj if obj.count(".") == 2 else "%s.%s" % (trino_schema, obj.split(".")[-1])
            try:
                return trino_table(trino_url, fq)
            except Exception as e:                       # a failed lookup is an unknown, never a crash
                warnings.append("Trino lookup of %s failed (%s); mappings left unknown" % (fq, str(e).splitlines()[0][:120]))
                return None
        key = obj.lower()
        if key in tables:
            return tables[key]
        hits = [k for k in tables if k.split(".")[-1] == key.split(".")[-1]]
        return tables[hits[0]] if len(hits) == 1 else None

    sha = hashlib.sha256(sqoop.code.encode("utf-8")).hexdigest()
    specs, skipped, seen = [], [], set()
    for lines, cmd in read_commands(sqoop.code):
        try:
            job = parse(cmd)
        except ValueError as e:                          # unbalanced quotes
            warnings.append("lines %d-%d: cannot read the command (%s)" % (lines[0], lines[1], e))
            skipped.append(lines[0])
            continue
        if not job["tool"]:
            warnings.append("line %d: not a Sqoop tool invocation; skipped" % lines[0])
            skipped.append(lines[0])
            continue
        spec = build(job, lines, cmd, sqoop.label, opt, lookup, sha, query_by_name=bool(trino_url))
        if spec["job_id"] in seen:                       # two commands landing in the same place: keep both files
            n = 2
            while "%s_%d" % (spec["job_id"], n) in seen:
                n += 1
            warnings.append("lines %d-%d: job id %s is already used; this job is %s_%d"
                            % (lines[0], lines[1], spec["job_id"], spec["job_id"], n))
            spec["job_id"] = "%s_%d" % (spec["job_id"], n)
        seen.add(spec["job_id"])
        specs.append(spec)
    return {"specs": specs, "skipped": skipped, "warnings": warnings}


# ---------- summary ----------

def build_report(label: str, specs: List[dict], skipped: List[int], warnings: List[str]) -> dict:
    jobs, findings, coverage = [], [], []
    for s in specs:
        blocking = [f for f in s["findings"] if "execution" in f["blocks"]]
        jobs.append({"job_id": s["job_id"], "lines": s["source"]["files"][0]["locator"],
                     "saved_job": s["source"]["saved_job"], "load_mode": s["ingestion"]["load"]["mode"],
                     "source": s["ingestion"]["read"]["table"] or "query",
                     "columns": len(s["ingestion"]["column_mappings"]),
                     "findings": len(s["findings"]), "blocking_execution": len(blocking)})
        for f in s["findings"]:
            findings.append({"job": s["job_id"], "id": f["id"].split(".")[-1], "category": f["category"],
                             "severity": f["severity"], "blocks": ",".join(f["blocks"]) or "-",
                             "evidence": f["evidence"], "resolution": f["resolution"]})
        for c in s["coverage"]:
            coverage.append({"job": s["job_id"], "construct": c["construct"], "status": c["status"],
                             "detail": (c.get("refs") or c.get("finding_refs") or [c.get("reason", "")])[0]})
    full = sum(1 for j in jobs if j["load_mode"] == "full")
    return {
        "sqoop_file": label,
        "summary": {"jobs": len(jobs), "full": full, "incremental": len(jobs) - full,
                    "findings": len(findings), "blocking_execution": sum(j["blocking_execution"] for j in jobs),
                    "skipped_lines": skipped},
        "jobs": jobs,
        "findings": findings,
        "coverage": coverage,
        "warnings": warnings,
    }


def _cell(text) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def to_markdown(r: dict, yaml_names: Dict[str, str]) -> str:
    s = r["summary"]
    out = ["# Sqoop jobs to YAML: %s" % r["sqoop_file"], "",
           "Jobs: **%d** (%d full, %d incremental). Findings: **%d**, %d of them block execution."
           % (s["jobs"], s["full"], s["incremental"], s["findings"], s["blocking_execution"]), "",
           "Every file is a proposed draft (`execution_eligible: false`) until a `sqoop_to_spark` schema and capability "
           "registry are installed and the findings are reviewed.", ""]
    if r["warnings"]:
        out += ["## Warnings", ""] + ["- %s" % w for w in r["warnings"]] + [""]
    out += ["## Jobs", "",
            "| Job | Lines | Load mode | Source | Columns mapped | Findings | Blocking execution | YAML |",
            "|---|---|---|---|---:|---:|---:|---|"]
    for j in r["jobs"]:
        out.append("| %s | %s | %s | %s | %d | %d | %d | `%s` |" % (j["job_id"], j["lines"], j["load_mode"], _cell(j["source"]),
                                                               j["columns"], j["findings"], j["blocking_execution"],
                                                               yaml_names.get(j["job_id"], "")))
    out += ["", "## Findings", "", "| Job | Id | Category | Severity | Blocks | Evidence |", "|---|---|---|---|---|---|"]
    for f in r["findings"]:
        out.append("| %s | %s | %s | %s | %s | %s |" % (f["job"], f["id"], f["category"], f["severity"], f["blocks"],
                                                       _cell(f["evidence"])))
    out += ["", "## Coverage: how every Sqoop option was handled", "",
            "| Job | Option | Status | YAML path, finding or reason |", "|---|---|---|---|"]
    for c in r["coverage"]:
        out.append("| %s | `%s` | %s | %s |" % (c["job"], c["construct"], c["status"], _cell(c["detail"])))
    out.append("")
    return "\n".join(out)


# ---------- output ----------

def convert(sqoop: Source, ddl: Optional[Source] = None, options: Optional[Options] = None,
            trino_url: Optional[str] = None, trino_schema: Optional[str] = None,
            out_base: Optional[str] = None, now=None) -> dict:
    """Build every job's YAML and write them, with the Markdown and JSON summary, into a new run folder."""
    built = build_specs(sqoop, ddl, options, trino_url, trino_schema)
    report = build_report(sqoop.label, built["specs"], built["skipped"], built["warnings"])
    folder, stamp = new_run_dir(out_base or ui_settings()["out_dir"], stamp_now(now), name=safe_stem(sqoop.label))
    yaml_names = {s["job_id"]: out_name(s["job_id"], stamp, "yaml") for s in built["specs"]}
    texts = {s["job_id"]: spec_yaml(s) for s in built["specs"]}
    paths = {"yaml": {j: os.path.join(folder, n) for j, n in yaml_names.items()},
             "markdown": os.path.join(folder, out_name("sqoop_yaml", stamp, "md")),
             "json": os.path.join(folder, out_name("sqoop_yaml", stamp, "json"))}
    report["stamp"] = stamp
    for job_id, path in paths["yaml"].items():
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(texts[job_id])
    markdown = to_markdown(report, yaml_names)
    with open(paths["markdown"], "w", encoding="utf-8") as fh:
        fh.write(markdown)
    with open(paths["json"], "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    return {"folder": folder, "files": paths, "report": report, "markdown": markdown, "yaml": texts}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="agentic.sqoop_yaml", description="Turn Sqoop jobs into intermediate YAML files")
    ap.add_argument("sqoop_file", help="text file of Sqoop commands")
    meta = ap.add_mutually_exclusive_group()
    meta.add_argument("--ddl", help="CREATE TABLE statements of the source database")
    meta.add_argument("--trino", help="Trino URL to read table metadata (use with --trino-schema)")
    ap.add_argument("--trino-schema", help="catalog.schema in Trino that holds the tables, e.g. hive.sqoop_poc_spark "
                                          "(the Sqoop-landed copies)")
    d = Options()
    ap.add_argument("--project-id", default=d.project_id)
    ap.add_argument("--sqoop-version", default=d.sqoop_version)
    ap.add_argument("--target-runtime", default=d.target_runtime)
    ap.add_argument("--target-catalog")
    ap.add_argument("--target-namespace")
    ap.add_argument("--target-storage")
    ap.add_argument("--environment-ref")
    ap.add_argument("--skill-ref", help="default: agentic/skills/sqoop-to-spark@<hash of its SKILL.md>")
    ap.add_argument("--out", default=None, help="output folder (default: ui.out_dir in config.yaml)")
    a = ap.parse_args(argv)
    opt = Options(a.project_id, a.sqoop_version, a.target_runtime, a.target_catalog, a.target_namespace,
                  a.target_storage, a.environment_ref, a.skill_ref)
    try:
        res = convert(load_source(path=a.sqoop_file, name=a.sqoop_file), load_source(path=a.ddl) if a.ddl else None,
                      opt, a.trino, a.trino_schema, a.out)
    except (OSError, ValueError) as e:
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    for w in res["report"]["warnings"]:
        print("note:", w)
    for j in res["report"]["jobs"]:
        print("%-34s %-26s findings=%d (blocking execution: %d)" % (os.path.basename(res["files"]["yaml"][j["job_id"]]),
                                                                   j["load_mode"], j["findings"], j["blocking_execution"]))
    print(f"Files written to {res['folder']}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
