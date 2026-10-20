"""Migration Workbench (Streamlit).

    python -m streamlit run ui/app.py --server.port 8501 --server.address 0.0.0.0

Sections
  Migration    Submit one or more legacy job files, uploaded or retrieved from a git repository, execute the migration, and
               review every stage, step and file as it is produced (stages, analysis steps, intermediate YAML, SQL and
               code, recorded findings). The resulting YAML blueprint can be validated.
  Run History  Reopen any earlier run from output/<input file>_<date>_<time>/ together with its step history and files.
  Data Query   Read-only Hive and Trino queries with a schema browser. A single SELECT, WITH, VALUES, SHOW, DESCRIBE or
               EXPLAIN statement is accepted per execution; result sets are size-limited and queries are time-limited.

Set AGENTIC_UI_PASSWORD to require an access password. Service credentials are read from the environment or from the local
configuration file and are never entered on this page.
"""
import hmac
import os
import sys
import uuid
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # project root, so `import agentic` works

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402
import yaml  # noqa: E402

from agentic import gitsource  # noqa: E402
from agentic.config import metadata_settings, query_settings, ui_settings  # noqa: E402
from agentic.jobs import (JobManager, find_file, list_runs, run_events, run_files, save_upload,  # noqa: E402
                          zip_run)
from agentic.metadata import load_snapshot  # noqa: E402
from agentic.orchestrator import STAGES  # noqa: E402
from agentic.query.connectors import (QueryError, describe_table, list_databases, list_tables,  # noqa: E402
                                      run_query)
from agentic.query.guard import QueryRejected  # noqa: E402
from agentic.validator.spark_session import spark_unavailable_reason  # noqa: E402

UI = ui_settings()
OUT_DIR = UI["out_dir"]
SOURCE_TYPES = ["auto", "oozie", "pyspark2", "sqoop", "hiveql", "excel"]
STAGE_TEXT = {"start": "In progress", "done": "Completed", "error": "Failed", "skipped": "Not applicable", None: "Pending"}
JOB_TEXT = {"queued": "Queued", "running": "In progress", "done": "Completed", "failed": "Failed", "incomplete": "Incomplete"}
LANG = {".yaml": "yaml", ".yml": "yaml", ".py": "python", ".sql": "sql", ".hql": "sql", ".json": "json",
        ".jsonl": "json", ".xml": "xml", ".sh": "bash"}
CATALOG_DEFAULT, CATALOG_UPLOAD, CATALOG_NONE = "Configured default", "Upload DDL / YAML", "None"
TECH_PREFIXES = ("run_meta_", "events_", "provenance_")   # audit files: kept on disk, hidden in the file list by default

st.set_page_config(page_title="Migration Workbench", layout="wide")


def rerun() -> None:
    (getattr(st, "rerun", None) or st.experimental_rerun)()


@st.cache_resource
def get_manager() -> JobManager:
    return JobManager(max_parallel=int(UI["max_parallel_jobs"]))


MGR = get_manager()


# ---- access control ---------------------------------------------------------------------------------
def gate() -> None:
    pw = os.getenv("AGENTIC_UI_PASSWORD")
    if not pw or st.session_state.get("authed"):
        return
    st.title("Migration Workbench")
    entered = st.text_input("Access password", type="password", key="gate_pw")
    if st.button("Sign In", key="gate_btn"):
        if hmac.compare_digest(entered.encode(), pw.encode()):
            st.session_state["authed"] = True
            rerun()
        else:
            st.error("The password entered is incorrect.")
    st.stop()


gate()


# ---- presentation helpers ------------------------------------------------------------------------------
def fmt_size(n: int) -> str:
    return f"{n} B" if n < 1024 else f"{n / 1024:.1f} KB" if n < 1024 * 1024 else f"{n / (1024 * 1024):.1f} MB"


def stage_status(evs: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for e in evs:
        if e.get("kind") == "stage":
            out[e["stage"]] = (e.get("status"), e.get("seconds"))
    return out


def step_lines(evs: List[Dict[str, Any]]) -> List[str]:
    lines = []
    for e in evs:
        k, t = e.get("kind"), e.get("t", "")
        if k == "stage":
            secs = f" ({e['seconds']} s)" if e.get("seconds") is not None else ""
            err = f" - {e['error']}" if e.get("error") else ""
            lines.append(f"{t}  Stage '{e.get('title')}': {STAGE_TEXT.get(e.get('status'), e.get('status'))}{secs}{err}")
        elif k == "tool":
            detail = str(e.get("detail", ""))[:160].replace(chr(10), " ")
            lines.append(f"{t}    {e.get('agent')}, step {e.get('turn')}: {e.get('tool')} - "
                         f"{'Accepted' if e.get('ok') else 'Rejected'}. {detail}")
        elif k == "log":
            lines.append(f"{t}    {e.get('agent')}: {e.get('message')}")
        elif k == "file":
            lines.append(f"{t}    File created: {e.get('name')} ({e.get('label')})")
        elif k == "note":
            lines.append(f"{t}    {e.get('message')}")
        elif k == "run_start":
            lines.append(f"{t}  Run started")
        elif k == "run_done":
            lines.append(f"{t}  Run completed")
        elif k == "run_failed":
            lines.append(f"{t}  Run failed. {e.get('error') or ''}")
    return lines


def show_file(path: str) -> None:
    ext = os.path.splitext(path)[1].lower()
    try:
        if os.path.getsize(path) > 2 * 1024 * 1024:
            st.warning("This file exceeds 2 MB and cannot be displayed. Please download it instead.")
            return
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
    except OSError as e:
        st.error(f"The file could not be read: {e}")
        return
    if ext == ".md":
        st.markdown(text)
    else:
        st.code(text, language=LANG.get(ext))


def findings_df(run_dir: str) -> Optional[pd.DataFrame]:
    spec = find_file(run_dir, "pipeline_spec_") if run_dir else None
    if not spec:
        return None
    try:
        with open(spec, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError):
        return None
    rows = [{"Identifier": x.get("id"), "Severity": x.get("severity"), "Category": x.get("category"),
             "Blocks": ", ".join(x.get("blocks") or []), "Reference": x.get("source_ref"),
             "Finding": str(x.get("evidence") or x.get("summary") or "")[:300]}
            for x in data.get("findings") or [] if isinstance(x, dict)]
    return pd.DataFrame(rows) if rows else None


def render_run(run_dir: str, evs: List[Dict[str, Any]], status: str, label: str, key: str, error: Optional[str] = None) -> None:
    """One run: stage summary, step log, files with preview, findings, download. Used for live and completed runs."""
    st.markdown(f"**{label}**  \nStatus: {JOB_TEXT.get(status, status)}" + (f"  \nFolder: `{run_dir}`" if run_dir else ""))
    sts = stage_status(evs)
    for col, (k, title) in zip(st.columns(len(STAGES)), STAGES):
        s, secs = sts.get(k, (None, None))
        col.markdown(f"**{title}**  \n{STAGE_TEXT.get(s, s)}" + (f" ({secs} s)" if secs is not None else ""))
    if status == "failed" and error:
        st.error(error)
    lines = step_lines(evs)
    with st.expander(f"Execution steps ({len(lines)})", expanded=status in ("running", "queued", "failed")):
        st.code("\n".join(lines[-400:]) or "No steps have been recorded yet.", language=None)

    all_files = run_files(run_dir) if run_dir and os.path.isdir(run_dir) else []
    labels = {e.get("name"): e.get("label") for e in evs if e.get("kind") == "file"}
    show_tech = st.checkbox("Show technical files (run record, event log, traceability record)", key=f"tech_{key}")
    files = all_files if show_tech else [f for f in all_files if not f["name"].startswith(TECH_PREFIXES)]
    st.markdown(f"**Files ({len(files)})**")
    if files:
        st.dataframe(pd.DataFrame([{"File": f["name"], "Description": labels.get(f["name"], ""), "Size": fmt_size(f["size"])}
                                   for f in files]), use_container_width=True, hide_index=True)
        names = [f["name"] for f in files]
        pick = st.selectbox("Select a file to view", names, index=len(names) - 1, key=f"pick_{key}")
        show_file(os.path.join(run_dir, pick))
        st.download_button("Download all files (ZIP)", zip_run(run_dir), file_name=os.path.basename(run_dir) + ".zip",
                           key=f"zip_{key}")
    else:
        st.caption("No files have been produced yet.")
    fdf = findings_df(run_dir)
    if fdf is not None:
        st.markdown(f"**Findings recorded in the blueprint ({len(fdf)})**")
        st.dataframe(fdf, use_container_width=True, hide_index=True)


def default_snapshot():
    path = metadata_settings().get("snapshot")
    return load_snapshot(path) if path and os.path.exists(path) else None


def validation_block(run_dir: str, key: str) -> None:
    vids: Dict[str, str] = st.session_state.setdefault("val_jobs", {})
    vj = MGR.get(vids.get(run_dir, ""))
    st.caption("Validation compares the blueprint with the source (coverage of reads, writes, constants, operators, joins "
               "and windows), confirms that each operation is executable, and tests the blueprint's sensitivity to its own logic.")
    if st.button("Validate YAML Blueprint", key=f"valbtn_{key}", disabled=bool(vj and vj.active)):
        try:
            vids[run_dir] = MGR.submit_validation(run_dir, snapshot=default_snapshot(),
                                                  aliases=metadata_settings().get("aliases")).id
            rerun()
        except Exception as e:
            st.error(f"Validation could not be started: {e}")
    if vj:
        st.markdown(f"Validation status: {JOB_TEXT.get(vj.status, vj.status)} ({vj.seconds} s)")
        notes = [e.get("message") for e in vj.events.snapshot() if e.get("kind") == "note"]
        if notes:
            st.caption(" | ".join(str(n) for n in notes[-3:]))
        if vj.error:
            st.error(vj.error)
        if vj.result:
            st.markdown(f"**Validation result: {vj.result['result']}**")
            with st.expander("Validation report", expanded=True):
                st.markdown(vj.result["markdown"])


# ---- sidebar ------------------------------------------------------------------------------------------------
with st.sidebar:
    st.header("Migration Workbench")
    st.write(f"Catalog metadata: `{metadata_settings().get('snapshot') or 'not configured'}`")
    st.write(f"Output folder: `{OUT_DIR}`")
    st.write("Validation engine (Spark): " + ("Available" if spark_unavailable_reason() is None
                                              else "Unavailable; only the static checks are performed"))
    st.caption("Credentials are read from the environment or the local configuration file and are never entered on this page.")


tab_mig, tab_runs, tab_q = st.tabs(["Migration", "Run History", "Data Query (Hive / Trino)"])


# ---- Migration -------------------------------------------------------------------------------------------------
def build_snapshot(choice: str, uploads: list):
    if choice == CATALOG_NONE:
        return None
    if choice == CATALOG_DEFAULT:
        return default_snapshot()
    if not uploads:
        raise ValueError("Upload at least one DDL or YAML file, or select a different catalog option.")
    folder = os.path.join(OUT_DIR, "_uploads", uuid.uuid4().hex[:8], "catalog")
    os.makedirs(folder, exist_ok=True)
    for u in uploads:
        with open(os.path.join(folder, os.path.basename(u.name)), "wb") as f:
            f.write(u.getvalue())
    return load_snapshot(folder)


def parse_aliases(text: str) -> Dict[str, str]:
    out = dict(metadata_settings().get("aliases") or {})
    for line in (text or "").splitlines():
        if "=" in line and not line.strip().startswith("#"):
            a, b = line.split("=", 1)
            if a.strip() and b.strip():
                out[a.strip()] = b.strip()
    return out


def live(fn):
    """Refresh a panel every two seconds without rerunning the whole page (Streamlit 1.37 or later; otherwise a Refresh button is shown)."""
    frag = getattr(st, "fragment", None)
    return frag(run_every=2)(fn) if frag else fn


@live
def job_panel() -> None:
    jobs = [j for j in (MGR.get(i) for i in st.session_state.get("job_ids", [])) if j]
    if not jobs:
        st.info("No migration has been started in this session. Select an input and choose Start Migration; the stages, "
                "steps and files are displayed here as they are produced.")
        return
    jobs = sorted(jobs, key=lambda j: j.started, reverse=True)
    labels = {j.id: f"{j.label}  [{JOB_TEXT.get(j.status, j.status)}]" for j in jobs}
    ids = list(labels)
    cur = st.session_state.get("current_job")
    pick = st.selectbox("Migration job", ids, index=ids.index(cur) if cur in ids else 0, format_func=lambda i: labels[i], key="job_pick")
    job = MGR.get(pick)
    if not getattr(st, "fragment", None):
        st.button("Refresh", key="refresh_btn")
    render_run(job.run_dir, job.events.snapshot(), job.status, job.label, key=job.id, error=job.error)
    if job.status == "done" and job.run_dir:
        validation_block(job.run_dir, job.id)


with tab_mig:
    c1, c2 = st.columns([5, 7], gap="large")
    with c1:
        st.subheader("1. Source")
        mode = st.radio("Source of the legacy job", ["Upload files", "Git repository"], horizontal=True, key="mode")
        picked: List[Dict[str, Any]] = []
        if mode == "Upload files":
            ups = st.file_uploader("Supported types: Oozie XML, PySpark 2 (.py), Sqoop (.sh), HiveQL (.hql, .sql), Excel (.xlsx)",
                                   type=[e.lstrip(".") for e in gitsource.SOURCE_EXTS], accept_multiple_files=True, key="uploads")
            picked = [{"kind": "upload", "file": u} for u in ups or []]
        else:
            url = st.text_input("Repository URL (HTTPS)", key="git_url", placeholder="https://github.com/organisation/repository.git")
            ref = st.text_input("Branch or tag (optional)", key="git_ref")
            sub = st.text_input("Folder within the repository (optional)", key="git_sub")
            token = st.text_input("Access token for private repositories (not stored)", type="password", key="git_token")
            if st.button("Retrieve Files", key="git_fetch"):
                with st.spinner("Retrieving the repository (shallow, read-only clone)..."):
                    try:
                        old = st.session_state.pop("git", None)
                        if old:
                            gitsource.cleanup(old["root"])
                        root, commit = gitsource.clone_repo(url, ref or None, token or None)
                        st.session_state["git"] = {"root": root, "commit": commit, "url": url.strip(), "files": gitsource.list_sources(root, sub)}
                    except gitsource.GitSourceError as e:
                        st.error(str(e))
            g = st.session_state.get("git")
            if g:
                st.caption(f"{g['url']} at commit {g['commit']}: {len(g['files'])} candidate file(s) found.")
                sel = st.multiselect("Files to migrate", g["files"], key="git_sel")
                if sel:
                    with st.expander("Preview of the first selected file"):
                        st.code(gitsource.read_text(g["root"], sel[0]), language=LANG.get(os.path.splitext(sel[0])[1].lower()))
                picked = [{"kind": "git", "rel": r} for r in sel]

        with st.expander("Advanced Options"):
            stype = st.selectbox("Source type", SOURCE_TYPES, key="stype", help="With 'auto', the type is detected from the file.")
            mchoice = st.radio("Catalog metadata (table schemas)", [CATALOG_DEFAULT, CATALOG_UPLOAD, CATALOG_NONE], key="mchoice")
            mups = st.file_uploader("DDL (.hql, .sql) or canonical YAML / JSON", type=["hql", "sql", "ddl", "yaml", "yml", "json"],
                                    accept_multiple_files=True, key="mups") if mchoice == CATALOG_UPLOAD else []
            alias_text = st.text_area("Table aliases (job_name=catalog_name, one per line; required only where the names differ)",
                                      key="aliases", height=70)

        if st.button("Start Migration", type="primary", disabled=not picked, key="start"):
            try:
                snap = build_snapshot(mchoice, mups or [])
                aliases = parse_aliases(alias_text)
                for p in picked:
                    if p["kind"] == "upload":
                        path, label = save_upload(OUT_DIR, p["file"].name, p["file"].getvalue()), f"Upload: {p['file'].name}"
                    else:
                        g = st.session_state["git"]
                        path = gitsource.copy_file(g["root"], p["rel"], os.path.join(OUT_DIR, "_uploads", uuid.uuid4().hex[:8]))
                        label = f"Git: {g['url']} at {g['commit']}: {p['rel']}"
                    job = MGR.submit_migration(path, label, OUT_DIR, source_type=stype, snapshot=snap, aliases=aliases)
                    st.session_state.setdefault("job_ids", []).append(job.id)
                    st.session_state["current_job"] = job.id
                    st.session_state["job_pick"] = job.id   # the progress panel displays the newest job
                rerun()
            except Exception as e:
                st.error(f"The migration could not be started: {e}")
    with c2:
        st.subheader("2. Execution Progress")
        job_panel()


# ---- Run History -------------------------------------------------------------------------------------------------
with tab_runs:
    runs = list_runs(OUT_DIR)
    if not runs:
        st.info(f"No runs are available in {OUT_DIR}/.")
    else:
        names = [r["name"] for r in runs]

        def run_label(n: str) -> str:
            r = next(x for x in runs if x["name"] == n)
            return f"{n}  [{JOB_TEXT.get(r.get('status'), r.get('status'))}]  {(r.get('input') or {}).get('label', '')}"[:140]

        sel = st.selectbox("Run", names, format_func=run_label, key="hist_pick")
        r = next(x for x in runs if x["name"] == sel)
        render_run(r["path"], run_events(r["path"]), r.get("status", "incomplete"),
                   (r.get("input") or {}).get("label") or sel, key="hist_" + sel, error=r.get("error"))
        if r.get("status") == "done":
            validation_block(r["path"], "hist_" + sel)


# ---- Data Query ------------------------------------------------------------------------------------------------------
def _unique(cols: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out = []
    for c in cols:
        seen[c] = seen.get(c, 0) + 1
        out.append(c if seen[c] == 1 else f"{c}_{seen[c]}")
    return out


def _use_sql(sql: str) -> None:
    st.session_state["q_sql"] = sql


with tab_q:
    qs = query_settings()
    st.caption("Access is read-only. Exactly one SELECT, WITH, VALUES, SHOW, DESCRIBE or EXPLAIN statement is permitted per "
               "execution. Result sets are limited in size and each query is subject to a time limit. Results are displayed on "
               "this page only. A database account with read-only privileges should be used.")
    engine = st.radio("Engine", ["Hive", "Trino"], horizontal=True, key="q_engine").lower()
    d = qs[engine]
    with st.expander("Connection settings", expanded=not d["host"]):
        a, b, c, e = st.columns(4)
        host = a.text_input("Host", value=d["host"], key=f"{engine}_host")
        port = b.number_input("Port", value=int(d["port"]), step=1, key=f"{engine}_port")
        if engine == "hive":
            user = c.text_input("Username", value=d["username"], key="hive_user")
            auth = e.selectbox("Authentication", ["NONE", "LDAP", "CUSTOM", "KERBEROS"],
                               index=["NONE", "LDAP", "CUSTOM", "KERBEROS"].index(str(d["auth"]).upper()) if str(d["auth"]).upper() in ["NONE", "LDAP", "CUSTOM", "KERBEROS"] else 0,
                               key="hive_auth")
            database = st.text_input("Database", value=d["database"], key="hive_db")
        else:
            user = c.text_input("User", value=d["user"], key="trino_user")
            scheme = e.selectbox("Protocol", ["http", "https"], index=1 if str(d["http_scheme"]).lower() == "https" else 0, key="trino_scheme")
            f1, f2, f3 = st.columns(3)
            catalog = f1.text_input("Catalog", value=d["catalog"], key="trino_catalog")
            schema = f2.text_input("Schema", value=d["schema"], key="trino_schema")
            verify = f3.checkbox("Verify the TLS certificate", value=bool(d["verify"]), key="trino_verify")
        pw = st.text_input(f"Password (not stored; alternatively set the environment variable {d['password_env']})", type="password", key=f"{engine}_pw")
    password = pw or os.getenv(d["password_env"], "")
    conn: Dict[str, Any] = ({"host": host, "port": port, "username": user, "auth": auth, "database": database, "password": password}
                            if engine == "hive" else
                            {"host": host, "port": port, "user": user, "http_scheme": scheme, "catalog": catalog, "schema": schema,
                             "verify": verify, "password": password})
    kw = dict(row_limit=int(qs["row_limit"]), timeout=int(qs["timeout_seconds"]), max_row_limit=int(qs["max_row_limit"]), log_path=qs["log"])

    if st.button("Test Connection", key=f"{engine}_test"):
        try:
            r0 = run_query(engine, conn, "SELECT 1", **kw)
            st.success(f"The connection was established successfully ({r0.seconds} s).")
        except (QueryError, QueryRejected) as ex:
            st.error(str(ex))

    left, right = st.columns([3, 9], gap="large")
    with left:
        st.markdown("**Schema browser**")
        dbk, tbk = f"{engine}_dbs", f"{engine}_tables"
        if st.button("Load Databases" if engine == "hive" else "Load Schemas", key=f"{engine}_load_dbs"):
            try:
                st.session_state[dbk] = list_databases(engine, conn, **kw)
            except (QueryError, QueryRejected) as ex:
                st.error(str(ex))
        dbs = st.session_state.get(dbk, [])
        db = st.selectbox("Database" if engine == "hive" else "Schema", dbs, key=f"{engine}_db_pick") if dbs else None
        if db and st.button("Load Tables", key=f"{engine}_load_tables"):
            try:
                st.session_state[tbk] = list_tables(engine, conn, db, **kw)
            except (QueryError, QueryRejected) as ex:
                st.error(str(ex))
        tables = st.session_state.get(tbk, [])
        table = st.selectbox("Table", tables, key=f"{engine}_table_pick") if tables and db else None
        if db and table:
            qualified = f"{db}.{table}" if engine == "hive" else f"{conn.get('catalog') or 'hive'}.{db}.{table}"
            st.button("Sample 100 Rows", key=f"{engine}_sample", on_click=_use_sql, args=(f"SELECT * FROM {qualified} LIMIT 100",))
            if st.button("Describe Table", key=f"{engine}_describe"):
                try:
                    st.session_state["q_result"] = describe_table(engine, conn, db, table, **kw)
                except (QueryError, QueryRejected) as ex:
                    st.error(str(ex))
    with right:
        st.session_state.setdefault("q_sql", "SHOW DATABASES" if engine == "hive" else "SHOW CATALOGS")
        sql = st.text_area("SQL statement", key="q_sql", height=170)
        g1, g2 = st.columns(2)
        row_limit = g1.number_input("Maximum rows", min_value=1, max_value=int(qs["max_row_limit"]), value=int(qs["row_limit"]), key="q_rows")
        timeout = g2.number_input("Time limit (seconds)", min_value=5, max_value=3600, value=int(qs["timeout_seconds"]), key="q_timeout")
        if st.button("Execute Query", type="primary", key="q_run"):
            try:
                with st.spinner("Executing the query..."):
                    res = run_query(engine, conn, sql, row_limit=int(row_limit), timeout=int(timeout),
                                    max_row_limit=int(qs["max_row_limit"]), log_path=qs["log"])
                st.session_state["q_result"] = res
                hist = st.session_state.setdefault("q_hist", [])
                hist.insert(0, res.sql)
                del hist[15:]
            except QueryRejected as ex:
                st.error(f"The statement was rejected (read-only access): {ex}")
            except QueryError as ex:
                st.error(str(ex))
        res = st.session_state.get("q_result")
        if res is not None:
            df = pd.DataFrame(res.rows, columns=_unique(res.columns)) if res.columns else pd.DataFrame(res.rows)
            st.caption(f"{res.engine.capitalize()}: {len(res.rows)} row(s) returned"
                       + (f"; the result was truncated at the limit of {res.row_limit} rows" if res.truncated else "")
                       + f"; elapsed time {res.seconds} s.")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"), file_name=f"{res.engine}_result.csv", key="q_csv")
        hist = st.session_state.get("q_hist", [])
        if hist:
            with st.expander("Recent statements"):
                for i, h in enumerate(hist):
                    st.code(h, language="sql")
                    st.button("Use This Statement", key=f"q_use_{i}", on_click=_use_sql, args=(h,))
