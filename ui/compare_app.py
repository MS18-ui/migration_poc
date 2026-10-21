"""Spark 2 to Spark 3 comparison, Sqoop jobs to intermediate YAML, and job code execution, through the migration API
(agentic/api.py).

This page calls the API over HTTP, so it does not import the framework. Start the API first:

    python -m agentic.api --host 127.0.0.1 --port 8000

then the page:

    python -m streamlit run ui/compare_app.py --server.port 8502

The API address and the execution token can be set in the sidebar, or through the environment variables
MIGRATION_API_URL and FRAMEWORK_API_TOKEN. The token is never written to disk by this page.
"""
import io
import os
import zipfile

import requests
import streamlit as st
import streamlit.components.v1 as components

DEFAULT_URL = os.environ.get("MIGRATION_API_URL", "http://127.0.0.1:8000")
DEFAULT_TOKEN = os.environ.get("FRAMEWORK_API_TOKEN", "")
RUN_DATE_DEFAULT = "2026-10-18"

st.set_page_config(page_title="Migration Tools", layout="wide")


def api(method: str, base: str, path: str, token: str = "", json_body=None, timeout: int = 60):
    """Call the API. Returns (data, error). Errors are returned as text, never raised."""
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        r = requests.request(method, base.rstrip("/") + path, json=json_body, headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        return None, f"The API could not be reached at {base}. Check that it is running. ({exc})"
    if r.status_code >= 400:
        try:
            detail = r.json().get("detail", r.text)
        except ValueError:
            detail = r.text
        return None, f"The API returned HTTP {r.status_code}: {detail}"
    return r.json(), None


def read_text(path: str):
    """Read a report file written by the API. Works when the API and this page share a machine."""
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return None


def names(values) -> str:
    return ", ".join(values) if values else "-"


# ---------- sidebar ----------

with st.sidebar:
    st.header("Connection")
    base_url = st.text_input("API address", value=DEFAULT_URL)
    token = st.text_input("Execution token", value=DEFAULT_TOKEN, type="password",
                          help="Needed only to check or run code. Set FRAMEWORK_API_TOKEN on the API host.")
    if st.button("Check API"):
        data, err = api("GET", base_url, "/api/health")
        if err:
            st.error(err)
        else:
            st.success("The API is available.")
            if data.get("execution_enabled"):
                st.info("Code execution is enabled.")
            else:
                st.info("Code execution is disabled on the API host.")

st.title("Migration Tools")
tab_compare, tab_sqoop, tab_run = st.tabs(["Compare Spark 2 and Spark 3", "Sqoop jobs to YAML", "Check or run job code"])

# ---------- compare ----------

with tab_compare:
    st.write("Upload the Spark 2 job and its Spark 3 version. The comparison is performed by the API and does not use "
             "an LLM or Spark.")
    c1, c2 = st.columns(2)
    with c1:
        up2 = st.file_uploader("Spark 2 job", type=["py"], key="up_spark2")
    with c2:
        up3 = st.file_uploader("Spark 3 job", type=["py"], key="up_spark3")

    if st.button("Compare", type="primary", disabled=not (up2 and up3)):
        body = {
            "spark2": {"name": up2.name, "code": up2.getvalue().decode("utf-8")},
            "spark3": {"name": up3.name, "code": up3.getvalue().decode("utf-8")},
        }
        with st.spinner("Comparing the two jobs..."):
            data, err = api("POST", base_url, "/api/compare", json_body=body)
        if err:
            st.session_state.pop("comparison", None)
            st.error(err)
        else:
            st.session_state["comparison"] = data

    result = st.session_state.get("comparison")
    if result:
        s = result["summary"]
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Sections", s["sections"])
        m2.metric("Rewritten", s["rewritten"])
        m3.metric("Same logic", s["same logic"])
        m4.metric("Lines of code, Spark 2", s["loc"]["spark2"])
        m5.metric("Lines of code, Spark 3", s["loc"]["spark3"])

        st.subheader("Step by step")
        st.dataframe(
            [{"Section": st_["section"], "Title": st_["title"], "Spark 2 LOC": st_["spark2_loc"],
              "Spark 3 LOC": st_["spark3_loc"], "Result": st_["result"],
              "Calls only in Spark 2": names(st_["only_spark2"]),
              "Calls only in Spark 3": names(st_["only_spark3"])} for st_ in result["steps"]],
            use_container_width=True, hide_index=True)

        st.subheader("API evidence")
        st.dataframe(
            [{"Construct": e["construct"], "Spark 2": e["spark2"], "Spark 3": e["spark3"],
              "What it means": e["meaning"]} for e in result["evidence"]],
            use_container_width=True, hide_index=True)

        st.subheader("Differences that can change the result")
        if result["differences"]:
            st.dataframe(result["differences"], use_container_width=True, hide_index=True)
        else:
            st.write("None found by static comparison. Run both versions on the same data before relying on this.")

        st.subheader("Side-by-side diff")
        files = result["files"]
        html_doc = read_text(files["html"])
        if html_doc:
            components.html(html_doc, height=700, scrolling=True)
        else:
            st.info("The diff is not shown because the report files are not on this machine. "
                    f"They are in {result['folder']}.")

        d1, d2, d3 = st.columns(3)
        for col, key, label, mime in [(d1, "markdown", "Download report (Markdown)", "text/markdown"),
                                      (d2, "html", "Download diff (HTML)", "text/html"),
                                      (d3, "json", "Download record (JSON)", "application/json")]:
            text = read_text(files[key])
            if text is not None:
                col.download_button(label, text, file_name=os.path.basename(files[key]), mime=mime)
        st.caption(f"Saved to {result['folder']}")

# ---------- sqoop to yaml ----------

with tab_sqoop:
    st.write("Upload a file of Sqoop commands (one per line; `\\` continuations and `sqoop job --create` saved jobs "
             "work) and, optionally, the source database's CREATE TABLE statements for the column mappings. The API "
             "writes one draft YAML file per job. The conversion does not use an LLM or Sqoop.")
    c1, c2 = st.columns(2)
    with c1:
        up_sqoop = st.file_uploader("Sqoop commands", type=["txt", "sh"], key="up_sqoop")
    with c2:
        up_ddl = st.file_uploader("Source table DDL (optional)", type=["sql", "ddl", "hql"], key="up_ddl")

    with st.expander("Target and metadata settings"):
        t1, t2 = st.columns(2)
        project_id = t1.text_input("Project id", value="project_01")
        target_catalog = t2.text_input("Iceberg catalog", help="Never guessed: if left empty, a finding blocks generation.")
        target_namespace = t1.text_input("Iceberg namespace")
        target_storage = t2.text_input("Warehouse location", help="For example hdfs://<namenode>:9000/user/hive/warehouse/<namespace>.db")
        trino_url = t1.text_input("Trino URL (instead of DDL, optional)", disabled=bool(up_ddl),
                                  help="Reads the column metadata from Trino's information_schema, read-only.")
        trino_schema = t2.text_input("Trino catalog.schema", disabled=bool(up_ddl), help="For example hive.sqoop_poc_spark")

    if st.button("Convert to YAML", type="primary", disabled=not up_sqoop):
        body = {"sqoop": {"name": up_sqoop.name, "code": up_sqoop.getvalue().decode("utf-8")},
                "project_id": project_id.strip() or "project_01"}
        if up_ddl:
            body["ddl"] = {"name": up_ddl.name, "code": up_ddl.getvalue().decode("utf-8")}
        elif trino_url.strip():
            body.update(trino_url=trino_url.strip(), trino_schema=trino_schema.strip() or None)
        for key, value in [("target_catalog", target_catalog), ("target_namespace", target_namespace),
                           ("target_storage", target_storage)]:
            if value.strip():
                body[key] = value.strip()
        with st.spinner("Converting the Sqoop jobs..."):
            data, err = api("POST", base_url, "/api/sqoop-yaml", json_body=body, timeout=180)
        if err:
            st.session_state.pop("sqoop_yaml", None)
            st.error(err)
        else:
            st.session_state["sqoop_yaml"] = data

    result = st.session_state.get("sqoop_yaml")
    if result:
        s = result["summary"]
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Jobs", s["jobs"])
        m2.metric("Full loads", s["full"])
        m3.metric("Incremental loads", s["incremental"])
        m4.metric("Findings", s["findings"])
        m5.metric("Blocking execution", s["blocking_execution"])
        st.caption("Every file is a proposed draft (execution_eligible: false) until a sqoop_to_spark schema and "
                   "capability registry are installed and the findings are reviewed.")
        for w in result["warnings"]:
            st.warning(w)

        st.subheader("Jobs")
        st.dataframe(
            [{"Job": j["job_id"], "Lines": j["lines"], "Saved job": j["saved_job"] or "-", "Load mode": j["load_mode"],
              "Source": j["source"], "Columns mapped": j["columns"], "Findings": j["findings"],
              "Blocking execution": j["blocking_execution"]} for j in result["jobs"]],
            use_container_width=True, hide_index=True)

        st.subheader("Findings")
        st.dataframe(
            [{"Job": f["job"], "Id": f["id"], "Category": f["category"], "Severity": f["severity"],
              "Blocks": f["blocks"], "Evidence": f["evidence"], "Resolution": f["resolution"]}
             for f in result["findings"]],
            use_container_width=True, hide_index=True)

        st.subheader("Coverage: how every Sqoop option was handled")
        st.dataframe(
            [{"Job": c["job"], "Option": c["construct"], "Status": c["status"],
              "YAML path, finding or reason": c["detail"]} for c in result["coverage"]],
            use_container_width=True, hide_index=True)

        st.subheader("YAML")
        yamls = result["yaml"]
        if yamls:
            job = st.selectbox("Job", list(yamls))
            st.code(yamls[job], language="yaml")
            yaml_files = result["files"]["yaml"]
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for job_id, text in yamls.items():
                    zf.writestr(os.path.basename(yaml_files[job_id]), text)
            y1, y2, y3, y4 = st.columns(4)
            y1.download_button("Download this YAML", yamls[job], file_name=os.path.basename(yaml_files[job]),
                               mime="application/x-yaml")
            y2.download_button("Download all YAML (ZIP)", buf.getvalue(),
                               file_name=os.path.basename(result["folder"]) + ".zip", mime="application/zip")
            for col, key, label, mime in [(y3, "markdown", "Download summary (Markdown)", "text/markdown"),
                                          (y4, "json", "Download summary (JSON)", "application/json")]:
                text = read_text(result["files"][key])
                if text is not None:
                    col.download_button(label, text, file_name=os.path.basename(result["files"][key]), mime=mime)
        else:
            st.info("No Sqoop command was found in the file.")
        st.caption(f"Saved to {result['folder']}")

# ---------- run ----------

with tab_run:
    st.write("Check (compile) or run a job's code on the API host. Execution needs the token in the sidebar. "
             "Running a Spark job uses the API host's Spark setup.")
    up_run = st.file_uploader("Load code from a file (optional)", type=["py"], key="up_run")
    default_code = up_run.getvalue().decode("utf-8") if up_run else "print('hello from the API')"
    code = st.text_area("Python code", value=default_code, height=320, key=f"code_{up_run.name if up_run else 'none'}")

    r1, r2, r3 = st.columns([1, 1, 1])
    mode = r1.radio("Mode", ["check", "run"], horizontal=True,
                    help="check = compile only. run = execute the code.")
    runner = r2.selectbox("Runner", ["python", "spark-submit"],
                          help="spark-submit requires Spark on the API host.")
    timeout_s = r3.number_input("Timeout (seconds)", min_value=1, max_value=1800, value=120)
    run_date = st.text_input("RUN_DATE environment variable (optional)", value=RUN_DATE_DEFAULT,
                             help="Passed to the job for jobs that read RUN_DATE. Leave empty to omit it.")

    if st.button("Execute", type="primary", disabled=not code.strip()):
        env = {"RUN_DATE": run_date.strip()} if run_date.strip() else {}
        body = {"code": code, "mode": mode, "runner": runner, "timeout_seconds": int(timeout_s), "env": env}
        with st.spinner("Waiting for the API..."):
            data, err = api("POST", base_url, "/api/execute", token=token, json_body=body,
                            timeout=int(timeout_s) + 30)
        if err:
            st.error(err)
        elif data["status"] == "passed":
            st.success(f"Passed. Exit code {data.get('exit_code', 0)}, {data.get('duration_s', 0)} seconds."
                       if mode == "run" else "Syntax check passed.")
        else:
            st.error(f"{data['status'].capitalize()}: {data.get('error') or 'see the output below'}")
        if data and mode == "run":
            if data.get("stdout"):
                st.subheader("Output")
                st.code(data["stdout"])
            if data.get("stderr"):
                st.subheader("Errors")
                st.code(data["stderr"])
