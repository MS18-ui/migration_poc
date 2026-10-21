"""Spark 2 to Spark 3 comparison and job code execution, through the migration API (agentic/api.py).

This page calls the API over HTTP, so it does not import the framework. Start the API first:

    python -m agentic.api --host 127.0.0.1 --port 8000

then the page:

    python -m streamlit run ui/compare_app.py --server.port 8502

The API address and the execution token can be set in the sidebar, or through the environment variables
MIGRATION_API_URL and FRAMEWORK_API_TOKEN. The token is never written to disk by this page.
"""
import os

import requests
import streamlit as st
import streamlit.components.v1 as components

DEFAULT_URL = os.environ.get("MIGRATION_API_URL", "http://127.0.0.1:8000")
DEFAULT_TOKEN = os.environ.get("FRAMEWORK_API_TOKEN", "")
RUN_DATE_DEFAULT = "2026-10-18"

st.set_page_config(page_title="Spark Job Comparison and Execution", layout="wide")


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

st.title("Spark Job Comparison and Execution")
tab_compare, tab_run = st.tabs(["Compare Spark 2 and Spark 3", "Check or run job code"])

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
