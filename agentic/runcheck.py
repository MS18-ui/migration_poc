"""Run-and-diff validation, exposed as agent tools.

  run_pipeline : execute the candidate script on generated test data (catches runtime errors
                 that syntax checks cannot: bad column refs, type errors, nested-type misuse)
  diff_vs_baseline : run the candidate AND a trusted baseline script on the SAME data and
                 compare their target tables (schema, column order, row count, per-column values)

Needs `pyspark` and a Java runtime. Standalone use:
  python -m agentic.runcheck --spec output/pipeline_spec.yaml --script output/generated_pipeline.py \
         [--baseline Claude_op/daily_txn_risk_job_spark3.py]
"""
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable, Dict, List, Optional

from .diffing import compare_outputs
from .schemas import PipelineBlueprint, SchemaField
from .sqlguard import scan_cluster_safety
from .testdata import field_type_str, generate
from .tools import ToolSpec
from .validation import strip_fences

RUNNER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_spark_runner.py")


def spark_available() -> Optional[str]:
    """None if a local Spark run is possible, else the reason it is not."""
    if importlib.util.find_spec("pyspark") is None:
        return "pyspark is not installed (pip install pyspark)"
    if not (shutil.which("java") or os.getenv("JAVA_HOME")):
        return "no Java runtime found (install a JDK 8/11/17 and set JAVA_HOME)"
    return None


def _target_ddl(name: str, schema: List[SchemaField], partition_by: List[str]) -> str:
    cols = {f.name: field_type_str(f) for f in schema}
    for p in partition_by:
        cols.setdefault(p, "STRING")
    data = [f"`{c}` {t}" for c, t in cols.items() if c not in partition_by]
    part = [f"`{p}` {cols[p]}" for p in partition_by]
    ddl = f"CREATE TABLE {name} ({', '.join(data + part)}) USING parquet"
    return ddl + (f" PARTITIONED BY ({', '.join(f'`{p}`' for p in partition_by)})" if partition_by else "")


class RunCheck:
    def __init__(self, bp: PipelineBlueprint, settings: Dict[str, Any] = None,
                 runner: Callable[[dict], dict] = None):
        """`runner(spec) -> result` is injectable for tests; default spawns _spark_runner.py."""
        self.bp, self.cfg = bp, settings or {}
        self.runner = runner or self._subprocess_runner
        self.work = tempfile.mkdtemp(prefix="runcheck_")
        self.mode = self.cfg.get("mode", "local")
        if self.mode not in ("local", "cluster"):
            raise ValueError(f"validation.mode must be 'local' or 'cluster', got '{self.mode}'")
        self.cluster = dict(self.cfg.get("cluster") or {})
        self.params = {**bp.parameters, **(self.cluster.get("params") or {} if self.mode == "cluster" else {})}
        self._data_dirs: Dict[str, str] = {}
        self._baseline_cache: Dict[str, dict] = {}
        if self.mode == "cluster":
            self._check_cluster_config()
        else:
            self._build_data()

    def _check_cluster_config(self):
        if not self.cluster.get("master"):
            raise ValueError("validation.cluster.master is required in cluster mode.")
        if not (self.cluster.get("scratch_db_prefix") or "").strip():
            raise ValueError("validation.cluster.scratch_db_prefix must be set (it is the only namespace "
                             "the run may write to or drop).")
        if not self.cfg.get("baseline_table_map"):
            raise ValueError("cluster mode needs validation.baseline_table_map to map blueprint tables to "
                             "the real tables on the cluster.")

    # ---- data ----------------------------------------------------------------------------
    def _build_data(self):
        hook = self.cfg.get("data_script")
        if hook:
            spec = importlib.util.spec_from_file_location("runcheck_data_hook", hook)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            data = mod.generate(self.bp, self.params)
        else:
            data = generate(self.bp, self.params, n_rows=int(self.cfg.get("rows", 2000)),
                            seed=int(self.cfg.get("seed", 7)))
        self.data = data
        for sid, d in data.items():
            path = os.path.join(self.work, "data", sid)
            os.makedirs(path, exist_ok=True)
            with open(os.path.join(path, "part.json"), "w", encoding="utf-8") as f:
                for r in d["rows"]:
                    f.write(json.dumps(r, default=str) + "\n")
            self._data_dirs[sid] = path

    # ---- execution -----------------------------------------------------------------------
    @staticmethod
    def _subprocess_runner(spec: dict) -> dict:
        sp = spec["out"] + ".spec.json"
        json.dump(spec, open(sp, "w", encoding="utf-8"))
        try:
            p = subprocess.run([sys.executable, RUNNER, sp], capture_output=True, text=True,
                               timeout=spec["timeout"])
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": f"timed out after {spec['timeout']}s", "targets": {}}
        if not os.path.exists(spec["out"]):
            return {"ok": False, "error": (p.stderr or p.stdout)[-3000:], "targets": {}}
        return json.load(open(spec["out"], encoding="utf-8"))

    def _cluster_spec(self, label: str, script_path: str, table_map: Dict[str, str], argv, env) -> dict:
        """Reads: real tables. Writes: only into a per-run scratch database."""
        prefix = self.cluster["scratch_db_prefix"].strip()
        scratch = f"{prefix}{time.strftime('%m%d%H%M%S')}_{os.getpid()}_{label}"
        tmap = self.cfg.get("baseline_table_map") or {}
        read_map, write_map, targets, alias = {}, {}, [], {}
        for t in self.bp.targets:
            if not t.object:
                continue
            short = t.object.rsplit(".", 1)[-1]
            if label == "candidate":
                script_name, stem = t.object, "cand"
            else:
                script_name, stem = table_map.get(t.object, t.object), "base"
            st = f"{scratch}.{stem}_{short}"
            write_map[script_name] = st
            alias[st] = script_name
            targets.append({"name": st, "ddl": _target_ddl(st, t.schema_, t.partition_by)})
        if label == "candidate":  # candidate reads the real tables that stand in for its sources
            for s in self.bp.sources:
                key = s.object or s.path
                if key in tmap:
                    read_map[key] = tmap[key]
        return {"mode": "cluster", "script": script_path, "argv": argv, "env": env,
                "master": self.cluster["master"], "spark_conf": self.cluster.get("spark_conf") or {},
                "scratch_db": scratch, "scratch_prefix": prefix,
                "keep_scratch": bool(self.cluster.get("keep_scratch", False)),
                "read_map": read_map, "write_map": write_map, "targets": targets, "alias": alias,
                "out": os.path.join(self.work, label, "out.json"),
                "timeout": int(self.cfg.get("timeout_seconds", 600))}

    def _spec(self, label: str, script_path: str, table_map: Dict[str, str], argv, env) -> dict:
        if self.mode == "cluster":
            return self._cluster_spec(label, script_path, table_map, argv, env)
        tables, files = [], []
        for s in self.bp.sources:
            d = self.data[s.id]
            if not d["ddl"]:  # schema-less source: no table can be created for it
                continue
            if s.object:
                tables.append({"name": table_map.get(s.object, s.object), "ddl": d["ddl"],
                               "data_dir": self._data_dirs[s.id]})
            elif s.path:
                local = os.path.join(self.work, label, "files", s.id)
                files.append({"path": s.path, "ddl": d["ddl"], "data_dir": self._data_dirs[s.id],
                              "local_dir": local})
                if s.path in table_map:  # baseline reads this source from a table instead
                    tables.append({"name": table_map[s.path], "ddl": d["ddl"],
                                   "data_dir": self._data_dirs[s.id]})
        targets = [{"name": table_map.get(t.object or t.id, t.object or t.id),
                    "ddl": _target_ddl(table_map.get(t.object or t.id, t.object or t.id),
                                       t.schema_, t.partition_by)}
                   for t in self.bp.targets if t.object]
        return {"script": script_path, "argv": argv, "env": env, "tables": tables,
                "file_sources": files, "targets": targets,
                "warehouse": os.path.join(self.work, label, "warehouse"),
                "out": os.path.join(self.work, label, "out.json"),
                "timeout": int(self.cfg.get("timeout_seconds", 600))}

    def _run(self, label: str, code: str, table_map: Dict[str, str], argv, env) -> dict:
        os.makedirs(os.path.join(self.work, label), exist_ok=True)
        script = os.path.join(self.work, label, "script.py")
        with open(script, "w", encoding="utf-8") as f:
            f.write(strip_fences(code))
        spec = self._spec(label, script, table_map, argv, env)
        res = self.runner(spec)
        alias = spec.get("alias") or {}  # cluster: scratch table -> the name the script used
        res["targets"] = {alias.get(k, k): v for k, v in (res.get("targets") or {}).items()}
        res["target_names"] = [alias.get(t["name"], t["name"]) for t in spec["targets"]]
        res.setdefault("notes", [])
        return res

    def _unsafe(self, code: str) -> Optional[str]:
        """Cluster mode only: refuse code using raw JVM/JDBC/shell access the runtime guard can't see."""
        if self.mode != "cluster":
            return None
        problems = scan_cluster_safety(strip_fences(code))
        if problems:
            return ("ERROR: not run - cluster mode forbids constructs that could bypass the scratch-database "
                    "write protection: " + "; ".join(problems[:6]) +
                    ". Use only the Spark DataFrame/SQL APIs and write to the blueprint target table.")
        return None

    def run_candidate(self, code: str) -> dict:
        argv = [a for k, v in self.params.items() for a in (f"--{k.replace('_', '-')}", str(v))]
        env = {k.upper(): str(v) for k, v in self.params.items()}
        return self._run("candidate", code, {}, argv, env)

    def run_baseline(self) -> dict:
        path = self.cfg["baseline_script"]
        code = open(path, encoding="utf-8").read()
        key = hashlib.sha1(code.encode()).hexdigest()
        if key not in self._baseline_cache:
            env = {k.upper(): str(v) for k, v in self.params.items()}
            self._baseline_cache[key] = self._run(
                "baseline", code, dict(self.cfg.get("baseline_table_map") or {}), [], env)
        return self._baseline_cache[key]

    # ---- tool bodies ---------------------------------------------------------------------
    @staticmethod
    def _tail(err: str, n: int = 1400) -> str:
        return err.strip()[-n:]

    def run_tool(self, code: str) -> str:
        if (bad := self._unsafe(code)):
            return bad
        res = self.run_candidate(code)
        where = "real cluster tables (writes sandboxed to a scratch database)" if self.mode == "cluster" else "test data"
        if not res["ok"]:
            return f"ERROR: script failed when run on {where}:\n" + self._tail(res["error"])
        rows = {n: len(t["rows"]) for n, t in res["targets"].items()}
        empty = [n for n, c in rows.items() if c == 0]
        if empty:
            return (f"ERROR: script ran but target(s) {empty} have 0 rows - joins/filters drop "
                    "everything on the input; check join keys, filters and the run date.")
        return f"OK: ran on {where}; target rows {rows}. " + " ".join(res["notes"])

    def diff_tool(self, code: str) -> str:
        if (bad := self._unsafe(code)):
            return bad
        res = self.run_candidate(code)
        if not res["ok"]:
            return "ERROR: candidate failed when run:\n" + self._tail(res["error"])
        base = self.run_baseline()
        if not base["ok"]:
            return ("ERROR: the BASELINE script failed (not your fault - tell the "
                    "user):\n" + self._tail(base["error"]))
        problems, summary = [], []
        for t in self.bp.targets:
            cand_n = t.object or t.id
            base_n = (self.cfg.get("baseline_table_map") or {}).get(cand_n, cand_n)
            keys = list(self.cfg.get("diff_keys") or ([t.schema_[0].name] if t.schema_ else []))
            c, b = res["targets"].get(cand_n), base["targets"].get(base_n)
            if c is None or b is None:
                problems.append(f"target '{cand_n}': missing output (candidate wrote to the wrong table?)")
                continue
            if not c["rows"]:
                problems.append(f"target '{cand_n}': candidate produced 0 rows")
                continue
            diffs = compare_outputs(c["rows"], b["rows"], c["dtypes"], b["dtypes"], keys)
            if diffs:
                problems.append(f"target '{cand_n}' differs from baseline:\n  - " + "\n  - ".join(diffs[:8]))
            else:
                summary.append(f"{cand_n}: {len(c['rows'])} rows identical")
        if problems:
            return "ERROR: output does not match the baseline.\n" + "\n".join(problems)
        return "OK: output matches baseline (" + "; ".join(summary) + ")."


def runcheck_applicability(bp: PipelineBlueprint, settings: Dict[str, Any]) -> Dict[str, Any]:
    """Can run-and-diff work for THIS blueprint? -> {"enabled", "baseline", "notes"}.

    * local mode needs a schema for every source (test data is generated from it); without one the run-check is
      skipped, otherwise the agent would be gated on a check that can never pass
    * the baseline script is specific to one job: it is only used when this blueprint's tables appear in
      validation.baseline_table_map
    """
    notes: List[str] = []
    missing = [s.id for s in bp.sources if not s.schema_]
    if settings.get("mode", "local") == "local" and missing:
        return {"enabled": False, "baseline": False,
                "notes": [f"run-and-diff skipped: source(s) {missing} have no schema (no catalog match), so test data cannot be "
                          "generated. Supply a catalog snapshot that contains them."]}
    baseline = bool(settings.get("baseline_script"))
    if baseline:
        keys = {s.object or s.path for s in bp.sources} | {t.object for t in bp.targets if t.object}
        if not (keys & set(settings.get("baseline_table_map") or {})):
            baseline = False
            notes.append("baseline diff skipped: validation.baseline_script is configured for a different job (none of this "
                         "blueprint's tables appear in validation.baseline_table_map).")
    return {"enabled": True, "baseline": baseline, "notes": notes}


def make_runcheck_tools(bp: PipelineBlueprint, settings: Dict[str, Any], holder: Dict[str, Any],
                        runner: Callable = None) -> List[ToolSpec]:
    """Tools for the PySpark agent. `holder['verified']` records the hash of the last code that
    passed (so submit_final can require it). Returns [] (with `holder['runcheck_notes']` saying why) when
    run-and-diff cannot work for this blueprint."""
    app = runcheck_applicability(bp, settings)
    holder["runcheck_notes"] = app["notes"]
    if not app["enabled"]:
        return []
    rc = RunCheck(bp, settings, runner)
    holder["runcheck"] = rc

    def stamp(code: str, out: str) -> str:
        if out.startswith("OK"):
            holder["verified"] = hashlib.sha1(strip_fences(code).encode()).hexdigest()
        return out

    tools = [ToolSpec("run_pipeline",
                      "Execute the script on generated test data in local Spark; reports runtime errors "
                      "(with traceback) or target row counts.",
                      lambda a: stamp(a["content"], rc.run_tool(a["content"])))]
    if app["baseline"]:
        tools.append(ToolSpec(
            "diff_vs_baseline",
            "Run the script and the trusted baseline on the SAME test data and diff the target tables "
            "(columns, order, types, row count, values). Fix every reported difference.",
            lambda a: stamp(a["content"], rc.diff_tool(a["content"]))))
    return tools


def code_is_verified(holder: Dict[str, Any], code: str) -> bool:
    return holder.get("verified") == hashlib.sha1(strip_fences(code).encode()).hexdigest()


# ---- CLI ---------------------------------------------------------------------------------
def _main():
    import argparse
    from .config import validation_settings
    from .validation import ValidationEngine, load_yaml  # noqa: F401
    ap = argparse.ArgumentParser(description="Run (and diff) a generated PySpark script on test data")
    ap.add_argument("--spec", required=True, help="pipeline_spec.yaml (rich blueprint)")
    ap.add_argument("--script", required=True)
    ap.add_argument("--baseline", help="trusted script to diff against (overrides config)")
    ap.add_argument("--mode", choices=["local", "cluster"], help="overrides validation.mode")
    a = ap.parse_args()
    why = spark_available()
    if why:
        sys.exit(f"Cannot run: {why}")
    bp = ValidationEngine.validate_yaml_structure(open(a.spec, encoding="utf-8").read())
    cfg = validation_settings()
    if a.baseline:
        cfg["baseline_script"] = a.baseline
    if a.mode:
        cfg["mode"] = a.mode
    code = open(a.script, encoding="utf-8").read()
    rc = RunCheck(bp, cfg)
    if rc.mode == "cluster":
        print(f"CLUSTER MODE: reading real tables via {cfg['cluster']['master']}; writes go only to scratch "
              f"databases '{cfg['cluster']['scratch_db_prefix']}*' (dropped afterwards).")
    print(rc.run_tool(code))
    if cfg.get("baseline_script"):
        print(rc.diff_tool(code))


if __name__ == "__main__":
    _main()

