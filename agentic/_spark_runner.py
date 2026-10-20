"""Runs ONE script against test data in an isolated Spark session.

Invoked as a subprocess by runcheck.py:  python _spark_runner.py <spec.json>
Spec keys (common): mode (local|cluster), script, argv[], env{}, targets[{name, ddl}], out
  local  : warehouse, tables[{name, ddl, data_dir}], file_sources[{path, ddl, data_dir, local_dir}]
  cluster: master, spark_conf{}, scratch_db, keep_scratch, read_map{real-name-or-path: table},
           write_map{script-target: scratch table}
Writes <out> JSON: {ok, error, notes[], targets{name: {dtypes, rows}}}.

Why a subprocess: the generated script builds its own SparkSession, calls spark.stop() and may
sys.exit(); isolation keeps the framework alive and gives each run a clean warehouse/scratch DB.

Cluster mode safety (see sqlguard.py): reads hit real tables (via read_map); EVERY write is
redirected to / restricted to the scratch database, which is dropped afterwards.
"""
import json
import os
import runpy
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sqlguard  # noqa: E402


def main(spec_path: str) -> None:
    spec = json.load(open(spec_path, encoding="utf-8"))
    cluster = spec.get("mode") == "cluster"
    result = {"ok": False, "error": "", "notes": [], "targets": {}}
    phase = "setup"   # 'setup' = building the session / loading test data (an environment problem, not the script's fault)
    spark = None
    orig_sql = None
    try:
        from pyspark.sql import DataFrameReader, DataFrameWriter, SparkSession

        b = SparkSession.builder.appName("runcheck")
        if cluster:
            b = b.master(spec["master"]).enableHiveSupport()
            for k, v in (spec.get("spark_conf") or {}).items():
                b = b.config(k, v)
        else:
            # a private, in-memory catalog: a host's spark-defaults.conf (Hive metastore, cluster master...) must not leak in
            b = (b.master("local[2]").config("spark.sql.warehouse.dir", spec["warehouse"])
                 .config("spark.sql.catalogImplementation", "in-memory"))
        spark = (b.config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                 .config("spark.sql.sources.partitionOverwriteMode", "dynamic").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        orig_sql = spark.sql  # un-guarded handle, used only by this runner for setup/cleanup

        def db_of(name):
            return name.rsplit(".", 1)[0] if "." in name else None

        scratch = spec.get("scratch_db")
        read_map = {k.lower(): v for k, v in (spec.get("read_map") or {}).items()}
        write_map = {k.lower(): v for k, v in (spec.get("write_map") or {}).items()}

        if cluster:
            missing = [t for t in set(read_map.values()) if not spark.catalog.tableExists(t)]
            if missing:
                raise RuntimeError(f"source table(s) not found on the cluster: {sorted(missing)} "
                                   "- fix validation.baseline_table_map")
            orig_sql(f"CREATE DATABASE IF NOT EXISTS {scratch}")
            result["notes"].append(f"scratch database: {scratch}")
        else:
            def load(ddl, data_dir):
                return (spark.read.schema(ddl).option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
                        .json(data_dir))
            for t in spec["tables"]:
                if db_of(t["name"]):
                    orig_sql(f"CREATE DATABASE IF NOT EXISTS {db_of(t['name'])}")
                load(t["ddl"], t["data_dir"]).write.mode("overwrite").saveAsTable(t["name"])
            for f in spec["file_sources"]:
                load(f["ddl"], f["data_dir"]).write.mode("overwrite").parquet(f["local_dir"])
        for t in spec["targets"]:
            if not cluster and db_of(t["name"]):
                orig_sql(f"CREATE DATABASE IF NOT EXISTS {db_of(t['name'])}")
            orig_sql(f"DROP TABLE IF EXISTS {t['name']}")
            orig_sql(t["ddl"])

        # ---- patches: make cluster-specific session code harmless -----------------------------
        B = SparkSession.Builder
        B.master = lambda self, *a, **k: self
        B.enableHiveSupport = lambda self: self
        _cfg = B.config
        def cfg(self, key=None, value=None, **k):
            if isinstance(key, str) and key.startswith(("spark.hadoop", "hive.", "spark.sql.hive",
                                                         "spark.sql.warehouse", "spark.sql.catalogImplementation")):
                return self
            return _cfg(self, key, value, **k)
        B.config = cfg
        SparkSession.stop = lambda self: None  # keep session alive to read the target afterwards

        if cluster:
            path_to_table = {k: v for k, v in read_map.items() if "/" in k}
            name_map = {k: v for k, v in {**read_map, **write_map}.items() if "/" not in k}

            def map_table(name, writing):
                n = name.replace("`", "").lower()
                if writing:
                    mapped = write_map.get(n, name)
                    sqlguard.assert_scratch_table(mapped, scratch, "write")
                    return mapped
                return name_map.get(n, name)

            _table = SparkSession.table
            SparkSession.table = lambda self, n: _table(self, map_table(n, False))
            _rtable = DataFrameReader.table
            DataFrameReader.table = lambda self, n: _rtable(self, map_table(n, False))
            for meth in ("parquet", "orc", "csv", "json", "load"):
                orig = getattr(DataFrameReader, meth)
                def wrapped(self, *a, _orig=orig, **kw):
                    p = a[0] if a else kw.get("path")
                    if isinstance(p, str) and p.lower() in path_to_table:
                        return spark.table(path_to_table[p.lower()])
                    return _orig(self, *a, **kw)
                setattr(DataFrameReader, meth, wrapped)

            def guarded_sql(self, query, *a, **kw):
                q = sqlguard.rewrite_names(sqlguard.rewrite_names(query, write_map), name_map)
                sqlguard.check_sql(q, scratch)
                return orig_sql(q, *a, **kw)
            SparkSession.sql = guarded_sql

            _ins, _sat = DataFrameWriter.insertInto, DataFrameWriter.saveAsTable
            DataFrameWriter.insertInto = lambda self, n, *a, **k: _ins(self, map_table(n, True), *a, **k)
            DataFrameWriter.saveAsTable = lambda self, n, *a, **k: _sat(self, map_table(n, True), *a, **k)
            def blocked(name):
                def _b(self, *a, **k):
                    raise PermissionError(f"BLOCKED: DataFrameWriter.{name}() writes outside the scratch "
                                          "database; write to the blueprint target table instead.")
                return _b
            for meth in ("save", "parquet", "orc", "csv", "json", "text", "jdbc"):
                setattr(DataFrameWriter, meth, blocked(meth))
        else:
            path_map = {f["path"]: f["local_dir"] for f in spec["file_sources"]}
            for meth in ("parquet", "orc", "csv", "json", "load"):
                orig = getattr(DataFrameReader, meth)
                def wrapped(self, *a, _orig=orig, **kw):
                    a = tuple(path_map.get(x, x) if isinstance(x, str) else x for x in a[:1]) + a[1:]
                    if isinstance(kw.get("path"), str):
                        kw["path"] = path_map.get(kw["path"], kw["path"])
                    return _orig(self, *a, **kw)
                setattr(DataFrameReader, meth, wrapped)

        # ---- run ------------------------------------------------------------------------------
        phase = "script"   # from here on a failure is the generated script's
        os.environ.update(spec.get("env", {}))
        sys.argv = [spec["script"]] + spec.get("argv", [])
        sys.path.insert(0, os.path.dirname(spec["script"]))
        try:
            runpy.run_path(spec["script"], run_name="__main__")
        except SystemExit as e:
            if e.code not in (None, 0):
                raise RuntimeError(f"script exited with status {e.code}")

        for t in spec["targets"]:
            df = spark.table(t["name"]) if not cluster else orig_table(spark, t["name"])
            result["targets"][t["name"]] = {
                "dtypes": [list(x) for x in df.dtypes],
                "rows": [json.loads(r) for r in df.toJSON().collect()],
            }
        result["ok"] = True
    except BaseException:  # report everything, including SystemExit/KeyboardInterrupt
        result["error"] = traceback.format_exc()[-3500:]
        result["phase"] = phase
    finally:
        if cluster and spark is not None and orig_sql is not None and not spec.get("keep_scratch"):
            try:  # only ever drops the scratch DB created for this run
                if spec["scratch_db"].lower().startswith(spec["scratch_prefix"].lower()):
                    orig_sql(f"DROP DATABASE IF EXISTS {spec['scratch_db']} CASCADE")
                    result["notes"].append(f"scratch database {spec['scratch_db']} dropped")
            except BaseException as e:
                result["notes"].append(f"WARNING: could not drop scratch database {spec['scratch_db']}: {e}")
    json.dump(result, open(spec["out"], "w", encoding="utf-8"), default=str)


def orig_table(spark, name):
    """Read a scratch table after patches are installed (names there are already final)."""
    return spark.read.table(name)


if __name__ == "__main__":
    main(sys.argv[1])
