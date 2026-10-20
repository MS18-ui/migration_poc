"""Catalog metadata snapshot: loading, lookup (never guessing), deterministic resolution, prompt block, wiring."""
import copy
import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout

import yaml

from agentic.llm import BaseLLM, LLMResponse, ToolCall
from agentic.metadata import apply_snapshot, load_snapshot, main as metadata_cli
from agentic.orchestrator import AgenticFramework
from agentic.validation import ValidationEngine
from agentic.validator.static_checks import metadata_findings

HERE = os.path.dirname(os.path.abspath(__file__))
CATALOG = os.path.join(HERE, "fixtures", "catalog")
SNAP = load_snapshot(CATALOG)


def bp_of(d):
    return ValidationEngine.validate_yaml_structure(yaml.dump(d, sort_keys=False))


def rich(**over):
    d = {"name": "j", "job": {"name": "j"},
         "sources": [{"id": "acc", "type": "hive_table", "object": "curated.accounts", "schema": []}],
         "transformations": [{"id": "t1", "type": "select", "inputs": ["acc"], "select": ["account_id"]}],
         "targets": [{"id": "out", "input": "t1", "type": "hive_table", "object": "risk.customer_daily_risk"}]}
    d.update(over)
    return d


class Loading(unittest.TestCase):
    def test_tables_columns_partitions_location(self):
        t = SNAP.tables
        self.assertEqual(set(t), {"curated.customers", "curated.accounts", "curated.transactions", "curated.fx_rates",
                                  "ref.high_risk_countries", "risk.customer_daily_risk"})
        tx = t["curated.transactions"]
        self.assertEqual(tx.partition_columns, ["txn_date"])
        self.assertEqual(tx.column("amount").type, "DECIMAL(18,2)")
        self.assertEqual(tx.column("counterparty").type, "STRUCT<name:STRING,bank_bic:STRING,account_no:STRING,country:STRING>")
        self.assertEqual(tx.format, "parquet")
        fx = t["curated.fx_rates"]
        self.assertTrue(fx.external)
        self.assertEqual(fx.location, "hdfs:///data/ref/fx_rates/")
        self.assertEqual(fx.partition_columns, ["rate_date"])
        self.assertEqual(fx.column("rate_date").type, "DATE")
        self.assertEqual(t["curated.customers"].column("addresses").type,
                         "ARRAY<STRUCT<type:STRING,line1:STRING,city:STRING,country:STRING,is_primary:BOOLEAN>>")
        self.assertEqual(t["curated.accounts"].column("attributes").type, "MAP<STRING,STRING>")
        self.assertEqual(t["risk.customer_daily_risk"].partition_columns, ["run_date"])

    def test_non_catalog_files_in_a_folder_are_ignored(self):
        self.assertEqual(sorted(SNAP.files), ["02_create_source_tables.hql", "03_create_target_tables.hql"])

    def test_snapshot_id_is_stable_and_changes_with_content(self):
        self.assertEqual(load_snapshot(CATALOG).id, SNAP.id)
        with tempfile.TemporaryDirectory() as d:
            open(os.path.join(d, "a.hql"), "w").write("CREATE TABLE x.t (a INT);")
            s1 = load_snapshot(d).id
            open(os.path.join(d, "a.hql"), "w").write("CREATE TABLE x.t (a BIGINT);")
            self.assertNotEqual(s1, load_snapshot(d).id)

    def test_use_statement_sets_default_database_and_views_are_recorded(self):
        with tempfile.TemporaryDirectory() as d:
            open(os.path.join(d, "a.hql"), "w").write("USE mydb;\nCREATE TABLE t (a INT);\nCREATE VIEW v AS SELECT 1;")
            s = load_snapshot(d)
        self.assertEqual(set(s.tables), {"mydb.t", "mydb.v"})
        self.assertEqual(s.tables["mydb.v"].kind, "view")

    def test_canonical_yaml_round_trip(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "snap.yaml")
            yaml.dump(SNAP.to_dict(), open(p, "w"), sort_keys=False)
            back = load_snapshot(p)
        for name, t in SNAP.tables.items():
            self.assertEqual([(c.name, c.type, c.partition) for c in back.tables[name].columns],
                             [(c.name, c.type, c.partition) for c in t.columns], name)
        self.assertEqual(back.tables["curated.fx_rates"].location, "hdfs:///data/ref/fx_rates/")

    def test_missing_path_is_an_error_and_unparsable_ddl_is_a_warning(self):
        with self.assertRaises(FileNotFoundError):
            load_snapshot(os.path.join(CATALOG, "nope"))
        with tempfile.TemporaryDirectory() as d:
            open(os.path.join(d, "a.hql"), "w").write("CREATE TABLE t (a INT, b  );\nCREATE TABLE ok.t2 (a INT);")
            s = load_snapshot(d)
        self.assertIn("ok.t2", s.tables)

    def test_cli_prints_and_exports(self):
        with tempfile.TemporaryDirectory() as d:
            out = os.path.join(d, "s.yaml")
            buf = io.StringIO()
            with redirect_stdout(buf):
                self.assertEqual(metadata_cli([CATALOG, "--out", out]), 0)
            self.assertIn("curated.transactions", buf.getvalue())
            self.assertIn("curated.fx_rates", yaml.safe_load(open(out))["tables"])


class Lookup(unittest.TestCase):
    def test_exact_name_alias_and_location(self):
        self.assertEqual(SNAP.lookup("curated.accounts")[0].name, "curated.accounts")
        self.assertEqual(SNAP.lookup("CURATED.Accounts")[0].name, "curated.accounts")
        self.assertEqual(SNAP.lookup("old.acc", {"old.acc": "curated.accounts"})[0].name, "curated.accounts")
        for loc in ("hdfs:///data/ref/fx_rates/", "hdfs:///data/ref/fx_rates", "/data/ref/fx_rates/"):
            with self.subTest(loc=loc):
                self.assertEqual(SNAP.lookup(loc)[0].name, "curated.fx_rates")

    def test_an_identical_name_beats_an_alias(self):
        t, _ = SNAP.lookup("curated.accounts", {"curated.accounts": "curated.customers"})
        self.assertEqual(t.name, "curated.accounts")

    def test_never_guesses_near_misses_are_only_candidates(self):
        t, cands = SNAP.lookup("staging.transactions")
        self.assertIsNone(t)
        self.assertEqual(cands, ["curated.transactions"])

    def test_alias_to_a_missing_table_is_reported(self):
        t, cands = SNAP.lookup("x.y", {"x.y": "nope.nope"})
        self.assertIsNone(t)
        self.assertIn("not in the snapshot", cands[0])


class Resolver(unittest.TestCase):
    def test_empty_source_schema_is_filled_and_target_gets_catalog_partitioning(self):
        bp = bp_of(rich())
        s = apply_snapshot(bp, SNAP, source_ref="job.py")
        self.assertEqual([f.name for f in bp.sources[0].schema_], [c.name for c in SNAP.tables["curated.accounts"].columns])
        self.assertEqual(bp.targets[0].partition_by, ["run_date"])
        self.assertEqual(len(bp.targets[0].schema_), 13)  # partition column is not part of the data schema
        self.assertEqual(bp.sources[0].catalog["table"], "curated.accounts")
        self.assertEqual(s["unmatched"], [])

    def test_fx_path_source_resolves_through_the_table_location(self):
        d = rich(sources=[{"id": "fx", "type": "parquet", "path": "hdfs:///data/ref/fx_rates/", "schema": []}],
                 transformations=[{"id": "t1", "type": "select", "inputs": ["fx"], "select": ["currency"]}], targets=[])
        bp = bp_of(d)
        apply_snapshot(bp, SNAP)
        self.assertEqual(bp.sources[0].catalog["table"], "curated.fx_rates")
        self.assertIn("rate_date", [f.name for f in bp.sources[0].schema_])

    def test_declared_type_conflict_is_corrected_with_a_finding(self):
        d = rich(sources=[{"id": "acc", "type": "hive_table", "object": "curated.accounts",
                           "schema": [{"name": "account_id", "type": "INT"}, {"name": "customer_id", "type": "STRING", "rename_to": "cid"}]}])
        bp = bp_of(d)
        apply_snapshot(bp, SNAP)
        f = {x.name: x for x in bp.sources[0].schema_}
        self.assertEqual(f["account_id"].type, "STRING")
        self.assertEqual(f["customer_id"].rename_to, "cid")  # the model's renames survive
        self.assertTrue(any("account_id" in x["evidence"] and "catalog type" in x["evidence"] for x in bp.findings))

    def test_unknown_column_is_a_high_severity_finding_and_is_not_invented(self):
        d = rich(sources=[{"id": "acc", "type": "hive_table", "object": "curated.accounts",
                           "schema": [{"name": "account_id", "type": "STRING"}, {"name": "ghost", "type": "STRING"}]}])
        bp = bp_of(d)
        apply_snapshot(bp, SNAP)
        hit = [x for x in bp.findings if "ghost" in x["evidence"]]
        self.assertEqual(hit[0]["severity"], "high")
        self.assertEqual(hit[0]["status"], "open")

    def test_unmatched_table_is_reported_with_candidates_but_not_guessed(self):
        d = rich(sources=[{"id": "acc", "type": "hive_table", "object": "staging.accounts", "schema": []}])
        bp = bp_of(d)
        s = apply_snapshot(bp, SNAP)
        self.assertEqual(bp.sources[0].schema_, [])
        self.assertTrue(s["unmatched"])
        f = bp.findings[0]
        self.assertIn("curated.accounts", f["evidence"])
        self.assertIn("does not guess", f["evidence"])
        self.assertIn("execution", f["blocks"])

    def test_pre_filter_partition_column_is_added(self):
        d = rich(sources=[{"id": "tx", "type": "hive_table", "object": "curated.transactions", "pre_filter": "txn_date = :run_date",
                           "schema": [{"name": "txn_id", "type": "STRING"}]}],
                 transformations=[{"id": "t1", "type": "select", "inputs": ["tx"], "select": ["txn_id"]}], targets=[])
        bp = bp_of(d)
        apply_snapshot(bp, SNAP)
        self.assertEqual([f.name for f in bp.sources[0].schema_], ["txn_id", "txn_date"])

    def test_target_drift_is_reported_not_overwritten(self):
        d = rich()
        d["targets"][0].update(partition_by=["feature_date"], schema=[{"name": "customer_id", "type": "INT"}, {"name": "extra", "type": "STRING"}])
        bp = bp_of(d)
        apply_snapshot(bp, SNAP)
        ev = " | ".join(f["evidence"] for f in bp.findings)
        self.assertIn("customer_id", ev)             # type conflict
        self.assertIn("'extra'", ev)                 # column not in the table
        self.assertIn("partitions by ['feature_date']", ev)
        self.assertEqual(bp.targets[0].partition_by, ["feature_date"])  # the declaration is kept; a human decides

    def test_missing_target_table_is_low_severity_ddl_needed(self):
        d = rich()
        d["targets"][0]["object"] = "risk.brand_new"
        bp = bp_of(d)
        apply_snapshot(bp, SNAP)
        f = [x for x in bp.findings if "brand_new" in x["evidence"]][0]
        self.assertEqual((f["severity"], f["blocks"]), ("low", ["execution"]))

    def test_view_sources_are_flagged(self):
        with tempfile.TemporaryDirectory() as dd:
            open(os.path.join(dd, "v.hql"), "w").write("CREATE VIEW curated.v AS SELECT 1 AS a;")
            snap = load_snapshot(dd)
        d = rich(sources=[{"id": "acc", "type": "hive_table", "object": "curated.v", "schema": []}], targets=[])
        bp = bp_of(d)
        apply_snapshot(bp, snap)
        self.assertTrue(any("VIEW" in f["evidence"] for f in bp.findings))

    def test_idempotent_and_records_provenance(self):
        bp = bp_of(rich())
        apply_snapshot(bp, SNAP)
        before = copy.deepcopy(bp.to_dict())
        apply_snapshot(bp, SNAP)
        self.assertEqual(bp.to_dict()["findings"] if "findings" in bp.to_dict() else [], before.get("findings", []))
        self.assertEqual(bp.to_dict()["sources"], before["sources"])
        prov = bp.provenance["metadata_snapshot"]
        self.assertEqual(prov["id"], SNAP.id)
        self.assertIn("acc -> curated.accounts", prov["matched"])

    def test_resolved_blueprint_survives_validation_and_round_trips(self):
        bp = bp_of(rich(transformations=[{"id": "t1", "type": "select", "inputs": ["acc"], "select": ["account_id", "run_date"]}]))
        apply_snapshot(bp, SNAP)
        ValidationEngine.validate_logical_dag(bp)
        again = bp_of(bp.to_dict())
        self.assertEqual(again.sources[0].catalog["table"], "curated.accounts")


class PromptBlock(unittest.TestCase):
    SRC = 'a = spark.table("curated.accounts")\nfx = spark.read.parquet("hdfs:///data/ref/fx_rates/")'

    def test_only_mentioned_tables_with_full_types(self):
        b = SNAP.prompt_block(self.SRC)
        self.assertIn("table curated.accounts", b)
        self.assertIn("attributes MAP<STRING,STRING>", b)
        self.assertIn("table curated.fx_rates", b)
        self.assertIn("rate_date DATE (partition)", b)
        self.assertNotIn("curated.transactions", b)
        self.assertIn(SNAP.id, b)
        self.assertIn("EXACTLY", b)

    def test_alias_is_called_out(self):
        b = SNAP.prompt_block('spark.table("legacy.acc")', {"legacy.acc": "curated.accounts"})
        self.assertIn("the job calls it 'legacy.acc'", b)

    def test_nothing_mentioned_says_no_schemas_available(self):
        self.assertIn("do not invent", SNAP.prompt_block("select 1").lower())

    def test_truncation_is_announced(self):
        b = SNAP.prompt_block("curated.accounts curated.customers curated.transactions", max_chars=300)
        self.assertIn("truncated", b)


class Stub(BaseLLM):
    def __init__(self, content):
        self.content, self.task, self.i = content, None, 0

    def chat(self, system, history, tools):
        self.task = history[0]["content"]
        self.i += 1
        return LLMResponse(text="", tool_calls=[ToolCall(f"s{self.i}", "submit_final", {"content": self.content})])

    def user_message(self, t):
        return {"role": "user", "content": t}

    def assistant_message(self, r):
        return {"role": "assistant", "content": ""}

    def tool_result_messages(self, results):
        return [{"role": "tool", "content": r.output} for r in results]


class OrchestratorWiring(unittest.TestCase):
    SRC = 'accounts = spark.table("curated.accounts")\n'

    def run_fw(self, snapshot):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "job.py")
            open(p, "w").write(self.SRC)
            stub = Stub(yaml.dump(rich(targets=[{"id": "out", "input": "t1", "type": "parquet", "path": "/tmp/out"}])))
            fw = AgenticFramework(p, stub, out_dir=d, snapshot=snapshot)
            fw.load_input()
            fw.run_ingestion_agent()
            return fw, stub

    def test_model_sees_the_catalog_and_the_result_is_resolved_from_it(self):
        fw, stub = self.run_fw(SNAP)
        self.assertIn("CATALOG METADATA", stub.task)
        self.assertIn("customer_id STRING", stub.task)
        self.assertEqual(len(fw.blueprint.sources[0].schema_), 10)
        self.assertEqual(fw.blueprint.provenance["metadata_snapshot"]["id"], SNAP.id)

    def test_without_a_snapshot_the_model_is_told_not_to_invent_schemas(self):
        fw, stub = self.run_fw(None)
        self.assertIn("No catalog metadata snapshot was supplied", stub.task)
        self.assertEqual(fw.blueprint.sources[0].schema_, [])
        self.assertNotIn("metadata_snapshot", fw.blueprint.provenance)

    def test_a_blueprint_that_stops_validating_after_resolution_is_flagged_not_hidden(self):
        class S2(Stub):
            pass
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "job.py")
            open(p, "w").write(self.SRC)
            fw = AgenticFramework(p, S2(yaml.dump(rich())), out_dir=d, snapshot=SNAP)  # target partition run_date is never produced
            fw.load_input()
            fw.run_ingestion_agent()
        self.assertTrue(any(f["id"] == "F-META-REVALIDATE" for f in fw.blueprint.findings))


class ValidatorRecheck(unittest.TestCase):
    def test_resolved_blueprint_has_no_metadata_findings(self):
        bp = bp_of(rich(targets=[]))
        apply_snapshot(bp, SNAP)
        self.assertEqual(metadata_findings(bp, SNAP), [])

    def test_drift_after_resolution_is_an_error(self):
        bp = bp_of(rich(targets=[]))
        apply_snapshot(bp, SNAP)
        bp.sources[0].schema_.append(__import__("agentic.schemas", fromlist=["SchemaField"]).SchemaField(name="ghost", type="STRING"))
        fs = metadata_findings(bp, SNAP)
        self.assertTrue(any(f.severity == "error" and "ghost" in f.message for f in fs))


if __name__ == "__main__":
    unittest.main()

