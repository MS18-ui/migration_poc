"""Sqoop jobs -> intermediate YAML (agentic.sqoop_yaml), on the POC's Sqoop commands (samples/sqoop_jobs.txt).

The expected account_holders file is the one the POC notebook generated (tests/fixtures/sqoop_account_holders_expected.yaml),
so these tests also show the merged module reproduces the original script.
"""
import json
import os
import tempfile
import unittest
from unittest import mock

import yaml

from agentic.spark_compare import Source, load_source
from agentic.sqoop_yaml import Options, build_specs, convert, spec_yaml

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SQOOP = os.path.join(ROOT, "samples", "sqoop_jobs.txt")
DDL = os.path.join(ROOT, "sqoop_poc", "01_schema.sql")
EXPECTED = os.path.join(HERE, "fixtures", "sqoop_account_holders_expected.yaml")
NOTEBOOK_SKILL = ".agents/skills/sqoop-to-spark@94b4b18"

POC = Options(project_id="sqoop_poc", target_catalog="ice", target_namespace="sqoop_poc_iceberg",
              target_storage="hdfs://172.16.0.4:9000/user/hive/warehouse/sqoop_poc_iceberg.db")


def read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def poc_specs(options=POC, ddl=True):
    # same labels as the notebook run: the commands file and the DDL file as named there
    sqoop = load_source(path=SQOOP, name="sqoop_jobs.txt")
    schema = load_source(path=DDL, name="01_schema.sql") if ddl else None
    return {s["job_id"]: s for s in build_specs(sqoop, schema, options)["specs"]}


class SqoopYamlCase(unittest.TestCase):
    def test_reproduces_notebook_output(self):
        opt = Options(**{**POC.__dict__, "skill_ref": NOTEBOOK_SKILL})
        spec = poc_specs(opt)["account_holders"]
        self.assertEqual(spec, yaml.safe_load(read(EXPECTED)))

    def test_jobs_and_finding_counts_match_notebook(self):
        specs = poc_specs()
        counts = {j: (len(s["findings"]), sum("execution" in f["blocks"] for f in s["findings"]), s["ingestion"]["load"]["mode"])
                  for j, s in specs.items()}
        self.assertEqual(counts, {
            "branches": (4, 3, "full"), "customers": (6, 5, "full"), "account_holders": (4, 3, "full"),
            "fx_rates_log": (5, 4, "full"), "acct_enriched": (6, 5, "full"),
            "poc_transactions_append": (4, 3, "incremental_append"),
            "poc_accounts_lastmodified": (5, 4, "incremental_lastmodified")})

    def test_every_option_is_covered(self):
        for s in poc_specs().values():
            self.assertTrue(s["coverage"])
            for c in s["coverage"]:
                self.assertIn(c["status"], ("represented", "irrelevant", "unresolved", "unsupported"))
                if c["status"] in ("unresolved", "unsupported"):
                    self.assertTrue(c.get("finding_refs"), c)

    def test_incremental_keeps_watermark_and_merge_key(self):
        load = poc_specs()["poc_accounts_lastmodified"]["ingestion"]["load"]
        self.assertEqual(load["watermark"]["column"], "updated_at")
        self.assertEqual(load["watermark"]["state_store"], "sqoop_saved_job")
        self.assertEqual(load["merge_keys"], ["account_id"])
        self.assertEqual(load["write_intent"], "merge")

    def test_inline_password_is_redacted_and_blocks(self):
        cmd = ("sqoop import --connect jdbc:postgresql://db:5432/src --username etl --password s3cretValue "
               "--table orders --target-dir /raw/orders -m 1 -- --schema sales\n")
        spec = build_specs(Source("jobs.txt", cmd), options=POC)["specs"][0]
        text = spec_yaml(spec)
        self.assertNotIn("s3cretValue", text)
        self.assertEqual(spec["connection"]["authentication"]["method"], "inline_password")
        self.assertTrue(any(f["category"] == "security" and f["severity"] == "critical" for f in spec["findings"]))

    def test_missing_target_blocks_generation(self):
        spec = poc_specs(Options())["branches"]
        f = [x for x in spec["findings"] if x["rule_ref"].endswith("/target_explicit")]
        self.assertEqual(len(f), 1)
        self.assertIn("generation", f[0]["blocks"])

    def test_export_is_not_converted_to_import(self):
        cmd = "sqoop export --connect jdbc:mysql://db/sales --username etl --password-file /p --table t --export-dir /x\n"
        spec = build_specs(Source("jobs.txt", cmd), options=POC)["specs"][0]
        self.assertEqual(spec["ingestion"]["direction"], "export")
        self.assertTrue(any(f["rule_ref"].endswith("/direction") and f["severity"] == "critical" for f in spec["findings"]))

    def test_ddl_and_trino_are_exclusive(self):
        with self.assertRaises(ValueError):
            build_specs(Source("j.txt", ""), Source("d.sql", ""), trino_url="http://trino:8080")

    def test_convert_writes_one_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            res = convert(load_source(path=SQOOP, name="sqoop_jobs.txt"), load_source(path=DDL), POC, out_base=tmp)
            self.assertEqual(os.path.dirname(res["folder"]), tmp)
            self.assertTrue(os.path.basename(res["folder"]).startswith("sqoop_jobs_"))
            self.assertEqual(len(res["files"]["yaml"]), 7)
            for job_id, path in res["files"]["yaml"].items():
                self.assertEqual(yaml.safe_load(read(path))["job_id"], job_id)
            report = json.loads(read(res["files"]["json"]))
            self.assertEqual(report["summary"]["jobs"], 7)
            self.assertEqual(report["summary"]["findings"], 34)
            self.assertIn("## Coverage", read(res["files"]["markdown"]))


class SqoopYamlApiCase(unittest.TestCase):
    def setUp(self):
        try:
            from fastapi.testclient import TestClient
        except ImportError:
            self.skipTest("fastapi (and httpx) are not installed")
        from agentic import api
        self.client = TestClient(api.app)
        self.tmp = tempfile.TemporaryDirectory()
        patch = mock.patch("agentic.sqoop_yaml.ui_settings", return_value={"out_dir": self.tmp.name})
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(self.tmp.cleanup)

    def test_convert_by_path(self):
        r = self.client.post("/api/sqoop-yaml", json={
            "sqoop": {"path": "samples/sqoop_jobs.txt"}, "ddl": {"path": "sqoop_poc/01_schema.sql"},
            "project_id": "sqoop_poc", "target_catalog": "ice", "target_namespace": "sqoop_poc_iceberg",
            "target_storage": "hdfs://nn:9000/warehouse/sqoop_poc_iceberg.db"})
        self.assertEqual(r.status_code, 200, r.text)
        data = r.json()
        self.assertEqual(data["summary"]["jobs"], 7)
        self.assertIn("account_holders", data["yaml"])
        self.assertTrue(data["yaml"]["account_holders"].startswith("# Proposed draft"))

    def test_path_outside_project_is_rejected(self):
        r = self.client.post("/api/sqoop-yaml", json={"sqoop": {"path": "../outside.txt"}})
        self.assertEqual(r.status_code, 400)

    def test_ddl_and_trino_together_is_rejected(self):
        r = self.client.post("/api/sqoop-yaml", json={"sqoop": {"code": "sqoop import --table t"},
                                                      "ddl": {"code": "CREATE TABLE t (a int)"},
                                                      "trino_url": "http://trino:8080"})
        self.assertEqual(r.status_code, 422)


if __name__ == "__main__":
    unittest.main()
