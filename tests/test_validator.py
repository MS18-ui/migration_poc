"""Regression tests for the validation agent's deterministic core.

Run:  python -m unittest discover -s tests -v

Two tiers:
  * offline  - static checks (source<->YAML coverage, compatibility rules, operation shapes): no Spark needed
  * Spark    - scenarios, mutation testing, whole-graph runs: need pyspark + Java (e.g. the cluster host);
               skipped here when Spark is unavailable (they are reported as skipped, never as passed)

The key tests inject realistic conversion mistakes into the reference blueprint and assert each one is
detected: a validator that only passes good input proves nothing.
"""
import copy
import os
import unittest

import yaml

from agentic.testdata import generate
from agentic.validation import ValidationEngine, load_yaml
from agentic.validator.engine import Engine
from agentic.validator.pipeline import Session
from agentic.validator.spark_session import spark_unavailable_reason
from agentic.validator.static_checks import run_static

SPARK_REASON = spark_unavailable_reason()
needs_spark = unittest.skipIf(SPARK_REASON is not None, f"Spark unavailable: {SPARK_REASON}")

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = os.path.join(HERE, "fixtures", "daily_txn_risk_blueprint.yaml")
SRC = os.path.join(HERE, "fixtures", "daily_txn_risk_job.py")
SOURCE = open(SRC, encoding="utf-8").read()
BASE = load_yaml(open(SPEC, encoding="utf-8").read())


def node(d, nid):
    return next(t for t in d["transformations"] if t["id"] == nid)


def bp_of(d):
    return ValidationEngine.validate_yaml_structure(yaml.dump(d, sort_keys=False))


def static(d):
    return run_static(bp_of(d), SOURCE, "daily_txn_risk_job.py")[0]


def dynamic(d, nodes):
    s = Session(bp_of(d), SOURCE, "daily_txn_risk_job.py", n_random=8, synth_budget=60)
    s.run_static()
    s.run_tests(only_nodes=set(nodes), whole_graph=False)
    return s


def has(findings, severity=None, category=None, text=""):
    return any((severity is None or f.severity == severity) and (category is None or f.category == category)
               and text.lower() in f.message.lower() for f in findings)


class ReferenceIsClean(unittest.TestCase):
    def test_no_errors_static(self):
        errs = [f.message for f in static(BASE) if f.severity == "error"]
        self.assertEqual(errs, [])

    @needs_spark
    def test_graph_executes_end_to_end(self):
        bp = bp_of(BASE)
        eng = Engine(bp).run({sid: d["rows"] for sid, d in generate(bp, n_rows=300).items()})
        self.assertEqual({k: str(v) for k, v in eng.errors.items()}, {})
        rows = eng.fetch("customer_daily_risk")
        self.assertTrue(rows)
        self.assertEqual(set(rows[0]), {c["name"] for c in BASE["targets"][0]["schema"]} | {"run_date"})


class InjectedDefectsAreCaught(unittest.TestCase):
    def mutate(self, fn):
        d = copy.deepcopy(BASE)
        fn(d)
        return d

    def test_operator_drift_static_and_dynamic(self):
        def f(d):
            for c in node(d, "enriched_derived")["columns"]:
                if c["name"] == "is_large_txn":
                    c["expr"] = "amount_usd > 10000"
        d = self.mutate(f)
        self.assertTrue(has(static(d), "error", "coverage", "operator changed"))
        if SPARK_REASON is None:  # the dynamic layer independently catches it at the boundary value
            s = dynamic(d, ["enriched_derived"])
            self.assertTrue(has(s.report.findings, "error", "behavior", "differs from the SOURCE"))

    def test_threshold_changed(self):
        d = self.mutate(lambda d: [c.update(expr=c["expr"].replace("10000", "10500")) for c in node(d, "enriched_derived")["columns"]])
        self.assertTrue(has(static(d), "error", "coverage", "threshold changed") or has(static(d), "error", "coverage", "10000"))

    def test_dropped_filter(self):
        d = self.mutate(lambda d: node(d, "posted").pop("filter"))
        self.assertTrue(has(static(d), None, "coverage", "POSTED"))

    def test_dropped_dedupe(self):
        d = self.mutate(lambda d: node(d, "posted").pop("dedupe_keys"))
        self.assertTrue(has(static(d), "error", "coverage", "dropDuplicates"))

    def test_join_type_changed(self):
        def f(d):
            for j in node(d, "enriched_joined")["joins"]:
                if j["right"] == "fx":
                    j["type"] = "inner"
        self.assertTrue(has(static(self.mutate(f)), "error", "coverage", "join structure differs"))

    def test_window_frame_changed(self):
        d = self.mutate(lambda d: node(d, "enriched_structuring")["window"].update(frame="RANGE BETWEEN 172800 PRECEDING AND CURRENT ROW"))
        fs = static(d)
        self.assertTrue(has(fs, "error", "coverage", "window definition differs"))
        self.assertTrue(has(fs, "error", "coverage", "259200"))

    def test_dropped_rename(self):
        def f(d):
            for s in d["sources"]:
                for fld in s["schema"]:
                    fld.pop("rename_to", None)
        fs = static(self.mutate(f))
        self.assertTrue(has(fs, "error", None, "fx_currency") or has(fs, "error", "schema", "ambiguous"))

    def test_rule_score_changed(self):
        def f(d):
            for r in node(d, "scored")["rules"]:
                if r["rule_id"] == "R01":
                    r["score"] = 80
        fs = static(self.mutate(f))
        self.assertTrue(has(fs, "error", "coverage", "constant 90"))
        self.assertTrue(has(fs, "warning", "coverage", "constant 80"))

    def test_invented_logic(self):
        d = self.mutate(lambda d: node(d, "cust").update(filter="size(addresses) > 7"))
        self.assertTrue(has(static(d), "warning", "coverage", "constant 7 in the YAML has no basis"))

    def test_free_text_steps_not_executable(self):
        d = self.mutate(lambda d: node(d, "exposure").update(steps=["GROUP BY customer_id, currency: usd = SUM(amount_usd)"]))
        fs = static(d)
        self.assertTrue(has(fs, "error", "executability", "free text"))
        if SPARK_REASON is None:
            self.assertTrue(has(fs, None, "executability", "blocked"))

    @needs_spark
    def test_undefined_target_column(self):
        d = self.mutate(lambda d: d["targets"][0]["schema"].append({"name": "ghost_col", "type": "STRING"}))
        self.assertTrue(has(static(d), "error", None, "ghost_col"))


@needs_spark
class DynamicLayer(unittest.TestCase):
    def test_window_node_mutants_all_killed(self):
        s = dynamic(BASE, ["enriched_structuring"])
        ms = [m for m in s.report.mutants if m["node"] == "enriched_structuring"]
        self.assertGreaterEqual(len(ms), 8)
        self.assertEqual([m["description"] for m in ms if m["status"] == "survived"], [])
        self.assertFalse(has(s.report.findings, "error", "behavior"))

    def test_null_key_crash_is_reported(self):
        s = dynamic(BASE, ["exposure"])
        self.assertTrue(has(s.report.findings, "warning", "behavior", "map keys can not be null"))

    def test_source_reference_fanout_reported(self):
        s = dynamic(BASE, ["enriched_joined"])
        self.assertTrue(has(s.report.findings, "warning", "behavior", "fan-out"))

    def test_suite_is_minimized_and_kills_recorded(self):
        s = dynamic(BASE, ["enriched_structuring"])
        self.assertTrue(s.suite.scenarios)
        self.assertTrue(any(v for v in s.suite.kills.values()))


if __name__ == "__main__":
    unittest.main()
