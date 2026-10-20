"""Engine translation logic, tested WITHOUT Spark using the recording fake (tests/fake_spark.py).

These tests check what the engine asks Spark to do; whether Spark then behaves as the legacy job did is
covered by the Spark-backed tests in test_validator.py (skipped where Spark/Java are not installed).
"""
import copy
import os
import unittest

import yaml

import agentic.validator.engine as engine_mod
from agentic.validation import ValidationEngine, load_yaml
from agentic.validator.engine import Engine, check_shape, shape_problems
from agentic.validator.static_checks import run_static
from tests.fake_spark import FFunctions, FSpark, plan

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = os.path.join(HERE, "fixtures", "daily_txn_risk_blueprint.yaml")
SRC = os.path.join(HERE, "fixtures", "daily_txn_risk_job.py")
BASE = load_yaml(open(SPEC, encoding="utf-8").read())


def bp_of(d):
    return ValidationEngine.validate_yaml_structure(yaml.dump(d, sort_keys=False))


def node(d, nid):
    return next(t for t in d["transformations"] if t["id"] == nid)


class FakeSparkCase(unittest.TestCase):
    def setUp(self):
        self._orig = engine_mod._F
        engine_mod._F = lambda: FFunctions
        self.eng = Engine(bp_of(BASE), spark=FSpark()).run({})

    def tearDown(self):
        engine_mod._F = self._orig


class WholeGraph(FakeSparkCase):
    def test_every_node_builds_and_column_names_flow_to_the_target(self):
        self.assertEqual({k: str(v) for k, v in self.eng.errors.items()}, {})
        want = {f["name"] for f in BASE["targets"][0]["schema"]}
        self.assertTrue(want <= {c for c, _ in self.eng.cols["customer_daily_risk"]})

    def test_missing_target_column_is_attributed_to_the_target(self):
        d = copy.deepcopy(BASE)
        d["targets"][0]["schema"].append({"name": "ghost_col", "type": "STRING"})
        e = Engine(bp_of(d), spark=FSpark()).run({})
        self.assertIn("ghost_col", str(e.errors["customer_daily_risk"]))


class Translation(FakeSparkCase):
    def test_parameters_are_substituted_in_pre_filters(self):
        self.assertIn("rate_date = '2026-10-18'", plan(self.eng, "fx"))

    def test_source_renames_become_aliases(self):
        self.assertIn("`currency` AS `fx_currency`", plan(self.eng, "fx"))

    def test_derived_columns_keep_spark_sql_text_and_order(self):
        p = plan(self.eng, "enriched_derived")
        self.assertIn("withColumn(amount_usd := expr(CAST(ROUND(amount * COALESCE(usd_rate, 1.0), 2) AS DOUBLE)))", p)
        self.assertLess(p.index("amount_usd :="), p.index("is_large_txn :="))

    def test_window_over_w_expands_to_the_declared_window(self):
        p = plan(self.eng, "enriched_structuring")
        self.assertIn("OVER (PARTITION BY customer_id ORDER BY CAST(txn_ts AS BIGINT) "
                      "RANGE BETWEEN 259200 PRECEDING AND CURRENT ROW)", p)

    def test_bare_aggregate_in_window_node_is_wrapped(self):
        e = Engine(bp_of(BASE), spark=FSpark())
        txt = e.expr_text("SUM(x)", "n", {"partition_by": ["c"], "order_by": ["t"], "frame": "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW"})
        self.assertIn("OVER (PARTITION BY c ORDER BY t ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)", txt)

    def test_joins_use_key_lists_expressions_and_broadcast(self):
        p = plan(self.eng, "enriched_joined")
        self.assertIn("join(on=['account_id'], how=inner)", p)
        self.assertIn("join(on=expr(currency = fx_currency), how=left broadcast)", p)

    def test_dedupe_and_filter_order(self):
        p = plan(self.eng, "posted")
        self.assertLess(p.index("filter(expr(status = 'POSTED'))"), p.index("dropDuplicates(['txn_id'])"))

    def test_group_by_alias_is_split(self):
        self.assertIn("expr(r.rule_id) AS rule_id", plan(self.eng, "rule_rows"))
        self.assertIn("expr(customer_id)", plan(self.eng, "rule_rows"))

    def test_explode_adds_the_alias(self):
        self.assertIn("explode(col(`fired_rules`)) AS r", plan(self.eng, "rule_rows"))

    def test_collect_to_map_runs_each_step_in_order(self):
        p = plan(self.eng, "exposure")
        self.assertEqual(p.count("groupBy("), 2)
        self.assertLess(p.index("SUM(amount_usd)"), p.index("MAP_FROM_ENTRIES"))

    def test_rules_become_one_filtered_array_of_named_structs(self):
        p = plan(self.eng, "scored")
        self.assertIn("named_struct('rule_id', 'R01', 'description', 'Cash structuring', 'score', 90)", p)
        self.assertIn("filter(array(", p)
        self.assertIn("r -> r IS NOT NULL", p)

    def test_target_is_cast_to_the_declared_types(self):
        self.assertIn("MAP<STRING,DOUBLE>", str(self.eng.df["customer_daily_risk"].ops[-1]))


class ShapeChecks(unittest.TestCase):
    """Pure Python: no Spark, no fake."""

    def bp(self, fn):
        d = copy.deepcopy(BASE)
        fn(d)
        return bp_of(d)

    def test_reference_is_fully_executable(self):
        self.assertEqual(shape_problems(bp_of(BASE)), {})

    def test_free_text_steps_rejected(self):
        bp = self.bp(lambda d: node(d, "exposure").update(steps=["GROUP BY a: x = SUM(y)"]))
        self.assertIn("free text", shape_problems(bp)["exposure"])

    def test_custom_and_unknown_types_rejected(self):
        bp = self.bp(lambda d: node(d, "cust").update(type="custom"))
        self.assertIn("no deterministic implementation", shape_problems(bp)["cust"])
        bp = self.bp(lambda d: node(d, "cust").update(type="mystery"))
        self.assertIn("mystery", shape_problems(bp)["cust"])

    def test_rule_without_condition_rejected(self):
        bp = self.bp(lambda d: node(d, "scored")["rules"][0].pop("when"))
        self.assertIn("'when'/'condition'", shape_problems(bp)["scored"])

    def test_join_without_on_rejected(self):
        bp = self.bp(lambda d: node(d, "enriched_joined")["joins"][0].pop("on"))
        self.assertIn("no usable 'on'", shape_problems(bp)["enriched_joined"])

    def test_bad_column_spec_rejected(self):
        bp = self.bp(lambda d: node(d, "cust")["columns"].append({"name": "x"}))
        self.assertIn("'name' and 'expr'", shape_problems(bp)["cust"])


class StaticWithFake(FakeSparkCase):
    def test_static_runs_with_an_injected_engine(self):
        findings, matrix, eng = run_static(bp_of(BASE), open(SRC, encoding="utf-8").read(), "daily_txn_risk_job.py", engine=self.eng)
        self.assertFalse([f for f in findings if f.severity == "error"])
        self.assertTrue(matrix)


if __name__ == "__main__":
    unittest.main()
