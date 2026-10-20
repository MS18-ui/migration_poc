"""Review-agent plumbing: tools, gates, secret redaction, LLM-claim limits. Offline (no network)."""
import os
import unittest

from agentic.validation import ValidationEngine
from agentic.validator.agent import OfflineLLM, build_tools, run_review_agent
from agentic.validator.compat import redact_secrets
from agentic.validator.findings import Finding
from agentic.validator.pipeline import Session

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = open(os.path.join(HERE, "fixtures", "daily_txn_risk_blueprint.yaml"), encoding="utf-8").read()
SOURCE = open(os.path.join(HERE, "fixtures", "daily_txn_risk_job.py"), encoding="utf-8").read()


def session(**kw):
    return Session(ValidationEngine.validate_yaml_structure(SPEC), SOURCE, "daily_txn_risk_job.py",
                   n_random=4, synth_budget=20, **kw)


class Redaction(unittest.TestCase):
    def test_masks_credentials(self):
        text = 'password = "hunter2abc"\nurl = "jdbc:mysql://h/db?user=a&password=Zz9secret"\nx = 1'
        out, n = redact_secrets(text)
        self.assertGreaterEqual(n, 2)
        self.assertNotIn("hunter2abc", out)
        self.assertNotIn("Zz9secret", out)
        self.assertIn("x = 1", out)


from agentic.validator.spark_session import spark_unavailable_reason

SPARK_REASON = spark_unavailable_reason()
needs_spark = unittest.skipIf(SPARK_REASON is not None, f"Spark unavailable: {SPARK_REASON}")


class Tools(unittest.TestCase):
    def setUp(self):
        self.s = session()
        self.holder = {}
        self.tools = {t.name: t for t in build_tools(self.s, self.holder)}

    def test_submit_requires_tests_first(self):
        self.assertTrue(self.tools["submit_report"].fn({"summary": "x"}).startswith("ERROR"))

    def test_llm_findings_are_capped_and_unverified(self):
        self.tools["add_finding"].fn({"message": "looks wrong", "severity": "error", "node": "cust"})
        f = [x for x in self.s.report.findings if x.origin == "llm"][0]
        self.assertEqual((f.severity, f.confidence), ("warning", "low"))
        self.assertTrue(self.s.report.passed)  # an LLM claim can never make a report fail (or pass)

    @needs_spark
    def test_add_scenario_validates_input(self):
        self.s.run_static()
        self.s.run_tests(only_nodes={"posted"}, whole_graph=False)
        self.assertIn("unknown input", self.tools["add_scenario"].fn({"node": "posted", "inputs": {"nope": []}}))
        self.assertTrue(self.tools["add_scenario"].fn({"node": "nonexistent", "inputs": {}}).startswith("ERROR"))
        self.assertTrue(self.tools["add_scenario"].fn({"node": "posted", "inputs": "not json"}).startswith("ERROR"))

    def test_classify_requires_a_real_survivor(self):
        self.assertTrue(self.tools["classify_survivor"].fn(
            {"node": "x", "mutant": "nothing", "verdict": "equivalent", "justification": "j"}).startswith("ERROR"))
        self.assertTrue(self.tools["classify_survivor"].fn(
            {"node": "x", "mutant": "m", "verdict": "maybe", "justification": "j"}).startswith("ERROR"))


class OfflineAgent(unittest.TestCase):
    def test_full_loop(self):
        s = session()
        summary = run_review_agent(s, offline=True)
        self.assertIn("Offline review", summary)
        self.assertTrue(s.static_done and s.tests_done)
        if SPARK_REASON is None:
            self.assertIn("mutation_score", s.report.stats)
        else:  # without Spark the dynamic layer must say it did not run, not pretend it passed
            self.assertEqual(s.report.stats.get("dynamic_layer"), "not run")
            self.assertEqual(s.report.result, "INCOMPLETE")  # never "PASSED" when a layer did not run
            self.assertTrue(any("NOT run" in f.message for f in s.report.findings))

    def test_secret_in_source_is_redacted_before_model(self):
        s = session()
        s.source_text += '\nDB_PASSWORD = "supersecretvalue"\n'
        seen = {}

        class Spy(OfflineLLM):
            def chat(self, system, history, tools):
                seen["task"] = history[0]["content"]
                return super().chat(system, history, tools)
        run_review_agent(s, llm=Spy(s))
        self.assertNotIn("supersecretvalue", seen["task"])
        self.assertTrue(any("redacted" in f.message for f in s.report.findings))


if __name__ == "__main__":
    unittest.main()
