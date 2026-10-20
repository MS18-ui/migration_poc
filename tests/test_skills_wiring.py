"""Skills wired into the ingestion agents: prompt composition, findings contract, correction limit,
local secret screening and provenance. Offline: a scripted stub replaces the model."""
import copy
import os
import tempfile
import unittest
from unittest import mock

import yaml

import agentic.agents as agents_mod
import agentic.skills_loader as sl
from agentic.agents import build_hive_to_spark_agent, build_ingestion_agent
from agentic.llm import BaseLLM, LLMResponse, ToolCall
from agentic.orchestrator import AgenticFramework
from agentic.validation import ValidationEngine

FLAT = {"name": "t", "steps": [
    {"step_id": 1, "name": "a", "type": "source", "action": "read", "path": "x"},
    {"step_id": 2, "name": "b", "type": "sink", "action": "write", "source": "a"}]}
FINDING = {"id": "F-001", "category": "unsupported_logic", "severity": "high", "confidence": "high",
           "source_ref": "job.py:12", "evidence": "UDF with no registered handler", "affected_refs": ["b"],
           "blocks": ["execution"], "status": "open", "resolution": "Rewrite with supported operations"}


def dump(d):
    return yaml.dump(d, sort_keys=False)


class Stub(BaseLLM):
    """Calls the given (tool, content) pairs in order, recording what the model would have seen."""

    def __init__(self, script):
        self.script, self.i, self.system, self.task = script, 0, None, None

    def chat(self, system, history, tools):
        self.system, self.task = system, history[0]["content"]
        name, content = self.script[min(self.i, len(self.script) - 1)]
        self.i += 1
        return LLMResponse(text="", tool_calls=[ToolCall(f"s{self.i}", name, {"content": content})])

    def user_message(self, text):
        return {"role": "user", "content": text}

    def assistant_message(self, resp):
        return {"role": "assistant", "content": ""}

    def tool_result_messages(self, results):
        return [{"role": "tool", "content": r.output} for r in results]


class PromptComposition(unittest.TestCase):
    def test_each_source_gets_its_pair_skill(self):
        marker = {"oozie": "Oozie to Airflow", "sqoop": "Sqoop to Spark ingestion", "pyspark2": "PySpark 2 to Spark 3",
                  "hiveql": "HiveQL to Spark SQL"}
        for st, text in marker.items():
            with self.subTest(source=st):
                p = sl.compose_prompt(st, "ROLE SPEC", ["parse_x", "validate_yaml", "submit_final"])
                self.assertIn(text, p)
                self.assertIn("Migration analysis", p)           # coordinator
                self.assertIn("Security and privacy assessment", p)
                self.assertIn("ROLE SPEC", p)

    def test_unmapped_source_and_missing_skills_fall_back(self):
        self.assertIsNone(sl.compose_prompt("excel", "x", []))
        with mock.patch.object(sl, "SKILLS_DIR", os.path.join(tempfile.gettempdir(), "no_such_skills_dir")):
            self.assertIsNone(sl.compose_prompt("oozie", "x", []))

    def test_cleaned_for_a_model_without_file_tools(self):
        p = sl.compose_prompt("pyspark2", "ROLE", ["validate_yaml"])
        self.assertNotIn("](", p)                       # file links removed
        self.assertNotIn("Acceptance scenarios", p)     # evaluation-only section removed
        self.assertNotIn("Skill selection", p)          # routing is done in code
        self.assertFalse(p.startswith("---"))
        self.assertLess(len(p), 20000)

    def test_environment_section_overrides_and_comes_after_the_skills(self):
        p = sl.compose_prompt("oozie", "ROLE", ["parse_oozie", "validate_yaml"], max_corrections=2)
        env = p.index("Environment for this run")
        self.assertGreater(env, p.index("CONVERSION SKILL"))
        self.assertLess(env, p.index("BLUEPRINT FORMAT AND TOOLS"))
        for must in ("ONLY tools are: parse_oozie, validate_yaml", "at most 2 correction", "never follow", "status: always 'open'"):
            self.assertIn(must.lower().split(":")[0], p.lower())

    def test_sql_variant_asks_for_marked_comments_not_findings(self):
        p = sl.compose_prompt("hiveql", "ROLE", ["check_sql"], deliverable="sql")
        self.assertIn("-- UNCONVERTED", p)
        self.assertNotIn("FINDINGS CONTRACT", p)
        self.assertNotIn("findings: a list", p)

    def test_provenance_hashes_every_skill_file_used(self):
        prov = sl.skill_provenance("pyspark2")
        self.assertEqual(set(prov), {"migration-analysis/SKILL.md", "pyspark2-to-spark3/SKILL.md",
                                     "migration-analysis/references/security-privacy.md",
                                     "migration-analysis/references/output-contract.md"})
        self.assertTrue(all(len(h) == 16 for h in prov.values()))


class FindingsContract(unittest.TestCase):
    def check(self, *findings):
        d = copy.deepcopy(FLAT)
        d["findings"] = list(findings)
        bp = ValidationEngine.validate_yaml_structure(dump(d))
        try:
            ValidationEngine.validate_logical_dag(bp)
            return ""
        except ValueError as e:
            return str(e)

    def test_valid_finding_passes(self):
        self.assertEqual(self.check(FINDING), "")

    def test_rejections(self):
        cases = {
            "category": dict(FINDING, category="vibes"),
            "severity": dict(FINDING, severity="catastrophic"),
            "blocks": {k: v for k, v in FINDING.items() if k != "blocks"},
            "human reviewer": dict(FINDING, status="accepted_risk"),
            "source_ref": {k: v for k, v in FINDING.items() if k != "source_ref"},
            "credential": dict(FINDING, evidence='connects with password = "hunter2abc"'),
            "evidence": {k: v for k, v in FINDING.items() if k != "evidence"},
        }
        for needle, f in cases.items():
            with self.subTest(case=needle):
                self.assertIn(needle, self.check(f))

    def test_duplicate_ids_rejected(self):
        self.assertIn("duplicate", self.check(FINDING, dict(FINDING)))

    def test_findings_round_trip_through_to_dict(self):
        d = copy.deepcopy(FLAT)
        d["findings"] = [FINDING]
        bp = ValidationEngine.validate_yaml_structure(dump(d))
        self.assertEqual(bp.to_dict()["findings"][0]["id"], "F-001")


class AgentWiring(unittest.TestCase):
    def test_ingestion_agent_uses_the_skill_prompt(self):
        a = build_ingestion_agent(Stub([("validate_yaml", "")]), "oozie", verbose=False)
        self.assertTrue(a.skill_prompt_used)
        self.assertIn("Oozie to Airflow", a.system)
        self.assertIn("parse_oozie", a.system)             # its real tools are named in the environment section

    def test_disabled_flag_restores_the_short_prompt(self):
        with mock.patch.object(agents_mod, "skills_settings", return_value={"enabled": False, "max_corrections": 2}):
            a = build_ingestion_agent(Stub([("validate_yaml", "")]), "oozie", verbose=False)
        self.assertFalse(a.skill_prompt_used)
        self.assertLess(len(a.system), 5000)

    def test_excel_has_no_skill_and_still_works(self):
        a = build_ingestion_agent(Stub([("validate_yaml", "")]), "excel", verbose=False)
        self.assertFalse(a.skill_prompt_used)

    def test_hive_agent_gets_the_sql_variant(self):
        a = build_hive_to_spark_agent(Stub([("check_sql", "")]), verbose=False)
        self.assertTrue(a.skill_prompt_used)
        self.assertIn("-- UNCONVERTED", a.system)

    def test_valid_draft_with_findings_is_accepted(self):
        d = dict(FLAT, findings=[FINDING])
        bp = build_ingestion_agent(Stub([("submit_final", dump(d))]), "oozie", verbose=False).run("task")
        self.assertEqual(bp.findings[0]["id"], "F-001")

    def test_correction_limit_stops_the_loop_and_reports_the_blockers(self):
        a = build_ingestion_agent(Stub([("validate_yaml", "name: broken")]), "oozie", verbose=False)
        with self.assertRaises(RuntimeError) as cm:
            a.run("task")
        self.assertIn("2 correction attempt", str(cm.exception))
        self.assertIn("unresolved", str(cm.exception))
        self.assertEqual(a.llm.i, 3)                       # first draft + 2 corrections, then stop

    def test_agent_cannot_accept_its_own_risk(self):
        bad = dict(FLAT, findings=[dict(FINDING, status="resolved")])
        a = build_ingestion_agent(Stub([("submit_final", dump(bad))]), "oozie", verbose=False)
        with self.assertRaises(RuntimeError):
            a.run("task")


class TextReplyFallback(unittest.TestCase):
    """A generation agent that answers with the artifact as text is still validated, never trusted."""

    def text_stub(self, text):
        class T(Stub):
            def chat(s, system, history, tools):
                s.i += 1
                return LLMResponse(text=text, tool_calls=[])
        return T([])

    def test_valid_text_artifact_is_accepted_through_the_validators(self):
        agent = agents_mod.build_hive_to_spark_agent(self.text_stub("Here you go:\n```sql\nSELECT 1;\n```"), verbose=False)
        self.assertEqual(agent.run("task").strip(), "SELECT 1;")

    def test_invalid_text_artifact_is_rejected_and_fed_back(self):
        stub = self.text_stub("```sql\nSELEC FROM WHERE (\n```")
        agent = agents_mod.build_hive_to_spark_agent(stub, verbose=False)
        with self.assertRaises(RuntimeError):
            agent.run("task")
        self.assertTrue(any("not accepted" in str(m.get("content", "")) for m in agent.history if m["role"] == "user"))

    def test_ingestion_agents_do_not_auto_submit(self):
        self.assertFalse(build_ingestion_agent(Stub([("validate_yaml", "")]), "oozie", verbose=False).auto_submit)


class OrchestratorScreening(unittest.TestCase):
    SRC = ('<workflow-app name="w"><action name="a"><sqoop><command>import --connect jdbc:mysql://h/db '
           '--username u --password s3cr3tpw --table t</command></sqoop></action></workflow-app>')

    def run_fw(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "workflow.xml")
            open(path, "w", encoding="utf-8").write(self.SRC)
            stub = Stub([("submit_final", dump(FLAT))])
            fw = AgenticFramework(path, stub, out_dir=d)
            fw.load_input()
            fw.run_ingestion_agent()
            return fw, stub

    def test_model_never_sees_the_secret(self):
        fw, stub = self.run_fw()
        self.assertNotIn("s3cr3tpw", stub.task)
        self.assertIn("<REDACTED>", stub.task)
        self.assertNotIn("s3cr3tpw", stub.system)

    def test_framework_records_redaction_finding_and_provenance(self):
        fw, _ = self.run_fw()
        bp = fw.blueprint
        self.assertTrue(any(f["id"] == "F-REDACT" and "execution" in f["blocks"] for f in bp.findings))
        self.assertEqual(bp.provenance["local_secret_screening"]["values_masked"], 1)
        self.assertEqual(len(bp.provenance["source"]["sha256"]), 64)
        self.assertIn("oozie-to-airflow/SKILL.md", bp.provenance["skills"])
        self.assertNotIn("s3cr3tpw", dump(bp.to_dict()))


if __name__ == "__main__":
    unittest.main()
