"""Orchestrates the full validation: static checks -> per-node scenarios -> mutation testing ->
mutant-guided synthesis -> branch coverage -> whole-graph smoke runs -> report + minimized test suite."""
import json
import os
import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..schemas import PipelineBlueprint, Transformation
from ..testdata import generate
from .coverage import python_facts, sql_facts
from .engine import Engine, spark_msg
from .findings import Finding, Report
from .mutants import Mutant, mutants_for
from .scenarios import (Scenario, branch_observations, check_expectations, expected_branch_values, gen_scenarios,
                        invariants, node_info, pool, random_multi, result_key, run_node, run_variants,
                        source_expectations)
from .spark_session import spark_unavailable_reason
from .static_checks import detect_lang, run_static
from .values import RowFactory, parse_type


@dataclass
class Suite:
    """The minimized test suite: smoke + boundary scenarios plus every scenario that killed a mutant."""
    scenarios: List[Scenario] = field(default_factory=list)
    kills: Dict[str, List[str]] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"scenarios": [dict(s.to_dict(), kills=self.kills.get(s.id, [])) for s in self.scenarios]}


@dataclass
class NodeResult:
    node: str
    scenarios: int = 0
    mutants: List[dict] = field(default_factory=list)


class Session:
    """Everything the validator knows about one job; also the state behind the agent's tools."""

    def __init__(self, bp: PipelineBlueprint, source_text: str, source_path: str, lang: str = None,
                 seed: int = 1, n_random: int = 10, synth_budget: int = 60, params: dict = None,
                 progress: Callable[[str], None] = None):
        self.bp, self.source_text, self.source_path = bp, source_text, source_path
        self.lang = lang or detect_lang(source_path, source_text)
        self.seed, self.n_random, self.synth_budget, self.params = seed, n_random, synth_budget, params
        self.say = progress or (lambda m: None)
        self.report = Report(job=bp.name or "job")
        self.suite = Suite()
        self.engine: Optional[Engine] = None
        self.scenarios: Dict[str, List[Scenario]] = {}
        self.baseline: Dict[str, Dict[str, Any]] = {}
        self.schemas: Dict[str, Dict[str, list]] = {}
        self.mutant_status: Dict[Tuple[str, str], dict] = {}
        self.static_done = self.tests_done = False

    # ---- layer 1 -----------------------------------------------------------------------
    def run_static(self) -> Report:
        findings, matrix, self.engine = run_static(self.bp, self.source_text, self.source_path, self.lang)
        self.report.extend(findings)
        self.report.coverage_matrix = matrix
        self.static_done = True
        return self.report

    # ---- layer 2-4 ---------------------------------------------------------------------
    def _src_preds(self):
        try:
            f = sql_facts(self.source_text) if self.lang == "hiveql" else python_facts(self.source_text)
            return f.preds
        except Exception:
            return []

    def run_tests(self, only_nodes: Optional[set] = None, whole_graph: bool = True) -> Report:
        if not self.static_done:
            self.run_static()
        r, bp = self.report, self.bp
        if self.engine is None:  # no Spark here: say so; never report an unrun check as passed
            why = spark_unavailable_reason() or "Spark session could not be created"
            r.add(Finding("test-gap", "warning", f"dynamic tests (scenarios, mutation testing, whole-graph runs) were NOT run: {why}",
                          confidence="high", resolution="Run the validator on a host with Spark (e.g. the cluster host)."))
            r.stats["dynamic_layer"] = "not run"
            r.complete = False
            self.tests_done = True
            return r
        src_preds = self._src_preds()
        killers: Dict[str, List[str]] = defaultdict(list)
        total_m = killed_m = n_scen = 0
        untested = []
        for t in bp.transformations:
            if only_nodes is not None and t.id not in only_nodes:
                continue
            if t.id not in self.engine.df or any(i not in self.engine.df for i in t.inputs):
                untested.append(t.id)
                continue
            self.say(f"testing node {t.id} ({t.type})")
            schemas = {i: self.engine.cols[i] for i in t.inputs}
            self.schemas[t.id] = schemas
            expect = source_expectations(t, src_preds)
            scs = gen_scenarios(t, schemas, self.seed, self.n_random, expect)
            muts = mutants_for(t)
            base: Dict[str, Any] = {}
            kill_map: Dict[str, List[str]] = {m.description: [] for m in muts}
            alive = list(muts)  # a Spark action is expensive: a mutant stops being evaluated once something kills it
            for s in scs:  # inputs are loaded once per scenario; the original and every live mutant run on them
                res = run_variants(bp, [t] + [m.transformation for m in alive], schemas, s.inputs, self.params)
                base[s.id] = res[0]
                still = []
                for m, rm in zip(alive, res[1:]):
                    if result_key(rm) != result_key(res[0]):
                        kill_map[m.description].append(s.id)
                    else:
                        still.append(m)
                alive = still
            self._judge_baseline(t, scs, base)
            survivors = [m for m in muts if not kill_map[m.description]]
            if survivors and base:
                for d, kid in self._synthesize(t, schemas, survivors, expect, scs, base).items():
                    kill_map[d].append(kid)
            status = {m.description: (m, (kill_map[m.description][0] if kill_map[m.description] else None)) for m in muts}
            for d, (m, kid) in status.items():
                total_m += 1
                killed_m += kid is not None
                for sid_ in kill_map[d]:
                    killers[sid_].append(f"{m.node}: {m.description}")
                r.mutants.append({**m.to_dict(), "status": "killed" if kid else "survived", "killed_by": kid})
                if not kid:
                    r.add(Finding("test-gap", "warning", f"mutant survived: {m.description}", node=m.node, confidence="medium",
                                  impact="No test distinguishes this change from the YAML as written: either the logic is not "
                                         "exercised by any test or the mutation is equivalent.",
                                  resolution="Add a scenario that distinguishes them, or document why they are equivalent.",
                                  evidence={"operator": m.operator}))
            self.scenarios[t.id] = scs
            self.baseline[t.id] = base
            n_scen += len(scs)
            self._branch_coverage(t, scs, base)
        self._minimize(killers)
        if whole_graph:
            self._whole_graph()
        for n in untested:
            r.add(Finding("executability", "info", f"node '{n}' could not be tested (it does not build or an input failed)", node=n))
        r.stats.update({"nodes_tested": len(self.scenarios), "nodes_untested": len(untested),
                        "scenarios_generated": n_scen, "suite_size": len(self.suite.scenarios),
                        "mutants": total_m, "mutants_killed": killed_m,
                        "mutation_score": f"{(100 * killed_m / total_m):.0f}%" if total_m else "n/a"})
        r.scenarios = [{"id": s.id, "node": s.node, "kind": s.kind, "origin": s.origin,
                        "kills": len(self.suite.kills.get(s.id, []))} for s in self.suite.scenarios]
        self.tests_done = True
        return r

    def _judge_baseline(self, t: Transformation, scs: List[Scenario], base: Dict[str, Any]) -> None:
        r = self.report
        source_ids = {x.id for x in self.bp.sources}
        t_full = t.model_copy(update={"select": None})  # keep input columns so expectations can read them
        for s in scs:
            res = base[s.id]
            if res[0] == "error":
                if s.kind in ("typical", "empty"):
                    r.add(Finding("executability", "error", f"node fails on {s.kind} input: {res[1]}", node=t.id,
                                  evidence={"scenario": s.id}, resolution="The operation as specified cannot run."))
                else:
                    r.add(Finding("behavior", "warning", f"node raises an error on some inputs: {res[1]}", node=t.id,
                                  confidence="medium", evidence={"scenario": s.id, "description": s.description},
                                  impact="A NULL / edge value makes the operation fail instead of degrading gracefully "
                                         "(the legacy job may fail the same way - confirm).",
                                  resolution="Handle the edge case explicitly (filter or default) or record it as an accepted limitation."))
                continue
            bad, obs = invariants(t, s.inputs, res[1], source_ids)
            for b in bad:
                r.add(Finding("behavior", "error", f"independent oracle disagrees: {b}", node=t.id, evidence={"scenario": s.id}))
            for o in obs:
                r.add(Finding("behavior", "warning", o, node=t.id, confidence="high", evidence={"scenario": s.id},
                              resolution="Confirm the reference table is unique on the join key, or deduplicate it."))
            if s.expect:
                full = run_variants(self.bp, [t_full], self.schemas[t.id], s.inputs, self.params)[0]
                if full[0] == "ok":
                    for b in check_expectations(s.expect, full[1]):
                        r.add(Finding("behavior", "error", f"boundary behaviour differs from the SOURCE: {b}", node=t.id,
                                      evidence={"scenario": s.id}, resolution="Align the YAML expression with the source operator/threshold."))

    def _synthesize(self, t, schemas, survivors: List[Mutant], expect, scs, base) -> Dict[str, Optional[str]]:
        """Search for rows on which a surviving mutant differs from the original; keep what kills."""
        bp, info = self.bp, node_info(t)
        rf = RowFactory(self.seed + 17)
        pools = {i: {c: pool(parse_type(ty), c, info, rf) for c, ty in schema if c.lower() in info.refs} for i, schema in schemas.items()}
        remaining = {m.description: m for m in survivors}
        found: Dict[str, Optional[str]] = {}
        for n in range(self.synth_budget):
            if not remaining:
                break
            s = random_multi(t, schemas, info, pools, rf, random.Random(f"synth:{self.seed}:{t.id}:{n}"),
                             f"{t.id}:synth{n + 1}", expect, "synth", hot=True)
            ms = list(remaining.values())
            res = run_variants(bp, [t] + [m.transformation for m in ms], schemas, s.inputs, self.params)
            b = res[0]
            hit = [m.description for m, rm in zip(ms, res[1:]) if result_key(rm) != result_key(b)]
            if hit:
                scs.append(s)
                base[s.id] = b
                for d in hit:
                    found[d] = s.id
                    remaining.pop(d)
        return found

    def _branch_coverage(self, t, scs, base) -> None:
        outs, counts = [], []
        for s in scs:
            res = base[s.id]
            if res[0] == "ok":
                outs.append(res[1])
                counts.append(sum(len(v) for v in s.inputs.values()) if len(s.inputs) == 1 else 0)
        if not outs:
            return
        seen = branch_observations(t, outs, counts, self.engine.cols[t.id])
        missing_total = 0
        for label, vals in seen.items():
            want = expected_branch_values(label)
            miss = want - vals
            if miss:
                missing_total += 1
                self.report.add(Finding("test-gap", "warning", f"branch not exercised: {label} never took value(s) {sorted(map(str, miss))}",
                                        node=t.id, confidence="high", resolution="Add a scenario that drives this branch."))
        self.report.stats.setdefault("branches_checked", 0)
        self.report.stats["branches_checked"] += len(seen)
        self.report.stats.setdefault("branches_unexercised", 0)
        self.report.stats["branches_unexercised"] += missing_total

    def _minimize(self, killers: Dict[str, List[str]]) -> None:
        """Smoke scenarios + a greedy minimal set of scenarios that together kill every killable mutant."""
        smoke = {"typical", "empty", "nulls"}
        for node, scs in self.scenarios.items():
            uncovered = {m for s in scs for m in killers.get(s.id, [])}
            chosen = set()
            while uncovered:
                best = max(scs, key=lambda s: len(uncovered & set(killers.get(s.id, []))))
                gain = uncovered & set(killers.get(best.id, []))
                if not gain:
                    break
                chosen.add(best.id)
                uncovered -= gain
            for s in scs:
                if s.kind in smoke or s.id in chosen or (s.kind == "boundary" and s.expect):
                    self.suite.scenarios.append(s)
                    self.suite.kills[s.id] = killers.get(s.id, [])

    def _whole_graph(self) -> None:
        bp, r = self.bp, self.report
        data = {sid: d["rows"] for sid, d in generate(bp, self.params, n_rows=300).items()}
        variants = {"baseline fixtures": data}
        for s in bp.sources:
            variants[f"source '{s.id}' empty"] = {**data, s.id: []}
        results = {}
        for label, d in variants.items():
            eng = Engine(bp, self.params).run(d)
            for node, err in eng.errors.items():
                if err.kind != "blocked":
                    r.add(Finding("executability", "error", f"whole-graph run ({label}) fails at node '{node}': {err.msg}", node=node))
            failed = False
            for tg in bp.targets:
                if tg.id in eng.df and not failed:
                    try:
                        results[(label, tg.id)] = len(eng.fetch(tg.id))
                    except Exception as e:  # runtime failure: pin it on the node that raised it
                        failed = True
                        node, msg = eng.diagnose() or (tg.id, spark_msg(e))
                        r.add(Finding("behavior" if label != "baseline fixtures" else "executability", "error",
                                      f"whole-graph run ({label}) fails at node '{node}': {msg}", node=node,
                                      confidence="medium", resolution="Handle the data condition that makes this node fail."))
        for tg in bp.targets:
            n = results.get(("baseline fixtures", tg.id))
            if n is not None:
                r.stats[f"target '{tg.id}' rows on fixtures"] = n
                if n == 0:
                    r.add(Finding("test-gap", "warning", f"target '{tg.id}' is empty on the generated fixtures; end-to-end behaviour is not exercised",
                                  confidence="medium", resolution="Provide fixtures (validation.data_script) that flow through every join."))

    # ---- agent-facing helpers ------------------------------------------------------------
    def try_scenario(self, node: str, inputs: Dict[str, List[dict]], why: str = "") -> dict:
        """Run an externally proposed (e.g. LLM) scenario against the original node and its surviving mutants."""
        t = next((x for x in self.bp.transformations if x.id == node), None)
        if t is None or node not in self.schemas:
            return {"ok": False, "error": f"node '{node}' is unknown or was not tested"}
        schemas = self.schemas[node]
        bad = [i for i in inputs if i not in schemas]
        if bad:
            return {"ok": False, "error": f"unknown input(s) {bad}; this node's inputs are {list(schemas)}"}
        full = {i: inputs.get(i, []) for i in schemas}
        base = run_node(self.bp, t, schemas, full, self.params)
        if base[0] == "error":
            return {"ok": False, "error": f"scenario does not run: {base[1]}"}
        killed = []
        for m in mutants_for(t):
            rec = next((x for x in self.report.mutants if x["node"] == node and x["description"] == m.description), None)
            if rec and rec["status"] == "survived" and result_key(run_node(self.bp, m.transformation, schemas, full, self.params)) != result_key(base):
                rec["status"], rec["killed_by"] = "killed", f"{node}:llm{len(self.suite.scenarios) + 1}"
                killed.append(m.description)
                self.report.findings = [f for f in self.report.findings if not (f.category == "test-gap" and f.node == node and f.message == f"mutant survived: {m.description}")]
        sc = Scenario(f"{node}:llm{len(self.suite.scenarios) + 1}", node, "proposed", why or "proposed by reviewer", full, origin="llm")
        if killed:
            self.suite.scenarios.append(sc)
            self.suite.kills[sc.id] = [f"{node}: {d}" for d in killed]
        bad_inv, _ = invariants(t, full, base[1])
        return {"ok": True, "killed_mutants": killed, "oracle_violations": bad_inv, "output_rows": len(base[1]),
                "kept": bool(killed)}

    def finish_stats(self) -> None:
        ms = self.report.mutants
        k = sum(m["status"] == "killed" for m in ms)
        self.report.stats["mutants_killed"] = k
        self.report.stats["mutation_score"] = f"{(100 * k / len(ms)):.0f}%" if ms else "n/a"
        self.report.stats["suite_size"] = len(self.suite.scenarios)


def run_validation(bp: PipelineBlueprint, source_text: str, source_path: str, out_dir: Optional[str] = None,
                   progress: Callable[[str], None] = None, **kw) -> Tuple[Report, Suite, Session]:
    s = Session(bp, source_text, source_path, progress=progress, **kw)
    s.run_static()
    s.run_tests()
    s.finish_stats()
    if out_dir:
        write_outputs(s, out_dir)
    return s.report, s.suite, s


def write_outputs(s: Session, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    s.finish_stats()
    with open(os.path.join(out_dir, "validation_report.json"), "w", encoding="utf-8") as f:
        f.write(s.report.to_json())
    with open(os.path.join(out_dir, "validation_report.md"), "w", encoding="utf-8") as f:
        f.write(s.report.to_markdown())
    with open(os.path.join(out_dir, "test_suite.json"), "w", encoding="utf-8") as f:
        json.dump(s.suite.to_dict(), f, indent=1, default=str)

