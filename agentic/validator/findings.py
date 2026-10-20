"""Finding / Report model (fields follow PLAN.md section 4: category, severity, confidence, location,
impact, resolution)."""
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

SEVERITIES = ("error", "warning", "info")


@dataclass
class Finding:
    category: str            # coverage | executability | schema | compat | security | test-gap | behavior | llm-review
    severity: str            # error | warning | info
    message: str
    node: Optional[str] = None            # blueprint node id
    source_location: Optional[str] = None  # file:line in the legacy source
    confidence: str = "high"              # high | medium | low (llm-proposed findings are never 'high')
    impact: str = ""
    resolution: str = ""
    origin: str = "deterministic"         # deterministic | llm
    evidence: Dict[str, Any] = field(default_factory=dict)

    def key(self):
        return (self.category, self.node, self.message)


@dataclass
class Report:
    job: str = ""
    findings: List[Finding] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)
    coverage_matrix: List[Dict[str, Any]] = field(default_factory=list)  # source construct -> status
    scenarios: List[Dict[str, Any]] = field(default_factory=list)
    mutants: List[Dict[str, Any]] = field(default_factory=list)

    def add(self, f: Finding) -> None:
        if f.key() not in {x.key() for x in self.findings}:
            self.findings.append(f)

    def extend(self, fs) -> None:
        for f in fs:
            self.add(f)

    def count(self, severity: str, category: str = None) -> int:
        return sum(1 for f in self.findings if f.severity == severity and (category is None or f.category == category))

    complete: bool = True  # False when a layer (e.g. the Spark-backed tests) could not run

    @property
    def result(self) -> str:
        """FAILED (error findings) > INCOMPLETE (a layer did not run) > PASSED. Warnings need review but do not block."""
        if self.count("error"):
            return "FAILED"
        return "PASSED" if self.complete else "INCOMPLETE"

    @property
    def passed(self) -> bool:
        return self.result == "PASSED"

    def to_dict(self) -> dict:
        return {"job": self.job, "result": self.result, "passed": self.passed, "complete": self.complete, "stats": self.stats,
                "findings": [asdict(f) for f in self.findings], "coverage_matrix": self.coverage_matrix,
                "scenarios": self.scenarios, "mutants": self.mutants}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)

    def to_markdown(self) -> str:
        s = self.stats
        out = [f"# Validation report: {self.job}", "",
               f"**Result: {self.result}** - {self.count('error')} error(s), "
               f"{self.count('warning')} warning(s), {self.count('info')} info"
               + ("" if self.complete else " - some checks did NOT run (see the warnings); this is not a pass"), ""]
        if s:
            out += ["## Summary", ""] + [f"- {k}: {v}" for k, v in s.items()] + [""]
        for sev in SEVERITIES:
            items = [f for f in self.findings if f.severity == sev]
            if not items:
                continue
            out += [f"## {sev.capitalize()}s ({len(items)})", ""]
            for f in items:
                loc = f" `{f.source_location}`" if f.source_location else ""
                node = f" [node `{f.node}`]" if f.node else ""
                out.append(f"- **{f.category}**{node}{loc}: {f.message}"
                           + (f" _(confidence: {f.confidence}, origin: {f.origin})_" if f.origin != "deterministic" or f.confidence != "high" else ""))
                if f.resolution:
                    out.append(f"  - Resolution: {f.resolution}")
            out.append("")
        if self.coverage_matrix:
            out += ["## Source -> YAML coverage", "", "| Source construct | Status | Detail |", "|---|---|---|"]
            for r in self.coverage_matrix:
                out.append(f"| {r['construct']} | {r['status']} | {r.get('detail', '')} |")
            out.append("")
        if self.mutants:
            surv = [m for m in self.mutants if m["status"] == "survived"]
            out += [f"## Mutation testing: {len(self.mutants) - len(surv)}/{len(self.mutants)} killed", ""]
            for m in surv:
                out.append(f"- SURVIVED `{m['node']}`: {m['description']}")
            out.append("")
        return "\n".join(out)
