# Analysis output contract

This document defines the skill's reporting interface. It is not the executable conversion schema.

## Package

Write only to the designated output directory:

- `job.yaml`: conversion-specific draft using an installed schema, or explicitly labelled proposed draft when the schema is missing.
- `findings.json`: structured unresolved and resolved findings.
- `coverage.json`: source constructs and their representation status.
- `analysis.md`: concise human review report.
- `validation.json`: checks actually attempted and their results.
- Referenced SQL artifacts when the conversion requires them.

Reference original source paths and hashes; do not copy private source bodies unnecessarily. Do not overwrite an existing package silently. Use a new revision directory when prior analysis exists.

## Job draft

Identify the conversion type, contract version when known, source/target versions, source hashes, metadata snapshot, parameters, asset mappings, proposed operations or artifact references, prerequisites, findings, and provenance.

Never substitute the historical `migration-spec.template.yaml` for a missing conversion-specific schema. Missing schema or capability registry is an explicit blocker. Do not invent a supported schema version.

Keep proposed operation names distinguishable from registered runtime operations. Do not write executable Python, shell commands, or arbitrary callbacks into the operation graph.

## Findings

Each finding contains:

- `id`: package-unique identifier.
- `category`: compatibility, unsupported_logic, metadata, configuration, security, privacy, operational, or business_assumption.
- `severity`: info, low, medium, high, or critical.
- `confidence`: low, medium, or high, with an evidence-based explanation.
- `source_ref`: file and available line/cell/XML/statement location.
- `evidence`: sanitized observation; no credentials or real personal records.
- `affected_refs`: job, operation, or asset identifiers.
- `blocks`: list containing analysis, generation, and/or execution as appropriate.
- `status`: open, resolved, or accepted_risk.
- `resolution`: required action; reviewer reference and rationale when actually supplied.
- `rule_ref`: applied rule and version, or null for a clearly identified analyst inference.

An accepted risk does not remove structural failures or missing runtime support. Link reviewer decisions to exact source and artifact hashes.

## Coverage and validation

For each relevant construct, record its source location, status (`represented`, `irrelevant`, `unsupported`, or `unresolved`), operation/artifact references, and finding references. Explain irrelevant classifications. Report incomplete parsing explicitly.

Each validation result records check name, tool/version when available, input hashes, status (`passed`, `failed`, `not_run`, or `unavailable`), sanitized evidence, and blockers.

Report separately:

1. Analysis completion and coverage.
2. Contract validation.
3. Semantic validation.
4. Security and privacy checks.
5. Runtime equivalence testing.
6. Review and execution eligibility.

For this skill alone, runtime execution is `not_run`. Execution eligibility remains blocked until required independent checks and approvals exist. Never manufacture an execution result.

## Human report

Lead with the job's purpose, proposed conversion, and readiness limits. Summarize supported logic, unresolved behavior, sensitive-data handling, target assumptions, checks performed, and required reviewer decisions. Link the machine-readable artifacts.
