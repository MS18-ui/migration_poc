---
name: pyspark2-to-spark3
description: Analyze PySpark 2 batch DataFrame jobs for execution by the Type Exchange Spark 3 runtime. Use for Python source-to-YAML migration, Spark compatibility assessment, and supported transformation extraction.
---

# PySpark 2 to Spark 3

Use this skill within the single Type Exchange analysis agent. Load [migration-analysis](../migration-analysis/SKILL.md) for shared intake, tool permissions, privacy/security assessment, evidence, and review rules. If invoked directly, apply that workflow first, then continue here without restarting routing. Do not create another agent or parallel thread.

Conversion identifier: `pyspark2_to_spark3`. This selects the intended contract family; it is not a claim that an executable schema or runtime handler is installed.

## Inputs

Python scripts, referenced local modules, dependency manifests, Spark settings, declared versions, and catalog metadata.

Use supplied metadata and sanitized source. Record missing inputs and continue only the analysis that does not depend on them.

## Analysis

Start with batch DataFrame workloads. Resolve aliases and variable lineage before assigning Spark meaning to a method name. A method called `select` on an unknown object is not proven to be a Spark operation.

Extract reads, filters, projections, explicit casts, joins, aggregates, nested-field access, array expansion, windows, duplicate handling, and writes. Record operation inputs, outputs, expressions, source locations, and unsupported fragments.

For every read and write, resolve asset references, schemas, formats, partition behavior, and write scope. Unknown paths or dynamic table names require findings.

Inspect date/timestamp parsing, timezone, decimal precision, cast failures, null behavior, case sensitivity, ordering, and Spark configuration. Consult version-matched Apache documentation when making a compatibility claim; record the exact versions and source. Do not declare an API incompatible solely because it appears in Spark 2 code.

Do not translate arbitrary Python, unresolved UDFs, RDD transformations, or streaming state into guessed SQL. A known call pattern is insufficient without supported semantics.

For this project's fixtures:

- Preserve the transaction view's missing-FX fallback while flagging its business assumption.
- Do not assume account IDs or currency/date FX keys are unique unless verified or declared.
- Distinguish absent addresses from a missing customer; flag multiple primary addresses.
- Preserve exact decimal intent and nested output types.
- Define deterministic tie handling before deduplication. Equal timestamps with conflicting records must not select an arbitrary winner.
- Record inclusive/exclusive boundaries and empty-input behavior for range replacement.

## Pair-specific YAML content

Typed reads, filters, projections, joins, aggregates, windows, expressions, nested-type operations, and writes. Each operation needs input/output references, source evidence, known schema information, and explicit semantics. Record Iceberg target mappings and configuration separately from source facts.

Produce one job YAML using the installed `pyspark2_to_spark3` schema. Follow the shared [output contract](../migration-analysis/references/output-contract.md) for findings, coverage, validation, and the human report. If the schema or capability registry is missing, label the output a proposed draft and block execution. Do not copy the historical universal template or invent a supported schema version.

## Pair-specific blockers

Missing handlers, unresolved UDFs or dynamic code, ambiguous duplicate handling, unknown date/time behavior, and unresolved write scope.

Distinguish supported analysis from executable conversion support. An approval cannot replace a missing runtime handler. Preserve source behavior and flag assumptions rather than silently changing it.

## Acceptance scenarios

A supported read/filter/aggregate chain has connected operations and evidence; a non-Spark object is not misclassified; an ambiguous latest-record tie blocks execution; missing schema support yields a draft, not execution-ready YAML.

Also apply the shared [security and privacy cases](../migration-analysis/references/acceptance-cases.md). These scenarios are evaluation requirements, not evidence of passing runtime tests.
