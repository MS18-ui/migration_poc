---
name: sqoop-to-spark
description: Analyze Sqoop import/export definitions for Spark ingestion conversion. Use to extract JDBC mappings and load semantics; the initial supported proposal subset is full imports.
---

# Sqoop to Spark ingestion

Use this skill within the single Type Exchange analysis agent. Load [migration-analysis](../migration-analysis/SKILL.md) for shared intake, tool permissions, privacy/security assessment, evidence, and review rules. If invoked directly, apply that workflow first, then continue here without restarting routing. Do not create another agent or parallel thread.

Conversion identifier: `sqoop_to_spark`. This selects the intended contract family; it is not a claim that an executable schema or runtime handler is installed.

## Inputs

Sqoop commands/job definitions, supplied properties, source schemas, connection secret references, destination mappings, and declared versions.

Use supplied metadata and sanitized source. Record missing inputs and continue only the analysis that does not depend on them.

## Analysis

Identify import/export direction, JDBC secret references, query/table selection, column mappings, split key, parallelism, incremental mode, watermark, and destination behavior.

The first supported subset is full imports. Export, connector-specific features, and unresolved incremental state remain findings unless explicitly supported. Do not infer retry or transaction equivalence from a matching JDBC query.

## Pair-specific YAML content

Import/export direction, JDBC connection reference, table or extraction query, column mappings, split settings, read consistency assumptions, load mode, and destination. Incremental proposals retain watermark state and update rules explicitly.

Produce one job YAML using the installed `sqoop_to_spark` schema. Follow the shared [output contract](../migration-analysis/references/output-contract.md) for findings, coverage, validation, and the human report. If the schema or capability registry is missing, label the output a proposed draft and block execution. Do not copy the historical universal template or invent a supported schema version.

## Pair-specific blockers

Exports or incremental modes without registered support, connector-specific behavior, missing watermarks, unclear transaction/retry behavior, and embedded credentials.

Distinguish supported analysis from executable conversion support. An approval cannot replace a missing runtime handler. Preserve source behavior and flag assumptions rather than silently changing it.

## Acceptance scenarios

Full import has complete source/destination mappings; credentials are redacted; unresolved watermark blocks incremental execution; an export is not silently converted to import.

Also apply the shared [security and privacy cases](../migration-analysis/references/acceptance-cases.md). These scenarios are evaluation requirements, not evidence of passing runtime tests.
