---
name: hive-to-iceberg
description: Analyze Hive table metadata and DDL for copy-based migration to Iceberg using Spark SQL DDL. Use for schema, partition, storage, and table-migration planning, not application transformation conversion.
---

# Hive metadata to Iceberg

Use this skill within the single Type Exchange analysis agent. Load [migration-analysis](../migration-analysis/SKILL.md) for shared intake, tool permissions, privacy/security assessment, evidence, and review rules. If invoked directly, apply that workflow first, then continue here without restarting routing. Do not create another agent or parallel thread.

Conversion identifier: `hive_to_iceberg`. This selects the intended contract family; it is not a claim that an executable schema or runtime handler is installed.

## Inputs

Metadata export, source DDL, table/view identity, source locations, explicit destination catalog/storage settings, and requested migration scope.

Use supplied metadata and sanitized source. Record missing inputs and continue only the analysis that does not depend on them.

## Analysis

Capture source catalog/database/table names, columns and nested types, partition columns, storage format, location, table properties, and table/view distinction. A view is not a physical table migration.

Plan copy-based migration into isolated test tables. Require explicit destination catalog, namespace, storage, partition specification, schema mapping, and reconciliation requirements. Existing Parquet files do not by themselves establish an Iceberg table.

Flag casing conflicts, unknown nullability, unsupported types, active-writer assumptions, and inferred keys. Do not copy all Hive properties to Iceberg without a supported mapping. Azure storage mappings must be supplied.

## Pair-specific YAML content

Source/target asset schemas, explicit type and partition mappings, separate Spark SQL DDL references, copy method, prerequisite ordering, reconciliation criteria, and cutover assumptions. No production cutover is executed.

Produce one job YAML using the installed `hive_to_iceberg` schema. Follow the shared [output contract](../migration-analysis/references/output-contract.md) for findings, coverage, validation, and the human report. If the schema or capability registry is missing, label the output a proposed draft and block execution. Do not copy the historical universal template or invent a supported schema version.

## Pair-specific blockers

Views treated as physical tables, unknown destination storage, unsupported type or property mappings, schema conflicts, and unresolved active-writer assumptions.

Distinguish supported analysis from executable conversion support. An approval cannot replace a missing runtime handler. Preserve source behavior and flag assumptions rather than silently changing it.

## Acceptance scenarios

A view is flagged for separate treatment; nested columns retain types; unknown Azure storage blocks execution; DDL remains an artifact; source tables remain unchanged.

Also apply the shared [security and privacy cases](../migration-analysis/references/acceptance-cases.md). These scenarios are evaluation requirements, not evidence of passing runtime tests.
