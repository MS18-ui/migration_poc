---
name: hiveql-to-spark-sql
description: Analyze and propose HiveQL queries and DML as Spark SQL with statement coverage and semantic review. Use for Hive SQL migration; coordinate table DDL requirements with the Hive-to-Iceberg skill.
---

# HiveQL to Spark SQL

Use this skill within the single Type Exchange analysis agent. Load [migration-analysis](../migration-analysis/SKILL.md) for shared intake, tool permissions, privacy/security assessment, evidence, and review rules. If invoked directly, apply that workflow first, then continue here without restarting routing. Do not create another agent or parallel thread.

Conversion identifier: `hiveql_to_spark_sql`. This selects the intended contract family; it is not a claim that an executable schema or runtime handler is installed.

## Inputs

SQL files, variable bindings, source and target versions, referenced schemas, and session settings.

Use supplied metadata and sanitized source. Record missing inputs and continue only the analysis that does not depend on them.

## Analysis

Account for every statement, including session settings and variable substitutions. Identify tables, columns, functions, statement order, and dependencies.

Queries and DML become proposed Spark SQL artifacts referenced by the job YAML. DDL becomes separate Spark SQL DDL for Iceberg, with hashes and prerequisites referenced by YAML. Do not encode DDL bodies as transformation operations.

Flag unresolved substitutions, dynamic SQL, custom functions, Hive-specific commands, and unsupported storage clauses. Parse success alone does not establish equivalent SQL behavior. Trino is not a conversion target in this project.

## Pair-specific YAML content

Ordered Spark SQL artifact references, hashes, parameter definitions, asset references, source dialect, target dialect, and prerequisites. Bind values safely and validate identifiers; do not rely on unrestricted string substitution.

Produce one job YAML using the installed `hiveql_to_spark_sql` schema. Follow the shared [output contract](../migration-analysis/references/output-contract.md) for findings, coverage, validation, and the human report. If the schema or capability registry is missing, label the output a proposed draft and block execution. Do not copy the historical universal template or invent a supported schema version.

## Pair-specific blockers

Unresolved substitutions, unsupported functions or commands, missing schema, and unverified type/null/date behavior.

Distinguish supported analysis from executable conversion support. An approval cannot replace a missing runtime handler. Preserve source behavior and flag assumptions rather than silently changing it.

## Acceptance scenarios

Every statement is accounted for; unsupported Hive commands remain findings; DML references a SQL artifact; DDL is handed to the Iceberg job with a dependency rather than silently discarded.

Also apply the shared [security and privacy cases](../migration-analysis/references/acceptance-cases.md). These scenarios are evaluation requirements, not evidence of passing runtime tests.
