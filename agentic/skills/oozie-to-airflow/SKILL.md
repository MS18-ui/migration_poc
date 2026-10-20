---
name: oozie-to-airflow
description: Analyze Oozie workflows and coordinator definitions for Airflow scheduling conversion. Use for task graphs and scheduling semantics; reference Spark jobs instead of translating their application logic.
---

# Oozie to Airflow

Use this skill within the single Type Exchange analysis agent. Load [migration-analysis](../migration-analysis/SKILL.md) for shared intake, tool permissions, privacy/security assessment, evidence, and review rules. If invoked directly, apply that workflow first, then continue here without restarting routing. Do not create another agent or parallel thread.

Conversion identifier: `oozie_to_airflow`. This selects the intended contract family; it is not a claim that an executable schema or runtime handler is installed.

## Inputs

Workflow/coordinator XML, properties, referenced action files, source/target versions, and supplied timezone/schedule settings.

Use supplied metadata and sanitized source. Record missing inputs and continue only the analysis that does not depend on them.

## Analysis

Parse workflow actions, success/failure transitions, decisions, forks/joins, parameters, retries, and external dependencies. Distinguish workflow XML from coordinator scheduling; missing schedule information stays unknown.

Reference a separately analyzed Spark job from an Airflow task. Do not translate Oozie actions into Spark transformation logic. Identify unsupported action types and ambiguous failure/branch behavior before DAG generation.

## Pair-specific YAML content

Tasks, action references, success/failure/conditional edges, fork/join behavior, retries, timeouts, schedule/data dependencies, parameters, and connection references. Track separately analyzed job dependencies.

Produce one job YAML using the installed `oozie_to_airflow` schema. Follow the shared [output contract](../migration-analysis/references/output-contract.md) for findings, coverage, validation, and the human report. If the schema or capability registry is missing, label the output a proposed draft and block execution. Do not copy the historical universal template or invent a supported schema version.

## Pair-specific blockers

Unsupported actions, unresolved schedules or timezone, unavailable external actions, cycles, and ambiguous failure or branch/join semantics.

Distinguish supported analysis from executable conversion support. An approval cannot replace a missing runtime handler. Preserve source behavior and flag assumptions rather than silently changing it.

## Acceptance scenarios

A Spark action references a separate Spark job; a workflow-only input does not invent a coordinator schedule; failure transitions survive; unresolved fork/join behavior blocks DAG readiness.

Also apply the shared [security and privacy cases](../migration-analysis/references/acceptance-cases.md). These scenarios are evaluation requirements, not evidence of passing runtime tests.
