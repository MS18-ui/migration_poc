---
name: yarn-to-ocp
description: Analyze Spark-on-YARN submission and HDFS dependencies for an OCP deployment proposal. Use for resource, image, identity, storage, and submission mappings, not Spark application conversion.
---

# YARN/HDFS to OCP

Use this skill within the single Type Exchange analysis agent. Load [migration-analysis](../migration-analysis/SKILL.md) for shared intake, tool permissions, privacy/security assessment, evidence, and review rules. If invoked directly, apply that workflow first, then continue here without restarting routing. Do not create another agent or parallel thread.

Conversion identifier: `yarn_to_ocp`. This selects the intended contract family; it is not a claim that an executable schema or runtime handler is installed.

## Inputs

Submission commands/configuration, dependency manifests, storage references, environment mappings, and target OCP requirements.

Use supplied metadata and sanitized source. Record missing inputs and continue only the analysis that does not depend on them.

## Analysis

Extract submission arguments, resources, dependencies, storage assumptions, credentials references, and entry points. Separate application transformations from deployment requirements.

Require explicit image, namespace, service account, catalog access, storage mapping, and secrets references. Flag YARN-specific settings without a mapping. Never assume copying a path or generating a manifest proves deployment or data movement.

Azure hosting is a project constraint; it does not select the OCP environment, catalog implementation, or storage topology.

## Pair-specific YAML content

Runtime job reference, image/dependency requirements, resources, namespace, service account, catalog access, storage mapping, secret references, submission configuration, and environment preflight checks.

Produce one job YAML using the installed `yarn_to_ocp` schema. Follow the shared [output contract](../migration-analysis/references/output-contract.md) for findings, coverage, validation, and the human report. If the schema or capability registry is missing, label the output a proposed draft and block execution. Do not copy the historical universal template or invent a supported schema version.

## Pair-specific blockers

Unmapped YARN options or HDFS paths, missing image/identity/catalog settings, unavailable OCP environment, and unsupported submission behavior.

Distinguish supported analysis from executable conversion support. An approval cannot replace a missing runtime handler. Preserve source behavior and flag assumptions rather than silently changing it.

## Acceptance scenarios

Unknown HDFS replacement stays unresolved; application logic is linked rather than rewritten; Azure hosting does not imply a particular cluster; artifact generation is not reported as deployment success.

Also apply the shared [security and privacy cases](../migration-analysis/references/acceptance-cases.md). These scenarios are evaluation requirements, not evidence of passing runtime tests.
