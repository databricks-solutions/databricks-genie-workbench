---
sidebar_position: 4
description: "Use Genie Code and GSO's Delta audit tables to reconstruct an optimization run."
---

# Debug GSO runs with Genie Code

GSO notebook output now provides a concise first-response summary: run and log
identifiers, benchmark quality counts, baseline and attempt decisions, terminal
reason, and the next Delta table to inspect. The notebook output is intentionally
bounded; it does not dump benchmark SQL, prompts, full configurations, or every
question result.

For a complete root-cause analysis, invoke Genie Code in the same Databricks
workspace and give it the repository's
[GSO Run Debugger prompt](https://github.com/databricks-solutions/databricks-genie-workbench/blob/main/docs/debug-prompt.md).
The prompt is read-only and current-schema aware. It starts by discovering the
installed tables and columns, then reconstructs the four-task run from durable
Delta evidence.

## Values to collect

Copy these values from the Workbench run page or notebook diagnostics:

- GSO log catalog and schema;
- optimizer run ID;
- Genie Agent Space ID;
- Databricks Job run URL, when available.

Paste them into the prompt placeholders before asking Genie Code to begin. The
run ID, log catalog, and log schema are required.

## Evidence model

| Source | Debugging signal |
|---|---|
| `genie_opt_runs` | Run envelope, trigger snapshot presence, status, champion pointer, and final reason |
| `genie_opt_stages` | Chronological four-task and nested-stage timeline, errors, and durations |
| `genie_opt_artifacts` | Latest run manifest, benchmark QC, enrichment context, and publish record |
| `genie_opt_iterations` | Baseline, attempts, per-question assessments, hypotheses, decisions, rollbacks, and terminal reason |
| `genie_opt_patches` | Patch targets, provenance, and rollback history |
| `genie_opt_benchmark_mutations` | Benchmark additions, removals, changes, and their reasons |

The prompt requires a table-row citation for every conclusion and tells Genie
Code to distinguish facts from hypotheses. It also selects the latest artifact
per kind, parses `rows_json` assessments and reasons, and handles older installs
by describing the schema before issuing projections.

## What notebook output can answer by itself

Notebook diagnostics should be enough to identify:

- which of the four tasks failed or completed;
- benchmark validity, repair, semantic-review, and window counts;
- baseline accuracy and native evaluation status;
- each attempted patch family's accept, retry, or rollback decision;
- terminal reason, champion accuracy, and whether anything was published;
- the exact log schema and primary table for deeper investigation.

Use the Delta debugger when you need question-level causes, artifact history,
patch provenance, inconsistencies between terminal sources, or a defensible
cross-table narrative.

## Safety boundary

Benchmark expected SQL is evaluation truth. Do not paste it into Genie Agent
instructions, examples, descriptions, or proposed patches. The debugger prompt
defaults to IDs, counts, reason codes, and bounded evidence, and only permits
the minimum SQL fragment needed for a specific root-cause analysis.

## Related documentation

- [Auto-Optimize (GSO)](/docs/features/auto-optimize)
- [Troubleshooting](/docs/reference/troubleshooting)
- [Operations Guide](/docs/platform/operations)
