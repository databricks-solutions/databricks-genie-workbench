# Genie Space Optimizer

GSO is the benchmark-driven optimization engine used by Genie Workbench. It is
a Python wheel plus a four-notebook Databricks Job; it is not a standalone
Databricks App and has no package-local frontend.

For repository-wide rules, deployment paths, and dependency policy, also read
the root `AGENTS.md`.

## Supported workflow

The job is a linear four-task DAG:

1. `run_intake_and_snapshot.py`
2. `run_benchmark_qc_and_repair.py`
3. `run_optimize.py`
4. `run_publish_and_audit.py`

Each task reads job parameters and durable Delta state by `run_id`. Do not add
notebook-local task-value handoffs or revive the retired harness/stage engine.
The root bundle and notebook installer must keep equivalent task definitions.

## Commands

```bash
uv sync --frozen
uv run pytest
uv build
```

The authoritative dependency lock is the repository-root `uv.lock`; there is no
package-local Python or npm lockfile.

## Active package layout

```text
src/genie_space_optimizer/
  common/          Shared clients, configuration, prompts, and Delta helpers
  integration/     Workbench-facing trigger/apply/discard/revert API
  iq_scan/         Pure IQ scoring shared with Workbench
  jobs/            Four Databricks notebook entrypoints
  optimization/    Native benchmark QC, patch/eval loop, publish, and state
  backend/
    job_launcher.py  Shared bundle-job launcher used by Workbench
    utils.py         Shared numeric/JSON coercion helpers
```

Prompt templates are version-controlled in `common/config.py`. GSO uses MLflow
for experiment tracking, traces, and evaluation datasets, but has no MLflow
Prompt Registry runtime or permission dependency.

## Persistence rules

- `optimization/ddl.py` defines only tables written by the four-task workflow.
- Schema evolution is additive. Never automatically drop or rename historical
  Delta tables or columns.
- New iteration fields must be added to DDL, the additive migration list,
  `state.write_iteration`, the Workbench API model, and tests together.
- `genie_opt_benchmark_mutations` is the supported benchmark-change ledger.

## Evaluation and leakage

The native Genie benchmark evaluation is authoritative. Per-question results
use `GOOD`, `BAD`, and `NEEDS_REVIEW`; do not reintroduce the retired arbiter or
repeatability scorer contracts.

`optimization/leakage.py` guards inference-visible writes from copying benchmark
questions or expected SQL into the optimized Space. New patch types that can
carry text must be routed through the same firewall.

## Testing

Offline tests live under `tests/`. Keep architecture coverage that prevents the
four notebooks from importing retired modules such as `harness`, `evaluation`,
`optimizer`, `scorers`, `stages`, RCA, synthesis, or replay modules.

Integration testing that requires Genie APIs, SQL warehouses, model serving, or
Databricks auth must run in a deployed workspace; do not start a local server.
