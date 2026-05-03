---
name: gso-postmortem
description: Use when an operator asks to "postmortem", "diagnose", "troubleshoot", or "analyze" a Genie Space Optimizer Databricks Job run from a `(job_id, run_id)` pair. This is the single entry point that sequences evidence-bundle → analysis → optional trace fetch → optional intake hand-off into one flow. Operators should not need to memorize CLI commands or which skill to call first; this skill does that. Read-only by default; only the (operator-confirmed) `--auto-backfill` and `gso-replay-cycle-intake` hand-off perform writes outside `runid_analysis/`.
---

# GSO Run Postmortem Orchestrator

Use this skill when the operator says "postmortem", "diagnose", "analyze", or "troubleshoot" a lever-loop run and supplies `(job_id, run_id)`. The skill sequences three lower-level skills/tools into a single deterministic flow.

## Required Inputs

- `job_id`: Databricks Job ID.
- `run_id`: Databricks parent Job Run ID.

## Optional Inputs

- `profile`: Databricks CLI profile. Default: ask the operator if not obvious from context.
- `auto_backfill`: When `true`, the bundle invocation runs `mlflow_backfill` automatically for missing decision-trail artifacts. Default: `false` (operator approves before any writes).
- `output_dir`: Bundle root. Default: `packages/genie-space-optimizer/docs/runid_analysis`.

## Required Related Skills

- `gso-lever-loop-run-analysis` — produces the structured postmortem from the bundle.
- `gso-replay-cycle-intake` — write-side hand-off when the postmortem verdict is "ready to advance the burn-down".
- `databricks-jobs` — fallback when `evidence_bundle` cannot reach the workspace.

## Operating Principle

This skill orchestrates; it does not analyze. Reasoning lives in `gso-lever-loop-run-analysis`. Writes live in `gso-replay-cycle-intake`. This skill is read-only by default and only invokes write-side commands when the operator confirms.

## Workflow

1. **Build the bundle.**

   ```bash
   python -m genie_space_optimizer.tools.evidence_bundle \
       --job-id <job_id> --run-id <run_id> --profile <profile> \
       --output-dir <output_dir> [--auto-backfill]
   ```

   The CLI prints `manifest.json` to stdout. Capture `resolved.optimization_run_id`.

2. **If the manifest reports `OPTIMIZATION_RUN_ID_UNRESOLVED`,** stop and ask the operator. Without it, no postmortem can be filed; the bundle dir is `runid_analysis/unresolved_<run_id>/`.

3. **Inspect `missing_pieces`.**
   - If `PHASE_*_ARTIFACT_MISSING_ON_ANCHOR` is present AND `auto_backfill=false`, ask the operator: "decision-trail artifacts are missing on the anchor run; rerun the bundle with `--auto-backfill` (writes to MLflow) or proceed without?"
   - If `MLFLOW_AUDIT_FAILED`, surface the root error to the operator and stop. (This usually means the wrong workspace profile.)

4. **Hand off to `gso-lever-loop-run-analysis`** with `bundle_dir=<output_dir>/<opt_run_id>`. The analysis skill reads `evidence/manifest.json`, walks its checklists, decides whether to invoke `trace_fetcher`, and writes `postmortem.md` + `postmortem.json`.

5. **Read the postmortem verdict** (`postmortem.json.status`):
   - `READY_TO_MERGE` → ask: "advance the burn-down? hand off to `gso-replay-cycle-intake` with `fixture_source=bundle://<opt_run_id>`?"
   - `PILOT_NEEDS_RERUN` → recommend the smallest defect fix; do not rerun automatically.
   - `MERGE_GATE_GAP` → recommend codebase work; do not rerun automatically.
   - `BASELINE_REGRESSION` → recommend rollback or rescope.
   - `INSUFFICIENT_EVIDENCE` → only possible if Trace Fetch Decision Rule was tripped and traces still didn't help; surface what's missing and ask for guidance.

6. **Print a one-paragraph summary to the operator** with:
   - Bundle path.
   - Postmortem path.
   - Verdict.
   - Recommended next step (a single sentence).

## Cross-Skill Hand-offs

- This skill **calls** `gso-lever-loop-run-analysis`, `gso-replay-cycle-intake` (with operator confirmation), and the `evidence_bundle` / `trace_fetcher` CLIs.
- This skill is **never called by** another skill — it is the user-facing entry point.

## Safety

- Read-only by default. The only writes are: a) the evidence bundle directory under `runid_analysis/` (gitignored), b) `mlflow_backfill` invocations when `auto_backfill=true` (operator-confirmed), c) `gso-replay-cycle-intake` hand-offs (operator-confirmed).
- Never invoke `trace_fetcher` directly; that decision belongs to `gso-lever-loop-run-analysis`.
- Never write outside `runid_analysis/<opt_run_id>/` from this skill's own steps.

## Failure Modes And Recovery

| Failure | Recovery |
|---|---|
| `evidence_bundle` exits 2 (`OPTIMIZATION_RUN_ID_UNRESOLVED`) | Ask the operator for the opt_run_id, or rerun with `--opt-run-id <id>` once the harness fix lands. |
| `mlflow_audit` fails (wrong workspace) | Ask for the correct `--profile`. |
| `mlflow_backfill` fails | Ask the operator before retrying; do not loop. |
| Postmortem verdict = `INSUFFICIENT_EVIDENCE` | Ask the operator whether to widen the trace fetch beyond `--from-recommendations` (manual `--trace-id` flags). |
