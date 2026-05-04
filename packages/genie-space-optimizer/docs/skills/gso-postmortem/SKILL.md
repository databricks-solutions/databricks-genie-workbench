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

0. **Verify the environment can reach the workspace.** Two prerequisites are easy to miss and produce silent empty audits:

   - `databricks auth profiles` lists `<profile>` and the listed host matches the workspace that ran the job. (Wrong workspace ⇒ `mlflow_audit` returns zero sibling runs even when the run exists.)
   - The MLflow client points at the workspace, not local. Set `MLFLOW_TRACKING_URI=databricks` and `DATABRICKS_CONFIG_PROFILE=<profile>` in the bundle invocation env. Without these, `MlflowClient.search_runs()` searches local MLflow (which is empty) and the audit returns no runs without raising.

   Pass them explicitly when invoking the bundle, e.g.:

   ```bash
   MLFLOW_TRACKING_URI=databricks DATABRICKS_CONFIG_PROFILE=<profile> \
     python -m genie_space_optimizer.tools.evidence_bundle ...
   ```

1. **Build the bundle.**

   ```bash
   python -m genie_space_optimizer.tools.evidence_bundle \
       --job-id <job_id> --run-id <run_id> --profile <profile> \
       --output-dir <output_dir> [--auto-backfill] \
       [--opt-run-id <recovered>] [--experiment-id <id>]
   ```

   The CLI prints `manifest.json` to stdout. Capture `resolved.optimization_run_id`.

   **Idempotence note.** The bundle short-circuits when `<output_dir>/<opt_run_id>/evidence/manifest.json` already exists with matching `(job_id, run_id, profile)` inputs. To force a re-pull (e.g., after deploying harness changes, after fixing tag schema, or after pointing at a different workspace), delete `manifest.json` or vary one input. There is no `--force` flag today.

2. **If the manifest reports `OPTIMIZATION_RUN_ID_UNRESOLVED`,** the harness on the workspace is older than the `GSO_RUN_MANIFEST_V1` emitter and stdout markers are absent. Do **not** stop yet — the opt_run_id is recoverable from the parent job run before asking the operator:

   1. Read `<bundle_dir>/evidence/job_run.json` and look at `job_parameters[*].run_id`. The lever_loop notebook receives the opt_run_id as a job parameter named `run_id`. Capture that value.
   2. Re-run the bundle with `--opt-run-id <recovered>`. Delete the prior `manifest.json` first (idempotence note above) so the audit is allowed to re-run with the resolved ID.
   3. Only ask the operator if step 1 returns no value (very rare — would mean the optimizer was invoked outside the standard lever-loop notebook contract).

3. **Inspect `missing_pieces`.**
   - If `PHASE_*_ARTIFACT_MISSING_ON_ANCHOR` is present AND `auto_backfill=false`, ask the operator: "decision-trail artifacts are missing on the anchor run; rerun the bundle with `--auto-backfill` (writes to MLflow) or proceed without?"
   - If the audit anchor resolved to `enrichment_snapshot` (or any non-`lever_loop` `genie.run_type`) **and** sibling artifacts are empty, the deployed harness is not tagging any run with `genie.run_type=lever_loop`. Do not treat this as `MLFLOW_AUDIT_FAILED`. Pass the bundle to the analysis skill — its degraded-mode rules cover this case by searching iteration full_eval runs by `genie.run_id`.
   - If `MLFLOW_AUDIT_FAILED`, surface the root error to the operator and stop. (This usually means the wrong workspace profile, or `MLFLOW_TRACKING_URI` is unset — see Step 0.)

4. **Hand off to `gso-lever-loop-run-analysis`** with `bundle_dir=<output_dir>/<opt_run_id>`. The analysis skill reads `evidence/manifest.json`, walks its checklists, decides whether to invoke `trace_fetcher`, and writes `postmortem.md` + `postmortem.json`.

   **Partial-run policy.** When the parent job run is `RUNNING` but the `lever_loop` task is `TERMINATED/SUCCESS` (typically because `finalize` and `deploy` tasks are still pending), the lever-loop is fully analyzable. Proceed with the postmortem and annotate `Metadata` with `finalize_state`/`deploy_state` as `pending` so the reader knows post-loop steps were not evaluated. Do not wait for the parent run to finish.

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
| `evidence_bundle` exits 2 (`OPTIMIZATION_RUN_ID_UNRESOLVED`) | First read `evidence/job_run.json :: job_parameters[*].run_id` and rerun with `--opt-run-id <recovered>`. Only ask the operator if that field is empty. |
| `mlflow_audit` returned 0 sibling runs but the run exists in the workspace UI | Almost always `MLFLOW_TRACKING_URI` was unset; the client searched local MLflow. Re-invoke with `MLFLOW_TRACKING_URI=databricks DATABRICKS_CONFIG_PROFILE=<profile>` set. See Step 0. |
| `mlflow_audit` anchor = `enrichment_snapshot` (no `genie.run_type=lever_loop` tag found) | Not a tooling failure — the deployed harness predates the canonical tag schema. Pass to the analysis skill anyway; its degraded rules search iteration runs directly. |
| `mlflow_audit` fails (wrong workspace) | Ask for the correct `--profile`. |
| `mlflow_backfill` fails | Ask the operator before retrying; do not loop. |
| Bundle short-circuited (manifest already exists) and you wanted a fresh pull | Delete `<bundle_dir>/evidence/manifest.json` and re-invoke. There is no `--force` flag. |
| Lever-loop stdout file `lever_loop_stdout.txt` is empty even though the task ran | Lever-loop is a notebook task in production; `databricks jobs get-run-output` returns empty `logs` for notebook tasks. The structured result is in `notebook_output.result`. The analysis skill is responsible for falling back to that. |
| Postmortem verdict = `INSUFFICIENT_EVIDENCE` | Ask the operator whether to widen the trace fetch beyond `--from-recommendations` (manual `--trace-id` flags). |

## Phase H: GSO Run Output Contract

When the run was produced by a lever-loop with Phase H landed, prefer the `gso_postmortem_bundle/` artifact tree over the legacy phase artifacts. The bundle is self-describing: every iteration's per-stage input/output/decisions is captured to MLflow under the parent lever-loop run.

### Inputs

- `optimization_run_id` (preferred): the canonical run id. Resolves the parent MLflow run via `genie.run_role=lever_loop` + `genie.optimization_run_id=<id>`.
- `(job_id, run_id)`: also accepted; the bundle resolves `optimization_run_id` from the job's `run_id` parameter and proceeds.

### Phase H workflow

1. **Locate the parent bundle.** Read the `GSO_ARTIFACT_INDEX_V1` marker from stdout (or `MarkerLog.artifact_index`) for `parent_bundle_run_id` + `artifact_index_path`. If the marker is absent, fall back to `tools.mlflow_audit.audit_parent_bundle(optimization_run_id=...)` which discovers the parent run by tag and reports manifest presence.
2. **Materialize the bundle locally.** Use `tools.evidence_bundle.download_parent_bundle(parent_run_id, target_dir=...)` to pull `gso_postmortem_bundle/*` into `runid_analysis/<opt>/evidence/gso_postmortem_bundle/`. The helper never raises — on failure it returns `(False, [MissingPiece(MLFLOW_AUDIT_FAILED, ...)])` so the postmortem can fall back to legacy phase artifacts.
3. **Read the manifest.** `gso_postmortem_bundle/manifest.json` carries `iteration_count`, `iterations`, `missing_pieces`, and `stage_keys_in_process_order` (the 9 executable stages). Use it to discover what to read; do not walk directories.
4. **For each iteration, read the per-stage capture.** `iter_NN/stages/<NN>_<stage_key>/{input,output,decisions}.json` is the authoritative typed record of every stage's I/O. The `<NN>_<stage_key>` directory name is process-ordered so a `ls` is naturally readable.
5. **Cross-check the iteration transcript.** `iter_NN/operator_transcript.md` is the human-readable view; `iter_NN/decision_trace.json` is the machine view. The transcript renderer (Phase H T6) mirrors `PROCESS_STAGE_ORDER` exactly, so the section for stage *N* in the transcript matches the directory `<NN>_<stage_key>/`.
6. **Inspect `journey_validation_all.json` for contract violations.** Phase H captures journey validation per iteration; the all-iterations view is the postmortem's canary for journey-contract gaps.
7. **Stage I/O attribution.** Phase H's distinguishing capability over legacy artifacts: comparing `iter_(N-1)/stages/<X>/output.json` to `iter_N/stages/<X>/output.json` lets the postmortem answer "which stage's output changed between iterations and why?" without re-running the optimizer. Use this for "Stage I/O attribution" sections in `postmortem.md`.

### Constraints

- Never grep raw stdout when the bundle is present and `manifest.missing_pieces` does not declare a stdout fallback was needed.
- Treat `manifest.missing_pieces` entries as authoritative: if a stage's capture failed, the postmortem must say so explicitly rather than silently presenting partial data.
- The bundle is read-only; the postmortem skill never writes back to MLflow.

