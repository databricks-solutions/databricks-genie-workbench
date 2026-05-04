---
name: gso-postmortem
description: Use when an operator asks to "postmortem", "diagnose", "troubleshoot", or "analyze" a Genie Space Optimizer Databricks Job run from a `(job_id, run_id)` pair. This is the single entry point that sequences evidence-bundle → analysis → optional trace fetch → optional intake hand-off into one flow. Operators should not need to memorize CLI commands or which skill to call first; this skill does that. Read-only by default; only the (operator-confirmed) `--auto-backfill` and `gso-replay-cycle-intake` hand-off perform writes outside `runid_analysis/`.
---

# GSO Run Postmortem Orchestrator

> **Canonical example.** Run
> `0ade1a99-9406-4a68-a3bc-8c77be78edcb` is preserved as the
> canonical postmortem of a `MERGE_GATE_GAP` produced by a
> below-threshold attribution-drift acceptance. Reading
> `docs/runid_analysis/0ade1a99-9406-4a68-a3bc-8c77be78edcb/postmortem.md`
> alongside `evidence/lever_loop_export_run_text.txt` gives a
> complete worked example of the workflow below.

Use this skill when the operator says "postmortem", "diagnose", "analyze", or "troubleshoot" a lever-loop run and supplies `(job_id, run_id)`. The skill sequences three lower-level skills/tools into a single deterministic flow.

## Required Inputs

- `job_id`: Databricks Job ID.
- `run_id`: Databricks parent Job Run ID.

## Optional Inputs

- `profile`: Databricks CLI profile. Default: ask the operator if not obvious from context.
- `auto_backfill`: When `true`, the bundle invocation runs `mlflow_backfill` automatically for missing decision-trail artifacts. Default: `false` (operator approves before any writes).
- `output_dir`: Bundle root. Default: `packages/genie-space-optimizer/docs/runid_analysis`.
- `lever_loop_task_run_id`: Optional but must be verified against the parent run. If multiple `lever_loop` task attempts exist, analyze the latest attempt, not the first matching task.

## Required Related Skills

- `gso-lever-loop-run-analysis` — produces the structured postmortem from the bundle.
- `gso-replay-cycle-intake` — write-side hand-off when the postmortem verdict is "ready to advance the burn-down".
- `databricks-jobs` — fallback when `evidence_bundle` cannot reach the workspace.

## Operating Principle

This skill orchestrates; it does not analyze. Reasoning lives in `gso-lever-loop-run-analysis`. Writes live in `gso-replay-cycle-intake`. This skill is read-only by default and only invokes write-side commands when the operator confirms.

## Terminal-success first (read this before any other section)

For each iteration `N` in the operator transcript:

  - Read `SECTION "Terminal Success"` first (Plan N2 cycle 4).
  - For every `cluster_id` listed there:
    - Mark it `RESOLVED` in the postmortem analysis.
    - Skip any `Re-run RCA prompt for {cluster_id}` or
      `rca_ungrounded` finding for that cluster — that is
      stale-by-design pre-acceptance noise that the renderer is
      annotating with `[RESOLVED BY {ag_id} ✓]`, not a real RCA gap.

This avoids the 2afb0be2-style misdiagnosis where the transcript said
`outcome=unresolved reason=rca_ungrounded` for a cluster the
optimizer just resolved.

## Contract quality (always report, even on READY_TO_MERGE)

For each iteration in `GSO_ITERATION_SUMMARY_V1` (Plan N1 cycle 4):

  - if `journey_violation_count > 0`:
    - report iteration `N` had `K` violations (cite kind + detail
      from `phase_a/journey_validation/iter_N.json`, capped at
      5 examples).
    - file under `Contract gaps to fix before next merge`, NOT
      under `What Failed`. The optimizer can have reached 100%
      accuracy and still produced contract violations; both
      facts are real and both belong in the postmortem.
  - Distinguish `READY_TO_MERGE optimizer-quality` (accuracy +
    convergence + acceptance-decisions clean) from
    `READY_TO_MERGE contract-quality` (journey + decision-trace
    + transcript projections clean). A run can be the former and
    not the latter; the postmortem must surface that.

## Workflow

0. **Verify the environment can reach the workspace.** Two prerequisites are easy to miss and produce silent empty audits:

   - `databricks auth profiles` lists `<profile>` and the listed host matches the workspace that ran the job. (Wrong workspace ⇒ `mlflow_audit` returns zero sibling runs even when the run exists.)
   - The MLflow client points at the workspace, not local. Set `MLFLOW_TRACKING_URI=databricks` and `DATABRICKS_CONFIG_PROFILE=<profile>` in the bundle invocation env. Without these, `MlflowClient.search_runs()` searches local MLflow (which is empty) and the audit returns no runs without raising.

   Pass them explicitly when invoking the bundle, e.g.:

   ```bash
   MLFLOW_TRACKING_URI=databricks DATABRICKS_CONFIG_PROFILE=<profile> \
     python -m genie_space_optimizer.tools.evidence_bundle ...
   ```

0.5. **Resolve the lever-loop task attempt before analysis.** Always inspect the parent run's task list before trusting bundle stdout. When a parent run contains multiple tasks with `task_key == "lever_loop"` (original plus repairs/retries), choose the latest task attempt by task `start_time`/`end_time` when present, otherwise by the order returned in `job_run.tasks`. Record all attempts in the postmortem metadata.

   - If the latest `lever_loop` attempt is `TERMINATED/SUCCESS`, analyze that attempt.
   - If the latest attempt failed and an earlier attempt succeeded, default to the latest failed attempt and classify the run as a task/harness failure unless the operator explicitly asks for the latest successful optimizer attempt.
   - If the operator supplied `lever_loop_task_run_id` but it is not the latest attempt, say so and ask before analyzing the older task.

   The current `evidence_bundle` helper may still anchor to the first matching task in legacy workspaces. After bundle creation, compare `manifest.resolved.lever_loop_task_run_id` and `evidence/job_run.json` against the latest attempt selected here. If they differ, export the latest attempt manually with Step 4 and use that transcript as the primary evidence. Keep the bundle's original anchor as a tooling finding, not as the optimizer evidence source.

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

3.5. **Check for loud bundle-assembly failure.** Parse stdout markers (via `tools.marker_parser.parse_markers(...)`) and inspect `MarkerLog.bundle_assembly_failed`. If non-empty:
    - The Phase H bundle was *intended* (the harness reached the C18 block) but assembly raised. Treat this as a `PHASE_H_BUNDLE_ASSEMBLY_FAILED` finding, distinct from "harness predates Phase H" (which leaves the marker absent and `manifest.json` missing).
    - Surface `error_type` and `error_message` from the marker payload to the operator. Do not silently fall back to legacy artifacts: legacy artifacts are still useful, but the postmortem must call out that Phase H was attempted and failed so the next run's harness deploy can be fixed.

4. **Recover the full notebook transcript for the selected lever-loop attempt when the Jobs output is only an exit JSON.** This is mandatory when `evidence/lever_loop_stdout.txt` lacks the expected human-readable sections such as `EVALUATION SUMMARY — Iteration`, `FULL EVAL [`, `GSO_CONVERGENCE_V1`, or `PHASE_A_REPLAY_FIXTURE_JSON_BEGIN`, or when Step 0.5 found that the bundle anchored to an older `lever_loop` attempt.

   `databricks jobs get-run-output` often returns only `notebook_output.result` for notebook tasks. The full cell output is recoverable through `jobs export-run`:

   ```bash
   databricks jobs export-run <latest_lever_loop_task_run_id> \
     --profile <profile> --views-to-export ALL --output json \
     > <bundle_dir>/evidence/lever_loop_export_run.json
   ```

   The JSON contains HTML with an encoded `__DATABRICKS_NOTEBOOK_MODEL`. Decode it and write the text to `<bundle_dir>/evidence/lever_loop_export_run_text.txt`. Use that file as the primary legacy transcript for:
   - `GSO_RUN_MANIFEST_V1` start/end markers.
   - `GSO_CONVERGENCE_V1` (`best_accuracy`, `iteration_counter`, `thresholds_met`).
   - `FULL EVAL [...]` accept/rollback sections.
   - `target_fixed_qids`, `target_still_hard_qids`, `out_of_target_regressed_qids`.
   - `PHASE_A_REPLAY_FIXTURE_JSON_BEGIN/END` for replay-intake evidence.

   **Then parse it with the typed parser.** Once `lever_loop_export_run_text.txt` is on disk, hand it to `tools.lever_loop_stdout_parser.parse_lever_loop_stdout(...)` rather than grepping the file directly:

   ```python
   from genie_space_optimizer.tools.lever_loop_stdout_parser import (
       parse_lever_loop_stdout,
   )
   text = (bundle_dir / "evidence" / "lever_loop_export_run_text.txt").read_text()
   view = parse_lever_loop_stdout(text)
   # view.optimization_run_summary.final_accuracy_pct
   # view.evaluation_summary[3].target_still_hard_qids
   # view.evaluation_summary[3].target_still_hard_qids_source  # "explicit" | "derived" | "unknown"
   # view.proposal_inventory[3]["AG_COVERAGE_H003"]
   # view.blast_radius_drops[3]
   # view.patch_survival[3]
   # view.acceptance_decision[3]
   ```

   The parser is read-only and forgiving: missing blocks return `None` / empty rather than raising. Pass `view` to the analysis skill alongside the bundle directory; the analysis skill's "Lever-Loop Mechanics Health" checklist consumes it directly. When `target_still_hard_qids_source == "derived"`, footnote that fact in the postmortem (the harness omits the explicit field on accepted iters; the parser computed it from `target_qids - target_fixed_qids` — this is the canonical `ACCEPTANCE_TARGET_BLIND` evidence path).

   **Do not ask the operator to paste notebook output until both `export-run` and the parser have run.** If the parser returns `optimization_run_summary=None`, then ask for the pasted transcript and save it as `<bundle_dir>/evidence/lever_loop_operator_paste.txt` before analysis.

5. **Hand off to `gso-lever-loop-run-analysis`** with `bundle_dir=<output_dir>/<opt_run_id>`. The analysis skill reads `evidence/manifest.json`, walks its checklists, decides whether to invoke `trace_fetcher`, and writes `postmortem.md` + `postmortem.json`.

   The analysis must include a grounded "Optimizer Improvement Next Steps" section. Each next step must map evidence from the run to a lever-loop stage and a concrete module/file to fix. Do not end with a generic rerun recommendation. For each unresolved failure mode, name:
   - the broken stage (`rca_evidence`, `cluster_formation`, `action_group_selection`, `proposal_generation`, `safety_gates`, `applied_patches`, `post_patch_evaluation`, `acceptance_decision`, or `learning_next_action`);
   - the observed evidence (QID, AG, proposal/gate/rollback marker, or artifact path);
   - the module likely responsible;
   - the smallest code/test change before the next run.

   **Partial-run policy.** When the parent job run is `RUNNING` but the `lever_loop` task is `TERMINATED/SUCCESS` (typically because `finalize` and `deploy` tasks are still pending), the lever-loop is fully analyzable. Proceed with the postmortem and annotate `Metadata` with `finalize_state`/`deploy_state` as `pending` so the reader knows post-loop steps were not evaluated. Do not wait for the parent run to finish.

6. **Read the postmortem verdict** (`postmortem.json.status`):
   - `READY_TO_MERGE` → ask: "advance the burn-down? hand off to `gso-replay-cycle-intake` with `fixture_source=bundle://<opt_run_id>`?"
   - `PILOT_NEEDS_RERUN` → recommend the smallest defect fix; do not rerun automatically.
   - `MERGE_GATE_GAP` → recommend codebase work; do not rerun automatically.
   - `BASELINE_REGRESSION` → recommend rollback or rescope.
   - `INSUFFICIENT_EVIDENCE` → only possible if Trace Fetch Decision Rule was tripped and traces still didn't help; surface what's missing and ask for guidance.

7. **Print a one-paragraph summary to the operator** with:
   - Bundle path.
   - Postmortem path.
   - Verdict.
   - Recommended next step (a single sentence grounded in the highest-priority lever-loop stage/module).

## Legacy Lever-Loop Mechanism Checklist

When Phase H is absent and the postmortem relies on `lever_loop_export_run_text.txt`, the analysis must separate Databricks task success from optimizer progress:

- Extract `GSO_CONVERGENCE_V1`. If `thresholds_met=false`, do not call the run merge-ready even if the task result is `SUCCESS`.
- For each `FULL EVAL [...]` section, record the AG ID, accept/rollback decision, primary and secondary accuracy deltas, `target_fixed_qids`, `target_still_hard_qids`, and regressed QIDs.
- If a targeted AG is accepted while `target_fixed_qids` is empty and thresholds are still unmet, classify this as a `MERGE_GATE_GAP` in the full-eval acceptance/control-plane gate.
- If a root cause has a narrow SQL-shape/filter signature (for example `LIMIT 10 vs RANK()`, `PAYMENT_CURRENCY_CD` unrequested filter) but the selected patches are broad join specs or broad metadata updates, call out the proposal-survival/patch-selection stage as secondary.
- If coverage-gap AGs are used with `rca_cards_present=false` or repeated `rca_ungrounded` drops, call out the RCA/diagnostic-AG handoff. Missing RCA cards should force RCA regeneration or human review, not broad patch application.
- If a SQL-shape root cause (`missing_filter`, `plural_top_n_collapse`, `top_n_cardinality_collapse`, `wrong_aggregation`, etc.) produces only instruction/metadata patches, call out `proposal_generation` or `question_shape_lever_preference` and recommend a structural `example_sql`/SQL-expression path.
- Distinguish `structural_gate_dropped_instruction_only` from `proposal_generation_empty`. A gate drop means a proposal existed and was rejected; `Proposals (0 total)` with no drop reason means the proposer returned empty and needs RCA/proposer-context repair.
- Check patch IDs for collisions across levers inside the same AG. If `P001#2` or another bare proposal ID represents multiple patches, recommend using `expanded_patch_id` or `(lever, proposal_id)` in signatures, survival tables, decision records, and postmortem parsing.
- Cross-check failed-question lists against `out_of_target_regressed_qids`, `soft_to_hard_regressed_qids`, and `passing_to_hard_regressed_qids`. If a newly hard QID is missing from all regression buckets, report a regression-accounting defect.
- Treat journey-validation violations as evidence-quality and gate-quality defects. They do not by themselves prove optimization failure, but they weaken attribution and should be included when recommending codebase work.

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
| Multiple `lever_loop` task attempts exist | Analyze the latest attempt by task timing/order. If `evidence_bundle` anchored to an older attempt, export the latest attempt manually and record the mismatch as a tooling finding. |
| Lever-loop stdout file `lever_loop_stdout.txt` is empty or only contains final JSON even though the task printed rich logs | Lever-loop is a notebook task in production; `databricks jobs get-run-output` often returns only `notebook_output.result`. Use `databricks jobs export-run <lever_loop_task_run_id> --views-to-export ALL` and decode `__DATABRICKS_NOTEBOOK_MODEL` before asking the operator to paste logs. |
| `lever_loop_stdout_parser.parse_lever_loop_stdout(...)` returns `optimization_run_summary=None` | Either the recovered text is truncated or the harness emitted a different stdout format. Compare the text against `tests/unit/fixtures/lever_loop_stdout_0ade1a99.txt`; if shapes diverge, file a follow-up to extend the parser regexes. |
| `GSO_BUNDLE_ASSEMBLY_FAILED_V1` marker present | The C18 block raised inside the harness. Read `error_type`/`error_message` from the marker; if it's an MLflow connection error, retry the bundle pull; if it's a Python error, file an issue and fall back to legacy `phase_a`/`phase_b` artifacts for the analysis. |
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


### Phase H verification

The contract documented in this section is enforced by
`tests/integration/test_phase_h_skills_retrieval_smoke.py`. If the
test fails, this skill's Phase H workflow is out of sync with the
bundle layout. Update either the skill or the bundle assembler so
they agree, then update the test fixture.

Concretely:

- Step 3 (read manifest) must remain reachable as
  `gso_postmortem_bundle/manifest.json` per
  `run_output_contract.bundle_artifact_paths`.
- Step 4 (per-stage capture) must remain reachable as
  `gso_postmortem_bundle/iterations/iter_NN/stages/<NN>_<key>/{input,output,decisions}.json`
  for every executable stage in `PROCESS_STAGE_ORDER` (today: 9 of 11).
- Step 5 (transcript mirrors stage dirs) is a structural invariant
  on `PROCESS_STAGE_ORDER` order; do not reorder without updating
  the transcript renderer at the same time.
