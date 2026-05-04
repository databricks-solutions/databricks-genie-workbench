# Genie Space Optimizer Canonical Schema

This document is the rejection criterion for optimizer schema drift. New PRs
that introduce another alias for an existing concept must update this document
and delete an older alias in the same PR.

For the first 30 days after this document lands, treat it as frozen except for
typo fixes and corrections that remove ambiguity. After 30 days, incremental
edits are allowed only when each edit deletes or deprecates an existing alias.

## Canonical Vocabulary

| Slot | Canonical Name | Definition | Deprecated Aliases | Owner |
| --- | --- | --- | --- | --- |
| Raw judge metadata label | `asi_failure_type` | The failure label emitted by a judge's ASI metadata before optimizer normalization. | `failure_type` when used outside raw judge metadata | Judge/ASI extraction |
| Optimizer RCA class | `root_cause` | The normalized optimizer diagnosis used for clustering, ranking, and lever mapping. | `RCA defect`, `dominant defect`, `failure root` | `optimization/optimizer.py::cluster_failures` |
| Deterministic SQL diff enum | `diff_kind` | The AST-derived SQL difference enum from `feature_mining.DiffKind`. | `primary_kind` when referring to the enum type itself | `optimization/feature_mining.py` |
| Primary SQL diff value | `primary_diff_kind` | The single highest-priority `DiffKind` value surfaced to prompts and AFS. | `primary_kind` in prompt context, `structural_diff.primary_kind` | `optimization/feature_mining.py::compute_diff` |
| Prompt-safe failure projection | `failure_features` | Leak-safe, typed summary of deterministic SQL features and diff kinds. It may contain enum names and allowlisted identifiers, never raw benchmark SQL. | `_feature_diff` in prompt-facing data, raw `SqlDiff` object in AFS | `optimization/optimizer.py::_summarize_feature_diffs` |
| Cluster identity | `cluster_id` | Stable per-iteration cluster identifier such as `H001` or `S002`. | `signature` when used as the public ID | `optimization/optimizer.py::cluster_failures` |
| Action group source | `source_cluster_ids` | Cluster IDs the strategist claims the action group addresses. | `target_clusters`, `source_ids` | Strategist output schema |
| Action group questions | `affected_questions` | Benchmark question IDs the action group claims it may affect. | `target_qids` in strategist JSON | Strategist output schema |
| Patch causal targets | `target_qids` | Benchmark question IDs attached to a proposal or patch for gating and patch cap selection. | `_grounding_target_qids` outside grounding internals | Proposal grounding / patch selection |
| RCA theme ID | `rca_id` | Identifier for a typed RCA theme or execution plan that produced a proposal. | `theme_id` when used for patch provenance | RCA ledger / synthesis |
| Regression debt | `regression_debt_qids` | Bounded out-of-target hard regressions accepted because the action group produced a net causal win. | `collateral_qids`, `new regressions` when accepted | `optimization/control_plane.py::decide_control_plane_acceptance` |
| Rollback trust | `rollback_state_trusted` | Boolean control-plane state indicating whether the live space matches the pre-AG snapshot after rollback. | `rollback_verified` when used as a loop-wide learning flag | `optimization/harness.py` |
| Optimizer trace container | `OptimizationTrace` | Canonical in-memory container for journey events, decision records, validation reports, and operator transcript projections. | `decision log`, `trace rows` when used as a schema name | `optimization/rca_decision_trace.py` |
| Optimizer decision row | `DecisionRecord` | Canonical record for a Lever Loop choice with evidence refs, RCA, root cause, causal target qids, expected effect, observed effect, regression qids, reason code, and next action. | `decision audit row` outside Delta persistence, `gate row` outside gate-specific adapters | `optimization/rca_decision_trace.py` |
| Operator transcript | `operator_transcript` | Deterministic pretty stdout projection rendered from `OptimizationTrace`. | ad hoc print sections, scoreboard prose | `optimization/rca_decision_trace.py` |
| Phase B observability manifest | `loop_out["phase_b"]` | CLI-truth Phase B summary on the lever-loop notebook exit JSON (`contract_version`, `decision_records_total`, `iter_record_counts`, `iter_violation_counts`, `no_records_iterations`, `artifact_paths`, `producer_exceptions`, `target_qids_missing_count`, `total_violations`). The postmortem analyzer reads this from `databricks jobs get-run-output` because that CLI exposes only `dbutils.notebook.exit(...)` for the lever_loop task. | grep stdout for transcript text outside operator review | `optimization/harness.py` (built) + `jobs/run_lever_loop.py:548-563` (allowlisted into exit JSON) |

## Data Flow As Types

```text
EvalRow
  -> ASI[judge]
  -> SqlFeatures
  -> SqlDiff
  -> FailureEntry
  -> Cluster
  -> AFS
  -> ActionGroup
  -> Proposal
  -> Patch
  -> Outcome
```

| Arrow | Producer Function | Contract Test |
| --- | --- | --- |
| `EvalRow -> ASI[judge]` | `optimization/rca_failure_context.py::failure_contexts_by_qid` and ASI extraction inside `optimization/optimizer.py::analyze_failures` | `tests/unit/test_rca_failure_context.py` |
| `EvalRow -> SqlFeatures` | `optimization/feature_mining.py::mine_sql_features` | `tests/unit/test_ast_diff_threading.py` |
| `SqlFeatures -> SqlDiff` | `optimization/feature_mining.py::compute_diff` | `tests/unit/test_ast_diff_threading.py` |
| `ASI + SqlDiff -> FailureEntry` | `optimization/optimizer.py::analyze_failures` | `tests/unit/test_unified_rca_control_plane.py` |
| `FailureEntry -> Cluster` | `optimization/optimizer.py::cluster_failures` | `tests/unit/test_ast_diff_threading.py` |
| `Cluster -> AFS` | `optimization/afs.py::format_afs` | `tests/unit/test_ast_diff_threading.py` |
| `AFS -> ActionGroup` | `optimization/optimizer.py::_call_llm_for_adaptive_strategy` | `tests/unit/test_unified_rca_prompt_alignment.py` |
| `ActionGroup -> Proposal` | proposal generation paths in `optimization/harness.py`, `optimization/cluster_driven_synthesis.py`, and `optimization/synthesis.py` | `tests/unit/test_patch_causal_backfill.py` and `tests/unit/test_cluster_driven_synthesis.py` |
| `Proposal -> Patch` | `optimization/applier.py::normalize_patch` and `optimization/proposal_grounding.py` gates | `tests/unit/test_applier_proposal_metadata.py` and `tests/unit/test_proposal_grounding.py` |
| `Patch -> Outcome` | `optimization/harness.py::_run_gate_checks` and `optimization/control_plane.py::decide_control_plane_acceptance` | `tests/unit/test_static_judge_replay.py` and `tests/unit/test_control_plane.py` |

## Determinism Declaration

| Stage | Mechanism | Function Or LLM Justification |
| --- | --- | --- |
| Row classification | deterministic | `optimization/control_plane.py::row_status`, `hard_failure_qids`, and `is_actionable_soft_signal_row` classify rows from stored verdict fields. |
| ASI extraction | LLM | Judge ASI is produced by LLM-based judges; this is unavoidable because judge rationales and counterfactual fixes are semantic evaluations. |
| `SqlFeatures` / `SqlDiff` | deterministic | `optimization/feature_mining.py::mine_sql_features` and `compute_diff` use SQL parsing and structured comparison. |
| `DiffKind` classification | deterministic | `optimization/feature_mining.py::compute_diff` dispatches to fixed enum values. |
| Cluster formation | deterministic | `optimization/optimizer.py::cluster_failures` groups by normalized `root_cause` and blame. |
| Ranking | deterministic | `optimization/optimizer.py::rank_clusters` uses score formula and deterministic tiebreakers. |
| Strategist proposal | LLM | The strategist selects patch strategy and wording across competing RCA clusters; an LLM is used because cross-cluster conflict resolution and instruction wording are semantic planning tasks. |
| Synthesis: Lever 6 SQL example | LLM | SQL expression/example synthesis fills schema-bounded templates where wording and SQL shape depend on natural-language RCA context. |
| Apply, gates, acceptance | deterministic | `optimization/applier.py`, `optimization/proposal_grounding.py`, `optimization/patch_selection.py`, and `optimization/control_plane.py::decide_control_plane_acceptance` make fixed policy decisions from proposals, patches, and eval rows. |

## How To Use This Document

Use this document as a schema contract, not as an implementation plan.

- New PRs that introduce a new name for an existing slot must be rejected unless they update this document and remove or deprecate an older alias.
- New implementation plans must point at one row in the vocabulary table or one arrow in the data-flow table and state which contract they refine.
- Prompt-facing data must use canonical names unless a deprecated alias is required for backward compatibility at a boundary.
- Backward-compatible aliases are allowed only at parse boundaries and must be normalized before cluster formation, patch selection, or control-plane acceptance.

## Thirty-Day Freeze

The initial version should be reviewed by 2-3 engineers who have worked on the loop. After approval, freeze the document for 30 days. During the freeze, edits are limited to correctness fixes that reduce ambiguity. After the freeze, each schema edit must delete, rename, or explicitly deprecate at least one existing alias.

## GSO Run Output Contract (Phase H)

Phase H attaches a single MLflow artifact tree — `gso_postmortem_bundle/` — to the parent lever-loop run. It is the canonical input for every postmortem skill, CLI tool, and integration test that needs to inspect a completed run. Implementation lives in `optimization/run_output_contract.py`, `optimization/stage_io_capture.py`, `optimization/run_output_bundle.py`, `optimization/operator_process_transcript.py`, and the `tools/marker_parser.py` + `tools/evidence_layout.py` + `tools/mlflow_audit.py` triplet that consumes it.

### Bundle directory tree

```
gso_postmortem_bundle/
├── manifest.json
├── run_summary.json
├── artifact_index.json
├── operator_transcript.md
├── decision_trace_all.json
├── journey_validation_all.json
├── replay_fixture.json
├── scoreboard.json
├── failure_buckets.json
└── iterations/
    ├── iter_01/
    │   ├── summary.json
    │   ├── operator_transcript.md
    │   ├── decision_trace.json
    │   ├── journey_validation.json
    │   ├── rca_ledger.json
    │   ├── proposal_inventory.json
    │   ├── patch_survival.json
    │   └── stages/
    │       ├── 01_evaluation_state/{input,output,decisions}.json
    │       ├── 02_rca_evidence/{input,output,decisions}.json
    │       ├── 03_cluster_formation/{input,output,decisions}.json
    │       ├── 04_action_group_selection/{input,output,decisions}.json
    │       ├── 05_proposal_generation/{input,output,decisions}.json
    │       ├── 06_safety_gates/{input,output,decisions}.json
    │       ├── 07_applied_patches/{input,output,decisions}.json
    │       ├── 09_acceptance_decision/{input,output,decisions}.json
    │       └── 10_learning_next_action/{input,output,decisions}.json
    └── iter_02/...
```

The `<NN>_<stage_key>` directory name comes from the stage's position in `PROCESS_STAGE_ORDER`. Position 8 (`post_patch_evaluation`) and position 11 (`contract_health`) are transcript-only — they appear in the operator transcript but not under `iterations/iter_NN/stages/`. A `ls` of the iteration's `stages/` directory is naturally process-ordered.

### `manifest.json`

```json
{
  "schema_version": "v1",
  "optimization_run_id": "opt-abc-123",
  "databricks_job_id": "j1",
  "databricks_parent_run_id": "r1",
  "lever_loop_task_run_id": "t1",
  "iteration_count": 3,
  "iterations": [1, 2, 3],
  "missing_pieces": [],
  "stage_keys_in_process_order": [
    "evaluation_state", "rca_evidence", "cluster_formation",
    "action_group_selection", "proposal_generation", "safety_gates",
    "applied_patches", "acceptance_decision", "learning_next_action"
  ]
}
```

Built by `optimization/run_output_bundle.build_manifest`. `missing_pieces` records any per-stage capture failure so a postmortem can distinguish "stage didn't run" from "stage ran but capture failed."

### `artifact_index.json`

A flat path map for postmortem skills. Top-level keys mirror the bundle tree; per-iteration keys carry per-stage paths so the skill can read every stage's I/O without walking directories. Built by `optimization/run_output_bundle.build_artifact_index`.

### `run_summary.json`

```json
{
  "schema_version": "v1",
  "baseline": {"overall_accuracy": 0.875, ...},
  "terminal_state": {"status": "convergence", "should_continue": false},
  "iteration_count": 5,
  "accuracy_delta_pp": 4.2
}
```

Built by `optimization/run_output_bundle.build_run_summary`.

### Per-stage `iter_NN/stages/<NN>_<stage_key>/{input,output,decisions}.json`

`stage_io_capture.wrap_with_io_capture(execute, stage_key)` writes:

- `input.json`: `dataclasses.asdict(stage_input)` serialized via `json.dumps`. Set fields are normalized to sorted lists for deterministic output.
- `output.json`: same shape, for the stage output.
- `decisions.json`: list of every `DecisionRecord` (or other emitted record) the stage emitted via `ctx.decision_emit` during the call.

The decorator NEVER raises. MLflow log_text failures are caught and warned — diagnostic capture must never break the optimizer.

### `GSO_ARTIFACT_INDEX_V1` marker

Single-line stdout marker emitted by `optimization/run_analysis_contract.artifact_index_marker(...)` and parsed by `tools/marker_parser.parse_markers` into `MarkerLog.artifact_index`:

```
GSO_ARTIFACT_INDEX_V1 {"artifact_index_path":"gso_postmortem_bundle/artifact_index.json","iterations":[1,2],"optimization_run_id":"opt-1","parent_bundle_run_id":"br-1"}
```

The marker carries `parent_bundle_run_id` so the gso-postmortem skill can locate the bundle in MLflow even when stdout is truncated.

### Run-role tags on the parent MLflow run

Built by `common/mlflow_names.lever_loop_parent_run_tags(...)`:

| Tag | Value |
| --- | --- |
| `genie.run_role` | `lever_loop` |
| `genie.optimization_run_id` | `<optimization_run_id>` |
| `genie.databricks.job_id` | `<job_id>` |
| `genie.databricks.parent_run_id` | `<parent_run_id>` |
| `genie.databricks.lever_loop_task_run_id` | `<task_run_id>` |

`tools/mlflow_audit.audit_parent_bundle(...)` discovers the parent run by `genie.run_role=lever_loop` + `genie.optimization_run_id`, with a fallback to the legacy `genie.run_id` tag.

### Reconciliation with the stage registry

`PROCESS_STAGE_ORDER` (11 entries: 9 executable stages + Stage 1/8 split (`post_patch_evaluation`) + `contract_health` meta) is the human-readable transcript ordering. `STAGES` (9 entries) in `optimization/stages/_registry.py` is the executable iteration target. The reconciliation rule is locked by `tests/unit/test_process_stage_order_matches_stages_registry.py`: every `STAGES.stage_key` must appear in `PROCESS_STAGE_ORDER` in the same relative order. Transcript-only keys (`post_patch_evaluation`, `contract_health`) must be explicitly listed.
