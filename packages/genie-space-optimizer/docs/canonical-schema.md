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
