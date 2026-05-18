# Stage-Handoff Contract for the Optimizer Critical Path

> **Status:** active as of 2026-05-17 (Phase 1+2 of the
> `2026-05-17-failure-record-typed-contract.md` plan family).
> Updates required for any PR that adds or changes a stage on
> the critical synthesis path.

## Purpose

This document is the canonical reference for what every stage on
the optimizer's critical path must propagate to the next stage.
It exists because two live runs (airline `59a173d3`, 7now
`ab65fefe`) showed seven different boundaries where causal
context was silently lost across stage boundaries, producing
empty `skipped_reason`, `attempted_archetypes=[]`, and unmatched
forbidden-set keys.

The optimizer is large; this document covers only the four
stages where typed `FailureCluster` flows. Other boundaries
(strategist input, RCA card construction, full-eval acceptance,
finalize) still use dict handoffs and are out of scope.

## Critical path

```
              ┌──────────────────────────────────────────────────┐
              │ harness collision guard (line ~22283)            │
              │ Input:  ag (dict) + source_cluster (dict)        │
              │ Output: FailureCluster + _CollisionKeyPair       │
              │ Refuses: identity mismatch raises                │
              └──────────────────────────────────────────────────┘
                                  ↓
              ┌──────────────────────────────────────────────────┐
              │ forced_synthesis_dispatch.dispatch_forced_*      │
              │ Input:  ag (dict) + cluster (dict)               │
              │ Output: ForcedSynthesisDispatchResult with       │
              │         emitted_decision_records                 │
              │ Builds: FailureCluster.from_legacy(cluster, ag)  │
              │ Refuses: ungrounded cluster (no RCA card) before │
              │          invoking synthesizer; emits             │
              │          missing_rca_card                        │
              └──────────────────────────────────────────────────┘
                                  ↓
              ┌──────────────────────────────────────────────────┐
              │ cluster_driven_synthesis.run_cluster_driven_*    │
              │ Input:  FailureCluster | dict                    │
              │ Output: ClusterSynthesisResult                   │
              │ Invariant: proposal=None implies                 │
              │            skipped_reason != ""                  │
              └──────────────────────────────────────────────────┘
                                  ↓
              ┌──────────────────────────────────────────────────┐
              │ no_structural_candidate_record + _marker         │
              │ Input:  ClusterSynthesisResult fields            │
              │ Output: DecisionRecord + stdout marker line      │
              │ Refuses: both skipped_reason and                 │
              │          attempted_archetypes empty              │
              └──────────────────────────────────────────────────┘
```

## Stage-by-stage required propagation

### Boundary 1: harness collision guard → FailureCluster

Required fields the typed `FailureCluster` must carry:

| Field | Source | Used by |
|---|---|---|
| `cluster_id` | `cluster.cluster_id` | downstream identity |
| `target_qids` | `ag.affected_questions` (reconciled with `cluster.question_ids`) | retired-signature axis |
| `root_cause` | `cluster.root_cause` | leakage check root-cause axis |
| `asi_failure_type` | `cluster.asi_failure_type` | archetype derivation |
| `failure_keys` | `cluster.failure_keys` or derived | archetype derivation |
| `blame_set_raw` | `cluster.asi_blame_set` | resolver input |
| `blame_set_normalized` | `cluster.asi_blame_set_normalized` | resolver output |
| `rca_card_id` | `cluster.rca_card.id` | grounding gate |
| `is_grounded` | `bool(cluster.rca_card)` | pre-flight refusal |

Construction-time invariant: `cluster.question_ids ≡ ag.affected_questions` (set equality). Mismatch raises `FailureClusterIdentityError`.

### Boundary 2: dispatcher → synthesizer

Pass the `FailureCluster` as the first positional argument to `synthesize(...)`. The synthesizer auto-wraps if a dict is passed (Phase 1.2 migration shim).

### Boundary 3: synthesizer → ClusterSynthesisResult

Invariant: `proposal=None` implies `skipped_reason != ""` (or `attempted_archetypes != ()`). The documented `skipped_reason` taxonomy values are:

| Value | Meaning |
|---|---|
| `safety_cap:{n}>={cap}` | `example_question_sqls` already at cap |
| `budget:{n}>={cap}` | per-iteration synthesis budget exhausted |
| `format_afs_failed` | `format_afs(cluster)` raised |
| `validate_afs_rejected` | `validate_afs(afs)` raised |
| `no_top_n_archetype` | derived slice None on `plural_top_n_collapse` cluster |
| `no_archetype_or_slice` | derived slice None on non-top-N cluster |
| `synth_none` | LLM returned a non-parseable proposal |
| `gate:{first_fail.gate}:{first_fail.reason}` | proposal validated but rejected at gate |
| `missing_space_id` | snapshot missing space identifier |
| `genie_agreement:{reason}` | arbiter rejected the proposal |
| `missing_rca_card` | pre-flight refusal (Phase 0.3) |

### Boundary 4: dispatcher → decision_emitters / run_analysis_contract

`no_structural_candidate_record(skipped_reason=..., attempted_archetypes=...)` and `no_structural_candidate_marker(skipped_reason=..., attempted_archetypes=...)` raise `ValueError` when both are empty (Phase 1.5). The synthesizer always knows something; double-empty is upstream context loss.

## Update protocol

Any PR that adds or changes a stage on the critical path must:

1. Update this document to reflect the new propagation requirements.
2. Add or update a `FailureCluster` field if a new identifier alias is introduced.
3. Add or update a `skipped_reason` taxonomy value if the synthesizer learns a new decline mode.
4. Add a live-shape fixture if a new failure shape is observed in a real run.

Mechanical enforcement: a pre-commit hook (Task 9) refuses commits that modify the Phase 1 critical-path files without also modifying this document.

## Out of scope (deliberately)

The following boundaries are dict-based and not covered:

- Strategist input dicts (`optimizer._call_llm_for_adaptive_strategy`)
- RCA card construction (`rca_card_builder`)
- Full-eval acceptance (`acceptance_outcome`, `acceptance_policy`)
- Finalize / deploy (`finalize_task`, `deploy_task`)

Future plans may extend the typed contract to these boundaries; doing so requires demonstrating concrete identity-mismatch defects with live evidence (the bar Phase 0+1 cleared).
