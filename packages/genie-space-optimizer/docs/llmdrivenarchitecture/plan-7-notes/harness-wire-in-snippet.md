# Plan 7 Harness Wire-In Snippet

This snippet shows the harness call-site changes required to invoke
Plan 7's helper. It is deliberately NOT applied as part of Plan 7's
landing commits — Plan 8 cleanup will land it once telemetry on
hypothesis quality is collected.

Place this block in `harness.py` immediately AFTER the line that
receives `ag_outcome` from `stages.acceptance.decide(...)` and
BEFORE the line that builds `_LearningInputLegacy` for
`stages.learning.update(...)`.

```python
from genie_space_optimizer.optimization.rollback_learning import (
    apply_forbidden_signatures_to_rollback_fingerprints,
    hypothesize_next_attempts_for_iteration,
    stamp_hypotheses_on_metadata_snapshot,
)

hypotheses_by_cluster = hypothesize_next_attempts_for_iteration(
    ctx=stage_context,
    ag_outcome=ag_outcome,
    repair_intents_by_id=proposal_slate.repair_intents_by_id,
    per_qid_evidence_by_cluster=per_qid_evidence_by_cluster,
    critique_verdicts_by_proposal_id=(
        critique_outcome.verdict_by_proposal_id
        if critique_outcome is not None else {}
    ),
    pre_rows=pre_rows,
    post_rows=post_rows,
    applied_patch_fingerprints_by_ag=applied_patch_fingerprints_by_ag,
    identifier_allowlist_by_ag=identifier_allowlist_by_ag,
    cluster_id_by_intent_id=cluster_id_by_intent_id,
)
stamp_hypotheses_on_metadata_snapshot(metadata_snapshot, hypotheses_by_cluster)
rolled_back_content_fingerprints = (
    apply_forbidden_signatures_to_rollback_fingerprints(
        prior_set=rolled_back_content_fingerprints,
        hypotheses_by_cluster_id=hypotheses_by_cluster,
    )
)
```

The four inputs that need explicit harness-side construction
(`per_qid_evidence_by_cluster`, `applied_patch_fingerprints_by_ag`,
`identifier_allowlist_by_ag`, `cluster_id_by_intent_id`) come from
existing harness state:

- `per_qid_evidence_by_cluster`: the typed sidecar Plan 3 already
  builds — accessible via `rca_evidence_typed_by_cluster` on the
  iteration's RcaEvidenceBundle equivalent (Plan 3 Task 13/14
  threading).
- `applied_patch_fingerprints_by_ag`: union of
  `AgOutcomeRecord.content_fingerprints` for each AG that produced
  any applied patch this iteration.
- `identifier_allowlist_by_ag`: built per AG from the cluster's
  `target_qids` → blame_set normalization. Plan 5's
  `repair_intent_synthesizer` already builds this — surface it as a
  separate dict.
- `cluster_id_by_intent_id`: built from
  `proposal_slate.repair_intents_by_id` by reading
  `RepairIntent.cluster_id` on each entry.

## Flag rollout

The wire-in is gated behind `GSO_PLAN7_ROLLBACK_LEARNING` (default
`false`). Operators should:

1. Land this Plan-7 PR with the helper unwired and the flag off.
2. Manually invoke the helper in a postmortem replay environment to
   collect a few iterations of hypotheses.
3. Review the hypothesis quality (postmortem reads the
   `NEXT_ATTEMPT_HYPOTHESIZED` decision records).
4. Land the harness wire-in in a follow-up PR.
5. Flip `GSO_PLAN7_ROLLBACK_LEARNING=true` once the wire-in is in
   production.
