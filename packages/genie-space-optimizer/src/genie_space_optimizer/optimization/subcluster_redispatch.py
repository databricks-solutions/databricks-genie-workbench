"""Trial 23 W6 — real partitioned re-dispatch of oversized Stage 3
RCA-subcluster requests.

Trial 22 W7 (``stage3_prompt_sizer.partition_rca_subcluster_by_token_budget``
+ the marker in ``stages/synthesize.py``) computed a token-budget
partition for an oversized RCA-subcluster Stage 3 request, emitted
``GSO_TRIAL22_STAGE3_SUBCLUSTER_SPLIT_V1``, and then issued ONE call
with the *un-sliced* request — which the LLM declined as
``prompt_too_large`` (16 declines/run in the d139 postmortem). The
partition was observe-only.

W6 closes the loop: it issues N smaller Stage 3 calls (one per QID
partition) and merges their proposals into a single response so the
rest of the synthesizer is unchanged. The corrective mechanism family
is actually synthesized instead of declined.

This module is the *pure* merge/slice + marker brain. The synthesizer
owns the flag gate, the per-partition ``_build_request`` calls, and the
``LlmReasoningCall().invoke`` fan-out.
"""
from __future__ import annotations

import json
from collections.abc import Iterable, Sequence

from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)


def slice_member_evidence(
    member_qid_evidence: Iterable[dict] | None,
    partition_qids: Sequence[str],
) -> list[dict]:
    """Return the member-evidence entries whose ``qid`` is in the
    partition, preserving arrival order.

    Empty inputs (no evidence, or an empty partition) return ``[]`` so
    the per-partition request is built with only its own QIDs' cards —
    the whole point of the slice.
    """
    qset = {str(q) for q in (partition_qids or ())}
    if not qset:
        return []
    out: list[dict] = []
    for ev in member_qid_evidence or []:
        if isinstance(ev, dict) and str(ev.get("qid", "")) in qset:
            out.append(ev)
    return out


def merge_subcluster_responses(
    responses: Sequence[LlmReasoningResponse],
    *,
    call_id: str,
    skill_id: str,
) -> LlmReasoningResponse:
    """Merge N per-partition Stage 3 responses into one.

    * Proposals from every succeeded response are concatenated in
      partition order (deterministic — the caller iterates the
      deterministic partition).
    * Token counts and durations are summed across all responses (real
      cost of the fan-out).
    * The merged response succeeds iff at least one partition
      succeeded. When all partitions declined, the first decline is
      carried so the downstream classifier sees a real abstain. When
      there are no responses at all, an error-state response is
      returned.
    """
    merged_proposals: list[dict] = []
    tokens_in = 0
    tokens_out = 0
    duration = 0
    succeeded_any = False
    first_decline = None
    for r in responses or ():
        tokens_in += int(getattr(r, "tokens_input", 0) or 0)
        tokens_out += int(getattr(r, "tokens_output", 0) or 0)
        duration += int(getattr(r, "duration_ms", 0) or 0)
        if getattr(r, "succeeded", False) and getattr(r, "parsed_output", None):
            succeeded_any = True
            for p in (r.parsed_output.get("proposals") or []):
                merged_proposals.append(p)
        elif getattr(r, "declined", None) is not None and first_decline is None:
            first_decline = r.declined

    if succeeded_any:
        return LlmReasoningResponse(
            call_id=str(call_id),
            skill_id=str(skill_id),
            succeeded=True,
            parsed_output={"proposals": merged_proposals},
            declined=None,
            raw_text="",
            tokens_input=tokens_in,
            tokens_output=tokens_out,
            duration_ms=duration,
            error=None,
        )
    return LlmReasoningResponse(
        call_id=str(call_id),
        skill_id=str(skill_id),
        succeeded=False,
        parsed_output=None,
        declined=first_decline,
        raw_text="",
        tokens_input=tokens_in,
        tokens_output=tokens_out,
        duration_ms=duration,
        error=(None if first_decline is not None else "subcluster_redispatch_no_responses"),
    )


def subcluster_real_slice_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    cluster_id: str,
    batch_count: int,
    batch_sizes: Sequence[int],
    proposals_merged: int,
) -> str:
    """Build the ``GSO_TRIAL23_SUBCLUSTER_REAL_SLICE_V1`` marker line.

    Distinct from the Trial 22 ``..._SUBCLUSTER_SPLIT_V1`` marker (which
    recorded the *planned* partition): this one records that N real LLM
    calls were dispatched and merged, so postmortems can prove the
    split actually executed (``done == live``).
    """
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "cluster_id": str(cluster_id),
        "batch_count": int(batch_count),
        "batch_sizes": [int(s) for s in batch_sizes],
        "proposals_merged": int(proposals_merged),
    }
    return (
        "GSO_TRIAL23_SUBCLUSTER_REAL_SLICE_V1 "
        + json.dumps(payload, sort_keys=True)
    )


def trial27_w6_extended_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    cluster_id: str,
    member_qids_count: int,
    partition_count: int,
    partition_sizes: Sequence[int],
) -> str:
    """Build the ``GSO_TRIAL27_W6_EXTENDED_V1`` marker line.

    Emitted only when the Trial 23 W6 partitioned re-dispatch fires on
    a NON-subcluster cluster (i.e., the W27.1 extension engaged). The
    existing ``GSO_TRIAL23_SUBCLUSTER_REAL_SLICE_V1`` marker still
    records the dispatch facts on the same call site; this marker
    isolates the W27.1-attributable population so postmortems can
    measure the extension's impact and rollback efficacy.
    """
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "cluster_id": str(cluster_id),
        "member_qids_count": int(member_qids_count),
        "partition_count": int(partition_count),
        "partition_sizes": [int(s) for s in partition_sizes],
    }
    return (
        "GSO_TRIAL27_W6_EXTENDED_V1 "
        + json.dumps(payload, sort_keys=True)
    )


__all__ = [
    "slice_member_evidence",
    "merge_subcluster_responses",
    "subcluster_real_slice_marker",
    "trial27_w6_extended_marker",
]
