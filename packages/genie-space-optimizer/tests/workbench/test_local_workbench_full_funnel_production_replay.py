"""Full-funnel workbench rehearsal against the production-replay corpus.

This is the gate the prior workbench plan strict-xfail'd: with the
Trial 13 typed-evidence cutover in place (probe + SM lane both thread
``rca_evidence_typed``) and the tape harness routing by ``qid``, every
hard QID in the committed production-replay corpus must reach
``APPLIED`` in ``sm-tape`` mode and produce one recorded patch.

If a future change regresses any of:

* The typed-evidence drop at Stage 1
  (``diagnose_llm._invoke_stage1_llm`` not threading
  ``ctx.rca_evidence_typed``),
* The workbench bundle not surfacing typed evidence via
  ``metadata_snapshot["_rca_evidence_typed"]`` /
  ``ctx.rca_evidence_typed``,
* The tape harness mis-routing per-QID responses when QIDs drift from
  arrival order,

then this test fires before any deploy. It is intentionally a hard
assertion (not xfail) — both findings 2 and 3 from the workbench v0.1
run are fixed in the same change as this test lands.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pytest

from local_lever_workbench.input_bundle import from_production_replay
from local_lever_workbench.local_runner import (
    LLM_MODE_TAPE,
    run_workbench_iteration,
    summarize_stage_progress,
)
from local_lever_workbench.models import WorkbenchRunConfig
from local_lever_workbench.stage1_probe import probe_bundle


def _serialize_tape(entries: Iterable, path: Path) -> Path:
    """Serialize TapeEntry objects to JSONL, preserving the new ``qid`` field.

    The runtime ``TapeReplayHarness.load_tape`` consumes this format
    via ``TapeEntry.from_json``; ``qid`` is what flips the harness into
    its QID-keyed routing mode for this corpus.
    """
    path.write_text(
        "\n".join(
            json.dumps(
                {
                    "kind": e.kind,
                    "skill_id": e.skill_id,
                    "call_id": e.call_id,
                    "iteration": e.iteration,
                    "qid": e.qid,
                    "parsed_output": e.parsed_output,
                    "raw_text": e.raw_text,
                    "tokens_input": e.tokens_input,
                    "tokens_output": e.tokens_output,
                    "duration_ms": e.duration_ms,
                    "exception_class": e.exception_class,
                    "exception_message": e.exception_message,
                }
            )
            for e in entries
        )
    )
    return path


@pytest.mark.workbench
@pytest.mark.integration
def test_full_production_replay_corpus_reaches_applied(tmp_path: Path) -> None:
    """End-to-end workbench rehearsal: all committed QIDs reach APPLIED."""
    from tests.integration.sm_forward_tapes import (
        cluster_response_tape,
        diagnose_response_tape,
        synthesize_response_tape,
    )

    bundle = from_production_replay()
    qids = bundle.hard_qids
    assert qids, "production-replay corpus must commit at least one QID"

    # Stage 1 must already be green for the corpus before we attempt
    # the full funnel — otherwise an APPLIED-failure could be mistaken
    # for a tape problem when it is actually the typed-evidence drop.
    stage1 = probe_bundle(bundle)
    assert stage1.all_pass, (
        f"Stage 1 probe failed for production-replay corpus "
        f"before the full-funnel run; check the typed-evidence drop "
        f"regression. violations={[(f.qid, list(f.violations)) for f in stage1.findings if f.violations]!r}"
    )

    # Stock enough Stage 1/2/3 entries per QID to absorb the SM's
    # normal retry/cycle cadence (applier rejections, narrow-replacement
    # bounces, etc.) without exhausting the tape mid-iteration. Mirrors
    # the established pattern in
    # ``test_local_workbench_runner_tape_mode.py``.
    tape: list = []
    for _ in range(5):
        tape += diagnose_response_tape(qids)
    for _ in range(5):
        tape += cluster_response_tape(qids)
    for _ in range(5):
        tape += synthesize_response_tape(qids)

    tape_path = _serialize_tape(tape, tmp_path / "forward.jsonl")
    config = WorkbenchRunConfig(
        bundle_path=tmp_path / "bundle.json",
        output_dir=tmp_path / "run",
        llm_mode=LLM_MODE_TAPE,
        tape_path=tape_path,
        iteration=1,
    )
    artifacts = run_workbench_iteration(bundle, config)

    progress = summarize_stage_progress(artifacts)
    deepest_by_qid = {p.qid: p.deepest_stage for p in progress}
    applied_or_deeper = ("applied", "evaluated", "accepted")

    not_applied = {
        qid: deepest
        for qid, deepest in deepest_by_qid.items()
        if deepest not in applied_or_deeper
    }
    terminal_reasons = {
        p.qid: p.terminal_reason for p in progress if p.terminal_reason
    }
    assert not not_applied, (
        f"workbench did not reach APPLIED for: {not_applied!r}. "
        f"terminal_reasons={terminal_reasons!r}. "
        f"This is exactly the silent decline mode the workbench is "
        f"meant to surface — see the Trial 13 typed-evidence + tape "
        f"QID-key fixes for the expected wiring."
    )

    recorded = artifacts.recorder.as_tuple()
    recorded_qids = {rp.qid for rp in recorded}
    assert recorded_qids == set(qids), (
        f"recording applier captured PATCHes for {sorted(recorded_qids)!r}; "
        f"expected one per admitted QID {sorted(set(qids))!r}. "
        f"A QID reached APPLIED but the applier-gate did not record "
        f"its patch — the workbench applier wiring drifted."
    )


@pytest.mark.workbench
@pytest.mark.integration
def test_full_funnel_survives_single_qid_stage1_abstain(tmp_path: Path) -> None:
    """One QID abstaining at Stage 1 must not cascade-fail the rest.

    The fragility this pins: in production, each Stage 1 LLM call is
    per-QID with ``call_id = "plan11_stage1_diagnose.iter_N"`` — the QID
    lives only in the ``user_prompt`` body, not in the call_id. If the
    tape harness routes by ``call_id`` alone, an upstream Stage 1
    abstain (e.g. ``evidence_card_empty``) leaves that QID's tape entry
    unconsumed; every subsequent QID then receives the wrong entry and
    aborts with ``diagnose_returned_no_matching_qid:<other-qid>``. One
    real abstain silently cascades into N-1 false negatives.

    The fix is: ``TapeReplayHarness._request_mentions_qid`` peeks at
    ``user_prompt`` JSON for ``failing_qids[*].qid`` so QID-keyed
    routing works for production-shaped call_ids.

    This test reproduces the cascade by stripping ``typed_evidence``
    from a single load-bearing QID (``domain_b_gs_013`` — the row
    whose native fields cannot satisfy the Stage 1 contract without
    typed evidence) and asserts the other QIDs still advance through
    the funnel (``deepest_stage_reached`` at APPLIED or deeper).
    """
    from tests.integration.sm_forward_tapes import (
        cluster_response_tape,
        diagnose_response_tape,
        synthesize_response_tape,
    )

    bundle = from_production_replay()
    qids = bundle.hard_qids
    target_qid = "domain_b_gs_013"
    assert target_qid in qids, (
        f"corpus regression: expected {target_qid!r} in committed "
        f"production-replay corpus, got {qids!r}"
    )

    # Strip typed_evidence from the one QID whose native row fields
    # do not satisfy the Stage 1 contract.
    import dataclasses

    stripped_cases = tuple(
        dataclasses.replace(
            case,
            typed_evidence=None if case.qid == target_qid else case.typed_evidence,
        )
        for case in bundle.hard_cases
    )
    bundle = dataclasses.replace(bundle, hard_cases=stripped_cases)

    tape: list = []
    for _ in range(5):
        tape += diagnose_response_tape(qids)
    for _ in range(5):
        tape += cluster_response_tape(qids)
    for _ in range(5):
        tape += synthesize_response_tape(qids)

    tape_path = _serialize_tape(tape, tmp_path / "forward.jsonl")
    config = WorkbenchRunConfig(
        bundle_path=tmp_path / "bundle.json",
        output_dir=tmp_path / "run",
        llm_mode=LLM_MODE_TAPE,
        tape_path=tape_path,
        iteration=1,
    )
    artifacts = run_workbench_iteration(bundle, config)
    progress = summarize_stage_progress(artifacts)
    deepest_by_qid = {p.qid: p.deepest_stage for p in progress}
    terminal_by_qid = {p.qid: p.terminal_reason for p in progress}

    # The stripped QID legitimately abstains at Stage 1.
    assert deepest_by_qid[target_qid] == "hard_qid_seen", deepest_by_qid
    assert "evidence_card_empty" in terminal_by_qid[target_qid], (
        terminal_by_qid[target_qid]
    )

    # Every other QID must still advance past diagnose/propose (cascade
    # did not stall). summarize_stage_progress maps deepest_stage_reached
    # into deepest_stage; V1.5 sm-tape typically reaches evaluated then
    # terminates at acceptance_gate (target_unchanged when apply is faked).
    cascade_success_stages = ("applied", "evaluated", "accepted", "terminated")
    expected_peers = [q for q in qids if q != target_qid]
    stalled_peers = {
        q: deepest_by_qid[q]
        for q in expected_peers
        if deepest_by_qid[q] not in cascade_success_stages
    }
    assert not stalled_peers, (
        f"cascade regression: {target_qid!r} legitimately abstained but "
        f"the following QIDs were collaterally damaged: {stalled_peers!r}. "
        f"terminal_reasons={terminal_by_qid!r}. "
        f"Check TapeReplayHarness._request_mentions_qid handles "
        f"production-shaped call_ids by inspecting user_prompt."
    )

    recorded_qids = {rp.qid for rp in artifacts.recorder.as_tuple()}
    assert recorded_qids == set(expected_peers), (
        f"applier recorded {sorted(recorded_qids)!r}; expected exactly "
        f"the non-abstaining QIDs {sorted(expected_peers)!r}."
    )


@pytest.mark.workbench
def test_tape_harness_qid_routing_survives_dispatch_order_drift(
    tmp_path: Path,
) -> None:
    """Out-of-order tape entries still resolve when ``qid`` is set.

    The Trial 13 finding 4 was: when the SM dispatched QIDs in a
    different order than the tape was authored in (typical when an
    upstream QID abstains and the orchestrator moves on to the next),
    the strict arrival-order harness consumed responses meant for a
    different QID and the receiving transformer aborted with
    ``diagnose_returned_no_matching_qid``.

    This test pins the fix: with ``qid``-keyed entries, the harness
    matches each request by the QID embedded in the request's
    ``call_id`` regardless of arrival order.
    """
    from tests.integration.sm_forward_tapes import diagnose_response_tape
    from tests.integration.sm_tape_replay import (
        TapeReplayHarness,
        load_tape,
    )
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningRequest,
    )

    qids_authoring_order = ("gs_001", "gs_002", "gs_003")
    qids_sm_dispatch_order = ("gs_003", "gs_001", "gs_002")
    tape = diagnose_response_tape(qids_authoring_order)
    tape_path = _serialize_tape(tape, tmp_path / "out_of_order.jsonl")

    harness = TapeReplayHarness(tape=load_tape(tape_path))

    # The harness only cares about ``request.skill_id`` + ``request.call_id``;
    # ``result_cls`` is opaque to the tape replay path, so any ``type`` works.
    for qid in qids_sm_dispatch_order:
        req = LlmReasoningRequest(
            call_id=f"plan11_stage1_diagnose.iter_1.{qid}",
            skill_id="plan11_diagnose",
            system_msg="",
            user_prompt="",
            result_cls=dict,
            max_tokens=1024,
        )
        resp = harness._invoke(w=None, request=req)
        diagnoses = (resp.parsed_output or {}).get("diagnoses") or [{}]
        assert diagnoses[0].get("qid") == qid, (
            f"harness returned diagnosis for "
            f"{diagnoses[0].get('qid')!r} when SM asked for {qid!r}; "
            f"QID-keyed routing drifted."
        )
