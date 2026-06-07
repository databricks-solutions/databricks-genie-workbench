"""Forward-pipeline applyability boundary test.

Extends ``test_sm_forward_pipeline_to_proposed`` past the PROPOSED
gate. Covers the two deterministic outcomes of the
``NORMALIZED → APPLYABLE`` transition:

  * **Safe path**: a contract-passing proposal with no
    ``passing_dependents`` field takes the
    ``no_passing_dependents_field`` safe-by-default fallback in
    :func:`proposal_grounding.patch_blast_radius_is_safe`,
    ``blast_radius_batch`` advances ``NORMALIZED → APPLYABLE``, and
    the state reaches ``APPLYABLE`` ahead of ``applier_gate``.

  * **Unsafe path**: the same proposal shape, plus
    ``passing_dependents=[<qid outside target>]``, triggers the
    ``blast_radius_exceeds_threshold`` rejection. ``blast_radius_batch``
    cycles the state ``NORMALIZED → PROPOSED`` with a typed
    :class:`ProposalAttempt` (``outcome="blast_radius_rejected"``) and
    a ``GSO_GATE_REASONING_V1`` line that names the collateral QIDs.

These are the assertions that pin the "blast-radius / applyability /
narrowing" stage in the lever-loop architecture. They run with no
Databricks dependency — ``applier_gate``'s real Genie-API call is
allowed to fail at the boundary because the harness passes
``workspace_client=None``; what we lock in here is the funnel-depth
progress and the rejection-outcome shape that downstream postmortems
read.

Aligned with the ``fast-optimizer-testing`` plan Step 1 follow-up:
"asserted applyability test" — the first of the three steps that
extends the harness from PROPOSED to the full forward loop.
"""
from __future__ import annotations

import io
import time
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from tests.integration.sm_forward_fixtures import (
    expected_hard_qids,
    forward_metadata_snapshot,
    load_production_hydration_rows,
    parse_gate_reasoning_markers,
    parse_patch_outcome_markers,
    parse_qstate_transitions,
    states_by_qid,
)
from tests.integration.sm_forward_tapes import (
    cluster_response_tape,
    diagnose_response_tape,
    synthesize_blast_radius_unsafe_tape,
    synthesize_response_tape,
)
from tests.integration.sm_tape_replay import TapeReplayHarness


_ACCEPTANCE_CEILING_SECONDS = 5.0

# These tests exercise the blast-radius gate (safe/unsafe) and the
# normalized→applyable boundary — concerns orthogonal to kit synthesis.
# Trial 26 W26.2 added ``wrong_aggregation`` (the previous default
# diagnose RCA kind) to the KIT_FOR_RCA companion map, so a single-lever
# proposal for it is now hard-rejected as ``kit_for_rca_violation:...:
# singleton`` and never reaches APPLYABLE. To keep these tests focused on
# blast radius, the forward tape diagnoses a kit-FREE RCA kind, for which
# a single-lever proposal is legitimately admissible and reaches the
# applyability boundary. The kit-at-source path for the W26.2 kinds is
# covered separately by ``test_trial26_kit_map_coverage_replay`` (slate
# survival) and the synthesis-prompt unit test.
_KIT_FREE_RCA_KIND = "soft_policy_violation"


@pytest.mark.integration
def test_safe_blast_radius_advances_through_normalized_to_applyable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default forward tape (no ``passing_dependents`` on the patch)
    must take the blast-radius safe-by-default path and every QID must
    reach ``APPLYABLE``.

    Locks in three production-visible invariants:

      1. ``deepest_stage_reached`` is at least ``APPLYABLE`` for every
         admitted hard QID — the proof the safe path made it through
         ``blast_radius_batch`` and ``narrow_replacement_gate``.

      2. No ``gate=blast_radius_batch verdict=rejected`` marker fires —
         the false positives that haunted earlier trials are absent.

      3. At least one ``GSO_PATCH_OUTCOME_V1`` payload is emitted
         (success or applyability_rejected). Their presence is the
         canonical proof ``applier_gate`` ran, which means the state
         reached ``APPLYABLE`` and the applyability boundary itself
         was exercised — not just the funnel index.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    assert qids, "production fixture must declare hard hydration QIDs"

    # Stock enough tape for the escalation cycle: with ``workspace_client
    # = None`` the applier rejects every proposal at APPLYABLE, the
    # state cycles back to PROPOSED, and the orchestrator may re-enter
    # Stage 3 for a retry. The state machine's per-QID processing tops
    # out at ~10 attempts before the escalation ladder gives up, so
    # ten copies per stage per QID is a safe upper bound.
    cycles = 10
    tape = []
    for _ in range(cycles):
        tape += diagnose_response_tape(qids, rca_kind_label=_KIT_FREE_RCA_KIND)
    for _ in range(cycles):
        tape += cluster_response_tape(qids)
    for _ in range(cycles):
        tape += synthesize_response_tape(qids)
    harness = TapeReplayHarness(tape=tape)

    from genie_space_optimizer.optimization import optimizer as opt_mod
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
        stage_index,
    )

    buf = io.StringIO()
    t0 = time.monotonic()
    with redirect_stdout(buf), harness.patch():
        final_states = opt_mod.run_state_machine_iteration_and_persist(
            eval_rows=rows,
            iteration=1,
            run_id="applyable-safe",
            run_root=tmp_path,
            workspace_client=None,
            metadata_snapshot=forward_metadata_snapshot(rows),
            forbidden_signatures=(),
        )
    elapsed = time.monotonic() - t0
    stdout = buf.getvalue()

    assert elapsed < _ACCEPTANCE_CEILING_SECONDS, (
        f"Applyability-safe replay took {elapsed:.2f}s; ceiling is "
        f"{_ACCEPTANCE_CEILING_SECONDS}s. A slow boundary test means "
        f"developers will skip it before deploying."
    )

    # Admission and clean-termination invariants. The test asserts the
    # safe blast-radius path reaches APPLYABLE; what must NOT happen is a
    # QID dropping out via a *failure* terminal before that boundary.
    #
    # With ``workspace_client=None`` the apply step is a no-op against a
    # synthetic config: each QID reaches APPLYABLE, ``applier_gate`` runs,
    # and the state terminates cleanly as ``OPTIMIZER_STALLED_SAFE_NOOP``
    # at the boundary (reason prefixed ``applyability_rejected:``) instead
    # of cycling back to PROPOSED. The trailing no-op detail varies —
    # either ``applied:render_and_apply_succeeded`` (nothing to deploy) or
    # ``apply_failed:Validation failed…`` (the synthetic fixture config
    # omits ``data_sources``, so post-patch validation no-ops). Both are
    # acceptable clean stalls; what must NOT happen is a *failure* terminal
    # BEFORE the boundary. We therefore key on the ``applyability_rejected``
    # boundary token rather than the trailing no-op reason.
    by = states_by_qid(final_states)
    assert set(by) == set(qids), (
        f"SM admitted {sorted(by)!r}; expected {sorted(qids)!r}."
    )
    for s in final_states:
        if s.qid in set(qids) and s.terminal is not None:
            assert s.terminal.kind == "OPTIMIZER_STALLED_SAFE_NOOP" and (
                "applyability_rejected" in (s.terminal.reason or "")
            ), (
                f"qid={s.qid!r} terminated unexpectedly: "
                f"kind={s.terminal.kind!r} reason={s.terminal.reason!r}; "
                f"expected either no terminal or a clean "
                f"OPTIMIZER_STALLED_SAFE_NOOP at the applyability boundary."
            )

    # Funnel-depth invariant: every hard QID must reach APPLYABLE.
    for qid in qids:
        s = by[qid]
        actual_idx = stage_index(s.deepest_stage_reached)
        applyable_idx = stage_index(FunnelStage.APPLYABLE)
        assert actual_idx >= applyable_idx, (
            f"qid={qid!r} deepest_stage_reached="
            f"{s.deepest_stage_reached.value!r} (index {actual_idx}); "
            f"expected APPLYABLE (index {applyable_idx}) or deeper. "
            f"The safe blast-radius path did not advance the state."
        )

    # ``blast_radius_batch`` must not reject on the safe-by-default
    # path. If it does, the
    # ``no_passing_dependents_field`` fallback in
    # ``patch_blast_radius_is_safe`` is regressed.
    gate_markers = parse_gate_reasoning_markers(stdout)
    blast_rejects = [
        m for m in gate_markers
        if m["gate"] == "blast_radius_batch" and m["verdict"] == "rejected"
    ]
    assert not blast_rejects, (
        f"blast_radius_batch fired {len(blast_rejects)} rejections on "
        f"the safe path. The safe-by-default fallback "
        f"(no_passing_dependents_field) is regressed. Sample: "
        f"{blast_rejects[:2]!r}"
    )

    # ``narrow_replacement_gate`` is wired in but must no-op when no
    # blast-radius drop is registered — i.e. it must not emit a
    # rejection marker on this safe path.
    narrow_rejects = [
        m for m in gate_markers
        if m["gate"] == "narrow_replacement_gate"
        and m["verdict"] == "rejected"
    ]
    assert not narrow_rejects, (
        f"narrow_replacement_gate fired {len(narrow_rejects)} rejections "
        f"on the safe path. The gate should no-op when no "
        f"BlastRadiusDropRecord is registered for the QID. Sample: "
        f"{narrow_rejects[:2]!r}"
    )

    # ``applier_gate`` must have run for at least one QID — proof we
    # crossed APPLYABLE. ``workspace_client=None`` means the real apply
    # call will fail, so the outcomes will be ``applyability_rejected``
    # rather than ``applied`` until Step 2 (FakeWorkspaceClient) lands.
    outcome_markers = parse_patch_outcome_markers(stdout)
    applyability_outcomes = [
        m for m in outcome_markers
        if m.get("deepest_stage_in_attempt") in (
            FunnelStage.APPLYABLE.value, FunnelStage.APPLIED.value,
        )
    ]
    assert applyability_outcomes, (
        "applier_gate emitted no GSO_PATCH_OUTCOME_V1 payloads — the "
        "state never reached APPLYABLE despite the safe path. "
        f"all outcomes: {outcome_markers[:3]!r}"
    )

    # Transition assertion: the canonical safe-path step
    # ``normalized → applyable`` must fire for every hard QID.
    by_qid: dict[str, list[tuple[str, str]]] = {}
    for t in parse_qstate_transitions(stdout):
        by_qid.setdefault(t["qid"], []).append(
            (t["from_stage"], t["to_stage"])
        )
    for qid in qids:
        steps = by_qid.get(qid, [])
        assert ("normalized", "applyable") in steps, (
            f"qid={qid!r} never emitted the normalized → applyable "
            f"transition; observed steps: {steps!r}."
        )


@pytest.mark.integration
def test_unsafe_blast_radius_cycles_state_back_to_proposed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the Stage 3 proposal carries
    ``passing_dependents=[<qid outside target>]``,
    :func:`patch_blast_radius_is_safe` returns
    ``safe=False reason=blast_radius_exceeds_threshold``,
    ``blast_radius_batch`` cycles the state ``NORMALIZED → PROPOSED``
    and records a typed ``ProposalAttempt`` with
    ``outcome="blast_radius_rejected"``.

    Locks in three observable surfaces:

      1. At least one ``GSO_GATE_REASONING_V1`` line emits
         ``gate=blast_radius_batch verdict=rejected``. Its
         ``reason`` carries the
         ``blast_radius_exceeds_threshold`` token and the
         ``predicate_inputs.collateral_qids`` field is populated.

      2. The corresponding QID's state has at least one
         ``ProposalAttempt`` with
         ``outcome == "blast_radius_rejected"``. This is the typed
         surface postmortems read.

      3. A ``normalized → proposed`` transition is recorded — the
         escalation cycle's hallmark step.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    assert qids, "production fixture must declare hard hydration QIDs"

    cycles = 10
    collateral = ("gs_outside_target_001", "gs_outside_target_002")
    tape = []
    for _ in range(cycles):
        tape += diagnose_response_tape(qids, rca_kind_label=_KIT_FREE_RCA_KIND)
    for _ in range(cycles):
        tape += cluster_response_tape(qids)
    for _ in range(cycles):
        tape += synthesize_blast_radius_unsafe_tape(
            qids, collateral_qids=collateral,
        )
    harness = TapeReplayHarness(tape=tape)

    from genie_space_optimizer.optimization import optimizer as opt_mod

    buf = io.StringIO()
    t0 = time.monotonic()
    with redirect_stdout(buf), harness.patch():
        final_states = opt_mod.run_state_machine_iteration_and_persist(
            eval_rows=rows,
            iteration=1,
            run_id="applyable-reject",
            run_root=tmp_path,
            workspace_client=None,
            metadata_snapshot=forward_metadata_snapshot(rows),
            forbidden_signatures=(),
        )
    elapsed = time.monotonic() - t0
    stdout = buf.getvalue()

    assert elapsed < _ACCEPTANCE_CEILING_SECONDS, (
        f"Unsafe blast-radius replay took {elapsed:.2f}s; ceiling is "
        f"{_ACCEPTANCE_CEILING_SECONDS}s."
    )

    # Surface 1: typed gate-reasoning marker.
    gate_markers = parse_gate_reasoning_markers(stdout)
    blast_rejects = [
        m for m in gate_markers
        if m["gate"] == "blast_radius_batch" and m["verdict"] == "rejected"
    ]
    assert blast_rejects, (
        "blast_radius_batch never emitted a rejection marker even "
        "though the patch carried passing_dependents outside the "
        f"target set. all gate_markers={gate_markers!r}"
    )
    for m in blast_rejects:
        assert "blast_radius_exceeds_threshold" in m["reason"], (
            f"blast_radius_batch rejection for qid={m['qid']!r} did "
            f"not name the expected reason token. reason="
            f"{m['reason']!r}"
        )
        # ``predicate_inputs.collateral_qids`` is a list of strings the
        # marker emits so postmortems can attribute the rejection
        # without code spelunking.
        collateral_list = m["predicate_inputs"].get("collateral_qids", [])
        assert collateral_list, (
            f"blast_radius_batch rejection for qid={m['qid']!r} emitted "
            f"an empty collateral_qids list; expected at least one of "
            f"{collateral!r}. predicate_inputs="
            f"{m['predicate_inputs']!r}"
        )
        assert any(q in collateral_list for q in collateral), (
            f"qid={m['qid']!r} collateral list {collateral_list!r} "
            f"does not overlap the injected collateral {collateral!r}."
        )

    # Surface 2: typed ProposalAttempt on at least one rejected QID.
    rejected_qids = {m["qid"] for m in blast_rejects}
    by = states_by_qid(final_states)
    for qid in rejected_qids:
        s = by[qid]
        attempts = [
            a for a in s.proposals
            if getattr(a, "outcome", "") == "blast_radius_rejected"
        ]
        assert attempts, (
            f"qid={qid!r} has no ProposalAttempt with "
            f"outcome='blast_radius_rejected' despite the gate marker. "
            f"state.proposals={s.proposals!r}"
        )

    # Surface 3: ``normalized → proposed`` escalation step recorded
    # for every QID the gate rejected.
    by_qid_steps: dict[str, list[tuple[str, str]]] = {}
    for t in parse_qstate_transitions(stdout):
        by_qid_steps.setdefault(t["qid"], []).append(
            (t["from_stage"], t["to_stage"])
        )
    for qid in rejected_qids:
        steps = by_qid_steps.get(qid, [])
        assert ("normalized", "proposed") in steps, (
            f"qid={qid!r} was rejected by blast_radius_batch but the "
            f"escalation cycle (normalized → proposed) is missing from "
            f"the transition log. steps={steps!r}"
        )
