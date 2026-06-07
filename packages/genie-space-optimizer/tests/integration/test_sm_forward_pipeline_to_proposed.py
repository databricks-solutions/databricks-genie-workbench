"""Forward-pipeline smoke test — the pre-deploy gate.

A single tape-driven run that proves production-shaped eval rows
hydrate, dispatch, advance through Stage 1, Stage 2, and Stage 3 to
at least :class:`FunnelStage.PROPOSED`. Any future deploy that fails
the forward pipeline must reproduce here in seconds before it burns
a 45-minute Databricks trial.

Aligned with the ``fast-optimizer-testing`` plan Task 3.
"""
from __future__ import annotations

import io
import time
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from tests.integration.sm_forward_fixtures import (
    assert_no_terminal_reason,
    assert_stage_reached,
    expected_hard_qids,
    forward_metadata_snapshot,
    load_production_hydration_rows,
    parse_qstate_transitions,
    parse_stage1_diagnosis_markers,
    parse_stage2_clustering_markers,
    parse_stage3_synthesis_markers,
    states_by_qid,
)
from tests.integration.sm_forward_tapes import (
    KIT_FREE_RCA_KIND,
    full_forward_tape,
)
from tests.integration.sm_tape_replay import TapeReplayHarness


_ACCEPTANCE_CEILING_SECONDS = 5.0


@pytest.mark.integration
def test_forward_pipeline_advances_production_rows_to_proposed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production-shape eval rows must advance through every Plan 11
    stage end-to-end under tape replay, in under five seconds, with
    no QID terminating via the dc89d1a9-class failure shapes the
    hydration sweep was meant to eliminate.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = load_production_hydration_rows()
    qids = expected_hard_qids(rows)
    assert qids, (
        "production_eval_rows.json must declare at least one hard "
        "hydration QID; the fixture loader is mis-wired."
    )

    # Trial 26 W26.2: diagnose a kit-FREE RCA so the single-lever forward
    # vehicle advances past the kit gate (this test verifies funnel
    # mechanics, not kit synthesis — see KIT_FREE_RCA_KIND docstring).
    tape = full_forward_tape(qids, rca_kind_label=KIT_FREE_RCA_KIND)
    harness = TapeReplayHarness(tape=tape)

    from genie_space_optimizer.optimization import optimizer as opt_mod
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
    )

    buf = io.StringIO()
    t0 = time.monotonic()
    with redirect_stdout(buf), harness.patch():
        final_states = opt_mod.run_state_machine_iteration_and_persist(
            eval_rows=rows,
            iteration=1,
            run_id="forward-smoke",
            run_root=tmp_path,
            workspace_client=None,
            metadata_snapshot=forward_metadata_snapshot(rows),
            forbidden_signatures=(),
        )
    elapsed = time.monotonic() - t0
    stdout = buf.getvalue()

    # Speed: the entire forward pipeline must complete within the
    # acceptance ceiling so the test is a usable developer feedback
    # loop. Slow drift here defeats the purpose of the harness.
    assert elapsed < _ACCEPTANCE_CEILING_SECONDS, (
        f"Forward pipeline replay took {elapsed:.2f}s; acceptance "
        f"ceiling is {_ACCEPTANCE_CEILING_SECONDS}s. A slower test "
        f"means developers will skip it before deploying."
    )

    # Admission: the SM must observe every expected hard QID. Anything
    # less means the dispatch / admission path lost a row, the same
    # starvation failure mode the 2026-05-23 postmortems flagged.
    final_qids = tuple(s.qid for s in final_states)
    assert set(final_qids) == set(qids), (
        f"SM admitted {sorted(set(final_qids))!r}; expected "
        f"{sorted(set(qids))!r}. Missing QIDs: "
        f"{sorted(set(qids) - set(final_qids))!r}."
    )

    # Progress: every QID must reach at least PROPOSED. The full
    # cluster + synthesize tape stocks the survival contract, so any
    # QID that stops short of PROPOSED indicates a regression in the
    # transformer chain (Stage 1, Stage 2, Stage 3 contract gate, or
    # the survival contract).
    for qid in qids:
        assert_stage_reached(final_states, qid, FunnelStage.PROPOSED)

    # No QID may terminate with the dc89d1a9 / Trial 11 failure shapes
    # the hydration sweep was meant to eliminate. These fragments are
    # observed in real terminal-reason strings:
    #
    #   * ``diagnose_returned_empty``: Stage 1 LLM returned no diagnoses.
    #   * ``missing_schema_context``: classic Trial 11 abstain reason.
    #   * ``evidence_card_empty``: pre-flight contract caught an empty card.
    for fragment in (
        "diagnose_returned_empty",
        "missing_schema_context",
        "evidence_card_empty",
    ):
        assert_no_terminal_reason(final_states, fragment)

    # Markers: Stage 1 must emit one ``diagnosed`` marker per QID.
    diagnosis_markers = parse_stage1_diagnosis_markers(stdout)
    diagnosed_qids = {
        m["qid"] for m in diagnosis_markers if m.get("outcome") == "diagnosed"
    }
    assert diagnosed_qids == set(qids), (
        f"Stage 1 emitted diagnosed markers for {sorted(diagnosed_qids)!r}; "
        f"expected {sorted(qids)!r}. Missing: "
        f"{sorted(set(qids) - diagnosed_qids)!r}."
    )
    for m in diagnosis_markers:
        if m.get("outcome") != "diagnosed":
            continue
        assert m.get("diagnosis_actionable") is True, (
            f"Stage 1 marker for qid={m.get('qid')!r} flipped "
            f"diagnosis_actionable=False. The tape default is meant to "
            f"satisfy the actionable predicate."
        )

    # Markers: Stage 2 and Stage 3 must each emit at least one outcome
    # marker. Stage 3 must report at least one ``synthesized`` outcome
    # — the proof Stage 3 actually returned a proposal.
    cluster_markers = parse_stage2_clustering_markers(stdout)
    assert any(
        m.get("outcome") == "clustered" for m in cluster_markers
    ), (
        f"Stage 2 emitted no 'clustered' outcome. cluster_markers="
        f"{cluster_markers!r}"
    )
    synth_markers = parse_stage3_synthesis_markers(stdout)
    assert any(
        m.get("outcome") == "synthesized" for m in synth_markers
    ), (
        f"Stage 3 emitted no 'synthesized' outcome. synth_markers="
        f"{synth_markers!r}"
    )

    # Trajectory and qstate persistence — the canonical postmortem
    # surfaces. ``run_state_machine_iteration_and_persist`` writes
    # ``iteration_<n>/qstate_<qid>.json`` and
    # ``trajectories/trajectory_<qid>.json`` per the persistence layout.
    for qid in qids:
        qstate_files = list(tmp_path.glob(f"iteration_*/qstate_{qid}.json"))
        trajectory_files = list(
            tmp_path.glob(f"trajectories/trajectory_{qid}.json"),
        )
        assert qstate_files, (
            f"qstate JSON for qid={qid!r} not persisted under "
            f"{tmp_path}. Postmortem reconstruction depends on this."
        )
        assert trajectory_files, (
            f"trajectory JSON for qid={qid!r} not persisted under "
            f"{tmp_path}. Phase 7 acceptance reads trajectories to "
            f"compute deepest_stage_by_qid."
        )

    # All tape entries must be consumed — leftover entries indicate
    # the SM took an unexpected shortcut that the harness didn't
    # account for.
    assert harness.unconsumed() == [], (
        f"{len(harness.unconsumed())} tape entries unconsumed after "
        f"forward pipeline run. Either the SM stopped early or the "
        f"tape over-captured. unconsumed="
        f"{[(e.skill_id, e.call_id) for e in harness.unconsumed()]!r}"
    )

    # Transitions: every QID must emit ``hard_qid_seen → diagnosed``
    # and ``diagnosed → clustered`` and ``clustered → proposed``.
    transitions = parse_qstate_transitions(stdout)
    by_qid: dict[str, list[tuple[str, str]]] = {}
    for t in transitions:
        by_qid.setdefault(t["qid"], []).append(
            (t["from_stage"], t["to_stage"])
        )
    expected_steps = (
        ("hard_qid_seen", "diagnosed"),
        ("diagnosed", "clustered"),
        ("clustered", "proposed"),
    )
    for qid in qids:
        steps = by_qid.get(qid, [])
        for step in expected_steps:
            assert step in steps, (
                f"qid={qid!r} missing transition {step!r}; observed "
                f"transitions: {steps!r}."
            )

    # Final-state sanity: every QID's deepest_stage_reached must be
    # ``proposed`` or deeper by funnel index (StrEnum string ordering
    # is alphabetical, which would falsely classify ``applyable`` <
    # ``proposed``; the funnel index reflects the real pipeline depth).
    from genie_space_optimizer.optimization.state_machine.funnel import (
        stage_index,
    )

    by_qid_state = states_by_qid(final_states)
    for qid in qids:
        s = by_qid_state[qid]
        assert stage_index(s.deepest_stage_reached) >= stage_index(
            FunnelStage.PROPOSED,
        ), (
            f"qid={qid!r} deepest_stage_reached="
            f"{s.deepest_stage_reached!r} (index "
            f"{stage_index(s.deepest_stage_reached)}) is below PROPOSED "
            f"(index {stage_index(FunnelStage.PROPOSED)}). terminal="
            f"{s.terminal!r}"
        )
