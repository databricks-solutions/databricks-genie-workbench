"""Workbench Stage 1 preflight tests.

Three guarantees the probe ships with:

* **Drift prevention** — the probe and the SM canonical lane share a
  single Stage 1 card builder. If anyone refactors the runtime helper
  (``diagnose_llm._build_failing_qid_payload``) without updating the
  probe, the drift test fires.
* **Corpus passes the SM canonical lane** — with Plan 12 typed RCA
  evidence threaded (the Trial 13 typed-evidence cutover), every QID
  in the committed sanitized production-replay corpus produces zero
  Stage 1 violations. Replaces the pre-cutover xfail.
* **Bare row surfaces the Trial 12 silent decline mode** — a row
  without question text still flags ``question_text_empty`` before any
  deploy.
"""
from __future__ import annotations

import pytest

from local_lever_workbench.input_bundle import from_production_replay
from local_lever_workbench.models import (
    WorkbenchHardCase,
    WorkbenchInputBundle,
    WorkbenchProvenance,
)
from local_lever_workbench.stage1_probe import (
    _build_card_via_runtime_helper,
    _rebuild_typed_evidence,
    probe_bundle,
    probe_case,
)


@pytest.mark.workbench
def test_probe_matches_sm_canonical_lane_runtime_helper() -> None:
    """Probe output must equal the SM canonical lane's runtime helper.

    The probe wraps ``_build_failing_qid_payload`` so the two paths
    cannot drift. If anyone changes the helper's body (e.g. adds
    pre-/post-processing around ``build_stage1_evidence_card``) without
    teaching the probe, the violations diverge and this test fires.
    """
    from genie_space_optimizer.optimization.stage1_input_evidence_contract import (
        DEFAULT_STAGE1_CONTRACT,
    )
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
    )
    from genie_space_optimizer.optimization.state_machine.records import (
        HardQidSeenRecord,
    )
    from genie_space_optimizer.optimization.state_machine.state import (
        QuestionStateInIteration,
    )
    from genie_space_optimizer.optimization.state_machine.transformers.diagnose_llm import (
        _build_failing_qid_payload,
    )

    row = {
        "request": {
            "kwargs": {"question_id": "qid_x", "question": "what is X?"},
        },
        "expected_response/value": "SELECT 1",
        "response": "SELECT 2",
    }
    runtime_state = QuestionStateInIteration(
        qid="qid_x",
        iteration=0,
        current_stage=FunnelStage.HARD_QID_SEEN,
        deepest_stage_reached=FunnelStage.HARD_QID_SEEN,
        seen=HardQidSeenRecord(
            eval_row_id="drift-test",
            predicate="row_is_hard_failure",
            score=0.0,
            baseline_sql="",
            expected_shape="",
            iteration_first_seen=0,
        ),
    )
    runtime_card = _build_failing_qid_payload(
        runtime_state, row, typed_evidence=None,
    )
    probe_card = _build_card_via_runtime_helper(
        "qid_x", row, typed_evidence=None,
    )
    runtime_violations = tuple(
        str(getattr(v, "field", v))
        for v in DEFAULT_STAGE1_CONTRACT.validate(runtime_card)
    )

    case = WorkbenchHardCase(qid="qid_x", row=row, typed_evidence=None)
    finding = probe_case(case)
    assert finding.violations == runtime_violations
    # Card structure parity is the stronger invariant; assert it
    # explicitly so future refactors that change the card shape are
    # caught even if the contract still happens to agree.
    assert probe_card == runtime_card


@pytest.mark.workbench
def test_sm_canonical_lane_accepts_full_production_replay_corpus() -> None:
    """With typed evidence threaded, every production-replay QID passes.

    This is the post-Trial 13 gate. The bundle commits Plan 12 typed
    RCA evidence per hard case; the probe forwards that into the
    runtime helper exactly like ``ctx.rca_evidence_typed`` is forwarded
    by ``run_state_machine_iteration_and_persist``. If a future change
    re-introduces the typed-evidence drop (the Trial 12 / 13 silent
    decline mode), this assertion names the failing QIDs:
    ``evidence_card_empty:blame_set_empty,rca_evidence_empty``.
    """
    bundle = from_production_replay()
    result = probe_bundle(bundle)
    failing = [
        (f.qid, list(f.violations)) for f in result.findings if f.violations
    ]
    assert not failing, (
        f"Stage 1 preflight failed for committed production-replay "
        f"cases: {failing!r}. This is the Trial 12 / 13 typed-evidence "
        f"drop silent decline mode. Confirm "
        f"diagnose_llm._invoke_stage1_llm still threads "
        f"ctx.rca_evidence_typed into _build_failing_qid_payload, and "
        f"that harness.py passes metadata_snapshot to "
        f"run_state_machine_iteration_and_persist."
    )
    assert result.all_pass is True


@pytest.mark.workbench
def test_probe_flags_question_text_empty_for_bare_row() -> None:
    """A row with no question/SQL must surface ``question_text_empty``.

    Pins the workbench's value proposition: the exact Trial 12
    silent decline mode must be observable locally before deploy.
    """
    bare_case = WorkbenchHardCase(
        qid="qid_bare",
        row={"request": {"kwargs": {"question_id": "qid_bare"}}},
        typed_evidence=None,
        expected_card_violations=(),
    )
    finding = probe_case(bare_case)
    assert "question_text_empty" in finding.violations, (
        f"workbench should detect the bare-row silent decline mode; "
        f"got violations={list(finding.violations)!r}"
    )
    assert finding.would_dispatch_llm is False


@pytest.mark.workbench
def test_probe_field_sources_carry_origin_path_for_real_rows() -> None:
    """A real-row case should resolve ``question_text`` to a concrete path.

    Field sources are what postmortems quote — surfacing the resolved
    row path makes the Stage 1 success readable without re-running.
    """
    bundle = from_production_replay(run_tags=["98ec"])
    case = bundle.hard_cases[0]
    finding = probe_case(case)
    source = finding.field_sources.get("question_text", "")
    assert source not in ("", "absent"), (
        f"question_text source unresolved for real-row case "
        f"qid={case.qid}; got field_sources={dict(finding.field_sources)!r}. "
        f"The Trial 12 RCA called this out — postmortems need the "
        f"verbatim production row path."
    )


@pytest.mark.workbench
def test_probe_bundle_with_only_failing_cases_returns_all_pass_false() -> None:
    """Aggregate flag should flip when any QID violates the contract."""
    failing_bundle = WorkbenchInputBundle(
        provenance=WorkbenchProvenance(source_kind="synthetic"),
        space_id="deadbeefcafebabe1234567890abcdef",
        hard_cases=(
            WorkbenchHardCase(
                qid="qid_x",
                row={"request": {"kwargs": {"question_id": "qid_x"}}},
            ),
        ),
    )
    result = probe_bundle(failing_bundle)
    assert result.all_pass is False


@pytest.mark.workbench
def test_rebuild_typed_evidence_round_trips_canonical_payload() -> None:
    """Bundle deserialization must round-trip the canonical typed dict.

    Workbench bundles serialize typed evidence as plain JSON-friendly
    dicts; the probe rehydrates them so the runtime helper sees the
    real type. Regressions in the rebuild helper would silently drop
    blame_set / rca subfields and re-introduce the Trial 12 mode.
    """
    payload = {
        "qid": "qid_x",
        "observed_failure": "missing limit",
        "generated_sql_issue": "no LIMIT 10 on outer SELECT",
        "expected_sql_shape": "outer ORDER BY ... LIMIT 10",
        "blame_set": ["main.public.orders.amount"],
        "suggested_repair_family": "add_example_sql",
        "confidence": "high",
        "quoted_evidence": [],
    }
    ev = _rebuild_typed_evidence(payload)
    assert ev is not None
    assert ev.qid == "qid_x"
    assert ev.blame_set == ("main.public.orders.amount",)
    assert ev.observed_failure == "missing limit"
    assert ev.confidence == "high"
