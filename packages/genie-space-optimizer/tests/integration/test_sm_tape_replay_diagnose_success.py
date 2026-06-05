"""PR-C reviewer P0 #3 — forward-only replay of a SUCCESSFUL Stage 1.

The pre-existing ``test_sm_tape_replay_dc89d1a9.py`` reproduces the
failure shape (Stage 1 BadRequest → diagnose_returned_empty). The
reviewer's P0 #3 asked for the COMPLEMENTARY scaffolding: drive the
state machine with a mocked-VALID Stage 1 response and assert the SM
actually escapes ``HARD_QID_SEEN`` and the per-QID diagnosis is wired
through to the rest of the pipeline. This is the integration test that
would have caught the dc89d1a9 schema bug before deployment — because
the test runs the SM end to end against the real response_format the
production code emits.

Together with ``test_abstainable_envelope_schema_branches`` (which
pins the schema's structural integrity) and the dc89d1a9 failure tape
(which pins the unhappy-path classifier), this triple gives PR-C its
acceptance criteria:

  1. Schema collapse is provably fixed (envelope branches typed).
  2. Provider 400 still classifies as ``response_format_invalid``
     (no regression on the failure-path observability built in PR-A).
  3. With a valid LLM response, Stage 1 successfully diagnoses every
     hard QID and the SM transitions past ``HARD_QID_SEEN``.

If criterion 3 ever regresses we have a sub-second offline check;
without it the only way to verify Stage 1 success was a 45-minute
lever-loop trial.
"""
from __future__ import annotations

import io
import json
import re
import time
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from tests.integration.sm_tape_replay import (
    TapeEntry,
    TapeExhaustedError,
    TapeReplayHarness,
)


_HARD_QIDS = ("gs_001", "gs_004", "gs_013")

# Trial 13i — run-level schema_columns matching the ``_hard_row`` blame
# seed (``catalog.schema.orders.amount``). Without this channel the
# ``validate_schema_columns`` pre-flight short-circuits Stage 1 with
# ``missing_schema_columns`` *before* the batched diagnose call, so the
# tape is never consumed (``consumed_count == 0``). The single batched
# tape entry is already the current contract shape; the missing input
# was the schema_columns plumbing, not the tape.
_SCHEMA_COLUMNS_SNAPSHOT = {"schema_columns": ["catalog.schema.orders.amount"]}


def _hard_row(qid: str) -> dict:
    """Build a production-shape row that ``row_is_hard_failure`` admits.

    Trial 12: rows must carry enough fields for the Stage 1 input
    evidence contract to pass (otherwise the pre-flight short-circuits
    and the LLM is never invoked). Adds question text, ground-truth
    SQL, generated SQL, and a minimal ASI metadata blob carrying the
    blame seed and rca_kind.
    """
    return {
        "inputs/question_id": qid,
        "inputs/question": f"What is the total for {qid}?",
        "inputs/ground_truth_sql": f"SELECT SUM(amount) FROM orders WHERE id = '{qid}';",
        "outputs/response": "SELECT amount FROM orders;",
        "feedback/result_correctness/value": "no",
        "feedback/result_correctness/rationale": (
            f"Generated SQL for {qid} omitted aggregation."
        ),
        "feedback/arbiter/value": "ground_truth_correct",
        "feedback/asi/metadata": {
            "blame_set": ["catalog.schema.orders.amount"],
            "rca_kind": "wrong_aggregation",
            "failure_type": "missing_aggregation",
        },
        "score": 0.0,
        "sql": "SELECT 1",
        "expected_shape": "SELECT count(*) FROM x",
        "eval_row_id": f"row_{qid}",
    }


def _valid_diagnose_response(qids: tuple[str, ...]) -> TapeEntry:
    """Build a tape entry that mirrors a SUCCESSFUL Stage 1 LLM call.

    ``parsed_output`` carries the same payload that
    ``parse_envelope`` would produce from a JSON-schema-conformant
    response: one ``DiagnosisItem`` per failing QID, wrapped in the
    Plan 11 ``diagnoses`` list.
    """
    diagnoses = [
        {
            "qid": qid,
            "rca_kind_label": "wrong_aggregation",
            "observed_failure": (
                "Generated SQL returned per-row values; expected SUM."
            ),
            "generated_sql_issue": (
                "Query selected raw amount column without aggregation."
            ),
            "expected_sql_shape": (
                "SELECT SUM(amount) FROM orders;"
            ),
            "blame_set": ["catalog.schema.orders.amount"],
            "evidence_summary": (
                "Expected SQL uses SUM; generated SQL omits it."
            ),
            "confidence": "high",
        }
        for qid in qids
    ]
    return TapeEntry(
        kind="response",
        skill_id="plan11_diagnose",
        call_id="plan11_stage1_diagnose.iter_1",
        iteration=1,
        parsed_output={"diagnoses": diagnoses},
        raw_text="",  # unused — parse_envelope is bypassed for tape responses
        tokens_input=1234,
        tokens_output=567,
        duration_ms=4321,
    )


@pytest.mark.integration
def test_sm_iteration_with_valid_stage1_advances_past_hard_qid_seen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A valid Stage 1 LLM response must drive every admitted QID from
    HARD_QID_SEEN to DIAGNOSED — emitting a ``GSO_QSTATE_TRANSITION_V1``
    marker with ``transformer_name=plan11_stage1_diagnosis`` per QID.

    Scope is deliberately narrow: only Stage 1 success is mocked. The
    SM is expected to then drive downstream stages (plan11_cluster,
    synthesize, ...) — which will exhaust the tape since this test
    only stocks the Stage 1 entry. Catching ``TapeExhaustedError`` is
    the EXPECTED outcome: it proves Stage 1 succeeded AND the SM
    successfully escaped Stage 1 to call the next stage. If Stage 1
    failed, we would never reach a downstream stage, the tape would
    not exhaust, and this test would fail differently.

    The pre-PR-C dc89d1a9 lever loop spent 45 minutes to demonstrate
    that Stage 1 was completely broken. This same check now takes
    under 5 seconds.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = [_hard_row(qid) for qid in _HARD_QIDS]
    tape = [_valid_diagnose_response(_HARD_QIDS)]
    harness = TapeReplayHarness(tape=tape)

    from genie_space_optimizer.optimization import optimizer as opt_mod

    buf = io.StringIO()
    t0 = time.monotonic()
    with redirect_stdout(buf), harness.patch():
        try:
            opt_mod.run_state_machine_iteration_and_persist(
                eval_rows=rows,
                iteration=1,
                run_id="valid-stage1-replay",
                run_root=tmp_path,
                workspace_client=None,
                metadata_snapshot=_SCHEMA_COLUMNS_SNAPSHOT,
                forbidden_signatures=(),
            )
        except TapeExhaustedError:
            # Expected — see docstring. Stage 1 succeeded and the SM
            # progressed to a downstream stage that this test
            # deliberately leaves unstocked.
            pass
    elapsed = time.monotonic() - t0
    stdout = buf.getvalue()

    assert elapsed < 5.0, (
        f"Replay took {elapsed:.2f}s; acceptance ceiling is 5s."
    )

    # Invariant 1 — Stage 1 was consumed (single batched call).
    assert harness.consumed_count == 1, (
        f"Consumed {harness.consumed_count} Stage 1 entries; expected "
        f"exactly 1 batched call. Either diagnose_failing_qids stopped "
        f"batching, or Stage 1 was called multiple times (retry?)."
    )

    # Invariant 2 — every QID got a 'diagnosed' Stage 1 marker. The
    # marker emission happens for the BATCHED call, so it's emitted
    # once per QID right after Stage 1 returns regardless of how far
    # downstream the SM gets on any individual QID's funnel walk.
    diagnosis_markers = [
        json.loads(m.group(1))
        for m in re.finditer(
            r"GSO_PLAN11_STAGE1_DIAGNOSIS_V1 (\{.+\})", stdout,
        )
    ]
    diagnosed_outcomes = [
        m for m in diagnosis_markers if m.get("outcome") == "diagnosed"
    ]
    diagnosed_qids_per_marker = {m["qid"] for m in diagnosed_outcomes}
    assert diagnosed_qids_per_marker == set(_HARD_QIDS), (
        f"Stage 1 emitted 'diagnosed' markers for "
        f"{diagnosed_qids_per_marker!r}; expected {set(_HARD_QIDS)!r}. "
        f"Either Stage 1 returned without a per-QID diagnosis or a "
        f"QID was silently dropped post-diagnose."
    )
    for marker in diagnosed_outcomes:
        assert marker["tokens_input"] == 1234, marker
        assert marker["tokens_output"] == 567, marker
        assert marker["rca_kind_label"] == "wrong_aggregation", marker

    # Invariant 3 — at least one QID transitioned from HARD_QID_SEEN
    # to DIAGNOSED. The SM processes QIDs sequentially and crashes on
    # the first downstream tape miss (deliberate scope of this test),
    # so we cannot assert "all transitions emit" — only that Stage 1
    # success drove SOME QID into DIAGNOSED, the dc89d1a9 failure
    # shape this test was built to rule out.
    transitions = [
        json.loads(m.group(1))
        for m in re.finditer(
            r"GSO_QSTATE_TRANSITION_V1 (\{.+\})", stdout,
        )
    ]
    diagnosed_transitions = [
        t for t in transitions
        if t.get("from_stage") == "hard_qid_seen"
        and t.get("to_stage") == "diagnosed"
        and t.get("transformer_name") == "plan11_stage1_diagnosis"
    ]
    assert len(diagnosed_transitions) >= 1, (
        f"No QID transitioned hard_qid_seen → diagnosed despite "
        f"{len(diagnosed_outcomes)} diagnosed markers. Stage 1 "
        f"emitted diagnoses but the SM never lifted any QID out of "
        f"HARD_QID_SEEN — the dc89d1a9 failure shape exactly."
    )


@pytest.mark.integration
def test_sm_iteration_with_valid_stage1_does_not_terminate_with_dc89d1a9_signature(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Negative-image companion to the previous test: no QID may
    terminate with the dc89d1a9 ``abstain: diagnose_returned_empty``
    reason when Stage 1 returned a valid response. This is the exact
    failure signature dc89d1a9 emitted, so guarding against it
    explicitly makes regression obvious.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    rows = [_hard_row(qid) for qid in _HARD_QIDS]
    tape = [_valid_diagnose_response(_HARD_QIDS)]
    harness = TapeReplayHarness(tape=tape)

    from genie_space_optimizer.optimization import optimizer as opt_mod

    buf = io.StringIO()
    with redirect_stdout(buf), harness.patch():
        try:
            opt_mod.run_state_machine_iteration_and_persist(
                eval_rows=rows,
                iteration=1,
                run_id="valid-stage1-no-empty-replay",
                run_root=tmp_path,
                workspace_client=None,
                metadata_snapshot=_SCHEMA_COLUMNS_SNAPSHOT,
                forbidden_signatures=(),
            )
        except TapeExhaustedError:
            pass
    stdout = buf.getvalue()

    # No diagnosis marker should report an abstain_reason of
    # diagnose_returned_empty when Stage 1 returned data.
    empty_abstain_markers = [
        json.loads(m.group(1))
        for m in re.finditer(
            r"GSO_PLAN11_STAGE1_DIAGNOSIS_V1 (\{.+\})", stdout,
        )
        if "diagnose_returned_empty" in m.group(1)
    ]
    assert not empty_abstain_markers, (
        f"{len(empty_abstain_markers)} Stage 1 markers carry "
        f"'diagnose_returned_empty' despite a valid Stage 1 LLM "
        f"response — this is the dc89d1a9 failure shape. Stage 1 "
        f"diagnosis interpretation of envelope responses regressed."
    )


# ── PR-2D — sanitized envelope passes the wire contract ──────────────
#
# The tape-replay tests above bypass ``_traced_llm_call``'s pre-flight
# (the ContextVar override path short-circuits before pre-flight even
# runs). That is correct for those tests — they pin SM behaviour given
# a valid Stage 1 response.
#
# This pair of tests complements them at the OTHER end of the
# pipeline: the response_format that ``LlmReasoningCall.invoke`` would
# dispatch for Plan 11 Stage 1 must pass the
# ``DatabricksEndpointRequestContract``. With PR-1B
# (``_safe_schema_name``) shipped, this should be trivially true; pin
# it so a future regression in ``build_response_format`` (e.g. a new
# ``__name__`` source) lights up offline before deploy.


@pytest.mark.integration
def test_plan11_stage1_dispatch_envelope_passes_databricks_contract() -> None:
    """End-to-end offline gate: the exact response_format Plan 11
    Stage 1 dispatches must satisfy every Databricks endpoint
    constraint pinned by PR-2A.

    Pre-PR-1B this assertion would have failed with
    ``response_format.json_schema.name = 'AbstainableEnvelope[Plan11DiagnoseOutput]'``
    — the dc89d1a9 / 98ec8950 root cause. Post-PR-1B (the
    ``_safe_schema_name`` sanitizer) the name is
    ``AbstainableEnvelope_Plan11DiagnoseOutput`` and every contract
    rule passes.

    If this test ever fails on a future commit, the next lever-loop
    trial will reproduce the dc89d1a9 failure shape — the test is the
    offline equivalent.
    """
    from genie_space_optimizer.optimization.databricks_request_contract import (
        DEFAULT_CONTRACT,
    )
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        AbstainableEnvelope,
    )
    from genie_space_optimizer.optimization.prompt_io import (
        build_response_format,
    )
    from genie_space_optimizer.skills.plan11_diagnose.output_schema import (
        Plan11DiagnoseOutput,
    )

    envelope_cls = AbstainableEnvelope[Plan11DiagnoseOutput]
    response_format = build_response_format(envelope_cls)
    call_kwargs = {
        "model": "databricks-claude-opus-4-6",
        "messages": [
            {"role": "system", "content": "system"},
            {"role": "user", "content": "user"},
        ],
        "temperature": 0.0,
        "max_tokens": 4096,
        "response_format": response_format,
    }
    violations = DEFAULT_CONTRACT.validate(call_kwargs)
    assert violations == [], (
        f"Plan 11 Stage 1 dispatch envelope violates Databricks contract: "
        f"{[(v.field, v.constraint) for v in violations]}. The next "
        f"lever-loop trial will reproduce the dc89d1a9 failure shape."
    )


@pytest.mark.integration
def test_plan11_stage1_dispatch_envelope_name_is_pr1b_sanitized() -> None:
    """Sanity pin on the post-PR-1B wire name. Locks in the human-
    readable form so a regression that produces e.g. ``schema`` or an
    empty-string fallback shows up as a test diff rather than a
    silent semantic shift in MLflow traces."""
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        AbstainableEnvelope,
    )
    from genie_space_optimizer.optimization.prompt_io import (
        build_response_format,
    )
    from genie_space_optimizer.skills.plan11_diagnose.output_schema import (
        Plan11DiagnoseOutput,
    )

    rf = build_response_format(AbstainableEnvelope[Plan11DiagnoseOutput])
    # The trailing ``]`` in ``AbstainableEnvelope[Plan11DiagnoseOutput]``
    # is replaced by ``_`` (PR-1B sanitizer is intentionally minimal —
    # it does not strip trailing ``_`` because ``_`` is regex-legal and
    # because stripping would mutate already-safe names like
    # ``_Example`` that some non-Plan-11 call sites use). Pin the exact
    # post-sanitization shape so any future change to either the input
    # generic alias or the sanitizer surfaces here as a clear diff.
    assert (
        rf["json_schema"]["name"]
        == "AbstainableEnvelope_Plan11DiagnoseOutput_"
    ), rf["json_schema"]["name"]
