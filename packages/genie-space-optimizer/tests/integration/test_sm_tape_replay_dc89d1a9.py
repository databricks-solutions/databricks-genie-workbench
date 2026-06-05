"""PR-B acceptance test — SM tape replay of dc89d1a9 BadRequest cascade.

Trial 12 update: the dc89d1a9 production failure shape (Stage 1
``BadRequestError`` → ``diagnose_returned_empty``) is now blocked one
boundary earlier by the ``Stage1InputEvidenceContract`` pre-flight.
With empty input cards (no question text / ground truth / generated
SQL / ASI metadata) the LLM is never invoked, so:

  * the tape stays untouched (no BadRequestError replay),
  * no ``GSO_PLAN11_STAGE1_DIAGNOSIS_V1`` marker fires,
  * instead each admitted QID emits
    ``GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1`` with typed
    ``violations`` and ``field_sources``,
  * and the terminal reason becomes ``abstain:
    evidence_card_empty:<violation_csv>``.

The dc89d1a9 failure mode is *deprecated* by the new contract — these
tests are now pinned to the Trial 12 cascade (per the
``gso-postmortem`` skill's "Trial 12 — Stage 1 input evidence contract"
section). The previous PR-A instrumentation invariants (``error_kind``,
``GSO_PLAN11_STAGE1_REQUEST_V1`` markers, on-disk error dumps) are no
longer reachable from the empty-card path; the test that asserted them
is now scoped to documenting the deprecation explicitly.

If a future change re-introduces the dc89d1a9 path (e.g. by relaxing
the input contract), these tests light up immediately and point at the
contract layer that regressed.
"""
from __future__ import annotations

import io
import json
import re
import time
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tests.integration.sm_tape_replay import (
    TapeReplayHarness,
    load_tape,
)


_TAPE_PATH = (
    Path(__file__).resolve().parents[1]
    / "fixtures" / "sm_tapes" / "dc89d1a9.jsonl"
)

# The five hard QIDs the production run dc89d1a9 admitted (from the
# postmortem's INPUT_PROJECTION_PARITY analysis).
_HARD_QIDS = (
    "gs_001",
    "gs_004",
    "gs_013",
    "gs_021",
    "gs_026",
)

# Trial 13i — a non-empty run-level ``schema_columns`` channel so the
# ``validate_schema_columns`` pre-flight (which fires *before* the
# per-card ``validate``) does not pre-empt this test's intent. These
# ``_hard_row`` fixtures carry deliberately empty evidence cards (no
# question / ground-truth / blame), so the contract layer under test is
# the *per-card* one — it must surface ``question_text_empty`` &c. in
# ``field_sources``. Without schema_columns the run-level gate
# short-circuits first with ``missing_schema_columns`` (whose marker
# carries only ``schema_columns`` in field_sources), masking the
# per-card contract this test pins. The value is an inert placeholder —
# the empty cards yield no resolvable blame seeds regardless.
_SCHEMA_COLUMNS_SNAPSHOT = {"schema_columns": ["main.schema.tkt.col"]}


def _hard_row(qid: str) -> dict:
    """Build a production-shape row that ``row_is_hard_failure`` admits.

    Mirrors the ``inputs/question_id`` slash-flattened key MLflow emits
    so the canonical-row-shape adapter routes via the same path the
    production run did.
    """
    return {
        "inputs/question_id": qid,
        # row_is_hard_failure requires result_correctness == "no" AND
        # arbiter ∉ correct verdicts. (See optimization/evaluation.py)
        "feedback/result_correctness/value": "no",
        "feedback/arbiter/value": "ground_truth_correct",
        "score": 0.0,
        "sql": "SELECT 1",
        "expected_shape": "SELECT count(*) FROM x",
        "eval_row_id": f"row_{qid}",
    }


@pytest.mark.integration
def test_dc89d1a9_replay_blocked_by_stage1_input_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Trial 12 cascade — the dc89d1a9 BadRequest replay must NOT reach
    the LLM. The Stage 1 input evidence contract pre-flight rejects
    the empty cards built from ``_hard_row``, so every admitted QID
    terminates with the typed contract violation instead of the legacy
    ``diagnose_returned_empty`` signature.

    Per the ``gso-postmortem`` skill: when
    ``GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1`` fires, classify the run
    as ``STAGE1_INPUT_CARD_EMPTY_*`` and do NOT classify as the
    deprecated ``PLAN11_STAGE1_EVIDENCE_HYDRATION_EMPTY``.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))
    tape = load_tape(_TAPE_PATH)
    harness = TapeReplayHarness(tape=tape)
    rows = [_hard_row(qid) for qid in _HARD_QIDS]

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
            run_id="dc89d1a9-replay",
            run_root=tmp_path,
            workspace_client=None,
            metadata_snapshot=_SCHEMA_COLUMNS_SNAPSHOT,
            forbidden_signatures=(),
        )
    elapsed = time.monotonic() - t0
    stdout = buf.getvalue()
    assert elapsed < 5.0, (
        f"Replay took {elapsed:.2f}s; the PR-B acceptance ceiling is 5s."
    )

    qids_seen = {s.qid for s in final_states}
    assert qids_seen == set(_HARD_QIDS), (
        f"SM admitted {qids_seen!r}, expected {set(_HARD_QIDS)!r}."
    )

    for s in final_states:
        assert s.deepest_stage_reached == FunnelStage.HARD_QID_SEEN, (
            f"{s.qid} reached {s.deepest_stage_reached!r}; expected "
            f"HARD_QID_SEEN — Trial 12 pre-flight should terminate at "
            f"Stage 1 input."
        )
        assert s.terminal is not None, f"{s.qid} not terminated"
        assert s.terminal.reason.startswith(
            "abstain: evidence_card_empty:"
        ), (
            f"{s.qid} terminal.reason={s.terminal.reason!r}; expected "
            f"the Trial 12 typed contract violation prefix "
            f"'abstain: evidence_card_empty:'. If this regresses, the "
            f"Stage 1 input evidence contract pre-flight is no longer "
            f"intercepting empty cards."
        )

    empty_markers = [
        json.loads(m.group(1))
        for m in re.finditer(
            r"GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1 (\{.+\})", stdout,
        )
    ]
    assert len(empty_markers) == len(_HARD_QIDS), (
        f"Expected {len(_HARD_QIDS)} "
        f"GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1 markers (one per hard "
        f"QID); got {len(empty_markers)}."
    )
    for marker in empty_markers:
        assert marker["qid"] in _HARD_QIDS, marker
        assert isinstance(marker["violations"], list), marker
        assert marker["violations"], (
            f"{marker['qid']} carries an empty violations list; "
            f"contract emission must be fail-loud about which fields "
            f"are missing."
        )
        assert isinstance(marker["field_sources"], dict), marker
        assert "question_text" in marker["field_sources"], marker

    assert harness.consumed_count == 0, (
        f"Replay consumed {harness.consumed_count} tape entries; "
        f"expected 0 because the input contract pre-flight should "
        f"short-circuit before any LLM call. If non-zero, the contract "
        f"layer is bypassed and the dc89d1a9 BadRequest path is "
        f"reachable again."
    )


@pytest.mark.integration
def test_dc89d1a9_replay_does_not_emit_legacy_pr_a_instrumentation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Trial 12 deprecation pin — the dc89d1a9 ``_hard_row`` no longer
    reaches the LLM, so the PR-A instrumentation surface
    (``GSO_PLAN11_STAGE1_DIAGNOSIS_V1`` with ``outcome=llm_error``,
    ``GSO_PLAN11_STAGE1_REQUEST_V1`` fingerprint, on-disk error dump)
    must be absent on this path.

    The PR-A instrumentation invariants are still exercised wherever a
    real LLM call fails — they are simply unreachable from a contract
    pre-flight rejection. This test pins that distinction so a future
    relaxation of the input contract surfaces here immediately.
    """
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))

    harness = TapeReplayHarness(tape=load_tape(_TAPE_PATH))
    rows = [_hard_row(qid) for qid in _HARD_QIDS]

    from genie_space_optimizer.optimization import optimizer as opt_mod

    buf = io.StringIO()
    with redirect_stdout(buf), harness.patch():
        opt_mod.run_state_machine_iteration_and_persist(
            eval_rows=rows,
            iteration=1,
            run_id="dc89d1a9-replay",
            run_root=tmp_path,
            workspace_client=None,
            metadata_snapshot=_SCHEMA_COLUMNS_SNAPSHOT,
            forbidden_signatures=(),
        )
    stdout = buf.getvalue()

    diag_payloads = [
        json.loads(m.group(1))
        for m in re.finditer(
            r"GSO_PLAN11_STAGE1_DIAGNOSIS_V1 (\{.+\})", stdout,
        )
    ]
    req_payloads = [
        json.loads(m.group(1))
        for m in re.finditer(
            r"GSO_PLAN11_STAGE1_REQUEST_V1 (\{.+\})", stdout,
        )
    ]

    llm_error_diags = [
        d for d in diag_payloads if d.get("outcome") == "llm_error"
    ]
    assert llm_error_diags == [], (
        f"Expected zero llm_error diagnosis markers (the input "
        f"contract pre-flight should short-circuit the LLM call); got "
        f"{len(llm_error_diags)}."
    )
    assert req_payloads == [], (
        f"Expected zero GSO_PLAN11_STAGE1_REQUEST_V1 markers on the "
        f"contract-rejected path; got {len(req_payloads)}."
    )
    assert harness.consumed_count == 0, (
        f"Tape was consumed {harness.consumed_count} times; expected "
        f"0 because the pre-flight should block the LLM call."
    )

    dumps = sorted((tmp_path / "llm_errors").glob("stage1_1_*.json"))
    assert dumps == [], (
        f"Expected zero disk dumps under {tmp_path / 'llm_errors'} on "
        f"the contract-rejected path; got "
        f"{[d.name for d in dumps]}."
    )

    empty_markers = [
        json.loads(m.group(1))
        for m in re.finditer(
            r"GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1 (\{.+\})", stdout,
        )
    ]
    assert empty_markers, (
        "Expected at least one GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1 "
        "marker proving the contract pre-flight fired."
    )
