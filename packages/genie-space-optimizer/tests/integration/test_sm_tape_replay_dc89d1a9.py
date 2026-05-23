"""PR-B acceptance test — SM tape replay of dc89d1a9 BadRequest cascade.

This test is the unit-speed counterpart of the 45-minute lever-loop
trial that produced postmortem ``dc89d1a9``. It feeds the same hard
QID set the production run admitted through the state machine, with
``LlmReasoningCall.invoke`` patched to replay the recorded
``BadRequestError`` exceptions from the dc89d1a9 tape, and asserts:

  1. Every admitted QID terminates with declined reason
     ``diagnose_returned_empty`` (the production failure signature).
  2. The PR-A diagnostic instrumentation fires on the replay path —
     each ``GSO_PLAN11_STAGE1_DIAGNOSIS_V1`` marker now carries an
     ``error_kind`` that pinpoints ``response_format_invalid``
     (instead of the ``unknown`` the production trial emitted).
  3. The entire replay finishes well under 5 seconds without any
     Databricks network round-trip.

Acceptance criterion (per the PR-B plan): ``pytest`` reproduces
``diagnose_returned_empty`` in < 5s offline.

If this test ever regresses, the next debug cycle starts here — not at
deploying a new wheel.
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
def test_dc89d1a9_replay_reproduces_diagnose_returned_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drive one SM iteration with the dc89d1a9 tape and assert every
    admitted QID lands on ``diagnose_returned_empty``."""
    monkeypatch.setenv("GSO_PHASE_H_BUNDLE_ROOT", str(tmp_path))
    tape = load_tape(_TAPE_PATH)
    assert len(tape) == len(_HARD_QIDS), (
        f"dc89d1a9 tape carries {len(tape)} entries but the test feeds "
        f"{len(_HARD_QIDS)} hard QIDs — keep them aligned so the cursor "
        f"never under-runs and surfaces the failure deterministically."
    )

    harness = TapeReplayHarness(tape=tape)
    rows = [_hard_row(qid) for qid in _HARD_QIDS]

    from genie_space_optimizer.optimization import optimizer as opt_mod
    from genie_space_optimizer.optimization.state_machine.funnel import (
        FunnelStage,
    )

    t0 = time.monotonic()
    with harness.patch():
        final_states = opt_mod.run_state_machine_iteration_and_persist(
            eval_rows=rows,
            iteration=1,
            run_id="dc89d1a9-replay",
            run_root=tmp_path,
            workspace_client=None,
            forbidden_signatures=(),
        )
    elapsed = time.monotonic() - t0
    assert elapsed < 5.0, (
        f"Replay took {elapsed:.2f}s; the PR-B acceptance ceiling is "
        f"5s. If this regresses the replay has fallen back to a real "
        f"Databricks call (or the SM is doing real work it should be "
        f"mocking)."
    )

    # Invariant 1 — every QID was admitted into the SM.
    qids_seen = {s.qid for s in final_states}
    assert qids_seen == set(_HARD_QIDS), (
        f"SM admitted {qids_seen!r}, expected {set(_HARD_QIDS)!r}. "
        f"If this regresses the canonical-row-shape adapter has "
        f"drifted again."
    )

    # Invariant 2 — every QID terminated at HARD_QID_SEEN (the SM
    # never escaped Stage 1 because diagnose_failing_qids returned
    # []). This is the exact production failure signature dc89d1a9
    # recorded.
    for s in final_states:
        assert s.deepest_stage_reached == FunnelStage.HARD_QID_SEEN, (
            f"{s.qid} reached {s.deepest_stage_reached!r}; expected "
            f"HARD_QID_SEEN — the LLM call must error out before any "
            f"transformer escapes Stage 1, otherwise the replay "
            f"drifted from dc89d1a9."
        )
        # Invariant 2b — terminal record names diagnose_returned_empty,
        # the exact failure signature the dc89d1a9 postmortem recorded.
        assert s.terminal is not None, f"{s.qid} not terminated"
        assert s.terminal.reason == "abstain: diagnose_returned_empty", (
            f"{s.qid} terminal.reason={s.terminal.reason!r}; expected "
            f"'abstain: diagnose_returned_empty'."
        )

    # Invariant 3 — tape was fully consumed (one entry per QID).
    assert harness.consumed_count == len(_HARD_QIDS), (
        f"Replay consumed {harness.consumed_count}/{len(_HARD_QIDS)} "
        f"tape entries; unconsumed={harness.unconsumed()!r}."
    )


@pytest.mark.integration
def test_dc89d1a9_replay_surfaces_pr_a_instrumentation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The replay exercises the PR-A marker emission code paths.

    On the production trial every Stage 1 marker carried
    ``error_kind="unknown"`` and no ``error_message``. With PR-A in
    place the same replay must now expose:

      * ``error_kind == "response_format_invalid"`` (the dc89d1a9
        BadRequest body explicitly references ``response_format`` and
        ``json_schema``).
      * a non-empty ``error_message`` truncated to ≤500 chars on the
        marker.
      * one ``GSO_PLAN11_STAGE1_REQUEST_V1`` request-fingerprint
        marker per failing QID.
      * one on-disk dump at ``{tmp_path}/llm_errors/stage1_1_<qid>.json``
        containing the full untruncated body.

    Together these prove the diagnostic instrumentation reaches the
    marker even when the LLM call route is a tape replay.
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

    # Filter to llm_error outcomes (one per QID).
    llm_error_diags = [d for d in diag_payloads if d["outcome"] == "llm_error"]
    assert len(llm_error_diags) == len(_HARD_QIDS), (
        f"Expected one llm_error diagnosis marker per hard QID; got "
        f"{len(llm_error_diags)} out of {len(diag_payloads)} total."
    )

    for payload in llm_error_diags:
        assert payload["exception_class"] == "BadRequestError", payload
        assert payload["error_kind"] == "response_format_invalid", (
            f"{payload['qid']} error_kind={payload['error_kind']!r}; "
            f"expected response_format_invalid (the tape body explicitly "
            f"references response_format and json_schema)."
        )
        assert "response_format" in payload["error_message"], payload
        assert payload["endpoint"], payload
        # Truncation invariant.
        assert len(payload["error_message"]) <= 500, payload

    # Invariant: one request fingerprint per failing QID.
    assert len(req_payloads) == len(_HARD_QIDS), (
        f"Expected one GSO_PLAN11_STAGE1_REQUEST_V1 marker per hard "
        f"QID; got {len(req_payloads)}."
    )
    for payload in req_payloads:
        assert payload["skill_id"] == "plan11_diagnose"
        assert payload["max_tokens"] > 0
        assert payload["system_msg_chars"] > 0
        assert payload["user_prompt_chars"] > 0
        assert any("json_schema" in k for k in payload["response_format_keywords"])

    # Invariant: full untruncated body persisted to disk.
    dumps = sorted((tmp_path / "llm_errors").glob("stage1_1_*.json"))
    assert len(dumps) == len(_HARD_QIDS), (
        f"Expected {len(_HARD_QIDS)} disk dumps under "
        f"{tmp_path / 'llm_errors'}; got {len(dumps)} ({[d.name for d in dumps]})."
    )
    for dump_path in dumps:
        dump = json.loads(dump_path.read_text())
        assert dump["skill_id"] == "plan11_diagnose"
        assert "response_format" in dump["error_message"]
        assert dump["endpoint"]
