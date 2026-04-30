from __future__ import annotations

import json
import logging

from genie_space_optimizer.optimization.eval_progress import (
    EvalProgressLogger,
    eval_debug_row_cap,
    eval_force_sequential,
    slice_eval_records_for_debug,
)


def test_eval_progress_logger_emits_json_payload(caplog) -> None:
    logger = logging.getLogger("gso-test-progress")
    progress = EvalProgressLogger(logger=logger, run_id="run-1", eval_scope="full")

    with caplog.at_level(logging.INFO, logger="gso-test-progress"):
        progress.emit(
            "predict_start",
            question_id="q1",
            row_index=1,
            row_count=14,
            question="What is revenue?",
        )

    assert len(caplog.records) == 1
    assert caplog.records[0].message.startswith("GSO_EVAL_PROGRESS ")
    payload = json.loads(caplog.records[0].message.removeprefix("GSO_EVAL_PROGRESS "))
    assert payload["phase"] == "predict_start"
    assert payload["run_id"] == "run-1"
    assert payload["eval_scope"] == "full"
    assert payload["question_id"] == "q1"
    assert payload["row_index"] == 1
    assert payload["row_count"] == 14
    assert payload["question_prefix"] == "What is revenue?"


def test_eval_progress_phase_context_emits_done_event(caplog) -> None:
    logger = logging.getLogger("gso-test-progress-context")
    progress = EvalProgressLogger(
        logger=logger,
        run_id="run-1",
        eval_scope="full",
        clock=lambda: 100.0,
    )
    times = iter([100.0, 103.25])
    progress._clock = lambda: next(times)  # type: ignore[attr-defined]

    with caplog.at_level(logging.INFO, logger="gso-test-progress-context"):
        with progress.phase("gt_execute", question_id="q2"):
            pass

    messages = [r.message for r in caplog.records]
    assert len(messages) == 2
    start_payload = json.loads(messages[0].removeprefix("GSO_EVAL_PROGRESS "))
    done_payload = json.loads(messages[1].removeprefix("GSO_EVAL_PROGRESS "))
    assert start_payload["phase"] == "gt_execute_start"
    assert done_payload["phase"] == "gt_execute_done"
    assert done_payload["elapsed_seconds"] == 3.25


def test_debug_env_parsing(monkeypatch) -> None:
    monkeypatch.setenv("GENIE_SPACE_OPTIMIZER_EVAL_MAX_ROWS_FOR_DEBUG", "3")
    monkeypatch.setenv("GENIE_SPACE_OPTIMIZER_EVAL_FORCE_SEQUENTIAL", "true")

    assert eval_debug_row_cap() == 3
    assert eval_force_sequential() is True


def test_slice_eval_records_for_debug_caps_rows(monkeypatch) -> None:
    monkeypatch.setenv("GENIE_SPACE_OPTIMIZER_EVAL_MAX_ROWS_FOR_DEBUG", "2")
    records = [{"id": "q1"}, {"id": "q2"}, {"id": "q3"}]

    assert slice_eval_records_for_debug(records) == [{"id": "q1"}, {"id": "q2"}]


def test_slice_eval_records_for_debug_noops_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("GENIE_SPACE_OPTIMIZER_EVAL_MAX_ROWS_FOR_DEBUG", raising=False)
    records = [{"id": "q1"}, {"id": "q2"}]

    assert slice_eval_records_for_debug(records) is records


def test_build_eval_heartbeat_detail_is_small_and_actionable() -> None:
    from genie_space_optimizer.optimization.eval_progress import build_eval_heartbeat_detail

    detail = build_eval_heartbeat_detail(
        phase="gt_execute_start",
        question_id="q1",
        row_index=1,
        row_count=14,
        conversation_id="c1",
        statement_id="stmt1",
        elapsed_seconds=12.34,
    )

    assert detail == {
        "phase": "gt_execute_start",
        "question_id": "q1",
        "row_index": 1,
        "row_count": 14,
        "conversation_id": "c1",
        "statement_id": "stmt1",
        "elapsed_seconds": 12.34,
    }
