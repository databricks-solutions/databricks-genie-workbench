"""Unit tests for the official Benchmark Eval-Run API seam (GSO v2 Phase 1).

Covers the poll loop, subset passing, paginated result collection, result→row
mapping, the legacy-output mapper, and the feature-switch activation guard. The
SDK is faked (no live workspace), and the clock/sleep are injected so the poll
loop is deterministic.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from genie_space_optimizer.common import config
from genie_space_optimizer.optimization import eval_runner
from genie_space_optimizer.optimization.eval_runner import (
    EVAL_SOURCE,
    EvalRunResult,
    EvalRunTimeoutError,
    OfficialBenchmarkRunner,
    build_eval_output_from_official,
    map_eval_detail_to_row,
)


# ── SDK fakes ──────────────────────────────────────────────────────────────
def _status(value: str) -> SimpleNamespace:
    """Mimic an ``EvaluationStatusType`` enum member (has ``.value``)."""
    return SimpleNamespace(value=value)


def _reason(value: str) -> SimpleNamespace:
    return SimpleNamespace(value=value)


def _summary(status, *, correct=0, done=0, needs=0, total=0):
    return SimpleNamespace(
        eval_run_id="er-1",
        eval_run_status=status,
        num_correct=correct,
        num_done=done,
        num_needs_review=needs,
        num_questions=total,
    )


def _result_row(result_id: str, qid: str, question: str):
    return SimpleNamespace(
        result_id=result_id,
        space_id="space-1",
        benchmark_question_id=qid,
        question=question,
        benchmark_answer="SELECT gt",
        status=_status("DONE"),
    )


def _detail(result_id, qid, assessment, reasons, actual="SELECT a", expected="SELECT e"):
    return SimpleNamespace(
        result_id=result_id,
        space_id="space-1",
        benchmark_question_id=qid,
        assessment=_status(assessment),
        assessment_reasons=[_reason(r) for r in reasons],
        actual_response=[SimpleNamespace(response=actual, response_type=_status("SQL"))],
        expected_response=[SimpleNamespace(response=expected, response_type=_status("SQL"))],
        manual_assessment=False,
        eval_run_status=_status("DONE"),
    )


class FakeGenie:
    """Configurable fake of the SDK ``GenieAPI`` eval-run surface."""

    def __init__(self, *, statuses, pages, details, summary):
        self._statuses = list(statuses)  # returned successively by get_eval_run
        self._pages = pages              # list of (eval_results, next_page_token)
        self._details = details          # {result_id: detail}
        self._final_summary = summary
        self.create_calls: list = []
        self.get_run_calls = 0
        self.list_calls: list = []
        self.detail_calls: list = []

    def genie_create_eval_run(self, space_id, *, benchmark_question_ids=None):
        self.create_calls.append((space_id, benchmark_question_ids))
        return SimpleNamespace(eval_run_id="er-1", eval_run_status=_status("NOT_STARTED"))

    def genie_get_eval_run(self, space_id, eval_run_id):
        self.get_run_calls += 1
        if self._statuses:
            return self._statuses.pop(0)
        return self._final_summary

    def genie_list_eval_results(self, space_id, eval_run_id, *, page_size=None, page_token=None):
        self.list_calls.append((page_token, page_size))
        idx = 0 if page_token is None else int(page_token)
        results, next_token = self._pages[idx]
        return SimpleNamespace(eval_results=results, next_page_token=next_token)

    def genie_get_eval_result_details(self, space_id, eval_run_id, result_id):
        self.detail_calls.append(result_id)
        return self._details[result_id]


class FakeClock:
    """Monotonic clock that advances a fixed step each time it is sleep()ed."""

    def __init__(self, step=10.0):
        self.t = 0.0
        self.step = step

    def __call__(self):
        return self.t

    def sleep(self, seconds):
        self.t += seconds


def _two_good_one_bad():
    """Build a fake genie with 3 questions: 2 GOOD, 1 BAD (single page)."""
    rows = [
        _result_row("r1", "q1", "How many orders?"),
        _result_row("r2", "q2", "Revenue by region?"),
        _result_row("r3", "q3", "Top customers?"),
    ]
    details = {
        "r1": _detail("r1", "q1", "GOOD", []),
        "r2": _detail("r2", "q2", "GOOD", []),
        "r3": _detail("r3", "q3", "BAD", ["LLM_JUDGE_WRONG_COLUMNS", "RESULT_MISSING_ROWS"]),
    }
    summary = _summary(_status("DONE"), correct=2, done=3, needs=0, total=3)
    return FakeGenie(
        statuses=[_summary(_status("RUNNING"), total=3), summary],
        pages=[(rows, None)],
        details=details,
        summary=summary,
    )


def _make_runner(genie, *, clock=None, **kw):
    w = SimpleNamespace(genie=genie)
    clock = clock or FakeClock()
    return OfficialBenchmarkRunner(
        w,
        poll_interval_seconds=kw.pop("poll_interval_seconds", 5),
        timeout_seconds=kw.pop("timeout_seconds", 600),
        page_size=kw.pop("page_size", 100),
        clock=clock,
        sleep=clock.sleep,
        **kw,
    )


# ── poll loop ───────────────────────────────────────────────────────────────
def test_run_polls_until_terminal_done() -> None:
    genie = _two_good_one_bad()
    runner = _make_runner(genie)
    result = runner.run("space-1", eval_scope="full")

    # RUNNING then DONE ⇒ two get_eval_run calls.
    assert genie.get_run_calls == 2
    assert result.status == "DONE"
    assert result.succeeded is True


def test_run_times_out_when_never_terminal() -> None:
    summary = _summary(_status("DONE"), correct=1, total=1)
    genie = FakeGenie(
        statuses=[_summary(_status("RUNNING"), total=1)] * 100,
        pages=[([], None)],
        details={},
        summary=summary,
    )
    clock = FakeClock()
    runner = _make_runner(genie, clock=clock, poll_interval_seconds=10, timeout_seconds=30)
    with pytest.raises(EvalRunTimeoutError) as exc:
        runner.run("space-1")
    assert exc.value.last_status == "RUNNING"
    assert exc.value.timeout_seconds == 30


def test_non_success_terminal_returns_empty_rows() -> None:
    summary = _summary(_status("EVALUATION_FAILED"), correct=0, done=0, total=3)
    genie = FakeGenie(
        statuses=[summary],
        pages=[([], None)],
        details={},
        summary=summary,
    )
    runner = _make_runner(genie)
    result = runner.run("space-1")
    assert result.status == "EVALUATION_FAILED"
    assert result.succeeded is False
    assert result.rows == []


# ── subset passing ───────────────────────────────────────────────────────────
def test_subset_question_ids_are_passed_through() -> None:
    genie = _two_good_one_bad()
    runner = _make_runner(genie)
    runner.run("space-1", ["q1", "q3"], eval_scope="slice")
    assert genie.create_calls == [("space-1", ["q1", "q3"])]


def test_none_question_ids_runs_all() -> None:
    genie = _two_good_one_bad()
    runner = _make_runner(genie)
    runner.run("space-1", None, eval_scope="full")
    # None must be forwarded verbatim (server omits the field ⇒ runs all).
    assert genie.create_calls == [("space-1", None)]


# ── result mapping + accuracy ────────────────────────────────────────────────
def test_result_mapping_and_accuracy() -> None:
    genie = _two_good_one_bad()
    runner = _make_runner(genie)
    result = runner.run("space-1", eval_scope="full")

    assert result.num_correct == 2
    assert result.num_questions == 3
    assert result.accuracy == pytest.approx(66.67, abs=0.01)
    assert result.accuracy_fraction == pytest.approx(2 / 3)
    assert sorted(result.failure_question_ids) == ["q3"]
    assert len(result.rows) == 3

    by_qid = {r["question_id"]: r for r in result.rows}
    assert by_qid["q1"]["result_correctness/value"] == "yes"
    assert by_qid["q1"]["feedback/result_correctness/value"] == "yes"
    assert by_qid["q1"]["assessment"] == "GOOD"
    assert by_qid["q3"]["result_correctness/value"] == "no"
    assert by_qid["q3"]["assessment"] == "BAD"
    assert by_qid["q3"]["assessment_reasons"] == [
        "LLM_JUDGE_WRONG_COLUMNS",
        "RESULT_MISSING_ROWS",
    ]
    assert by_qid["q3"]["_eval_source"] == EVAL_SOURCE
    assert by_qid["q3"]["response"]["response"] == "SELECT a"
    assert by_qid["q3"]["expectations"]["expected_response"] == "SELECT e"


def test_pagination_collects_all_results() -> None:
    rows_p1 = [_result_row("r1", "q1", "Q1")]
    rows_p2 = [_result_row("r2", "q2", "Q2")]
    details = {
        "r1": _detail("r1", "q1", "GOOD", []),
        "r2": _detail("r2", "q2", "BAD", ["LLM_JUDGE_OTHER"]),
    }
    summary = _summary(_status("DONE"), correct=1, done=2, total=2)
    genie = FakeGenie(
        statuses=[summary],
        pages=[(rows_p1, "1"), (rows_p2, None)],
        details=details,
        summary=summary,
    )
    runner = _make_runner(genie)
    result = runner.run("space-1")
    assert len(result.rows) == 2
    assert {r["question_id"] for r in result.rows} == {"q1", "q2"}
    # Two list calls: page 0 (token=None) then page 1 (token="1").
    assert [c[0] for c in genie.list_calls] == [None, "1"]


# ── map_eval_detail_to_row unit ──────────────────────────────────────────────
@pytest.mark.parametrize(
    "assessment,expected_rc,needs_review",
    [
        ("GOOD", "yes", False),
        ("BAD", "no", False),
        ("NEEDS_REVIEW", "no", True),
    ],
)
def test_map_row_verdicts(assessment, expected_rc, needs_review) -> None:
    summary = _result_row("rX", "qX", "question text")
    detail = _detail("rX", "qX", assessment, ["LLM_JUDGE_SEMANTIC_ERROR"])
    row = map_eval_detail_to_row(summary, detail)
    assert row["result_correctness/value"] == expected_rc
    assert row["needs_review"] is needs_review
    assert row["assessment"] == assessment
    assert row["question"] == "question text"
    block = row["genie_equivalent_eval"]
    assert block["assessment"] == assessment
    assert block["mapped_from"]["source"] == EVAL_SOURCE
    if assessment == "GOOD":
        # GOOD rows still attach the source block; reasons may be empty.
        assert block["assessment_reasons"] == ["LLM_JUDGE_SEMANTIC_ERROR"]


# ── legacy-output mapper ─────────────────────────────────────────────────────
def test_build_eval_output_from_official_contract() -> None:
    result = EvalRunResult(
        eval_run_id="er-9",
        status="DONE",
        num_correct=3,
        num_done=4,
        num_needs_review=1,
        num_questions=4,
        rows=[
            {"question_id": "q1", "assessment": "GOOD"},
            {"question_id": "q2", "assessment": "BAD"},
            {"question_id": "q3", "assessment": "GOOD"},
            {"question_id": "q4", "assessment": "NEEDS_REVIEW"},
        ],
        wall_clock_seconds=12.0,
        eval_scope="full",
    )
    out = build_eval_output_from_official(result, iteration=2, eval_scope="full", model_id="m-1")

    assert out["overall_accuracy"] == 75.0
    assert out["pre_arbiter_accuracy"] == 75.0
    assert out["total_questions"] == 4
    assert out["correct_count"] == 3
    assert out["evaluated_count"] == 4
    assert out["scores"]["result_correctness"] == 75.0
    assert out["per_judge"]["result_correctness"] == pytest.approx(0.75)
    assert sorted(out["failure_question_ids"]) == ["q2", "q4"]
    # Native official fields are present (Phase 6 UI consumes these).
    assert out["eval_run_id"] == "er-9"
    assert out["eval_run_status"] == "DONE"
    assert out["num_needs_review"] == 1
    assert out["num_done"] == 4
    assert out["_eval_source"] == EVAL_SOURCE
    assert out["model_id"] == "m-1"
    assert isinstance(out["thresholds_met"], bool)


# ── feature-switch activation guard ──────────────────────────────────────────
def test_maybe_build_runner_off_when_flag_false(monkeypatch) -> None:
    monkeypatch.setattr(config, "USE_OFFICIAL_BENCHMARK_RUNNER", False)
    assert eval_runner.maybe_build_official_runner(SimpleNamespace(genie=object())) is None


def test_maybe_build_runner_off_for_non_workspace_client(monkeypatch) -> None:
    monkeypatch.setattr(config, "USE_OFFICIAL_BENCHMARK_RUNNER", True)
    # A MagicMock-like object is not a real WorkspaceClient ⇒ legacy path.
    assert eval_runner.maybe_build_official_runner(SimpleNamespace(genie=object())) is None


def test_maybe_build_runner_on_for_real_client(monkeypatch) -> None:
    import databricks.sdk as sdk

    class FakeWC:
        def __init__(self):
            self.genie = _two_good_one_bad()

    monkeypatch.setattr(config, "USE_OFFICIAL_BENCHMARK_RUNNER", True)
    monkeypatch.setattr(sdk, "WorkspaceClient", FakeWC)
    runner = eval_runner.maybe_build_official_runner(FakeWC())
    assert isinstance(runner, OfficialBenchmarkRunner)


# ── space-side qid resolution ────────────────────────────────────────────────
def test_resolve_qids_prefers_explicit_space_id() -> None:
    benchmarks = [
        {"id": "g1", "question": "Q1", "space_question_id": "s1"},
        {"id": "g2", "question": "Q2", "genie_question_id": "s2"},
    ]
    # No workspace read needed when every row carries an explicit id.
    resolved = eval_runner.resolve_space_benchmark_qids(object(), "space-1", benchmarks)
    assert resolved == ["s1", "s2"]


def test_resolve_qids_text_matches_space_config(monkeypatch) -> None:
    benchmarks = [{"id": "g1", "question": "How many orders?"}]

    def _fake_text_map(w, space_id):
        from genie_space_optimizer.common.genie_client import _normalize_question_text

        return {_normalize_question_text("How many orders?"): "space-qid-7"}

    monkeypatch.setattr(eval_runner, "_space_question_text_to_id", _fake_text_map)
    resolved = eval_runner.resolve_space_benchmark_qids(object(), "space-1", benchmarks)
    assert resolved == ["space-qid-7"]


def test_resolve_qids_returns_none_when_unresolvable(monkeypatch) -> None:
    monkeypatch.setattr(eval_runner, "_space_question_text_to_id", lambda w, s: {})
    assert eval_runner.resolve_space_benchmark_qids(object(), "space-1", [{"id": "g1", "question": "X"}]) is None
    assert eval_runner.resolve_space_benchmark_qids(object(), "space-1", []) is None
