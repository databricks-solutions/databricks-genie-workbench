"""Contract tests for the OfficialBenchmarkRunner eval-run seam."""

from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Any

import pytest

from genie_space_optimizer.optimization.genie_eval_taxonomy import (
    ASSESSMENT_REASON_CODES,
    GENIE_REASON_RATIONALES,
)
from genie_space_optimizer.optimization.eval_runner import (
    EvalRunError,
    EvalRunResult,
    EvalRunTimeoutError,
    LiftReport,
    OfficialBenchmarkRunner,
    lift_report,
    map_eval_detail_to_row,
)


def _row(
    question_id: str,
    assessment: str,
    *,
    reasons: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "question_id": question_id,
        "assessment": assessment,
        "assessment_reasons": list(reasons or []),
        "needs_review": assessment == "NEEDS_REVIEW",
    }


def _run_result(
    *,
    eval_run_id: str,
    rows: list[dict[str, Any]],
    status: str = "DONE",
) -> EvalRunResult:
    n_correct = sum(1 for r in rows if r["assessment"] == "GOOD")
    n_nr = sum(1 for r in rows if r["assessment"] == "NEEDS_REVIEW")
    return EvalRunResult(
        eval_run_id=eval_run_id,
        status=status,
        num_correct=n_correct,
        num_done=len(rows),
        num_needs_review=n_nr,
        num_questions=len(rows),
        rows=rows,
        wall_clock_seconds=1.0,
    )


class _FakeGenie:
    def __init__(
        self,
        *,
        statuses: list[str],
        details: list[Any] | None = None,
        eval_run_id: str = "er-1",
        list_pages: list[Any] | None = None,
    ) -> None:
        self.statuses = list(statuses)
        self.details = list(details or [])
        self.eval_run_id = eval_run_id
        self.create_calls: list[Any] = []
        self.get_calls = 0
        self.in_flight = 0
        self.max_in_flight = 0
        self._lock = threading.Lock()
        self.list_pages = list_pages
        self.list_calls: list[dict[str, Any]] = []

    def genie_create_eval_run(self, space_id: str, benchmark_question_ids=None):
        with self._lock:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
        self.create_calls.append(
            {"space_id": space_id, "benchmark_question_ids": benchmark_question_ids}
        )
        return SimpleNamespace(eval_run_id=self.eval_run_id)

    def genie_get_eval_run(self, space_id: str, eval_run_id: str):
        self.get_calls += 1
        status = self.statuses.pop(0) if self.statuses else "DONE"
        if status in {"DONE", "EVALUATION_CANCELLED", "EVALUATION_FAILED", "EVALUATION_TIMEOUT"}:
            with self._lock:
                self.in_flight = max(0, self.in_flight - 1)
        return SimpleNamespace(
            eval_run_id=eval_run_id,
            eval_run_status=status,
            num_correct=1,
            num_done=1,
            num_needs_review=0,
            num_questions=1,
        )

    def genie_list_eval_results(self, space_id, eval_run_id, page_size=None, page_token=None):
        summary = SimpleNamespace(
            result_id="r1",
            benchmark_question_id="q1",
            question="How many?",
        )
        return SimpleNamespace(eval_results=[summary], next_page_token=None)

    def genie_get_eval_result_details(self, space_id, eval_run_id, result_id):
        if self.details:
            return self.details[0]
        return SimpleNamespace(
            result_id=result_id,
            benchmark_question_id="q1",
            assessment="GOOD",
            assessment_reasons=[],
            actual_response=[SimpleNamespace(response="SELECT 1")],
            expected_response=[SimpleNamespace(response="SELECT 1")],
            manual_assessment=False,
        )

    def genie_list_eval_runs(self, space_id: str, *, page_size=None, page_token=None):
        self.list_calls.append(
            {"space_id": space_id, "page_size": page_size, "page_token": page_token}
        )
        if self.list_pages is not None:
            return self.list_pages.pop(0)
        return SimpleNamespace(
            eval_runs=[SimpleNamespace(eval_run_id="hist-1", eval_run_status="DONE")],
            next_page_token=None,
        )


def _runner(fake: _FakeGenie, **kwargs) -> OfficialBenchmarkRunner:
    clock = {"t": 0.0}

    def _clock() -> float:
        clock["t"] += 1.0
        return clock["t"]

    return OfficialBenchmarkRunner(
        SimpleNamespace(genie=fake),
        poll_interval_seconds=0.0,
        timeout_seconds=kwargs.pop("timeout_seconds", 100.0),
        page_size=10,
        clock=_clock,
        sleep=lambda _s: None,
        **kwargs,
    )


@pytest.mark.parametrize(
    "status",
    ["DONE", "EVALUATION_CANCELLED", "EVALUATION_FAILED", "EVALUATION_TIMEOUT"],
)
def test_run_handles_every_terminal_status(status: str) -> None:
    fake = _FakeGenie(statuses=["RUNNING", status])
    result = _runner(fake).run("space-1")
    assert result.status == status
    assert result.eval_run_id == "er-1"
    if status == "DONE":
        assert result.rows
        assert result.rows[0]["question_id"] == "q1"
        assert result.succeeded
    else:
        assert result.rows == []
        assert not result.succeeded
        assert not result.is_complete_success


def test_run_times_out_while_non_terminal() -> None:
    fake = _FakeGenie(statuses=["RUNNING", "RUNNING", "RUNNING"])
    with pytest.raises(EvalRunTimeoutError) as exc:
        _runner(fake, timeout_seconds=1.0).run("space-1")
    assert exc.value.eval_run_id == "er-1"
    assert exc.value.last_status == "RUNNING"


def test_run_subset_passes_question_ids_and_label() -> None:
    fake = _FakeGenie(statuses=["DONE"])
    result = _runner(fake).run_subset("space-1", ["q1", "q2"], "mv_lift")
    assert fake.create_calls[0]["benchmark_question_ids"] == ["q1", "q2"]
    assert result.eval_scope == "mv_lift"
    assert result.requested_question_ids == ("q1", "q2")


def test_run_subset_rejects_empty_question_ids() -> None:
    fake = _FakeGenie(statuses=["DONE"])
    with pytest.raises(EvalRunError):
        _runner(fake).run_subset("space-1", [], "mv_lift")


def test_run_subset_is_serialized_across_threads() -> None:
    fake = _FakeGenie(statuses=["RUNNING", "DONE", "RUNNING", "DONE"])
    runner = _runner(fake)
    errors: list[BaseException] = []

    def _go() -> None:
        try:
            runner.run_subset("space-1", ["q1"], "mv_lift")
        except BaseException as exc:  # noqa: BLE001 — collect for assertion
            errors.append(exc)

    threads = [threading.Thread(target=_go) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert fake.max_in_flight == 1
    assert len(fake.create_calls) == 2


@pytest.mark.parametrize("reason", sorted(ASSESSMENT_REASON_CODES))
def test_map_eval_detail_consumes_every_taxonomy_reason(reason: str) -> None:
    summary = SimpleNamespace(
        result_id="r1",
        benchmark_question_id="q-reason",
        question="why?",
    )
    detail = SimpleNamespace(
        result_id="r1",
        benchmark_question_id="q-reason",
        assessment="BAD",
        assessment_reasons=[reason],
        actual_response=[SimpleNamespace(response="SELECT 0")],
        expected_response=[SimpleNamespace(response="SELECT 1")],
        manual_assessment=False,
    )
    row = map_eval_detail_to_row(summary, detail)
    assert row["assessment_reasons"] == [reason]
    block = row["genie_equivalent_eval"]
    assert block["assessment_reasons"] == [reason]
    assert block["primary_assessment_reason"] == reason
    assert block["reason_rationales"][reason] == GENIE_REASON_RATIONALES[reason]


def test_lift_report_excludes_needs_review_from_numerator_and_denominator() -> None:
    pre = _run_result(
        eval_run_id="pre",
        rows=[
            _row("a", "BAD"),
            _row("b", "GOOD"),
            _row("c", "NEEDS_REVIEW"),
            _row("d", "GOOD"),
        ],
    )
    post = _run_result(
        eval_run_id="post",
        rows=[
            _row("a", "GOOD"),
            _row("b", "GOOD"),
            _row("c", "NEEDS_REVIEW"),
            _row("d", "GOOD"),
        ],
    )
    report = lift_report(pre, post, ["a", "c"])
    # subset {a,c}: c excluded → only a, BAD→GOOD ⇒ 0.0 → 1.0
    assert report.delta_affected == pytest.approx(1.0)
    # suite {a,b,d}: pre 2/3, post 3/3
    assert report.delta_suite == pytest.approx(1.0 / 3.0)
    assert report.needs_review_count == 1
    assert report.regressed_question_ids == []
    payload = report.to_dict()
    assert payload["delta_affected"] == report.delta_affected
    assert payload["delta_suite"] == report.delta_suite
    assert payload["regressed_question_ids"] == []
    assert payload["needs_review_count"] == 1
    assert payload["pre_eval_run_id"] == "pre"
    assert payload["post_eval_run_id"] == "post"


def test_lift_report_flags_good_to_bad_regressions() -> None:
    pre = _run_result(
        eval_run_id="pre",
        rows=[_row("a", "GOOD"), _row("b", "GOOD")],
    )
    post = _run_result(
        eval_run_id="post",
        rows=[_row("a", "BAD", reasons=["EMPTY_RESULT"]), _row("b", "GOOD")],
    )
    report = lift_report(pre, post, ["a", "b"])
    assert report.regressed_question_ids == ["a"]
    assert report.delta_affected < 0
    assert report.delta_suite < 0


def test_lift_report_rejects_non_done_runs() -> None:
    pre = _run_result(eval_run_id="pre", rows=[_row("a", "GOOD")], status="DONE")
    post = _run_result(
        eval_run_id="post",
        rows=[],
        status="EVALUATION_FAILED",
    )
    post.num_questions = 1
    with pytest.raises(EvalRunError):
        lift_report(pre, post, ["a"])


def test_lift_report_to_dict_is_json_shaped() -> None:
    pre = _run_result(eval_run_id="pre", rows=[_row("a", "GOOD")])
    post = _run_result(eval_run_id="post", rows=[_row("a", "GOOD")])
    payload = lift_report(pre, post, ["a"]).to_dict()
    assert set(payload) >= {
        "delta_affected",
        "delta_suite",
        "regressed_question_ids",
        "needs_review_count",
        "pre_eval_run_id",
        "post_eval_run_id",
        "question_subset",
        "pre_accuracy_affected",
        "post_accuracy_affected",
        "pre_accuracy_suite",
        "post_accuracy_suite",
        "needs_review_question_ids",
        "graded_affected_count",
        "graded_suite_count",
    }
    assert isinstance(LiftReport(**{k: payload[k] for k in LiftReport.__dataclass_fields__}), LiftReport)


def test_list_eval_runs_paginates() -> None:
    fake = _FakeGenie(
        statuses=["DONE"],
        list_pages=[
            SimpleNamespace(
                eval_runs=[SimpleNamespace(eval_run_id="h1")],
                next_page_token="p2",
            ),
            SimpleNamespace(
                eval_runs=[SimpleNamespace(eval_run_id="h2")],
                next_page_token=None,
            ),
        ],
    )
    runs = _runner(fake).list_eval_runs("space-1")
    assert [r.eval_run_id for r in runs] == ["h1", "h2"]
    assert fake.list_calls[0]["page_token"] is None
    assert fake.list_calls[1]["page_token"] == "p2"


def test_existing_run_signature_unchanged() -> None:
    import inspect

    sig = inspect.signature(OfficialBenchmarkRunner.run)
    assert list(sig.parameters) == ["self", "space_id", "benchmark_question_ids", "eval_scope"]
    assert sig.parameters["benchmark_question_ids"].default is None
    assert sig.parameters["eval_scope"].kind is inspect.Parameter.KEYWORD_ONLY
