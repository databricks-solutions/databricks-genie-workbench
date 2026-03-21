"""Unit tests for cross-iteration arbiter correction pipeline."""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.harness import (
    VerdictEntry,
    _build_verdict_history,
    _extract_confirmed_corrections,
    _extract_neither_correct_repair_candidates,
    _get_arbiter_verdict,
    _get_question_id,
    _should_quarantine,
)
from genie_space_optimizer.optimization.benchmarks import _CORRECTABLE_VERDICTS
from genie_space_optimizer.optimization.evaluation import (
    _compute_arbiter_adjusted_accuracy,
)


# ── Helpers ──────────────────────────────────────────────────────────────


def _make_row(qid: str, verdict: str, *, genie_sql: str = "SELECT 1", question: str = "q?", expected_sql: str = "SELECT 2", rationale: str = ""):
    """Build a minimal evaluation row dict."""
    return {
        "inputs/question_id": qid,
        "inputs/question": question,
        "inputs/expected_response": expected_sql,
        "outputs/response": genie_sql,
        "arbiter/value": verdict,
        "arbiter/rationale": rationale,
        "result_correctness/value": "no",
    }


def _make_iteration(iteration: int, rows: list[dict]) -> dict:
    return {
        "iteration": iteration,
        "eval_scope": "full",
        "rows_json": rows,
    }


class _FakeSparkSession:
    """Minimal fake for load_all_full_iterations without Delta."""

    def __init__(self, iterations: list[dict]):
        self._iterations = iterations

    def sql(self, _query: str):
        raise NotImplementedError("fake spark")


# ── Tests: verdict extraction helpers ────────────────────────────────────


class TestVerdictHelpers:
    def test_get_arbiter_verdict_from_primary_key(self):
        assert _get_arbiter_verdict({"arbiter/value": "genie_correct"}) == "genie_correct"

    def test_get_arbiter_verdict_from_feedback_key(self):
        assert _get_arbiter_verdict({"feedback/arbiter/value": "neither_correct"}) == "neither_correct"

    def test_get_arbiter_verdict_defaults_to_skipped(self):
        assert _get_arbiter_verdict({}) == "skipped"

    def test_get_question_id_from_inputs(self):
        assert _get_question_id({"inputs/question_id": "q42"}) == "q42"

    def test_get_question_id_fallback(self):
        assert _get_question_id({"question_id": "q99"}) == "q99"

    def test_get_question_id_missing(self):
        assert _get_question_id({}) == "?"


# ── Tests: _build_verdict_history ────────────────────────────────────────


class TestBuildVerdictHistory:
    def test_builds_history_across_iterations(self, monkeypatch):
        iter0_rows = [
            _make_row("q1", "both_correct"),
            _make_row("q2", "genie_correct", genie_sql="SELECT a FROM t"),
        ]
        iter1_rows = [
            _make_row("q1", "both_correct"),
            _make_row("q2", "genie_correct", genie_sql="SELECT b FROM t"),
        ]

        fake_iterations = [
            _make_iteration(0, iter0_rows),
            _make_iteration(1, iter1_rows),
        ]

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.harness.load_all_full_iterations",
            lambda *a, **kw: fake_iterations,
        )

        history = _build_verdict_history(None, "run1", "cat", "sch")

        assert "q1" in history
        assert "q2" in history
        assert len(history["q2"]) == 2
        assert all(e.verdict == "genie_correct" for e in history["q2"])
        assert history["q2"][0].genie_sql == "SELECT a FROM t"
        assert history["q2"][1].genie_sql == "SELECT b FROM t"

    def test_empty_iterations(self, monkeypatch):
        monkeypatch.setattr(
            "genie_space_optimizer.optimization.harness.load_all_full_iterations",
            lambda *a, **kw: [],
        )
        assert _build_verdict_history(None, "run1", "cat", "sch") == {}


# ── Tests: _extract_confirmed_corrections ────────────────────────────────


class TestExtractConfirmedCorrections:
    def test_confirms_after_two_independent_genie_correct(self, monkeypatch):
        iter0_rows = [_make_row("q2", "genie_correct", genie_sql="SELECT a", question="Weekly trends?")]
        iter1_rows = [_make_row("q2", "genie_correct", genie_sql="SELECT b", question="Weekly trends?")]

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.harness.load_all_full_iterations",
            lambda *a, **kw: [_make_iteration(0, iter0_rows), _make_iteration(1, iter1_rows)],
        )

        actions = _extract_confirmed_corrections(None, "run1", "cat", "sch")
        assert len(actions) == 1
        assert actions[0]["verdict"] == "genie_correct"
        assert actions[0]["new_expected_sql"] == "SELECT b"
        assert actions[0]["confirmation_count"] == 2

    def test_single_occurrence_not_confirmed(self, monkeypatch):
        iter0_rows = [_make_row("q2", "genie_correct", genie_sql="SELECT a")]

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.harness.load_all_full_iterations",
            lambda *a, **kw: [_make_iteration(0, iter0_rows)],
        )

        actions = _extract_confirmed_corrections(None, "run1", "cat", "sch")
        assert len(actions) == 0

    def test_skips_already_corrected(self, monkeypatch):
        iter0_rows = [_make_row("q2", "genie_correct", genie_sql="SELECT a")]
        iter1_rows = [_make_row("q2", "genie_correct", genie_sql="SELECT b")]

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.harness.load_all_full_iterations",
            lambda *a, **kw: [_make_iteration(0, iter0_rows), _make_iteration(1, iter1_rows)],
        )

        actions = _extract_confirmed_corrections(
            None, "run1", "cat", "sch",
            already_corrected={"q2"},
        )
        assert len(actions) == 0

    def test_same_iteration_twice_counts_as_one(self, monkeypatch):
        """Two genie_correct in the same iteration = 1 independent observation."""
        iter0_rows = [
            _make_row("q2", "genie_correct", genie_sql="SELECT a"),
            _make_row("q2", "genie_correct", genie_sql="SELECT a"),
        ]

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.harness.load_all_full_iterations",
            lambda *a, **kw: [_make_iteration(0, iter0_rows)],
        )

        actions = _extract_confirmed_corrections(None, "run1", "cat", "sch")
        assert len(actions) == 0


# ── Tests: _extract_neither_correct_repair_candidates ────────────────────


class TestNeitherCorrectCandidates:
    def test_identifies_candidates_after_threshold(self, monkeypatch):
        iter0_rows = [_make_row("q16", "neither_correct", rationale="Both wrong")]
        iter1_rows = [_make_row("q16", "neither_correct", rationale="Still both wrong")]

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.harness.load_all_full_iterations",
            lambda *a, **kw: [_make_iteration(0, iter0_rows), _make_iteration(1, iter1_rows)],
        )

        candidates = _extract_neither_correct_repair_candidates(None, "run1", "cat", "sch")
        assert len(candidates) == 1
        assert candidates[0]["question_id"] == "q16"
        assert candidates[0]["nc_count"] == 2
        assert "Both wrong" in candidates[0]["rationale"]

    def test_single_neither_correct_not_a_candidate(self, monkeypatch):
        iter0_rows = [_make_row("q16", "neither_correct")]

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.harness.load_all_full_iterations",
            lambda *a, **kw: [_make_iteration(0, iter0_rows)],
        )

        candidates = _extract_neither_correct_repair_candidates(None, "run1", "cat", "sch")
        assert len(candidates) == 0


# ── Tests: _should_quarantine ────────────────────────────────────────────


class TestShouldQuarantine:
    def test_quarantines_at_threshold(self):
        assert _should_quarantine({"consecutive_nc": 3}) is True

    def test_does_not_quarantine_below_threshold(self):
        assert _should_quarantine({"consecutive_nc": 2}) is False

    def test_does_not_quarantine_zero(self):
        assert _should_quarantine({"consecutive_nc": 0}) is False


# ── Tests: _CORRECTABLE_VERDICTS ─────────────────────────────────────────


class TestCorrectableVerdicts:
    def test_genie_correct_is_correctable(self):
        assert "genie_correct" in _CORRECTABLE_VERDICTS

    def test_arbiter_repair_is_correctable(self):
        assert "arbiter_repair" in _CORRECTABLE_VERDICTS

    def test_neither_correct_is_not_correctable(self):
        assert "neither_correct" not in _CORRECTABLE_VERDICTS


# ── Tests: quarantine-aware accuracy ─────────────────────────────────────


class TestQuarantineAwareAccuracy:
    def test_quarantined_questions_excluded_from_denominator(self):
        rows = [
            {"result_correctness/value": "yes", "arbiter/value": "both_correct",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "no", "arbiter/value": "neither_correct",
             "inputs/question_id": "q16"},
            {"result_correctness/value": "yes", "arbiter/value": "both_correct",
             "inputs/question_id": "q3"},
        ]
        accuracy, correct, failures, excluded = _compute_arbiter_adjusted_accuracy(
            rows, quarantined_qids={"q16"},
        )
        assert excluded == 1
        assert correct == 2
        assert accuracy == 100.0
        assert failures == []

    def test_no_quarantine_preserves_original_behavior(self):
        rows = [
            {"result_correctness/value": "yes", "arbiter/value": "both_correct",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "no", "arbiter/value": "neither_correct",
             "inputs/question_id": "q16"},
        ]
        accuracy, correct, failures, excluded = _compute_arbiter_adjusted_accuracy(rows)
        assert excluded == 0
        assert correct == 1
        assert accuracy == 50.0
        assert failures == ["q16"]

    def test_quarantine_plus_excluded_both_removed(self):
        rows = [
            {"result_correctness/value": "yes", "arbiter/value": "both_correct",
             "inputs/question_id": "q1"},
            {"result_correctness/value": "excluded", "arbiter/value": "skipped",
             "inputs/question_id": "q2"},
            {"result_correctness/value": "no", "arbiter/value": "neither_correct",
             "inputs/question_id": "q3"},
        ]
        accuracy, correct, failures, excluded = _compute_arbiter_adjusted_accuracy(
            rows, quarantined_qids={"q3"},
        )
        assert excluded == 2
        assert correct == 1
        assert accuracy == 100.0


# ── Tests: VerdictEntry dataclass ────────────────────────────────────────


class TestVerdictEntry:
    def test_fields(self):
        e = VerdictEntry(
            iteration=3, verdict="genie_correct",
            genie_sql="SELECT 1", expected_sql="SELECT 2",
            question_text="What?", rationale="Because",
        )
        assert e.iteration == 3
        assert e.verdict == "genie_correct"
        assert e.genie_sql == "SELECT 1"
