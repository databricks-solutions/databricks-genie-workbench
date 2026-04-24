"""Display-only tests for the per-question evaluation summary.

These tests lock two display contracts:
  * Task 3 — per-question cards annotate FAIL verdicts that the arbiter
    will flip to PASS in the aggregate (so readers don't see red FAILs
    next to a 100%% scorecard without context).
  * Task 4 — the scorecard reports both the pre-arbiter result_correctness
    and the arbiter-adjusted one, with unambiguous labels.

They call ``_print_eval_summary`` directly (it is the module-private
renderer invoked by ``run_evaluation``).
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _base_row(
    *,
    question_id: str = "q42",
    question: str = "What is the YoY by region?",
    genie_sql: str = "SELECT region FROM t WHERE region IS NOT NULL",
    expected_sql: str = "SELECT region FROM t",
    verdicts: dict | None = None,
    arbiter: str = "genie_correct",
    gt_rows: int = 4,
    genie_rows: int = 3,
) -> dict:
    """Construct an evaluation row with configurable judge verdicts."""
    default_verdicts = {
        "syntax_validity": "yes",
        "schema_accuracy": "no",
        "logical_accuracy": "no",
        "semantic_equivalence": "no",
        "completeness": "no",
        "response_quality": "yes",
        "asset_routing": "yes",
        "result_correctness": "no",
    }
    default_verdicts.update(verdicts or {})

    row: dict = {
        "question_id": question_id,
        "inputs/question_id": question_id,
        "inputs/question": question,
        "request": {
            "question_id": question_id,
            "question": question,
            "expected_sql": expected_sql,
        },
        "response": {
            "response": genie_sql,
            "status": "MessageStatus.COMPLETED",
            "comparison": {
                "match": False,
                "match_type": "mismatch",
                "gt_rows": gt_rows,
                "genie_rows": genie_rows,
                "gt_hash": "aaa",
                "genie_hash": "bbb",
            },
        },
        "arbiter/value": arbiter,
        "arbiter/rationale": "intent matches",
    }
    for judge, val in default_verdicts.items():
        row[f"{judge}/value"] = val
        row[f"{judge}/rationale"] = "n/a"
    return row


def _scores_100_all_100():
    """A scores_100 dict that makes every judge PASS its threshold.

    Task-3 tests only care about the per-question card; the scorecard
    thresholds are uninteresting here, so pass a uniform 100%% map.
    """
    return {
        "syntax_validity": 100.0,
        "schema_accuracy": 100.0,
        "logical_accuracy": 100.0,
        "semantic_equivalence": 100.0,
        "completeness": 100.0,
        "response_quality": 100.0,
        "asset_routing": 100.0,
        "result_correctness": 100.0,
    }


# ---------------------------------------------------------------------------
# Task 3: arbiter-override annotation on per-question cards
# ---------------------------------------------------------------------------

class TestArbiterOverrideAnnotation:
    def test_fail_with_genie_correct_arbiter_shows_override_suffix(self, capsys):
        from genie_space_optimizer.optimization.evaluation import (
            _print_eval_summary,
        )

        row = _base_row(arbiter="genie_correct")
        _print_eval_summary(
            rows=[row],
            scores_100=_scores_100_all_100(),
            thresholds_passed=True,
            iteration=0,
            eval_scope="full",
            total_questions=1,
        )

        out = capsys.readouterr().out
        # The per-question card must render the FAIL AND the override hint
        # on the same line for each arbiter-adjustable judge.
        assert "result_correctness       FAIL" in out
        assert "schema_accuracy          FAIL" in out
        # The override suffix must appear next to each FAIL that the arbiter
        # will rescue (adjustable judges only).
        assert out.count("(arbiter override → counts as PASS)") >= 1
        for judge in (
            "result_correctness", "schema_accuracy", "logical_accuracy",
            "semantic_equivalence", "completeness",
        ):
            # Every adjustable-judge FAIL should wear the annotation.
            assert f"{judge:<24s} FAIL  (arbiter override → counts as PASS)" in out

    def test_fail_with_both_correct_arbiter_shows_override_suffix(self, capsys):
        from genie_space_optimizer.optimization.evaluation import (
            _print_eval_summary,
        )

        row = _base_row(arbiter="both_correct", verdicts={"result_correctness": "no"})
        _print_eval_summary(
            rows=[row],
            scores_100=_scores_100_all_100(),
            thresholds_passed=True,
            iteration=0,
            eval_scope="full",
            total_questions=1,
        )

        out = capsys.readouterr().out
        assert "(arbiter override → counts as PASS)" in out

    def test_fail_with_ground_truth_correct_arbiter_no_override(self, capsys):
        """If the arbiter sides with GT, FAILs are not overridden — no suffix."""
        from genie_space_optimizer.optimization.evaluation import (
            _print_eval_summary,
        )

        row = _base_row(arbiter="ground_truth_correct")
        _print_eval_summary(
            rows=[row],
            scores_100=_scores_100_all_100(),
            thresholds_passed=True,
            iteration=0,
            eval_scope="full",
            total_questions=1,
        )

        out = capsys.readouterr().out
        assert "(arbiter override" not in out

    def test_non_adjustable_judge_fail_not_annotated(self, capsys):
        """asset_routing is NOT in the adjustable set; FAIL stays unannotated."""
        from genie_space_optimizer.optimization.evaluation import (
            _print_eval_summary,
        )

        # Make asset_routing FAIL AND result_correctness FAIL. result_correctness
        # failing forces the detailed card to render (logical_pass=False). Then
        # inside the detail we can verify asset_routing (non-adjustable) does
        # NOT get the override suffix even though arbiter=genie_correct.
        row = _base_row(
            arbiter="genie_correct",
            verdicts={
                "asset_routing": "no",
                "result_correctness": "no",
                "schema_accuracy": "yes",
                "logical_accuracy": "yes",
                "semantic_equivalence": "yes",
                "completeness": "yes",
            },
        )
        _print_eval_summary(
            rows=[row],
            scores_100=_scores_100_all_100(),
            thresholds_passed=True,
            iteration=0,
            eval_scope="full",
            total_questions=1,
        )

        out = capsys.readouterr().out
        asset_line = next(
            (l for l in out.splitlines() if "asset_routing" in l and "FAIL" in l),
            None,
        )
        assert asset_line is not None, f"asset_routing FAIL line missing:\n{out}"
        assert "arbiter override" not in asset_line
        # And as a sanity check, result_correctness (adjustable) IS annotated.
        rc_line = next(
            (l for l in out.splitlines() if "result_correctness" in l and "FAIL" in l),
            None,
        )
        assert rc_line is not None
        assert "arbiter override" in rc_line

    def test_display_constant_matches_aggregate_override_list(self):
        """``_ARBITER_ADJUSTABLE_DISPLAY_JUDGES`` must match the set of judges
        that the aggregate scorer actually overrides (4 regular + result_correctness).

        If someone adds a new adjustable judge in the aggregate without updating
        the display constant, per-question cards will silently fail to annotate
        the override. This test catches that drift.
        """
        from genie_space_optimizer.optimization.evaluation import (
            _ARBITER_ADJUSTABLE_DISPLAY_JUDGES,
        )

        # Source: result_correctness has its own dedicated adjust block at the
        # top of the per_judge loop; the rest live in the inline list. The
        # display constant must equal their union.
        aggregate_side = {
            "result_correctness",       # dedicated block
            "logical_accuracy",         # list
            "semantic_equivalence",     # list
            "completeness",             # list
            "schema_accuracy",          # list
        }
        assert _ARBITER_ADJUSTABLE_DISPLAY_JUDGES == aggregate_side

    def test_pass_verdict_never_annotated(self, capsys):
        """A PASS verdict never gets the override suffix, even if arbiter=genie_correct."""
        from genie_space_optimizer.optimization.evaluation import (
            _print_eval_summary,
        )

        row = _base_row(
            arbiter="genie_correct",
            verdicts={j: "yes" for j in (
                "syntax_validity", "schema_accuracy", "logical_accuracy",
                "semantic_equivalence", "completeness", "response_quality",
                "asset_routing", "result_correctness",
            )},
        )
        _print_eval_summary(
            rows=[row],
            scores_100=_scores_100_all_100(),
            thresholds_passed=True,
            iteration=0,
            eval_scope="full",
            total_questions=1,
        )

        out = capsys.readouterr().out
        assert "(arbiter override" not in out


# ---------------------------------------------------------------------------
# Task 4: rename the misleading ``result_correctness raw`` label
# ---------------------------------------------------------------------------

class TestResultCorrectnessRawLabel:
    def test_scorecard_reports_pre_arbiter_and_adjusted_result_correctness(
        self, capsys,
    ):
        """Two rows: one arbiter-overridden FAIL, one clean PASS.

        Pre-arbiter result_correctness is 1/2 = 50%%. Arbiter-adjusted is
        passed in via scores_100 (which is already arbiter-adjusted upstream)
        at 100%%. The scorecard must print BOTH with unambiguous labels and
        must NOT use the word "raw".
        """
        from genie_space_optimizer.optimization.evaluation import (
            _print_eval_summary,
        )

        overridden = _base_row(
            question_id="q-override",
            arbiter="genie_correct",
            verdicts={"result_correctness": "no"},
        )
        clean = _base_row(
            question_id="q-clean",
            arbiter="both_correct",
            verdicts={
                "syntax_validity": "yes", "schema_accuracy": "yes",
                "logical_accuracy": "yes", "semantic_equivalence": "yes",
                "completeness": "yes", "response_quality": "yes",
                "asset_routing": "yes", "result_correctness": "yes",
            },
        )
        clean["response"]["comparison"].update(
            {"match": True, "match_type": "exact"}
        )

        _print_eval_summary(
            rows=[overridden, clean],
            scores_100=_scores_100_all_100(),
            thresholds_passed=True,
            iteration=0,
            eval_scope="full",
            total_questions=2,
        )
        out = capsys.readouterr().out

        # The two distinct numbers must both be printed on one line with
        # unambiguous labels. Use a single contiguous assertion so we pin the
        # exact layout rather than two weak substring checks.
        assert (
            "result_correctness (pre-arbiter): 50.0%  "
            "(arbiter-adjusted: 100.0%)"
        ) in out

        # The word "raw" (in the old label) must no longer appear in the
        # Overall-accuracy paragraph.
        scorecard_tail = out.split("Overall accuracy:", 1)[1]
        assert "raw:" not in scorecard_tail

    def test_pre_arbiter_excludes_excluded_rows(self, capsys):
        """Excluded rows (GT infra / both-empty / genie unavailable) are not
        in the pre-arbiter denominator either."""
        from genie_space_optimizer.optimization.evaluation import (
            _print_eval_summary,
        )

        excluded = _base_row(question_id="q-excluded", arbiter="ground_truth_correct")
        excluded["result_correctness/value"] = "excluded"

        unavailable = _base_row(question_id="q-unavail", arbiter="ground_truth_correct")
        unavailable["outputs/comparison/error_type"] = "genie_result_unavailable"

        ok = _base_row(
            question_id="q-ok",
            arbiter="both_correct",
            verdicts={
                "syntax_validity": "yes", "schema_accuracy": "yes",
                "logical_accuracy": "yes", "semantic_equivalence": "yes",
                "completeness": "yes", "response_quality": "yes",
                "asset_routing": "yes", "result_correctness": "yes",
            },
        )

        _print_eval_summary(
            rows=[excluded, unavailable, ok],
            scores_100=_scores_100_all_100(),
            thresholds_passed=True,
            iteration=0,
            eval_scope="full",
            total_questions=3,
        )
        out = capsys.readouterr().out

        # Only 1 row contributes to the pre-arbiter denominator — and it's yes.
        assert "result_correctness (pre-arbiter): 100.0%" in out

    def test_handles_zero_scorable_rows(self, capsys):
        """If every row is excluded, the pre-arbiter line falls back to 0.0%."""
        from genie_space_optimizer.optimization.evaluation import (
            _print_eval_summary,
        )

        excluded = _base_row(question_id="q-excluded")
        excluded["result_correctness/value"] = "excluded"

        _print_eval_summary(
            rows=[excluded],
            scores_100=_scores_100_all_100(),
            thresholds_passed=True,
            iteration=0,
            eval_scope="full",
            total_questions=1,
        )
        out = capsys.readouterr().out

        assert "result_correctness (pre-arbiter): 0.0%" in out
