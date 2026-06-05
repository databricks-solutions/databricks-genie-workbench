"""Trial 18 Step 1 — canonical ``row_semantic_score`` accessor.

The eval pipeline stamps four correctness signals on every row in a
documented precedence order; before Trial 18 the SM-lane gates only
read the raw byte-match scalar, missing the arbiter-aware boolean in
74% of production arbiter-rescued rows (postmortems e94376a3 +
d13938e7). These tests pin down:

* The precedence chain
  (``_is_semantic_correct`` -> ``arbiter_override_value`` -> arbiter
  verdict -> raw byte-match).
* Regression guards for the two parsing hazards the reviewer flagged
  on the Trial 18 plan (``bool('false') == True`` and
  ``float('yes') -> ValueError``).
* The ``gs_013`` iter-2 production-row replay so the false-negative
  reject that motivated Trial 18 cannot regress silently.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.evaluation import row_semantic_score


# ── Precedence: _is_semantic_correct wins ─────────────────────────────


def test_uses_is_semantic_correct_bool_true():
    """A row stamped with ``_is_semantic_correct=True`` returns 1.0
    even when the raw byte-match is 0.0. This is the canonical
    arbiter-rescued shape the eval pipeline writes."""
    row = {
        "_is_semantic_correct": True,
        "feedback/result_correctness/value": 0.0,
    }
    assert row_semantic_score(row) == 1.0


def test_is_semantic_correct_string_false_returns_zero():
    """Regression guard against ``bool("false") == True``.

    The eval pipeline sometimes serialises booleans as strings (round-
    trip through JSON / parquet / Spark). If we naively did
    ``bool(row["_is_semantic_correct"])`` the string ``"false"`` would
    silently evaluate to True. The hardened truth parser must return
    0.0 here.
    """
    row = {
        "_is_semantic_correct": "false",
        "feedback/result_correctness/value": 1.0,  # byte-match says yes
    }
    assert row_semantic_score(row) == 0.0


def test_is_semantic_correct_string_no_returns_zero():
    """``"no"`` is a recognised falsy literal."""
    row = {
        "_is_semantic_correct": "no",
        "feedback/result_correctness/value": 1.0,
    }
    assert row_semantic_score(row) == 0.0


# ── Precedence: arbiter_override_value wins when _is_semantic_correct missing


def test_uses_arbiter_override_value_yes():
    """``result_correctness/arbiter_override_value="yes"`` returns 1.0
    even when ``result_correctness/value=0.0`` — the precedence-2 lane.
    """
    row = {
        "feedback/result_correctness/arbiter_override_value": "yes",
        "feedback/result_correctness/value": 0.0,
    }
    assert row_semantic_score(row) == 1.0


# ── Precedence: arbiter verdict wins when both overrides missing


def test_falls_through_to_arbiter_verdict_correct():
    for verdict in ("both_correct", "genie_correct"):
        row = {
            "feedback/arbiter/value": verdict,
            "feedback/result_correctness/value": 0.0,
        }
        assert row_semantic_score(row) == 1.0, verdict


def test_falls_through_to_arbiter_verdict_incorrect():
    """``ground_truth_correct`` / ``neither_correct`` produce a
    definite 0.0 — we do NOT fall through to a possibly-misleading
    byte-match."""
    for verdict in ("ground_truth_correct", "neither_correct"):
        row = {
            "feedback/arbiter/value": verdict,
            "feedback/result_correctness/value": 1.0,  # byte-match noise
        }
        assert row_semantic_score(row) == 0.0, verdict


# ── Last-resort fallback: raw byte-match


def test_falls_through_to_byte_match_when_no_arbiter_signal():
    """Synthetic rows without ``_is_semantic_correct`` / arbiter
    override / arbiter verdict fall back to ``result_correctness/value``.
    This preserves Trial 16 behaviour for fixtures / replay tapes that
    don't run the full eval pipeline.
    """
    row_yes = {"feedback/result_correctness/value": 1.0}
    row_no = {"feedback/result_correctness/value": 0.0}
    assert row_semantic_score(row_yes) == 1.0
    assert row_semantic_score(row_no) == 0.0


def test_byte_match_yes_no_string_parses_correctly():
    """Regression guard against ``float("yes") -> ValueError``.

    The MLflow flattening sometimes writes ``"yes"`` / ``"no"`` rather
    than numeric ``1.0`` / ``0.0``. The hardened truth parser must
    accept both shapes.
    """
    row_yes = {"feedback/result_correctness/value": "yes"}
    row_no = {"feedback/result_correctness/value": "no"}
    assert row_semantic_score(row_yes) == 1.0
    assert row_semantic_score(row_no) == 0.0


# ── gs_013 production replay


def test_gs013_replay_returns_passing():
    """gs_013 iter-2 production row from postmortem
    ``d13938e7-d8a6-4570-a605-9fe231e5f99c``.

    The eval framework reported semantic correctness via arbiter
    rescue (judge-oracle disagreement, ``arbiter=both_correct``,
    thresholds met). Before Trial 18 the SM-lane gate read the raw
    byte-match (0.0) and rejected as ``target_unchanged``. After
    Trial 18 the canonical accessor must return 1.0 so the gate
    accepts.

    The shape mirrors what the eval pipeline stamps on each row
    after Tier-1.8 arbiter adjustment (see ``evaluation.py`` around
    line 8279 — "_is_semantic_correct should be used by gate logic").
    """
    row = {
        # Pipeline-stamped arbiter-aware boolean (precedence 1).
        "_is_semantic_correct": True,
        # Arbiter override that produced it (precedence 2).
        "feedback/result_correctness/arbiter_override_value": "yes",
        # Arbiter verdict (precedence 3).
        "feedback/arbiter/value": "both_correct",
        # Raw byte-match that misled the pre-Trial-18 gate
        # (precedence 4 — never reached in this row).
        "feedback/result_correctness/value": 0.0,
        # Audit-only metadata carried alongside.
        "generated_sql": "SELECT customer_id, COUNT(*) AS n FROM ...",
    }
    assert row_semantic_score(row) == 1.0
