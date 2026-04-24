"""Tests for the run-scoped scorer feedback cache (A1).

Covers :class:`_ScorerFeedbackCache`, :func:`_scorer_feedback_scope`,
:func:`_cache_scorer_feedback`, :func:`_drain_scorer_feedback_cache`.

Invariants under test:
- A fresh scope starts empty; two sequential scopes with overlapping
  ``question_id``s do not cross-contaminate.
- Exception inside the scope still resets state for the next scope.
- Nested scopes are isolated from one another (the inner scope does not
  leak into the outer one on exit).
- Duplicate ``(question_id, judge_name)`` writes increment the collision
  counter (overwrite matches legacy behavior).
- When no scope is active, writes still succeed (module-global fallback
  preserves back-compat) and can be drained explicitly.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization import evaluation as ev


def test_fresh_scope_starts_empty():
    with ev._scorer_feedback_scope() as cache:
        assert cache.drain() == {}


def test_write_then_drain_in_scope():
    with ev._scorer_feedback_scope():
        ev._cache_scorer_feedback("q1", "response_quality", "rat-A", {"sev": "minor"})
        ev._cache_scorer_feedback("q1", "arbiter", "rat-B")
        drained = ev._drain_scorer_feedback_cache()

    assert drained == {
        "q1": {
            "response_quality": {"rationale": "rat-A", "metadata": {"sev": "minor"}},
            "arbiter": {"rationale": "rat-B", "metadata": {}},
        }
    }


def test_sequential_scopes_do_not_cross_contaminate():
    with ev._scorer_feedback_scope():
        ev._cache_scorer_feedback("q1", "arbiter", "first-run")

    with ev._scorer_feedback_scope():
        drained = ev._drain_scorer_feedback_cache()
        assert drained == {}, (
            "State from a prior scope must not leak into the next scope"
        )


def test_exception_inside_scope_does_not_poison_next_scope():
    try:
        with ev._scorer_feedback_scope():
            ev._cache_scorer_feedback("q1", "arbiter", "will-be-wiped")
            raise RuntimeError("boom")
    except RuntimeError:
        pass

    with ev._scorer_feedback_scope():
        drained = ev._drain_scorer_feedback_cache()
        assert drained == {}


def test_duplicate_qid_collision_is_counted_and_warned(caplog):
    with caplog.at_level("WARNING"):
        with ev._scorer_feedback_scope() as cache:
            ev._cache_scorer_feedback("dup", "arbiter", "first")
            ev._cache_scorer_feedback("dup", "arbiter", "second")
            assert cache.collision_count == 1
            drained = cache.drain()

    # Legacy behavior: last write wins on collision.
    assert drained == {"dup": {"arbiter": {"rationale": "second", "metadata": {}}}}
    assert any("collision" in rec.message for rec in caplog.records)


def test_nested_scopes_are_isolated():
    with ev._scorer_feedback_scope():
        ev._cache_scorer_feedback("outer", "arbiter", "O")
        with ev._scorer_feedback_scope():
            ev._cache_scorer_feedback("inner", "arbiter", "I")
            inner = ev._drain_scorer_feedback_cache()
            assert inner == {"inner": {"arbiter": {"rationale": "I", "metadata": {}}}}

        outer = ev._drain_scorer_feedback_cache()
        assert outer == {"outer": {"arbiter": {"rationale": "O", "metadata": {}}}}


def test_write_without_scope_uses_legacy_global(monkeypatch):
    ev._LEGACY_SCORER_FEEDBACK_CACHE.drain()

    ev._cache_scorer_feedback("q-global", "arbiter", "legacy-path")
    drained = ev._drain_scorer_feedback_cache()

    assert drained == {
        "q-global": {"arbiter": {"rationale": "legacy-path", "metadata": {}}}
    }
    assert ev._drain_scorer_feedback_cache() == {}


def test_drain_clears_collision_counter():
    with ev._scorer_feedback_scope() as cache:
        ev._cache_scorer_feedback("x", "arbiter", "a")
        ev._cache_scorer_feedback("x", "arbiter", "b")
        assert cache.collision_count == 1
        cache.drain()
        assert cache.collision_count == 0
        ev._cache_scorer_feedback("x", "arbiter", "c")
        assert cache.collision_count == 0


def test_drain_returns_new_dict_each_call():
    with ev._scorer_feedback_scope() as cache:
        ev._cache_scorer_feedback("x", "arbiter", "a")
        first = cache.drain()
        second = cache.drain()
        assert first == {"x": {"arbiter": {"rationale": "a", "metadata": {}}}}
        assert second == {}
