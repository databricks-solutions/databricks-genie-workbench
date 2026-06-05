"""Phase 3 P3.4 — blast-radius unstamped guard tests."""
from __future__ import annotations

from genie_space_optimizer.optimization.blast_radius_guard import (
    BLAST_RADIUS_UNSTAMPED_UNSAFE,
    blast_radius_unstamped_rejection_reason,
    is_blast_radius_unstamped_in_production,
)


def test_no_benchmarks_means_workbench_path_returns_false() -> None:
    # No benchmarks → workbench / tape-replay; guard must return
    # False so the legacy E1 setdefault fallback continues.
    assert (
        is_blast_radius_unstamped_in_production(
            patch_body={"foo": 1},
            benchmarks=None,
        )
        is False
    )
    assert (
        is_blast_radius_unstamped_in_production(
            patch_body={"foo": 1},
            benchmarks=(),
        )
        is False
    )


def test_missing_passing_dependents_key_with_benchmarks_returns_true() -> None:
    # KEY ABSENT with a non-empty benchmark catalog is the
    # production contract violation.
    assert (
        is_blast_radius_unstamped_in_production(
            patch_body={"foo": 1},
            benchmarks=({"qid": "q1"},),
        )
        is True
    )


def test_empty_passing_dependents_list_is_safe() -> None:
    # The synthesize-llm transformer stamps [] when the scanner ran
    # and found no dependents — that's a positive witness, not a
    # violation.
    assert (
        is_blast_radius_unstamped_in_production(
            patch_body={"passing_dependents": []},
            benchmarks=({"qid": "q1"},),
        )
        is False
    )


def test_none_passing_dependents_is_safe() -> None:
    # ``None`` is also acceptable — the key is present so a witness
    # exists, even if the value is ambiguous downstream.
    assert (
        is_blast_radius_unstamped_in_production(
            patch_body={"passing_dependents": None},
            benchmarks=({"qid": "q1"},),
        )
        is False
    )


def test_non_empty_passing_dependents_list_is_safe() -> None:
    assert (
        is_blast_radius_unstamped_in_production(
            patch_body={"passing_dependents": ["q1", "q2"]},
            benchmarks=({"qid": "q3"},),
        )
        is False
    )


def test_none_patch_body_is_safe() -> None:
    assert (
        is_blast_radius_unstamped_in_production(
            patch_body=None,
            benchmarks=({"qid": "q1"},),
        )
        is False
    )


def test_canonical_rejection_reason_format() -> None:
    assert (
        blast_radius_unstamped_rejection_reason(
            patch_type="add_example_sql",
            intent_id="intent_42",
        )
        == "blast_radius_unstamped_unsafe:add_example_sql:intent_42"
    )


def test_canonical_rejection_reason_handles_empty_inputs() -> None:
    assert (
        blast_radius_unstamped_rejection_reason(
            patch_type="",
            intent_id="",
        )
        == "blast_radius_unstamped_unsafe:unknown:unknown"
    )


def test_sentinel_constant_value_is_stable() -> None:
    # Pin the canonical string so postmortem grading can
    # match without re-importing the predicate.
    assert BLAST_RADIUS_UNSTAMPED_UNSAFE == "blast_radius_unstamped_unsafe"
