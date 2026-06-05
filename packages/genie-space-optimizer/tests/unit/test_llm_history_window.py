"""Unit tests for Phase 1 P1.3 fixed-window history cappers."""
from __future__ import annotations

from genie_space_optimizer.optimization.llm_history_window import (
    LAST_N_ITERATIONS,
    LAST_N_SIGNATURES,
    cap_iteration_bucketed_history,
    cap_signature_list,
)


def test_cap_iteration_bucketed_history_keeps_last_n_iterations() -> None:
    items = [
        {"iteration": i, "rca_kind_label": f"k{i}"} for i in range(1, 11)
    ]
    out = cap_iteration_bucketed_history(items, current_iteration=10)
    # Last 3 iterations (10, 9, 8) plus a digest at the head.
    assert out[0]["__digest__"] is True
    recent_iters = [e.get("iteration") for e in out if "iteration" in e]
    assert recent_iters == [8, 9, 10]
    assert out[0]["older_iterations_count"] == 7
    assert out[0]["older_iterations_range"] == [1, 7]


def test_cap_iteration_bucketed_history_outcome_and_family_histograms_in_digest() -> None:
    items = [
        {"iteration": 1, "rca_kind_label": "join", "outcome": "applied"},
        {"iteration": 2, "rca_kind_label": "join", "outcome": "kept_insufficient"},
    ]
    out = cap_iteration_bucketed_history(items, current_iteration=10)
    digest = out[0]
    assert digest["family_histogram"] == {"join": 2}
    assert digest["outcome_histogram"] == {
        "applied": 1,
        "kept_insufficient": 1,
    }


def test_cap_iteration_bucketed_history_no_digest_when_all_recent() -> None:
    items = [
        {"iteration": i, "rca_kind_label": f"k{i}"} for i in range(8, 11)
    ]
    out = cap_iteration_bucketed_history(items, current_iteration=10)
    # All items are within the last-3-iterations window (8, 9, 10);
    # no digest should appear.
    assert all("__digest__" not in e for e in out)
    assert [e["iteration"] for e in out] == [8, 9, 10]


def test_cap_iteration_bucketed_history_empty_returns_empty() -> None:
    assert cap_iteration_bucketed_history([], current_iteration=5) == []
    assert cap_iteration_bucketed_history(None, current_iteration=5) == []


def test_cap_iteration_bucketed_history_skips_non_dict_entries() -> None:
    items: list = [
        {"iteration": 1, "rca_kind_label": "k1"},
        "not_a_dict",
        {"iteration": 10, "rca_kind_label": "k10"},
    ]
    out = cap_iteration_bucketed_history(items, current_iteration=10)
    iters = [e.get("iteration") for e in out if "iteration" in e]
    assert iters == [10]


def test_cap_iteration_bucketed_history_family_histogram() -> None:
    items = [
        {"iteration": 1, "rca_kind_label": "join"},
        {"iteration": 2, "rca_kind_label": "join"},
        {"iteration": 3, "rca_kind_label": "topn"},
        {"iteration": 10, "rca_kind_label": "value"},
    ]
    out = cap_iteration_bucketed_history(items, current_iteration=10)
    digest = out[0]
    assert digest["__digest__"] is True
    assert digest["family_histogram"] == {"join": 2, "topn": 1}
    assert digest["older_iterations_count"] == 3


def test_cap_iteration_bucketed_history_outcome_histogram() -> None:
    items = [
        {"iteration": 1, "rca_kind_label": "join", "outcome": "applied"},
        {"iteration": 2, "rca_kind_label": "join", "outcome": "kept_insufficient"},
        {"iteration": 3, "rca_kind_label": "topn", "outcome": "applied"},
        {"iteration": 10, "rca_kind_label": "value", "outcome": "applied"},
    ]
    out = cap_iteration_bucketed_history(items, current_iteration=10)
    digest = out[0]
    assert digest["outcome_histogram"] == {
        "applied": 2,
        "kept_insufficient": 1,
    }


def test_cap_iteration_bucketed_history_uses_patch_family_fallback() -> None:
    items = [
        {"iteration": 1, "patch_family": "ambiguity"},
        {"iteration": 10, "rca_kind_label": "join"},
    ]
    out = cap_iteration_bucketed_history(items, current_iteration=10)
    digest = out[0]
    assert digest["family_histogram"] == {"ambiguity": 1}


def test_cap_iteration_bucketed_history_default_window() -> None:
    """Verify the default LAST_N_ITERATIONS=3 is respected."""
    items = [{"iteration": i} for i in range(1, 21)]
    out = cap_iteration_bucketed_history(items, current_iteration=20)
    recent_count = sum(1 for e in out if "iteration" in e)
    assert recent_count == LAST_N_ITERATIONS  # iterations 18, 19, 20


def test_cap_signature_list_drops_older_to_digest() -> None:
    sigs = tuple(f"lever-{i}:patch:rca_kind_{i}:behavior" for i in range(60))
    out = cap_signature_list(sigs, last_n=30)
    # First element is digest.
    assert out[0].startswith("__digest__:older_count=30")
    # Remaining 30 elements are the last 30 signatures verbatim.
    assert out[1:] == list(sigs[30:])
    assert len(out) == 31


def test_cap_signature_list_no_digest_when_within_cap() -> None:
    sigs = tuple(f"lever:patch:rca_{i}" for i in range(5))
    out = cap_signature_list(sigs, last_n=30)
    assert out == list(sigs)
    assert all("__digest__" not in s for s in out)


def test_cap_signature_list_default_cap() -> None:
    sigs = tuple(f"lever:patch:rca_{i}:b" for i in range(LAST_N_SIGNATURES + 5))
    out = cap_signature_list(sigs)
    assert out[0].startswith("__digest__:older_count=5")
    assert len(out) == LAST_N_SIGNATURES + 1


def test_cap_signature_list_empty_returns_empty() -> None:
    assert cap_signature_list([]) == []
    assert cap_signature_list(None) == []
    assert cap_signature_list(()) == []


def test_cap_signature_list_digest_family_histogram() -> None:
    older = [
        "lever-1:patch:rca_join:b",
        "lever-1:patch:rca_join:b",
        "lever-2:patch:rca_topn:b",
    ]
    recent = [f"lever:patch:rca_recent_{i}:b" for i in range(2)]
    out = cap_signature_list(older + recent, last_n=2)
    assert out[0].startswith("__digest__:older_count=3")
    assert "rca_join=2" in out[0]
    assert "rca_topn=1" in out[0]
    assert out[1:] == recent
