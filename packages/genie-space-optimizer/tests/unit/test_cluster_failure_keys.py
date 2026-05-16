"""Unit tests for ``cluster_failure_keys`` — the canonical lookup helper
that bridges the asi_failure_type / root_cause label divergence.

The helper returns a tuple of non-empty failure-label strings the dispatch
loop should match against. Order is asi_failure_type first, then root_cause,
de-duplicated. Empties are dropped.
"""
from __future__ import annotations


def test_returns_both_when_present_and_distinct() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        cluster_failure_keys,
    )

    cluster = {
        "cluster_id": "H001",
        "root_cause": "plural_top_n_collapse",
        "asi_failure_type": "wrong_aggregation",
    }
    assert cluster_failure_keys(cluster) == (
        "wrong_aggregation",
        "plural_top_n_collapse",
    )


def test_dedupes_when_aligned() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        cluster_failure_keys,
    )

    cluster = {
        "cluster_id": "H001",
        "root_cause": "wrong_aggregation",
        "asi_failure_type": "wrong_aggregation",
    }
    assert cluster_failure_keys(cluster) == ("wrong_aggregation",)


def test_drops_empty_asi_failure_type() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        cluster_failure_keys,
    )

    cluster = {
        "cluster_id": "H001",
        "root_cause": "plural_top_n_collapse",
        "asi_failure_type": "",
    }
    assert cluster_failure_keys(cluster) == ("plural_top_n_collapse",)


def test_drops_missing_asi_failure_type() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        cluster_failure_keys,
    )

    cluster = {
        "cluster_id": "H001",
        "root_cause": "plural_top_n_collapse",
    }
    assert cluster_failure_keys(cluster) == ("plural_top_n_collapse",)


def test_drops_empty_root_cause() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        cluster_failure_keys,
    )

    cluster = {
        "cluster_id": "H001",
        "root_cause": "",
        "asi_failure_type": "wrong_aggregation",
    }
    assert cluster_failure_keys(cluster) == ("wrong_aggregation",)


def test_returns_empty_when_both_missing() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        cluster_failure_keys,
    )

    cluster = {"cluster_id": "H001"}
    assert cluster_failure_keys(cluster) == ()


def test_returns_empty_when_input_not_a_dict() -> None:
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        cluster_failure_keys,
    )

    assert cluster_failure_keys(None) == ()
    assert cluster_failure_keys("not a dict") == ()
    assert cluster_failure_keys([]) == ()


def test_coerces_non_string_values_to_str() -> None:
    """Defensive against upstream fixture/legacy entries where the
    failure label is captured as a non-string (e.g., an enum member).
    """
    from genie_space_optimizer.optimization.forced_synthesis_dispatch import (
        cluster_failure_keys,
    )

    class _FakeEnum:
        def __str__(self) -> str:  # pragma: no cover - trivial
            return "wrong_aggregation"

    cluster = {
        "root_cause": _FakeEnum(),
        "asi_failure_type": None,
    }
    assert cluster_failure_keys(cluster) == ("wrong_aggregation",)
