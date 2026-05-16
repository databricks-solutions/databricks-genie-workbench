"""Unit tests for Plan B's L5b rich-dispatch routing.

Covers (in order):
  - rich_synthesis_primary_for_sql_shape_enabled() — feature flag.
  - should_route_l5b_to_rich_synthesizer(cluster) — routing predicate.
  - _normalize_rich_proposal_to_l5b_shape(proposal) — output adapter.
  - _dispatch_rich_synthesis_for_l5b(...) — rich-path executor.
  - drain_l5b_rich_path_declines() — ledger drain.
"""
from __future__ import annotations

from typing import Any


def test_flag_defaults_off(monkeypatch: Any) -> None:
    """Default state: rich-path routing is OFF. Byte-stable fixtures
    continue to pin the lean path."""
    monkeypatch.delenv(
        "GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", raising=False,
    )
    from genie_space_optimizer.common.config import (
        rich_synthesis_primary_for_sql_shape_enabled,
    )
    assert rich_synthesis_primary_for_sql_shape_enabled() is False


def test_flag_on_when_env_var_set_to_one(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.common.config import (
        rich_synthesis_primary_for_sql_shape_enabled,
    )
    assert rich_synthesis_primary_for_sql_shape_enabled() is True


def test_flag_off_when_env_var_set_to_zero(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "0")
    from genie_space_optimizer.common.config import (
        rich_synthesis_primary_for_sql_shape_enabled,
    )
    assert rich_synthesis_primary_for_sql_shape_enabled() is False


def test_flag_accepts_truthy_strings(monkeypatch: Any) -> None:
    """Accepts ``true``, ``True``, ``yes``, ``Y`` (case-insensitive).
    Rejects empty / unknown values."""
    from genie_space_optimizer.common.config import (
        rich_synthesis_primary_for_sql_shape_enabled,
    )
    for v in ("true", "TRUE", "True", "yes", "Yes", "Y", "1"):
        monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", v)
        assert rich_synthesis_primary_for_sql_shape_enabled() is True, v
    for v in ("0", "false", "no", "off", "", "garbage"):
        monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", v)
        assert rich_synthesis_primary_for_sql_shape_enabled() is False, v


def test_routes_when_flag_on_and_root_cause_sql_shape(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        should_route_l5b_to_rich_synthesizer,
    )
    cluster = {
        "cluster_id": "C1",
        "root_cause": "plural_top_n_collapse",
        "asi_failure_type": "",
    }
    assert should_route_l5b_to_rich_synthesizer(cluster) is True


def test_routes_when_flag_on_and_asi_failure_type_sql_shape(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        should_route_l5b_to_rich_synthesizer,
    )
    cluster = {
        "cluster_id": "C1",
        "root_cause": "",
        "asi_failure_type": "wrong_aggregation",
    }
    assert should_route_l5b_to_rich_synthesizer(cluster) is True


def test_does_not_route_when_flag_off(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "0")
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        should_route_l5b_to_rich_synthesizer,
    )
    cluster = {
        "cluster_id": "C1",
        "root_cause": "plural_top_n_collapse",
    }
    assert should_route_l5b_to_rich_synthesizer(cluster) is False


def test_does_not_route_when_failure_label_not_sql_shape(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        should_route_l5b_to_rich_synthesizer,
    )
    cluster = {
        "cluster_id": "C1",
        "root_cause": "ambiguous_question",
        "asi_failure_type": "ambiguity",
    }
    assert should_route_l5b_to_rich_synthesizer(cluster) is False


def test_does_not_route_when_both_labels_empty(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        should_route_l5b_to_rich_synthesizer,
    )
    cluster = {"cluster_id": "C1"}
    assert should_route_l5b_to_rich_synthesizer(cluster) is False


def test_routes_when_either_label_is_sql_shape(monkeypatch: Any) -> None:
    """Either ``asi_failure_type`` or ``root_cause`` being in
    ``_SQL_SHAPE_ROOT_CAUSES`` is sufficient. Plan A's
    ``cluster_failure_keys`` returns both."""
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        should_route_l5b_to_rich_synthesizer,
    )
    cluster = {
        "cluster_id": "C1",
        "root_cause": "plural_top_n_collapse",
        "asi_failure_type": "wrong_aggregation",
    }
    assert should_route_l5b_to_rich_synthesizer(cluster) is True


def test_returns_false_for_non_dict_input(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        should_route_l5b_to_rich_synthesizer,
    )
    assert should_route_l5b_to_rich_synthesizer(None) is False
    assert should_route_l5b_to_rich_synthesizer("not a cluster") is False
    assert should_route_l5b_to_rich_synthesizer([]) is False


def test_normalize_extracts_canonical_l5b_fields() -> None:
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _normalize_rich_proposal_to_l5b_shape,
    )
    rich = {
        "patch_type": "add_example_sql",
        "example_question": "Show top route",
        "example_sql": "SELECT route FROM flights GROUP BY route LIMIT 1",
        "parameters": [{"name": "k", "type": "int"}],
        "usage_guidance": "Use when ranking.",
        "rationale": "Original rationale",
        "_archetype_name": "single_row_top_n",
        "provenance": {"source": "cluster_driven_synthesis"},
    }
    out = _normalize_rich_proposal_to_l5b_shape(rich)
    assert out == {
        "example_question": "Show top route",
        "example_sql": "SELECT route FROM flights GROUP BY route LIMIT 1",
        "parameters": [{"name": "k", "type": "int"}],
        "usage_guidance": "Use when ranking.",
    }


def test_normalize_uses_rationale_when_usage_guidance_empty() -> None:
    """Mirrors the lean path's fallback at optimizer.py:9538-9539
    (``usage_guidance or rationale``)."""
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _normalize_rich_proposal_to_l5b_shape,
    )
    rich = {
        "example_question": "Q",
        "example_sql": "SELECT 1",
        "parameters": [],
        "usage_guidance": "",
        "rationale": "Rationale text",
    }
    out = _normalize_rich_proposal_to_l5b_shape(rich)
    assert out["usage_guidance"] == "Rationale text"


def test_normalize_handles_missing_parameters() -> None:
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _normalize_rich_proposal_to_l5b_shape,
    )
    rich = {
        "example_question": "Q",
        "example_sql": "SELECT 1",
        "usage_guidance": "G",
    }
    out = _normalize_rich_proposal_to_l5b_shape(rich)
    assert out["parameters"] == []


def test_normalize_returns_none_for_missing_required_fields() -> None:
    """A proposal without example_question or example_sql is unusable
    downstream; the adapter returns None and the rich-path dispatcher
    treats it as a decline."""
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _normalize_rich_proposal_to_l5b_shape,
    )
    assert _normalize_rich_proposal_to_l5b_shape({"example_sql": "SELECT 1"}) is None
    assert _normalize_rich_proposal_to_l5b_shape({"example_question": "Q"}) is None
    assert _normalize_rich_proposal_to_l5b_shape({}) is None
    assert _normalize_rich_proposal_to_l5b_shape(None) is None
