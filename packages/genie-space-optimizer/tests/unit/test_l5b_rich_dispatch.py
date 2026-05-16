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


def test_dispatch_rich_returns_normalized_proposal_on_success() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _dispatch_rich_synthesis_for_l5b,
        drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()  # reset ledger

    cluster = {
        "cluster_id": "C1",
        "root_cause": "plural_top_n_collapse",
        "question_ids": ["q1"],
    }
    call_args = {}

    def _synth_stub(cluster_arg, metadata_arg, **kwargs):
        call_args["cluster"] = cluster_arg
        call_args["metadata"] = metadata_arg
        call_args["kwargs"] = kwargs
        return ClusterSynthesisResult(
            proposal={
                "example_question": "Show top route",
                "example_sql": "SELECT route FROM flights LIMIT 1",
                "parameters": [],
                "usage_guidance": "Use for ranking.",
                "_archetype_name": "single_row_top_n",
            },
            attempted_archetypes=("single_row_top_n",),
            skipped_reason=None,
        )

    out = _dispatch_rich_synthesis_for_l5b(
        cluster=cluster,
        metadata_snapshot={"_space_id": "test"},
        w=None,
        benchmarks=[],
        _synthesize=_synth_stub,
    )
    assert out == [{
        "example_question": "Show top route",
        "example_sql": "SELECT route FROM flights LIMIT 1",
        "parameters": [],
        "usage_guidance": "Use for ranking.",
    }]
    assert call_args["cluster"] is cluster
    assert call_args["metadata"] == {"_space_id": "test"}
    assert call_args["kwargs"]["benchmarks"] == []
    assert drain_l5b_rich_path_declines() == []


def test_dispatch_rich_appends_to_ledger_on_decline() -> None:
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _dispatch_rich_synthesis_for_l5b,
        drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()

    cluster = {
        "cluster_id": "C1",
        "root_cause": "plural_top_n_collapse",
        "asi_failure_type": "wrong_aggregation",
        "question_ids": ["q1", "q2"],
    }

    def _synth_decline(cluster_arg, metadata_arg, **kwargs):
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=("single_row_top_n", "ordered_list_by_metric"),
            skipped_reason="no_viable_archetype",
        )

    out = _dispatch_rich_synthesis_for_l5b(
        cluster=cluster,
        metadata_snapshot={"_space_id": "test"},
        w=None,
        benchmarks=[],
        _synthesize=_synth_decline,
    )
    assert out == []
    declines = drain_l5b_rich_path_declines()
    assert len(declines) == 1
    rec = declines[0]
    assert rec["cluster_id"] == "C1"
    assert rec["root_cause"] == "plural_top_n_collapse"
    assert rec["asi_failure_type"] == "wrong_aggregation"
    assert rec["attempted_archetypes"] == (
        "single_row_top_n", "ordered_list_by_metric",
    )
    assert rec["skipped_reason"] == "no_viable_archetype"
    assert rec["question_ids"] == ("q1", "q2")
    # Drain is destructive — second call returns empty.
    assert drain_l5b_rich_path_declines() == []


def test_dispatch_rich_appends_to_ledger_when_normalize_returns_none() -> None:
    """If the rich synthesizer returns a proposal that lacks
    example_question/example_sql, the normalizer returns None and the
    dispatcher treats this as a decline (ledger entry with a synthetic
    skipped_reason)."""
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        ClusterSynthesisResult,
    )
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _dispatch_rich_synthesis_for_l5b,
        drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()

    cluster = {
        "cluster_id": "C1",
        "root_cause": "wrong_aggregation",
        "question_ids": ["q1"],
    }

    def _synth_malformed(cluster_arg, metadata_arg, **kwargs):
        return ClusterSynthesisResult(
            proposal={"example_sql": "SELECT 1"},  # missing example_question
            attempted_archetypes=("single_row_top_n",),
            skipped_reason=None,
        )

    out = _dispatch_rich_synthesis_for_l5b(
        cluster=cluster,
        metadata_snapshot={},
        w=None,
        benchmarks=None,
        _synthesize=_synth_malformed,
    )
    assert out == []
    declines = drain_l5b_rich_path_declines()
    assert len(declines) == 1
    rec = declines[0]
    assert rec["cluster_id"] == "C1"
    assert rec["skipped_reason"] == "normalize_returned_none"


def test_dispatch_rich_handles_synthesizer_exception() -> None:
    """When the rich synthesizer raises, the dispatcher logs + returns
    [] + records a decline with skipped_reason='exception'."""
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        _dispatch_rich_synthesis_for_l5b,
        drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()

    cluster = {
        "cluster_id": "C1",
        "root_cause": "wrong_aggregation",
    }

    def _synth_raises(cluster_arg, metadata_arg, **kwargs):
        raise RuntimeError("simulated synthesizer failure")

    out = _dispatch_rich_synthesis_for_l5b(
        cluster=cluster,
        metadata_snapshot={},
        w=None,
        benchmarks=None,
        _synthesize=_synth_raises,
    )
    assert out == []
    declines = drain_l5b_rich_path_declines()
    assert len(declines) == 1
    rec = declines[0]
    assert rec["cluster_id"] == "C1"
    assert rec["skipped_reason"] == "exception"
    # No attempted archetypes when the synthesizer raised before
    # selecting one.
    assert rec["attempted_archetypes"] == ()


def test_drain_returns_empty_when_no_declines() -> None:
    from genie_space_optimizer.optimization.l5b_rich_dispatch import (
        drain_l5b_rich_path_declines,
    )
    drain_l5b_rich_path_declines()  # ensure starting empty
    assert drain_l5b_rich_path_declines() == []
