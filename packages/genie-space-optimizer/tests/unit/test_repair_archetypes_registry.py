"""Tests for the RepairArchetype registry (Phase 2 Action 2.1)."""

from __future__ import annotations

import pytest


def test_registry_has_exactly_five_named_archetypes() -> None:
    from genie_space_optimizer.optimization.repair_archetypes import REPAIR_ARCHETYPES

    assert {a.name for a in REPAIR_ARCHETYPES} == {
        "plural_top_n_collapse",
        "top_n_exact_cardinality",
        "default_time_window_filter",
        "dimension_disambiguation",
        "payment_reporting_amount_semantics",
    }


def test_plural_top_n_collapse_triggers_on_top_n_cardinality_collapse() -> None:
    from genie_space_optimizer.optimization.rca import RcaKind
    from genie_space_optimizer.optimization.repair_archetypes import (
        archetype_by_name,
    )

    arch = archetype_by_name("plural_top_n_collapse")
    assert RcaKind.TOP_N_CARDINALITY_COLLAPSE in arch.applicable_rca_kinds


def test_top_n_exact_cardinality_requires_explicit_cardinality_term() -> None:
    from genie_space_optimizer.optimization.repair_archetypes import (
        archetype_by_name,
    )

    arch = archetype_by_name("top_n_exact_cardinality")
    assert "exact_cardinality_required" in arch.evidence_predicates


def test_default_time_window_filter_requires_time_window_token() -> None:
    from genie_space_optimizer.optimization.rca import RcaKind
    from genie_space_optimizer.optimization.repair_archetypes import (
        archetype_by_name,
    )

    arch = archetype_by_name("default_time_window_filter")
    assert RcaKind.TIME_WINDOW_LOGIC_MISMATCH in arch.applicable_rca_kinds
    expected_tokens = {"time_window", "mtd", "ytd", "qtd", "wtw"}
    assert arch.required_grounding_tokens == frozenset(expected_tokens)


def test_dimension_disambiguation_triggers_on_two_rca_kinds() -> None:
    from genie_space_optimizer.optimization.rca import RcaKind
    from genie_space_optimizer.optimization.repair_archetypes import (
        archetype_by_name,
    )

    arch = archetype_by_name("dimension_disambiguation")
    assert RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING in arch.applicable_rca_kinds
    assert RcaKind.METRIC_VIEW_ROUTING_CONFUSION in arch.applicable_rca_kinds


def test_payment_reporting_amount_semantics_requires_payment_token() -> None:
    from genie_space_optimizer.optimization.rca import RcaKind
    from genie_space_optimizer.optimization.repair_archetypes import (
        archetype_by_name,
    )

    arch = archetype_by_name("payment_reporting_amount_semantics")
    assert RcaKind.MEASURE_SWAP in arch.applicable_rca_kinds
    expected_tokens = {"payment_amt", "payment_currency_cd"}
    assert arch.required_grounding_tokens == frozenset(expected_tokens)


def test_archetype_by_name_raises_on_unknown_name() -> None:
    from genie_space_optimizer.optimization.repair_archetypes import (
        archetype_by_name,
    )

    with pytest.raises(KeyError):
        archetype_by_name("not_a_real_archetype")


def test_each_archetype_declares_default_priority_step() -> None:
    from genie_space_optimizer.optimization.repair_archetypes import REPAIR_ARCHETYPES

    valid_steps = {
        "semantic_clarification",
        "scoped_instruction",
        "repair_kit",
        "non_verbatim_example_pattern",
        "narrow_l6_snippet",
    }
    for arch in REPAIR_ARCHETYPES:
        assert arch.default_priority_step in valid_steps, (
            f"{arch.name}: default_priority_step={arch.default_priority_step!r}"
            f" must be one of {valid_steps}"
        )
