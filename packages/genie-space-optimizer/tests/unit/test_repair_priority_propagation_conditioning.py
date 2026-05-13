"""Tests for select_priority_step + propagation conditioning (Phase 2 Action 2.1)."""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.repair_archetypes import (
    archetype_by_name,
)


def test_priority_order_constant_has_five_steps_in_documented_order() -> None:
    from genie_space_optimizer.optimization.repair_priority import PRIORITY_ORDER

    assert PRIORITY_ORDER == (
        "semantic_clarification",
        "scoped_instruction",
        "repair_kit",
        "non_verbatim_example_pattern",
        "narrow_l6_snippet",
    )


def test_select_priority_step_returns_archetype_default_when_propagation_unknown() -> None:
    from genie_space_optimizer.optimization.repair_priority import (
        select_priority_step,
    )

    arch = archetype_by_name("default_time_window_filter")
    step = select_priority_step(
        archetype=arch,
        propagation_root_cause="unknown",
    )
    # default_time_window_filter's default is scoped_instruction
    assert step == "scoped_instruction"


def test_plural_top_n_upgrades_to_kit_with_l6_companion_when_insufficient_force() -> None:
    """When 1.3 reports instruction_insufficient_force, plural_top_n_collapse
    must not be a text instruction alone; it pairs with an L6 snippet
    or non_verbatim_example_pattern."""
    from genie_space_optimizer.optimization.repair_priority import (
        select_priority_step,
    )

    arch = archetype_by_name("plural_top_n_collapse")
    step = select_priority_step(
        archetype=arch,
        propagation_root_cause="instruction_insufficient_force",
    )
    # The default for plural_top_n_collapse is repair_kit; under
    # instruction_insufficient_force we promote to narrow_l6_snippet
    # so the kit *requires* an L6 companion (kit assembly enforces it).
    assert step == "narrow_l6_snippet"


def test_plural_top_n_default_is_repair_kit_when_propagation_unknown() -> None:
    from genie_space_optimizer.optimization.repair_priority import (
        select_priority_step,
    )

    arch = archetype_by_name("plural_top_n_collapse")
    step = select_priority_step(
        archetype=arch,
        propagation_root_cause="unknown",
    )
    assert step == "repair_kit"


def test_propagation_lag_keeps_default_priority_step() -> None:
    """propagation_lag indicates a verification-step problem, not a
    repair-shape problem. The priority step is unchanged; the planner's
    plan_repair function will additionally insert a verification hook."""
    from genie_space_optimizer.optimization.repair_priority import (
        select_priority_step,
    )

    arch = archetype_by_name("plural_top_n_collapse")
    step = select_priority_step(
        archetype=arch,
        propagation_root_cause="propagation_lag",
    )
    assert step == "repair_kit"


def test_instruction_not_scoped_to_qid_promotes_scoped_instruction_to_kit() -> None:
    """When 1.3 says scoping was wrong, instruction-level repairs
    must escalate to kit-level so the scope decision is bundled."""
    from genie_space_optimizer.optimization.repair_priority import (
        select_priority_step,
    )

    arch = archetype_by_name("default_time_window_filter")  # default: scoped_instruction
    step = select_priority_step(
        archetype=arch,
        propagation_root_cause="instruction_not_scoped_to_qid",
    )
    assert step == "repair_kit"


def test_eval_cache_stale_keeps_default_priority_step() -> None:
    """eval_cache_stale is an infrastructure issue; repair shape unchanged."""
    from genie_space_optimizer.optimization.repair_priority import (
        select_priority_step,
    )

    arch = archetype_by_name("dimension_disambiguation")
    step = select_priority_step(
        archetype=arch,
        propagation_root_cause="eval_cache_stale",
    )
    assert step == "semantic_clarification"


def test_select_priority_step_rejects_unknown_propagation_value() -> None:
    from genie_space_optimizer.optimization.repair_priority import (
        select_priority_step,
    )

    arch = archetype_by_name("dimension_disambiguation")
    with pytest.raises(ValueError, match="unknown propagation_root_cause"):
        select_priority_step(
            archetype=arch,
            propagation_root_cause="something_unrecognized",
        )


def test_propagation_root_cause_unknown_string_is_explicitly_allowed() -> None:
    """``unknown`` is the default value when 1.3 hasn't been filled in.
    It must round-trip without raising."""
    from genie_space_optimizer.optimization.repair_priority import (
        select_priority_step,
    )

    arch = archetype_by_name("payment_reporting_amount_semantics")
    step = select_priority_step(
        archetype=arch,
        propagation_root_cause="unknown",
    )
    assert step == arch.default_priority_step
