"""Plan 12 — after a survival-failure terminal, the next AG for the
same cluster MUST pick a different patch_family. The canonical pivot
target is ``add_example_sql`` — the most forgiving patch family
(no SQL-validation surface, no structural repair gates, no
blast-radius collision risk beyond the question itself).
"""
from genie_space_optimizer.optimization.terminal_reason import (
    TerminalReason,
)
from genie_space_optimizer.optimization.terminal_signature import (
    build_terminal_signature,
)


def test_patch_family_pivot_after_no_applied():
    from genie_space_optimizer.optimization.stages.action_groups import (
        next_patch_family_for_cluster,
    )
    prior_sig = build_terminal_signature(
        root_cause="top_n_collapse",
        blame_set=["catalog.schema.orders"],
        lever_set={6},
        target_qids={"gs_009"},
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    chosen = next_patch_family_for_cluster(
        cluster_id="H001",
        prior_terminal_signatures=[prior_sig],
        prior_patch_family="add_sql_snippet_expression",
    )
    assert chosen != "add_sql_snippet_expression"
    assert chosen == "add_example_sql"


def test_pivot_after_structural_gate_dropped():
    from genie_space_optimizer.optimization.stages.action_groups import (
        next_patch_family_for_cluster,
    )
    prior_sig = build_terminal_signature(
        root_cause="missing_filter",
        blame_set=["catalog.schema.orders.col"],
        lever_set={6},
        target_qids={"gs_021"},
        terminal_reason=(
            TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY
        ),
    )
    chosen = next_patch_family_for_cluster(
        cluster_id="H001",
        prior_terminal_signatures=[prior_sig],
        prior_patch_family="add_sql_snippet_filter",
    )
    assert chosen == "add_example_sql"


def test_pivot_after_applyability_rejected():
    from genie_space_optimizer.optimization.stages.action_groups import (
        next_patch_family_for_cluster,
    )
    prior_sig = build_terminal_signature(
        root_cause="wrong_aggregation",
        blame_set=["catalog.schema.orders.revenue"],
        lever_set={5},
        target_qids={"gs_004"},
        terminal_reason=TerminalReason.APPLYABILITY_REJECTED,
    )
    chosen = next_patch_family_for_cluster(
        cluster_id="H001",
        prior_terminal_signatures=[prior_sig],
        prior_patch_family="add_sql_snippet_expression",
    )
    assert chosen == "add_example_sql"


def test_no_pivot_when_no_prior_failure():
    from genie_space_optimizer.optimization.stages.action_groups import (
        next_patch_family_for_cluster,
    )
    chosen = next_patch_family_for_cluster(
        cluster_id="H001",
        prior_terminal_signatures=[],
        prior_patch_family="add_sql_snippet_filter",
    )
    assert chosen == "add_sql_snippet_filter"


def test_no_pivot_when_prior_terminal_is_unrelated():
    """A non-survival-failure terminal (e.g. content regression
    rollback) doesn't trigger the pivot — the patch family is
    retained."""
    from genie_space_optimizer.optimization.stages.action_groups import (
        next_patch_family_for_cluster,
    )
    prior_sig = build_terminal_signature(
        root_cause="x",
        blame_set=["a.b.c"],
        lever_set={5},
        target_qids={"gs_001"},
        terminal_reason=TerminalReason.CONTENT_REGRESSION_ROLLBACK,
    )
    chosen = next_patch_family_for_cluster(
        cluster_id="H001",
        prior_terminal_signatures=[prior_sig],
        prior_patch_family="add_sql_snippet_filter",
    )
    assert chosen == "add_sql_snippet_filter"


def test_pivot_keys_on_most_recent_signature():
    """The policy reads the LAST entry first — a non-survival terminal
    in iter N-2 should NOT override a survival-failure terminal in
    iter N-1."""
    from genie_space_optimizer.optimization.stages.action_groups import (
        next_patch_family_for_cluster,
    )
    older_sig = build_terminal_signature(
        root_cause="x",
        blame_set=["a.b.c"],
        lever_set={5},
        target_qids={"gs_001"},
        terminal_reason=TerminalReason.CONTENT_REGRESSION_ROLLBACK,
    )
    recent_sig = build_terminal_signature(
        root_cause="y",
        blame_set=["a.b.c"],
        lever_set={5},
        target_qids={"gs_001"},
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    chosen = next_patch_family_for_cluster(
        cluster_id="H001",
        prior_terminal_signatures=[older_sig, recent_sig],
        prior_patch_family="add_sql_snippet_filter",
    )
    assert chosen == "add_example_sql"
