"""Structural and contract tests for the regression-mining lane wiring.

The lever loop in ``harness.py`` is far too large to drive end-to-end in
a unit test. Instead these tests assert the *invariants* the
regression-mining plan depends on, by reading the harness source and by
exercising the public regression-mining helpers in isolation:

* Mining is wired *only* inside the failed-gate rollback branch.
* Mining runs *after* :func:`_build_reflection_entry` and *before*
  :func:`update_iteration_reflection`, so the summary lands on the
  reflection JSON.
* Audit-row persistence is conditional on having mined insights and is
  guarded by ``try/except`` so a writer failure cannot block rollback.
* The strategist input path is feature-flagged off by default and
  yields a byte-empty hint string when the flag is off, regardless of
  whether high-confidence insights exist in the reflection buffer.
* ``mine_regression_insights`` is soft-fail by contract: malformed
  rows must produce ``[]`` rather than propagate exceptions.

Together with ``test_regression_mining.py`` (pure module behavior) and
``test_cluster_driven_synthesis.py`` (byte-equivalent prompt path),
this file closes out the verification suite from the plan.
"""

from __future__ import annotations

import inspect
import re

from genie_space_optimizer.optimization import harness
from genie_space_optimizer.optimization.regression_mining import (
    RegressionInsight,
    collect_insights_from_reflection_buffer,
    mine_regression_insights,
    render_strategist_hint_block,
    select_strategist_visible_insights,
)


# ---------------------------------------------------------------------------
# Harness ordering invariants (source-level)
# ---------------------------------------------------------------------------


def _harness_source() -> str:
    return inspect.getsource(harness)


def test_mining_call_lives_inside_failed_gate_branch():
    """``mine_regression_insights`` must only be invoked from the
    rollback path (``if not gate_result.get("passed")``). If a future
    refactor moves the call outside this branch we would silently mine
    on accepted iterations too — corrupting the contrastive signal.
    """
    src = _harness_source()
    assert "mine_regression_insights" in src, (
        "Harness should import or call mine_regression_insights"
    )

    rollback_marker = 'if not gate_result.get("passed")'
    assert rollback_marker in src, (
        "Harness must still gate mining on the failed-gate branch"
    )

    rollback_idx = src.index(rollback_marker)
    mining_idx = src.index("mine_regression_insights")
    assert mining_idx > rollback_idx, (
        "Regression mining must happen inside the failed-gate branch, "
        "after rollback handling, not before."
    )


def test_mining_runs_after_build_reflection_entry_and_before_update():
    """Plan-mandated ordering: build reflection entry → mine →
    attach summary → ``update_iteration_reflection``. Reordering
    breaks the audit promise that the reflection JSON carries the
    mined summary for the same iteration.
    """
    src = _harness_source()

    build_idx = src.find("_build_reflection_entry(")
    mine_idx = src.find("mine_regression_insights(")
    update_idx = src.find("update_iteration_reflection(")

    assert build_idx != -1, "Expected _build_reflection_entry call in harness"
    assert mine_idx != -1, "Expected mine_regression_insights call in harness"
    assert update_idx != -1, "Expected update_iteration_reflection call in harness"
    assert build_idx < mine_idx < update_idx, (
        "Lever loop must build the reflection, then mine insights, then "
        "persist — in that exact order. Found: build=%d, mine=%d, update=%d"
        % (build_idx, mine_idx, update_idx)
    )


def test_mining_is_wrapped_in_try_except_for_soft_fail():
    """Mining is non-authoritative — its failures must never abort the
    rollback. Source must show the call site reachable from a ``try:``
    block, with an ``except`` handler that swallows failures."""
    src = _harness_source()
    # The first ``mine_regression_insights(`` occurrence is the actual
    # call; the import uses ``mine_regression_insights,``.
    mine_idx = src.index("mine_regression_insights(")
    # The mining lane's ``try:`` opens at the top of the section, well
    # above the call (it covers import + extraction + call + summary
    # attach). A generous window catches it even if the section grows.
    pre_window = src[max(0, mine_idx - 2000): mine_idx]
    assert "try:" in pre_window, (
        "mine_regression_insights call must be reachable from a `try:` "
        "block so a malformed row never aborts the rollback path."
    )
    # And there must be a matching ``except`` after the call so
    # exceptions actually get caught.
    post_window = src[mine_idx: mine_idx + 2000]
    assert "except Exception" in post_window, (
        "Mining lane must catch broad exceptions (logged at debug) so "
        "no mining failure can leak into the rollback path."
    )


def test_audit_writer_is_conditional_on_mined_insights():
    """The ``write_lever_loop_decisions`` writer must only run when
    insights were mined; otherwise we'd write empty audit rows on
    every rollback and dilute the table."""
    src = _harness_source()
    # Guard pattern used in harness.
    assert "if _mined_insights:" in src, (
        "Audit-row writer must be gated by `if _mined_insights:`"
    )


# ---------------------------------------------------------------------------
# Soft-fail contract for the miner itself
# ---------------------------------------------------------------------------


def test_mining_returns_empty_for_malformed_rows_without_raising():
    """Malformed rows (non-dict, missing keys, garbage SQL) must not
    propagate to the rollback path."""
    bad_rows = [
        None,
        "not-a-row",
        {},
        {"qid": "q-x"},
        {
            "qid": "q-y",
            "outputs.predictions.sql": object(),
            "inputs.expected_sql": 42,
        },
    ]
    out = mine_regression_insights(
        failed_eval_rows=bad_rows, regressed_qids={"q-x", "q-y"},
    )
    assert out == []


def test_mining_returns_empty_when_regressed_qids_is_none_or_empty():
    """Defensive: a missing/empty regressed-qids set yields no insights
    even if rows look mineable. Prevents over-mining on raw-acceptance
    failures the gate did not flag as per-question regressions."""
    rows = [{
        "qid": "q-1",
        "outputs.predictions.sql": "SELECT use_mtdate_flag FROM t",
        "inputs.expected_sql": "SELECT is_month_to_date FROM t",
    }]
    assert mine_regression_insights(
        failed_eval_rows=rows, regressed_qids=set(),
    ) == []
    assert mine_regression_insights(
        failed_eval_rows=rows, regressed_qids=None,
    ) == []


# ---------------------------------------------------------------------------
# Strategist input — flag-off no-op behavior
# ---------------------------------------------------------------------------


def _high_confidence_insights() -> list[RegressionInsight]:
    return [
        RegressionInsight(
            insight_type="column_confusion",
            question_id="q-mtd",
            intended_column="is_month_to_date",
            confused_column="use_mtdate_flag",
            table="cat.sch.dim_date",
            sql_clause="WHERE",
            confidence=0.95,
            rationale="shared-prefix swap",
        )
    ]


def test_strategist_visible_returns_empty_when_flag_off():
    """The selector is the choke-point for strategist exposure. With
    ``enabled=False`` even highly-confident insights are dropped, and
    the rendered hint block is byte-empty."""
    insights = _high_confidence_insights()
    visible = select_strategist_visible_insights(
        insights, min_confidence=0.7, enabled=False,
    )
    assert visible == []
    assert render_strategist_hint_block(visible) == ""


def test_strategist_visible_drops_insights_below_min_confidence():
    """Even with the flag on, sub-threshold insights must not reach
    the strategist. This is the secondary safety belt against noisy
    early-iteration mining."""
    low = RegressionInsight(
        insight_type="column_confusion",
        question_id="q-low",
        intended_column="a", confused_column="b",
        sql_clause="WHERE", confidence=0.4, rationale="weak",
    )
    high = _high_confidence_insights()[0]
    visible = select_strategist_visible_insights(
        [low, high], min_confidence=0.7, enabled=True,
    )
    assert [v.question_id for v in visible] == ["q-mtd"]


def test_regression_mining_rca_context_enabled_without_strategist_hints():
    from genie_space_optimizer.optimization.harness import (
        _collect_regression_mining_iteration_context,
    )
    from genie_space_optimizer.optimization.regression_mining import (
        summarize_insights_for_reflection,
    )

    reflection_buffer = [{
        "iteration": 1,
        "regression_mining": summarize_insights_for_reflection(
            _high_confidence_insights(),
        ),
    }]

    context = _collect_regression_mining_iteration_context(
        reflection_buffer,
        enable_rca_ledger=True,
        enable_strategist_hints=False,
        min_confidence=0.7,
    )

    assert context["strategist_hints"] == ""
    assert [f.question_id for f in context["rca_findings"]] == ["q-mtd"]


def test_regression_mining_context_empty_when_rca_and_hints_disabled():
    from genie_space_optimizer.optimization.harness import (
        _collect_regression_mining_iteration_context,
    )
    from genie_space_optimizer.optimization.regression_mining import (
        summarize_insights_for_reflection,
    )

    reflection_buffer = [{
        "iteration": 1,
        "regression_mining": summarize_insights_for_reflection(
            _high_confidence_insights(),
        ),
    }]

    context = _collect_regression_mining_iteration_context(
        reflection_buffer,
        enable_rca_ledger=False,
        enable_strategist_hints=False,
        min_confidence=0.7,
    )

    assert context["visible_insights"] == []
    assert context["rca_findings"] == []
    assert context["strategist_hints"] == ""


def test_collect_from_empty_buffer_yields_no_insights():
    """A run with no prior rollbacks (or with rollbacks that produced
    no mining summary) must round-trip to ``[]``. Prevents the
    strategist hint block from ever appearing on the very first
    iteration."""
    assert collect_insights_from_reflection_buffer([]) == []
    assert collect_insights_from_reflection_buffer([{"iteration": 1}]) == []
    assert (
        collect_insights_from_reflection_buffer([
            {"iteration": 1, "regression_mining": {"total": 0, "items": []}}
        ])
        == []
    )


def test_strategist_input_path_is_feature_flagged_in_harness():
    """The harness must consult ``ENABLE_REGRESSION_MINING_STRATEGIST``
    when collecting the shared mining context, so flipping the env var
    off disables strategist hints without disabling RCA ledger feed."""
    src = _harness_source()
    assert "ENABLE_REGRESSION_MINING_STRATEGIST" in src, (
        "Harness must import the regression-mining strategist flag"
    )
    assert (
        "enable_strategist_hints=ENABLE_REGRESSION_MINING_STRATEGIST" in src
    ), (
        "Harness must pass the strategist flag into the shared mining "
        "context collector"
    )


def test_strategist_hints_default_to_empty_string_when_flag_off():
    """The default code path stamps an empty string onto the snapshot
    when the flag is off, so cluster-driven synthesis renders the
    legacy byte-equivalent prompt."""
    src = _harness_source()
    # Both branches (flag-off-via-else and flag-off-via-empty-buffer)
    # must explicitly initialise the snapshot key to "".
    matches = re.findall(
        r'metadata_snapshot\["_regression_mining_hints"\]\s*=\s*""',
        src,
    )
    assert matches, (
        "Harness must initialize `_regression_mining_hints` to '' before "
        "calling the shared collector so flag-off paths keep the legacy "
        "prompt path."
    )


def test_audit_writer_is_independently_try_excepted():
    """A failure in the audit writer must not abort the rollback path.
    Source must show the writer call inside a ``try:`` block with a
    matching ``except`` so a transient Spark/Delta error cannot
    affect acceptance semantics."""
    src = _harness_source()
    # The first occurrence is the import alias; we want the actual call
    # site, which is the *last* occurrence of ``_write_mining_decisions(``.
    writer_idx = src.rindex("_write_mining_decisions(")
    pre_window = src[max(0, writer_idx - 2000): writer_idx]
    post_window = src[writer_idx: writer_idx + 2000]
    assert "try:" in pre_window, (
        "Audit writer must be wrapped in its own try/except so the "
        "non-authoritative table never blocks the rollback path."
    )
    assert "except Exception" in post_window, (
        "Audit writer must catch broad exceptions so a Spark/Delta "
        "error never cascades into rollback handling."
    )


def test_harness_uses_hard_first_control_plane_clusters_for_strategy() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)

    assert "clusters_for_strategy" in src
    assert "_strategy_hard_clusters" in src
    assert "_strategy_soft_clusters" in src
    assert "clusters=_strategy_hard_clusters" in src
    assert "soft_signal_clusters=_strategy_soft_clusters" in src


def test_harness_uses_causal_grounding_for_action_group_targets() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)

    assert "causal_relevance_score" in src
    assert "explain_causal_relevance" in src
    assert "target_qids_from_action_group" in src
    assert "_ag_target_qids" in src
    assert "_rows_for_grounding" in src


def test_harness_uses_control_plane_acceptance_and_arbiter_completion() -> None:
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)

    assert "decide_control_plane_acceptance" in src
    assert "arbiter_objective_complete" in src
    assert "_control_plane_decision" in src
    assert "_ag_target_qids" in src


def test_harness_loads_control_plane_baseline_before_acceptance_decision() -> None:
    """``decide_control_plane_acceptance`` reads
    ``_baseline_rows_for_control_plane``; if the load block runs after
    the call, the gate path raises ``UnboundLocalError``. Pin the
    ordering so a future refactor can't reintroduce that bug.
    """
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)

    init_marker = "_baseline_rows_for_control_plane: list[dict] = []"
    call_marker = "_control_plane_decision = decide_control_plane_acceptance("

    init_idx = src.find(init_marker)
    call_idx = src.find(call_marker)

    assert init_idx != -1, (
        "Expected to find _baseline_rows_for_control_plane initialisation"
    )
    assert call_idx != -1, (
        "Expected to find _control_plane_decision = decide_control_plane_acceptance("
    )
    assert init_idx < call_idx, (
        "Control-plane baseline rows must be loaded before "
        "decide_control_plane_acceptance is called; otherwise the gate "
        "path raises UnboundLocalError."
    )


def test_harness_guards_control_plane_regression_with_kill_switch() -> None:
    """The rollback-driving ``control_plane_acceptance`` regression must
    be guarded by ``ENABLE_CONTROL_PLANE_ACCEPTANCE`` so operators have
    a kill switch if the new gate over-rejects.
    """
    import inspect

    from genie_space_optimizer.optimization import harness

    src = inspect.getsource(harness)

    assert "ENABLE_CONTROL_PLANE_ACCEPTANCE" in src, (
        "harness must import the control-plane acceptance flag"
    )
    assert (
        "ENABLE_CONTROL_PLANE_ACCEPTANCE\n"
        "        and not _control_plane_decision.accepted"
    ) in src, (
        "control_plane_acceptance regression append must be guarded by "
        "ENABLE_CONTROL_PLANE_ACCEPTANCE so operators can disable "
        "rollback while keeping diagnostics."
    )
