"""Plan 12 PR 5 deferred promotion — active AG mutation tests.

The PR 5 helper ``_emit_plan12_ag_pivot_decision`` is promoted from
observation-only (``pivot_applied=False`` always) to active mutation:
when both ``GSO_PLAN12_LIVE_AG_RETRY_PIVOT=1`` AND
``GSO_PLAN12_LIVE_AG_RETRY_PIVOT_MUTATE=1``, AGs whose source cluster
carries a prior terminal signature are MUTATED to include the
recommended patch-family's lever_key in ``lever_directives``.

The mutation is additive: existing levers stay so the strategist's
original choice still dispatches. The recommended family's
no-directive fallback gives the new lane a chance to produce a
patch even without strategist-provided directive content.
"""
import json
import os
from unittest.mock import patch


def _parse_markers(out: str) -> list[dict]:
    rows = []
    for line in out.splitlines():
        if line.startswith("GSO_PLAN12_AG_PIVOT_DECIDED_V1 "):
            rows.append(json.loads(line.partition(" ")[2]))
    return rows


def _ag_with_lever_6_filter() -> dict:
    return {
        "id": "AG_pivot",
        "source_cluster_ids": ["H001"],
        "lever_directives": {
            "6": {
                "sql_expressions": [
                    {"patch_type": "add_sql_snippet_filter"},
                ],
            },
        },
    }


def _survival_failure_signature():
    from genie_space_optimizer.optimization.terminal_reason import (
        TerminalReason,
    )
    from genie_space_optimizer.optimization.terminal_signature import (
        build_terminal_signature,
    )
    return build_terminal_signature(
        # Sentinel RCA with no kit-map coverage so the companion picker
        # returns empty and the Trial 20 C1 pivot graph applies. (Used
        # to be ``"top_n_collapse"`` before Trial 26 W26.1 canonicalised
        # that label into the Trial 24 kit map.)
        root_cause="unmandated_demo_rca",
        blame_set=["catalog.schema.orders"],
        lever_set={6},
        target_qids={"gs_009"},
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )


# ── Flag tests ────────────────────────────────────────────────────────


def test_mutate_flag_off_by_default():
    from genie_space_optimizer.common.config import (
        plan12_live_ag_retry_pivot_mutate_enabled,
    )
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_PLAN12_LIVE_AG_RETRY_PIVOT_MUTATE", None)
        assert plan12_live_ag_retry_pivot_mutate_enabled() is False


def test_mutate_flag_on_with_truthy_values():
    from genie_space_optimizer.common.config import (
        plan12_live_ag_retry_pivot_mutate_enabled,
    )
    for val in ("true", "True", "TRUE", "1", "yes", "on"):
        with patch.dict(
            os.environ,
            {"GSO_PLAN12_LIVE_AG_RETRY_PIVOT_MUTATE": val},
        ):
            assert plan12_live_ag_retry_pivot_mutate_enabled() is True, (
                f"Expected True for {val!r}"
            )


# ── Mutation helper tests ─────────────────────────────────────────────


def test_apply_pivot_mutation_adds_recommended_lever():
    from genie_space_optimizer.optimization.harness import (
        _apply_pivot_mutation,
    )
    ag = _ag_with_lever_6_filter()
    # Recommend add_example_sql → lever_key "5".
    mutated = _apply_pivot_mutation(ag, "add_example_sql")
    assert mutated is True
    # Existing lever_directives["6"] stays; new "5" is added.
    assert "6" in ag["lever_directives"]
    assert "5" in ag["lever_directives"]
    seeded = ag["lever_directives"]["5"]
    assert seeded["_plan12_pivot_seeded"] is True
    assert seeded["patch_type"] == "add_example_sql"


def test_apply_pivot_mutation_skips_when_lever_already_present():
    """If the AG already has the recommended family's lever_key, no
    mutation happens — returns False."""
    from genie_space_optimizer.optimization.harness import (
        _apply_pivot_mutation,
    )
    ag = {
        "id": "AG",
        "source_cluster_ids": ["H001"],
        "lever_directives": {
            "5": {"example_sqls": [{"patch_type": "add_example_sql"}]},
            "6": {"sql_expressions": [{"patch_type": "add_sql_snippet_filter"}]},
        },
    }
    mutated = _apply_pivot_mutation(ag, "add_example_sql")
    assert mutated is False
    # No new entries added — the existing "5" is untouched.
    assert ag["lever_directives"]["5"].get("example_sqls")


def test_apply_pivot_mutation_unknown_family_returns_false():
    from genie_space_optimizer.optimization.harness import (
        _apply_pivot_mutation,
    )
    ag = _ag_with_lever_6_filter()
    mutated = _apply_pivot_mutation(ag, "unknown_family_xyz")
    assert mutated is False
    # AG unchanged.
    assert list(ag["lever_directives"].keys()) == ["6"]


# ── End-to-end emitter tests ──────────────────────────────────────────


def test_emitter_observation_mode_does_not_mutate(capsys, monkeypatch):
    """Parent flag ON, mutate=False → marker emits with pivot_applied=False
    and AG is NOT mutated."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_AG_RETRY_PIVOT", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_plan12_ag_pivot_decision,
    )
    sig = _survival_failure_signature()
    ag = _ag_with_lever_6_filter()
    fired = _emit_plan12_ag_pivot_decision(
        action_groups=[ag],
        forbidden_signatures=frozenset({sig}),
        cluster_id_to_signatures={"H001": [sig]},
        optimization_run_id="run_x",
        iteration=2,
        mutate=False,
    )
    assert fired is True
    markers = _parse_markers(capsys.readouterr().out)
    assert len(markers) == 1
    m = markers[0]
    assert m["pivot_recommended"] is True
    assert m["pivot_applied"] is False
    # AG untouched — only the original "6" lever_directive present.
    assert list(ag["lever_directives"].keys()) == ["6"]


def test_emitter_active_mutation_widens_lever_directives(capsys, monkeypatch):
    """Parent flag ON, mutate=True → marker emits with pivot_applied=True
    AND the AG's lever_directives is widened to include the
    recommended family's lever_key.

    The prior family must pivot ACROSS levers for a widening to occur.
    The Trial 20 C1 pivot graph (default-on) maps add_example_sql (L5)
    -> add_sql_snippet_filter (L6), so a lever-5 AG widens to add the
    lever-6 directive. (A lever-6 AG would pivot add_sql_snippet_filter
    -> add_sql_snippet_expression, which is L6 -> L6: no widening, so
    pivot_applied would correctly stay False.)
    """
    monkeypatch.setenv("GSO_PLAN12_LIVE_AG_RETRY_PIVOT", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_plan12_ag_pivot_decision,
    )
    sig = _survival_failure_signature()
    ag = {
        "id": "AG_pivot",
        "source_cluster_ids": ["H001"],
        "lever_directives": {
            "5": {"example_sqls": [{"patch_type": "add_example_sql"}]},
        },
    }
    fired = _emit_plan12_ag_pivot_decision(
        action_groups=[ag],
        forbidden_signatures=frozenset({sig}),
        cluster_id_to_signatures={"H001": [sig]},
        optimization_run_id="run_x",
        iteration=2,
        mutate=True,
    )
    assert fired is True
    markers = _parse_markers(capsys.readouterr().out)
    assert len(markers) == 1
    m = markers[0]
    assert m["pivot_recommended"] is True
    assert m["pivot_applied"] is True
    # AG widened — the recommended family's lever_key (L6) was added.
    assert "6" in ag["lever_directives"], (
        f"expected widened lever_directives; got {ag['lever_directives']}"
    )
    # The original "5" lever directive is still present.
    assert "5" in ag["lever_directives"]


def test_emitter_no_pivot_recommended_does_not_mutate(capsys, monkeypatch):
    """When no prior signature → no pivot recommended → no mutation
    even with mutate=True."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_AG_RETRY_PIVOT", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_plan12_ag_pivot_decision,
    )
    ag = _ag_with_lever_6_filter()
    fired = _emit_plan12_ag_pivot_decision(
        action_groups=[ag],
        forbidden_signatures=frozenset(),
        cluster_id_to_signatures={},  # no prior signatures
        optimization_run_id="run_x",
        iteration=2,
        mutate=True,
    )
    assert fired is True
    markers = _parse_markers(capsys.readouterr().out)
    assert len(markers) == 1
    m = markers[0]
    assert m["pivot_recommended"] is False
    assert m["pivot_applied"] is False
    # AG untouched.
    assert list(ag["lever_directives"].keys()) == ["6"]


def test_emitter_parent_flag_off_short_circuits(capsys, monkeypatch):
    """Even with mutate=True, the helper does nothing when the parent
    flag is OFF — preserves byte-stable replay for fixtures that
    don't enable Plan 12 retry-pivot at all."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_AG_RETRY_PIVOT", "0")

    from genie_space_optimizer.optimization.harness import (
        _emit_plan12_ag_pivot_decision,
    )
    sig = _survival_failure_signature()
    ag = _ag_with_lever_6_filter()
    fired = _emit_plan12_ag_pivot_decision(
        action_groups=[ag],
        forbidden_signatures=frozenset({sig}),
        cluster_id_to_signatures={"H001": [sig]},
        optimization_run_id="run_x",
        iteration=2,
        mutate=True,
    )
    assert fired is False
    assert _parse_markers(capsys.readouterr().out) == []
    assert list(ag["lever_directives"].keys()) == ["6"]
