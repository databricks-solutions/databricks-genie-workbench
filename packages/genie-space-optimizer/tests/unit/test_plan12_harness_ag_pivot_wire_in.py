"""Plan 12 PR 5 deferred — harness wire-in for the AG-retry pivot
observation. The helper consults the loop-scoped forbidden_set for
the AG's source cluster's prior terminal signature and emits a
GSO_PLAN12_AG_PIVOT_DECIDED_V1 marker when the policy fires.

Three branches:

  1. Flag OFF → helper returns False (no marker).
  2. Flag ON, no prior signature for cluster → marker with
     pivot_recommended=False.
  3. Flag ON, survival-failure prior signature → marker with
     pivot_recommended=True and recommended_patch_family per the
     Trial 20 C1 pivot graph (the prior family's successor).
"""
import json

from genie_space_optimizer.optimization.terminal_reason import (
    TerminalReason,
)
from genie_space_optimizer.optimization.terminal_signature import (
    build_terminal_signature,
)


def _parse_markers(out: str) -> list[dict]:
    rows = []
    for line in out.splitlines():
        if line.startswith("GSO_PLAN12_AG_PIVOT_DECIDED_V1 "):
            rows.append(json.loads(line.partition(" ")[2]))
    return rows


def _ag_with_l6_filter() -> dict:
    return {
        "id": "AG2",
        "source_cluster_ids": ["H001"],
        "lever_directives": {
            "6": {"sql_expressions": [{"patch_type": "add_sql_snippet_filter"}]},
        },
    }


def _ag_with_l5_example_sql() -> dict:
    return {
        "id": "AG2",
        "source_cluster_ids": ["H001"],
        "lever_directives": {
            "5": {"example_sqls": [{"patch_type": "add_example_sql"}]},
        },
    }


def _survival_failure_signature() -> object:
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


def test_flag_off_returns_false_no_marker(capsys, monkeypatch):
    monkeypatch.setenv("GSO_PLAN12_LIVE_AG_RETRY_PIVOT", "0")

    from genie_space_optimizer.optimization.harness import (
        _emit_plan12_ag_pivot_decision,
    )

    fired = _emit_plan12_ag_pivot_decision(
        action_groups=[_ag_with_l6_filter()],
        forbidden_signatures=frozenset({_survival_failure_signature()}),
        cluster_id_to_signatures={"H001": [_survival_failure_signature()]},
        optimization_run_id="run_x",
        iteration=2,
    )
    assert fired is False
    out = capsys.readouterr().out
    assert "GSO_PLAN12_AG_PIVOT_DECIDED_V1" not in out


def test_flag_on_no_prior_signature_marker_no_pivot(capsys, monkeypatch):
    """The AG's source cluster has NO prior terminal signature →
    pivot_recommended=False. The marker still fires so postmortems
    see the policy was consulted."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_AG_RETRY_PIVOT", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_plan12_ag_pivot_decision,
    )

    fired = _emit_plan12_ag_pivot_decision(
        action_groups=[_ag_with_l6_filter()],
        forbidden_signatures=frozenset(),
        cluster_id_to_signatures={},  # no prior signatures
        optimization_run_id="run_x",
        iteration=2,
    )
    assert fired is True
    markers = _parse_markers(capsys.readouterr().out)
    assert len(markers) == 1
    m = markers[0]
    assert m["ag_id"] == "AG2"
    assert m["cluster_id"] == "H001"
    assert m["pivot_recommended"] is False
    assert m["pivot_applied"] is False
    # No prior, so prior_patch_family is empty + recommended falls
    # back to the strategist's choice (the L6 filter family).
    assert m["prior_terminal_reason"] == ""


def test_flag_on_survival_failure_emits_pivot_marker(capsys, monkeypatch):
    monkeypatch.setenv("GSO_PLAN12_LIVE_AG_RETRY_PIVOT", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_plan12_ag_pivot_decision,
    )

    sig = _survival_failure_signature()
    fired = _emit_plan12_ag_pivot_decision(
        action_groups=[_ag_with_l6_filter()],
        forbidden_signatures=frozenset({sig}),
        cluster_id_to_signatures={"H001": [sig]},
        optimization_run_id="run_x",
        iteration=2,
    )
    assert fired is True
    markers = _parse_markers(capsys.readouterr().out)
    assert len(markers) == 1
    m = markers[0]
    assert m["cluster_id"] == "H001"
    assert m["prior_terminal_reason"] == "no_applied_patches"
    # Trial 20 C1 pivot graph (default-on): the prior family is the L6
    # filter (add_sql_snippet_filter), so the recommended successor is
    # add_sql_snippet_expression — not the pre-Trial-20 constant
    # add_example_sql. (The sentinel root_cause is not a KIT_FOR_RCA
    # diagnosis, so the companion picker returns empty and the graph
    # applies.)
    assert m["recommended_patch_family"] == "add_sql_snippet_expression"
    assert m["pivot_recommended"] is True
    # Pivot is observation-only in this commit — pivot_applied stays
    # False until a future commit wires the AG mutation.
    assert m["pivot_applied"] is False


def test_flag_on_emits_one_marker_per_ag(capsys, monkeypatch):
    monkeypatch.setenv("GSO_PLAN12_LIVE_AG_RETRY_PIVOT", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_plan12_ag_pivot_decision,
    )

    sig = _survival_failure_signature()
    ag1 = dict(_ag_with_l6_filter(), id="AG1", source_cluster_ids=["H001"])
    ag2 = dict(_ag_with_l5_example_sql(), id="AG2", source_cluster_ids=["H002"])

    fired = _emit_plan12_ag_pivot_decision(
        action_groups=[ag1, ag2],
        forbidden_signatures=frozenset({sig}),
        cluster_id_to_signatures={"H001": [sig]},  # only H001 has a prior
        optimization_run_id="run_x",
        iteration=2,
    )
    assert fired is True
    markers = _parse_markers(capsys.readouterr().out)
    assert len(markers) == 2
    by_ag = {m["ag_id"]: m for m in markers}
    # AG1 (H001) — survival failure → pivot recommended.
    assert by_ag["AG1"]["pivot_recommended"] is True
    # AG2 (H002) — no prior signature → no pivot.
    assert by_ag["AG2"]["pivot_recommended"] is False


def test_flag_on_skips_ag_without_source_cluster(capsys, monkeypatch):
    """An AG with no source_cluster_ids has no cluster to look up; the
    helper skips it cleanly (no marker, no crash)."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_AG_RETRY_PIVOT", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_plan12_ag_pivot_decision,
    )

    ag = {"id": "AG_orphan", "source_cluster_ids": []}
    fired = _emit_plan12_ag_pivot_decision(
        action_groups=[ag],
        forbidden_signatures=frozenset(),
        cluster_id_to_signatures={},
        optimization_run_id="run_x",
        iteration=2,
    )
    assert fired is True  # the helper ran
    assert _parse_markers(capsys.readouterr().out) == []  # but emitted no marker
