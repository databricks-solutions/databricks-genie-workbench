"""P-E2 — pure-helper tests for ``_check_proposal_stage_forbidden_ag_leakage``."""
from __future__ import annotations


def _ag_with_signature(sig: str, levers: tuple[int, ...] = (5, 6)) -> dict:
    return {
        "id": "AG_X",
        "source_cluster_signatures": [sig],
        "levers": list(levers),
    }


def _empty_forbidden_pair():
    """Helper: build an empty ``_ForbiddenSetPair`` for negative tests."""
    from genie_space_optimizer.optimization.harness import _ForbiddenSetPair
    return _ForbiddenSetPair(
        by_root_cause=frozenset(),
        by_signature=frozenset(),
    )


def _normalised(blame):
    """Helper: build the canonical blame tuple the way
    ``_ag_collision_key_pair`` does."""
    from genie_space_optimizer.optimization.harness import _normalise_blame
    return _normalise_blame(blame)


def test_check_returns_none_when_forbidden_pair_empty(monkeypatch):
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "1")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _check_proposal_stage_forbidden_ag_leakage,
    )
    ag = _ag_with_signature("sig_A")
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["5", "6"],
    )
    iter_inputs = {"decision_records": [], "markers": []}
    result = _check_proposal_stage_forbidden_ag_leakage(
        run_id="r1", iteration=2, ag_id="AG_X",
        cluster_id="H004", root_cause="missing_filter",
        collision_pair=pair,
        forbidden_pair=_empty_forbidden_pair(),
        cluster_signature="sig_A",
        lever_set=(5, 6),
        call_site="cluster_driven_synthesis",
        iter_inputs=iter_inputs,
    )
    assert result is None
    assert iter_inputs["decision_records"] == []
    assert iter_inputs["markers"] == []


def test_check_returns_none_when_collision_pair_empty(monkeypatch):
    """Pair with no root_cause_key and no signature_keys → no match
    possible → silent."""
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "1")
    from genie_space_optimizer.optimization.harness import (
        _CollisionKeyPair,
        _ForbiddenSetPair,
        _check_proposal_stage_forbidden_ag_leakage,
    )
    iter_inputs = {"decision_records": [], "markers": []}
    result = _check_proposal_stage_forbidden_ag_leakage(
        run_id="r1", iteration=2, ag_id="AG_X",
        cluster_id="H004", root_cause="",
        collision_pair=_CollisionKeyPair(root_cause_key=None, signature_keys=()),
        forbidden_pair=_ForbiddenSetPair(
            by_root_cause=frozenset({("foo", (), frozenset([5]))}),
            by_signature=frozenset({("sig_A", frozenset([5]))}),
        ),
        cluster_signature="",
        lever_set=(),
        call_site="force_lever6",
        iter_inputs=iter_inputs,
    )
    assert result is None
    assert iter_inputs["decision_records"] == []


def test_check_silent_when_flag_off(monkeypatch):
    """Flag off → byte-stable; no record, no marker, no probe of the
    forbidden set."""
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "0")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _ForbiddenSetPair,
        _check_proposal_stage_forbidden_ag_leakage,
    )
    ag = _ag_with_signature("sig_A")
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["5", "6"],
    )
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset({(
            "missing_filter", _normalised(["t.col"]),
            frozenset([5, 6]),
        )}),
        by_signature=frozenset(),
    )
    iter_inputs = {"decision_records": [], "markers": []}
    result = _check_proposal_stage_forbidden_ag_leakage(
        run_id="r1", iteration=2, ag_id="AG_X",
        cluster_id="H004", root_cause="missing_filter",
        collision_pair=pair, forbidden_pair=forbidden,
        cluster_signature="sig_A", lever_set=(5, 6),
        call_site="cluster_driven_synthesis",
        iter_inputs=iter_inputs,
    )
    assert result is None
    assert iter_inputs["decision_records"] == []


def test_check_fires_on_root_cause_axis_match(monkeypatch, capsys):
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "1")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _ForbiddenSetPair,
        _check_proposal_stage_forbidden_ag_leakage,
    )
    ag = _ag_with_signature("sig_A")
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["5", "6"],
    )
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset({(
            "missing_filter", _normalised(["t.col"]),
            frozenset([5, 6]),
        )}),
        by_signature=frozenset(),  # only root-cause axis matches
    )
    iter_inputs = {"decision_records": [], "markers": []}
    result = _check_proposal_stage_forbidden_ag_leakage(
        run_id="r1", iteration=2, ag_id="AG_X",
        cluster_id="H004", root_cause="missing_filter",
        collision_pair=pair, forbidden_pair=forbidden,
        cluster_signature="sig_A", lever_set=(5, 6),
        call_site="cluster_driven_synthesis",
        iter_inputs=iter_inputs,
    )
    assert result == "root_cause"
    assert len(iter_inputs["decision_records"]) == 1
    rec = iter_inputs["decision_records"][0]
    assert rec["reason_code"] == "proposal_stage_forbidden_ag_observed"
    assert rec["metrics"]["call_site"] == "cluster_driven_synthesis"
    assert rec["metrics"]["match_axis"] == "root_cause"
    out = capsys.readouterr().out
    assert "GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED_V1" in out


def test_check_fires_on_signature_axis_match(monkeypatch, capsys):
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "1")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _ForbiddenSetPair,
        _check_proposal_stage_forbidden_ag_leakage,
    )
    ag = _ag_with_signature("sig_A")
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["6"],
    )
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset(),  # only signature axis matches
        by_signature=frozenset({("sig_A", frozenset([6]))}),
    )
    iter_inputs = {"decision_records": [], "markers": []}
    result = _check_proposal_stage_forbidden_ag_leakage(
        run_id="r1", iteration=2, ag_id="AG_X",
        cluster_id="H004", root_cause="missing_filter",
        collision_pair=pair, forbidden_pair=forbidden,
        cluster_signature="sig_A", lever_set=(6,),
        call_site="force_lever6",
        iter_inputs=iter_inputs,
    )
    assert result == "cluster_signature"
    rec = iter_inputs["decision_records"][0]
    assert rec["metrics"]["match_axis"] == "cluster_signature"
    assert rec["metrics"]["call_site"] == "force_lever6"


def test_check_classifies_both_when_both_axes_match(monkeypatch):
    monkeypatch.setenv("GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "1")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _ForbiddenSetPair,
        _check_proposal_stage_forbidden_ag_leakage,
    )
    ag = _ag_with_signature("sig_A")
    pair = _ag_collision_key_pair(
        ag=ag, ag_root_cause="missing_filter",
        ag_blame_set=["t.col"], lever_keys=["5", "6"],
    )
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset({(
            "missing_filter", _normalised(["t.col"]),
            frozenset([5, 6]),
        )}),
        by_signature=frozenset({("sig_A", frozenset([5, 6]))}),
    )
    iter_inputs = {"decision_records": [], "markers": []}
    result = _check_proposal_stage_forbidden_ag_leakage(
        run_id="r1", iteration=2, ag_id="AG_X",
        cluster_id="H004", root_cause="missing_filter",
        collision_pair=pair, forbidden_pair=forbidden,
        cluster_signature="sig_A", lever_set=(5, 6),
        call_site="cluster_driven_synthesis",
        iter_inputs=iter_inputs,
    )
    assert result == "both"
    assert iter_inputs["decision_records"][0]["metrics"]["match_axis"] == "both"


def test_check_validates_call_site():
    """The helper must reject unknown call_site values so a typo at
    a sub-AG site fails fast in development."""
    import pytest
    from genie_space_optimizer.optimization.harness import (
        _CollisionKeyPair,
        _ForbiddenSetPair,
        _check_proposal_stage_forbidden_ag_leakage,
    )
    with pytest.raises(ValueError, match="call_site"):
        _check_proposal_stage_forbidden_ag_leakage(
            run_id="r1", iteration=2, ag_id="AG_X",
            cluster_id="H004", root_cause="",
            collision_pair=_CollisionKeyPair(
                root_cause_key=None, signature_keys=()
            ),
            forbidden_pair=_ForbiddenSetPair(
                by_root_cause=frozenset(),
                by_signature=frozenset(),
            ),
            cluster_signature="",
            lever_set=(),
            call_site="not_a_real_site",
            iter_inputs={"decision_records": [], "markers": []},
        )
