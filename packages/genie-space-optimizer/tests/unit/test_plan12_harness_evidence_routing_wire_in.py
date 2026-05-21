"""Plan 12 PR 6 deferred — harness wire-in for the evidence→lever
routing policy.

The helper sits in front of the harness's two
``generate_proposals_from_strategy`` callsites (Best-of-N and
single-shot) and reroutes ``target_lever=1`` (non-generating
``add_column_description``) away from the dispatcher when the AG's
evidence demands generation.

Flag OFF (default): returns ``target_lever`` unchanged, no marker.
Flag ON: consults the policy, emits one marker per call, returns the
rerouted lever.
"""
import json


def _parse_markers(out: str) -> list[dict]:
    rows = []
    for line in out.splitlines():
        if line.startswith("GSO_PLAN12_EVIDENCE_ROUTING_DECIDED_V1 "):
            rows.append(json.loads(line.partition(" ")[2]))
    return rows


def _ag_with_evidence(evidence_kind: str) -> dict:
    return {
        "id": "AG1",
        "source_cluster_ids": ["H001"],
        "asi_failure_type": evidence_kind,
        "lever_directives": {"1": {"column_descriptions": []}},
    }


def _ag_without_evidence() -> dict:
    return {
        "id": "AG2",
        "source_cluster_ids": ["H002"],
        "lever_directives": {"1": {"column_descriptions": []}},
    }


# ── Flag OFF: byte-stable replay ──────────────────────────────────────


def test_flag_off_returns_target_lever_unchanged_no_marker(capsys, monkeypatch):
    monkeypatch.setenv("GSO_PLAN12_LIVE_EVIDENCE_ROUTING", "0")

    from genie_space_optimizer.optimization.harness import (
        _resolve_effective_lever_with_evidence_policy,
    )

    effective = _resolve_effective_lever_with_evidence_policy(
        target_lever=1,
        action_group=_ag_with_evidence("wrong_aggregation"),
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
    )
    assert effective == 1
    assert _parse_markers(capsys.readouterr().out) == []


def test_flag_off_does_not_call_policy_even_when_lever_is_1(
    capsys, monkeypatch,
):
    """Defense in depth — even for a clear reroute case
    (lever=1 + wrong_aggregation), the helper must not invoke the
    policy or emit a marker when the flag is OFF."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_EVIDENCE_ROUTING", "0")

    from genie_space_optimizer.optimization import (
        optimizer as _opt,
    )
    calls = []
    original = _opt._apply_evidence_to_lever_policy

    def _spy(target_lever, ag):
        calls.append((target_lever, ag))
        return original(target_lever, ag)

    monkeypatch.setattr(_opt, "_apply_evidence_to_lever_policy", _spy)

    from genie_space_optimizer.optimization.harness import (
        _resolve_effective_lever_with_evidence_policy,
    )
    effective = _resolve_effective_lever_with_evidence_policy(
        target_lever=1,
        action_group=_ag_with_evidence("wrong_aggregation"),
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
    )
    assert effective == 1
    assert calls == []  # policy never invoked
    assert _parse_markers(capsys.readouterr().out) == []


# ── Flag ON: active routing + marker emission ─────────────────────────


def test_flag_on_passthrough_when_target_lever_not_1(capsys, monkeypatch):
    """Lever 2/5/6 are already generating lanes — the policy passes
    through unchanged. The marker still fires with reroute_applied=False
    so postmortems see the policy was consulted."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_EVIDENCE_ROUTING", "1")

    from genie_space_optimizer.optimization.harness import (
        _resolve_effective_lever_with_evidence_policy,
    )
    effective = _resolve_effective_lever_with_evidence_policy(
        target_lever=6,
        action_group=_ag_with_evidence("wrong_aggregation"),
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
    )
    assert effective == 6
    markers = _parse_markers(capsys.readouterr().out)
    assert len(markers) == 1
    m = markers[0]
    assert m["target_lever_before"] == 6
    assert m["target_lever_after"] == 6
    assert m["reroute_applied"] is False
    assert m["evidence_kind"] == "wrong_aggregation"


def test_flag_on_reroutes_lever_1_for_generating_evidence(
    capsys, monkeypatch,
):
    monkeypatch.setenv("GSO_PLAN12_LIVE_EVIDENCE_ROUTING", "1")

    from genie_space_optimizer.optimization.harness import (
        _resolve_effective_lever_with_evidence_policy,
    )
    effective = _resolve_effective_lever_with_evidence_policy(
        target_lever=1,
        action_group=_ag_with_evidence("wrong_aggregation"),
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
    )
    # Policy prefers 5b → maps to 5 in the helper.
    assert effective == 5
    markers = _parse_markers(capsys.readouterr().out)
    assert len(markers) == 1
    m = markers[0]
    assert m["target_lever_before"] == 1
    assert m["target_lever_after"] == 5
    assert m["reroute_applied"] is True
    assert m["evidence_kind"] == "wrong_aggregation"
    assert m["cluster_id"] == "H001"


def test_flag_on_keeps_lever_1_for_metadata_only_evidence(
    capsys, monkeypatch,
):
    """For ``ambiguous_column_description`` Lever 1 is in the policy's
    eligibility tuple — the policy passes through. Marker fires with
    reroute_applied=False."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_EVIDENCE_ROUTING", "1")

    from genie_space_optimizer.optimization.harness import (
        _resolve_effective_lever_with_evidence_policy,
    )
    effective = _resolve_effective_lever_with_evidence_policy(
        target_lever=1,
        action_group=_ag_with_evidence("ambiguous_column_description"),
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
    )
    assert effective == 1
    markers = _parse_markers(capsys.readouterr().out)
    assert len(markers) == 1
    m = markers[0]
    assert m["target_lever_before"] == 1
    assert m["target_lever_after"] == 1
    assert m["reroute_applied"] is False
    assert m["evidence_kind"] == "ambiguous_column_description"


def test_flag_on_reroutes_lever_1_when_evidence_unknown(capsys, monkeypatch):
    """No evidence on the AG → policy's unknown-evidence default
    refuses Lever 1 (NEVER defaults to Lever 1 — the postmortem-observed
    regression). Marker records the reroute."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_EVIDENCE_ROUTING", "1")

    from genie_space_optimizer.optimization.harness import (
        _resolve_effective_lever_with_evidence_policy,
    )
    effective = _resolve_effective_lever_with_evidence_policy(
        target_lever=1,
        action_group=_ag_without_evidence(),
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG2",
        cluster_id="H002",
    )
    assert effective == 5  # 5b → 5
    markers = _parse_markers(capsys.readouterr().out)
    assert len(markers) == 1
    m = markers[0]
    assert m["target_lever_before"] == 1
    assert m["target_lever_after"] == 5
    assert m["reroute_applied"] is True
    assert m["evidence_kind"] == ""


def test_flag_on_falls_back_to_root_cause_when_asi_failure_type_missing(
    capsys, monkeypatch,
):
    """The helper reads ``asi_failure_type`` first then ``root_cause``
    — matches the convention used throughout harness.py."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_EVIDENCE_ROUTING", "1")

    from genie_space_optimizer.optimization.harness import (
        _resolve_effective_lever_with_evidence_policy,
    )
    ag = {
        "id": "AG1",
        "source_cluster_ids": ["H001"],
        "root_cause": "missing_filter",  # only root_cause, no asi_failure_type
        "lever_directives": {"1": {}},
    }
    effective = _resolve_effective_lever_with_evidence_policy(
        target_lever=1,
        action_group=ag,
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
    )
    assert effective == 5
    markers = _parse_markers(capsys.readouterr().out)
    assert markers[0]["evidence_kind"] == "missing_filter"
    assert markers[0]["reroute_applied"] is True
