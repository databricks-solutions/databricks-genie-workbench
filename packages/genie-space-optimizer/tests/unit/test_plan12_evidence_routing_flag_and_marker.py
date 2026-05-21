"""Plan 12 PR 6 deferred — feature flag + evidence-routing-decided marker.

The flag (default OFF) gates the live evidence→lever routing in the
harness's lever loop. The marker records the per-AG, per-lever-call
decision the policy makes so postmortems can audit routing before
a high-tier invariant codifies it.
"""
import json
import os
from unittest.mock import patch


# ── Flag tests ────────────────────────────────────────────────────────


def test_flag_off_by_default():
    from genie_space_optimizer.common.config import (
        plan12_live_evidence_routing_enabled,
    )
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_PLAN12_LIVE_EVIDENCE_ROUTING", None)
        assert plan12_live_evidence_routing_enabled() is False


def test_flag_on_with_truthy_values():
    from genie_space_optimizer.common.config import (
        plan12_live_evidence_routing_enabled,
    )
    for val in ("true", "True", "TRUE", "1", "yes", "on"):
        with patch.dict(
            os.environ, {"GSO_PLAN12_LIVE_EVIDENCE_ROUTING": val},
        ):
            assert plan12_live_evidence_routing_enabled() is True, (
                f"Expected True for {val!r}"
            )


def test_flag_off_with_falsy_values():
    from genie_space_optimizer.common.config import (
        plan12_live_evidence_routing_enabled,
    )
    for val in ("false", "False", "0", "no", "off", ""):
        with patch.dict(
            os.environ, {"GSO_PLAN12_LIVE_EVIDENCE_ROUTING": val},
        ):
            assert plan12_live_evidence_routing_enabled() is False, (
                f"Expected False for {val!r}"
            )


# ── Marker tests ──────────────────────────────────────────────────────


def _parse(line: str) -> tuple[str, dict]:
    name, _, payload = line.partition(" ")
    return name, json.loads(payload)


def test_marker_records_reroute_for_lever1_with_generating_evidence():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan12_evidence_routing_decided_marker,
    )
    line = plan12_evidence_routing_decided_marker(
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        evidence_kind="wrong_aggregation",
        target_lever_before=1,
        target_lever_after=5,
        reroute_applied=True,
    )
    name, payload = _parse(line)
    assert name == "GSO_PLAN12_EVIDENCE_ROUTING_DECIDED_V1"
    assert payload["evidence_kind"] == "wrong_aggregation"
    assert payload["target_lever_before"] == 1
    assert payload["target_lever_after"] == 5
    assert payload["reroute_applied"] is True


def test_marker_records_passthrough_when_lever_is_not_1():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan12_evidence_routing_decided_marker,
    )
    line = plan12_evidence_routing_decided_marker(
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        evidence_kind="wrong_aggregation",
        target_lever_before=6,
        target_lever_after=6,
        reroute_applied=False,
    )
    _, payload = _parse(line)
    assert payload["target_lever_before"] == payload["target_lever_after"] == 6
    assert payload["reroute_applied"] is False


def test_marker_records_metadata_only_evidence_keeps_lever_1():
    """For ``ambiguous_column_description`` Lever 1 is eligible (the
    failure is genuinely metadata-level). The marker fires with
    reroute_applied=False."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan12_evidence_routing_decided_marker,
    )
    line = plan12_evidence_routing_decided_marker(
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        evidence_kind="ambiguous_column_description",
        target_lever_before=1,
        target_lever_after=1,
        reroute_applied=False,
    )
    _, payload = _parse(line)
    assert payload["evidence_kind"] == "ambiguous_column_description"
    assert payload["target_lever_before"] == payload["target_lever_after"] == 1
    assert payload["reroute_applied"] is False


def test_marker_validates_payload_shape():
    """All fields are required at marker build time — the postmortem
    renderer and a future I26-class invariant will key off these."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan12_evidence_routing_decided_marker,
    )
    line = plan12_evidence_routing_decided_marker(
        optimization_run_id="r",
        iteration=0,
        ag_id="AG",
        cluster_id="C",
        evidence_kind="",
        target_lever_before=1,
        target_lever_after=5,
        reroute_applied=True,
    )
    _, payload = _parse(line)
    assert set(payload.keys()) == {
        "optimization_run_id",
        "iteration",
        "ag_id",
        "cluster_id",
        "evidence_kind",
        "target_lever_before",
        "target_lever_after",
        "reroute_applied",
    }
