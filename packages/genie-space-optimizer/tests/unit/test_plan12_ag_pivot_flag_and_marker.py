"""Plan 12 PR 5 deferred — feature flag + AG-pivot-decided marker.

The flag (default OFF) gates the live AG-retry pivot observation
emitted at the harness's AG-construction site. The marker is
observation-only — it records the pivot decision the policy WOULD
make for the next iteration's AG without actually mutating the AG.
A future commit promotes the marker to an active mutation once the
postmortem stream confirms the policy is firing correctly.
"""
import json
import os
from unittest.mock import patch


# ── Flag tests ────────────────────────────────────────────────────────


def test_flag_off_by_default():
    from genie_space_optimizer.common.config import (
        plan12_live_ag_retry_pivot_enabled,
    )
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_PLAN12_LIVE_AG_RETRY_PIVOT", None)
        assert plan12_live_ag_retry_pivot_enabled() is False


def test_flag_on_with_truthy_values():
    from genie_space_optimizer.common.config import (
        plan12_live_ag_retry_pivot_enabled,
    )
    for val in ("true", "True", "TRUE", "1", "yes", "on"):
        with patch.dict(
            os.environ, {"GSO_PLAN12_LIVE_AG_RETRY_PIVOT": val},
        ):
            assert plan12_live_ag_retry_pivot_enabled() is True, (
                f"Expected True for {val!r}"
            )


def test_flag_off_with_falsy_values():
    from genie_space_optimizer.common.config import (
        plan12_live_ag_retry_pivot_enabled,
    )
    for val in ("false", "False", "0", "no", "off", ""):
        with patch.dict(
            os.environ, {"GSO_PLAN12_LIVE_AG_RETRY_PIVOT": val},
        ):
            assert plan12_live_ag_retry_pivot_enabled() is False, (
                f"Expected False for {val!r}"
            )


# ── Marker tests ──────────────────────────────────────────────────────


def _parse(line: str) -> tuple[str, dict]:
    name, _, payload = line.partition(" ")
    return name, json.loads(payload)


def test_pivot_decided_marker_recommends_pivot():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan12_ag_pivot_decided_marker,
    )
    line = plan12_ag_pivot_decided_marker(
        optimization_run_id="run_x",
        iteration=2,
        ag_id="AG2",
        cluster_id="H001",
        prior_terminal_reason="no_applied_patches",
        prior_patch_family="add_sql_snippet_expression",
        recommended_patch_family="add_example_sql",
        pivot_recommended=True,
        pivot_applied=False,
    )
    name, payload = _parse(line)
    assert name == "GSO_PLAN12_AG_PIVOT_DECIDED_V1"
    assert payload["cluster_id"] == "H001"
    assert payload["pivot_recommended"] is True
    assert payload["pivot_applied"] is False
    assert payload["recommended_patch_family"] == "add_example_sql"


def test_pivot_decided_marker_no_pivot_needed():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan12_ag_pivot_decided_marker,
    )
    line = plan12_ag_pivot_decided_marker(
        optimization_run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        prior_terminal_reason="",
        prior_patch_family="add_sql_snippet_filter",
        recommended_patch_family="add_sql_snippet_filter",
        pivot_recommended=False,
        pivot_applied=False,
    )
    _, payload = _parse(line)
    assert payload["pivot_recommended"] is False
    assert (
        payload["prior_patch_family"]
        == payload["recommended_patch_family"]
    )


def test_pivot_decided_marker_validates_payload_shape():
    """All fields are required at marker build time (positionally only —
    callers always know them since they're computed locally)."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        plan12_ag_pivot_decided_marker,
    )
    line = plan12_ag_pivot_decided_marker(
        optimization_run_id="r",
        iteration=0,
        ag_id="AG",
        cluster_id="C",
        prior_terminal_reason="",
        prior_patch_family="",
        recommended_patch_family="add_example_sql",
        pivot_recommended=True,
        pivot_applied=False,
    )
    _, payload = _parse(line)
    # The payload contract — postmortem renderers + I26-class
    # invariants in a future commit will key off these.
    assert set(payload.keys()) == {
        "optimization_run_id",
        "iteration",
        "ag_id",
        "cluster_id",
        "prior_terminal_reason",
        "prior_patch_family",
        "recommended_patch_family",
        "pivot_recommended",
        "pivot_applied",
    }
