"""P-E1 — replay byte-stability with both flags OFF.

The ``run_ccf1d60d_7now.json`` fixture contains records of the form
``"reason": "unrecognized_patch_type"`` and
``"reason_detail": "original_patch_type=; reason=unrecognized_patch_type"``.
With ``GSO_NARROW_SKIPPED_NO_ORIGINAL_PATCH_TYPE=0`` and
``GSO_L6_DECLINE_CACHE=0``, P-E1 must reproduce these reason codes
verbatim — no behavioural drift relative to the historical fixture.

The post-flip case (both flags ON) is exercised by the unit tests in
``test_p_e1_l6_decline_cache.py`` and
``test_p_e1_narrow_skipped_no_original_patch_type.py``. A separate
fixture refresh task (out of scope for P-E1) will update the
historical replay snapshots to reflect the new reason codes.
"""
from __future__ import annotations

import json
import pathlib

import pytest

FIXTURE_PATH = (
    pathlib.Path(__file__).parent
    / "fixtures"
    / "run_ccf1d60d_7now.json"
)


@pytest.fixture(scope="module")
def fixture() -> dict:
    if not FIXTURE_PATH.exists():
        pytest.skip(f"ccf1d60d anchor fixture missing at {FIXTURE_PATH}")
    return json.loads(FIXTURE_PATH.read_text())


def test_fixture_contains_legacy_unrecognized_patch_type(fixture):
    """Sanity check: the fixture still encodes the legacy reason
    so the byte-stability comparison below is meaningful.
    """
    text = json.dumps(fixture)
    assert "unrecognized_patch_type" in text
    assert "original_patch_type=; reason=unrecognized_patch_type" in text


def test_narrow_replacement_diagnosis_flag_off_byte_stable(monkeypatch):
    """With the narrow-skipped flag OFF, the diagnosis returns the
    legacy reason code regardless of patch_type value.
    """
    monkeypatch.setenv("GSO_NARROW_SKIPPED_NO_ORIGINAL_PATCH_TYPE", "0")
    from genie_space_optimizer.optimization.cluster_driven_synthesis import (
        narrow_replacement_diagnosis,
    )
    diag_empty = narrow_replacement_diagnosis(
        original_patch={"patch_type": ""},
        ag_target_qids=("q1",),
        root_cause="missing_filter",
    )
    diag_unknown = narrow_replacement_diagnosis(
        original_patch={"patch_type": "not_a_real_patch_type"},
        ag_target_qids=("q1",),
        root_cause="missing_filter",
    )
    assert diag_empty["reason"] == "unrecognized_patch_type"
    assert diag_unknown["reason"] == "unrecognized_patch_type"


def test_force_l6_helper_flag_off_byte_stable(monkeypatch):
    """With ``GSO_L6_DECLINE_CACHE=0`` the helper makes one LLM call
    per attempt — no cache short-circuit, matching the pre-P-E1
    legacy behaviour.
    """
    monkeypatch.setenv("GSO_L6_DECLINE_CACHE", "0")
    monkeypatch.setenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", "1")
    from genie_space_optimizer.optimization.harness import (
        _ag_collision_key_pair,
        _maybe_force_lever6_with_cache,
    )

    iter_inputs = {"decision_records": [], "markers": []}
    decline_cache: dict[tuple, int] = {}
    llm_calls = {"n": 0}

    def fake_force_lever6() -> dict | None:
        llm_calls["n"] += 1
        return None

    pair = _ag_collision_key_pair(
        ag={"id": "AG_X", "source_cluster_signatures": ["sig_A"]},
        ag_root_cause="missing_filter",
        ag_blame_set=["t.col"],
        lever_keys=["6"],
    )

    for _ in range(3):
        _maybe_force_lever6_with_cache(
            run_id="r1", iteration=2, ag_id="AG_X",
            collision_pair=pair, snippet_type=None,
            decline_cache=decline_cache,
            iter_inputs=iter_inputs,
            force_l6_call=fake_force_lever6,
            cluster={"cluster_id": "H004", "root_cause": "missing_filter"},
            target_qids=(),
        )

    # With cache off, the cache stays empty and every call hits the LLM.
    assert decline_cache == {}
    assert llm_calls["n"] == 3
    declined = [
        r for r in iter_inputs["decision_records"]
        if r["reason_code"] == "lever6_force_llm_declined"
    ]
    assert len(declined) == 3
    assert all(r["metrics"]["cached"] is False for r in declined)
