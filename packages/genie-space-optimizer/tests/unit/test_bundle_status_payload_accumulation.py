"""Bundle-status payload-accumulation behavioural test.

The harness wiring pattern is:

    _bundle_assembly_incomplete_payloads = []  # init at top of Phase H section
    ...
    if not _completeness["complete"]:
        _payload = {
            "optimization_run_id": run_id,
            "parent_bundle_run_id": _phase_h_anchor_run_id,
            "total_declared": ...,
            "total_materialized": ...,
            "missing_count": ...,
            "parent_level_missing": [...],
            "unmigrated_per_iteration_missing": [...],
        }
        _bundle_assembly_incomplete_payloads.append(_payload)
        print(_incomplete_marker(**_payload))
    ...
    # Later, after the Phase H block:
    _emit_contract_health_summary(
        ...
        bundle_assembly_incomplete=locals().get(
            "_bundle_assembly_incomplete_payloads"
        ),
        ...
    )

This test exercises that pattern in isolation by mirroring the
shim and asserting the resulting ``GSO_CONTRACT_HEALTH_V1`` payload
on stdout reports ``bundle_status="incomplete"``.
"""
from __future__ import annotations

import io
import json
from contextlib import redirect_stdout

from genie_space_optimizer.optimization.harness import (
    _emit_contract_health_summary,
)
from genie_space_optimizer.optimization.run_analysis_contract import (
    bundle_assembly_incomplete_marker,
)


def test_incomplete_payload_in_list_yields_bundle_status_incomplete(
    monkeypatch,
) -> None:
    monkeypatch.delenv("GSO_CONTRACT_HEALTH_SUMMARY_V1", raising=False)

    _bundle_assembly_incomplete_payloads: list[dict] = []
    payload = {
        "optimization_run_id": "run-wiring-1",
        "parent_bundle_run_id": "parent-run-x",
        "total_declared": 49,
        "total_materialized": 9,
        "missing_count": 40,
        "parent_level_missing": ["a", "b"],
        "unmigrated_per_iteration_missing": ["iter1/x"],
    }
    _bundle_assembly_incomplete_payloads.append(payload)

    buf = io.StringIO()
    with redirect_stdout(buf):
        print(bundle_assembly_incomplete_marker(**payload))
        _emit_contract_health_summary(
            optimization_run_id="run-wiring-1",
            invariant_violations=[],
            phase_h_strict_validation={
                "listing_status": "ok", "validator_status": "ok",
            },
            bundle_assembly_failed=(),
            bundle_assembly_incomplete=tuple(
                _bundle_assembly_incomplete_payloads
            ),
            replay_validation={"is_valid": True, "violation_count": 0},
        )

    out = buf.getvalue()
    assert "GSO_BUNDLE_ASSEMBLY_INCOMPLETE_V1 " in out, (
        "incomplete marker was not printed — fixture itself is broken"
    )
    assert "GSO_CONTRACT_HEALTH_V1 " in out, (
        "contract-health marker was not emitted"
    )

    ch_prefix = "GSO_CONTRACT_HEALTH_V1 "
    ch_line = next(
        line for line in out.splitlines() if line.startswith(ch_prefix)
    )
    ch_payload = json.loads(ch_line[len(ch_prefix):])
    assert ch_payload["bundle_status"] == "incomplete", (
        f"contract-health reports {ch_payload['bundle_status']!r} but "
        f"the incomplete marker fired with missing_count="
        f"{payload['missing_count']} — wiring did not propagate the "
        f"payload list to the emission call"
    )
    assert ch_payload["merge_gate_status"] == "warn", (
        "bundle_status=incomplete must drive merge_gate_status to "
        "warn (the RCO-2a policy)"
    )


def test_empty_payload_list_yields_bundle_status_complete(
    monkeypatch,
) -> None:
    """Negative case: when the Phase H bundle assembly is complete
    (the if-block at line 26204 was false), the incomplete-payload
    list is empty and the contract-health emission must report
    ``bundle_status="complete"``.
    """
    monkeypatch.delenv("GSO_CONTRACT_HEALTH_SUMMARY_V1", raising=False)
    _bundle_assembly_incomplete_payloads: list[dict] = []
    buf = io.StringIO()
    with redirect_stdout(buf):
        _emit_contract_health_summary(
            optimization_run_id="run-wiring-clean",
            invariant_violations=[],
            phase_h_strict_validation={
                "listing_status": "ok", "validator_status": "ok",
            },
            bundle_assembly_failed=(),
            bundle_assembly_incomplete=tuple(
                _bundle_assembly_incomplete_payloads
            ),
            replay_validation={"is_valid": True, "violation_count": 0},
        )
    out = buf.getvalue()
    ch_prefix = "GSO_CONTRACT_HEALTH_V1 "
    ch_line = next(
        line for line in out.splitlines() if line.startswith(ch_prefix)
    )
    ch_payload = json.loads(ch_line[len(ch_prefix):])
    assert ch_payload["bundle_status"] == "complete"
    assert ch_payload["merge_gate_status"] == "healthy"
