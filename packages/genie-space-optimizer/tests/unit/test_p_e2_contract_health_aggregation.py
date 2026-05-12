"""P-E2 — contract-health aggregation tests."""
from __future__ import annotations


def _observe_record(call_site: str, iteration: int = 1) -> dict:
    return {
        "reason_code": "proposal_stage_forbidden_ag_observed",
        "iteration": iteration,
        "metrics": {
            "call_site": call_site,
            "match_axis": "root_cause",
            "cluster_signature": "sig_A",
            "lever_set": [6],
        },
    }


def test_summary_zero_counts_by_default():
    from genie_space_optimizer.optimization.contract_health import (
        build_contract_health_summary,
    )
    summary = build_contract_health_summary(
        optimization_run_id="r1",
        invariant_violations=(),
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation=None,
        proposal_stage_forbidden_ag_observed_records=(),
    )
    d = summary.to_json_dict()
    counts = dict(d["proposal_stage_forbidden_ag_observed_count_by_call_site"])
    assert counts == {"cluster_driven_synthesis": 0, "force_lever6": 0}


def test_summary_counts_by_call_site():
    from genie_space_optimizer.optimization.contract_health import (
        build_contract_health_summary,
    )
    records = [
        _observe_record("cluster_driven_synthesis", iteration=1),
        _observe_record("cluster_driven_synthesis", iteration=2),
        _observe_record("force_lever6", iteration=2),
    ]
    summary = build_contract_health_summary(
        optimization_run_id="r1",
        invariant_violations=(),
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation=None,
        proposal_stage_forbidden_ag_observed_records=records,
    )
    counts = dict(summary.to_json_dict()[
        "proposal_stage_forbidden_ag_observed_count_by_call_site"
    ])
    assert counts == {
        "cluster_driven_synthesis": 2,
        "force_lever6": 1,
    }


def test_summary_ignores_non_observe_records():
    """Records with a different ``reason_code`` must not be counted —
    the aggregator filters by reason_code."""
    from genie_space_optimizer.optimization.contract_health import (
        build_contract_health_summary,
    )
    noise = [
        {"reason_code": "lever6_force_llm_declined", "metrics": {
            "call_site": "force_lever6",  # red herring — wrong reason_code
        }},
        {"reason_code": "narrow_skipped_no_original_patch_type", "metrics": {}},
    ]
    summary = build_contract_health_summary(
        optimization_run_id="r1",
        invariant_violations=(),
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation=None,
        proposal_stage_forbidden_ag_observed_records=noise,
    )
    counts = dict(summary.to_json_dict()[
        "proposal_stage_forbidden_ag_observed_count_by_call_site"
    ])
    assert counts == {"cluster_driven_synthesis": 0, "force_lever6": 0}


def test_summary_ignores_unknown_call_sites():
    """Defense in depth — a record with a malformed call_site is
    skipped (not counted under either bucket and not added as a new
    bucket)."""
    from genie_space_optimizer.optimization.contract_health import (
        build_contract_health_summary,
    )
    records = [
        _observe_record("cluster_driven_synthesis"),
        {
            "reason_code": "proposal_stage_forbidden_ag_observed",
            "metrics": {"call_site": "garbage_value"},
        },
    ]
    summary = build_contract_health_summary(
        optimization_run_id="r1",
        invariant_violations=(),
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation=None,
        proposal_stage_forbidden_ag_observed_records=records,
    )
    counts = dict(summary.to_json_dict()[
        "proposal_stage_forbidden_ag_observed_count_by_call_site"
    ])
    assert counts == {
        "cluster_driven_synthesis": 1,
        "force_lever6": 0,
    }


def test_summary_roundtrip_json():
    from genie_space_optimizer.optimization.contract_health import (
        ContractHealthSummary,
        build_contract_health_summary,
    )
    summary = build_contract_health_summary(
        optimization_run_id="r1",
        invariant_violations=(),
        phase_h_strict_validation=None,
        bundle_assembly_failed=(),
        bundle_assembly_incomplete=None,
        replay_validation=None,
        proposal_stage_forbidden_ag_observed_records=[
            _observe_record("force_lever6"),
        ],
    )
    roundtripped = ContractHealthSummary.from_json_dict(summary.to_json_dict())
    assert roundtripped.proposal_stage_forbidden_ag_observed_count_by_call_site == (
        ("cluster_driven_synthesis", 0),
        ("force_lever6", 1),
    )
