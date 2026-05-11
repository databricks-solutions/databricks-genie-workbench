"""Cycle 11 — invariant-suite tests. One test per invariant, each
exercises the pure function on synthetic inputs.

I1: phase_b.total_records >= replay_fixture.records
"""

from __future__ import annotations


def test_run_invariants_returns_empty_for_clean_run() -> None:
    from genie_space_optimizer.optimization.invariants import run_invariants

    evidence = {
        "phase_b": {"total_records": 20, "producer_exceptions": {}},
        "replay_fixture_records": 12,
        "iterations": [],
        "manifest": {"declared_paths": [], "materialized_paths": []},
        "convergence": {"reason": "lever_loop_completed"},
    }
    violations = run_invariants(evidence)
    assert violations == []


def test_i1_red_when_phase_b_total_below_replay_records() -> None:
    from genie_space_optimizer.optimization.invariants import check_i1_phase_b_records_present

    evidence = {
        "phase_b": {"total_records": 0, "producer_exceptions": {"ag_outcome": 2}},
        "replay_fixture_records": 12,
    }
    violations = check_i1_phase_b_records_present(evidence)
    assert len(violations) == 1
    v = violations[0]
    assert v["invariant_id"] == "I1"
    assert v["phase_b_total_records"] == 0
    assert v["replay_fixture_records"] == 12


def test_i1_green_when_phase_b_total_meets_replay_records() -> None:
    from genie_space_optimizer.optimization.invariants import check_i1_phase_b_records_present

    evidence = {
        "phase_b": {"total_records": 24, "producer_exceptions": {}},
        "replay_fixture_records": 24,
    }
    assert check_i1_phase_b_records_present(evidence) == []


def test_loop_invariants_flag_default_on(monkeypatch) -> None:
    monkeypatch.delenv("GSO_LOOP_INVARIANTS_ENABLED", raising=False)
    from genie_space_optimizer.common.config import loop_invariants_enabled
    assert loop_invariants_enabled() is True


def test_loop_invariants_strict_flag_default_on(monkeypatch) -> None:
    monkeypatch.delenv("GSO_LOOP_INVARIANTS_STRICT", raising=False)
    from genie_space_optimizer.common.config import loop_invariants_strict
    assert loop_invariants_strict() is True


def test_target_delta_strict_flag_default_on(monkeypatch) -> None:
    monkeypatch.delenv("GSO_TARGET_DELTA_STRICT", raising=False)
    from genie_space_optimizer.common.config import target_delta_strict_enabled
    assert target_delta_strict_enabled() is True


def test_target_delta_strict_flag_off_via_env(monkeypatch) -> None:
    monkeypatch.setenv("GSO_TARGET_DELTA_STRICT", "0")
    from genie_space_optimizer.common.config import target_delta_strict_enabled
    assert target_delta_strict_enabled() is False


def test_partial_harvest_with_debt_flag_default_off(monkeypatch) -> None:
    monkeypatch.delenv("GSO_PARTIAL_HARVEST_WITH_DEBT", raising=False)
    from genie_space_optimizer.common.config import (
        partial_harvest_with_debt_enabled,
    )
    assert partial_harvest_with_debt_enabled() is False


def test_partial_harvest_with_debt_flag_on_via_env(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PARTIAL_HARVEST_WITH_DEBT", "1")
    from genie_space_optimizer.common.config import (
        partial_harvest_with_debt_enabled,
    )
    assert partial_harvest_with_debt_enabled() is True


def test_patch_subset_isolation_flag_default_off(monkeypatch) -> None:
    monkeypatch.delenv("GSO_PATCH_SUBSET_ISOLATION", raising=False)
    from genie_space_optimizer.common.config import (
        patch_subset_isolation_enabled,
    )
    assert patch_subset_isolation_enabled() is False


def test_patch_subset_isolation_flag_on_via_env(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PATCH_SUBSET_ISOLATION", "1")
    from genie_space_optimizer.common.config import (
        patch_subset_isolation_enabled,
    )
    assert patch_subset_isolation_enabled() is True


def test_patch_subset_isolation_live_flag_default_off(monkeypatch) -> None:
    monkeypatch.delenv("GSO_PATCH_SUBSET_ISOLATION_LIVE", raising=False)
    from genie_space_optimizer.common.config import (
        patch_subset_isolation_live_enabled,
    )
    assert patch_subset_isolation_live_enabled() is False


def test_patch_subset_isolation_live_flag_on_via_env(monkeypatch) -> None:
    monkeypatch.setenv("GSO_PATCH_SUBSET_ISOLATION_LIVE", "1")
    from genie_space_optimizer.common.config import (
        patch_subset_isolation_live_enabled,
    )
    assert patch_subset_isolation_live_enabled() is True


def test_i2_red_when_patch_lever_outside_ag_levers() -> None:
    from genie_space_optimizer.optimization.invariants import check_i2_lever_coherence

    evidence = {
        "iterations": [{
            "iteration": 1,
            "ags": [{
                "id": "AG1",
                "levers": [1, 5],
                "source_cluster_ids": ["H002"],
            }],
            "applied_patches": [
                {"ag_id": "AG1", "lever": 6, "proposal_id": "P5"},
            ],
            "clusters": [{"cluster_id": "H002", "recommended_levers": [3, 5]}],
        }],
    }
    violations = check_i2_lever_coherence(evidence)
    assert any(v["invariant_id"] == "I2" and "patch_lever_outside_ag" in v["title"]
               for v in violations), violations


def test_i2_red_when_ag_levers_missing_recommended() -> None:
    from genie_space_optimizer.optimization.invariants import check_i2_lever_coherence

    evidence = {
        "iterations": [{
            "iteration": 1,
            "ags": [{
                "id": "AG1",
                "levers": [1, 5],
                "source_cluster_ids": ["H002"],
            }],
            "applied_patches": [
                {"ag_id": "AG1", "lever": 5, "proposal_id": "P1"},
            ],
            "clusters": [{"cluster_id": "H002", "recommended_levers": [3, 5]}],
        }],
    }
    violations = check_i2_lever_coherence(evidence)
    assert any(v["invariant_id"] == "I2" and "ag_levers_missing_recommended" in v["title"]
               for v in violations), violations


def test_i2_green_when_levers_coherent() -> None:
    from genie_space_optimizer.optimization.invariants import check_i2_lever_coherence

    evidence = {
        "iterations": [{
            "iteration": 1,
            "ags": [{
                "id": "AG1",
                "levers": [3, 5, 6],
                "source_cluster_ids": ["H002"],
            }],
            "applied_patches": [
                {"ag_id": "AG1", "lever": 5, "proposal_id": "P1"},
                {"ag_id": "AG1", "lever": 6, "proposal_id": "P2"},
            ],
            "clusters": [{"cluster_id": "H002", "recommended_levers": [3, 5]}],
        }],
    }
    assert check_i2_lever_coherence(evidence) == []


def test_i3_red_when_buckets_do_not_partition_target_qids() -> None:
    from genie_space_optimizer.optimization.invariants import check_i3_acceptance_buckets

    evidence = {"iterations": [{
        "iteration": 1,
        "acceptance_decision": {
            "target_qids": ["q_026"],
            "target_fixed_qids": [],
            "target_still_hard_qids": [],
            "reason_code": "target_qids_not_improved",
        },
    }]}
    violations = check_i3_acceptance_buckets(evidence)
    assert any(v["invariant_id"] == "I3" for v in violations), violations


def test_i3_green_when_buckets_partition_and_reason_names_bucket() -> None:
    from genie_space_optimizer.optimization.invariants import check_i3_acceptance_buckets

    evidence = {"iterations": [{
        "iteration": 1,
        "acceptance_decision": {
            "target_qids": ["q_026"],
            "target_fixed_qids": [],
            "target_still_hard_qids": ["q_026"],
            "reason_code": "target_still_hard_qids",
        },
    }]}
    assert check_i3_acceptance_buckets(evidence) == []


def test_i3_red_when_qid_in_two_buckets() -> None:
    from genie_space_optimizer.optimization.invariants import check_i3_acceptance_buckets

    evidence = {"iterations": [{
        "iteration": 1,
        "acceptance_decision": {
            "target_qids": ["q_026"],
            "target_fixed_qids": ["q_026"],
            "target_still_hard_qids": ["q_026"],
            "reason_code": "target_fixed_qids",
        },
    }]}
    assert any(
        v["invariant_id"] == "I3" and "double_counted" in v["title"]
        for v in check_i3_acceptance_buckets(evidence)
    )


def test_i4_red_on_consecutive_empty_proposals_same_ag() -> None:
    from genie_space_optimizer.optimization.invariants import check_i4_no_silent_retry

    evidence = {"iterations": [
        {"iteration": 1, "selected_ag_id": "AG1", "proposal_count": 0},
        {"iteration": 2, "selected_ag_id": "AG1", "proposal_count": 0},
    ]}
    violations = check_i4_no_silent_retry(evidence)
    assert any(v["invariant_id"] == "I4" for v in violations)


def test_i4_red_on_same_body_fingerprint_set_after_rollback() -> None:
    from genie_space_optimizer.optimization.invariants import check_i4_no_silent_retry

    evidence = {"iterations": [
        {
            "iteration": 1, "selected_ag_id": "AG_H004",
            "proposal_count": 4,
            "applied_patch_body_fingerprints": ["fp_a", "fp_b"],
            "acceptance_decision": {"reason_code": "target_still_hard_qids"},
        },
        {
            "iteration": 2, "selected_ag_id": "AG_H004",
            "proposal_count": 4,
            "applied_patch_body_fingerprints": ["fp_a", "fp_b"],
            "acceptance_decision": {},
        },
    ]}
    violations = check_i4_no_silent_retry(evidence)
    assert any(
        v["invariant_id"] == "I4" and "same_body_fingerprints" in v["title"]
        for v in violations
    )


def test_i4_green_when_ag_rotates() -> None:
    from genie_space_optimizer.optimization.invariants import check_i4_no_silent_retry

    evidence = {"iterations": [
        {"iteration": 1, "selected_ag_id": "AG_H004", "proposal_count": 0},
        {"iteration": 2, "selected_ag_id": "AG_H001", "proposal_count": 3},
    ]}
    assert check_i4_no_silent_retry(evidence) == []


def test_i5_red_on_illegal_trunk_transitions() -> None:
    from genie_space_optimizer.optimization.invariants import check_i5_replay_validity

    evidence = {
        "replay_validation": {
            "is_valid": False,
            "violation_count": 4,
            "violation_details": {"trunk: clustered -> already_passing": 4},
        },
    }
    violations = check_i5_replay_validity(evidence)
    assert any(v["invariant_id"] == "I5" for v in violations)
    assert violations[0]["violation_count"] == 4


def test_i5_green_when_replay_is_valid() -> None:
    from genie_space_optimizer.optimization.invariants import check_i5_replay_validity

    evidence = {
        "replay_validation": {
            "is_valid": True, "violation_count": 0, "violation_details": {},
        },
    }
    assert check_i5_replay_validity(evidence) == []


def test_i6_red_when_declared_paths_not_materialized() -> None:
    from genie_space_optimizer.optimization.invariants import check_i6_manifest_paths

    evidence = {"manifest": {
        "declared_paths": ["a", "b", "c"],
        "materialized_paths": ["a"],
    }}
    violations = check_i6_manifest_paths(evidence)
    assert any(v["invariant_id"] == "I6" for v in violations)
    v = violations[0]
    assert sorted(v["missing_paths"]) == ["b", "c"]


def test_i6_green_when_paths_equal() -> None:
    from genie_space_optimizer.optimization.invariants import check_i6_manifest_paths

    evidence = {"manifest": {
        "declared_paths": ["a", "b"],
        "materialized_paths": ["a", "b"],
    }}
    assert check_i6_manifest_paths(evidence) == []


def test_i7_red_when_open_cluster_has_no_rca_card_or_block_record() -> None:
    from genie_space_optimizer.optimization.invariants import check_i7_rca_grounding

    evidence = {"iterations": [{
        "iteration": 1,
        "open_hard_cluster_ids": ["H001", "H003", "H004", "H005"],
        "rca_cards_present": {"H001": False, "H003": False, "H004": False, "H005": False},
        "decision_records": [],
    }]}
    violations = check_i7_rca_grounding(evidence)
    ungrounded = {v.get("cluster_id") for v in violations if v["invariant_id"] == "I7"}
    assert ungrounded == {"H001", "H003", "H004", "H005"}


def test_i7_green_when_block_record_emitted() -> None:
    from genie_space_optimizer.optimization.invariants import check_i7_rca_grounding

    evidence = {"iterations": [{
        "iteration": 1,
        "open_hard_cluster_ids": ["H001"],
        "rca_cards_present": {"H001": False},
        "decision_records": [
            {
                "decision_type": "cluster_blocked_no_rca",
                "cluster_id": "H001",
            }
        ],
    }]}
    assert check_i7_rca_grounding(evidence) == []


def test_i7_green_when_rca_card_present() -> None:
    from genie_space_optimizer.optimization.invariants import check_i7_rca_grounding

    evidence = {"iterations": [{
        "iteration": 1,
        "open_hard_cluster_ids": ["H002"],
        "rca_cards_present": {"H002": True},
        "decision_records": [],
    }]}
    assert check_i7_rca_grounding(evidence) == []


def test_i8_red_when_plateau_currently_failing_diverges_from_journey() -> None:
    from genie_space_optimizer.optimization.invariants import check_i8_plateau_input

    evidence = {
        "convergence": {"reason": "plateau_no_open_failures"},
        "plateau_input": {
            "source": "candidate_eval",
            "currently_failing_qids": [],
        },
        "final_iteration_journey_hard_qids": [
            "q_007", "q_009", "q_013", "q_024",
        ],
    }
    violations = check_i8_plateau_input(evidence)
    assert any(v["invariant_id"] == "I8" for v in violations)


def test_i8_green_when_inputs_align() -> None:
    from genie_space_optimizer.optimization.invariants import check_i8_plateau_input

    evidence = {
        "convergence": {"reason": "plateau_no_open_failures"},
        "plateau_input": {
            "source": "journey_ledger",
            "currently_failing_qids": [],
        },
        "final_iteration_journey_hard_qids": [],
    }
    assert check_i8_plateau_input(evidence) == []


def test_i8_no_op_when_terminal_reason_is_not_plateau() -> None:
    from genie_space_optimizer.optimization.invariants import check_i8_plateau_input

    evidence = {
        "convergence": {"reason": "lever_loop_completed"},
        "plateau_input": {},
        "final_iteration_journey_hard_qids": ["q_007"],
    }
    assert check_i8_plateau_input(evidence) == []


# ── Cycle 14-T0 — I13 target_delta_states totality ───────────────────


def test_i13_green_when_every_target_has_delta_state() -> None:
    from genie_space_optimizer.optimization.invariants import (
        check_i13_target_delta_totality,
    )

    evidence = {
        "iterations": [
            {
                "iteration": 1,
                "acceptance_decision": {
                    "target_qids": ["gs_026"],
                    "target_fixed_qids": ["gs_026"],
                    "target_still_hard_qids": [],
                    "reason_code": "accepted",
                    "target_delta_states": [["gs_026", "fixed"]],
                },
            }
        ]
    }
    assert check_i13_target_delta_totality(evidence) == []


def test_i13_red_when_target_missing_from_delta_states() -> None:
    from genie_space_optimizer.optimization.invariants import (
        check_i13_target_delta_totality,
    )

    evidence = {
        "iterations": [
            {
                "iteration": 1,
                "acceptance_decision": {
                    "target_qids": ["gs_026", "gs_001"],
                    "target_fixed_qids": [],
                    "target_still_hard_qids": [],
                    "reason_code": "target_qids_not_improved",
                    # gs_001 is missing from the map - the totality bug
                    "target_delta_states": [["gs_026", "fixed"]],
                },
            }
        ]
    }
    violations = check_i13_target_delta_totality(evidence)
    assert len(violations) == 1
    v = violations[0]
    assert v["invariant_id"] == "I13"
    assert v["title"] == "target_delta_states_not_total_over_target_qids"
    assert "gs_001" in v["missing_target_qids"]


def test_i13_red_when_lookup_failed_but_reason_is_legacy() -> None:
    """The new-anchor F2 reproduction at the invariant level: when
    a target landed in lookup_failed, the rollback reason MUST be
    target_resolution_failed. Anything else (e.g.
    target_qids_not_improved) is the silent-failure mode I13
    catches.
    """
    from genie_space_optimizer.optimization.invariants import (
        check_i13_target_delta_totality,
    )

    evidence = {
        "iterations": [
            {
                "iteration": 1,
                "acceptance_decision": {
                    "target_qids": ["gs_026"],
                    "target_fixed_qids": [],
                    "target_still_hard_qids": [],
                    "reason_code": "target_qids_not_improved",
                    "target_delta_states": [["gs_026", "lookup_failed"]],
                },
            }
        ]
    }
    violations = check_i13_target_delta_totality(evidence)
    assert len(violations) == 1
    v = violations[0]
    assert v["invariant_id"] == "I13"
    assert v["title"] == "lookup_failed_with_legacy_reason_code"
    assert v["reason_code"] == "target_qids_not_improved"


def test_i9_green_when_full_eval_marker_absent_on_every_iteration() -> None:
    """Legacy replay fixtures do not capture the typed stdout marker
    payload as a parallel evidence field. I9 must stay silent so
    pre-Cycle-15.1 fixtures remain byte-stable.
    """
    from genie_space_optimizer.optimization.invariants import (
        check_i9_acceptance_render_byte_equality,
    )

    evidence = {
        "iterations": [
            {
                "iteration": 1,
                "acceptance_decision": {
                    "reason_code": "accepted",
                    "target_qids": ["q_001"],
                    "target_fixed_qids": ["q_001"],
                    "target_still_hard_qids": [],
                    "out_of_target_regressed_qids": [],
                },
            },
        ],
    }
    assert check_i9_acceptance_render_byte_equality(evidence) == []


def test_i9_green_when_decision_record_and_marker_agree_byte_for_byte() -> None:
    """Both consumers of format_full_eval_marker_payload render the
    same canonical field set. I9 stays silent.
    """
    from genie_space_optimizer.optimization.invariants import (
        check_i9_acceptance_render_byte_equality,
    )

    canonical = {
        "reason_code": "accepted",
        "accepted": True,
        "target_qids": ["q_001", "q_002"],
        "target_fixed_qids": ["q_001"],
        "target_still_hard_qids": ["q_002"],
        "target_soft_passing_qids": [],
        "out_of_target_regressed_qids": [],
        "soft_to_hard_regressed_qids": [],
        "passing_to_hard_regressed_qids": [],
        "unknown_to_hard_regressed_qids": [],
        "accidentally_improved_qids": [],
        "unresolved_target_debt_qids": [],
    }
    evidence = {
        "iterations": [
            {
                "iteration": 1,
                "acceptance_decision": dict(canonical),
                "full_eval_marker": dict(canonical),
            },
        ],
    }
    assert check_i9_acceptance_render_byte_equality(evidence) == []


def test_i13_green_when_lookup_failed_and_typed_reason() -> None:
    from genie_space_optimizer.optimization.invariants import (
        check_i13_target_delta_totality,
    )

    evidence = {
        "iterations": [
            {
                "iteration": 1,
                "acceptance_decision": {
                    "target_qids": ["gs_026"],
                    "target_fixed_qids": [],
                    "target_still_hard_qids": [],
                    "reason_code": "target_resolution_failed",
                    "target_delta_states": [["gs_026", "lookup_failed"]],
                },
            }
        ]
    }
    assert check_i13_target_delta_totality(evidence) == []


def test_i13_red_when_fixed_state_disagrees_with_target_fixed_qids() -> None:
    """Drift catch: target_delta_states says FIXED but the legacy
    target_fixed_qids tuple is empty (or vice versa). C14-T2's
    canonical render is the eventual closure; until then, I13
    surfaces the disagreement so it does not silently ship.
    """
    from genie_space_optimizer.optimization.invariants import (
        check_i13_target_delta_totality,
    )

    evidence = {
        "iterations": [
            {
                "iteration": 1,
                "acceptance_decision": {
                    "target_qids": ["gs_026"],
                    "target_fixed_qids": [],  # legacy says nothing fixed
                    "target_still_hard_qids": [],
                    "reason_code": "accepted",
                    "target_delta_states": [["gs_026", "fixed"]],  # new says fixed
                },
            }
        ]
    }
    violations = check_i13_target_delta_totality(evidence)
    assert len(violations) == 1
    assert violations[0]["title"] == "target_delta_states_disagrees_with_legacy_buckets"


def test_i13_skipped_for_iterations_without_acceptance_decision() -> None:
    """Empty-AG iterations (proposal_count=0) do not have an
    acceptance decision; I13 must skip them silently.
    """
    from genie_space_optimizer.optimization.invariants import (
        check_i13_target_delta_totality,
    )

    evidence = {
        "iterations": [
            {"iteration": 1, "selected_ag_id": "AG1", "proposal_count": 0},
        ]
    }
    assert check_i13_target_delta_totality(evidence) == []


def test_i13_wired_into_run_invariants() -> None:
    """I13 must appear in the run_invariants aggregator so the merge
    gate (C16-T4) picks it up via canonical ID lookup.
    """
    from genie_space_optimizer.optimization.invariants import run_invariants

    evidence = {
        "iterations": [
            {
                "iteration": 1,
                "acceptance_decision": {
                    "target_qids": ["gs_026"],
                    "target_fixed_qids": [],
                    "target_still_hard_qids": [],
                    "reason_code": "target_qids_not_improved",
                    "target_delta_states": [["gs_026", "lookup_failed"]],
                },
            }
        ],
        "manifest": {"declared_paths": [], "materialized_paths": []},
        "convergence": {"reason": "lever_loop_completed"},
        "phase_b": {"total_records": 0},
        "replay_fixture_records": 0,
    }
    violations = run_invariants(evidence)
    i13 = [v for v in violations if v["invariant_id"] == "I13"]
    assert len(i13) == 1


def test_i9_red_when_target_fixed_qids_disagree_between_record_and_marker() -> None:
    """Reproducer for the D-6 Phase-H acceptance drift shape: the
    record claims gs_024 fixed; the marker claims it still hard.
    I9 fires once per disagreeing field.
    """
    from genie_space_optimizer.optimization.invariants import (
        check_i9_acceptance_render_byte_equality,
    )

    evidence = {
        "iterations": [
            {
                "iteration": 1,
                "acceptance_decision": {
                    "reason_code": "accepted_with_attribution_drift",
                    "accepted": True,
                    "target_qids": ["gs_024"],
                    "target_fixed_qids": ["gs_024"],
                    "target_still_hard_qids": [],
                },
                "full_eval_marker": {
                    "reason_code": "accepted_with_attribution_drift",
                    "accepted": True,
                    "target_qids": ["gs_024"],
                    "target_fixed_qids": [],
                    "target_still_hard_qids": ["gs_024"],
                },
            },
        ],
    }
    violations = check_i9_acceptance_render_byte_equality(evidence)
    fields = {v["field"] for v in violations}
    assert "target_fixed_qids" in fields
    assert "target_still_hard_qids" in fields
    assert all(v["invariant_id"] == "I9" for v in violations)
    assert all(v["iteration"] == 1 for v in violations)


def test_i9_red_when_reason_code_disagrees_between_record_and_marker() -> None:
    """A divergent renderer that re-derives reason_code from a stale
    field would surface here. I9 fires once for the reason_code field.
    """
    from genie_space_optimizer.optimization.invariants import (
        check_i9_acceptance_render_byte_equality,
    )

    evidence = {
        "iterations": [
            {
                "iteration": 2,
                "acceptance_decision": {
                    "reason_code": "accepted",
                    "accepted": True,
                    "target_qids": ["q_a"],
                    "target_fixed_qids": ["q_a"],
                    "target_still_hard_qids": [],
                },
                "full_eval_marker": {
                    "reason_code": "accepted_with_regression_debt",
                    "accepted": True,
                    "target_qids": ["q_a"],
                    "target_fixed_qids": ["q_a"],
                    "target_still_hard_qids": [],
                },
            },
        ],
    }
    violations = check_i9_acceptance_render_byte_equality(evidence)
    assert any(
        v["field"] == "reason_code" and v["iteration"] == 2
        for v in violations
    )


def test_i9_wired_into_run_invariants() -> None:
    """When I9 fires standalone, run_invariants must surface the
    violation in its combined output.
    """
    from genie_space_optimizer.optimization.invariants import run_invariants

    evidence = {
        "phase_b": {"total_records": 1, "producer_exceptions": {}},
        "replay_fixture_records": 1,
        "iterations": [
            {
                "iteration": 1,
                "acceptance_decision": {
                    "reason_code": "accepted",
                    "accepted": True,
                    "target_qids": ["q_a"],
                    "target_fixed_qids": ["q_a"],
                    "target_still_hard_qids": [],
                },
                "full_eval_marker": {
                    "reason_code": "rejected_no_gain",
                    "accepted": False,
                    "target_qids": ["q_a"],
                    "target_fixed_qids": [],
                    "target_still_hard_qids": ["q_a"],
                },
            },
        ],
        "manifest": {"declared_paths": [], "materialized_paths": []},
        "convergence": {"reason": "lever_loop_completed"},
    }
    violations = run_invariants(evidence)
    i9 = [v for v in violations if v["invariant_id"] == "I9"]
    assert i9, f"expected I9 in combined output, got {violations!r}"


def test_i10_green_when_applied_patch_identifiers_absent() -> None:
    """Legacy replay fixtures do not capture applied_patch_identifiers
    as a parallel evidence field. I10 must stay silent.
    """
    from genie_space_optimizer.optimization.invariants import (
        check_i10_applied_patch_id_injective,
    )

    evidence = {"iterations": [{"iteration": 1}, {"iteration": 2}]}
    assert check_i10_applied_patch_id_injective(evidence) == []


def test_i10_red_when_expanded_patch_id_duplicated_across_iterations() -> None:
    """Reproducer for stamper-bypass shape: the same expanded id
    appears in iter 1 and iter 2 — patch-subset isolation cannot
    distinguish them.
    """
    from genie_space_optimizer.optimization.invariants import (
        check_i10_applied_patch_id_injective,
    )

    evidence = {
        "iterations": [
            {
                "iteration": 1,
                "applied_patch_identifiers": [
                    {
                        "expanded_patch_id": "P001#1",
                        "parent_proposal_id": "P001",
                        "lever": 6,
                    },
                ],
            },
            {
                "iteration": 2,
                "applied_patch_identifiers": [
                    {
                        "expanded_patch_id": "P001#1",
                        "parent_proposal_id": "P001",
                        "lever": 6,
                    },
                ],
            },
        ],
    }
    violations = check_i10_applied_patch_id_injective(evidence)
    dup = [v for v in violations if v["title"] == "duplicate_expanded_patch_id"]
    assert len(dup) == 1
    assert dup[0]["expanded_patch_id"] == "P001#1"
    assert dup[0]["first_iteration"] == 1
    assert dup[0]["iteration"] == 2


def test_i10_red_when_parent_proposal_lever_pair_collides_within_iteration() -> None:
    """Two applied patches with the same parent proposal id and the
    same lever inside a single iteration is a stamping bug; their
    expanded ids should have distinct suffixes.
    """
    from genie_space_optimizer.optimization.invariants import (
        check_i10_applied_patch_id_injective,
    )

    evidence = {
        "iterations": [
            {
                "iteration": 1,
                "applied_patch_identifiers": [
                    {
                        "expanded_patch_id": "P007#1",
                        "parent_proposal_id": "P007",
                        "lever": 5,
                    },
                    {
                        "expanded_patch_id": "P007#2",
                        "parent_proposal_id": "P007",
                        "lever": 5,
                    },
                ],
            },
        ],
    }
    violations = check_i10_applied_patch_id_injective(evidence)
    col = [
        v for v in violations
        if v["title"] == "duplicate_parent_lever_within_iteration"
    ]
    assert len(col) == 1
    assert col[0]["parent_proposal_id"] == "P007"
    assert col[0]["lever"] == "5"


def test_i10_red_when_expanded_patch_id_is_empty() -> None:
    """A patch that bypassed the stamper has no expanded_patch_id —
    every downstream id-keyed contract (patch-subset isolation,
    per-iteration bundle paths) breaks. I10 fires once.
    """
    from genie_space_optimizer.optimization.invariants import (
        check_i10_applied_patch_id_injective,
    )

    evidence = {
        "iterations": [
            {
                "iteration": 1,
                "applied_patch_identifiers": [
                    {
                        "expanded_patch_id": "",
                        "parent_proposal_id": "P009",
                        "lever": 2,
                    },
                ],
            },
        ],
    }
    violations = check_i10_applied_patch_id_injective(evidence)
    empty = [v for v in violations if v["title"] == "empty_expanded_patch_id"]
    assert len(empty) == 1
    assert empty[0]["iteration"] == 1


def test_i10_wired_into_run_invariants() -> None:
    """A duplicate expanded_patch_id must surface in run_invariants's
    combined output, tagged with invariant_id='I10'.
    """
    from genie_space_optimizer.optimization.invariants import run_invariants

    evidence = {
        "phase_b": {"total_records": 1, "producer_exceptions": {}},
        "replay_fixture_records": 1,
        "iterations": [
            {
                "iteration": 1,
                "applied_patch_identifiers": [
                    {
                        "expanded_patch_id": "P001#1",
                        "parent_proposal_id": "P001",
                        "lever": 6,
                    },
                ],
            },
            {
                "iteration": 2,
                "applied_patch_identifiers": [
                    {
                        "expanded_patch_id": "P001#1",
                        "parent_proposal_id": "P001",
                        "lever": 6,
                    },
                ],
            },
        ],
        "manifest": {"declared_paths": [], "materialized_paths": []},
        "convergence": {"reason": "lever_loop_completed"},
    }
    violations = run_invariants(evidence)
    i10 = [v for v in violations if v["invariant_id"] == "I10"]
    assert i10, f"expected I10 in combined output, got {violations!r}"
