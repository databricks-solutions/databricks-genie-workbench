"""P-E1 — I14 invariant: ``check_i14_l6_decline_dedup``.

At most one *live* (``cached=False``) ``lever6_force_llm_declined``
decision record per ``(iteration, cluster_signature, root_cause)``
tuple. Cached records (``cached=True``) are unbounded — they are the
intended dedup mechanism in action.

Reads the same ``evidence["iterations"][i]["decision_records"]``
surface used by ``check_i7_rca_grounding`` (see ``invariants.py:319``).
The cluster-signature key is extracted from the record's
``evidence_refs`` (``signature:<sig>`` token emitted by P-E1's record
factory extension).
"""
from __future__ import annotations


def _live_decline_record(
    *,
    iteration: int,
    cluster_signature: str,
    root_cause: str,
    ag_id: str = "AG_X",
    cluster_id: str = "H004",
) -> dict:
    return {
        "iteration": int(iteration),
        "decision_type": "proposal_generated",
        "reason_code": "lever6_force_llm_declined",
        "ag_id": ag_id,
        "cluster_id": cluster_id,
        "root_cause": root_cause,
        "evidence_refs": [
            f"ag:{ag_id}",
            f"cluster:{cluster_id}",
            f"signature:{cluster_signature}",
        ],
        "metrics": {"cached": False, "original_decline_iteration": None},
    }


def _cached_decline_record(
    *,
    iteration: int,
    cluster_signature: str,
    root_cause: str,
    original_decline_iteration: int,
) -> dict:
    rec = _live_decline_record(
        iteration=iteration,
        cluster_signature=cluster_signature,
        root_cause=root_cause,
    )
    rec["metrics"] = {
        "cached": True,
        "original_decline_iteration": int(original_decline_iteration),
    }
    return rec


def test_i14_silent_when_no_lever6_declines():
    from genie_space_optimizer.optimization.invariants import (
        check_i14_l6_decline_dedup,
    )
    evidence = {
        "iterations": [
            {"iteration": 1, "decision_records": []},
            {"iteration": 2, "decision_records": [
                {"reason_code": "applied_patch_target_fixed",
                 "evidence_refs": []},
            ]},
        ]
    }
    assert check_i14_l6_decline_dedup(evidence) == []


def test_i14_silent_for_one_live_plus_many_cached():
    from genie_space_optimizer.optimization.invariants import (
        check_i14_l6_decline_dedup,
    )
    iter_records = [
        _live_decline_record(
            iteration=2, cluster_signature="sig_A",
            root_cause="missing_filter",
        ),
        _cached_decline_record(
            iteration=2, cluster_signature="sig_A",
            root_cause="missing_filter",
            original_decline_iteration=2,
        ),
        _cached_decline_record(
            iteration=2, cluster_signature="sig_A",
            root_cause="missing_filter",
            original_decline_iteration=2,
        ),
    ]
    evidence = {
        "iterations": [
            {"iteration": 2, "decision_records": iter_records},
        ]
    }
    assert check_i14_l6_decline_dedup(evidence) == []


def test_i14_silent_across_iterations():
    """A live decline per iteration is fine — the cache lifetime is
    per-iteration."""
    from genie_space_optimizer.optimization.invariants import (
        check_i14_l6_decline_dedup,
    )
    evidence = {
        "iterations": [
            {"iteration": 1, "decision_records": [
                _live_decline_record(
                    iteration=1, cluster_signature="sig_A",
                    root_cause="missing_filter",
                ),
            ]},
            {"iteration": 2, "decision_records": [
                _live_decline_record(
                    iteration=2, cluster_signature="sig_A",
                    root_cause="missing_filter",
                ),
            ]},
        ]
    }
    assert check_i14_l6_decline_dedup(evidence) == []


def test_i14_silent_for_different_root_causes_same_signature():
    from genie_space_optimizer.optimization.invariants import (
        check_i14_l6_decline_dedup,
    )
    iter_records = [
        _live_decline_record(
            iteration=2, cluster_signature="sig_A",
            root_cause="missing_filter",
        ),
        _live_decline_record(
            iteration=2, cluster_signature="sig_A",
            root_cause="missing_join",
        ),
    ]
    evidence = {
        "iterations": [
            {"iteration": 2, "decision_records": iter_records},
        ]
    }
    assert check_i14_l6_decline_dedup(evidence) == []


def test_i14_fires_on_two_live_declines_same_iter_sig_root_cause():
    from genie_space_optimizer.optimization.invariants import (
        check_i14_l6_decline_dedup,
    )
    iter_records = [
        _live_decline_record(
            iteration=2, cluster_signature="sig_A",
            root_cause="missing_filter",
        ),
        _live_decline_record(
            iteration=2, cluster_signature="sig_A",
            root_cause="missing_filter",
        ),
    ]
    evidence = {
        "iterations": [
            {"iteration": 2, "decision_records": iter_records},
        ]
    }
    violations = check_i14_l6_decline_dedup(evidence)
    assert len(violations) == 1
    v = violations[0]
    assert v["invariant_id"] == "I14"
    assert v["title"] == "lever6_force_llm_declined_dedup_violation"
    assert v["iteration"] == 2
    assert v["cluster_signature"] == "sig_A"
    assert v["root_cause"] == "missing_filter"
    assert v["live_decline_count"] == 2


def test_i14_fires_per_distinct_group():
    """Two distinct (sig, root_cause) groups each with 2 live declines
    produce two violations."""
    from genie_space_optimizer.optimization.invariants import (
        check_i14_l6_decline_dedup,
    )
    iter_records = [
        _live_decline_record(
            iteration=2, cluster_signature="sig_A",
            root_cause="missing_filter",
        ),
        _live_decline_record(
            iteration=2, cluster_signature="sig_A",
            root_cause="missing_filter",
        ),
        _live_decline_record(
            iteration=2, cluster_signature="sig_B",
            root_cause="missing_join",
        ),
        _live_decline_record(
            iteration=2, cluster_signature="sig_B",
            root_cause="missing_join",
        ),
    ]
    evidence = {
        "iterations": [
            {"iteration": 2, "decision_records": iter_records},
        ]
    }
    violations = check_i14_l6_decline_dedup(evidence)
    assert len(violations) == 2
    keys = {(v["cluster_signature"], v["root_cause"]) for v in violations}
    assert keys == {("sig_A", "missing_filter"), ("sig_B", "missing_join")}


def test_i14_skips_records_without_cluster_signature_evidence_ref():
    """Legacy records (no ``signature:*`` token) cannot be grouped and
    therefore cannot violate the invariant. They are silently skipped
    so I14 stays back-compat with pre-P-E1 fixtures."""
    from genie_space_optimizer.optimization.invariants import (
        check_i14_l6_decline_dedup,
    )
    legacy = {
        "iteration": 2,
        "reason_code": "lever6_force_llm_declined",
        "evidence_refs": ["ag:AG_X", "cluster:H004"],  # no signature:*
        "metrics": {"cached": False, "original_decline_iteration": None},
        "root_cause": "missing_filter",
    }
    evidence = {
        "iterations": [
            {"iteration": 2, "decision_records": [legacy, legacy]},
        ]
    }
    assert check_i14_l6_decline_dedup(evidence) == []


def test_i14_registered_in_run_invariants():
    from genie_space_optimizer.optimization import invariants as inv_mod
    src = inv_mod.run_invariants.__code__.co_consts
    # The check must be referenced inside run_invariants' tuple.
    assert any(
        getattr(c, "__name__", "") == "check_i14_l6_decline_dedup"
        for c in src if callable(c)
    ) or "check_i14_l6_decline_dedup" in (
        inv_mod.run_invariants.__code__.co_names
    )
