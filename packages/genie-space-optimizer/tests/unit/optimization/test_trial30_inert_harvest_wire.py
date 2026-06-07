from genie_space_optimizer.optimization.inert_mechanism_history import (
    extend_sm_inert_mechanism_history,
    harvest_sm_inert_mechanism_history,
)
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
)


def _reroute_record(rejected: str) -> AcceptanceDecisionRecord:
    return AcceptanceDecisionRecord(
        decision="kit_forced_inert_reroute",
        arbiter_reason="kit_forced_inert_reroute:behavior=unchanged",
        target_fixed=False,
        collateral_regressions=(),
        behavioral_diff="unchanged",
        insufficient_repair_signature=(
            f"{rejected}:add_example_sql:kit_forced_inert:"
            "rca=top_n_cardinality_collapse:behavior=unchanged"
        ),
        rejected_mechanism=rejected,
    )


def test_harvest_then_extend_accumulates_across_iterations():
    rec = _reroute_record("lever-5")
    fresh = harvest_sm_inert_mechanism_history(
        [rec], qid_rca_pairs=[("gs_026", "top_n_cardinality_collapse")]
    )
    assert len(fresh) == 1
    assert fresh[0].qid == "gs_026"
    assert fresh[0].rejected_mechanisms == ("lever-5",)

    # Second iteration rejects a different mechanism for the same pair.
    rec2 = _reroute_record("lever-1")
    fresh2 = harvest_sm_inert_mechanism_history(
        [rec2], qid_rca_pairs=[("gs_026", "top_n_cardinality_collapse")]
    )
    merged = extend_sm_inert_mechanism_history(fresh, fresh2)
    assert len(merged) == 1
    assert merged[0].rejected_mechanisms == ("lever-5", "lever-1")


def test_non_reroute_records_do_not_contribute():
    rec = AcceptanceDecisionRecord(
        decision="accepted",
        arbiter_reason="",
        target_fixed=True,
        collateral_regressions=(),
    )
    fresh = harvest_sm_inert_mechanism_history(
        [rec], qid_rca_pairs=[("gs_001", "wrong_column")]
    )
    assert fresh == ()


def test_harness_extractors_pair_records_and_qid_rca():
    # Pins the harness-side extractor contract: positional records and
    # (qid, rca) pairs from final SM states feed the harvest helper.
    from genie_space_optimizer.optimization.harness import (
        _inert_qid_rca_pairs_from_states,
        _t30_acceptance_records_from_states,
    )

    class _Diag:
        rca_kind_label = "top-n cardinality collapse"

    class _State:
        qid = "gs_026"
        diagnosed = _Diag()
        accepted = _reroute_record("lever-5")

    states = [_State()]
    records = _t30_acceptance_records_from_states(states)
    pairs = _inert_qid_rca_pairs_from_states(states)
    assert len(records) == len(pairs) == 1
    assert records[0].decision == "kit_forced_inert_reroute"
    assert pairs[0][0] == "gs_026"
    # rca is canonicalized to the RCA_CANONICAL_KEY_SET form.
    assert pairs[0][1] == "top_n_cardinality_collapse"

    harvested = harvest_sm_inert_mechanism_history(
        records, qid_rca_pairs=pairs
    )
    assert len(harvested) == 1
    assert harvested[0].qid == "gs_026"
    assert harvested[0].rca_kind == "top_n_cardinality_collapse"
