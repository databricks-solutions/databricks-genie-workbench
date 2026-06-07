"""Trial 29 W29.1 — InertMechanismHistory typed model + harvest.

Mirrors the threading pattern of ``forbidden_signatures.harvest_sm_*``
but for kit-forced inert-patch rejections. The harvest function reads
all AcceptanceDecisionRecord entries with
``decision == "kit_forced_inert_reroute"`` and aggregates them per
``(qid, rca_kind)`` so the next iteration's ``TransformerContext`` has
the history.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.inert_mechanism_history import (
    InertMechanismHistory,
    extend_sm_inert_mechanism_history,
    harvest_sm_inert_mechanism_history,
)
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
)


def _stub_record(*, decision: str) -> AcceptanceDecisionRecord:
    return AcceptanceDecisionRecord(
        decision=decision,  # type: ignore[arg-type]
        arbiter_reason="",
        target_fixed=False,
        collateral_regressions=(),
    )


def test_history_model_round_trip():
    history = InertMechanismHistory(
        qid="gs_009",
        rca_kind="wrong_aggregation",
        rejected_mechanisms=("add_sql_snippet_filter",),
        signatures=(
            "add_sql_snippet_filter:filter:insufficient:"
            "rca=wrong_aggregation:behavior=unchanged",
        ),
    )
    blob = history.model_dump()
    rebuilt = InertMechanismHistory.model_validate(blob)
    assert rebuilt == history


def test_harvest_extracts_only_kit_forced_inert_reroute():
    records = [
        _stub_record(decision="accepted"),
        _stub_record(decision="rolled_back"),
        _stub_record(decision="kept_insufficient"),
        AcceptanceDecisionRecord(
            decision="kit_forced_inert_reroute",
            arbiter_reason="kit_forced_inert_reroute:behavior=unchanged",
            target_fixed=False,
            collateral_regressions=(),
            insufficient_repair_signature=(
                "add_sql_snippet_filter:filter:insufficient:"
                "rca=wrong_aggregation:behavior=unchanged"
            ),
            behavioral_diff="unchanged",
            rejected_mechanism="add_sql_snippet_filter",
        ),
    ]
    qid_rca_pairs = [
        ("gs_001", "other"),
        ("gs_002", "other"),
        ("gs_003", "other"),
        ("gs_009", "wrong_aggregation"),
    ]
    out = harvest_sm_inert_mechanism_history(
        records, qid_rca_pairs=qid_rca_pairs
    )
    assert len(out) == 1
    assert out[0].qid == "gs_009"
    assert out[0].rca_kind == "wrong_aggregation"
    assert out[0].rejected_mechanisms == ("add_sql_snippet_filter",)


def test_extend_accumulates_across_iterations():
    prior = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=("sig1",),
        ),
    )
    fresh = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("replace_join",),
            signatures=("sig2",),
        ),
    )
    merged = extend_sm_inert_mechanism_history(prior, fresh)
    assert len(merged) == 1
    assert merged[0].qid == "gs_009"
    assert merged[0].rejected_mechanisms == (
        "add_sql_snippet_filter",
        "replace_join",
    )
    assert merged[0].signatures == ("sig1", "sig2")


def test_extend_keeps_distinct_qid_rca_pairs_separate():
    prior = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=("sig1",),
        ),
    )
    fresh = (
        InertMechanismHistory(
            qid="gs_026",
            rca_kind="plural_top_n_collapse",
            rejected_mechanisms=("add_example_sql",),
            signatures=("sig2",),
        ),
    )
    merged = extend_sm_inert_mechanism_history(prior, fresh)
    assert len(merged) == 2
    qids = {h.qid for h in merged}
    assert qids == {"gs_009", "gs_026"}


def test_extend_dedupes_same_mechanism_within_qid():
    prior = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=("sig1",),
        ),
    )
    fresh = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=("sig1",),
        ),
    )
    merged = extend_sm_inert_mechanism_history(prior, fresh)
    assert merged[0].rejected_mechanisms == ("add_sql_snippet_filter",)
    assert merged[0].signatures == ("sig1",)


# ── Phase 5: TransformerContext + cluster_batch threading ─────────────


def test_transformer_context_carries_inert_mechanism_history():
    from genie_space_optimizer.optimization.state_machine.verdict import (
        TransformerContext,
        ValidationContext,
    )

    ctx = TransformerContext(
        1, "r", ValidationContext(1, "r", {}),
    )
    assert ctx.inert_mechanism_history == ()

    history = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=("sig1",),
        ),
    )
    ctx2 = TransformerContext(
        1, "r", ValidationContext(1, "r", {}),
        inert_mechanism_history=history,
    )
    assert ctx2.inert_mechanism_history == history


def test_cluster_batch_propagates_inert_mechanism_history():
    from genie_space_optimizer.optimization.state_machine.transformers.cluster_batch import (  # noqa: E501
        Stage2BatchInput,
        build_stage2_batch_input,
    )

    history = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=("sig1",),
        ),
    )
    batch = build_stage2_batch_input(
        (),  # no diagnosed states needed for this test
        forbidden_signatures=(),
        insufficient_repair_signatures=(),
        inert_mechanism_history=history,
    )
    assert isinstance(batch, Stage2BatchInput)
    assert batch.inert_mechanism_history == history


def test_cluster_batch_default_empty_inert_history():
    """Default for the new param is empty tuple — back-compat for
    every existing caller that doesn't pass it."""
    from genie_space_optimizer.optimization.state_machine.transformers.cluster_batch import (  # noqa: E501
        build_stage2_batch_input,
    )

    batch = build_stage2_batch_input(
        (),
        forbidden_signatures=(),
    )
    assert batch.inert_mechanism_history == ()
