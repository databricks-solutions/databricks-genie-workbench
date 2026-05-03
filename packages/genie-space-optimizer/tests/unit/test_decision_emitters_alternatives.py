"""Phase D.5 — alternatives capture across the three trace-aware producers."""


def test_cluster_records_stamps_alternatives_on_each_record() -> None:
    from genie_space_optimizer.optimization.decision_emitters import cluster_records
    from genie_space_optimizer.optimization.rca_decision_trace import (
        AlternativeOption,
        RejectReason,
    )

    records = cluster_records(
        run_id="run_1",
        iteration=1,
        clusters=[
            {
                "cluster_id": "H001",
                "question_ids": ["q1", "q2"],
                "root_cause": "missing_filter",
            },
        ],
        rca_id_by_cluster={"H001": "rca_h001"},
        cluster_alternatives_by_id={
            "H001": (
                AlternativeOption(
                    option_id="C_005",
                    kind="cluster",
                    reject_reason=RejectReason.BELOW_HARD_THRESHOLD,
                    reject_detail="qid count 1 < hard_threshold=2",
                ),
                AlternativeOption(
                    option_id="C_007",
                    kind="cluster",
                    reject_reason=RejectReason.INSUFFICIENT_QIDS,
                ),
            ),
        },
    )

    assert len(records) == 1
    rec = records[0]
    assert rec.cluster_id == "H001"
    assert len(rec.alternatives_considered) == 2
    assert {opt.option_id for opt in rec.alternatives_considered} == {
        "C_005", "C_007",
    }


def test_cluster_records_default_alternatives_is_empty_tuple() -> None:
    from genie_space_optimizer.optimization.decision_emitters import cluster_records

    records = cluster_records(
        run_id="run_1",
        iteration=1,
        clusters=[{
            "cluster_id": "H001",
            "question_ids": ["q1"],
            "root_cause": "missing_filter",
        }],
        rca_id_by_cluster={"H001": "rca_h001"},
    )
    assert records[0].alternatives_considered == ()


def test_cluster_records_ignores_alternatives_for_unknown_cluster_id() -> None:
    from genie_space_optimizer.optimization.decision_emitters import cluster_records
    from genie_space_optimizer.optimization.rca_decision_trace import (
        AlternativeOption, RejectReason,
    )

    records = cluster_records(
        run_id="run_1",
        iteration=1,
        clusters=[{
            "cluster_id": "H001",
            "question_ids": ["q1", "q2"],
            "root_cause": "missing_filter",
        }],
        rca_id_by_cluster={"H001": "rca_h001"},
        cluster_alternatives_by_id={
            "H999": (
                AlternativeOption(
                    option_id="C_005",
                    kind="cluster",
                    reject_reason=RejectReason.BELOW_HARD_THRESHOLD,
                ),
            ),
        },
    )
    assert records[0].alternatives_considered == ()
