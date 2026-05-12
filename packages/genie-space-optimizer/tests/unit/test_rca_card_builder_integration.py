from __future__ import annotations

import os
from unittest.mock import patch

from genie_space_optimizer.optimization.rca import build_rca_card


def _evidence_pack() -> dict:
    """Realistic ASI evidence for ccf1d60d's gs_026 cluster."""
    return {
        "asi_metadata": {
            "gs_026": {
                "failure_type": "plural_top_n_collapse",
                "blame_set": ["zone_vp_name", "plural_top_n_collapse"],
                "counterfactual_fix": (
                    "Add an explicit ORDER BY zone_vp_name and LIMIT to preserve "
                    "plural top-N cardinality."
                ),
                "wrong_clause": "TOP 1",
            },
        },
        "generated_sql_by_qid": {
            "gs_026": "SELECT TOP 1 zone_combination FROM mv_7now_store_sales",
        },
        "reference_sql_by_qid": {
            "gs_026": (
                "SELECT zone_vp_name, SUM(sales) FROM mv_esr_dim_location "
                "GROUP BY zone_vp_name ORDER BY SUM(sales) DESC LIMIT 5"
            ),
        },
    }


def test_builder_returns_rca_id_when_evidence_grounds() -> None:
    cluster = {"primary_cluster_id": "cluster_1", "target_qids": ("gs_026",)}
    metadata_snapshot = {"_rca_card_store": {}}
    pack = _evidence_pack()
    with patch.dict(os.environ, {"GSO_RCA_CARD_BUILDER": "1"}, clear=True):
        out = build_rca_card(
            cluster_id="cluster_1",
            qids=("gs_026",),
            failure_buckets={},
            asi_metadata=pack["asi_metadata"],
            generated_sql_by_qid=pack["generated_sql_by_qid"],
            reference_sql_by_qid=pack["reference_sql_by_qid"],
            metadata_snapshot=metadata_snapshot,
            cluster=cluster,
        )
    assert isinstance(out, dict)
    assert out.get("rca_id"), "Builder must return a non-empty rca_id when evidence grounds"
    # Side-effect: cluster mutated with rca_card_id
    assert cluster.get("rca_card_id") == out["rca_id"]
    # Side-effect: card stored in the metadata snapshot for AG selection
    store = metadata_snapshot["_rca_card_store"]
    assert out["rca_id"] in store


def test_builder_returns_empty_when_self_grounding_fails(monkeypatch) -> None:
    """When build_card's self_grounding_check returns ok=False (verified
    in unit tests for self_grounding directly), build_rca_card returns
    the legacy stub shape AND records a typed failure in the snapshot
    so the caller can emit ``rca_card_self_check_failed``. The plan
    deterministic flow makes self-grounding tautologically true since
    grounding terms come from blame_set; we monkeypatch
    ``rca_card_builder.self_grounding_check`` to simulate the
    structural-bug case where a future code path might inject a
    mismatched grounding source."""
    cluster = {"primary_cluster_id": "cluster_2", "target_qids": ("gs_001",)}
    metadata_snapshot: dict = {"_rca_card_store": {}}

    from genie_space_optimizer.optimization import rca_card_builder

    def _force_fail(**kwargs):
        return rca_card_builder.SelfGroundingResult(
            ok=False,
            failure_reason="ungrounded_term",
            ungrounded_terms=("phantom_term",),
        )

    monkeypatch.setattr(rca_card_builder, "self_grounding_check", _force_fail)

    with patch.dict(os.environ, {"GSO_RCA_CARD_BUILDER": "1"}, clear=True):
        out = build_rca_card(
            cluster_id="cluster_2",
            qids=("gs_001",),
            failure_buckets={},
            asi_metadata={"gs_001": {"failure_type": "plural_top_n_collapse", "blame_set": ["t"]}},
            generated_sql_by_qid={},
            reference_sql_by_qid={},
            metadata_snapshot=metadata_snapshot,
            cluster=cluster,
        )
    assert out == {"rca_id": ""}
    assert cluster.get("rca_card_id") in (None, "")
    assert metadata_snapshot["_rca_card_store"] == {}
    # Failure was recorded for the caller to emit a typed record.
    failures = metadata_snapshot.get("_rca_card_self_check_failures") or []
    assert len(failures) == 1
    assert failures[0]["cluster_id"] == "cluster_2"
    assert failures[0]["failure_reason"] == "ungrounded_term"


def test_builder_falls_back_to_legacy_stub_when_flag_off() -> None:
    """Byte-stable: with the flag OFF, the new signature still works
    AND returns ``{"rca_id": ""}`` like the original stub. This
    guards every existing replay fixture."""
    with patch.dict(os.environ, {}, clear=True):
        out = build_rca_card(
            cluster_id="c", qids=(),
            failure_buckets={}, asi_metadata={},
        )
    assert out == {"rca_id": ""}


def test_orchestrator_drains_self_check_failure_into_decision_record() -> None:
    """When the builder rejects a card via self-grounding, the
    orchestrator's records list must include a
    rca_card_self_check_failed record."""
    from genie_space_optimizer.optimization.rca import (
        regenerate_rca_if_policy_permits,
    )
    from genie_space_optimizer.optimization.rca_execution import (
        RcaRegenerationCache,
        RcaRegenerationPolicy,
    )

    cluster = {
        "cluster_id": "cluster_2",
        "cluster_signature": "cluster_2_sig",
        "question_ids": ["gs_001"],
        "rca_card": False,  # ungrounded
    }

    # Driver returns a fake outcome that triggers the legacy
    # exhausted path, but the metadata_snapshot also carries a
    # self-check failure that the orchestrator must drain.
    def driver(*, spark, run_id, cluster, metadata_snapshot):
        # Simulate the builder having written a self-check failure
        # into metadata_snapshot during a prior call.
        metadata_snapshot.setdefault(
            "_rca_card_self_check_failures", []
        ).append({
            "cluster_id": "cluster_2",
            "qids": ["gs_001"],
            "failure_reason": "ungrounded_term",
        })
        return {"rca_id": "", "attempted_sources": ("failure_buckets",)}

    cache = RcaRegenerationCache()
    policy = RcaRegenerationPolicy.default()
    metadata_snapshot: dict = {}

    with patch.dict(os.environ, {"GSO_RCA_REGEN_RECOVERY_POLICY": "1"}, clear=True):
        records = regenerate_rca_if_policy_permits(
            cluster=cluster,
            findings=[],
            evidence_snapshot={},
            cache=cache,
            policy=policy,
            run_id="r1",
            iteration=1,
            attempt_driver=driver,
            metadata_snapshot=metadata_snapshot,
        )

    reason_codes = [r.get("reason_code") for r in records]
    assert "rca_card_self_check_failed" in reason_codes, (
        f"orchestrator must drain self-check failure into a DecisionRecord; "
        f"saw reason_codes={reason_codes}"
    )
