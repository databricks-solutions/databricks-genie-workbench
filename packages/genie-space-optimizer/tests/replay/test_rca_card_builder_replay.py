"""Phase 1 Action 1.1 — end-to-end replay test using ccf1d60d evidence."""
from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from genie_space_optimizer.optimization.rca import build_rca_card


FIXTURE = (
    Path(__file__).parent / "fixtures" / "rca_card" / "ccf1d60d_gs026_cluster.json"
)


@pytest.fixture
def ccf1d60d_pack() -> dict:
    return json.loads(FIXTURE.read_text())


def test_ccf1d60d_gs026_builder_produces_expected_card(ccf1d60d_pack: dict) -> None:
    cluster = {
        "primary_cluster_id": ccf1d60d_pack["cluster_id"],
        "target_qids": tuple(ccf1d60d_pack["qids"]),
    }
    snapshot: dict = {"_rca_card_store": {}}

    with patch.dict(os.environ, {"GSO_RCA_CARD_BUILDER": "1"}, clear=True):
        out = build_rca_card(
            cluster_id=ccf1d60d_pack["cluster_id"],
            qids=tuple(ccf1d60d_pack["qids"]),
            failure_buckets={},
            asi_metadata=ccf1d60d_pack["asi_by_qid"],
            generated_sql_by_qid=ccf1d60d_pack["generated_sql_by_qid"],
            reference_sql_by_qid=ccf1d60d_pack["reference_sql_by_qid"],
            metadata_snapshot=snapshot,
            cluster=cluster,
        )

    assert out["rca_id"], "Expected the builder to ground gs_026 evidence into a card"
    assert cluster["rca_card_id"] == out["rca_id"]

    card = snapshot["_rca_card_store"][out["rca_id"]]
    expected = ccf1d60d_pack["expected_card"]

    assert card.root_cause.value == expected["root_cause"]
    assert sorted(card.grounding_terms) == sorted(expected["grounding_terms"])
    assert card.intended_patch_shape == expected["intended_patch_shape"]
    assert sorted(card.allowed_patch_families) == sorted(expected["allowed_patch_families"])
    assert sorted(card.forbidden_patch_families) == sorted(expected["forbidden_patch_families"])
