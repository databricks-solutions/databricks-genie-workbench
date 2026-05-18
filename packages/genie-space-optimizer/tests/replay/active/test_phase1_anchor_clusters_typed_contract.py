"""Phase 2 — anchor cluster end-to-end typed-contract assertions.

These tests run the four live-anchor FailureCluster fixtures
through the synthesis dispatch path and verify:

1. The dispatcher accepts each fixture without raising.
2. The Phase 0.3 pre-flight emits a NSC decision record with
   skipped_reason="missing_rca_card" for each ungrounded anchor.
3. The collision_key_pair projection preserves target_qids
   identity (typed contract for Phase 6.1 terminal-signature axis).

If accuracy must move on these clusters, the first proof is that
the typed contract carries causal context all the way through.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization import forced_synthesis_dispatch

# Anchors live in tests/fixtures/failure_cluster_anchors. Make the
# ``fixtures`` package importable when pytest's rootdir is
# packages/genie-space-optimizer.
_TESTS_ROOT = Path(__file__).resolve().parents[2]
if str(_TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TESTS_ROOT))

from fixtures.failure_cluster_anchors import (  # noqa: E402
    ALL_ANCHORS,
    AIRLINE_GS_009,
    AIRLINE_GS_024,
    SEVEN_NOW_GS_013,
    SEVEN_NOW_GS_026,
)


@pytest.mark.parametrize(
    "anchor",
    [AIRLINE_GS_009, AIRLINE_GS_024, SEVEN_NOW_GS_013, SEVEN_NOW_GS_026],
    ids=lambda fc: fc.target_qids[0] if fc.target_qids else fc.cluster_id,
)
def test_anchor_cluster_pre_flight_emits_missing_rca_card(anchor):
    """Each anchor cluster has is_grounded=False (no RCA card in the
    live transcripts). The Phase 0.3 pre-flight refuses these
    upstream of synthesis with skipped_reason=missing_rca_card.
    Verify the typed reason reaches the emitted decision record."""
    cluster = {
        "cluster_id": anchor.cluster_id,
        "question_ids": list(anchor.target_qids),
        "root_cause": anchor.root_cause,
        "asi_failure_type": anchor.asi_failure_type,
        "asi_blame_set": list(anchor.blame_set_raw),
        "rca_card": {},  # ungrounded — matches live transcript
        "failure_keys": list(anchor.failure_keys),
    }
    ag = {
        "id": f"AG_DECOMPOSED_{anchor.cluster_id}",
        "source_cluster_ids": [anchor.cluster_id],
        "affected_questions": list(anchor.target_qids),
    }
    drop = {
        "drop_reason": "lever5_structural_sql_shape_no_example_sql",
        "root_causes": [anchor.root_cause],
        "source_clusters": [anchor.cluster_id],
    }

    def _synth_should_not_run(*args, **kwargs):
        pytest.fail(
            "Pre-flight RCA-card refusal failed: synthesizer was "
            "invoked on an ungrounded anchor cluster."
        )

    result = forced_synthesis_dispatch.dispatch_forced_structural_synthesis(
        ag=ag,
        run_id="anchor-test-run",
        iteration=1,
        l5_ag_drops=[drop],
        reflection_buffer=[],
        iter_source_clusters_by_id={anchor.cluster_id: cluster},
        iter_rca_id_by_cluster={anchor.cluster_id: ""},
        w=MagicMock(),
        benchmarks=[],
        metadata_snapshot={
            "data_sources": {"tables": [], "metric_views": []},
        },
        catalog="",
        schema="",
        spark=None,
        lever_keys=[5],
        current_iter_inputs={},
        synthesize=_synth_should_not_run,
        ag_proposals_so_far=[],
    )

    records = list(result.emitted_decision_records or ())
    matching = [
        r for r in records
        if r.get("ag_id") == ag["id"]
        and r.get("metrics", {}).get("skipped_reason") == "missing_rca_card"
    ]
    assert matching, (
        f"No missing_rca_card decision record for anchor "
        f"{anchor.cluster_id!r} ({anchor.root_cause}). Records: "
        f"{records!r}"
    )


@pytest.mark.parametrize(
    "anchor",
    ALL_ANCHORS,
    ids=lambda fc: fc.target_qids[0] if fc.target_qids else fc.cluster_id,
)
def test_anchor_cluster_identity_preserved_in_collision_key(anchor):
    """Anchor's collision_key_pair carries target_qids identically
    in the typed key, matching what the retired-signature producer
    would produce."""
    key = anchor.collision_key_pair(lever_keys=[5])
    assert key.terminal_signature_keys, (
        f"Anchor {anchor.cluster_id} produced empty "
        f"terminal_signature_keys; identity is lost."
    )
    qids_in_key = key.terminal_signature_keys[0][0]
    assert qids_in_key == frozenset(anchor.target_qids), (
        f"Anchor {anchor.cluster_id}: collision key qids "
        f"{sorted(qids_in_key)} != target_qids "
        f"{sorted(anchor.target_qids)}"
    )
