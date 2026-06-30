"""GSO v2 Phase 8 — two-mode strategist threading + invariant bypass (arch §5.2).

``_call_llm_for_adaptive_strategy`` is the SINGLE chokepoint for the one-source-
cluster-per-action-group invariant. Phase 8 adds ``attempt_mode`` / ``attempt_no``
(the controller signal) and ``allow_cluster_agnostic`` (the parameterized escape
hatch). When ``allow_cluster_agnostic=True`` (the attempt-1 coverage pass) the
invariant is RELAXED so a broad AG may span multiple clusters; surgical attempts
(default ``allow_cluster_agnostic=False``) keep the single-cluster bound enforced.
"""

from __future__ import annotations

import inspect
import json
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer


def _two_cluster_strategy_json() -> str:
    # One AG spanning two HARD clusters with DISJOINT blame sets (so they do not
    # share defect identity → surgical mode prunes to the winner).
    return json.dumps({
        "action_groups": [
            {
                "source_cluster_ids": ["H001", "H002"],
                "affected_questions": ["q1", "q2"],
                "lever_directives": {"5": {"kind": "sql_shape", "guidance": "x"}},
                "root_cause_summary": "multi",
            }
        ],
        "rationale": "test multi-cluster AG",
    })


_CLUSTERS = [
    {
        "cluster_id": "H001",
        "question_ids": ["q1", "q1b", "q1c"],  # higher impact → winner
        "root_cause": "missing_filter",
        # DISJOINT blame tokens (no shared catalog/schema words) so
        # clusters_share_defect_identity is False → surgical mode prunes.
        "blame_set": ["alphatable"],
        "asi_blame_set": ["alphatable"],
    },
    {
        "cluster_id": "H002",
        "question_ids": ["q2"],
        "root_cause": "missing_join_spec",
        "blame_set": ["bravotable"],
        "asi_blame_set": ["bravotable"],
    },
]


def _invoke(allow_cluster_agnostic: bool) -> dict:
    with patch.object(
        optimizer, "_traced_llm_call",
        return_value=(_two_cluster_strategy_json(), None),
    ):
        return optimizer._call_llm_for_adaptive_strategy(
            clusters=_CLUSTERS,
            soft_signal_clusters=[],
            metadata_snapshot={"data_sources": {"tables": []}},
            reflection_buffer=[],
            priority_ranking=[],
            tried_patches=set(),
            w=MagicMock(),
            attempt_no=(1 if allow_cluster_agnostic else 2),
            attempt_mode=("coverage" if allow_cluster_agnostic else "surgical"),
            allow_cluster_agnostic=allow_cluster_agnostic,
        )


# ── signature carries the Phase 8 controller params ─────────────────────────
def test_strategist_signature_has_mode_and_bypass_params() -> None:
    sig = inspect.signature(optimizer._call_llm_for_adaptive_strategy)
    for p in ("attempt_no", "attempt_mode", "allow_cluster_agnostic"):
        assert p in sig.parameters, f"missing {p}"
    assert sig.parameters["allow_cluster_agnostic"].default is False


# ── behavioral: surgical PRUNES the multi-cluster AG to one source cluster ──
def test_surgical_prunes_multi_cluster_ag_to_single_cluster() -> None:
    result = _invoke(allow_cluster_agnostic=False)
    ags = result.get("action_groups") or []
    assert ags, "surgical strategist returned no action groups"
    src_ids = [str(c) for c in ags[0].get("source_cluster_ids", [])]
    # RCA defect-identity scope bound keeps only the winner (disjoint blame).
    assert src_ids == ["H001"], src_ids


# ── behavioral: coverage PRESERVES the multi-cluster AG (invariant bypassed) ─
def test_coverage_preserves_multi_cluster_ag() -> None:
    result = _invoke(allow_cluster_agnostic=True)
    ags = result.get("action_groups") or []
    assert ags, "coverage strategist returned no action groups"
    src_ids = sorted(str(c) for c in ags[0].get("source_cluster_ids", []))
    # Cluster-agnostic coverage keeps both clusters — the scope bound is bypassed.
    assert src_ids == ["H001", "H002"], src_ids


# ── structural: the bypass is gated at the single chokepoint, not scattered ─
def test_invariant_bypass_gated_on_allow_cluster_agnostic() -> None:
    src = inspect.getsource(optimizer._call_llm_for_adaptive_strategy)
    # The bypass is referenced at each enforcement point (system prompt branch,
    # cross-namespace drop condition, RCA defect-identity scope-bound continue).
    assert src.count("allow_cluster_agnostic") >= 4
    # The gating is by the boolean escape hatch, never by branching on the raw
    # attempt number inside the strategist (that lives in the controller).
    assert "if attempt_no == 1:" not in src
