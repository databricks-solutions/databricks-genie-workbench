"""Trial 13g — SM Stage 3 transformer plumbs typed RCA evidence.

Pre-Trial-13g ``_build_failure_cluster_from_state`` hardcoded
``primary_blame_set=()`` and the call to
``run_plan11_synthesis_for_single_cluster`` passed
``member_qid_evidence=None``. The Stage 1 typed RCA evidence on
``ctx.rca_evidence_typed`` (populated by the Plan 12 typed-cutover)
never reached the Stage 3 LLM. The 98ec replay run showed proposals
emitted with ``blame_set=[]`` because the LLM had no evidence to
ground them in.

These tests pin:

  1. ``_build_failure_cluster_from_state`` populates
     ``primary_blame_set`` from the typed evidence when present.
  2. ``_build_member_qid_evidence_from_ctx`` emits a one-element
     ``member_qid_evidence`` list carrying the QID's blame_set.
  3. Legacy paths (no typed evidence on ``ctx``) keep the prior
     behaviour: empty ``primary_blame_set`` and empty list.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.records import (
    ClusterMembershipRecord,
    DiagnosisRecord,
    HardQidSeenRecord,
    StageTransition,
)
from genie_space_optimizer.optimization.state_machine.state import (
    build_initial_state,
)
from genie_space_optimizer.optimization.state_machine.transformers.synthesize_llm import (
    _build_failure_cluster_from_state,
    _build_member_qid_evidence_from_ctx,
)
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
    ValidationContext,
)


def _state_at_clustered(qid: str = "gs_009"):
    s = build_initial_state(
        qid=qid,
        iteration=1,
        seen=HardQidSeenRecord("r", "row_is_hard_failure", 0.0, "S", "x", 1),
    )
    s = s.advance(
        FunnelStage.DIAGNOSED,
        StageTransition(
            FunnelStage.HARD_QID_SEEN, FunnelStage.DIAGNOSED, 1, "t", "llm",
        ),
        diagnosed=DiagnosisRecord(
            "plan11_stage1", "row_count_mismatch", "summary",
            "observed_failure", "LIMIT N", "high", "rca_1",
        ),
    )
    s = s.advance(
        FunnelStage.CLUSTERED,
        StageTransition(
            FunnelStage.DIAGNOSED, FunnelStage.CLUSTERED, 2, "t", "batch",
        ),
        clustered=ClusterMembershipRecord(
            "H001", "AG_1", (qid,), 6, "row_count_mismatch",
        ),
    )
    return s


class _TypedEv:
    """Minimal duck-type for PerQidRcaEvidence (Trial 13g uses
    attribute access via ``getattr`` so a stub is sufficient)."""

    def __init__(
        self,
        blame_set: tuple[str, ...],
        observed_failure: str = "row count mismatch",
        expected_sql_shape: str = "LIMIT N",
        confidence: str = "high",
    ):
        self.blame_set = blame_set
        self.observed_failure = observed_failure
        self.expected_sql_shape = expected_sql_shape
        self.confidence = confidence


def test_failure_cluster_carries_blame_set_from_typed_evidence() -> None:
    """When ``ctx.rca_evidence_typed[qid].blame_set`` is non-empty,
    ``_build_failure_cluster_from_state`` reflects it on
    ``primary_blame_set``."""
    state = _state_at_clustered()
    ctx = TransformerContext(
        iteration=1,
        run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        rca_evidence_typed={
            "gs_009": _TypedEv(
                blame_set=("main.sales.orders.order_id",),
            ),
        },
    )
    cluster = _build_failure_cluster_from_state(state, ctx)
    assert cluster.primary_blame_set == (
        "main.sales.orders.order_id",
    )


def test_failure_cluster_primary_blame_set_empty_without_typed_evidence() -> None:
    """Legacy path (no typed evidence) yields ``primary_blame_set=()``
    so prior unit-test paths keep working."""
    state = _state_at_clustered()
    ctx = TransformerContext(
        iteration=1,
        run_id="r",
        validation_context=ValidationContext(1, "r", {}),
    )
    cluster = _build_failure_cluster_from_state(state, ctx)
    assert cluster.primary_blame_set == ()


def test_failure_cluster_handles_missing_qid_in_typed_map() -> None:
    """Typed evidence map present but missing this QID → empty
    primary_blame_set (no crash)."""
    state = _state_at_clustered(qid="gs_009")
    ctx = TransformerContext(
        iteration=1,
        run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        rca_evidence_typed={
            "OTHER_QID": _TypedEv(blame_set=("a.b.c",)),
        },
    )
    cluster = _build_failure_cluster_from_state(state, ctx)
    assert cluster.primary_blame_set == ()


def test_member_qid_evidence_one_element_when_typed_present() -> None:
    """``_build_member_qid_evidence_from_ctx`` emits a one-element
    list keyed to the QID with blame_set propagated."""
    state = _state_at_clustered()
    ctx = TransformerContext(
        iteration=1,
        run_id="r",
        validation_context=ValidationContext(1, "r", {}),
        rca_evidence_typed={
            "gs_009": _TypedEv(
                blame_set=("main.sales.orders.order_id",),
                observed_failure="row count too high",
                expected_sql_shape="LIMIT N",
                confidence="high",
            ),
        },
    )
    evidence = _build_member_qid_evidence_from_ctx(state, ctx)
    assert len(evidence) == 1
    entry = evidence[0]
    assert entry["qid"] == "gs_009"
    assert entry["blame_set"] == ["main.sales.orders.order_id"]
    assert entry["observed_failure"] == "row count too high"
    assert entry["expected_sql_shape"] == "LIMIT N"
    assert entry["confidence"] == "high"
    # Nested ``diagnosis`` mirror for prompt-shape compatibility.
    assert entry["diagnosis"]["blame_set"] == [
        "main.sales.orders.order_id",
    ]


def test_member_qid_evidence_empty_without_typed_evidence() -> None:
    """Legacy path (no typed evidence) yields ``[]`` so legacy unit
    tests that omit ``rca_evidence_typed`` keep working — the Stage 3
    entry point sees ``member_qid_evidence=None`` exactly as before."""
    state = _state_at_clustered()
    ctx = TransformerContext(
        iteration=1,
        run_id="r",
        validation_context=ValidationContext(1, "r", {}),
    )
    assert _build_member_qid_evidence_from_ctx(state, ctx) == []


def test_member_qid_evidence_empty_when_ctx_is_none() -> None:
    """Defensive: ``ctx=None`` returns ``[]`` (function never crashes)."""
    state = _state_at_clustered()
    assert _build_member_qid_evidence_from_ctx(state, None) == []
