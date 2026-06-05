"""Trial 18 Step 4 — Stage 3 resolved-table preflight.

The applier_gate preflight (Trial 17 step 4) catches metadata target
gaps after the Stage 3 round-trip. Postmortems e94376a3 / d13938e7
showed Stage 3 spending an LLM turn proposing
``add_column_description`` against unresolved ``tkt_payment`` and
``mv_7now_store_sales`` even though the schema slice that fed the
prompt did not contain those tables.

Trial 18 pushes the check upstream: Stage 3 builds a
``resolved_tables`` set from the same ``schema_slice`` it shows the
LLM, and drops any metadata proposal whose ``patch_body.table`` is
not in that set BEFORE the proposal is appended to the surviving
list. The drop is recorded under
``rejected_patch_types_raw[f"{pt.value}::target_table_unresolved"]``
so the Stage 3 ``plan11_stage3_synthesis`` marker surfaces it via
``synthesis_rejected_patch_types`` (existing channel) when no
proposal survives.

The preflight is **strictly additive**:

- It only fires when ``patch_body.table`` (or ``patch_body.target``)
  is a non-empty string — the legacy ``object_id="t:c"`` shape is
  opaque and falls through to the applier_gate preflight unchanged.
- It only fires for the metadata patch types the applier_gate
  already preflights (the ``_PREFLIGHT_METADATA_PATCH_TYPES`` set).
- It is gated by ``GSO_TRIAL18_ACCEPTANCE_OVERHAUL``; flag-off
  reverts byte-for-byte to today's pass-through (proposal survives
  Stage 3, applier_gate's late preflight catches it).
"""
from __future__ import annotations

from unittest.mock import patch as mock_patch

import pytest

from genie_space_optimizer.optimization.stages import synthesize as syn
from genie_space_optimizer.optimization.stages.plan11_types import (
    FailureCluster,
)


def _stub_reasoning_response(proposals: list[dict]):
    class _Resp:
        succeeded = True
        declined = None
        parsed_output = {"proposals": proposals}
        tokens_input = 100
        tokens_output = 100

    return _Resp()


def _cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="H_T18_S4",
        semantic_theme="missing_metadata",
        member_qids=("gs_013",),
        unifying_evidence="rows reference unresolved table tkt_payment",
        repair_hypothesis="describe tkt_payment.amount",
        primary_blame_set=("tkt_payment.amount",),
        confidence="high",
    )


_SCHEMA_WITH_ONE_TABLE = {
    "data_sources": {
        "tables": [
            {
                "identifier": "main.demo.t",
                "column_configs": [{"column_name": "col"}],
            },
        ],
        "metric_views": [],
    },
}


def _invoke(proposals: list[dict], *, schema_slice: dict | None = None):
    with mock_patch.object(
        syn.LlmReasoningCall,
        "invoke",
        return_value=_stub_reasoning_response(proposals),
    ):
        return syn.run_plan11_synthesis_for_single_cluster(
            cluster=_cluster(),
            schema_slice=schema_slice if schema_slice is not None
            else _SCHEMA_WITH_ONE_TABLE,
            history=[],
            member_qid_evidence=[
                {"qid": "gs_013", "blame_set": ["main.demo.t.col"]},
            ],
            optimization_run_id="r18",
            iteration=2,
            ag_id="AG_T18_S4",
            w=None,
        )


# ----------------------------------------------------------------------
# Step 4 core: filter at synthesis time when target_table unresolved.
# ----------------------------------------------------------------------


def test_add_column_description_against_unresolved_table_rejected_at_synthesis():
    """gs_013-shape: LLM proposes ``add_column_description`` against
    ``tkt_payment`` but the schema slice only resolves
    ``main.demo.t``. Trial 18 must filter this at synthesis time
    (single-proposal batch → empty_synthesis) and surface a typed
    ``synthesis_rejected_patch_types`` entry with reason
    ``target_table_unresolved``.
    """
    result = _invoke(
        [
            {
                "intent_name": "describe_tkt_payment",
                "intent_description": "missing metadata for tkt_payment.amount",
                "repair_hypothesis": "describe tkt_payment.amount",
                "patch_type": "add_column_description",
                "rationale": "judge cited tkt_payment",
                "confidence": "high",
                "patch_body": {
                    "table": "tkt_payment",
                    "column": "amount",
                    "description": "Payment amount in USD",
                },
                "blame_set": ["tkt_payment.amount"],
                "target_qids": ["gs_013"],
                "selected_lever": "lever-1",
            }
        ],
    )

    assert result.proposal is None, (
        "Stage 3 must drop the unresolved-table metadata proposal "
        "rather than let it reach Stage 4 / applier_gate."
    )
    assert result.skipped_reason != ""


def test_resolved_targets_still_emit_proposals():
    """When the table IS in resolved_tables, the proposal survives
    Stage 3 unchanged — the preflight is strictly a drop-filter, not
    a rewriting layer.
    """
    result = _invoke(
        [
            {
                "intent_name": "describe_t_col",
                "intent_description": "describe a real column",
                "repair_hypothesis": "describe main.demo.t.col",
                "patch_type": "add_column_description",
                "rationale": "explicit",
                "confidence": "high",
                "patch_body": {
                    "table": "main.demo.t",
                    "column": "col",
                    "description": "Numeric column",
                },
                "blame_set": ["main.demo.t.col"],
                "target_qids": ["gs_013"],
                "selected_lever": "lever-1",
            }
        ],
    )

    assert result.proposal is not None
    payload = result.proposal
    assert payload["patch_type"] == "add_column_description"


# ----------------------------------------------------------------------
# Non-regression guards.
# ----------------------------------------------------------------------


def test_opaque_object_id_shape_falls_through_to_applier_gate():
    """Patches that encode the target as ``patch_body.object_id="t:c"``
    instead of ``patch_body.table`` are opaque to a string-based
    preflight. The Trial 18 filter must skip them so the existing
    applier_gate preflight (which knows how to parse ``object_id``)
    remains the authoritative late check — this is exactly the same
    contract the applier_gate preflight uses.
    """
    result = _invoke(
        [
            {
                "intent_name": "object_id_shape",
                "intent_description": "legacy encoding",
                "repair_hypothesis": "x",
                "patch_type": "add_column_description",
                "rationale": "x",
                "confidence": "low",
                "patch_body": {
                    "object_id": "tkt_payment:amount",
                    "description": "x",
                },
                "blame_set": ["tkt_payment.amount"],
                "target_qids": ["gs_013"],
                "selected_lever": "lever-1",
            }
        ],
    )
    assert result.proposal is not None, (
        "Trial 18 must not drop legacy object_id-shaped proposals "
        "blindly — the existing applier_gate preflight is the "
        "authoritative late check for that shape."
    )


def test_empty_schema_slice_falls_through():
    """When ``schema_slice`` carries no tables (synthetic replays /
    tests / pre-snapshot iterations), the preflight cannot make a
    confident drop decision and must fall through to the applier_gate
    preflight unchanged. This matches the applier_gate's own
    empty-snapshot contract."""
    result = _invoke(
        [
            {
                "intent_name": "describe_anything",
                "intent_description": "x",
                "repair_hypothesis": "x",
                "patch_type": "add_column_description",
                "rationale": "x",
                "confidence": "low",
                "patch_body": {
                    "table": "anything",
                    "column": "col",
                    "description": "x",
                },
                "blame_set": ["anything.col"],
                "target_qids": ["gs_013"],
                "selected_lever": "lever-1",
            }
        ],
        schema_slice={},
    )
    assert result.proposal is not None


def test_non_metadata_patch_type_is_not_preflighted():
    """``add_instruction`` (lever-5 prose) has no notion of
    ``target_table`` — the preflight must skip it entirely. This
    matches the ``_PREFLIGHT_METADATA_PATCH_TYPES`` membership rule
    in applier_gate."""
    result = _invoke(
        [
            {
                "intent_name": "prose",
                "intent_description": "instruction",
                "repair_hypothesis": "x",
                "patch_type": "add_instruction",
                "rationale": "x",
                "confidence": "high",
                "patch_body": {"instruction_text": "Use LIMIT 10"},
                "blame_set": ["main.demo.t.col"],
                "target_qids": ["gs_013"],
                "selected_lever": "lever-5",
                # The Trial 22 required-assets gate drops solo levers with
                # no justification; this test pins that the metadata
                # preflight SKIPS non-metadata patch types, so supply a
                # justification to isolate the preflight behaviour.
                "single_lever_justification": "prose fix, no target table",
            }
        ],
    )
    assert result.proposal is not None


def test_mixed_batch_drops_only_unresolved_metadata():
    """A batch with one unresolved-table metadata proposal AND one
    resolved-table metadata proposal: only the unresolved one is
    dropped; the resolved one continues."""
    result = _invoke(
        [
            {
                "intent_name": "bad_target",
                "intent_description": "unresolved",
                "repair_hypothesis": "x",
                "patch_type": "add_column_description",
                "rationale": "x",
                "confidence": "high",
                "patch_body": {
                    "table": "mv_7now_store_sales",
                    "column": "rev",
                    "description": "x",
                },
                "blame_set": ["mv_7now_store_sales.rev"],
                "target_qids": ["gs_013"],
                "selected_lever": "lever-1",
            },
            {
                "intent_name": "good_target",
                "intent_description": "resolved",
                "repair_hypothesis": "x",
                "patch_type": "add_column_description",
                "rationale": "x",
                "confidence": "high",
                "patch_body": {
                    "table": "main.demo.t",
                    "column": "col",
                    "description": "ok",
                },
                "blame_set": ["main.demo.t.col"],
                "target_qids": ["gs_013"],
                "selected_lever": "lever-1",
            },
        ],
    )
    assert result.proposal is not None
    payload = result.proposal
    # The surviving proposal must be the resolved-target one.
    assert payload["patch_body"]["table"] == "main.demo.t"


# ----------------------------------------------------------------------
# Flag-off rollback — Trial 18 preflight is gated; flag-off restores
# byte-for-byte pre-Trial-18 Stage 3 behaviour.
# ----------------------------------------------------------------------


def test_flag_off_preserves_legacy_pass_through(monkeypatch):
    """``GSO_TRIAL18_ACCEPTANCE_OVERHAUL=0`` reverts to today's Stage
    3 contract: the unresolved-table proposal flows through (and
    applier_gate's late preflight is the only safety net). Critical
    for emergency rollback."""
    monkeypatch.setenv("GSO_TRIAL18_ACCEPTANCE_OVERHAUL", "0")
    result = _invoke(
        [
            {
                "intent_name": "unresolved",
                "intent_description": "x",
                "repair_hypothesis": "x",
                "patch_type": "add_column_description",
                "rationale": "x",
                "confidence": "high",
                "patch_body": {
                    "table": "tkt_payment",
                    "column": "amount",
                    "description": "x",
                },
                "blame_set": ["tkt_payment.amount"],
                "target_qids": ["gs_013"],
                "selected_lever": "lever-1",
            }
        ],
    )
    assert result.proposal is not None, (
        "Flag-off must restore pre-Trial-18 Stage 3 behaviour for "
        "rollback safety."
    )
