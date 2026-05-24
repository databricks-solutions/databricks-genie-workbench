"""Trial 13i — pin ``GSO_PLAN11_STAGE1_INPUT_QUALITY_V1`` payload shape.

This marker is the canary surface for the Trial 13i fix: every QID emits
it exactly once at Stage 1 pre-flight (before the LLM is invoked) so
postmortems can join on:

  * ``schema_columns_source`` — closed vocab from ``STAGE1_INPUT_QUALITY_SOURCES``.
    ``"empty"`` is the deploy-block canary.
  * ``schema_columns_size`` — cross-check against the source label.
  * Seed normalization stats (``seeds_pre_normalize``, ``seeds_post_normalize``,
    ``seeds_normalized``, ``seeds_dropped``).
  * ``contract_violation`` — non-empty when the run failed pre-flight.

Closed-vocab pinning mirrors Trial 13h's
``STAGE1_BLAME_SET_SOURCES`` pin in
``test_plan11_stage1_marker_blame_set_source_field.py``.
"""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.run_analysis_contract import (
    STAGE1_INPUT_QUALITY_SOURCES,
    plan11_stage1_input_quality_marker,
)


def _payload(line: str) -> dict:
    return json.loads(line.split(" ", 1)[1])


def test_source_labels_closed_vocabulary() -> None:
    assert STAGE1_INPUT_QUALITY_SOURCES == frozenset(
        {
            "metadata_snapshot",
            "typed_evidence_union",
            "identifier_allowlist",
            "empty",
        }
    )


def test_marker_carries_typed_evidence_union_source() -> None:
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_021",
        schema_columns_source="typed_evidence_union",
        schema_columns_size=6,
        seeds_pre_normalize=2,
        seeds_post_normalize=2,
        seeds_normalized=0,
        seeds_dropped=0,
    )
    payload = _payload(line)
    assert payload["schema_columns_source"] == "typed_evidence_union"
    assert payload["schema_columns_source"] in STAGE1_INPUT_QUALITY_SOURCES
    assert payload["schema_columns_size"] == 6
    assert payload["seeds_pre_normalize"] == 2
    assert payload["seeds_post_normalize"] == 2
    assert payload["seeds_normalized"] == 0
    assert payload["seeds_dropped"] == 0
    assert payload["contract_violation"] == ""


def test_marker_carries_metadata_snapshot_source() -> None:
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_001",
        schema_columns_source="metadata_snapshot",
        schema_columns_size=120,
    )
    payload = _payload(line)
    assert payload["schema_columns_source"] == "metadata_snapshot"
    assert payload["schema_columns_size"] == 120


def test_marker_carries_identifier_allowlist_source() -> None:
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_002",
        schema_columns_source="identifier_allowlist",
        schema_columns_size=42,
    )
    payload = _payload(line)
    assert payload["schema_columns_source"] == "identifier_allowlist"


def test_marker_carries_empty_source_with_contract_violation() -> None:
    """The Trial 13i bottleneck signal: empty schema_columns AND a
    pre-flight contract violation. Postmortems join on these fields."""
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_009",
        schema_columns_source="empty",
        schema_columns_size=0,
        contract_violation="missing_schema_columns",
    )
    payload = _payload(line)
    assert payload["schema_columns_source"] == "empty"
    assert payload["schema_columns_size"] == 0
    assert payload["contract_violation"] == "missing_schema_columns"


def test_marker_carries_seed_normalization_stats() -> None:
    """Capture-only bundles: free-text tokens partially resolve to FQNs."""
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_009",
        schema_columns_source="identifier_allowlist",
        schema_columns_size=42,
        seeds_pre_normalize=5,
        seeds_post_normalize=2,
        seeds_normalized=2,
        seeds_dropped=3,
    )
    payload = _payload(line)
    assert payload["seeds_pre_normalize"] == 5
    assert payload["seeds_post_normalize"] == 2
    assert payload["seeds_normalized"] == 2
    assert payload["seeds_dropped"] == 3


def test_marker_defaults_for_optional_fields() -> None:
    """Stats default to 0 / empty so the marker is callable with the
    minimal field set."""
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_minimal",
        schema_columns_source="empty",
        schema_columns_size=0,
    )
    payload = _payload(line)
    assert payload["seeds_pre_normalize"] == 0
    assert payload["seeds_post_normalize"] == 0
    assert payload["seeds_normalized"] == 0
    assert payload["seeds_dropped"] == 0
    assert payload["contract_violation"] == ""


def test_marker_name_constant() -> None:
    line = plan11_stage1_input_quality_marker(
        optimization_run_id="run_x",
        iteration=1,
        qid="gs_x",
        schema_columns_source="empty",
        schema_columns_size=0,
    )
    assert line.startswith("GSO_PLAN11_STAGE1_INPUT_QUALITY_V1 ")
