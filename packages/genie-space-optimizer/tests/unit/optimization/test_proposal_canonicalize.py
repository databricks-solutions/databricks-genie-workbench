"""Unit tests for ``canonicalize_stage_2_proposal``.

The helper is the only place that should know how to translate a raw
LLM-returned proposal dict into the shape every downstream consumer
(narrow_replacement_diagnosis, apply_patch_set, projection) expects.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.proposal_canonicalize import (
    canonicalize_stage_2_proposal,
)


def test_canonicalize_lifts_underscore_patch_type_to_canonical_key():
    raw = {"_patch_type": "update_column_description", "proposed_value": "x"}
    out = canonicalize_stage_2_proposal(
        raw,
        skill_id="lever-1-table-column-description",
        target="catalog.schema.fact_orders.amount",
        patch_type="update_column_description",
    )
    assert out["patch_type"] == "update_column_description"
    # The underscore-prefixed key is removed to avoid two-way confusion.
    assert "_patch_type" not in out


def test_canonicalize_lifts_underscore_target_to_canonical_key():
    raw = {"_target": "catalog.schema.t1", "proposed_value": "x"}
    out = canonicalize_stage_2_proposal(
        raw,
        skill_id="lever-1-table-column-description",
        target="catalog.schema.t1",
        patch_type="update_column_description",
    )
    assert out["target"] == "catalog.schema.t1"
    assert "_target" not in out


def test_canonicalize_preserves_existing_canonical_keys():
    """If a Stage-2 adapter already returned a canonical shape (Lever 6
    does today), the helper must be idempotent."""
    raw = {
        "patch_type": "add_sql_snippet_measure",
        "target": "metric_view.fact_orders.revenue",
        "proposed_value": "SUM(amount)",
    }
    out = canonicalize_stage_2_proposal(
        raw,
        skill_id="lever-6-sql-expression",
        target="metric_view.fact_orders.revenue",
        patch_type="add_sql_snippet_measure",
    )
    assert out["patch_type"] == "add_sql_snippet_measure"
    assert out["target"] == "metric_view.fact_orders.revenue"
    assert out["proposed_value"] == "SUM(amount)"


def test_canonicalize_adds_provenance_skill_id_when_missing():
    """``provenance.skill_id`` is required by the projection step (Phase
    1b) so it can route the proposal back to the correct legacy lever."""
    raw = {"_patch_type": "update_column_description", "proposed_value": "x"}
    out = canonicalize_stage_2_proposal(
        raw,
        skill_id="lever-1-table-column-description",
        target="catalog.schema.t1",
        patch_type="update_column_description",
    )
    assert out["provenance"]["skill_id"] == "lever-1-table-column-description"


def test_canonicalize_preserves_existing_provenance_skill_id():
    """If the adapter pre-populated provenance (Lever 6 does), don't
    overwrite it."""
    raw = {
        "patch_type": "add_sql_snippet_measure",
        "provenance": {"skill_id": "lever-6-sql-expression", "lever": 6},
        "proposed_value": "SUM(x)",
    }
    out = canonicalize_stage_2_proposal(
        raw,
        skill_id="lever-6-sql-expression",
        target="t",
        patch_type="add_sql_snippet_measure",
    )
    assert out["provenance"]["skill_id"] == "lever-6-sql-expression"
    assert out["provenance"]["lever"] == 6


def test_canonicalize_rejects_empty_patch_type():
    """Empty patch_type is what caused
    ``narrow_skipped_no_original_patch_type`` in Trial-5 Run B. The
    helper must reject it loudly so the caller has to choose a
    valid type."""
    with pytest.raises(ValueError, match="patch_type"):
        canonicalize_stage_2_proposal(
            {"proposed_value": "x"},
            skill_id="lever-1-table-column-description",
            target="t",
            patch_type="",
        )


def test_canonicalize_rejects_non_dict_input():
    with pytest.raises(TypeError, match="dict"):
        canonicalize_stage_2_proposal(
            ["not", "a", "dict"],
            skill_id="lever-1-table-column-description",
            target="t",
            patch_type="update_column_description",
        )
