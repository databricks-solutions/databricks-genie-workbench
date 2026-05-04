"""Optimizer Control-Plane Hardening Plan — Task E.

Lever-aware blast-radius gradation: when
``GSO_LEVER_AWARE_BLAST_RADIUS`` is on, non-semantic patch types
(metadata-only — column descriptions, synonyms, instructions)
downgrade the ``high_collateral_risk_flagged`` rejection to a
``non_semantic_collateral_warning`` pass. Semantic patches
(SQL snippets, join changes) still block.
"""

from genie_space_optimizer.optimization.proposal_grounding import (
    patch_blast_radius_is_safe,
)


_NON_SEMANTIC_TYPES = (
    "update_column_description",
    "add_column_synonym",
    "add_metric_view_instruction",
    "add_table_instruction",
    "update_table_description",
)
_SEMANTIC_TYPES = (
    "add_sql_snippet_filter",
    "add_sql_snippet_expression",
    "add_join_spec",
    "update_join_spec",
)


def test_non_semantic_patch_warns_when_flag_on(monkeypatch):
    monkeypatch.setenv("GSO_LEVER_AWARE_BLAST_RADIUS", "1")
    for patch_type in _NON_SEMANTIC_TYPES:
        result = patch_blast_radius_is_safe(
            {
                "patch_type": patch_type,
                "passing_dependents": ["gs_003", "gs_004"],
                "high_collateral_risk": True,
            },
            ag_target_qids=("gs_024",),
        )
        assert result["safe"] is True, patch_type
        assert result["reason"] == "non_semantic_collateral_warning", patch_type


def test_semantic_patch_still_blocked(monkeypatch):
    monkeypatch.setenv("GSO_LEVER_AWARE_BLAST_RADIUS", "1")
    for patch_type in _SEMANTIC_TYPES:
        result = patch_blast_radius_is_safe(
            {
                "patch_type": patch_type,
                "passing_dependents": ["gs_003"],
                "high_collateral_risk": True,
            },
            ag_target_qids=("gs_024",),
        )
        assert result["safe"] is False, patch_type
        assert result["reason"] == "high_collateral_risk_flagged", patch_type


