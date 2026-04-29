from __future__ import annotations

from genie_space_optimizer.optimization.optimizer import _format_rca_themes_for_strategy
from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme, ThemeConflict


def test_format_rca_themes_includes_levers_evidence_and_patch_intents() -> None:
    theme = RcaPatchTheme(
        rca_id="rca_measure_swap",
        rca_kind=RcaKind.MEASURE_SWAP,
        patch_family="contrastive_measure_disambiguation",
        patches=(
            {
                "type": "update_column_description",
                "lever": 1,
                "intent": "strengthen intended measure description",
                "column": "gross_sales",
            },
            {
                "type": "add_sql_snippet_measure",
                "lever": 6,
                "intent": "define reusable measure expression",
                "target_object": "gross_sales",
            },
            {
                "type": "request_example_sql_synthesis",
                "lever": 5,
                "intent": "synthesize original non-benchmark example SQL",
                "root_cause": "wrong_measure",
            },
        ),
        target_qids=("q_measure",),
        touched_objects=("gross_sales",),
        confidence=0.86,
        evidence_summary="judge=semantic_equivalence; failure_type=different_metric",
    )

    text = _format_rca_themes_for_strategy([theme], [])

    assert "recommended_levers=[1, 5, 6]" in text
    assert "evidence=judge=semantic_equivalence" in text
    assert "update_column_description" in text
    assert "add_sql_snippet_measure" in text
    assert "request_example_sql_synthesis" in text
    assert "synthesize original non-benchmark example SQL" in text


def test_format_rca_themes_includes_conflict_matrix() -> None:
    conflict = ThemeConflict(
        left_rca_id="rca_a",
        right_rca_id="rca_b",
        object_id="gross_sales",
        reason="multiple RCA themes touch the same object",
    )

    text = _format_rca_themes_for_strategy([], [conflict])

    assert "No typed RCA themes available" in text
    assert "RCA Theme Conflict Matrix" in text
    assert "rca_a -> rca_b" in text


def test_rca_themes_strategist_context_enabled_by_default(monkeypatch) -> None:
    import importlib
    import genie_space_optimizer.common.config as config

    monkeypatch.delenv("GSO_ENABLE_RCA_THEMES_STRATEGIST", raising=False)
    reloaded = importlib.reload(config)

    assert reloaded.ENABLE_RCA_THEMES_STRATEGIST is True
