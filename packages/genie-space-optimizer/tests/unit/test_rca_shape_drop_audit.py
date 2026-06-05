"""Pin explicit audit visibility for RCA-theme shape-normalization drops."""

from __future__ import annotations

from _harness_loop_source import lever_loop_source


def test_harness_audits_rca_theme_shape_drops() -> None:
    src = lever_loop_source()
    shape_idx = src.index("RCA COLUMN SHAPE NORMALIZATION")
    window = src[shape_idx: shape_idx + 1800]
    assert "rca_theme_shape_dropped" in window
    assert "_audit_emit(" in window
    assert "missing_table_for_column" in window
