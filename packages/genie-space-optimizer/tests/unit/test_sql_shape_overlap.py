"""Plan C2 — unit tests for the SQL-shape overlap helper and feature
flag used by ``_t24_counterfactual_scan``.

Suite ordering:
  - Flag tests (this file's first block).
  - Shape-token extraction (``extract_snippet_shape_tokens``).
  - Benchmark overlap predicate (``benchmark_has_shape_overlap``).
"""
from __future__ import annotations

from typing import Any


def test_flag_defaults_off(monkeypatch: Any) -> None:
    monkeypatch.delenv("GSO_SQL_SHAPE_OVERLAP_GATE", raising=False)
    from genie_space_optimizer.common.config import (
        sql_shape_overlap_gate_enabled,
    )
    assert sql_shape_overlap_gate_enabled() is False


def test_flag_on_when_env_var_set_to_one(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_SQL_SHAPE_OVERLAP_GATE", "1")
    from genie_space_optimizer.common.config import (
        sql_shape_overlap_gate_enabled,
    )
    assert sql_shape_overlap_gate_enabled() is True


def test_flag_accepts_truthy_strings(monkeypatch: Any) -> None:
    from genie_space_optimizer.common.config import (
        sql_shape_overlap_gate_enabled,
    )
    for v in ("true", "TRUE", "yes", "Yes", "Y", "1"):
        monkeypatch.setenv("GSO_SQL_SHAPE_OVERLAP_GATE", v)
        assert sql_shape_overlap_gate_enabled() is True, v
    for v in ("0", "false", "no", "off", "", "garbage"):
        monkeypatch.setenv("GSO_SQL_SHAPE_OVERLAP_GATE", v)
        assert sql_shape_overlap_gate_enabled() is False, v
