"""Unit tests for Plan B's L5b rich-dispatch routing.

Covers (in order):
  - rich_synthesis_primary_for_sql_shape_enabled() — feature flag.
  - should_route_l5b_to_rich_synthesizer(cluster) — routing predicate.
  - _normalize_rich_proposal_to_l5b_shape(proposal) — output adapter.
  - _dispatch_rich_synthesis_for_l5b(...) — rich-path executor.
  - drain_l5b_rich_path_declines() — ledger drain.
"""
from __future__ import annotations

from typing import Any


def test_flag_defaults_off(monkeypatch: Any) -> None:
    """Default state: rich-path routing is OFF. Byte-stable fixtures
    continue to pin the lean path."""
    monkeypatch.delenv(
        "GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", raising=False,
    )
    from genie_space_optimizer.common.config import (
        rich_synthesis_primary_for_sql_shape_enabled,
    )
    assert rich_synthesis_primary_for_sql_shape_enabled() is False


def test_flag_on_when_env_var_set_to_one(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "1")
    from genie_space_optimizer.common.config import (
        rich_synthesis_primary_for_sql_shape_enabled,
    )
    assert rich_synthesis_primary_for_sql_shape_enabled() is True


def test_flag_off_when_env_var_set_to_zero(monkeypatch: Any) -> None:
    monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", "0")
    from genie_space_optimizer.common.config import (
        rich_synthesis_primary_for_sql_shape_enabled,
    )
    assert rich_synthesis_primary_for_sql_shape_enabled() is False


def test_flag_accepts_truthy_strings(monkeypatch: Any) -> None:
    """Accepts ``true``, ``True``, ``yes``, ``Y`` (case-insensitive).
    Rejects empty / unknown values."""
    from genie_space_optimizer.common.config import (
        rich_synthesis_primary_for_sql_shape_enabled,
    )
    for v in ("true", "TRUE", "True", "yes", "Yes", "Y", "1"):
        monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", v)
        assert rich_synthesis_primary_for_sql_shape_enabled() is True, v
    for v in ("0", "false", "no", "off", "", "garbage"):
        monkeypatch.setenv("GSO_RICH_SYNTHESIS_PRIMARY_FOR_SQL_SHAPE", v)
        assert rich_synthesis_primary_for_sql_shape_enabled() is False, v
