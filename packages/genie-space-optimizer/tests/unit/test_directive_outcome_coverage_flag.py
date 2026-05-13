"""Phase 3 Task 4 — directive_outcome_coverage flag accessor."""

from __future__ import annotations


def test_directive_outcome_coverage_defaults_on(monkeypatch) -> None:
    monkeypatch.delenv("GSO_DIRECTIVE_OUTCOME_COVERAGE", raising=False)
    from genie_space_optimizer.common.config import (
        directive_outcome_coverage_enabled,
    )
    assert directive_outcome_coverage_enabled() is True


def test_directive_outcome_coverage_disabled_via_falsy_env(monkeypatch) -> None:
    monkeypatch.setenv("GSO_DIRECTIVE_OUTCOME_COVERAGE", "0")
    from genie_space_optimizer.common.config import (
        directive_outcome_coverage_enabled,
    )
    assert directive_outcome_coverage_enabled() is False
