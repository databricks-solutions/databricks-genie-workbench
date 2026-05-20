"""Plan 9 Task 11 — plan7_rollback_learning_enabled defaults ON."""
import os

from genie_space_optimizer.common.config import (
    plan7_rollback_learning_enabled,
)


def test_plan7_rollback_learning_default_on(monkeypatch):
    monkeypatch.delenv("GSO_PLAN7_ROLLBACK_LEARNING", raising=False)
    assert plan7_rollback_learning_enabled() is True


def test_plan7_rollback_learning_explicit_off_disables(monkeypatch):
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "0")
    assert plan7_rollback_learning_enabled() is False


def test_plan7_rollback_learning_explicit_on_enables(monkeypatch):
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "1")
    assert plan7_rollback_learning_enabled() is True
