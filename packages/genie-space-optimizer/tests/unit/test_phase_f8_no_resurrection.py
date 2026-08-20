"""Assert retired post-evaluation modules stay deleted."""

from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize(
    "deleted_module",
    [
        "genie_space_optimizer.optimization.ag_outcome",
        "genie_space_optimizer.optimization.post_eval",
    ],
)
def test_phase_f8_deleted_modules_stay_deleted(deleted_module: str) -> None:
    """Retired modules must not become importable again."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(deleted_module)
