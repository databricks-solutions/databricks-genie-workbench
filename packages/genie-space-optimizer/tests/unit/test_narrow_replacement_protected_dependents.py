"""Phase 2.5 — narrow-replacement helpers accept protected_dependents."""
from __future__ import annotations

import inspect

from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    build_narrow_l6_replacement,
    build_l5_example_sql_replacement,
)


def test_build_narrow_l6_replacement_accepts_protected_dependents():
    sig = inspect.signature(build_narrow_l6_replacement)
    assert "protected_dependents" in sig.parameters


def test_build_l5_example_sql_replacement_accepts_protected_dependents():
    sig = inspect.signature(build_l5_example_sql_replacement)
    assert "protected_dependents" in sig.parameters
