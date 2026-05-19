"""Plan 8 Task 4 — confirm harness invokes stages.candidate_critique
between the proposals stage and the acceptance stage."""
from __future__ import annotations

import inspect

import genie_space_optimizer.optimization.harness as h


def test_harness_imports_candidate_critique_stage():
    src = inspect.getsource(h)
    assert "from genie_space_optimizer.optimization.stages import (" in src
    # The wire-in introduces an import of the candidate_critique
    # stage and a call to its execute().
    assert "candidate_critique" in src, (
        "harness.py must reference the candidate_critique stage so "
        "Plan 6 fires per-iteration"
    )
    assert "CritiqueInput(" in src
    assert ".execute(" in src or "_crit_wrapped(" in src


def test_critique_gate_default_is_enforcing():
    from genie_space_optimizer.common.config import (
        critique_gate_enforcing_enabled,
    )
    # Default after Plan 8 Task 4 is ON; the flag is removed in
    # Task 12 (then this test is deleted).
    import os
    prior = os.environ.pop("GSO_CRITIQUE_GATE_ENFORCING", None)
    try:
        assert critique_gate_enforcing_enabled() is True
    finally:
        if prior is not None:
            os.environ["GSO_CRITIQUE_GATE_ENFORCING"] = prior
