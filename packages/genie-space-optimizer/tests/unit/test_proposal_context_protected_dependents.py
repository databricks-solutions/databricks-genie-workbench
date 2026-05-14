"""Phase 2.5 — ProposalContext.protected_dependents threading."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    ProposalContext,
    render_cluster_driven_prompt,
)


def test_proposal_context_has_protected_dependents_field():
    ctx = ProposalContext(
        cluster_id="c1",
        target_qids=("gs_001",),
        rca_card={"root_cause": "r"},
        protected_dependents=("gs_003", "gs_005"),
    )
    assert ctx.protected_dependents == ("gs_003", "gs_005")


def test_default_empty_tuple_when_not_set():
    ctx = ProposalContext(
        cluster_id="c1",
        target_qids=("gs_001",),
        rca_card={"root_cause": "r"},
    )
    assert ctx.protected_dependents == ()


def test_prompt_includes_protected_dependents_instruction():
    """When protected_dependents is non-empty, the rendered prompt
    must contain a 'preserve' / 'do not change' directive naming
    each protected QID."""
    ctx = ProposalContext(
        cluster_id="c1",
        target_qids=("gs_001",),
        rca_card={"root_cause": "r"},
        protected_dependents=("gs_003", "gs_005"),
    )
    prompt = render_cluster_driven_prompt(context=ctx)
    assert "gs_003" in prompt
    assert "gs_005" in prompt
    # The directive language — the prompt must explicitly mark the
    # protected QIDs as untouchable.
    lower = prompt.lower()
    assert "preserve" in lower or "do not change" in lower or "protect" in lower


def test_prompt_does_not_include_protected_section_when_empty():
    """When protected_dependents is empty, the prompt MUST NOT
    contain the 'preserve' directive (byte-stable with pre-Phase-2.5
    fixtures)."""
    ctx = ProposalContext(
        cluster_id="c1",
        target_qids=("gs_001",),
        rca_card={"root_cause": "r"},
        protected_dependents=(),
    )
    prompt = render_cluster_driven_prompt(context=ctx)
    # The protected-dependents section header must not appear.
    assert "Protected dependent QIDs" not in prompt
