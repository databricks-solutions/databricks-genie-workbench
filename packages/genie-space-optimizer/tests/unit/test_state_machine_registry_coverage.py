"""SM Cutover Phase 2 — registry coverage invariant.

The production state machine is the only path from a hard QID to an
APPLIED patch. That guarantee requires that **every** non-terminal,
non-final FunnelStage has at least one registered transformer in the
production registry. This test catches "forgot to wire a stage" bugs
that would otherwise surface as silent no-ops in production.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.funnel import FunnelStage
from genie_space_optimizer.optimization.state_machine.registry import (
    PHASE3_REGISTRY,
)


# Stages the SM does NOT transform (input is the boundary; ACCEPTED /
# TERMINATED are terminals).
_NON_TRANSFORM_STAGES = frozenset({
    FunnelStage.ACCEPTED,
    FunnelStage.TERMINATED,
})


def test_every_non_terminal_funnel_stage_has_a_transformer() -> None:
    """Every non-terminal stage must appear in the registry, else QIDs
    silently get stuck at that stage.
    """
    missing = []
    for stage in FunnelStage:
        if stage in _NON_TRANSFORM_STAGES:
            continue
        if stage not in PHASE3_REGISTRY or not PHASE3_REGISTRY[stage]:
            missing.append(stage)
    assert not missing, (
        f"Stages without a registered transformer: {missing!r}. "
        "Every non-terminal FunnelStage must be wired in PHASE3_REGISTRY."
    )


def test_registry_only_references_known_stages() -> None:
    """No typos / dead stages in the registry."""
    known = set(FunnelStage)
    extra = set(PHASE3_REGISTRY.keys()) - known
    assert not extra, f"Registry references unknown stages: {extra!r}"


def test_every_transformer_has_a_name() -> None:
    """Telemetry markers carry the transformer name; missing names make
    postmortems unreadable.
    """
    for stage, transformers in PHASE3_REGISTRY.items():
        for t in transformers:
            name = getattr(t, "name", "")
            assert name, (
                f"Transformer for stage {stage!r} has no .name; "
                f"telemetry would emit a blank transformer_name."
            )
