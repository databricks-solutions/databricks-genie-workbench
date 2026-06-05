"""Unit tests for Phase 1 P1.4 — within-iteration write-through of
``insufficient_repair_signature`` from ``acceptance_gate`` into
``ctx.extras["_live_insufficient_repair_signatures"]``, and the
``_live_insufficient_repair_signatures`` merger used by the
``synthesize_llm`` transformer.

These tests target the helper functions only; the full end-to-end
acceptance_gate → synthesize_llm SM dispatch is covered by integration
tests."""
from __future__ import annotations

from types import SimpleNamespace

from genie_space_optimizer.optimization.state_machine.transformers import (
    synthesize_llm,
)


def _ctx(*, static_sigs: tuple[str, ...] = (), extras: dict | None = None):
    return SimpleNamespace(
        insufficient_repair_signatures=static_sigs,
        extras=extras if extras is not None else {},
    )


def test_live_merger_returns_only_static_when_no_bucket() -> None:
    ctx = _ctx(static_sigs=("a:1:rca_join:b",))
    merged = synthesize_llm._live_insufficient_repair_signatures(ctx)
    assert merged == ("a:1:rca_join:b",)


def test_live_merger_returns_only_live_when_no_static() -> None:
    ctx = _ctx(
        extras={"_live_insufficient_repair_signatures": ["x:y:rca_topn:b"]},
    )
    merged = synthesize_llm._live_insufficient_repair_signatures(ctx)
    assert merged == ("x:y:rca_topn:b",)


def test_live_merger_combines_static_first_then_live() -> None:
    ctx = _ctx(
        static_sigs=("a:1:rca_join:b", "a:2:rca_topn:b"),
        extras={"_live_insufficient_repair_signatures": ["z:9:rca_value:b"]},
    )
    merged = synthesize_llm._live_insufficient_repair_signatures(ctx)
    # Static signatures retain their order; live signatures append.
    assert merged == (
        "a:1:rca_join:b",
        "a:2:rca_topn:b",
        "z:9:rca_value:b",
    )


def test_live_merger_dedupes_across_static_and_live() -> None:
    """When a live signature already appears in the static set, it is
    not duplicated in the merged tuple."""
    ctx = _ctx(
        static_sigs=("a:1:rca_join:b",),
        extras={
            "_live_insufficient_repair_signatures": [
                "a:1:rca_join:b",  # duplicate
                "b:2:rca_topn:b",  # new
            ],
        },
    )
    merged = synthesize_llm._live_insufficient_repair_signatures(ctx)
    assert merged == ("a:1:rca_join:b", "b:2:rca_topn:b")


def test_live_merger_drops_empty_strings() -> None:
    ctx = _ctx(
        extras={
            "_live_insufficient_repair_signatures": [
                "",
                None,
                "valid:1:rca:b",
            ],
        },
    )
    merged = synthesize_llm._live_insufficient_repair_signatures(ctx)
    assert merged == ("valid:1:rca:b",)


def test_live_merger_returns_empty_when_both_empty() -> None:
    ctx = _ctx()
    merged = synthesize_llm._live_insufficient_repair_signatures(ctx)
    assert merged == ()


def test_live_merger_handles_missing_extras() -> None:
    ctx = SimpleNamespace(
        insufficient_repair_signatures=("a:1:rca:b",),
        # No extras attribute at all.
    )
    # Should not raise — falls back to static sigs only.
    merged = synthesize_llm._live_insufficient_repair_signatures(ctx)
    assert merged == ("a:1:rca:b",)


def test_live_merger_handles_malformed_extras() -> None:
    """``extras`` is not a dict — best-effort fallthrough returns
    only the static signatures."""
    ctx = SimpleNamespace(
        insufficient_repair_signatures=("a:1:rca:b",),
        extras="not_a_dict",  # malformed
    )
    merged = synthesize_llm._live_insufficient_repair_signatures(ctx)
    assert merged == ("a:1:rca:b",)
