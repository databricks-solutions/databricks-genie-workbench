"""Workbench V1.5 — ctx_kwargs differs by llm_mode.

live-databricks: ctx.eval_kwargs and ctx.stage_ctx must be populated
with real production-shape values from
_build_canary_stage_ctx_and_eval_kwargs.

sm-tape: ctx.eval_kwargs stays None; the workbench stub remains the
post_apply_eval source via ctx.extras['post_apply_eval'].
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from local_lever_workbench.local_runner import (
    LLM_MODE_LIVE,
    LLM_MODE_LIVE_LLM_ONLY,
    LLM_MODE_TAPE,
    SUPPORTED_LLM_MODES,
    _is_live_llm,
    _make_ctx_kwargs_for_test,
    _needs_canary_stack,
)


@pytest.mark.workbench
def test_sm_tape_mode_uses_stub_eval_kwargs_remain_none() -> None:
    bundle = _minimal_bundle_fixture()
    ctx_kwargs = _make_ctx_kwargs_for_test(
        llm_mode=LLM_MODE_TAPE,
        bundle=bundle,
        workspace_client=None,
        spark=None,
    )
    assert ctx_kwargs.get("eval_kwargs") is None
    assert ctx_kwargs.get("stage_ctx") is None
    assert "post_apply_eval" in ctx_kwargs["extras"]


@pytest.mark.workbench
def test_live_databricks_mode_populates_real_eval_kwargs(monkeypatch) -> None:
    bundle = _minimal_bundle_fixture()
    workspace_client = MagicMock(name="WorkspaceClient")
    spark = MagicMock(name="SparkSession")

    # Stub make_predict_fn and make_all_scorers to avoid invoking the
    # real evaluation module (which would try to call Spark).
    import genie_space_optimizer.optimization.evaluation as _ev_mod
    import genie_space_optimizer.optimization.scorers as _sc_mod

    monkeypatch.setattr(_ev_mod, "make_predict_fn", lambda *a, **k: MagicMock(name="predict_fn"))
    monkeypatch.setattr(_sc_mod, "make_all_scorers", lambda *a, **k: [MagicMock(name="scorer")])

    ctx_kwargs = _make_ctx_kwargs_for_test(
        llm_mode=LLM_MODE_LIVE,
        bundle=bundle,
        workspace_client=workspace_client,
        spark=spark,
    )
    assert ctx_kwargs["eval_kwargs"] is not None
    assert ctx_kwargs["stage_ctx"] is not None
    assert ctx_kwargs["eval_kwargs"]["space_id"] == bundle.space_id
    assert ctx_kwargs["eval_qids"] == ("q1",)
    # extras["post_apply_eval"] must NOT be set in live mode — the real
    # gate path takes over.
    assert "post_apply_eval" not in ctx_kwargs["extras"]


@pytest.mark.workbench
def test_live_llm_only_mode_uses_tape_stub_and_skips_canary_stack() -> None:
    """live-llm-only must keep the post_apply_eval stub (tape-driven)
    and NEVER touch make_predict_fn / make_all_scorers — the whole
    point is to skip the canary stack so the operator doesn't need
    Spark/Genie API/MLflow scorers configured.
    """
    bundle = _minimal_bundle_fixture()
    # workspace_client is a real-shape mock; spark stays None because
    # _needs_canary_stack(live-llm-only) is False.
    workspace_client = MagicMock(name="WorkspaceClient")

    ctx_kwargs = _make_ctx_kwargs_for_test(
        llm_mode=LLM_MODE_LIVE_LLM_ONLY,
        bundle=bundle,
        workspace_client=workspace_client,
        spark=None,
    )
    # Canary stack must remain empty.
    assert ctx_kwargs["eval_kwargs"] is None
    assert ctx_kwargs["stage_ctx"] is None
    # Tape stub must be installed for evaluated_gate / acceptance_gate.
    assert "post_apply_eval" in ctx_kwargs["extras"]
    # The workspace_client must still be propagated so Stage 1/2/3 can
    # make real LLM calls via ctx.w.
    assert ctx_kwargs["w"] is workspace_client


@pytest.mark.workbench
def test_live_llm_only_is_in_supported_modes() -> None:
    """Plumbing sanity: the new mode is wired into the supported set
    and classified correctly by the helper predicates.
    """
    assert LLM_MODE_LIVE_LLM_ONLY in SUPPORTED_LLM_MODES
    assert _is_live_llm(LLM_MODE_LIVE_LLM_ONLY) is True
    assert _is_live_llm(LLM_MODE_LIVE) is True
    assert _is_live_llm(LLM_MODE_TAPE) is False
    # live-llm-only deliberately does NOT need the canary stack
    # (Spark + predict_fn + scorers) — that's the whole point.
    assert _needs_canary_stack(LLM_MODE_LIVE_LLM_ONLY) is False
    assert _needs_canary_stack(LLM_MODE_LIVE) is True


def _minimal_bundle_fixture():
    """Build a minimal bundle-shaped object for the ctx_kwargs builder."""
    from types import SimpleNamespace

    return SimpleNamespace(
        space_id="01234567890123456789012345678901",
        catalog="cat",
        schema="sch",
        domain="dom",
        benchmarks=[{"question_id": "q1"}],
        metadata_snapshot={},
        rca_evidence={},
        reference_sqls=None,
        uc_schema="cat.sch",
    )
