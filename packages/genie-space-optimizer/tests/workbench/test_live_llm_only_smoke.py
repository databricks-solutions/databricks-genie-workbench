"""Live-LLM-only smoke test for the workbench (Trial 16 v1.8).

These tests issue real Databricks model-serving calls and require:
* ``DATABRICKS_*`` env vars or a configured CLI profile (default
  ``fevm-prashanth``; override with ``GSO_WORKBENCH_LIVE_PROFILE``);
* the ``LLM_MODEL`` env var (defaults to
  ``databricks-claude-sonnet-4-6``) pointed at a serving endpoint the
  profile is allowed to query;
* network connectivity to that workspace.

They are guarded by the ``live_llm`` marker, so CI / default pytest
collection skips them. To run::

    DATABRICKS_CONFIG_PROFILE=fevm-prashanth \\
        uv run --frozen pytest -m live_llm \\
        tests/workbench/test_live_llm_only_smoke.py -v

The test's job is small and well-scoped: prove that the workbench's
new ``live-llm-only`` mode actually exercises Stage 1/2/3 LLM
endpoints against a real fixture and produces an SM-shaped funnel
report we can assert against. Discovery of new prompt regressions
happens in ``devtools/local_lever_workbench/sweeps/live_llm_sweep.py``.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from local_lever_workbench.input_bundle import from_production_replay
from local_lever_workbench.local_runner import (
    LLM_MODE_LIVE_LLM_ONLY,
    run_workbench_iteration,
    summarize_stage_progress,
)
from local_lever_workbench.models import WorkbenchRunConfig


# ── live_llm marker registration ──────────────────────────────────────


def _default_profile() -> str:
    return os.environ.get("GSO_WORKBENCH_LIVE_PROFILE", "fevm-prashanth")


# Opt-in switch. Default-collect must NOT issue live calls. Set
# ``GSO_WORKBENCH_LIVE_LLM=1`` (or run with ``-m live_llm`` and the
# environment variable) to enable the live smoke / sweep.
_LIVE_LLM_ENABLED = os.environ.get("GSO_WORKBENCH_LIVE_LLM", "") == "1"
_skip_unless_live = pytest.mark.skipif(
    not _LIVE_LLM_ENABLED,
    reason=(
        "live-llm smoke is opt-in: set GSO_WORKBENCH_LIVE_LLM=1 "
        "(and ensure GSO_WORKBENCH_LIVE_PROFILE / DATABRICKS_* env vars "
        "are configured) to run this test."
    ),
)


# ── smoke test ────────────────────────────────────────────────────────


@_skip_unless_live
@pytest.mark.live_llm
@pytest.mark.workbench
@pytest.mark.integration
def test_live_llm_only_runs_stage1_through_terminal_on_one_qid(
    tmp_path: Path,
) -> None:
    """End-to-end smoke: one QID through the live LLM stack.

    Assertions are deliberately permissive — the workbench's job here
    is to PROVE THAT WE'RE TALKING TO LIVE ENDPOINTS, not to grade
    the LLM's output. Any QID that fails the strategist/lever prompts
    (returns malformed JSON, hallucinated patch_type, etc.) will
    abstain at Stage 1/2/3 with a terminal reason — that is itself a
    valid signal: the new bug class this mode is built to find.

    The hard assertions are:

    * the run finishes without raising;
    * every admitted QID lands at a terminal state (TerminalRecord or
      ACCEPTED), so the SM contract holds even under live output.
    """
    profile = _default_profile()

    # Single-QID slice keeps the smoke fast. ``gs_009`` is the
    # canonical baseline in the production-replay corpus — typed
    # evidence is sufficient for the Stage 1 LLM to produce a
    # diagnosis envelope. NOTE: ``from_production_replay`` filters by
    # the fixture filename suffix (e.g. ``gs_009``), not by the
    # internal ``qid`` field, which is the sanitized ``domain_a_gs_009``.
    bundle = from_production_replay(qids=("gs_009",))
    assert bundle.hard_qids, (
        "production-replay corpus is missing gs_009 — check the "
        "fixture commit before suspecting the workbench wiring."
    )

    output_dir = tmp_path / "live_llm_smoke"
    config = WorkbenchRunConfig(
        bundle_path=tmp_path / "bundle.json",
        output_dir=output_dir,
        llm_mode=LLM_MODE_LIVE_LLM_ONLY,
        profile=profile,
        iteration=1,
    )
    artifacts = run_workbench_iteration(bundle, config)

    # Funnel report must be present and well-formed.
    progress = summarize_stage_progress(artifacts)
    assert progress, "live-llm-only produced an empty funnel — runner regression"

    # Every QID must end at a terminal state. ACCEPTED is the happy
    # path; a TerminalRecord with a typed reason is the "live LLM
    # surfaced a bug" path; anything else means the SM contract is
    # broken and the fuzzer's A1 invariant would flag it too.
    for p in progress:
        deepest = p.deepest_stage.lower()
        assert deepest in {
            "accepted",
            "terminated",
        } or p.terminal_reason, (
            f"QID {p.qid!r} ended at non-terminal {deepest!r} with no "
            f"terminal_reason — SM contract violation under live LLM. "
            f"funnel={progress!r}"
        )

    # Stdout must contain at least one stage marker so the operator
    # knows the live calls actually fired (vs a silent dispatch
    # short-circuit). The Stage 1 envelope marker is the earliest
    # stable signal — present in every successful live run.
    assert "plan11_stage1" in artifacts.stdout_text or "stage1" in artifacts.stdout_text.lower(), (
        f"live-llm-only produced no Stage 1 marker in stdout — Stage 1 "
        f"transformer never reached the LLM call. "
        f"stdout_head={artifacts.stdout_text[:500]!r}"
    )
