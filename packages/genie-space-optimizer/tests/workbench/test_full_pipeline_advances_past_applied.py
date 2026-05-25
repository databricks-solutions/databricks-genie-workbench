"""Workbench V1.5 acceptance — sm-tape mode with stub eval reaches
past APPLIED for a fixture bundle.

This is the smallest end-to-end demonstration that the registry
extension + ctx_kwargs branching work together. No live workspace
required; the Trial 15 stub keeps it hermetic.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pytest

from local_lever_workbench.input_bundle import from_bundle_json, from_production_replay
from local_lever_workbench.local_runner import (
    LLM_MODE_TAPE,
    run_workbench_iteration,
    summarize_stage_progress,
)
from local_lever_workbench.models import WorkbenchRunConfig


def _serialize_tape(entries: Iterable, path: Path) -> Path:
    """Serialize TapeEntry objects to JSONL for TapeReplayHarness.load_tape."""
    path.write_text(
        "\n".join(
            json.dumps(
                {
                    "kind": e.kind,
                    "skill_id": e.skill_id,
                    "call_id": e.call_id,
                    "iteration": e.iteration,
                    "qid": e.qid,
                    "parsed_output": e.parsed_output,
                    "raw_text": e.raw_text,
                    "tokens_input": e.tokens_input,
                    "tokens_output": e.tokens_output,
                    "duration_ms": e.duration_ms,
                    "exception_class": e.exception_class,
                    "exception_message": e.exception_message,
                }
            )
            for e in entries
        )
    )
    return path


PAST_APPLIED = ("evaluated", "accepted", "terminated")


@pytest.mark.workbench
@pytest.mark.integration
def test_sm_tape_fixture_bundle_advances_to_evaluated_or_terminated(
    tmp_path: Path,
) -> None:
    """A fixture bundle in sm-tape mode must reach a state past APPLIED.

    Acceptable terminal stages: ``evaluated`` (gate passed), ``accepted``
    (post>pre — won't happen with the stub since post==pre),
    or ``terminated`` (acceptance_gate rejected with target_unchanged).
    UN-acceptable: any state at ``applied`` or earlier — means the
    new gates never fired.
    """
    fixture_bundle_path = (
        Path(__file__).resolve().parents[2]
        / "devtools"
        / "local_lever_workbench"
        / "runs"
        / "98ec_post_trial14"
        / "bundle.json"
    )
    if not fixture_bundle_path.exists():
        pytest.skip(f"fixture bundle not found: {fixture_bundle_path}")

    from tests.integration.sm_forward_tapes import (
        cluster_response_tape,
        diagnose_response_tape,
        synthesize_response_tape,
    )

    bundle = from_bundle_json(fixture_bundle_path)
    qids = bundle.hard_qids
    assert qids, "fixture bundle must admit at least one hard QID"

    tape: list = []
    for _ in range(5):
        tape += diagnose_response_tape(qids)
    for _ in range(5):
        tape += cluster_response_tape(qids)
    for _ in range(5):
        tape += synthesize_response_tape(qids)

    tape_path = _serialize_tape(tape, tmp_path / "forward.jsonl")
    output_dir = tmp_path / "_tmp_e2e_smoke"
    config = WorkbenchRunConfig(
        bundle_path=fixture_bundle_path.resolve(),
        output_dir=output_dir,
        llm_mode=LLM_MODE_TAPE,
        apply_mode="fake-record",
        tape_path=tape_path,
        iteration=1,
    )
    artifacts = run_workbench_iteration(bundle, config)

    progress = summarize_stage_progress(artifacts)
    deepest_per_qid = {p.qid: p.deepest_stage for p in progress}
    assert deepest_per_qid, "no final states produced — bundle load failed"

    # At least one QID must reach past APPLIED. Allow the broader set
    # because in sm-tape with stub, post==pre, so acceptance_gate
    # rejects with target_unchanged → state lands at TERMINATED
    # but with deepest_stage_reached=EVALUATED.
    advanced = [
        qid for qid, stage in deepest_per_qid.items() if stage in PAST_APPLIED
    ]
    terminal_reasons = {
        p.qid: p.terminal_reason for p in progress if p.terminal_reason
    }
    assert advanced, (
        f"no QID advanced past APPLIED; deepest reached={deepest_per_qid}, "
        f"terminal_reasons={terminal_reasons}. "
        "Registry extension or stub wiring may be broken."
    )


@pytest.mark.workbench
@pytest.mark.integration
def test_production_replay_corpus_advances_past_applied_in_sm_tape(
    tmp_path: Path,
) -> None:
    """CI-runnable V1.5 acceptance — production-replay corpus reaches past APPLIED.

    Uses the committed production-replay corpus (no external fixture)
    so the V1.5 stub-eval contract is exercised on every CI run.
    """
    from tests.integration.sm_forward_tapes import (
        cluster_response_tape,
        diagnose_response_tape,
        synthesize_response_tape,
    )

    bundle = from_production_replay()
    qids = bundle.hard_qids
    assert qids, "production-replay corpus must commit at least one QID"

    tape: list = []
    for _ in range(5):
        tape += diagnose_response_tape(qids)
    for _ in range(5):
        tape += cluster_response_tape(qids)
    for _ in range(5):
        tape += synthesize_response_tape(qids)

    tape_path = _serialize_tape(tape, tmp_path / "forward.jsonl")
    output_dir = tmp_path / "run"
    config = WorkbenchRunConfig(
        bundle_path=tmp_path / "bundle.json",
        output_dir=output_dir,
        llm_mode=LLM_MODE_TAPE,
        apply_mode="fake-record",
        tape_path=tape_path,
        iteration=1,
    )
    artifacts = run_workbench_iteration(bundle, config)

    progress = summarize_stage_progress(artifacts)
    deepest_per_qid = {p.qid: p.deepest_stage for p in progress}
    assert deepest_per_qid, "no final states produced — bundle load failed"

    advanced = [
        qid for qid, stage in deepest_per_qid.items() if stage in PAST_APPLIED
    ]
    terminal_reasons = {
        p.qid: p.terminal_reason for p in progress if p.terminal_reason
    }
    assert advanced, (
        f"no QID advanced past APPLIED; deepest reached={deepest_per_qid}, "
        f"terminal_reasons={terminal_reasons}. "
        "Registry extension or stub wiring may be broken."
    )
