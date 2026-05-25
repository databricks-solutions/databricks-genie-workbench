"""Synthetic-input invariant fuzzing — v1.7 chunk 3.

Runs deterministic-seeded synthetic bundles (synthetic eval rows,
synthetic typed_evidence, synthetic post-apply tape) through the SM
and asserts the 12 invariants hold. Surfaces structural bugs that
only fire under evidence shapes the production corpus has not
captured yet.

The synthesizer reuses the corpus QIDs so the Stage-1/2/3 LLM tape
harnesses route correctly — only the *evidence shape* and post-apply
correctness are synthesised.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pytest

from local_lever_workbench.fuzzer import check_all_invariants
from local_lever_workbench.fuzzer.generators import synthesize_bundle
from local_lever_workbench.input_bundle import from_production_replay
from local_lever_workbench.local_runner import (
    LLM_MODE_TAPE,
    run_workbench_iteration,
)
from local_lever_workbench.models import WorkbenchRunConfig


_QID = ("gs_009",)
_SEEDS = (1, 17, 42, 137, 9999, 1000003)


def _serialize_tape(entries: Iterable, path: Path) -> Path:
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


def _full_pipeline_tape(qids: tuple[str, ...], tmp_path: Path) -> Path:
    from tests.integration.sm_forward_tapes import (
        cluster_response_tape,
        diagnose_response_tape,
        synthesize_response_tape,
    )

    tape: list = []
    for _ in range(5):
        tape += diagnose_response_tape(qids)
    for _ in range(5):
        tape += cluster_response_tape(qids)
    for _ in range(5):
        tape += synthesize_response_tape(qids)
    return _serialize_tape(tape, tmp_path / "forward.jsonl")


@pytest.mark.workbench
@pytest.mark.integration
@pytest.mark.parametrize("seed", _SEEDS)
def test_invariants_hold_on_synthetic_bundles(
    seed: int, tmp_path: Path,
) -> None:
    """Invariants hold under each seeded synthetic bundle."""
    base = from_production_replay(qids=_QID)
    bundle = synthesize_bundle(base=base, seed=seed)
    tape_path = _full_pipeline_tape(bundle.hard_qids, tmp_path)
    config = WorkbenchRunConfig(
        bundle_path=tmp_path / "bundle.json",
        output_dir=tmp_path / "out",
        llm_mode=LLM_MODE_TAPE,
        apply_mode="fake-record",
        tape_path=tape_path,
        iteration=1,
    )
    artifacts = run_workbench_iteration(bundle, config)
    result = check_all_invariants(artifacts)
    assert result.ok, (
        f"invariants failed on synthetic bundle seed={seed}:\n"
        + "\n".join(f"  - {v}" for v in result.violations)
        + f"\n\nReplay: synthesize_bundle(base, seed={seed})"
    )


@pytest.mark.workbench
@pytest.mark.integration
def test_synthesize_bundle_is_deterministic() -> None:
    """Same seed ⇒ byte-identical bundle. Pins the reproducibility contract."""
    base = from_production_replay(qids=_QID)
    a = synthesize_bundle(base=base, seed=123)
    b = synthesize_bundle(base=base, seed=123)
    assert a.to_dict() == b.to_dict()

    # Different seed ⇒ different bundle (otherwise the synthesizer is
    # not actually drawing from its RNG).
    c = synthesize_bundle(base=base, seed=124)
    assert a.to_dict() != c.to_dict()
