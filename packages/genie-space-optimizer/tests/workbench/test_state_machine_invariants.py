"""State-machine contract invariants — fast always-on suite (v1.7 chunk 1).

Asserts the 12 invariants in
``devtools/local_lever_workbench/fuzzer/invariants.py`` on every
committed production-replay fixture in ``sm-tape`` mode. The fixtures
are the same ones the v1.6 acceptance suite exercises, so this module
proves that on the *current* tree all 12 invariants hold simultaneously
on real, production-shaped inputs.

If a future change regresses an invariant — e.g. removes the Trial-16
``forbidden_signature`` wiring, drops the ``eval_qids`` slice, or
silently routes a QID off the funnel — this suite fires before any
deploy.

The procedural fuzzer (chunks 2-5) explores permutations and synthetic
inputs over the same predicates; chunk 6 reverts the Trial-16 fixes via
monkeypatch and asserts that this same predicate library flags them.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pytest

from local_lever_workbench.fuzzer import (
    InvariantResult,
    check_all_invariants,
)
from local_lever_workbench.input_bundle import from_production_replay
from local_lever_workbench.local_runner import (
    LLM_MODE_TAPE,
    run_workbench_iteration,
)
from local_lever_workbench.models import (
    WorkbenchInputBundle,
    WorkbenchRunConfig,
)


# Shortest replay fixture suffix → canonical QID admitted by the corpus
# loader. The set mirrors the committed production-replay corpus
# (post-Trial 13 cleanup).
_REPLAY_FIXTURES: tuple[tuple[str, str], ...] = (
    ("gs_001", "domain_b_gs_001"),
    ("gs_009", "domain_a_gs_009"),
    ("gs_013", "domain_b_gs_013"),
    ("gs_016", "domain_a_gs_016"),
    ("gs_021", "domain_b_gs_021"),
    ("gs_024", "domain_a_gs_024"),
    ("gs_026", "domain_b_gs_026"),
)


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


def _run_fixture(
    *,
    filter_suffix: str,
    qid_canonical: str,
    tmp_path: Path,
    post_apply_score: float,
) -> InvariantResult:
    """Build a bundle for ``filter_suffix``, run sm-tape mode with a
    post-apply tape that pushes the QID to ACCEPTED, and return the
    invariant aggregate.
    """
    base = from_production_replay(qids=(filter_suffix,))
    bundle = WorkbenchInputBundle(
        provenance=base.provenance,
        space_id=base.space_id,
        hard_cases=base.hard_cases,
        metadata_snapshot=base.metadata_snapshot,
        post_apply_eval_tape=(
            {
                "question_id": qid_canonical,
                "inputs/question_id": qid_canonical,
                "generated_sql": f"SELECT POST -- {qid_canonical}",
                "feedback/result_correctness/value": post_apply_score,
                "eval_row_id": f"workbench-inv-{qid_canonical}",
            },
        ),
    )
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
    return check_all_invariants(artifacts)


@pytest.mark.workbench
@pytest.mark.integration
@pytest.mark.parametrize(
    "filter_suffix,qid_canonical",
    _REPLAY_FIXTURES,
    ids=[qid for _suffix, qid in _REPLAY_FIXTURES],
)
def test_state_machine_invariants_hold_on_accepted_path(
    filter_suffix: str,
    qid_canonical: str,
    tmp_path: Path,
) -> None:
    """All 12 invariants hold when a hard QID reaches ACCEPTED.

    The post-apply tape says the QID was fixed (1.0 score), so the
    acceptance path closes. This is the happy path; the rolled-back
    path is covered by the next test.
    """
    result = _run_fixture(
        filter_suffix=filter_suffix,
        qid_canonical=qid_canonical,
        tmp_path=tmp_path,
        post_apply_score=1.0,
    )
    assert result.ok, (
        f"invariants failed for {qid_canonical}:\n"
        + "\n".join(f"  - {v}" for v in result.violations)
    )


@pytest.mark.workbench
@pytest.mark.integration
@pytest.mark.parametrize(
    "filter_suffix,qid_canonical",
    _REPLAY_FIXTURES,
    ids=[qid for _suffix, qid in _REPLAY_FIXTURES],
)
def test_state_machine_invariants_hold_on_rolled_back_path(
    filter_suffix: str,
    qid_canonical: str,
    tmp_path: Path,
) -> None:
    """All 12 invariants hold when the acceptance gate rolls back.

    The post-apply tape returns the baseline (0.0) score so
    ``acceptance_gate`` terminates with ``target_unchanged:...``. The
    invariants must hold equally on the terminated-by-gate path —
    that's exactly the path B2 and E1 were designed to police.
    """
    result = _run_fixture(
        filter_suffix=filter_suffix,
        qid_canonical=qid_canonical,
        tmp_path=tmp_path,
        post_apply_score=0.0,
    )
    assert result.ok, (
        f"invariants failed for {qid_canonical} on rolled-back path:\n"
        + "\n".join(f"  - {v}" for v in result.violations)
    )
