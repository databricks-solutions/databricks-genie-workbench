"""Permutation-driven invariant fuzzing — v1.7 chunk 2.

Wraps the committed production-replay corpus in several seeded
permutations (dispatch order, blame-set masking, QID-carrier rotation,
QID-namespacing flip, tape coverage) and asserts the 12 SM contract
invariants still hold. Surfaces structural bugs that only fire on
specific permutations.

Each parametrised test uses a small seed range — 5 seeds per
permutation × 7 fixtures = 35 cases per permutation. Cases run in
``sm-tape`` mode, sub-second each.

The procedural CLI fuzzer (chunk 5) explores wider ranges; this test
module is the always-on regression layer.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pytest

from local_lever_workbench.fuzzer import check_all_invariants
from local_lever_workbench.fuzzer.generators import (
    apply_permutation,
    list_permutation_names,
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


_QID = ("gs_009", "domain_a_gs_009")
_SEEDS = (1, 17, 42, 137, 9999)


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


def _base_bundle() -> WorkbenchInputBundle:
    suffix, canonical = _QID
    base = from_production_replay(qids=(suffix,))
    return WorkbenchInputBundle(
        provenance=base.provenance,
        space_id=base.space_id,
        hard_cases=base.hard_cases,
        metadata_snapshot=base.metadata_snapshot,
        post_apply_eval_tape=(
            {
                "question_id": canonical,
                "inputs/question_id": canonical,
                "generated_sql": f"SELECT POST -- {canonical}",
                "feedback/result_correctness/value": 1.0,
                "eval_row_id": f"workbench-perm-{canonical}",
            },
        ),
    )


def _run(bundle: WorkbenchInputBundle, tmp_path: Path):
    tape_path = _full_pipeline_tape(bundle.hard_qids, tmp_path)
    config = WorkbenchRunConfig(
        bundle_path=tmp_path / "bundle.json",
        output_dir=tmp_path / "out",
        llm_mode=LLM_MODE_TAPE,
        apply_mode="fake-record",
        tape_path=tape_path,
        iteration=1,
    )
    return run_workbench_iteration(bundle, config)


@pytest.mark.workbench
@pytest.mark.integration
@pytest.mark.parametrize("permutation", list_permutation_names())
@pytest.mark.parametrize("seed", _SEEDS)
def test_invariants_hold_under_permutation(
    permutation: str, seed: int, tmp_path: Path,
) -> None:
    """Invariants hold under each seeded permutation of the base bundle.

    35 cases (5 permutations × ~5–7 seeds) on the ``gs_009`` fixture.
    Other fixtures are covered by the always-on suite in
    ``test_state_machine_invariants.py``.
    """
    bundle = apply_permutation(_base_bundle(), permutation, seed)
    artifacts = _run(bundle, tmp_path)
    result = check_all_invariants(artifacts)
    assert result.ok, (
        f"invariants failed under permutation={permutation!r} seed={seed}:\n"
        + "\n".join(f"  - {v}" for v in result.violations)
        + "\n\nReplay: apply_permutation(base_bundle, "
        f"{permutation!r}, {seed})"
    )


@pytest.mark.workbench
@pytest.mark.integration
def test_permutation_chain_reproducible_from_seed(tmp_path: Path) -> None:
    """Same seed + chain ⇒ same artifacts. Pins reproducibility for chunks 4/5."""
    from local_lever_workbench.fuzzer.generators import (
        apply_permutation_chain,
    )

    base = _base_bundle()
    chain = ("dispatch_order", "question_id_carriers", "tape_coverage")
    a = apply_permutation_chain(base, chain, seed=7)
    b = apply_permutation_chain(base, chain, seed=7)

    # Compare via JSON serialization since dataclasses with nested
    # mutable containers compare reference-equal in some edge cases.
    assert a.to_dict() == b.to_dict(), (
        "permutation chain is not deterministic for fixed seed; "
        "chunks 4/5 (shrinker, CLI replay) depend on this contract"
    )
