"""Local lever-loop workbench fuzzer CLI — v1.7 chunk 5.

Usage::

    uv run python -m local_lever_workbench.fuzzer \\
        --iterations 100 --seed 42

    uv run python -m local_lever_workbench.fuzzer --replay 42 --shrink

Modes:

* default — generate `--iterations` permuted / synthetic bundles,
  run each through the workbench in ``sm-tape`` mode, assert
  invariants. On any violation, exit non-zero with the seed +
  permutation chain that triggered it.
* ``--replay N`` — reproduce the failing run for seed ``N``.
* ``--shrink`` — on violation, run the greedy shrinker and print the
  minimal triggering bundle.

The CLI does not require Databricks credentials, MLflow, or any
network access. It exercises the same code paths the pytest workbench
suite uses; the only difference is breadth (100s–1000s of seeds
versus 5 hand-picked).
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Iterable, Sequence

from local_lever_workbench.fuzzer.generators import (
    apply_permutation,
    apply_permutation_chain,
    list_permutation_names,
    synthesize_bundle,
)
from local_lever_workbench.fuzzer.invariants import (
    InvariantResult,
    InvariantViolation,
    check_all_invariants,
)
from local_lever_workbench.fuzzer.shrinker import shrink_bundle
from local_lever_workbench.input_bundle import from_production_replay
from local_lever_workbench.local_runner import (
    LLM_MODE_TAPE,
    run_workbench_iteration,
)
from local_lever_workbench.models import (
    WorkbenchInputBundle,
    WorkbenchRunConfig,
)


# ─── Tape harness import — runs from package root only ──────────────


try:
    from tests.integration.sm_forward_tapes import (  # type: ignore
        cluster_response_tape,
        diagnose_response_tape,
        synthesize_response_tape,
    )
except ImportError as exc:  # noqa: BLE001
    raise SystemExit(
        "fuzzer CLI requires tests/integration/sm_forward_tapes; run "
        "from the genie-space-optimizer package root."
    ) from exc


# ─── Helpers (mirrors test_state_machine_invariants*.py) ────────────


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


def _full_pipeline_tape(qids: tuple[str, ...], tmp_dir: Path) -> Path:
    tape: list = []
    for _ in range(5):
        tape += diagnose_response_tape(qids)
    for _ in range(5):
        tape += cluster_response_tape(qids)
    for _ in range(5):
        tape += synthesize_response_tape(qids)
    return _serialize_tape(tape, tmp_dir / "forward.jsonl")


def _run_one(
    bundle: WorkbenchInputBundle, tmp_dir: Path,
) -> InvariantResult:
    tape_path = _full_pipeline_tape(bundle.hard_qids, tmp_dir)
    config = WorkbenchRunConfig(
        bundle_path=tmp_dir / "bundle.json",
        output_dir=tmp_dir / "out",
        llm_mode=LLM_MODE_TAPE,
        apply_mode="fake-record",
        tape_path=tape_path,
        iteration=1,
    )
    artifacts = run_workbench_iteration(bundle, config)
    return check_all_invariants(artifacts)


def _base_bundle(qid_filter: str | None) -> WorkbenchInputBundle:
    qids = (qid_filter,) if qid_filter else None
    base = from_production_replay(qids=qids)
    if not base.hard_cases:
        raise SystemExit(f"no hard cases matched filter={qid_filter!r}")
    canonical = base.hard_cases[0].qid
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
                "eval_row_id": f"workbench-fuzz-{canonical}",
            },
        ),
    )


# ─── Bundle construction per seed ───────────────────────────────────


_CHAIN_LENGTHS: tuple[int, ...] = (1, 2, 3)


def _bundle_for_seed(
    base: WorkbenchInputBundle, seed: int, *, mode: str,
) -> tuple[WorkbenchInputBundle, str]:
    """Build a bundle for ``seed`` and return ``(bundle, description)``.

    Description is a human-readable string the CLI prints on failure
    so the operator can reproduce the case from the seed alone.
    """
    if mode == "synthetic":
        return (
            synthesize_bundle(base=base, seed=seed),
            f"synthesize_bundle(base, seed={seed})",
        )
    if mode == "permute":
        rng_seed = seed
        names = list_permutation_names()
        # Pick a chain length and chain in a way that's deterministic
        # for the seed without needing a separate RNG.
        chain_len = _CHAIN_LENGTHS[seed % len(_CHAIN_LENGTHS)]
        # Use deterministic hash of seed to pick chain members.
        chain: list[str] = []
        for i in range(chain_len):
            chain.append(names[(seed + i) % len(names)])
        return (
            apply_permutation_chain(base, chain, seed=rng_seed),
            f"apply_permutation_chain(base, {chain!r}, seed={rng_seed})",
        )
    # mixed: alternate
    if seed % 2 == 0:
        return _bundle_for_seed(base, seed, mode="permute")
    return _bundle_for_seed(base, seed, mode="synthetic")


# ─── Main entry point ───────────────────────────────────────────────


def _format_violations(violations: Sequence[InvariantViolation]) -> str:
    return "\n".join(f"  - {v}" for v in violations)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m local_lever_workbench.fuzzer",
        description=(
            "State-machine invariant fuzzer — explores permutations "
            "and synthetic bundles for invariant violations."
        ),
    )
    parser.add_argument(
        "--iterations", "-n", type=int, default=50,
        help="number of seeds to explore (default: 50)",
    )
    parser.add_argument(
        "--seed", type=int, default=0,
        help="starting seed (default: 0). Per-iteration seed is seed+i.",
    )
    parser.add_argument(
        "--mode", choices=("permute", "synthetic", "mixed"),
        default="mixed",
        help="bundle source per iteration (default: mixed)",
    )
    parser.add_argument(
        "--qid", default="gs_009",
        help="production-replay QID suffix to base on (default: gs_009)",
    )
    parser.add_argument(
        "--replay", type=int, default=None,
        help="reproduce a specific seed and exit",
    )
    parser.add_argument(
        "--shrink", action="store_true",
        help="on first violation, run the shrinker and print minimal repro",
    )
    parser.add_argument(
        "--fail-fast", action="store_true",
        help="stop on first violation (default: report all)",
    )
    args = parser.parse_args(argv)

    base = _base_bundle(args.qid)

    if args.replay is not None:
        bundle, desc = _bundle_for_seed(base, args.replay, mode=args.mode)
        with tempfile.TemporaryDirectory() as td:
            result = _run_one(bundle, Path(td))
        if result.ok:
            print(f"[OK] {desc} — all invariants hold")
            return 0
        print(f"[FAIL] {desc}")
        print(_format_violations(result.violations))
        if args.shrink:
            _print_shrink(bundle, desc)
        return 1

    n_ok = 0
    n_fail = 0
    failures: list[tuple[int, str, InvariantResult, WorkbenchInputBundle]] = []
    for i in range(args.iterations):
        seed = args.seed + i
        bundle, desc = _bundle_for_seed(base, seed, mode=args.mode)
        with tempfile.TemporaryDirectory() as td:
            result = _run_one(bundle, Path(td))
        if result.ok:
            n_ok += 1
            continue
        n_fail += 1
        failures.append((seed, desc, result, bundle))
        print(f"[FAIL seed={seed}] {desc}")
        print(_format_violations(result.violations))
        if args.shrink:
            _print_shrink(bundle, desc)
        if args.fail_fast:
            break

    print()
    print(
        f"summary: {n_ok} pass, {n_fail} fail "
        f"({args.iterations} iterations from seed={args.seed}, mode={args.mode})"
    )
    return 0 if n_fail == 0 else 1


def _print_shrink(bundle: WorkbenchInputBundle, desc: str) -> None:
    """Shrink the failing bundle and print the minimal repro."""
    print(f"  shrinking {desc} …")

    def predicate(b: WorkbenchInputBundle) -> bool:
        with tempfile.TemporaryDirectory() as td:
            return not _run_one(b, Path(td)).ok

    result = shrink_bundle(bundle, triggers_violation=predicate)
    print(f"  {result.summary()}")
    print(f"  drops: {list(result.drops)}")
    minimal_qids = [c.qid for c in result.minimal.hard_cases]
    print(f"  minimal hard_qids: {minimal_qids}")
    print(
        f"  minimal post_apply_eval_tape size: "
        f"{len(result.minimal.post_apply_eval_tape)}"
    )


if __name__ == "__main__":
    sys.exit(main())
