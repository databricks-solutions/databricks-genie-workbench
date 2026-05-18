#!/usr/bin/env python3
"""WU-A Task 5 — anchor-chain invariant verifier CLI.

Walks the artifacts a real GSO optimizer run already emits
(``postmortem.json`` + ``evidence/lever_loop_latest_export_*_text.txt``)
and prints a human-readable PASS/FAIL verdict. Exits 0 on PASS,
1 on FAIL, 2 on I/O error.

Usage:
    python scripts/verify_anchor_chain_invariants.py \\
        --runid-dir docs/runid_analysis/<runid>/

    python scripts/verify_anchor_chain_invariants.py \\
        --postmortem-json <path> [--transcript <path>]

This is the deferred WU-4 from
``2026-05-18-early-rca-preflight-and-slate-enforcement.md`` rescoped
against the real artifact shapes.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="verify_anchor_chain_invariants",
        description=(
            "Verify the GSO optimizer's anchor-chain lifecycle "
            "invariants against a real run's postmortem + transcript."
        ),
    )
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--runid-dir",
        type=Path,
        help=(
            "Path to a docs/runid_analysis/<runid>/ directory "
            "containing postmortem.json (+ optional evidence/)."
        ),
    )
    src.add_argument(
        "--postmortem-json",
        type=Path,
        help="Direct path to a postmortem.json file.",
    )
    p.add_argument(
        "--transcript",
        type=Path,
        default=None,
        help=(
            "Optional path to a lever-loop transcript text file. "
            "Required for the GSO_BEST_OF_N_RANKED_V1 fire-count "
            "and gate-admission-with-empty-intent global checks."
        ),
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-anchor reasons even on PASS.",
    )
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    from genie_space_optimizer.verification import (
        AnchorChainVerifier,
        verify_runid_dir,
    )

    if args.runid_dir is not None:
        try:
            result = verify_runid_dir(args.runid_dir)
        except FileNotFoundError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 2
    else:
        try:
            postmortem = json.loads(args.postmortem_json.read_text())
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"ERROR: cannot read postmortem.json: {e}", file=sys.stderr)
            return 2
        transcript_text = ""
        if args.transcript is not None:
            try:
                transcript_text = args.transcript.read_text(errors="replace")
            except FileNotFoundError as e:
                print(f"ERROR: cannot read transcript: {e}", file=sys.stderr)
                return 2
        result = AnchorChainVerifier(
            postmortem=postmortem,
            transcript_text=transcript_text,
        ).run()

    verdict = "PASS" if result.passed else "FAIL"
    print(f"Anchor-chain verifier verdict: {verdict}")
    print(
        f"  GSO_BEST_OF_N_RANKED_V1 structural fires: "
        f"{result.best_of_n_structural_fire_count}"
    )
    if result.anchor_verdicts:
        print("Per-anchor verdicts:")
        print(result.per_anchor_summary())
    else:
        print("Per-anchor verdicts: (no anchors targeted in this run)")
    if result.global_failures:
        print("Global-invariant failures:")
        for f in result.global_failures:
            print(f"  - {f}")
    elif args.verbose:
        print("Global-invariant failures: none")

    return 0 if result.passed else 1


if __name__ == "__main__":
    sys.exit(main())
