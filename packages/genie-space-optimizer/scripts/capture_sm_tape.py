"""PR-B — Capture a SM tape from a postmortem evidence bundle.

Reads the ``GSO_PLAN11_STAGE1_DIAGNOSIS_V1`` markers (and any other
``LLM_REASONING_CALL_*`` spans) from a postmortem evidence bundle and
writes a JSONL tape that the ``tests/integration/sm_tape_replay.py``
harness consumes.

Usage::

    uv run python packages/genie-space-optimizer/scripts/capture_sm_tape.py \\
        --evidence-dir docs/runid_analysis/<run_id>/evidence/ \\
        --out packages/genie-space-optimizer/tests/fixtures/sm_tapes/<tape_id>.jsonl \\
        --error-message-from packages/genie-space-optimizer/tests/fixtures/sm_tapes/<tape_id>.body.txt

Why this script lives in scripts/ rather than as part of the harness
itself:
  * Tape capture happens once per debug cycle; it is a developer
    workflow, not a runtime concern.
  * The script is intentionally small (~120 lines) and reads only
    publicly emitted markers — no MLflow API calls, no Databricks
    auth — so it runs anywhere a postmortem evidence bundle does.

PR-A note: until the 2026-05-23 trial reruns with the new diagnostic
instrumentation, postmortem markers carry exception class + duration
but NOT the actual BadRequest body. Operators bridge that gap by
supplying ``--error-message-from`` pointing at a text file with the
body (paste it from MLflow trace inspection or the model serving
endpoint logs). Once PR-A is shipped this gap closes — the dump in
``{run_root}/llm_errors/stage1_<iter>_<qid>.json`` is consumed
directly.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

_MARKER_LINE_RE = re.compile(
    r"GSO_PLAN11_STAGE1_DIAGNOSIS_V1\s+(\{.+\})"
)


def _read_marker_lines(evidence_dir: Path) -> list[dict]:
    """Return one parsed marker payload per ``GSO_PLAN11_STAGE1_DIAGNOSIS_V1``
    line in any ``*.txt`` or ``*.json`` file under ``evidence_dir``."""
    out: list[dict] = []
    for path in evidence_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix not in (".txt", ".json"):
            continue
        try:
            text = path.read_text(errors="ignore")
        except Exception:
            continue
        for match in _MARKER_LINE_RE.finditer(text):
            try:
                out.append(json.loads(match.group(1)))
            except json.JSONDecodeError:
                continue
    return out


def _llm_errors_dumps(evidence_dir: Path) -> dict[tuple[int, str], str]:
    """Pick up PR-A on-disk error dumps if present in the evidence bundle.

    Returns a map ``(iteration, qid) -> error_message_body``.
    """
    out: dict[tuple[int, str], str] = {}
    for path in evidence_dir.rglob("stage1_*_*.json"):
        try:
            payload = json.loads(path.read_text())
        except Exception:
            continue
        try:
            key = (int(payload["iteration"]), str(payload["qid"]))
            out[key] = str(payload.get("error_message") or "")
        except (KeyError, ValueError):
            continue
    return out


def _markers_to_tape_entries(
    markers: list[dict],
    fallback_message: str,
    error_dumps: dict[tuple[int, str], str],
) -> list[dict]:
    """One tape entry per UNIQUE ``(iteration, call_id_kindof)`` —
    diagnose_failing_qids batches all failing QIDs into one LLM call,
    so multiple markers per iteration map to a single tape entry.
    """
    seen_calls: dict[tuple[int, str], dict] = {}
    for m in markers:
        if m.get("outcome") != "llm_error":
            continue
        iteration = int(m.get("iteration", 0))
        # Use first qid we see in this iteration as a stable join key —
        # all markers from one batched call share an iteration.
        call_key = (iteration, "plan11_diagnose")
        if call_key in seen_calls:
            continue
        qid = str(m.get("qid", ""))
        message = error_dumps.get((iteration, qid)) or fallback_message
        seen_calls[call_key] = {
            "kind": "exception",
            "skill_id": "plan11_diagnose",
            "call_id": f"plan11_stage1_diagnose.iter_{iteration}",
            "iteration": iteration,
            "duration_ms": int(m.get("duration_ms", 0)),
            "exception_class": str(m.get("exception_class", "BadRequestError")),
            "exception_message": message,
        }
    return sorted(seen_calls.values(), key=lambda e: e["iteration"])


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--evidence-dir", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    p.add_argument(
        "--error-message-from",
        type=Path,
        help=(
            "Optional text file with the actual BadRequest body. Used "
            "as fallback when the evidence bundle predates PR-A and "
            "has no llm_errors/ dumps. Required when no dumps are "
            "found; otherwise ignored."
        ),
    )
    args = p.parse_args(argv)

    if not args.evidence_dir.is_dir():
        print(
            f"evidence dir not found: {args.evidence_dir}", file=sys.stderr,
        )
        return 2

    markers = _read_marker_lines(args.evidence_dir)
    if not markers:
        print(
            "no GSO_PLAN11_STAGE1_DIAGNOSIS_V1 markers found in "
            f"{args.evidence_dir}",
            file=sys.stderr,
        )
        return 2

    dumps = _llm_errors_dumps(args.evidence_dir)
    fallback = ""
    if args.error_message_from is not None:
        fallback = args.error_message_from.read_text().strip()
    if not fallback and not dumps:
        print(
            "no llm_errors/ dumps found and no --error-message-from "
            "provided; tape would carry empty error bodies. Pass "
            "--error-message-from with the actual 400 body text.",
            file=sys.stderr,
        )
        return 2

    entries = _markers_to_tape_entries(markers, fallback, dumps)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w") as fh:
        for e in entries:
            fh.write(json.dumps(e) + "\n")
    print(
        f"wrote {len(entries)} tape entries to {args.out} "
        f"(from {len(markers)} markers, "
        f"{len(dumps)} on-disk error dumps)",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
