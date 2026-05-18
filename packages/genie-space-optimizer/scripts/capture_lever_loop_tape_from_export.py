"""Phase 3.5 (2026-05-17) — capture a LeverLoopTape from an export JSON.

The exporter (``journey_fixture_exporter.py``) writes one
``llm_call_log`` entry per LLM call funnelled through
``optimizer._traced_llm_call``. This script reads the export and
emits a ``LeverLoopTape`` JSON ready to load via
``LeverLoopTape.from_json_file``.

Primary path (Phase 3.5):
    Walk ``iteration["llm_call_log"]`` and emit one ``TapeEntry``
    per recorded call, keyed by the recorded span_name / iteration /
    ag_id / cluster_id / prompt_sha256.

Legacy fallback (pre-Phase 3.5 exports):
    When ``llm_call_log`` is absent, log a WARNING and fall back to
    the old ``strategist_prompt`` / ``strategist_response`` path
    where available. When neither is present, emit an empty tape so
    the pipeline never blocks Phase 3 diagnostics on a borderline
    export.

Usage (CLI):
    python scripts/capture_lever_loop_tape_from_export.py \\
        --export <path/to/lever_loop_latest_export_*.json> \\
        --out    <path/to/output_tape.json>

Programmatic (Phase 3 backward-compat shim):
    from scripts.capture_lever_loop_tape_from_export import (
        capture_tape_from_export,
    )
    capture_tape_from_export(in_path, out_path, tape_id="my-tape")
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import sys
from pathlib import Path

from genie_space_optimizer.optimization.tape import prompt_sha256

logger = logging.getLogger("capture_lever_loop_tape_from_export")


def _build_entries_from_llm_call_log(iterations: list[dict]) -> list[dict]:
    """Primary path: one tape entry per llm_call_log entry."""
    entries: list[dict] = []
    for it in iterations:
        for call in (it.get("llm_call_log") or []):
            if not isinstance(call, dict):
                continue
            stage = str(call.get("span_name") or "").strip()
            if not stage:
                continue
            entries.append({
                "key": {
                    "stage": stage,
                    "iteration": int(
                        call.get("iteration")
                        if call.get("iteration") is not None
                        else (it.get("iteration") or 0)
                    ),
                    "ag_id": str(call.get("ag_id") or ""),
                    "cluster_id": str(call.get("cluster_id") or ""),
                    "prompt_sha256": str(call.get("prompt_sha256") or ""),
                },
                "prompt": str(call.get("prompt") or ""),
                "response_text": str(call.get("response_text") or ""),
                "response_metadata": dict(call.get("response_metadata") or {}),
            })
    return entries


def _build_entries_legacy_strategist(iterations: list[dict]) -> list[dict]:
    """Legacy fallback: pre-Phase 3.5 exports carry at most a
    ``strategist_prompt`` + ``strategist_response`` per iteration.
    Emit one tape entry per iteration where BOTH are present."""
    entries: list[dict] = []
    skipped = 0
    for it in iterations:
        iter_idx = int(
            it.get("iteration_idx")
            if it.get("iteration_idx") is not None
            else (it.get("iteration") or 0)
        )
        prompt = it.get("strategist_prompt")
        response = it.get("strategist_response")
        if not prompt or not response:
            skipped += 1
            continue
        # ``strategist_response`` may be a dict or a string; normalise.
        if isinstance(response, (dict, list)):
            response_text = json.dumps(response)
        else:
            response_text = str(response)
        entries.append({
            "key": {
                "stage": "adaptive_strategy",
                "iteration": iter_idx,
                "ag_id": "",
                "cluster_id": "",
                "prompt_sha256": prompt_sha256(str(prompt)),
            },
            "prompt": str(prompt),
            "response_text": response_text,
            "response_metadata": {"source": "lever_loop_export"},
        })
    if skipped:
        logger.warning(
            "Legacy fallback: skipped %d iteration(s) without both "
            "strategist_prompt and strategist_response.",
            skipped,
        )
    return entries


def _read_evals_and_clusters(
    iterations: list[dict],
) -> tuple[dict[int, list[dict]], dict[int, list[dict]]]:
    evals_by_iter: dict[int, list[dict]] = {}
    clusters_by_iter: dict[int, list[dict]] = {}
    for it in iterations:
        i = int(
            it.get("iteration")
            if it.get("iteration") is not None
            else (it.get("iteration_idx") or 0)
        )
        evals_by_iter[i] = list(it.get("eval_rows") or [])
        clusters_by_iter[i] = list(it.get("clusters") or [])
    return evals_by_iter, clusters_by_iter


def capture_tape_from_export(
    in_path: Path | str,
    out_path: Path | str,
    *,
    tape_id: str | None = None,
    miss_policy: str = "raise",
) -> None:
    """Read a lever_loop export and write a canonical tape JSON.

    Phase 3 backward-compat: this is the programmatic entry point.
    Callers may pass either a Path or a string for both args.
    """
    in_path = Path(in_path)
    out_path = Path(out_path)

    payload = json.loads(in_path.read_text(encoding="utf-8"))
    iterations = list(payload.get("iterations") or [])

    if any(isinstance(it, dict) and it.get("llm_call_log") for it in iterations):
        entries = _build_entries_from_llm_call_log(iterations)
        logger.info(
            "Phase 3.5 capture: extracted %d tape entries from "
            "llm_call_log across %d iterations.",
            len(entries), len(iterations),
        )
    else:
        logger.warning(
            "Legacy export detected (no llm_call_log) — falling back to "
            "strategist_prompt/strategist_response path. Re-capture "
            "against a fresh Phase-3.5 run for full Stage 1 / Stage 2 "
            "coverage."
        )
        entries = _build_entries_legacy_strategist(iterations)
        logger.warning(
            "Legacy fallback path used: emitted %d entries.",
            len(entries),
        )

    evals_by_iter, clusters_by_iter = _read_evals_and_clusters(iterations)

    tape_payload = {
        "tape_id": str(tape_id or payload.get("fixture_id") or out_path.stem),
        "source_run_id": str(payload.get("source_run_id") or ""),
        "captured_at": _dt.datetime.utcnow().isoformat() + "Z",
        "entries": entries,
        "evals_by_iteration": {str(k): v for k, v in evals_by_iter.items()},
        "clusters_by_iteration": {
            str(k): v for k, v in clusters_by_iter.items()
        },
        "rca_cards_by_cluster": {},
        "miss_policy": miss_policy,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(tape_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    logger.info("Wrote tape: %s (%d entries)", out_path, len(entries))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--export", required=True, help="Input export JSON path",
    )
    parser.add_argument(
        "--out", required=True, help="Output tape JSON path",
    )
    parser.add_argument(
        "--tape-id", default=None,
        help="Optional tape id (defaults to fixture_id or output stem)",
    )
    parser.add_argument(
        "--miss-policy", default="raise", choices=("raise", "warn"),
        help="Default miss policy embedded in the tape (replay-time)",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s: %(message)s",
    )

    capture_tape_from_export(
        args.export,
        args.out,
        tape_id=args.tape_id,
        miss_policy=args.miss_policy,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
