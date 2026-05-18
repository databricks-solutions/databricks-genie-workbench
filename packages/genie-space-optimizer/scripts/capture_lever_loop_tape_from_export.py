"""Capture a LeverLoopTape from a production ``lever_loop_latest_export`` JSON.

Usage (CLI):
    python scripts/capture_lever_loop_tape_from_export.py \\
        --input docs/runid_analysis/<runid>/lever_loop_latest_export.json \\
        --output tests/replay/active/tapes/<runid>.json \\
        --tape-id <runid>

Programmatic use (also from tests):
    from scripts.capture_lever_loop_tape_from_export import (
        capture_tape_from_export,
    )
    capture_tape_from_export(in_path, out_path, tape_id="...")

What it captures:
    - One ``adaptive_strategy`` tape entry per iteration, keyed by
      ``(stage="adaptive_strategy", iteration, ag_id="",
      cluster_id="", prompt_sha256(strategist_prompt))``.
    - One ``cluster_driven_synthesis`` entry per recorded synthesis
      call when the iteration includes a ``synthesis_calls`` list.
    - Per-iteration ``eval_rows`` and ``clusters`` for the
      ``evals_by_iteration`` / ``clusters_by_iteration`` side-channels.
    - Per-cluster ``rca_card`` when present on cluster dicts.

What it does NOT capture:
    - MLflow trace spans. The export-only path is sufficient for the
      four 2026-05-17 anchor shapes (gs_009, gs_024, gs_013, gs_026)
      because those iterations failed at the adaptive-strategist +
      structural-dispatch boundary and the synthesis LLM was never
      reached.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
from pathlib import Path

from genie_space_optimizer.optimization.tape import prompt_sha256


def capture_tape_from_export(
    in_path: Path | str,
    out_path: Path | str,
    *,
    tape_id: str,
) -> None:
    """Read a lever_loop export and write a canonical tape JSON to ``out_path``."""
    in_path = Path(in_path)
    out_path = Path(out_path)

    with in_path.open("r", encoding="utf-8") as fh:
        export = json.load(fh)

    iterations = export.get("iterations") or []
    if not iterations:
        raise ValueError(f"{in_path}: export has no iterations to capture.")

    entries: list[dict] = []
    evals_by_iteration: dict[int, list[dict]] = {}
    clusters_by_iteration: dict[int, list[dict]] = {}
    rca_cards_by_cluster: dict[str, dict] = {}

    for it in iterations:
        iter_idx = int(it.get("iteration_idx") or 0)

        strategist_prompt = it.get("strategist_prompt")
        strategist_response = it.get("strategist_response")
        if not strategist_prompt or not strategist_response:
            raise ValueError(
                f"{in_path}: iteration {iter_idx} is missing "
                "strategist_prompt or strategist_response."
            )

        entries.append({
            "key": {
                "stage": "adaptive_strategy",
                "iteration": iter_idx,
                "ag_id": "",
                "cluster_id": "",
                "prompt_sha256": prompt_sha256(str(strategist_prompt)),
            },
            "prompt": str(strategist_prompt),
            "response_text": str(strategist_response),
            "response_metadata": {"source": "lever_loop_export"},
        })

        for call in (it.get("synthesis_calls") or []):
            p = str(call.get("prompt") or "")
            r = str(call.get("response") or "")
            if not p or not r:
                continue
            entries.append({
                "key": {
                    "stage": "cluster_driven_synthesis",
                    "iteration": iter_idx,
                    "ag_id": str(call.get("ag_id") or ""),
                    "cluster_id": str(call.get("cluster_id") or ""),
                    "prompt_sha256": prompt_sha256(p),
                },
                "prompt": p,
                "response_text": r,
                "response_metadata": {"source": "lever_loop_export"},
            })

        evals_by_iteration[iter_idx] = list(it.get("eval_rows") or [])
        clusters_by_iteration[iter_idx] = list(it.get("clusters") or [])

        for cluster in clusters_by_iteration[iter_idx]:
            cid = str(cluster.get("cluster_id") or "")
            card = cluster.get("rca_card")
            if cid and isinstance(card, dict):
                rca_cards_by_cluster[cid] = dict(card)

    payload = {
        "tape_id": tape_id,
        "source_run_id": str(export.get("source_run_id") or ""),
        "captured_at": _dt.datetime.utcnow().isoformat() + "Z",
        "entries": entries,
        "evals_by_iteration": {
            str(k): v for k, v in evals_by_iteration.items()
        },
        "clusters_by_iteration": {
            str(k): v for k, v in clusters_by_iteration.items()
        },
        "rca_cards_by_cluster": rca_cards_by_cluster,
        "miss_policy": "raise",
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)


def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture a LeverLoopTape from a lever_loop export JSON.",
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--tape-id", required=True)
    args = parser.parse_args()

    capture_tape_from_export(args.input, args.output, tape_id=args.tape_id)
    print(f"Wrote tape to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
