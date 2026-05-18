#!/usr/bin/env python3
"""Phase 3.6 (2026-05-17) — capture a LeverLoopTape from an MLflow trace.

Pulls every LLM call MLflow recorded for a given run via the
``mlflow.openai.autolog`` instrumentation. Combines them with a
complementary ``lever_loop_latest_export_*.json`` (for the
per-iteration eval/cluster side-tables) and writes a tape JSON that
``LeverLoopTape.from_json_file`` can consume.

Usage:
    python scripts/capture_tape_from_mlflow.py \\
        --experiment-id $MLFLOW_EXPERIMENT_ID \\
        --run-id        <mlflow_run_id_for_lever_loop_task> \\
        --export-json   tests/replay/active/fixtures/production_tapes/lever_loop_latest_export_run_<id>.json \\
        --out           tests/replay/active/fixtures/production_tapes/airline_run_<id>.json \\
        --miss-policy   prompt_sha_only

Auth:
    Uses ``mlflow.set_tracking_uri("databricks")``. Run from a shell
    with a working ``databricks auth login`` / ``.databrickscfg``
    profile.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import sys
from pathlib import Path
from typing import Iterable

logger = logging.getLogger("capture_tape_from_mlflow")


def _build_mlflow_client():
    import mlflow
    from mlflow.client import MlflowClient
    mlflow.set_tracking_uri("databricks")
    return MlflowClient()


def _read_export_side_tables(
    export_path: Path,
) -> tuple[dict, dict]:
    evals_by_iter: dict[str, list] = {}
    clusters_by_iter: dict[str, list] = {}
    if not export_path or not export_path.exists():
        return evals_by_iter, clusters_by_iter
    payload = json.loads(export_path.read_text(encoding="utf-8"))
    for it in (payload.get("iterations") or []):
        # Phase 3.6.1 (2026-05-18) — the production export uses
        # 1-indexed iteration counters (human-readable, matches the
        # postmortem narrative). The in-memory replay harness queries
        # ``evals_by_iteration`` with ``_iter_num - 1`` per
        # ``harness.py:18933`` — i.e. 0-indexed. Store 0-indexed in
        # the tape so the on-disk side-tables match in-memory
        # semantics, not the display format. ``LeverLoopTape.from_json_file``
        # asserts this invariant at load time.
        raw = int(it.get("iteration") or 1)
        i = str(max(0, raw - 1))
        evals_by_iter[i] = list(it.get("eval_rows") or [])
        clusters_by_iter[i] = list(it.get("clusters") or [])
    return evals_by_iter, clusters_by_iter


def _calls_to_entries(calls: Iterable[dict]) -> list[dict]:
    """Convert extractor output dicts to the on-disk tape `entries` shape."""
    out: list[dict] = []
    for c in calls:
        out.append({
            "key": {
                "stage": c["span_name"],
                "iteration": int(c.get("iteration", -1)),
                "ag_id": str(c.get("ag_id", "")),
                "cluster_id": str(c.get("cluster_id", "")),
                "prompt_sha256": c["prompt_sha256"],
            },
            "prompt": c["prompt"],
            "response_text": c["response_text"],
            "response_metadata": c.get("response_metadata", {}),
        })
    return out


def _resolve_run_ids(
    client,
    experiment_id: str,
    *,
    run_ids: list[str],
    filter_string: str | None,
) -> list[str]:
    """Resolve the set of MLflow run ids to capture from.

    Phase 3.6 (2026-05-18) — runs that belong to a single optimization
    run live under multiple sibling MLflow runs (one per iteration
    "strategy" stage, plus the "enrichment_snapshot" stage, plus
    others). A single ``--run-id`` therefore captures only a slice of
    the lever loop's LLM activity.

    Two ways to widen the capture:
      - ``--run-id ID --run-id ID2 ...`` (repeatable, explicit set)
      - ``--filter-string 'tags."key" = "value"'`` (an MLflow
        ``search_runs`` filter expression; commonly used with
        ``genie.optimization_run_id`` to fetch every run for a
        single optimization)

    Both are accepted simultaneously; the resolved set is the union.
    """
    resolved: list[str] = list(run_ids or [])
    if filter_string:
        results = client.search_runs(
            experiment_ids=[experiment_id],
            filter_string=filter_string,
            max_results=10000,
        )
        for r in results:
            resolved.append(r.info.run_id)
    if not resolved:
        raise SystemExit(
            "No MLflow runs resolved. Provide --run-id (repeatable) "
            "or --filter-string."
        )
    # De-duplicate while preserving order.
    seen: set[str] = set()
    deduped: list[str] = []
    for rid in resolved:
        if rid in seen:
            continue
        seen.add(rid)
        deduped.append(rid)
    return deduped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-id", required=True)
    parser.add_argument(
        "--run-id", action="append", default=[],
        help=(
            "MLflow run id for the lever-loop task. Repeatable to "
            "capture from multiple sibling runs (e.g. enrichment + "
            "per-iteration strategy runs)."
        ),
    )
    parser.add_argument(
        "--filter-string", default=None,
        help=(
            "MLflow ``search_runs`` filter expression. Typical use: "
            "``tags.\"genie.optimization_run_id\" = \"<run-uuid>\"`` "
            "to capture every run belonging to one optimization."
        ),
    )
    parser.add_argument(
        "--export-json", required=True,
        help="Path to the run's lever_loop_latest_export_*.json",
    )
    parser.add_argument(
        "--out", required=True, help="Output tape JSON path",
    )
    parser.add_argument(
        "--miss-policy", default="raise",
        choices=("raise", "warn", "prompt_sha_only"),
        help=(
            "Tape miss policy. Use prompt_sha_only for historic runs "
            "captured without iteration/ag breadcrumbs."
        ),
    )
    parser.add_argument(
        "--tape-id", default=None,
        help="Override the tape id (default: derived from --out stem)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s: %(message)s",
    )

    export_path = Path(args.export_json)
    out_path = Path(args.out)

    # 1. Resolve the set of MLflow run ids to capture from. A single
    #    optimization run lives under many sibling MLflow runs (one
    #    enrichment_snapshot + one per iteration's strategy stage,
    #    plus per-attempt repeats). The ``_resolve_run_ids`` helper
    #    combines explicit ``--run-id`` flags and the optional
    #    ``--filter-string`` MLflow search expression.
    client = _build_mlflow_client()
    run_ids = _resolve_run_ids(
        client,
        args.experiment_id,
        run_ids=args.run_id,
        filter_string=args.filter_string,
    )
    logger.info(
        "Phase 3.6 capture: resolved %d MLflow run id(s)",
        len(run_ids),
    )

    # Iterate and concatenate traces in run-id order.
    traces: list = []
    for rid in run_ids:
        rid_traces = client.search_traces(
            experiment_ids=[args.experiment_id],
            run_id=rid,
            max_results=10000,
        )
        logger.info(
            "Phase 3.6 capture: %d trace(s) for run %s",
            len(rid_traces), rid,
        )
        traces.extend(rid_traces)
    logger.info(
        "Phase 3.6 capture: total %d trace(s) across %d run(s)",
        len(traces), len(run_ids),
    )

    # 2. Extract LLM calls.
    from genie_space_optimizer.optimization.mlflow_trace_extractor import (
        extract_llm_calls_from_traces,
    )
    calls = list(extract_llm_calls_from_traces(traces))
    logger.info(
        "Phase 3.6 capture: extracted %d LLM call(s) from traces.",
        len(calls),
    )

    # 3. Read complementary export side-tables.
    evals_by_iter, clusters_by_iter = _read_export_side_tables(export_path)
    logger.info(
        "Phase 3.6 capture: complementary export contributed %d "
        "iteration(s) of eval/cluster state.",
        len(evals_by_iter),
    )

    # 4. Assemble + write tape.
    tape_payload = {
        "tape_id": str(args.tape_id or out_path.stem),
        "source_run_id": str(args.run_id),
        "captured_at": _dt.datetime.utcnow().isoformat() + "Z",
        "entries": _calls_to_entries(calls),
        "evals_by_iteration": evals_by_iter,
        "clusters_by_iteration": clusters_by_iter,
        "rca_cards_by_cluster": {},
        "miss_policy": args.miss_policy,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(tape_payload, indent=2), encoding="utf-8",
    )
    logger.info(
        "Wrote tape: %s (%d entries, miss_policy=%s)",
        out_path, len(tape_payload["entries"]), args.miss_policy,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
