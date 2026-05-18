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


def _read_export_payload(export_path: Path) -> dict:
    """Phase 3.7 — return the parsed export JSON (or empty dict).

    Used both for side-table assembly AND for the Phase 3.7
    lever6 binding reconciliation in extract_llm_calls_from_traces.
    """
    if not export_path or not export_path.exists():
        return {}
    return json.loads(export_path.read_text(encoding="utf-8"))


def _backfill_source_cluster_ids_in_place(payload: dict) -> dict:
    """Phase 3.7 §2.3 (1B) — populate source_cluster_ids on every AG
    in every iteration's strategist_response.

    Priority chain (matches the production-time semantics empirically
    observed in the anchor exports — see
    ``docs/architecture/stage-prompt-fidelity-audit.md``):

      1. keep existing non-empty source_cluster_ids
      2. single AG in the iter → use the iter pool
         (iter_source_clusters_by_id keys ∪ clusters[*].cluster_id).
         Rationale: in the anchor exports the lone AG processed every
         cluster, but its ``patches[]`` only carry the clusters for
         which lever6 actually emitted a proposal — patches under-
         represent source_cluster_ids.
      3. multiple AGs → use that AG's ``patches[*].cluster_id``
         (each AG carries its own patch set, so patches identify the
         AG-to-cluster partition).
      4. multiple AGs with empty patches → iter pool fallback.

    The capture script applies this before writing iteration_payloads
    into the tape, so the replay harness reads a populated
    source_cluster_ids and lever6's eligible_clusters resolves to the
    real cluster dicts (not the synthetic ``{cluster_id: ag_id}``
    fallback in optimizer.py:17142).
    """
    if not isinstance(payload, dict):
        return payload
    for it in (payload.get("iterations") or []):
        if not isinstance(it, dict):
            continue
        pool: set[str] = set()
        src = it.get("iter_source_clusters_by_id") or {}
        if isinstance(src, dict):
            pool.update(str(k) for k in src.keys())
        for c in (it.get("clusters") or []):
            if isinstance(c, dict) and c.get("cluster_id"):
                pool.add(str(c["cluster_id"]))
        ag_groups = (
            (it.get("strategist_response") or {}).get("action_groups") or []
        )
        single_ag_in_iter = len(ag_groups) == 1
        for ag in ag_groups:
            if not isinstance(ag, dict):
                continue
            scids = ag.get("source_cluster_ids") or []
            scids_str = [str(s) for s in scids if str(s).strip()]
            if scids_str:
                ag["source_cluster_ids"] = scids_str
                continue
            if single_ag_in_iter and pool:
                ag["source_cluster_ids"] = sorted(pool)
                continue
            patches = ag.get("patches") or []
            patch_cids = sorted({
                str(p.get("cluster_id") or "").strip()
                for p in patches
                if isinstance(p, dict) and p.get("cluster_id")
            })
            patch_cids = [c for c in patch_cids if c]
            if patch_cids:
                ag["source_cluster_ids"] = patch_cids
            elif pool:
                ag["source_cluster_ids"] = sorted(pool)
    return payload


def _read_iteration_tag(client, run_id: str) -> int | None:
    """Phase 3.7 §2.3 (1A) — return the run's ``genie.iteration`` as a
    0-indexed int, or None if the tag is missing.

    Production lever-loop sibling runs are tagged with
    ``genie.iteration: "01" | "02" | ...``; we shift by -1 to match
    the live ``_RECORDER_BINDING`` semantics.
    """
    try:
        run = client.get_run(run_id)
    except Exception:
        return None
    tags = (run.data.tags if hasattr(run.data, "tags") else {}) or {}
    raw = tags.get("genie.iteration")
    if raw is None:
        return None
    try:
        return max(0, int(str(raw)) - 1)
    except (TypeError, ValueError):
        return None


def _read_export_side_tables(
    payload: dict,
) -> tuple[dict, dict, dict]:
    evals_by_iter: dict[str, list] = {}
    clusters_by_iter: dict[str, list] = {}
    iter_payloads: dict[str, dict] = {}
    if not payload:
        return evals_by_iter, clusters_by_iter, iter_payloads
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
        # Phase 3.6.2 E1 (2026-05-18) — full per-iteration row dict
        # for ``state.load_*`` replay stubs. The replay_fixture export
        # carries every field ``genie_opt_iterations`` does:
        # rows_json (= eval_rows), clusters, soft_clusters,
        # decision_records, strategist_response, ag_outcomes,
        # iter_rca_id_by_cluster, iter_source_clusters_by_id,
        # journey_validation, lever5_gate_drops,
        # metadata_failure_clusters, post_eval_passing_qids,
        # iteration. The harness's load_latest_full_iteration consumes
        # these via ``baseline_iter.get("rows_json")`` etc.
        payload_dict: dict = {
            "iteration": raw,            # preserve human 1-indexed in payload
            "rows_json": list(it.get("eval_rows") or []),
            "eval_scope": "full",
            "rolled_back": False,
        }
        # Carry every other top-level key the export provides.
        for k in (
            "clusters", "soft_clusters", "decision_records",
            "strategist_response", "ag_outcomes",
            "iter_rca_id_by_cluster", "iter_source_clusters_by_id",
            "journey_validation", "lever5_gate_drops",
            "metadata_failure_clusters", "post_eval_passing_qids",
            "scores_json", "soft_signal_qids", "mlflow_run_id",
            "evaluated_count",
        ):
            if k in it and it[k] is not None:
                payload_dict[k] = it[k]
        iter_payloads[i] = payload_dict
    return evals_by_iter, clusters_by_iter, iter_payloads


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
    parser.add_argument(
        "--replay-mode", action="append", default=[],
        metavar="STAGE=MODE",
        help=(
            "Phase 3.7 — set per-stage replay mode in the tape's "
            "replay_mode_by_stage dict. Repeatable. Format: STAGE=MODE. "
            "Currently supported modes: rebuild_and_match (default), "
            "historic_inject (lever6_llm only today — see "
            "docs/architecture/stage-prompt-fidelity-audit.md)."
        ),
    )
    args = parser.parse_args(argv)

    replay_mode_by_stage: dict[str, str] = {}
    from genie_space_optimizer.optimization.tape import _VALID_REPLAY_MODES
    for spec in args.replay_mode:
        if "=" not in spec:
            parser.error(
                f"--replay-mode must be STAGE=MODE, got {spec!r}"
            )
        stage, mode = spec.split("=", 1)
        stage = stage.strip()
        mode = mode.strip()
        if mode not in _VALID_REPLAY_MODES:
            parser.error(
                f"--replay-mode {spec!r}: unsupported mode {mode!r} "
                f"(valid: {sorted(_VALID_REPLAY_MODES)})"
            )
        replay_mode_by_stage[stage] = mode

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

    # 2. Read + backfill the complementary export payload BEFORE we
    # use it for either extraction (1A iteration backfill) or
    # iteration_payloads write-back (so the replay harness sees AGs
    # with source_cluster_ids populated; 1B).
    export_payload = _read_export_payload(export_path)
    _backfill_source_cluster_ids_in_place(export_payload)

    # 3. Iterate per-run, threading each run's iteration tag into the
    # extractor (1A) so lever6_llm calls in that run are bound to the
    # correct (iteration, ag) tuple. Non-tagged runs (e.g.
    # enrichment_snapshot) get None and fall back to the
    # iteration_scan path.
    from genie_space_optimizer.optimization.mlflow_trace_extractor import (
        extract_llm_calls_from_trace,
    )
    calls: list[dict] = []
    total_traces = 0
    for rid in run_ids:
        rid_traces = client.search_traces(
            experiment_ids=[args.experiment_id],
            run_id=rid,
            max_results=10000,
        )
        total_traces += len(rid_traces)
        logger.info(
            "Phase 3.6 capture: %d trace(s) for run %s",
            len(rid_traces), rid,
        )
        iter_override = _read_iteration_tag(client, rid)
        for trace in rid_traces:
            calls.extend(extract_llm_calls_from_trace(
                trace,
                export_payload=export_payload,
                iteration_override=iter_override,
            ))
    logger.info(
        "Phase 3.6 capture: total %d trace(s) across %d run(s)",
        total_traces, len(run_ids),
    )
    logger.info(
        "Phase 3.6 capture: extracted %d LLM call(s) from traces.",
        len(calls),
    )

    # 4. Build side-tables from the export payload.
    evals_by_iter, clusters_by_iter, iter_payloads = (
        _read_export_side_tables(export_payload)
    )
    logger.info(
        "Phase 3.6 capture: complementary export contributed %d "
        "iteration(s) of eval/cluster state.",
        len(evals_by_iter),
    )

    # 5. Assemble + write tape.
    from genie_space_optimizer.optimization.tape import TAPE_FORMAT_VERSION

    tape_payload = {
        "tape_id": str(args.tape_id or out_path.stem),
        "source_run_id": str(args.run_id),
        "captured_at": _dt.datetime.utcnow().isoformat() + "Z",
        "format_version": TAPE_FORMAT_VERSION,
        "entries": _calls_to_entries(calls),
        "evals_by_iteration": evals_by_iter,
        "clusters_by_iteration": clusters_by_iter,
        "iteration_payloads": iter_payloads,
        "rca_cards_by_cluster": {},
        "miss_policy": args.miss_policy,
        "replay_mode_by_stage": replay_mode_by_stage,
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
