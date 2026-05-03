"""Lazy MLflow trace fetcher for evidence bundles.

Invoked **only** when the analysis skill cannot localize root cause
from the bundled stdout + decision-trail artifacts. Pulls a finite
list of trace ids (either explicit ``--trace-id`` flags or those
recorded in ``manifest.trace_fetch_recommendations``) into
``evidence/traces/<trace_id>.json`` and updates the manifest.

Usage:
    python -m genie_space_optimizer.tools.trace_fetcher \
        --bundle-dir packages/genie-space-optimizer/docs/runid_analysis/<opt_run_id> \
        --trace-id tr-abc [--trace-id tr-def] [--from-recommendations]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Sequence

from mlflow.tracking import MlflowClient

from genie_space_optimizer.tools.evidence_layout import (
    bundle_paths_for,
    manifest_from_dict,
    manifest_to_dict,
)

logger = logging.getLogger(__name__)


def fetch_traces(
    *,
    bundle_root: Path,
    trace_ids: Sequence[str] | None = None,
    from_recommendations: bool = False,
) -> dict:
    if not (bundle_root / "evidence" / "manifest.json").exists():
        raise FileNotFoundError(
            f"manifest.json not found under {bundle_root}/evidence/. "
            "Run evidence_bundle first."
        )

    manifest = manifest_from_dict(
        json.loads((bundle_root / "evidence" / "manifest.json").read_text())
    )
    paths = bundle_paths_for(
        root=bundle_root.parent,
        optimization_run_id=manifest.resolved["optimization_run_id"],
    )

    selected: list[str] = list(trace_ids or [])
    if from_recommendations:
        for rec in manifest.trace_fetch_recommendations:
            selected.extend(rec.trace_ids)

    seen: set[str] = set()
    deduped: list[str] = []
    for tid in selected:
        if tid not in seen:
            seen.add(tid)
            deduped.append(tid)

    paths.traces_dir.mkdir(parents=True, exist_ok=True)
    client = MlflowClient()
    fetched: list[str] = []
    failed: list[dict] = []

    for trace_id in deduped:
        target = paths.traces_dir / f"{trace_id}.json"
        try:
            trace = client.get_trace(trace_id)
            payload = trace.to_dict() if hasattr(trace, "to_dict") else trace
            target.write_text(json.dumps(payload, indent=2, sort_keys=True))
            fetched.append(trace_id)
        except Exception as exc:  # noqa: BLE001 — surface to caller.
            failed.append({"trace_id": trace_id, "error": f"{type(exc).__name__}: {exc}"})

    existing_traces = list(manifest.artifacts_pulled.get("traces", ()))
    new_paths = [f"evidence/traces/{tid}.json" for tid in fetched]
    artifacts = dict(manifest.artifacts_pulled)
    artifacts["traces"] = tuple(sorted({*existing_traces, *new_paths}))
    updated = manifest_to_dict(
        manifest_from_dict(
            {**manifest_to_dict(manifest), "artifacts_pulled": artifacts}
        )
    )
    paths.manifest.write_text(json.dumps(updated, indent=2, sort_keys=True))

    return {"fetched": fetched, "failed": failed}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="evidence-bundle-trace-fetcher")
    parser.add_argument("--bundle-dir", required=True, type=Path)
    parser.add_argument("--trace-id", action="append", default=[])
    parser.add_argument("--from-recommendations", action="store_true")
    args = parser.parse_args(argv)

    if not args.trace_id and not args.from_recommendations:
        parser.error("supply at least one --trace-id or --from-recommendations")

    try:
        result = fetch_traces(
            bundle_root=args.bundle_dir,
            trace_ids=args.trace_id,
            from_recommendations=args.from_recommendations,
        )
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if not result["failed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
