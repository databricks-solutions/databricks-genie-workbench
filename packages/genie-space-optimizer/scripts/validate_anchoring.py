#!/usr/bin/env python
"""Plan 4 — anchoring validation. Replays a captured cluster set
through the L1 stage-2 adapter with N ∈ {1, 3, 5} raw-evidence
samples and writes a Markdown report comparing patch shapes per
cluster.

Inputs:
  --clusters-path PATH        JSON file: list[dict] of pre-AFS clusters.
  --metadata-path PATH        JSON file: metadata snapshot (one Genie space).
  --output PATH               Markdown report destination.
  --skill-id ID               Defaults to lever-1-table-column-description.

Output: a Markdown table with one row per cluster × N value.
Each cell summarizes the proposal's `target_column`, `proposed_value`
length, and the unique structural-archetype derived from the
proposal's `sections.<key>` set. Helps a human eyeball whether
N=1 produces narrowly-fit patches (one column changes per run) and
N=3 produces broader patches (multiple columns or more general
synonyms per run).

The script is for one-off review during the Plan 4 trial — it is NOT
imported by the test suite. Anchoring decisions are recorded in the
trial-run protocol section of the Plan 4 document.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _summarize_proposal(proposal: dict) -> str:
    """One-line compact view of a proposal for the report table."""
    if not isinstance(proposal, dict):
        return "(non-dict)"
    target = proposal.get("_target") or proposal.get("target") or ""
    pv = proposal.get("proposed_value") or proposal.get("proposed_text") or ""
    sections = list((proposal.get("sections") or {}).keys())
    return (
        f"target=`{target}` "
        f"len={len(str(pv))} "
        f"sections={sorted(sections)}"
    )


def _run_one(cluster: dict, metadata_snapshot: dict, skill_id: str,
              n: int, w: Any) -> list[dict]:
    """Build the bundle, run the L1 adapter, return its proposals.

    The historical raw-evidence rollout flag was retired by the
    2026-05-16 dead-flag cleanup, so we only set the sample-size env
    var here."""
    import os as _os
    _os.environ["GSO_RAW_EVIDENCE_N"] = str(n)
    import importlib
    from genie_space_optimizer.common import config as cfg
    importlib.reload(cfg)

    from genie_space_optimizer.optimization.activation_bundle import (
        build_activation_bundle,
    )
    from genie_space_optimizer.optimization.three_stage_pipeline import (
        _stage_2_for_skill,
    )

    pick = {"skill_id": skill_id, "target_objects": [],
            "evidence_refs": [], "expected_impact_qids": [],
            "why": "anchoring validation", "priority": 1}
    bundle = build_activation_bundle(
        pick=pick, ag_id="ANCHOR_VALIDATION",
        clusters=[cluster], metadata_snapshot=metadata_snapshot, w=w,
    )
    result = _stage_2_for_skill(bundle, w=w)
    return result.get("proposals") or []


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--clusters-path", required=True, type=Path)
    p.add_argument("--metadata-path", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--skill-id", default="lever-1-table-column-description")
    args = p.parse_args(argv)

    if not args.clusters_path.is_file():
        print(f"clusters file not found: {args.clusters_path}", file=sys.stderr)
        return 1
    clusters = _load_json(args.clusters_path)
    metadata_snapshot = _load_json(args.metadata_path)

    rows: list[str] = [
        "# Plan 4 anchoring validation report",
        "",
        f"Skill: `{args.skill_id}`",
        f"Clusters: {len(clusters)}",
        "",
        "| Cluster | N=1 proposals | N=3 proposals | N=5 proposals |",
        "|---|---|---|---|",
    ]

    for cluster in clusters:
        cid = cluster.get("cluster_id", "?")
        cells: list[str] = [f"`{cid}`"]
        for n in (1, 3, 5):
            try:
                proposals = _run_one(
                    cluster, metadata_snapshot, args.skill_id, n, w=None,
                )
            except Exception as exc:
                cells.append(f"ERROR: {type(exc).__name__}")
                continue
            if not proposals:
                cells.append("(empty)")
            else:
                cells.append("<br>".join(
                    f"{i + 1}. {_summarize_proposal(p)}"
                    for i, p in enumerate(proposals)
                ))
        rows.append("| " + " | ".join(cells) + " |")

    rows.extend([
        "",
        "## How to read this report",
        "",
        "- **Anchoring symptom**: N=1 cells consistently propose changes",
        "  to a SINGLE column / table (the one in the example). Same",
        "  cluster with N=3 should propose broader changes (synonyms",
        "  covering multiple naming conventions, definitions touching",
        "  multiple columns, etc.) if anti-anchoring framing is working.",
        "- **No-effect symptom**: N=1 and N=3 cells are identical for",
        "  most clusters. Either the framing is being ignored, or the",
        "  cluster's failures genuinely converge on a single column —",
        "  inspect the cluster's question_traces.failed_judges to",
        "  confirm.",
        "- **Diminishing return symptom**: N=5 cells are identical to",
        "  N=3 cells. Confirms 3 is the right default; >3 adds prompt",
        "  budget cost without adding signal.",
    ])

    args.output.write_text("\n".join(rows), encoding="utf-8")
    print(f"wrote: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
