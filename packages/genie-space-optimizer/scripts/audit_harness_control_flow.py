#!/usr/bin/env python3
"""Harness control-flow audit CLI.

Static AST enumeration + dynamic reachability trace against both
production anchor tapes. Outputs:

  * docs/runid_analysis/harness_control_flow_audit.json (sidecar)
  * docs/runid_analysis/harness_control_flow_audit.md   (report)

Usage:

  python scripts/audit_harness_control_flow.py
"""
from __future__ import annotations

import ast
import json
import sys
from dataclasses import asdict
from pathlib import Path

_THIS = Path(__file__).resolve()
_PKG_ROOT = _THIS.parents[1]
_SRC = _PKG_ROOT / "src"
sys.path.insert(0, str(_SRC))

from genie_space_optimizer.optimization.harness_control_flow_ast import (  # noqa: E402
    BranchPoint,
    enumerate_branch_points,
)
from genie_space_optimizer.optimization.harness_control_flow_tracer import (  # noqa: E402
    trace_run_lever_loop,
)
from genie_space_optimizer.optimization.tape import LeverLoopTape  # noqa: E402

_HARNESS_PATH = (
    _PKG_ROOT / "src" / "genie_space_optimizer" / "optimization" / "harness.py"
)
_TAPES_DIR = (
    _PKG_ROOT / "tests" / "replay" / "active" / "fixtures" / "production_tapes"
)
_OUT_DIR = _PKG_ROOT / "docs" / "harness" / "audit"
_OUT_JSON = _OUT_DIR / "harness_control_flow_audit.json"
_OUT_MD = _OUT_DIR / "harness_control_flow_audit.md"


_ANCHORS = [
    {
        "tape_file": "airline_run_59a173d3.json",
        "tape_id": "airline_run_59a173d3",
        "run_id": "audit-airline",
        "space_id": "space-airline",
        "domain": "airline",
        "prev_accuracy": 0.9167,
    },
    {
        "tape_file": "seven_now_run_ab65fefe.json",
        "tape_id": "seven_now_run_ab65fefe",
        "run_id": "audit-7now",
        "space_id": "space-7now",
        "domain": "7now",
        "prev_accuracy": 0.9130,
    },
]


def _function_lineno_range(source: str, function_name: str) -> tuple[int, int]:
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == function_name
        ):
            return node.lineno, (node.end_lineno or node.lineno)
    raise LookupError(function_name)


def main() -> int:
    source = _HARNESS_PATH.read_text()
    fn_start, fn_end = _function_lineno_range(source, "_run_lever_loop")
    branch_points = enumerate_branch_points(
        source, function_name="_run_lever_loop"
    )

    reachability: dict[str, frozenset[int]] = {}
    for anchor in _ANCHORS:
        tape_path = _TAPES_DIR / anchor["tape_file"]
        if not tape_path.exists():
            print(
                f"WARN: tape missing for {anchor['tape_id']}; skipping.",
                file=sys.stderr,
            )
            reachability[anchor["tape_id"]] = frozenset()
            continue
        tape = LeverLoopTape.from_json_file(tape_path)
        executed = trace_run_lever_loop(
            tape=tape,
            run_id=anchor["run_id"],
            space_id=anchor["space_id"],
            domain=anchor["domain"],
            prev_accuracy=anchor["prev_accuracy"],
        )
        reachability[anchor["tape_id"]] = executed

    # Annotate each branch point with reachability per tape.
    annotated: list[dict] = []
    for bp in branch_points:
        rec = asdict(bp)
        for anchor in _ANCHORS:
            tape_id = anchor["tape_id"]
            reached = bp.lineno in reachability.get(tape_id, frozenset())
            rec[f"reached_{tape_id}"] = reached
        annotated.append(rec)

    # Write JSON sidecar.
    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "harness_path": str(_HARNESS_PATH.relative_to(_PKG_ROOT)),
        "function_name": "_run_lever_loop",
        "function_lineno_range": [fn_start, fn_end],
        "anchors": [a["tape_id"] for a in _ANCHORS],
        "branch_points": annotated,
        "reachability_summary": {
            tape_id: len(lines) for tape_id, lines in reachability.items()
        },
        "executed_lines_by_anchor": {
            tape_id: sorted(lines) for tape_id, lines in reachability.items()
        },
    }
    _OUT_JSON.write_text(json.dumps(payload, indent=2))

    # Write Markdown report.
    md = _render_markdown(payload)
    _OUT_MD.write_text(md)

    # Stdout summary.
    print(f"Function: _run_lever_loop ({fn_start}–{fn_end})")
    print(f"Branch points: {len(annotated)}")
    for tape_id, lines in reachability.items():
        print(f"Reachable lines under {tape_id}: {len(lines)}")
    print(f"JSON: {_OUT_JSON}")
    print(f"MD:   {_OUT_MD}")
    return 0


def _render_markdown(payload: dict) -> str:
    rows = []
    anchors = payload["anchors"]
    header_cols = (
        ["lineno", "type", "depth", "parent", "detail"]
        + [f"reached:{a}" for a in anchors]
        + ["snippet"]
    )
    rows.append("| " + " | ".join(header_cols) + " |")
    rows.append("|" + "|".join(["---"] * len(header_cols)) + "|")
    for bp in payload["branch_points"]:
        line = [
            str(bp["lineno"]),
            bp["statement_type"],
            str(bp["depth"]),
            bp["parent_construct"],
            bp.get("detail") or "",
        ]
        for a in anchors:
            line.append("YES" if bp.get(f"reached_{a}") else "no")
        snippet = bp.get("snippet") or ""
        snippet = snippet.replace("|", "\\|").replace("\n", " ")
        line.append("`" + snippet[:80] + "`")
        rows.append("| " + " | ".join(line) + " |")

    summary_rows = [
        "# Harness Control-Flow Audit",
        "",
        f"Function: `_run_lever_loop`  ",
        f"Line range: {payload['function_lineno_range'][0]}–{payload['function_lineno_range'][1]}  ",
        f"Total branch points: {len(payload['branch_points'])}  ",
        "",
        "## Reachability summary",
        "",
    ]
    for tape_id, n in payload["reachability_summary"].items():
        summary_rows.append(f"* `{tape_id}`: {n} lines executed")
    summary_rows.append("")
    summary_rows.append("## Branch points")
    summary_rows.append("")

    return "\n".join(summary_rows + rows) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
