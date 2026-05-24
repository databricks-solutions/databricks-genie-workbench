"""Funnel report renderer for the local workbench.

Inputs: the raw ``LocalRunArtifacts`` from ``local_runner`` plus the
Stage 1 probe and bundle metadata. Outputs:

* ``result.json`` — JSON document with the structured workbench result.
* ``result.md`` — Markdown summary suitable for a PR description.

The renderer never re-runs the state machine; it only reads what was
captured. Marker parsing reuses the production ``marker_parser`` so
the workbench and the postmortem skill see exactly the same payloads.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Mapping

from local_lever_workbench.local_runner import LocalRunArtifacts, summarize_stage_progress
from local_lever_workbench.models import (
    Stage1ProbeResult,
    StageProgress,
    WorkbenchInputBundle,
    WorkbenchRunConfig,
    WorkbenchRunResult,
)


_FUNNEL_ORDER: tuple[str, ...] = (
    "hard_qid_seen",
    "diagnosed",
    "clustered",
    "proposed",
    "normalized",
    "applyable",
    "applied",
    "evaluated",
    "accepted",
    "terminated",
)


def _deepest_funnel_value(progress: tuple[StageProgress, ...]) -> str:
    """Return the deepest funnel stage observed across all QIDs."""
    if not progress:
        return "none"
    best_idx = -1
    best_value = "hard_qid_seen"
    for p in progress:
        try:
            idx = _FUNNEL_ORDER.index(p.deepest_stage)
        except ValueError:
            continue
        if idx > best_idx:
            best_idx = idx
            best_value = p.deepest_stage
    return best_value


# ── Marker / stdout parsing ──────────────────────────────────────────


_MARKER_RE = re.compile(r"^(GSO_[A-Z0-9_]+_V\d+)\s+(\{.*\})$", re.MULTILINE)


def _aggregate_markers(stdout_text: str) -> dict[str, list[dict]]:
    """Group stdout marker lines by marker name, JSON-decoding each payload.

    Mirrors ``marker_parser`` but flattens to ``{name: [payload, ...]}``
    so the report can show counts without depending on the typed
    ``MarkerLog`` schema (which is tuned for the orchestrator).
    """
    out: dict[str, list[dict]] = {}
    for match in _MARKER_RE.finditer(stdout_text or ""):
        name = match.group(1)
        try:
            payload = json.loads(match.group(2))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        out.setdefault(name, []).append(payload)
    return out


# ── Surprise detection ──────────────────────────────────────────────


_KNOWN_SURPRISE_RULES: tuple[tuple[str, str], ...] = (
    # Stage 1 hydration regressions: the 2026-05-23 trial 12 silent
    # decline mode. A failure here means the workbench bundle would
    # die in production at initial state.
    ("question_text_empty", "Stage 1 evidence card carries no question text"),
    (
        "evidence_card_empty",
        "Stage 1 evidence card emptied — same shape as Trial 12 silent declines",
    ),
    # Stage 3 archetype coverage gap surfaced by the trial 13 plan.
    (
        "empty_synthesis",
        "Stage 3 emitted no proposals — promote a typed reason into CI",
    ),
    (
        "synthesis_empty_reason",
        "Stage 3 emitted an untyped empty-synthesis reason",
    ),
    # Patch outcome absence — production saw this when apply never ran.
    (
        "no_applied_patches",
        "No PATCH outcome marker — applier gate did not fire",
    ),
)


def _detect_surprises(
    *,
    stage1: Stage1ProbeResult,
    progress: tuple[StageProgress, ...],
    markers: Mapping[str, list[dict]],
    recorded_patch_count: int,
) -> tuple[str, ...]:
    """Return human-readable surprise messages worth a CI promotion.

    The list is intentionally conservative — we only flag known
    silent decline modes the workbench is designed to catch. A
    surprise here should map to a future test promotion.
    """
    surprises: list[str] = []

    # Stage 1 violations surface immediately.
    for finding in stage1.findings:
        for code in finding.violations:
            for marker, message in _KNOWN_SURPRISE_RULES:
                if marker == code or marker in code:
                    surprises.append(
                        f"qid={finding.qid}: {message} ({code})"
                    )
                    break

    # Deepest stage less than APPLIED for every QID — workbench was
    # blocked downstream of Stage 1. Capture for the report so the
    # operator does not have to grep stdout for terminal reasons.
    if progress and all(
        p.deepest_stage not in ("applied", "evaluated", "accepted")
        for p in progress
    ):
        deepest = _deepest_funnel_value(progress)
        surprises.append(
            f"no QID reached APPLIED; deepest stage = {deepest}"
        )

    # Empty-synthesis marker without a typed reason — same shape as
    # the trial 13 Phase 7 xfail.
    empty_synth = markers.get("GSO_PLAN11_STAGE3_SYNTHESIS_V1") or []
    for payload in empty_synth:
        outcome = str(payload.get("outcome") or "")
        if outcome != "empty_synthesis":
            continue
        reason = str(payload.get("synthesis_empty_reason") or "")
        if not reason:
            surprises.append(
                "stage3 empty_synthesis without synthesis_empty_reason"
            )

    if recorded_patch_count == 0 and progress and any(
        p.deepest_stage in ("applied", "applyable") for p in progress
    ):
        surprises.append(
            "applier gate reached APPLYABLE but no PATCH was recorded"
        )

    return tuple(sorted(set(surprises)))


def _terminal_reason_set(progress: tuple[StageProgress, ...]) -> tuple[str, ...]:
    seen: list[str] = []
    for p in progress:
        if p.terminal_reason and p.terminal_reason not in seen:
            seen.append(p.terminal_reason)
    return tuple(seen)


# ── Top-level builder ───────────────────────────────────────────────


def build_run_result(
    *,
    bundle: WorkbenchInputBundle,
    config: WorkbenchRunConfig,
    stage1: Stage1ProbeResult,
    artifacts: LocalRunArtifacts,
) -> WorkbenchRunResult:
    """Assemble the typed ``WorkbenchRunResult`` from the run artefacts."""
    progress = summarize_stage_progress(artifacts)
    markers = _aggregate_markers(artifacts.stdout_text)
    deepest = _deepest_funnel_value(progress)
    surprises = _detect_surprises(
        stage1=stage1,
        progress=progress,
        markers=markers,
        recorded_patch_count=len(artifacts.recorder.as_tuple()),
    )

    # Keep a tail of stdout lines so postmortems can sanity-check the
    # workbench output without writing the full transcript to disk.
    stdout_lines = artifacts.stdout_text.splitlines()
    if len(stdout_lines) > 40:
        stdout_sample = tuple(stdout_lines[-40:])
    else:
        stdout_sample = tuple(stdout_lines)

    return WorkbenchRunResult(
        config=config,
        provenance=bundle.provenance,
        stage1_probe=stage1,
        stage_progress=progress,
        deepest_stage_reached=deepest,
        markers={k: len(v) for k, v in markers.items()},
        terminal_reasons=_terminal_reason_set(progress),
        recorded_patches=artifacts.recorder.as_tuple(),
        surprises=surprises,
        stdout_sample_lines=stdout_sample,
    )


# ── Markdown renderer ───────────────────────────────────────────────


def _render_markdown(result: WorkbenchRunResult) -> str:
    """Return a compact PR-ready Markdown summary of the workbench run."""
    lines: list[str] = []
    lines.append("# Local Lever-Loop Workbench Result")
    lines.append("")
    lines.append(f"- llm_mode: `{result.config.llm_mode}`")
    lines.append(f"- apply_mode: `{result.config.apply_mode}`")
    lines.append(
        f"- source_kind: `{result.provenance.source_kind}` "
        f"(run_id `{result.provenance.source_run_id or 'n/a'}`)"
    )
    lines.append(f"- deepest_stage_reached: **{result.deepest_stage_reached}**")
    lines.append(f"- recorded_patches: {len(result.recorded_patches)}")
    lines.append("")

    lines.append("## Stage 1 preflight")
    lines.append("")
    if result.stage1_probe.all_pass:
        lines.append("All hard QIDs pass the Stage 1 contract.")
    else:
        lines.append("| QID | violations | would_dispatch_llm |")
        lines.append("|---|---|---|")
        for finding in result.stage1_probe.findings:
            v = ", ".join(finding.violations) or "_(none)_"
            lines.append(
                f"| `{finding.qid}` | {v} | "
                f"{'yes' if finding.would_dispatch_llm else 'no'} |"
            )
    lines.append("")

    lines.append("## Funnel progress per QID")
    lines.append("")
    lines.append("| QID | deepest_stage | terminal_reason |")
    lines.append("|---|---|---|")
    for p in result.stage_progress:
        lines.append(
            f"| `{p.qid}` | `{p.deepest_stage}` | "
            f"{p.terminal_reason or '_(none)_'} |"
        )
    lines.append("")

    if result.markers:
        lines.append("## Marker counts")
        lines.append("")
        for name in sorted(result.markers.keys()):
            lines.append(f"- `{name}`: {result.markers[name]}")
        lines.append("")

    if result.terminal_reasons:
        lines.append("## Terminal reasons (deduplicated)")
        lines.append("")
        for r in result.terminal_reasons:
            lines.append(f"- `{r}`")
        lines.append("")

    if result.surprises:
        lines.append("## Surprises worth promoting into CI")
        lines.append("")
        for s in result.surprises:
            lines.append(f"- {s}")
        lines.append("")

    if result.recorded_patches:
        lines.append("## Recorded PATCH payloads")
        lines.append("")
        for rp in result.recorded_patches:
            lines.append(
                f"- `{rp.qid}` → `{rp.patch_type}` "
                f"(intent_id `{rp.intent_id}`)"
            )
        lines.append("")

    return "\n".join(lines) + "\n"


def write_report(
    *,
    result: WorkbenchRunResult,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Write ``result.json`` and ``result.md`` into ``output_dir``.

    Returns the ``(json_path, md_path)`` tuple. Creates the directory
    if it does not exist; never deletes anything.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "result.json"
    md_path = output_dir / "result.md"
    json_path.write_text(json.dumps(result.to_dict(), indent=2, default=str))
    md_path.write_text(_render_markdown(result))
    return json_path, md_path


__all__ = ["build_run_result", "write_report"]
