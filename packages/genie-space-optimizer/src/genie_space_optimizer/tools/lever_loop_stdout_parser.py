"""Typed parser over the lever-loop stdout / export-run text.

Returns a ``LeverLoopStdoutView`` that the gso-postmortem and
gso-lever-loop-run-analysis skills consume in place of grepping raw
text. The parser is forgiving: any block that is missing or
malformed is reported as ``None`` / empty rather than raising — the
analysis skill names the gap rather than failing closed.

The harness emits per-AG content inside ACTION GROUP blocks scoped by
``== ACTION GROUP <ag_id> — Iteration (index N / attempt M) ==``. Each
block contains: PROPOSAL INVENTORY, BLAST-RADIUS GATE, PATCH SURVIVAL,
a small EVALUATION SUMMARY box (``EVALUATION SUMMARY iter=N ag=AGID``),
and a FULL EVAL header. The parser uses the ACTION GROUP header as the
per-(iter, ag) anchor and partitions the rest of the text accordingly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class OptimizationRunSummary:
    baseline_accuracy_pct: float | None
    final_accuracy_pct: float | None
    iterations_attempted: int
    iterations_accepted: int
    iterations_rolled_back: int
    terminal_status: str


@dataclass(frozen=True)
class EvaluationSummary:
    iteration: int
    ag_id: str
    baseline_pre_arbiter: float | None
    candidate_pre_arbiter: float | None
    baseline_post_arbiter: float | None
    candidate_post_arbiter: float | None
    target_fixed_qids: tuple[str, ...]
    target_still_hard_qids: tuple[str, ...]
    target_still_hard_qids_source: str = "unknown"


@dataclass(frozen=True)
class ProposalInventoryEntry:
    proposal_id: str
    patch_type: str | None
    rca_id: str | None
    relevance: float | None


@dataclass(frozen=True)
class BlastRadiusDrop:
    iteration: int
    proposal_id: str
    reason: str
    patch_type: str | None
    passing_dependents: tuple[str, ...]


@dataclass(frozen=True)
class PatchSurvivalDrop:
    proposal_id: str
    reason: str
    detail: str | None = None


@dataclass(frozen=True)
class PatchSurvival:
    selected_count: int
    dropped_count: int
    selected: tuple[str, ...]
    dropped: tuple[PatchSurvivalDrop, ...]


@dataclass(frozen=True)
class AcceptanceDecision:
    iteration: int
    ag_id: str
    accepted: bool
    reason_code: str
    target_qids: tuple[str, ...]
    target_fixed_qids: tuple[str, ...]
    target_still_hard_qids: tuple[str, ...]
    target_still_hard_qids_source: str = "unknown"


@dataclass(frozen=True)
class StrategistOutput:
    iteration: int
    ag_ids: tuple[str, ...]
    cluster_coverage_count: int
    coverage_gap_signaled: bool


@dataclass
class LeverLoopStdoutView:
    optimization_run_summary: OptimizationRunSummary | None
    evaluation_summary: dict[int, EvaluationSummary] = field(default_factory=dict)
    strategist_output: dict[int, StrategistOutput] = field(default_factory=dict)
    proposal_inventory: dict[int, dict[str, tuple[ProposalInventoryEntry, ...]]] = field(
        default_factory=dict
    )
    blast_radius_drops: dict[int, tuple[BlastRadiusDrop, ...]] = field(default_factory=dict)
    patch_survival: dict[int, dict[str, PatchSurvival]] = field(default_factory=dict)
    acceptance_decision: dict[int, dict[str, AcceptanceDecision]] = field(
        default_factory=dict
    )


# ---------------------------------------------------------------------------
# Anchor patterns
# ---------------------------------------------------------------------------

# Each ACTION GROUP block is the per-AG body containing PROPOSAL INVENTORY,
# BLAST-RADIUS GATE, PATCH SURVIVAL, a small EVAL summary box, and FULL EVAL.
_ACTION_GROUP_RE = re.compile(
    r"==\s*ACTION GROUP\s+([A-Z0-9_]+)\s*[—-]\s*Iteration\s*\(index\s+(\d+)\s*/\s*attempt\s+\d+\)",
)

# Small EVAL summary box (one per AG; appears inside the AG's body).
_SMALL_EVAL_RE = re.compile(r"EVALUATION SUMMARY\s+iter=(\d+)\s+ag=([A-Z0-9_]+)")

# Strategist header (one per iteration).
_STRATEGIST_RE = re.compile(
    r"==\s*ADAPTIVE STRATEGIST\s*[—-]\s*Iteration\s*\(index\s+(\d+)",
)

_TARGET_FIXED_RE = re.compile(r"target_fixed_qids[=:\s]+\(?([^)\n]*)\)?")
_TARGET_STILL_RE = re.compile(r"target_still_hard_qids[=:\s]+\(?([^)\n]*)\)?")
_TARGET_QIDS_RE = re.compile(r"target_qids[=:\s]+\(?([^)\n;]*)\)?")


def _parse_qid_list(raw: str) -> tuple[str, ...]:
    raw = (raw or "").strip().strip("()").strip("[]")
    if not raw or raw.lower() in {"(none)", "none", ""}:
        return ()
    return tuple(
        q.strip().strip(",").strip("'\"")
        for q in raw.split(",")
        if q.strip() and q.strip().lower() not in {"(none)", "none"}
    )


# ---------------------------------------------------------------------------
# OPTIMIZATION RUN SUMMARY
# ---------------------------------------------------------------------------

def _parse_optimization_run_summary(text: str) -> OptimizationRunSummary | None:
    if "OPTIMIZATION RUN SUMMARY" not in text:
        return None
    # Extract individual fields with isolated regexes. The harness emits
    # them as a vertical-bar formatted table; field names are stable.
    body_start = text.index("OPTIMIZATION RUN SUMMARY")
    body = text[body_start : body_start + 4000]
    baseline = re.search(r"Baseline accuracy:\s*([\d.]+)%", body)
    final = re.search(r"Final accuracy:\s*([\d.]+)%", body)
    attempted = re.search(r"Action groups attempted:\s*(\d+)", body)
    accepted = re.search(r"Action groups accepted:\s*(\d+)", body)
    rolled = re.search(r"Action groups rolled back:\s*(\d+)", body)
    return OptimizationRunSummary(
        baseline_accuracy_pct=float(baseline.group(1)) if baseline else None,
        final_accuracy_pct=float(final.group(1)) if final else None,
        iterations_attempted=int(attempted.group(1)) if attempted else 0,
        iterations_accepted=int(accepted.group(1)) if accepted else 0,
        iterations_rolled_back=int(rolled.group(1)) if rolled else 0,
        terminal_status="max_iterations",
    )


# ---------------------------------------------------------------------------
# AG body partitioning
# ---------------------------------------------------------------------------

def _ag_blocks(text: str) -> list[tuple[int, str, int, int]]:
    """Return ordered list of (iteration, ag_id, body_start, body_end) blocks.

    body spans from the ACTION GROUP header to the next ACTION GROUP or
    the OPTIMIZATION RUN SUMMARY (whichever comes first).
    """
    matches = list(_ACTION_GROUP_RE.finditer(text))
    if not matches:
        return []
    end_marker = text.find("OPTIMIZATION RUN SUMMARY")
    if end_marker < 0:
        end_marker = len(text)
    blocks: list[tuple[int, str, int, int]] = []
    for i, m in enumerate(matches):
        start = m.end()
        next_start = matches[i + 1].start() if i + 1 < len(matches) else end_marker
        blocks.append((int(m.group(2)), m.group(1), start, next_start))
    return blocks


# ---------------------------------------------------------------------------
# Per-AG parsers (operate on a single ACTION GROUP body)
# ---------------------------------------------------------------------------

_PROPOSAL_INVENTORY_RE = re.compile(
    r"--\s*PROPOSAL INVENTORY\s*\[([A-Z0-9_]+)\][^\n]*\n(?P<body>.*?)(?=\n--\s|\n==\s|\Z)",
    re.DOTALL,
)
# A patches line carries N proposals separated by `;` with the shape:
#   P001#1 L4 add_join_spec target=[AGID] ... rel=1.00 rca=<rca_id|None> qids=['<qid>', ...]
_PROPOSAL_LINE_RE = re.compile(
    r"(P\d+(?:#\d+)?)\s+L\d+\s+([A-Za-z_]+)\s+target=\[([A-Z0-9_]+)\][^;]*?"
    r"rel=([\d.]+)\s+rca=([A-Za-z0-9_]+|None|none)"
    r"(?:[^;]*?qids=\[([^\]]*)\])?",
)


def _parse_proposal_inventory_for_ag(
    body: str, iteration: int, ag_id: str
) -> tuple[tuple[ProposalInventoryEntry, ...], tuple[str, ...]]:
    """Return (proposals, target_qids) for the AG's PROPOSAL INVENTORY."""
    m = _PROPOSAL_INVENTORY_RE.search(body)
    if not m:
        return (), ()
    inv_body = m.group("body")
    entries: list[ProposalInventoryEntry] = []
    qids_seen: list[str] = []
    qids_seen_set: set[str] = set()
    for prop_id, patch_type, owner, rel_raw, rca_raw, qids_raw in _PROPOSAL_LINE_RE.findall(
        inv_body
    ):
        if owner != ag_id:
            continue
        entries.append(
            ProposalInventoryEntry(
                proposal_id=prop_id,
                patch_type=patch_type,
                rca_id=None if rca_raw.lower() in {"none", ""} else rca_raw,
                relevance=float(rel_raw) if rel_raw else None,
            )
        )
        for q in _parse_qid_list(qids_raw):
            if q not in qids_seen_set:
                qids_seen_set.add(q)
                qids_seen.append(q)
    return tuple(entries), tuple(qids_seen)


_BLAST_RADIUS_RE = re.compile(
    r"\[([A-Z0-9_]+)\]\s+BLAST-RADIUS GATE[^\n]*\n(?P<body>.*?)(?=\n--\s|\n==\s|\n┌|\Z)",
    re.DOTALL,
)
# Per-proposal blast-radius drop line:
#   "|  - P001#2 (add_sql_snippet_expression): reason=high_collateral_risk_flagged, outside_target=['gs_016', ...]"
_BLAST_DROP_LINE_RE = re.compile(
    r"(P\d+(?:#\d+)?)\s*\(([A-Za-z_]+)\):\s*reason=([a-z_]+)"
    r"(?:[^\n]*?outside_target=\[([^\]]*)\])?",
)


def _parse_blast_radius_for_ag(
    body: str, iteration: int, ag_id: str
) -> tuple[BlastRadiusDrop, ...]:
    m = _BLAST_RADIUS_RE.search(body)
    if not m or m.group(1) != ag_id:
        return ()
    block = m.group("body")
    drops: list[BlastRadiusDrop] = []
    for prop_id, patch_type, reason, deps_raw in _BLAST_DROP_LINE_RE.findall(block):
        deps = tuple(
            q.strip().strip("'\"")
            for q in (deps_raw or "").split(",")
            if q.strip()
        )
        drops.append(
            BlastRadiusDrop(
                iteration=iteration,
                proposal_id=prop_id,
                reason=reason,
                patch_type=patch_type,
                passing_dependents=deps,
            )
        )
    return tuple(drops)


# Patch-cap selection/drop data lives in the OPERATOR TRANSCRIPT block as
# one-line operator records:
#   "| - outcome=accepted reason=patch_cap_selected qid=... ag=AGID proposal=P001#1 gate=patch_cap detail=target_coverage observed=..."
#   "| - outcome=dropped  reason=patch_cap_dropped  qid=... ag=AGID proposal=P004#1 gate=patch_cap detail=lower_causal_rank observed=..."
# The PATCH SURVIVAL box (a tabular `cluster | proposed | normalized | ...`
# layout) is human-readable but not regex-friendly; the operator-transcript
# emit is the structured projection of the same decision.
_PATCH_CAP_LINE_RE = re.compile(
    r"reason=(patch_cap_(?:selected|dropped))[^\n]*"
    r"ag=([A-Z0-9_]+)[^\n]*"
    r"proposal=(P\d+(?:#\d+)?)[^\n]*"
    r"detail=([a-z_]+)",
)


def _parse_patch_survival_for_ag(body: str, ag_id: str) -> PatchSurvival | None:
    selected: list[str] = []
    dropped: list[PatchSurvivalDrop] = []
    for reason, owner, prop_id, detail in _PATCH_CAP_LINE_RE.findall(body):
        if owner != ag_id:
            continue
        if reason == "patch_cap_selected":
            selected.append(prop_id)
        elif reason == "patch_cap_dropped":
            dropped.append(PatchSurvivalDrop(proposal_id=prop_id, reason=detail))
    if not selected and not dropped:
        return None
    return PatchSurvival(
        selected_count=len(selected),
        dropped_count=len(dropped),
        selected=tuple(selected),
        dropped=tuple(dropped),
    )


# Two FULL EVAL header forms:
#   "== FULL EVAL [AG_COVERAGE_H003]: PASS -- ACCEPTED ====="
#   "-- FULL EVAL [AG4]: FAIL (REGRESSION) -----"
_FULL_EVAL_RE = re.compile(
    r"(?:==|--)\s*FULL EVAL\s+\[([A-Z0-9_]+)\]:\s*"
    r"(?P<status>[A-Z][A-Z ]*?(?:\([A-Z]+\))?)\s*"
    r"(?:--\s*(?P<acceptance>[A-Z ]+?))?"
    r"\s*[-=]+",
)
_REGRESSIONS_LINE_RE = re.compile(
    r"Regressions:[^\n]*\[([^\]]*)\]",
)


def _parse_full_eval_for_ag(
    body: str, iteration: int, ag_id: str, target_qids: tuple[str, ...]
) -> AcceptanceDecision | None:
    m = _FULL_EVAL_RE.search(body)
    if not m or m.group(1) != ag_id:
        return None
    status_raw = (m.group("status") or "").strip().upper()
    acceptance_raw = (m.group("acceptance") or "").strip().upper()
    combined = f"{status_raw} {acceptance_raw}".strip()
    accepted = (
        ("ACCEPTED" in combined or "PASS" in status_raw)
        and "ROLLED BACK" not in combined
        and "FAIL" not in status_raw
    )
    # Body of the FULL EVAL block ends at the next == box boundary.
    eval_end = body.find("==============", m.end())
    dash_end = body.find("------------------------------", m.end())
    if eval_end < 0 or (0 <= dash_end < eval_end):
        eval_end = dash_end
    if eval_end < 0:
        eval_end = len(body)
    eval_body = body[m.end() : eval_end + 400]

    fixed_match = _TARGET_FIXED_RE.search(eval_body)
    # Look in the broader AG body for target_fixed_qids since the small
    # EVAL summary box (which carries it on accepted iters) is OUTSIDE
    # the FULL EVAL ==...== box but inside the same ACTION GROUP body.
    if not fixed_match:
        fixed_match = _TARGET_FIXED_RE.search(body)
    target_fixed = _parse_qid_list(fixed_match.group(1) if fixed_match else "")

    # target_still_hard_qids: explicit if Regressions: line present.
    regressions = _REGRESSIONS_LINE_RE.search(eval_body)
    explicit_block = regressions.group(1) if regressions else ""
    still_match = _TARGET_STILL_RE.search(explicit_block)
    targets_match = _TARGET_QIDS_RE.search(explicit_block)
    explicit_targets = _parse_qid_list(targets_match.group(1)) if targets_match else ()

    if still_match and still_match.group(1).strip():
        target_still = _parse_qid_list(still_match.group(1))
        source = "explicit"
    elif target_qids:
        fixed_set = set(target_fixed)
        target_still = tuple(q for q in target_qids if q not in fixed_set)
        source = "derived"
    else:
        target_still = ()
        source = "unknown"

    reason_match = re.search(r"reason=([a-z_]+)", explicit_block) or re.search(
        r"reason=([a-z_]+)", eval_body
    )

    return AcceptanceDecision(
        iteration=iteration,
        ag_id=ag_id,
        accepted=accepted,
        reason_code=(reason_match.group(1) if reason_match else ""),
        target_qids=explicit_targets or target_qids,
        target_fixed_qids=target_fixed,
        target_still_hard_qids=target_still,
        target_still_hard_qids_source=source,
    )


# ---------------------------------------------------------------------------
# Top-level per-AG dispatch
# ---------------------------------------------------------------------------

def _build_ag_target_qids_index(text: str) -> dict[int, dict[str, tuple[str, ...]]]:
    """Walk every ACTION GROUP block once and harvest (iter, ag) -> target_qids
    from the AG's PROPOSAL INVENTORY block. This index is the fallback
    source when a FULL EVAL block omits ``target_qids=(...)`` (which it
    routinely does on accepted iters — the canonical
    ``ACCEPTANCE_TARGET_BLIND`` case).
    """
    index: dict[int, dict[str, tuple[str, ...]]] = {}
    for iteration, ag_id, start, end in _ag_blocks(text):
        ag_body = text[start:end]
        _, qids = _parse_proposal_inventory_for_ag(ag_body, iteration, ag_id)
        if qids:
            index.setdefault(iteration, {})[ag_id] = qids
    return index


def _parse_evaluation_summaries(
    text: str,
    ag_targets_idx: Mapping[int, Mapping[str, tuple[str, ...]]] | None = None,
) -> dict[int, EvaluationSummary]:
    """Parse the small EVALUATION SUMMARY box per (iter, ag) pair.

    Schema:
      ``EVALUATION SUMMARY  iter=N  ag=AGID`` followed by lines:
        ``pre_arbiter  baseline=X%  candidate=Y%  delta=...``
        ``post_arbiter baseline=X%  candidate=Y%  delta=...``
        ``target_fixed_qids: <list-or-none>``
    The map is keyed by iteration (the last AG seen for that iteration
    wins, matching the harness's per-iteration eval cadence).
    """
    out: dict[int, EvaluationSummary] = {}
    ag_targets_idx = ag_targets_idx or {}
    for m in _SMALL_EVAL_RE.finditer(text):
        iteration = int(m.group(1))
        ag_id = m.group(2)
        # Body window spans this small EVAL box's start to the next
        # `└─` line (which closes the box) plus a small slack to capture
        # `target_fixed_qids:` and `regressed_only_pre_arbiter:`.
        end = text.find("└─", m.end())
        if end < 0:
            end = m.end() + 800
        else:
            end += 200
        body = text[m.start() : end]
        baseline_pre_match = re.search(
            r"pre_arbiter\s+baseline=([\d.]+)%\s+candidate=([\d.]+)%", body
        )
        baseline_post_match = re.search(
            r"post_arbiter\s+baseline=([\d.]+)%\s+candidate=([\d.]+)%", body
        )
        fixed = _TARGET_FIXED_RE.search(body)
        still = _TARGET_STILL_RE.search(body)
        target_fixed = _parse_qid_list(fixed.group(1) if fixed else "")
        if still and still.group(1).strip():
            target_still = _parse_qid_list(still.group(1))
            source = "explicit"
        else:
            ag_targets = ag_targets_idx.get(iteration, {}).get(ag_id, ())
            if ag_targets:
                fixed_set = set(target_fixed)
                target_still = tuple(q for q in ag_targets if q not in fixed_set)
                source = "derived"
            else:
                target_still = ()
                source = "unknown"
        out[iteration] = EvaluationSummary(
            iteration=iteration,
            ag_id=ag_id,
            baseline_pre_arbiter=(
                float(baseline_pre_match.group(1)) if baseline_pre_match else None
            ),
            candidate_pre_arbiter=(
                float(baseline_pre_match.group(2)) if baseline_pre_match else None
            ),
            baseline_post_arbiter=(
                float(baseline_post_match.group(1)) if baseline_post_match else None
            ),
            candidate_post_arbiter=(
                float(baseline_post_match.group(2)) if baseline_post_match else None
            ),
            target_fixed_qids=target_fixed,
            target_still_hard_qids=target_still,
            target_still_hard_qids_source=source,
        )
    return out


def _parse_strategist_output(text: str) -> dict[int, StrategistOutput]:
    out: dict[int, StrategistOutput] = {}
    matches = list(_STRATEGIST_RE.finditer(text))
    for i, m in enumerate(matches):
        iteration = int(m.group(1))
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else text.find(
            "OPTIMIZATION RUN SUMMARY", start
        )
        if end < 0:
            end = len(text)
        body = text[start:end]
        ag_ids = tuple(sorted(set(re.findall(r"\bAG[_A-Z0-9]+\b", body))))
        coverage_gap = "STRATEGIST COVERAGE GAP" in body or "coverage_gap" in body
        cluster_coverage = body.count("cluster=")
        out[iteration] = StrategistOutput(
            iteration=iteration,
            ag_ids=ag_ids,
            cluster_coverage_count=cluster_coverage,
            coverage_gap_signaled=coverage_gap,
        )
    return out


def parse_lever_loop_stdout(text: str) -> LeverLoopStdoutView:
    blocks = _ag_blocks(text)
    ag_targets_idx = _build_ag_target_qids_index(text)

    proposal_inventory: dict[int, dict[str, tuple[ProposalInventoryEntry, ...]]] = {}
    blast_radius_drops: dict[int, list[BlastRadiusDrop]] = {}
    patch_survival: dict[int, dict[str, PatchSurvival]] = {}
    acceptance_decision: dict[int, dict[str, AcceptanceDecision]] = {}

    for iteration, ag_id, start, end in blocks:
        ag_body = text[start:end]
        proposals, qids = _parse_proposal_inventory_for_ag(ag_body, iteration, ag_id)
        if proposals:
            proposal_inventory.setdefault(iteration, {})[ag_id] = proposals
        # Use index-derived qids if local inventory parse missed it.
        target_qids = qids or ag_targets_idx.get(iteration, {}).get(ag_id, ())

        drops = _parse_blast_radius_for_ag(ag_body, iteration, ag_id)
        if drops:
            blast_radius_drops.setdefault(iteration, []).extend(drops)

        survival = _parse_patch_survival_for_ag(ag_body, ag_id)
        if survival is not None:
            patch_survival.setdefault(iteration, {})[ag_id] = survival

        decision = _parse_full_eval_for_ag(ag_body, iteration, ag_id, target_qids)
        if decision is not None:
            acceptance_decision.setdefault(iteration, {})[ag_id] = decision

    return LeverLoopStdoutView(
        optimization_run_summary=_parse_optimization_run_summary(text),
        evaluation_summary=_parse_evaluation_summaries(text, ag_targets_idx),
        strategist_output=_parse_strategist_output(text),
        proposal_inventory=proposal_inventory,
        blast_radius_drops={k: tuple(v) for k, v in blast_radius_drops.items()},
        patch_survival=patch_survival,
        acceptance_decision=acceptance_decision,
    )
