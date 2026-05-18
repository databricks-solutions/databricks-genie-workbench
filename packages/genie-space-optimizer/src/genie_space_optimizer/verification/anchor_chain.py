"""WU-A — anchor-chain invariant verifier.

Consumes the artifacts a real GSO optimizer run already emits
(``postmortem.json`` + ``evidence/lever_loop_latest_export_*_text.txt``)
and asserts the three-path lifecycle contract per anchor cluster
plus the global ``GSO_BEST_OF_N_RANKED_V1`` fire-once invariant.

This is the deferred WU-4 from
``2026-05-18-early-rca-preflight-and-slate-enforcement.md`` rescoped
against the real artifact shapes (the original WU-4 assumed a
``LeverLoopReplayHarness.from_tape(...).run()`` API that does not
exist; Phase 4.5 abandoned re-execution as a viable test scaffold
at ~1.5k LoC of fixture wiring).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence


class LifecyclePath(str, Enum):
    """The three legal post-fix lifecycle outcomes per anchor."""

    GROUNDED_WITH_CANDIDATE = "A"
    GROUNDED_WITH_TYPED_DECLINE = "B"
    PREFLIGHT_SKIP = "C"
    UNKNOWN = "UNKNOWN"


TYPED_ARCHETYPE_DECLINES: frozenset[str] = frozenset({
    "no_filter_removal_archetype",
    "no_top_n_archetype",
    "no_grain_archetype",
    "no_mapping_archetype",
    "no_structural_archetype",
})


ANCHOR_QID_SUFFIXES: tuple[str, ...] = ("gs_009", "gs_013", "gs_024", "gs_026")


@dataclass(frozen=True, slots=True)
class IterationRecord:
    """Subset of ``iteration_summary[i]`` the classifier needs."""

    iteration: int
    ag_id: str
    cluster_ids: tuple[str, ...]
    target_qids: tuple[str, ...]
    directive_outcome: Mapping[str, str]
    no_structural_candidate: Mapping[str, Any]
    terminal_reason: str
    next_step: str
    structural_gate_roots: tuple[str, ...]
    full_eval: Mapping[str, Any]


def parse_iteration_records(
    postmortem: Mapping[str, Any],
) -> tuple[IterationRecord, ...]:
    """Walk ``postmortem["iteration_summary"]`` and emit typed
    IterationRecord tuples. Tolerant of missing keys."""
    items = postmortem.get("iteration_summary") or []
    if not isinstance(items, Sequence):
        return ()
    out: list[IterationRecord] = []
    for it in items:
        if not isinstance(it, Mapping):
            continue
        out.append(
            IterationRecord(
                iteration=int(it.get("iteration") or 0),
                ag_id=str(it.get("ag_id") or ""),
                cluster_ids=tuple(
                    str(c) for c in (it.get("cluster_ids") or ())
                ),
                target_qids=tuple(
                    str(q) for q in (it.get("target_qids") or ())
                ),
                directive_outcome=dict(it.get("directive_outcome") or {}),
                no_structural_candidate=dict(
                    it.get("no_structural_candidate") or {}
                ),
                terminal_reason=str(it.get("terminal_reason") or ""),
                next_step=str(it.get("next_step") or ""),
                structural_gate_roots=tuple(
                    str(r) for r in (it.get("structural_gate_roots") or ())
                ),
                full_eval=dict(it.get("full_eval") or {}),
            )
        )
    return tuple(out)


def qid_suffix_for_match(qid: str) -> str:
    """Strip any space prefix and return the suffix used by the
    canonical anchor map (e.g., ``"gs_013"``). The suffix is the
    last ``gs_<digits>`` pair in the underscore-tokenized qid;
    falls back to the whole string if no match is found."""
    if not qid:
        return ""
    parts = str(qid).split("_")
    for i in range(len(parts) - 1):
        if parts[i] == "gs" and parts[i + 1].isdigit():
            return f"gs_{parts[i + 1]}"
    return str(qid)


@dataclass(frozen=True, slots=True)
class AnchorVerdict:
    """Per-anchor lifecycle verdict + the reasons it passed or
    failed. Serializable via ``dataclasses.asdict``."""

    qid_suffix: str
    cluster_id: str
    iteration: int
    lifecycle_path: LifecyclePath
    passed: bool
    reasons: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class VerifierResult:
    """Top-level result. ``passed`` is True iff every per-anchor
    verdict passed AND no global invariants failed."""

    anchor_verdicts: tuple[AnchorVerdict, ...]
    global_failures: tuple[str, ...]
    best_of_n_structural_fire_count: int

    @property
    def passed(self) -> bool:
        return (
            all(v.passed for v in self.anchor_verdicts)
            and not self.global_failures
        )

    def per_anchor_summary(self) -> str:
        lines: list[str] = []
        for v in self.anchor_verdicts:
            status = "PASS" if v.passed else "FAIL"
            reasons = "; ".join(v.reasons) if v.reasons else "ok"
            lines.append(
                f"  [{status}] {v.qid_suffix} "
                f"(cluster={v.cluster_id} iter={v.iteration} "
                f"path={v.lifecycle_path.value}) — {reasons}"
            )
        return "\n".join(lines)


class AnchorChainVerifier:
    """Orchestrator. Wraps postmortem + transcript parsing + the
    classifier. Use ``verify_runid_dir`` for the common case;
    construct directly for finer-grained inspection."""

    def __init__(
        self,
        *,
        postmortem: Mapping[str, Any] | None = None,
        transcript_text: str | None = None,
        anchor_qid_suffixes: Sequence[str] = ANCHOR_QID_SUFFIXES,
    ) -> None:
        self._postmortem = dict(postmortem or {})
        self._transcript = transcript_text or ""
        self._anchor_suffixes = tuple(anchor_qid_suffixes)

    def run(self) -> VerifierResult:
        raise NotImplementedError("classifier wiring — see Task 4")


def verify_runid_dir(runid_dir: Path) -> VerifierResult:
    """Convenience entrypoint used by the CLI. Loads
    ``runid_dir/postmortem.json`` and the latest
    ``runid_dir/evidence/lever_loop_latest_export_*_text.txt`` (if
    present), then runs the verifier. Raises FileNotFoundError if
    postmortem.json is missing."""
    pm_path = runid_dir / "postmortem.json"
    if not pm_path.exists():
        raise FileNotFoundError(f"postmortem.json not found at {pm_path}")
    with pm_path.open() as fh:
        postmortem = json.load(fh)
    transcript_text = _load_latest_transcript(runid_dir)
    return AnchorChainVerifier(
        postmortem=postmortem,
        transcript_text=transcript_text,
    ).run()


def _load_latest_transcript(runid_dir: Path) -> str:
    """Find the most recent ``lever_loop_latest_export_*_text.txt``
    under ``runid_dir/evidence/`` and return its contents. Returns
    "" if no transcript is present."""
    evidence = runid_dir / "evidence"
    if not evidence.is_dir():
        return ""
    candidates = sorted(
        evidence.glob("lever_loop_latest_export_*_text.txt"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return ""
    return candidates[0].read_text(errors="replace")
