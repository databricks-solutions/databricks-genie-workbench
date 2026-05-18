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
