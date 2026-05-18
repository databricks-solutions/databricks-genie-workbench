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
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


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
class MarkerLine:
    """One parsed transcript marker — ``<NAME> {json_payload}``."""

    name: str
    payload: Mapping[str, Any]


_MARKER_LINE_RE = re.compile(r"^([A-Z0-9_]+) (\{.*\})$")


def parse_transcript_markers(text: str) -> tuple[MarkerLine, ...]:
    """Walk ``text`` line by line and emit MarkerLine entries.
    Malformed JSON payloads are silently dropped (transcripts are
    sometimes truncated mid-line). Non-marker lines are ignored."""
    if not text:
        return ()
    out: list[MarkerLine] = []
    for line in text.splitlines():
        m = _MARKER_LINE_RE.match(line)
        if m is None:
            continue
        name, payload_text = m.group(1), m.group(2)
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, Mapping):
            continue
        out.append(MarkerLine(name=name, payload=payload))
    return tuple(out)


def count_best_of_n_structural_fires(
    markers: Iterable[MarkerLine],
) -> int:
    """Number of GSO_BEST_OF_N_RANKED_V1 markers whose
    intended_patch_shape is exactly 'structural'.

    Pre-WU-3.5 this count was always zero in production because
    ``should_run_best_of_n`` saw an empty intended_patch_shape
    (the ``getattr(thin_dict, attr, "")`` bug). Post-fix this
    count must be > 0 for any run that had structural-intent AGs.
    """
    return sum(
        1
        for m in markers
        if m.name == "GSO_BEST_OF_N_RANKED_V1"
        and str(m.payload.get("intended_patch_shape") or "").lower() == "structural"
    )


def count_admitted_with_empty_intent(
    markers: Iterable[MarkerLine],
) -> int:
    """Number of GSO_STRUCTURAL_REPAIR_DECISION_V1 markers with
    ``gate_verdict='admitted'`` AND BOTH ``intended_patch_shape``
    and ``rca_root_cause`` empty. That signature is the canonical
    pre-WU-3.5 + pre-WU-5 bug: the gate's fail-open branch fired
    because the harness consumer read the empty default from
    getattr on the thin RCA card dict."""
    out = 0
    for m in markers:
        if m.name != "GSO_STRUCTURAL_REPAIR_DECISION_V1":
            continue
        if str(m.payload.get("gate_verdict") or "").lower() != "admitted":
            continue
        shape = str(m.payload.get("intended_patch_shape") or "").strip()
        root = str(m.payload.get("rca_root_cause") or "").strip()
        if not shape and not root:
            out += 1
    return out


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
    """Orchestrator. Walks ``postmortem["iteration_summary"]`` and
    the transcript markers; emits an AnchorVerdict per anchor qid
    that the run actually targeted; adds global-invariant failures
    for run-wide patterns (best-of-n fire count, admitted-with-
    empty-intent count)."""

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
        records = parse_iteration_records(self._postmortem)
        markers = parse_transcript_markers(self._transcript)

        anchor_verdicts: list[AnchorVerdict] = []
        for rec in records:
            for qid in rec.target_qids:
                suffix = qid_suffix_for_match(qid)
                if suffix not in self._anchor_suffixes:
                    continue
                anchor_verdicts.append(
                    self._classify_anchor(rec, suffix, markers)
                )

        global_failures: list[str] = []
        bon_fires = count_best_of_n_structural_fires(markers)
        if bon_fires == 0:
            global_failures.append(
                "best_of_n_structural_never_fired: "
                "GSO_BEST_OF_N_RANKED_V1 with intended_patch_shape='structural' "
                "did not fire in this run — WU-3.5 wiring may not have "
                "taken effect, or no structural-intent AG was selected"
            )
        admitted_empty = count_admitted_with_empty_intent(markers)
        if admitted_empty > 0:
            global_failures.append(
                f"admitted_with_empty_intent: "
                f"{admitted_empty} GSO_STRUCTURAL_REPAIR_DECISION_V1 "
                f"marker(s) with gate_verdict='admitted' AND both "
                f"intended_patch_shape and rca_root_cause empty — "
                f"this is the pre-WU-3.5+WU-5 bug signature"
            )

        return VerifierResult(
            anchor_verdicts=tuple(anchor_verdicts),
            global_failures=tuple(global_failures),
            best_of_n_structural_fire_count=bon_fires,
        )

    def _classify_anchor(
        self,
        rec: IterationRecord,
        qid_suffix: str,
        markers: Sequence[MarkerLine],
    ) -> AnchorVerdict:
        cluster_id = rec.cluster_ids[0] if rec.cluster_ids else ""

        # Path C — preflight skip.
        if (
            rec.terminal_reason == "early_preflight_cluster_blocked_no_rca"
            or rec.next_step == "skip_ag"
        ):
            return AnchorVerdict(
                qid_suffix=qid_suffix,
                cluster_id=cluster_id,
                iteration=rec.iteration,
                lifecycle_path=LifecyclePath.PREFLIGHT_SKIP,
                passed=True,
                reasons=(
                    f"preflight skip: terminal_reason="
                    f"{rec.terminal_reason!r}, next_step={rec.next_step!r}",
                ),
            )

        nsc = rec.no_structural_candidate or {}
        skipped_reason = str(nsc.get("skipped_reason") or "")
        attempted = tuple(nsc.get("attempted_archetypes") or ())

        # Failure mode 1 — missing_rca_card. Always a FAIL.
        if skipped_reason == "missing_rca_card":
            return AnchorVerdict(
                qid_suffix=qid_suffix,
                cluster_id=cluster_id,
                iteration=rec.iteration,
                lifecycle_path=LifecyclePath.UNKNOWN,
                passed=False,
                reasons=(
                    f"missing_rca_card emitted for {rec.ag_id}; "
                    f"attempted_archetypes={list(attempted)}",
                ),
            )

        # Failure mode 2 — grounded but skipped_reason is not in the
        # typed-decline set. Forbidden by the WU-4 contract.
        if skipped_reason and skipped_reason not in TYPED_ARCHETYPE_DECLINES:
            return AnchorVerdict(
                qid_suffix=qid_suffix,
                cluster_id=cluster_id,
                iteration=rec.iteration,
                lifecycle_path=LifecyclePath.UNKNOWN,
                passed=False,
                reasons=(
                    f"untyped skipped_reason={skipped_reason!r}; "
                    f"WU-4 requires one of "
                    f"{sorted(TYPED_ARCHETYPE_DECLINES)}",
                ),
            )

        # Path B — grounded + typed archetype decline.
        if skipped_reason in TYPED_ARCHETYPE_DECLINES:
            if not attempted:
                return AnchorVerdict(
                    qid_suffix=qid_suffix,
                    cluster_id=cluster_id,
                    iteration=rec.iteration,
                    lifecycle_path=LifecyclePath.UNKNOWN,
                    passed=False,
                    reasons=(
                        f"typed decline {skipped_reason!r} but "
                        f"attempted_archetypes=[]; synthesizer must "
                        f"surface which archetypes it tried",
                    ),
                )
            return AnchorVerdict(
                qid_suffix=qid_suffix,
                cluster_id=cluster_id,
                iteration=rec.iteration,
                lifecycle_path=LifecyclePath.GROUNDED_WITH_TYPED_DECLINE,
                passed=True,
                reasons=(
                    f"typed decline {skipped_reason!r}; "
                    f"attempted={list(attempted)}",
                ),
            )

        # Path A — directive_outcome shows proposal_emitted AND no
        # skipped_reason. Candidate-shape correctness is NOT
        # checked here (it's archetype tuning, a follow-on plan).
        proposal_emitted = any(
            "proposal_emitted" in str(v).lower()
            for v in rec.directive_outcome.values()
        )
        if proposal_emitted:
            return AnchorVerdict(
                qid_suffix=qid_suffix,
                cluster_id=cluster_id,
                iteration=rec.iteration,
                lifecycle_path=LifecyclePath.GROUNDED_WITH_CANDIDATE,
                passed=True,
                reasons=(
                    f"proposal_emitted; "
                    f"directive_outcome={dict(rec.directive_outcome)}",
                ),
            )

        # Otherwise — neither path matched. UNKNOWN/FAIL.
        return AnchorVerdict(
            qid_suffix=qid_suffix,
            cluster_id=cluster_id,
            iteration=rec.iteration,
            lifecycle_path=LifecyclePath.UNKNOWN,
            passed=False,
            reasons=(
                f"no path matched: directive_outcome="
                f"{dict(rec.directive_outcome)}, "
                f"no_structural_candidate={dict(nsc)}, "
                f"terminal_reason={rec.terminal_reason!r}",
            ),
        )


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
