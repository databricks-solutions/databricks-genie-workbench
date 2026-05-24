"""Trial 13 Phase 8 — single ``CanonicalEvalRow`` normalization boundary.

The architectural fix for the 4th instance of the "one boundary too
shallow" anti-pattern. Every eval row entering the lever loop is
normalized here, exactly once, into a typed dataclass. Downstream
consumers MUST accept :class:`CanonicalEvalRow`; new row-shape
extensions must land in :func:`normalize_eval_row` and nowhere else.

The companion CI golden test
(``tests/integration/test_eval_row_normalization_golden.py``)
enforces single-boundary invariance: no new module may sprout
``row.get(``, ``row["``, or ``nested_get(row`` outside of this module
or :mod:`eval_row_access` (which this module composes over).

Rollback strategy (per plan §Risk and rollback): this module remains
an *opt-in* helper if the migration triggers production regressions
— legacy ``dict``-accepting code paths continue to work via the
compat shims in :mod:`eval_row_access`.
"""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

from genie_space_optimizer.optimization.eval_row_access import (
    _collect_blame_set_from_asi,
    _first_non_empty_asi_field,
    _JUDGE_RATIONALE_ORDER,
    collect_judge_rationales,
    row_expected_sql,
    row_generated_sql,
    row_qid,
    row_question_with_source,
)

# ─── Typed projections ──────────────────────────────────────────────


@dataclass(frozen=True)
class JudgeRationales:
    """Per-judge narrative + canonical-order projections.

    ``by_judge`` is the raw ``{judge: rationale}`` mapping in row
    order. ``combined`` is the concatenation in canonical judge order.
    ``primary()`` returns the first non-empty rationale in canonical
    order — the single most authoritative narrative.
    """

    by_judge: Mapping[str, str] = field(default_factory=dict)
    combined: str = ""

    def primary(self) -> str:
        for judge in _JUDGE_RATIONALE_ORDER:
            text = self.by_judge.get(judge, "").strip() if isinstance(self.by_judge, Mapping) else ""
            if text:
                return text
        for judge in sorted(self.by_judge):
            text = self.by_judge.get(judge, "").strip()
            if text:
                return text
        return ""


@dataclass(frozen=True)
class AsiMetadata:
    """Stage-1 surface of the ASI judge metadata.

    Production rows carry these fields under ``metadata/<judge>/<field>``
    flat keys (Trial 13 fixture corpus) plus the legacy
    ``<judge>/metadata`` nested-dict shape. The normalizer collapses
    both surfaces here. Future shape additions must land in
    :func:`_build_asi_metadata` exclusively.
    """

    blame_set: list[str] = field(default_factory=list)
    failure_type: str = ""
    counterfactual_fix: str = ""
    wrong_clause: str = ""
    rca_kind: str = ""
    severity: str = ""
    confidence: str = ""
    patch_family: str = ""


@dataclass(frozen=True)
class CanonicalEvalRow:
    """The single canonical eval-row projection consumed by every
    Stage 1+ component."""

    qid: str
    namespaced_qid: str | None
    question_text: str
    ground_truth_sql: str
    generated_sql: str
    judge_rationales: JudgeRationales
    asi_metadata: AsiMetadata
    question_source_path: str
    raw: Mapping[str, Any]

    def find_by_state_qid(self, state_qid: str) -> bool:
        """Return True if ``state_qid`` matches either the canonical
        ``qid`` or the ``namespaced_qid``.

        Used by ``optimizer._find_eval_row`` to bridge the namespacing
        drift (Track 6).
        """
        if not state_qid:
            return False
        target = str(state_qid)
        if target == self.qid:
            return True
        return self.namespaced_qid is not None and target == self.namespaced_qid

    # ─── Backwards-compat Mapping shim ─────────────────────────────
    #
    # Legacy consumers iterate the canonical row as if it were a dict
    # (``row.get(...)``, ``row["key"]``, ``dict(row)``, ``for k in
    # row``). Until every callsite is rewritten to use the typed
    # accessors above, ``CanonicalEvalRow`` delegates Mapping protocol
    # methods to the raw payload so the migration is incremental and
    # rollback-safe (per plan §Risk and rollback). The golden CI test
    # at ``tests/integration/test_eval_row_normalization_golden.py``
    # blocks new ``row.get(`` / ``row["`` callsites from sprouting
    # outside the normalizer.

    def __getitem__(self, key: str) -> Any:
        return self.raw[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.raw)

    def __len__(self) -> int:
        return len(self.raw)

    def __contains__(self, key: object) -> bool:
        return key in self.raw

    def get(self, key: str, default: Any = None) -> Any:
        return self.raw.get(key, default) if isinstance(self.raw, Mapping) else default

    def keys(self):
        return self.raw.keys()

    def values(self):
        return self.raw.values()

    def items(self):
        return self.raw.items()


Mapping.register(CanonicalEvalRow)  # type: ignore[arg-type]


# ─── Builders ───────────────────────────────────────────────────────


def _split_namespaced_qid(raw_row: Mapping[str, Any]) -> tuple[str, str | None]:
    """Return ``(canonical_qid, namespaced_qid)``.

    The canonical QID is the suffix matching the ``gs_NNN`` pattern;
    the namespaced QID preserves the domain prefix
    (``airline_ticketing_and_fare_analysis_gs_009``) when present.

    Both forms must be retained so ``find_by_state_qid`` can satisfy
    both lookup conventions without forcing every caller to know
    which one the state machine emits.
    """
    qid = row_qid(dict(raw_row))
    if not qid:
        return "", None
    if "gs_" in qid:
        idx = qid.rfind("gs_")
        canonical = qid[idx:]
        if canonical != qid:
            return canonical, qid
    return qid, None


def _build_judge_rationales(raw_row: Mapping[str, Any]) -> JudgeRationales:
    by_judge = collect_judge_rationales(dict(raw_row))
    parts: list[str] = []
    for judge in _JUDGE_RATIONALE_ORDER:
        text = by_judge.get(judge, "").strip()
        if text:
            parts.append(f"[{judge}] {text}")
    for judge in sorted(by_judge):
        if judge in _JUDGE_RATIONALE_ORDER:
            continue
        text = by_judge.get(judge, "").strip()
        if text:
            parts.append(f"[{judge}] {text}")
    return JudgeRationales(by_judge=dict(by_judge), combined="\n\n".join(parts))


def _build_asi_metadata(raw_row: Mapping[str, Any]) -> AsiMetadata:
    row_dict = dict(raw_row)
    return AsiMetadata(
        blame_set=_collect_blame_set_from_asi(row_dict),
        failure_type=_first_non_empty_asi_field(row_dict, "failure_type"),
        counterfactual_fix=_first_non_empty_asi_field(row_dict, "counterfactual_fix"),
        wrong_clause=_first_non_empty_asi_field(row_dict, "wrong_clause"),
        rca_kind=_first_non_empty_asi_field(row_dict, "rca_kind"),
        severity=_first_non_empty_asi_field(row_dict, "severity"),
        confidence=_first_non_empty_asi_field(row_dict, "confidence"),
        patch_family=_first_non_empty_asi_field(row_dict, "patch_family"),
    )


def normalize_eval_row(row: Mapping[str, Any]) -> CanonicalEvalRow:
    """Single source of truth for production row shapes.

    Every shape extension MUST land here exclusively. Adding a new
    fallback path means editing exactly one site; the golden CI test
    enforces this invariant against future regressions.
    """
    if row is None:
        row = {}
    row_dict = dict(row) if not isinstance(row, dict) else row
    canonical_qid, namespaced_qid = _split_namespaced_qid(row_dict)
    question_text, question_source_path = row_question_with_source(row_dict)
    return CanonicalEvalRow(
        qid=canonical_qid,
        namespaced_qid=namespaced_qid,
        question_text=question_text,
        ground_truth_sql=row_expected_sql(row_dict),
        generated_sql=row_generated_sql(row_dict),
        judge_rationales=_build_judge_rationales(row_dict),
        asi_metadata=_build_asi_metadata(row_dict),
        question_source_path=question_source_path,
        raw=row_dict,
    )
