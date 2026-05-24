"""Local pre-flight validation of the Stage 1 evidence card the
Plan 11 diagnosis LLM consumes.

Background
----------
Trial 11 (runs 98ec8950 + dc89d1a9, commit 3ee01b81) put Stage 1 on
the LLM with real tokens for the first time. 55/55 hard QIDs declined
with ``abstain_reason=missing_schema_context`` and
``evidence_summary_chars=0``. The LLM was correct: the input card was
empty. Three Stage 1 input builders had been hydrating fields with
flat ``row.get(...)`` against production rows that carry data under
``inputs/...``, ``inputs.*``, or ``request.kwargs.*``, and hardcoded
empty ``rca_evidence.*`` / ``blame_set_seed`` regardless of upstream
evidence.

This module is symmetric to
:mod:`databricks_request_contract` — that contract sits at the
wire-boundary (provider request envelope); this contract sits at the
input-boundary (LLM evidence card). Both fail-closed before the LLM
is invoked so postmortems see the cause directly instead of a
generic ``missing_schema_context`` decline.

Design
------
``Stage1InputEvidenceContract.validate(card)`` returns a
``list[ConstraintViolation]``. Empty list ⇒ ok-to-dispatch. Non-empty
⇒ the caller MUST short-circuit Stage 1 with a typed
``Stage1InputCardEmptyError(violations)`` and emit
``GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1``; the QID terminates with
``OPTIMIZER_NO_CANDIDATES reason="abstain: evidence_card_empty:..."``.

Two consumers:

  1. **CI golden test**
     (``tests/integration/test_stage1_input_evidence_contract_golden.py``)
     — enumerates every Stage 1 input builder by direct import,
     feeds each a hydrated fixture row, asserts
     ``validate(card) == []`` for every (builder, fixture-row)
     pair. Catches future hydration regressions at PR time.
  2. **Runtime pre-flight**
     (``state_machine/transformers/diagnose_llm._invoke_stage1_llm``)
     — same module, same defaults. Defense in depth: if any future
     Stage 1 caller bypasses CI, the runtime check still skips the
     LLM call before tokens are burned.

Reference plan: ``docs/llmdrivenarchitecture/v5/
stage1-semantic-hydration-and-projection-parity_a4d7c2e1.plan.md``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence


@dataclass(frozen=True)
class ConstraintViolation:
    """One Stage 1 input-evidence contract failure.

    ``field`` is a stable identifier tag (e.g. ``"question_text_empty"``)
    that the postmortem skill greps for.

    ``value`` is the actual value the caller would have sent; long
    string values truncate to 200 chars defensively.

    ``constraint`` is a human-readable description of the rule that
    fired.
    """

    field: str
    value: Any
    constraint: str

    def __post_init__(self) -> None:  # noqa: D401 — dataclass hook
        if isinstance(self.value, str) and len(self.value) > 200:
            object.__setattr__(self, "value", self.value[:200] + "...")


class Stage1InputCardEmptyError(Exception):
    """Raised when ``Stage1InputEvidenceContract.validate(card)``
    returns a non-empty list of violations.

    The structured ``violations`` attribute is what the marker emitter
    serialises into the ``violations`` field of
    ``GSO_PLAN11_STAGE1_INPUT_CARD_EMPTY_V1``. The colon-joined
    rendered text feeds the typed ``declined`` string the
    abstain-aware transformer routes to ``OPTIMIZER_NO_CANDIDATES``.
    """

    def __init__(self, violations: list[ConstraintViolation]) -> None:
        self.violations = list(violations)
        rendered = ", ".join(v.field for v in self.violations)
        super().__init__(
            f"Stage 1 evidence card is empty: {rendered}"
        )

    def as_declined_reason(self) -> str:
        return "evidence_card_empty:" + ",".join(
            v.field for v in self.violations
        )


@dataclass(frozen=True)
class Stage1InputEvidenceContract:
    """Configurable bundle of Stage 1 input-evidence requirements.

    Defaults match the minimum information the Plan 11 Stage 1
    diagnose prompt needs to avoid declining with
    ``missing_schema_context``:

    * ``question_text`` non-empty (the LLM needs to know what was asked)
    * ``ground_truth_sql`` OR ``judge_rationale`` non-empty
      (one narrative grounding the failure analysis)
    * ``generated_sql`` non-empty (the artefact being diagnosed)
    * ``blame_set_seed`` non-empty
      (which objects the LLM should focus on)
    * ``rca_evidence`` carries at least one non-empty subfield
      (the upstream RCA hint, even if shallow)
    """

    min_blame_set_size: int = 1
    # Trial 13i — run-level ``schema_columns`` must carry at least N
    # 4-part FQN entries (``catalog.schema.table.column``) before the
    # Stage 1 LLM is invoked. Trial 13h tightened the prompt so the LLM
    # declines with ``insufficient_blame_set`` whenever
    # ``schema_columns`` is empty; the post-13h replay showed every
    # SM-lane Stage 1 call sent ``[]`` because the field was never
    # plumbed. The pre-flight check below short-circuits those calls
    # with a typed ``missing_schema_columns`` reason instead of burning
    # tokens on a guaranteed decline.
    schema_columns_min_size: int = 1
    rca_evidence_required_subfields: tuple[str, ...] = (
        "observed_failure",
        "generated_sql_issue",
        "expected_sql_shape",
        "suggested_repair_family",
    )

    def validate(
        self, card: dict[str, Any]
    ) -> list[ConstraintViolation]:
        """Return every violation a single Stage 1 input card would
        trigger when handed to ``diagnose_failing_qids``. Empty list
        ⇒ ok-to-dispatch. Aggregating rather than failing-fast is
        intentional: postmortems see every failing rule at once.
        """
        violations: list[ConstraintViolation] = []

        question_text = str(card.get("question_text") or "").strip()
        if not question_text:
            violations.append(
                ConstraintViolation(
                    field="question_text_empty",
                    value=card.get("question_text"),
                    constraint="question_text must be non-empty",
                )
            )

        ground_truth_sql = str(card.get("ground_truth_sql") or "").strip()
        judge_rationale = str(card.get("judge_rationale") or "").strip()
        if not ground_truth_sql and not judge_rationale:
            violations.append(
                ConstraintViolation(
                    field="expected_sql_or_judge_rationale_empty",
                    value={
                        "ground_truth_sql": card.get("ground_truth_sql"),
                        "judge_rationale": card.get("judge_rationale"),
                    },
                    constraint=(
                        "at least one of ground_truth_sql or "
                        "judge_rationale must be non-empty"
                    ),
                )
            )

        generated_sql = str(card.get("generated_sql") or "").strip()
        if not generated_sql:
            violations.append(
                ConstraintViolation(
                    field="generated_sql_empty",
                    value=card.get("generated_sql"),
                    constraint="generated_sql must be non-empty",
                )
            )

        blame_set = card.get("blame_set_seed") or ()
        blame_size = sum(1 for b in blame_set if str(b or "").strip())
        if blame_size < self.min_blame_set_size:
            # Three-way classification of "Stage 1 has no usable
            # blame_set_seed":
            #
            #  1. Trial 14 ``seeds_all_filter_kind`` — the typed
            #     structured payload (``_blame_structured``) was
            #     non-empty but *every* entry was ``filter`` /
            #     ``instruction`` (non-schema-resolvable). The ASI
            #     judges identified behaviour-level blame
            #     (predicates, business rules) and Stage 1 cannot
            #     patch without a schema target. Triage points at
            #     judge output quality vs schema_columns coverage
            #     in a different direction than 13k's
            #     ``seeds_unnormalizable``.
            #  2. Trial 13k ``seeds_unnormalizable`` — legacy seeds
            #     existed (``seeds_pre_normalize > 0``) but the FQN
            #     resolver dropped them. Either the seeds are
            #     compound text (now caught by case 1 once judges
            #     emit structured) or schema_columns coverage is
            #     incomplete.
            #  3. Trial 11 legacy ``blame_set_empty`` — no seeds
            #     and no structured payload at all (the upstream
            #     judges emitted nothing).
            blame_structured = card.get("_blame_structured") or ()
            structured_kinds = [
                str(e.get("kind") or "").strip().lower()
                for e in blame_structured
                if isinstance(e, dict)
            ]
            structured_kinds = [k for k in structured_kinds if k]
            schema_kinds_present = any(
                k in ("column", "table", "join") for k in structured_kinds
            )
            if structured_kinds and not schema_kinds_present:
                # Case 1: structured payload exists but is entirely
                # non-schema (all filter/instruction). This is the
                # Trial 14 canary signal — the upstream judges are
                # identifying behaviour-level blame.
                kind_counts: dict[str, int] = {}
                for k in structured_kinds:
                    kind_counts[k] = kind_counts.get(k, 0) + 1
                violations.append(
                    ConstraintViolation(
                        field="seeds_all_filter_kind",
                        value={
                            "blame_kind_distribution": kind_counts,
                            "structured_entries": list(blame_structured)[:8],
                        },
                        constraint=(
                            "blame_set_seed is empty because every "
                            "entry in blame_set_structured has "
                            "kind in {filter, instruction} — no "
                            "schema-resolvable column/table/join "
                            "blame. Triage: the upstream ASI judges "
                            "are identifying behaviour-level blame "
                            "(SQL predicates / business rules); "
                            "Stage 1 cannot patch without a schema "
                            "target. Investigate the judge that "
                            "emitted only filter/instruction kinds."
                        ),
                    )
                )
            else:
                seed_norm = card.get("_seed_normalization") or {}
                try:
                    seeds_pre_normalize = int(
                        seed_norm.get("seeds_pre_normalize") or 0
                    )
                except (TypeError, ValueError):
                    seeds_pre_normalize = 0
                if seeds_pre_normalize > 0:
                    try:
                        seeds_dropped = int(seed_norm.get("seeds_dropped") or 0)
                    except (TypeError, ValueError):
                        seeds_dropped = 0
                    violations.append(
                        ConstraintViolation(
                            field="seeds_unnormalizable",
                            value={
                                "seeds_pre_normalize": seeds_pre_normalize,
                                "seeds_dropped": seeds_dropped,
                            },
                            constraint=(
                                "blame_set_seed is empty after FQN "
                                "normalization; "
                                f"{seeds_pre_normalize} ASI seed(s) were "
                                "dropped because none matched a 4-part FQN in "
                                "schema_columns. Triage: blame_set seed "
                                "quality (compound text vs identifier "
                                "tokens) or schema_columns coverage."
                            ),
                        )
                    )
                else:
                    violations.append(
                        ConstraintViolation(
                            field="blame_set_empty",
                            value=(
                                list(blame_set) if blame_set is not None else None
                            ),
                            constraint=(
                                f"blame_set_seed must carry ≥ "
                                f"{self.min_blame_set_size} non-empty entries"
                            ),
                        )
                    )

        rca = card.get("rca_evidence") or {}
        if not isinstance(rca, dict) or not any(
            str(rca.get(k) or "").strip()
            for k in self.rca_evidence_required_subfields
        ):
            violations.append(
                ConstraintViolation(
                    field="rca_evidence_empty",
                    value=rca if isinstance(rca, dict) else None,
                    constraint=(
                        "rca_evidence must carry at least one non-empty "
                        f"subfield in {self.rca_evidence_required_subfields}"
                    ),
                )
            )

        return violations

    def validate_schema_columns(
        self, schema_columns: Sequence[str] | None,
    ) -> list[ConstraintViolation]:
        """Pre-flight check for the run-level ``schema_columns`` channel.

        Returns ``[]`` if the channel carries at least
        ``schema_columns_min_size`` non-empty entries, else a single
        :class:`ConstraintViolation` tagged ``"missing_schema_columns"``.

        Symmetric to :meth:`validate` (per-card) but lifted to the
        per-iteration scope: ``schema_columns`` is the same list passed
        to every QID's Stage 1 call, so checking it once before LLM
        dispatch avoids redundant work and gives postmortems a single
        violation marker per iteration rather than one per QID.

        Callers should short-circuit Stage 1 with a typed
        :class:`Stage1InputCardEmptyError` carrying the violation when
        the result is non-empty; this matches the per-card pathway in
        :meth:`validate`. The declined reason
        (:meth:`Stage1InputCardEmptyError.as_declined_reason`)
        renders as ``"evidence_card_empty:missing_schema_columns"`` —
        downstream consumers grep for the field tag, not the rendered
        prefix.
        """
        violations: list[ConstraintViolation] = []
        actual = [
            str(s).strip()
            for s in (schema_columns or ())
            if str(s or "").strip()
        ]
        if len(actual) < self.schema_columns_min_size:
            violations.append(
                ConstraintViolation(
                    field="missing_schema_columns",
                    value=list(actual),
                    constraint=(
                        f"schema_columns must carry ≥ "
                        f"{self.schema_columns_min_size} non-empty entries "
                        f"(catalog.schema.table.column FQNs)"
                    ),
                )
            )
        return violations

    def field_sources(self, card: dict[str, Any]) -> dict[str, str]:
        """Per-field provenance tag — useful for marker rendering.

        Returns a per-field string per top-level field the contract
        checks. Strings are:

        * the originating row path (e.g. ``"request.question"``,
          ``"inputs/question"``) when the card was built by
          :func:`eval_row_access.build_stage1_evidence_card` and the
          builder recorded an explicit source path for that field.
        * the literal ``"present"`` when the value is populated but
          no source path is recorded (e.g. hand-built test cards,
          typed-evidence override paths).
        * the literal ``"absent"`` when the value is empty.

        Backwards-compatibility: legacy readers comparing the value
        against the literal ``"present"`` continue to work for
        unattributed cards; readers that need the row path receive it
        verbatim. Postmortem markers should always quote the verbatim
        string so the next regression names the production-row path
        that drifted.
        """
        present = lambda v: bool(str(v or "").strip()) if not isinstance(
            v, (list, tuple)
        ) else bool(v)
        source_paths_raw = card.get("_source_paths")
        source_paths: dict[str, str] = (
            {str(k): str(v) for k, v in source_paths_raw.items()}
            if isinstance(source_paths_raw, dict)
            else {}
        )

        def _source_for(field: str, is_present: bool) -> str:
            if not is_present:
                return "absent"
            explicit = source_paths.get(field, "")
            if explicit and explicit != "absent":
                return explicit
            return "present"

        return {
            "question_text": _source_for(
                "question_text", present(card.get("question_text"))
            ),
            "ground_truth_sql": _source_for(
                "ground_truth_sql", present(card.get("ground_truth_sql"))
            ),
            "generated_sql": _source_for(
                "generated_sql", present(card.get("generated_sql"))
            ),
            "judge_rationale": _source_for(
                "judge_rationale", present(card.get("judge_rationale"))
            ),
            "blame_set_seed": _source_for(
                "blame_set_seed",
                bool(
                    card.get("blame_set_seed")
                    and any(
                        str(b or "").strip()
                        for b in card.get("blame_set_seed") or ()
                    )
                ),
            ),
            "rca_evidence": _source_for(
                "rca_evidence",
                bool(
                    isinstance(card.get("rca_evidence"), dict)
                    and any(
                        str(card.get("rca_evidence", {}).get(k) or "").strip()
                        for k in self.rca_evidence_required_subfields
                    )
                ),
            ),
        }


# Singleton instance every Stage 1 caller imports. Defaults are
# pinned by ``test_contract_is_frozen_with_stable_required_fields``;
# narrowing any requirement requires updating that test in lockstep.
DEFAULT_STAGE1_CONTRACT: Stage1InputEvidenceContract = (
    Stage1InputEvidenceContract()
)


__all__ = [
    "ConstraintViolation",
    "DEFAULT_STAGE1_CONTRACT",
    "Stage1InputCardEmptyError",
    "Stage1InputEvidenceContract",
]
