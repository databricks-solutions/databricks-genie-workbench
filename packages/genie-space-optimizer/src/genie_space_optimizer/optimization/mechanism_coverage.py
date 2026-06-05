"""P4 C5 — Evidence-based mechanism-coverage check.

The d139 / e943 postmortems showed Stage 3 repeatedly emitting
single-mechanism instruction-only patches against behavior_deltas
that semantically require a structural mechanism. A purely typed
``KIT_FOR_RCA`` map would have caught this but at the cost of
re-introducing deterministic RCA rigidity — exactly what C1 backs
away from.

This module owns the *evidence-based* check instead. The predicate
reads the structured ``RepairDiagnosis.behavior_delta`` (free text)
and asks: does the proposal's mechanism set adequately address the
behavior_delta?

The behavior_delta vocabulary is open-ended (free LLM text), so the
check is *rule-driven over keyword patterns*, not enum lookups.
Categories:

  * ``RANK_ORDER_TOPN``     — behavior_delta mentions rank / order /
    top-N / limit / sort. Requires structural mechanism
    (example_sql, sql_snippet, or routing) — instruction_text alone
    is insufficient.
  * ``COLUMN_AMBIGUITY``    — behavior_delta mentions column meaning,
    synonym, ambiguous column, misinterpreted column. Allows
    metadata_description alone.
  * ``VALUE_MAPPING``       — behavior_delta mentions value mapping,
    label, enum, encoded value. Requires example_sql or sql_snippet
    (instruction-only is too weak).
  * ``JOIN_GROUNDING``      — behavior_delta mentions wrong join,
    missing join, join key. Requires metadata_join or example_sql.
  * ``OTHER``               — no recognized pattern; the check
    abstains and allows the proposal (open-vocabulary fail-open).

The LLM override path: when the producer believes its mechanism
covers the behavior_delta despite the rule-based check, it MUST emit
a non-empty ``mechanism_coverage_override_justification`` on the
proposal. The check then returns ``allowed_with_override`` and the
audit marker records the justification so postmortems can audit
overrides.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

from genie_space_optimizer.optimization.patch_mechanism import (
    PatchMechanism,
)


class BehaviorDeltaCategory(StrEnum):
    """Classifier output for free-text behavior_delta."""

    RANK_ORDER_TOPN = "rank_order_topn"
    COLUMN_AMBIGUITY = "column_ambiguity"
    VALUE_MAPPING = "value_mapping"
    JOIN_GROUNDING = "join_grounding"
    OTHER = "other"


# Keyword regexes per category. Case-insensitive, word-boundary
# anchored where possible to avoid false positives on substrings.
_RANK_ORDER_PATTERN = re.compile(
    r"\b(?:rank|order(?:ing|ed|s)?|sort(?:ing|ed|s)?|top[- ]?(?:n|\d+)|"
    r"first[- ]?\d+|limit|head|highest|lowest|largest|smallest)\b",
    re.IGNORECASE,
)
_COLUMN_AMBIGUITY_PATTERN = re.compile(
    r"\b(?:ambig\w*|misinterpret\w*|synonym\w*|column meaning|"
    r"alias\w*|same name|column name|description|description-only)\b",
    re.IGNORECASE,
)
_VALUE_MAPPING_PATTERN = re.compile(
    r"\b(?:value[- ]mapping|encoded value|enum value|label\w*|"
    r"category map\w*|status code|magic number|coded value)\b",
    re.IGNORECASE,
)
_JOIN_GROUNDING_PATTERN = re.compile(
    r"\b(?:join\w*|fk\b|foreign key|join key|missing join|"
    r"wrong join|cross join)\b",
    re.IGNORECASE,
)


def classify_behavior_delta(behavior_delta: str) -> BehaviorDeltaCategory:
    """Return the BehaviorDeltaCategory for a free-text behavior_delta.

    Precedence (most-specific-first):
      1. JOIN_GROUNDING
      2. VALUE_MAPPING
      3. RANK_ORDER_TOPN
      4. COLUMN_AMBIGUITY
      5. OTHER (fail-open)

    Multi-category deltas (e.g. "wrong join AND missing ordering")
    resolve to the most-specific match per the precedence order.
    """
    text = str(behavior_delta or "")
    if _JOIN_GROUNDING_PATTERN.search(text):
        return BehaviorDeltaCategory.JOIN_GROUNDING
    if _VALUE_MAPPING_PATTERN.search(text):
        return BehaviorDeltaCategory.VALUE_MAPPING
    if _RANK_ORDER_PATTERN.search(text):
        return BehaviorDeltaCategory.RANK_ORDER_TOPN
    if _COLUMN_AMBIGUITY_PATTERN.search(text):
        return BehaviorDeltaCategory.COLUMN_AMBIGUITY
    return BehaviorDeltaCategory.OTHER


# Per-category sets of mechanisms that adequately cover the
# behavior_delta. Subset of PatchMechanism. The OTHER category is
# fail-open (all mechanisms covered).
_ADEQUATE_MECHANISMS: dict[BehaviorDeltaCategory, frozenset[PatchMechanism]] = {
    BehaviorDeltaCategory.RANK_ORDER_TOPN: frozenset(
        {
            PatchMechanism.EXAMPLE_SQL,
            PatchMechanism.SQL_SNIPPET,
            # Routing alone (e.g. add_table) is not adequate for a
            # rank/order/top-N delta — the SQL shape change still
            # needs to be expressed somewhere.
        }
    ),
    BehaviorDeltaCategory.VALUE_MAPPING: frozenset(
        {
            PatchMechanism.EXAMPLE_SQL,
            PatchMechanism.SQL_SNIPPET,
            PatchMechanism.METADATA_DESCRIPTION,  # synonym/description
            # can teach the value mapping
        }
    ),
    BehaviorDeltaCategory.JOIN_GROUNDING: frozenset(
        {
            PatchMechanism.METADATA_JOIN,
            PatchMechanism.EXAMPLE_SQL,
            # An example_sql with the right join can teach the join
            # shape even without a join_spec edit.
        }
    ),
    BehaviorDeltaCategory.COLUMN_AMBIGUITY: frozenset(
        {
            PatchMechanism.METADATA_DESCRIPTION,
            PatchMechanism.EXAMPLE_SQL,
            PatchMechanism.SQL_SNIPPET,
            PatchMechanism.INSTRUCTION_TEXT,  # instructions can teach
            # column meaning
        }
    ),
    BehaviorDeltaCategory.OTHER: frozenset(
        {m for m in PatchMechanism}
    ),
}


def adequate_mechanisms_for_category(
    category: BehaviorDeltaCategory,
) -> frozenset[PatchMechanism]:
    """Return the mechanism set that adequately covers ``category``."""
    return _ADEQUATE_MECHANISMS[category]


@dataclass(frozen=True, slots=True)
class MechanismCoverageVerdict:
    """Outcome of :func:`check_mechanism_coverage`.

    ``outcome`` ∈ {``"covered"``, ``"override"``, ``"uncovered"``}.

      * ``"covered"``     — at least one proposed mechanism is in the
        adequate set for the behavior_delta's category.
      * ``"override"``    — coverage check failed but the proposal
        carries a non-empty
        ``mechanism_coverage_override_justification``. The audit
        marker records the override; downstream gates still apply.
      * ``"uncovered"``   — coverage failed AND no override
        justification was provided. Caller MUST decline the proposal
        with abstain ``mechanism_does_not_cover_behavior_delta``.

    ``inferred_category`` is the classification of the behavior_delta
    that drove the decision; postmortems use this to audit drift
    between behavior_delta phrasing and the rule-based classifier.
    """

    outcome: str
    inferred_category: BehaviorDeltaCategory
    proposed_mechanisms: tuple[PatchMechanism, ...]
    adequate_mechanisms: tuple[PatchMechanism, ...]
    override_justification: str
    feedback: str


def check_mechanism_coverage(
    *,
    behavior_delta: str,
    proposed_mechanisms: tuple[PatchMechanism, ...],
    mechanism_coverage_override_justification: str = "",
) -> MechanismCoverageVerdict:
    """Return the coverage verdict for one proposal.

    Args:
      behavior_delta: free-text behavior_delta from the C1
        RepairDiagnosis.
      proposed_mechanisms: the mechanisms this proposal includes.
        For single-lever proposals this tuple has length 1; for
        multi-lever proposals it may be longer.
      mechanism_coverage_override_justification: optional LLM-authored
        free-text override. When non-empty AND coverage fails, the
        verdict is ``"override"`` (not ``"uncovered"``).

    The check fail-opens on the ``OTHER`` category (no recognized
    pattern → all mechanisms covered) so the open-vocabulary
    behavior_delta does not become a permanent block.
    """
    category = classify_behavior_delta(behavior_delta)
    adequate = _ADEQUATE_MECHANISMS[category]
    intersection = frozenset(proposed_mechanisms) & adequate
    if intersection:
        return MechanismCoverageVerdict(
            outcome="covered",
            inferred_category=category,
            proposed_mechanisms=tuple(proposed_mechanisms),
            adequate_mechanisms=tuple(sorted(adequate, key=lambda m: m.value)),
            override_justification="",
            feedback="",
        )
    # Coverage failed.
    override = str(mechanism_coverage_override_justification or "").strip()
    if override:
        return MechanismCoverageVerdict(
            outcome="override",
            inferred_category=category,
            proposed_mechanisms=tuple(proposed_mechanisms),
            adequate_mechanisms=tuple(sorted(adequate, key=lambda m: m.value)),
            override_justification=override,
            feedback=(
                f"coverage check would block but caller supplied "
                f"override justification ({len(override)} chars)"
            ),
        )
    return MechanismCoverageVerdict(
        outcome="uncovered",
        inferred_category=category,
        proposed_mechanisms=tuple(proposed_mechanisms),
        adequate_mechanisms=tuple(sorted(adequate, key=lambda m: m.value)),
        override_justification="",
        feedback=(
            f"behavior_delta category {category.value!r} requires one of "
            f"{[m.value for m in sorted(adequate, key=lambda m: m.value)]}; "
            f"proposed mechanisms "
            f"{[m.value for m in proposed_mechanisms]} cover none of them. "
            f"Either add an adequate mechanism or supply a non-empty "
            f"mechanism_coverage_override_justification."
        ),
    )


def mechanism_coverage_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    qid: str,
    behavior_delta: str,
    verdict: MechanismCoverageVerdict,
) -> str:
    """Return one ``GSO_MECHANISM_COVERAGE_V1`` marker line."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        marker_line,
    )

    return marker_line(
        "GSO_MECHANISM_COVERAGE_V1",
        {
            "optimization_run_id": str(optimization_run_id),
            "iteration": int(iteration),
            "qid": str(qid),
            "behavior_delta": str(behavior_delta),
            "inferred_category": verdict.inferred_category.value,
            "proposed_mechanisms": [m.value for m in verdict.proposed_mechanisms],
            "adequate_mechanisms": [m.value for m in verdict.adequate_mechanisms],
            "outcome": verdict.outcome,
            "override_justification": verdict.override_justification,
            "feedback": verdict.feedback,
        },
    )
