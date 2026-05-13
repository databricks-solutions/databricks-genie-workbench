"""Phase 2 Action 2.1 — RepairArchetype registry.

A ``RepairArchetype`` labels the *shape* of a cluster's repair so the
Repair Planner can map a cluster onto a deterministic repair template
instead of asking the strategist to invent one. The registry is closed
— exactly five archetypes — so the strategist's prompt can enumerate
them directly.

Mapping inputs:
* ``applicable_rca_kinds`` — the ``RcaKind`` values a cluster's
  ``RCACard.root_cause`` must be in.
* ``required_grounding_tokens`` — at least one token must appear in
  the cluster's ``RCACard.grounding_terms``.
* ``evidence_predicates`` — additional named guards (e.g.
  ``exact_cardinality_required`` for the exact-N variant).

Each archetype declares a ``default_priority_step`` placing it on the
five-step priority order (``semantic_clarification`` →
``scoped_instruction`` → ``repair_kit`` →
``non_verbatim_example_pattern`` → ``narrow_l6_snippet``). The
priority can be conditionally upgraded by the propagation root cause
(see ``repair_priority.select_priority_step``).
"""

from __future__ import annotations

from dataclasses import dataclass

from genie_space_optimizer.optimization.rca import RcaKind


@dataclass(frozen=True)
class RepairArchetype:
    name: str
    applicable_rca_kinds: frozenset[RcaKind]
    required_grounding_tokens: frozenset[str]
    evidence_predicates: frozenset[str]
    default_priority_step: str
    expected_causal_effect_template: str
    rationale: str
    # Phase 2 Action 2.5 — provenance + lifecycle for in-loop archetype
    # learning. Canonical archetypes default to ``canonical`` /
    # ``stable``; provisional archetypes synthesised via the
    # archetype_learning module use ``provisional_archetype`` /
    # ``provisional`` and are promoted to ``confirmed_in_run`` when
    # their kit clears the safety + acceptance gates. Cross-run
    # promotion to canonical is offline-only.
    provenance: str = "canonical"
    lifecycle_state: str = "stable"


REPAIR_ARCHETYPES: tuple[RepairArchetype, ...] = (
    RepairArchetype(
        name="plural_top_n_collapse",
        applicable_rca_kinds=frozenset({RcaKind.TOP_N_CARDINALITY_COLLAPSE}),
        required_grounding_tokens=frozenset(),
        evidence_predicates=frozenset({"plural_question_intent"}),
        default_priority_step="repair_kit",
        expected_causal_effect_template=(
            "Replace RANK = 1 collapse with cardinality-preserving ORDER BY <metric> "
            "DESC; produces multiple rows for plural ranking questions."
        ),
        rationale=(
            "Plural questions ('list', 'rank', 'top') ask for multiple rows. "
            "The collapse to RANK = 1 produces a single row that fails "
            "result-correctness on every plural ranking qid."
        ),
    ),
    RepairArchetype(
        name="top_n_exact_cardinality",
        applicable_rca_kinds=frozenset({RcaKind.TOP_N_CARDINALITY_COLLAPSE}),
        required_grounding_tokens=frozenset(),
        evidence_predicates=frozenset({"exact_cardinality_required"}),
        default_priority_step="scoped_instruction",
        expected_causal_effect_template=(
            "Constrain LIMIT to the exact cardinality referenced in the question "
            "('Virginia stores: 2 rows'); prevents unbounded LIMIT or LIMIT 1."
        ),
        rationale=(
            "Some top-N questions name an exact cardinality. A scoped "
            "instruction tied to the qid's cardinality term is cheaper and "
            "narrower than a kit-level repair."
        ),
    ),
    RepairArchetype(
        name="default_time_window_filter",
        applicable_rca_kinds=frozenset({RcaKind.TIME_WINDOW_LOGIC_MISMATCH}),
        required_grounding_tokens=frozenset({
            "time_window", "mtd", "ytd", "qtd", "wtw",
        }),
        evidence_predicates=frozenset(),
        default_priority_step="scoped_instruction",
        expected_causal_effect_template=(
            "Add or correct the default time-window filter (mtd/ytd/qtd/wtw) "
            "for questions with implicit time scope."
        ),
        rationale=(
            "Time-window blame in the cluster + a time-window grounding token "
            "(time_window, mtd, ytd, qtd, wtw) implies the missing-filter "
            "or wrong-filter-condition is a default-window omission."
        ),
    ),
    RepairArchetype(
        name="dimension_disambiguation",
        applicable_rca_kinds=frozenset({
            RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING,
            RcaKind.METRIC_VIEW_ROUTING_CONFUSION,
        }),
        required_grounding_tokens=frozenset(),
        evidence_predicates=frozenset({"dimension_term_in_grounding"}),
        default_priority_step="semantic_clarification",
        expected_causal_effect_template=(
            "Disambiguate dimension references (e.g. market_description vs "
            "market_combination) via column synonyms + clarifying instruction."
        ),
        rationale=(
            "Synonym/entity-match failures and MV-routing confusions both "
            "stem from ambiguous dimension references. Semantic clarification "
            "is the correct first lever."
        ),
    ),
    RepairArchetype(
        name="payment_reporting_amount_semantics",
        applicable_rca_kinds=frozenset({RcaKind.MEASURE_SWAP}),
        required_grounding_tokens=frozenset({
            "payment_amt", "payment_currency_cd",
        }),
        evidence_predicates=frozenset(),
        default_priority_step="semantic_clarification",
        expected_causal_effect_template=(
            "Distinguish PAYMENT_AMT (transaction value) from "
            "PAYMENT_CURRENCY_CD (currency code) via measure-level "
            "semantic clarification."
        ),
        rationale=(
            "Measure-swap failures with payment-domain grounding indicate the "
            "model conflated amount and currency-code measures. Clarification "
            "must address both column descriptions and the measure semantics."
        ),
    ),
)


_BY_NAME: dict[str, RepairArchetype] = {a.name: a for a in REPAIR_ARCHETYPES}


def archetype_by_name(name: str) -> RepairArchetype:
    """Look up an archetype by its stable name. Raises KeyError on miss.

    The registry is closed — five entries — so any unknown name is a
    programmer error.
    """
    if name not in _BY_NAME:
        raise KeyError(
            f"Unknown repair archetype: {name!r}. Known: {sorted(_BY_NAME)}"
        )
    return _BY_NAME[name]
