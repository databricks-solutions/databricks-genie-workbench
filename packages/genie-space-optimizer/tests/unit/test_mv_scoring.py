"""Unit tests for the metric view advisor scoring engine (Prompt 4).

The two POV Part 3 worked examples are asserted with ``==`` rather than
``approx``: both land on exact IEEE doubles, and a tolerance here would hide the
one class of regression these tests exist to catch — a weight or blend edit that
silently moves every score a fraction.
"""

from __future__ import annotations

import ast
import dataclasses
import json
import math
from collections.abc import Sequence

import pytest

from genie_space_optimizer.common import config
from genie_space_optimizer.optimization import mv_scoring
from genie_space_optimizer.optimization.mv_fingerprint import canonicalize_expr
from genie_space_optimizer.optimization.mv_scoring import (
    DemandSignal,
    InstructionDefinition,
    LineageOverlap,
    MetricViewCandidate,
    MetricViewField,
    RecurrenceSignal,
    ScoreComponents,
    SourceColumnMetadata,
    blended_score,
    dedup_gate,
    demand_decay,
    demand_score,
    lineage_overlap_score,
    metric_view_fields,
    normalized_recurrence,
    persist_proposal,
    score_candidate,
    score_candidates,
    semantic_reference_for,
    semantic_score,
    suggestion_id_for,
    syntactic_score,
    tier_for,
    trusted_asset_definitions,
)

GOVERNED_FIELDS = mv_scoring.SEMANTIC_REF_GOVERNED_MV_FIELDS
COLUMN_METADATA = mv_scoring.SEMANTIC_REF_SOURCE_COLUMN_METADATA

DISCOUNTED_REVENUE = "SUM(l_extendedprice * (1 - l_discount))"
LIST_REVENUE = "SUM(l_extendedprice)"
LINEITEM = "samples.tpch.lineitem"
ORDERS = "samples.tpch.orders"
SPACE_ID = "01ef_genie"
GOVERNED_MV = "finance.sales.revenue_metrics"

# A benchmark question. Its text must never appear in a proposal payload.
BENCHMARK_TEXT = "What was discounted revenue by region last quarter?"

SOURCE_COLUMNS = (
    SourceColumnMetadata(table=LINEITEM, column="l_extendedprice", comment="Extended price"),
    SourceColumnMetadata(table=LINEITEM, column="l_discount"),
)


# ── Fakes ────────────────────────────────────────────────────────────────


class FakeEmbeddingClient:
    """Deterministic embedding stand-in.

    Vectors are intentionally un-normalized (``(3, 4, 0)`` has norm 5) so any
    test passing through it also proves the scorer normalizes in our own code
    rather than trusting the endpoint — the GTE-vs-BGE difference the prod
    adapter exists to absorb.
    """

    def __init__(self, vectors: dict[str, Sequence[float]] | None = None) -> None:
        self.vectors = dict(vectors or {})
        self.default = (0.0, 0.0, 1.0)
        self.calls: list[list[str]] = []

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        return [list(self.vectors.get(text, self.default)) for text in texts]


class UpsertSpy:
    """Captures the kwargs ``persist_proposal`` hands the Prompt 1 accessor."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def __call__(self, spark, **kwargs):
        self.calls.append(kwargs)
        return kwargs["dedup_fingerprint"]


def governed_revenue_yaml() -> dict[str, dict]:
    return {
        GOVERNED_MV: {
            "source": LINEITEM,
            "measures": [
                {
                    "name": "discounted_revenue",
                    "expr": DISCOUNTED_REVENUE,
                    "display_name": "Discounted Revenue",
                    "comment": "Governed discounted revenue for line items",
                    "synonyms": ["revenue", "net revenue"],
                }
            ],
            "dimensions": [
                {"name": "order_status", "expr": "orders.o_orderstatus"},
            ],
        }
    }


def candidate(**overrides) -> MetricViewCandidate:
    base = {
        "space_id": SPACE_ID,
        "candidate_type": "NEW_METRIC_VIEW",
        "measure_expr": DISCOUNTED_REVENUE,
        "source_tables": (LINEITEM, ORDERS),
        "concept": "revenue",
        "proposed_object": "finance.sales.discounted_revenue_metrics",
        "benchmark_question_ids": ("bmk_12", "bmk_31"),
        "query_history_statement_ids": ("stmt_a1", "stmt_b7"),
    }
    base.update(overrides)
    return MetricViewCandidate(**base)


# ── POV Part 3 worked examples (exact values) ────────────────────────────


def test_worked_example_one_un_governed_revenue_scores_exactly_80_high() -> None:
    """POV Part 3: L=0.90, Y=0.95, S=0.40, D=0.80 -> 80.0 -> High."""
    components = ScoreComponents(L=0.90, Y=0.95, S=0.40, D=0.80)

    assert blended_score(components) == 80.0
    assert tier_for(blended_score(components)) == mv_scoring.TIER_HIGH
    assert components.weighted_terms() == {"L": 0.315, "Y": 0.285, "S": 0.08, "D": 0.12}
    assert components.to_dict()["weights"] == {"L": 0.35, "Y": 0.30, "S": 0.20, "D": 0.15}


def test_worked_example_two_raw_table_to_existing_mv_scores_exactly_58_75_medium() -> None:
    """POV Part 3: L=0.95, Y=0.20, S=0.75, D=0.30 -> 58.75 -> Medium."""
    components = ScoreComponents(L=0.95, Y=0.20, S=0.75, D=0.30)

    assert blended_score(components) == 58.75
    assert tier_for(blended_score(components)) == mv_scoring.TIER_MEDIUM
    assert components.weighted_terms() == {"L": 0.3325, "Y": 0.06, "S": 0.15, "D": 0.045}


def test_the_pinned_worked_examples_are_untouched_by_renormalization() -> None:
    """MV-D15 clause 5, made executable.

    Both POV Part 3 examples supply all four components, so all four are
    ``COMPUTED``, coverage is exactly 1.0, and the divisor is a no-op. The point
    of asserting coverage and the cap flag alongside the two scores is that a
    future change to the renormalization cannot move 80.0 or 58.75 without
    failing here — and cannot reach them by rounding back to them either, since
    ``==`` on the exact IEEE double is what is asserted.
    """
    for values, expected_score, expected_tier in (
        ({"L": 0.90, "Y": 0.95, "S": 0.40, "D": 0.80}, 80.0, mv_scoring.TIER_HIGH),
        ({"L": 0.95, "Y": 0.20, "S": 0.75, "D": 0.30}, 58.75, mv_scoring.TIER_MEDIUM),
    ):
        components = ScoreComponents(**values)

        assert components.evidence_coverage == 1.0
        assert all(
            components.status_of(key) == config.MV_SIGNAL_COMPUTED
            for key in ("L", "Y", "S", "D")
        )
        assert blended_score(components) == expected_score

        decision = mv_scoring.capped_tier(expected_score, components.evidence_coverage)
        assert decision.tier == expected_tier
        assert decision.uncapped_tier == expected_tier
        assert decision.capped_by_coverage is False


def test_worked_example_component_inputs_reproduce_their_stated_values() -> None:
    """The blend is pinned above; this pins the curves that feed it.

    POV states L=0.90 for the first example, Y=0.95 "seen 60x", and D=0.80.
    """
    nine_of_ten = LineageOverlap(
        candidate_columns=frozenset(f"c{i}" for i in range(9)),
        reference_columns=frozenset(f"c{i}" for i in range(10)),
    )
    assert lineage_overlap_score(nine_of_ten) == 0.90

    seen_sixty = RecurrenceSignal(recurrence=60, ast_equivalent=True)
    assert round(syntactic_score(seen_sixty), 2) == 0.95

    busy = DemandSignal(frequency=80, cost_ms=2_880_000, distinct_users=8, age_days=0.0)
    assert demand_score(busy) == pytest.approx(0.80)


def test_weights_come_from_config_not_code() -> None:
    """Retuning a weight must move the score without touching this module."""
    components = ScoreComponents(
        L=1.0, Y=0.0, S=0.0, D=0.0, weights={"L": 1.0, "Y": 0.0, "S": 0.0, "D": 0.0}
    )
    assert blended_score(components) == 100.0
    assert sum(config.MV_SCORE_WEIGHTS.values()) == pytest.approx(1.0)


# ── Tiers ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("score", "expected"),
    [
        (100.0, mv_scoring.TIER_HIGH),
        (75.0, mv_scoring.TIER_HIGH),
        (74.999, mv_scoring.TIER_MEDIUM),
        (50.0, mv_scoring.TIER_MEDIUM),
        (49.999, mv_scoring.TIER_LOW),
        (25.0, mv_scoring.TIER_LOW),
        (24.999, None),
        (0.0, None),
    ],
)
def test_tier_boundaries_are_inclusive_lower_bounds(score: float, expected: str | None) -> None:
    assert tier_for(score) == expected


def test_blend_is_not_rounded_so_the_suppression_floor_holds() -> None:
    """Rounding to two places would drag 24.999 into LOW — a rounding rule that
    changes which candidates reach a human is not a presentation detail."""
    assert tier_for(24.999) is None
    assert round(24.999, 2) == 25.0


# ── MV-D15 signal availability, renormalization, coverage caps ────────────


def test_an_unavailable_signal_does_not_score_zero() -> None:
    """The distinction MV-D15 exists for, stated as arithmetic.

    Same three measured signals; the only difference is whether the fourth is
    reported as never-measured or measured-at-zero. Scoring them identically
    would make a candidate nobody could gather lineage for indistinguishable
    from one whose lineage was gathered and found disjoint.
    """
    measured = {"L": 1.0, "Y": 1.0, "S": 1.0}

    unavailable = ScoreComponents(**measured, D=0.0, statuses={"D": config.MV_SIGNAL_UNAVAILABLE})
    assert unavailable.evidence_coverage == 0.85
    assert blended_score(unavailable) == 100.0

    empty = ScoreComponents(**measured, D=0.0, statuses={"D": config.MV_SIGNAL_EMPTY})
    assert empty.evidence_coverage == 1.0
    assert blended_score(empty) == 85.0


def test_an_unavailable_signal_cannot_smuggle_a_value_into_the_blend() -> None:
    """A value carried alongside an ``UNAVAILABLE`` status is ignored.

    Belt and braces: nothing should populate a component it also marks
    unavailable, but if a producer regresses and leaves a stale number behind,
    the status governs rather than the number. Otherwise the divisor shrinks
    while the numerator keeps the value, which inflates rather than renormalizes.
    """
    stale = ScoreComponents(
        L=0.9, Y=1.0, S=0.0, D=0.0,
        statuses={"L": config.MV_SIGNAL_UNAVAILABLE, "D": config.MV_SIGNAL_UNAVAILABLE},
    )

    assert stale.value_of("L") == 0.0
    assert stale.evidence_coverage == 0.50
    assert blended_score(stale) == 60.0
    assert stale.weighted_terms()["L"] == 0.0


def test_a_computed_y_only_candidate_renormalizes_and_is_low_capped() -> None:
    """The single-signal case renormalization alone would misreport.

    Y=0.90 on its own renormalizes to 90.0, which reads as a HIGH-tier finding
    and would outrank a fully-corroborated 78. Coverage 0.30 caps it at LOW and
    records both tiers, so the ranking reflects how much was measured rather
    than only what the one measurement said.
    """
    only_y = ScoreComponents(
        Y=0.90,
        statuses={
            "L": config.MV_SIGNAL_UNAVAILABLE,
            "S": config.MV_SIGNAL_UNAVAILABLE,
            "D": config.MV_SIGNAL_UNAVAILABLE,
        },
    )

    assert only_y.evidence_coverage == 0.30
    assert blended_score(only_y) == pytest.approx(90.0)

    decision = mv_scoring.capped_tier(blended_score(only_y), only_y.evidence_coverage)
    assert decision.tier == mv_scoring.TIER_LOW
    assert decision.uncapped_tier == mv_scoring.TIER_HIGH
    assert decision.capped_by_coverage is True
    assert decision.to_dict()["tier_capped_by_coverage"] is True


def test_todays_coverage_holds_everything_at_or_below_medium() -> None:
    """MV-D15's legibility claim: with no L producer, HIGH is out of reach.

    0.50 is today (L and D unavailable), 0.65 is a partial 6a that lands D, and
    both ceiling at MEDIUM because L alone is 0.35 and HIGH needs 0.80. This is
    the assertion that turns "nothing can exceed MEDIUM until 6a" from a claim
    in the decision note into something a regression would break.
    """
    todays = ScoreComponents(
        Y=1.0, S=1.0,
        statuses={"L": config.MV_SIGNAL_UNAVAILABLE, "D": config.MV_SIGNAL_UNAVAILABLE},
    )
    assert todays.evidence_coverage == 0.50
    assert mv_scoring.coverage_ceiling(todays.evidence_coverage) == mv_scoring.TIER_MEDIUM

    after_partial_6a = ScoreComponents(
        Y=1.0, S=1.0, D=1.0, statuses={"L": config.MV_SIGNAL_UNAVAILABLE}
    )
    assert after_partial_6a.evidence_coverage == 0.65
    assert mv_scoring.coverage_ceiling(0.65) == mv_scoring.TIER_MEDIUM

    # A perfect score on either still cannot present as HIGH.
    for components in (todays, after_partial_6a):
        decision = mv_scoring.capped_tier(100.0, components.evidence_coverage)
        assert decision.tier == mv_scoring.TIER_MEDIUM
        assert decision.capped_by_coverage is True


def test_the_high_coverage_boundary_survives_its_floating_point_seam() -> None:
    """The S-unavailable case sums to 0.7999999999999999 unrounded.

    That is the most common real coverage value — every candidate in a workspace
    with no embedding endpoint — and it sits exactly on
    ``MV_COVERAGE_HIGH_MIN``. Unrounded it fails ``>=`` by one ULP and HIGH
    becomes unreachable for the whole class by accident. Pinned here because the
    failure is invisible in a payload that prints ``0.8``.
    """
    assert 0.35 + 0.30 + 0.15 < 0.80  # the seam is real, not hypothetical

    no_embeddings = ScoreComponents(
        L=1.0, Y=1.0, D=1.0, statuses={"S": config.MV_SIGNAL_UNAVAILABLE}
    )
    assert no_embeddings.evidence_coverage == 0.80
    assert mv_scoring.coverage_ceiling(no_embeddings.evidence_coverage) == mv_scoring.TIER_HIGH


def test_coverage_ceiling_never_suppresses() -> None:
    """Coverage bounds a tier from above; suppression is the score's call.

    A candidate with almost no coverage still reports LOW rather than None, so
    "not enough evidence to rank confidently" stays distinguishable from "scored
    too low to be worth a reviewer's time."
    """
    assert mv_scoring.coverage_ceiling(0.0) == mv_scoring.TIER_LOW
    assert mv_scoring.capped_tier(10.0, 1.0).tier is None
    assert mv_scoring.capped_tier(10.0, 0.0).tier is None


def test_a_candidate_with_no_coverage_scores_zero_rather_than_dividing_by_zero() -> None:
    nothing = ScoreComponents(
        L=1.0, Y=1.0, S=1.0, D=1.0,
        statuses={key: config.MV_SIGNAL_UNAVAILABLE for key in ("L", "Y", "S", "D")},
    )

    assert nothing.evidence_coverage == 0.0
    assert blended_score(nothing) == 0.0
    assert mv_scoring.capped_tier(0.0, 0.0).tier is None


def test_a_caller_naming_one_status_does_not_reset_the_others() -> None:
    """Partial maps are the normal case, so the merge direction matters.

    ``score_candidate`` derives S from the embedding attempt and takes L/Y/D from
    the caller. A caller marking L unavailable must not thereby overwrite the S
    status the module just determined, or the endpoint's own report is lost.
    """
    client = FakeEmbeddingClient({"revenue": (1.0, 0.0, 0.0), "l discount": (1.0, 0.0, 0.0)})
    proposal = score_candidate(
        strong_candidate(source_column_metadata=SOURCE_COLUMNS),
        run_id="r1",
        intent_texts=("revenue",),
        embedding_client=client,
        statuses={"L": config.MV_SIGNAL_UNAVAILABLE},
    )

    assert proposal.components.status_of("L") == config.MV_SIGNAL_UNAVAILABLE
    assert proposal.components.status_of("S") == config.MV_SIGNAL_COMPUTED
    assert proposal.components.status_of("Y") == config.MV_SIGNAL_COMPUTED


# ── L ────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        ({"a", "b"}, {"a", "b"}, 1.0),
        ({"a", "b"}, {"c", "d"}, 0.0),
        ({"a", "b"}, {"b", "c"}, 1 / 3),
        (set(), {"a"}, 0.0),
        ({"a"}, set(), 0.0),
        (set(), set(), 0.0),
    ],
)
def test_lineage_overlap_is_jaccard_and_empty_means_no_evidence(
    left: set, right: set, expected: float
) -> None:
    overlap = LineageOverlap(
        candidate_columns=frozenset(left), reference_columns=frozenset(right)
    )
    assert lineage_overlap_score(overlap) == pytest.approx(expected)


def test_lineage_reference_kind_travels_into_evidence() -> None:
    """A score of 0.9 means different things for the two reference kinds, so the
    kind is recorded rather than inferred."""
    proposal = score_candidate(
        candidate(
            lineage=LineageOverlap(
                candidate_columns=frozenset({"l_extendedprice"}),
                reference_columns=frozenset({"l_extendedprice"}),
                reference_kind=mv_scoring.REFERENCE_GOVERNED_MV,
            )
        ),
        run_id="run_5521",
    )
    assert proposal.evidence["lineage_reference_kind"] == mv_scoring.REFERENCE_GOVERNED_MV


# ── Y ────────────────────────────────────────────────────────────────────


def test_recurrence_curve_saturates_and_never_exceeds_one() -> None:
    assert normalized_recurrence(0) == 0.0
    assert normalized_recurrence(-5) == 0.0
    assert normalized_recurrence(config.MV_RECURRENCE_SATURATION) == 1.0
    assert normalized_recurrence(config.MV_RECURRENCE_SATURATION * 100) == 1.0
    assert normalized_recurrence(1) < normalized_recurrence(10) < normalized_recurrence(60)


def test_recurrence_curve_has_diminishing_returns() -> None:
    """A linear curve would let one pathological dashboard saturate Y alone."""
    first_ten = normalized_recurrence(10) - normalized_recurrence(1)
    second_ten = normalized_recurrence(20) - normalized_recurrence(11)
    assert first_ten > second_ten


def test_y_is_zero_when_the_corpus_did_not_collapse_to_one_canonical_form() -> None:
    """The flag is corpus-internal (MV-D11): a consumer that bucketed by anything
    looser than canonical equality must not get credit for recurrence."""
    assert syntactic_score(RecurrenceSignal(recurrence=60, ast_equivalent=False)) == 0.0
    assert syntactic_score(RecurrenceSignal(recurrence=60, ast_equivalent=True)) > 0.9


def test_curated_provenance_is_neutral_at_zero() -> None:
    """MV-D17's load-bearing property: the credit is exactly zero occurrences when
    no source was curated (``k * 0 == 0`` returns the identical float), so a
    generated-only candidate — including the pinned POV worked examples — scores
    precisely as it did before the credit existed."""
    generated = RecurrenceSignal(recurrence=60, ast_equivalent=True)
    assert syntactic_score(generated) == normalized_recurrence(60)
    assert round(syntactic_score(generated), 2) == 0.95


def test_a_single_curated_source_discriminates_at_the_modal_case() -> None:
    """MV-D17's modal case — the one a ceiling-clamped test cannot see: one human
    curates a measure the generator derived once. The credit must lift Y off the
    generated floor *without* clamping, so the ordering it produces is a real
    ordering. The credit is added inside the log (``r + k*c``), so a single
    curated source is worth ``k`` generated occurrences: it outranks a measure the
    generator produced ``k`` times, and — because the credit is finite — it does
    not dominate an arbitrarily high generated volume."""
    k = config.MV_CURATED_OCCURRENCE_EQUIVALENT
    single_curated = RecurrenceSignal(
        recurrence=1, curated_provenance_count=1, ast_equivalent=True
    )
    # Exact functional form: credit added inside the log, not multiplied onto it.
    assert syntactic_score(single_curated) == normalized_recurrence(1 + k)
    # Discriminates rather than clamps — the property the old ceiling test masked.
    assert syntactic_score(single_curated) < 1.0
    # Worth ~k generated occurrences: strictly beats a measure generated k times.
    assert syntactic_score(single_curated) > syntactic_score(
        RecurrenceSignal(recurrence=k, ast_equivalent=True)
    )
    # Finite credit: one curated source does not swamp far larger generated volume.
    assert syntactic_score(single_curated) < syntactic_score(
        RecurrenceSignal(recurrence=2 * k + 2, ast_equivalent=True)
    )


def test_curated_credit_is_monotone_and_clamp_free_in_the_realistic_grid() -> None:
    """The shape guarantee MV-D17 buys: over a realistic grid (r ≤ 12, ≤ 3 curated
    sources) each additional curated source strictly increases Y and none pins it
    to 1.0. This is the "no wall / no cliff" property the multiplier form violated
    (it clamped a single curated source at r ≥ 8). The ``r + k*c < sat`` guard
    documents that the authored default calibration keeps this grid below the
    log's saturation point; if it fails, the default k has outgrown the grid."""
    k = config.MV_CURATED_OCCURRENCE_EQUIVALENT
    sat = config.MV_RECURRENCE_SATURATION
    for r in range(1, 13):
        previous = -1.0
        for c in range(0, 4):
            signal = RecurrenceSignal(
                recurrence=r, curated_provenance_count=c, ast_equivalent=True
            )
            y = syntactic_score(signal)
            assert r + k * c < sat, (r, c)
            assert y < 1.0, (r, c, y)
            assert y > previous, (r, c, y, previous)
            previous = y


def test_curated_credit_still_zeroes_on_a_non_equivalent_corpus() -> None:
    """The equivalence flag governs first (MV-D11): a curated source cannot buy Y
    for a bucket whose occurrences did not share one canonical form."""
    signal = RecurrenceSignal(
        recurrence=60, curated_provenance_count=5, ast_equivalent=False
    )
    assert syntactic_score(signal) == 0.0


def test_curated_credit_still_saturates_at_extreme_counts() -> None:
    """Y stays a [0, 1] signal: the credit is additive inside the log, so a
    genuinely saturating effective count still clamps at the ceiling — the clamp
    is a boundary for extremes, not the operating point for realistic curation."""
    signal = RecurrenceSignal(
        recurrence=60, curated_provenance_count=10, ast_equivalent=True
    )
    assert syntactic_score(signal) == 1.0


def test_mv_d17_register_worked_numbers_hold_at_the_default_k() -> None:
    """Pin the exact Y values the MV-D17 register and the gap-report Y row cite for
    the authored default, so the committed arithmetic cannot drift from the code —
    the committed-text-contradicts-code class the gap report exists to catch. The
    named inversion (3 curated losing to 60 generated) is fixed; the conservative
    default declines to let one curated source do the same."""
    assert config.MV_CURATED_OCCURRENCE_EQUIVALENT == 20
    assert config.MV_RECURRENCE_SATURATION == 75
    single_curated = RecurrenceSignal(
        recurrence=1, curated_provenance_count=1, ast_equivalent=True
    )
    three_curated = RecurrenceSignal(
        recurrence=3, curated_provenance_count=3, ast_equivalent=True
    )
    sixty_generated = RecurrenceSignal(recurrence=60, ast_equivalent=True)
    assert round(syntactic_score(single_curated), 3) == 0.714
    assert round(syntactic_score(three_curated), 3) == 0.960
    assert round(syntactic_score(sixty_generated), 3) == 0.949
    assert syntactic_score(three_curated) > syntactic_score(sixty_generated)
    assert syntactic_score(single_curated) < syntactic_score(sixty_generated)


def test_y_does_not_require_a_governed_metric_view_match() -> None:
    """POV Part 3's first worked example scores Y=0.95 for a measure with *no* MV
    equivalent. An MV-matching flag would zero the signal that example exists to
    demonstrate; MV equivalence is the dedup gate's business instead.
    """
    proposal = score_candidate(
        candidate(recurrence=RecurrenceSignal(recurrence=60, ast_equivalent=True)),
        run_id="run_5521",
        mv_fields=(),
    )
    assert proposal.components.Y == pytest.approx(0.9492, abs=1e-4)
    assert proposal.verdict == mv_scoring.VERDICT_PROPOSE


# ── S ────────────────────────────────────────────────────────────────────


def test_semantic_score_is_invariant_to_vector_magnitude() -> None:
    """(3, 4, 0) has norm 5; cosine against (1, 0, 0) is 0.6 only if we
    normalize. GTE does not normalize its own output."""
    fields = metric_view_fields(governed_revenue_yaml())
    measure = next(f for f in fields if f.field_name == "discounted_revenue")
    client = FakeEmbeddingClient({"revenue please": (3.0, 4.0, 0.0), measure.text: (1.0, 0.0, 0.0)})

    match = semantic_score(
        ("revenue please",), (measure,), client, reference_kind=GOVERNED_FIELDS
    )

    assert match.cosine == pytest.approx(0.6)
    assert match.field == f"{GOVERNED_MV}.discounted_revenue"
    assert match.reference_kind == GOVERNED_FIELDS


def test_semantic_score_reports_the_best_matching_field() -> None:
    fields = metric_view_fields(governed_revenue_yaml())
    measure = next(f for f in fields if f.field_name == "discounted_revenue")
    dimension = next(f for f in fields if f.field_name == "order_status")
    client = FakeEmbeddingClient(
        {
            "revenue": (1.0, 0.0, 0.0),
            measure.text: (1.0, 0.0, 0.0),
            dimension.text: (0.0, 1.0, 0.0),
        }
    )

    match = semantic_score(("revenue",), (measure, dimension), client)

    assert match.cosine == pytest.approx(1.0)
    assert match.field == f"{GOVERNED_MV}.discounted_revenue"


def test_semantic_score_separates_a_dead_endpoint_from_an_absent_reference_set() -> None:
    """MV-D15: both give cosine 0.0, and they are not the same fact.

    Before ``status`` existed these four cases returned byte-identical payloads,
    so an operator reading a zero afterwards could not tell a missing dependency
    from a negative finding. No client is ``UNAVAILABLE`` and leaves the blend;
    nothing to compare is ``EMPTY`` and keeps its weight.
    """
    fields = metric_view_fields(governed_revenue_yaml())

    no_client = semantic_score(("revenue",), fields, None)
    assert no_client.cosine == 0.0
    assert no_client.status == config.MV_SIGNAL_UNAVAILABLE

    for nothing_to_compare in (
        semantic_score(("revenue",), (), FakeEmbeddingClient()),
        semantic_score((), fields, FakeEmbeddingClient()),
        semantic_score(("   ",), fields, FakeEmbeddingClient()),
    ):
        assert nothing_to_compare.cosine == 0.0
        assert nothing_to_compare.status == config.MV_SIGNAL_EMPTY


def test_semantic_score_survives_an_embedding_failure() -> None:
    """A missing endpoint costs one signal out of four, not the run.

    Reported ``UNAVAILABLE`` rather than ``EMPTY``: the endpoint raising says
    nothing about whether the reference set had a match, so under MV-D15 the
    signal leaves the blend instead of contributing a zero it did not measure.
    """

    class Exploding:
        def embed(self, texts):
            raise RuntimeError("endpoint unavailable")

    fields = metric_view_fields(governed_revenue_yaml())
    match = semantic_score(("revenue",), fields, Exploding())
    assert match.cosine == 0.0
    assert match.status == config.MV_SIGNAL_UNAVAILABLE


def test_semantic_score_rejects_a_client_returning_the_wrong_vector_count() -> None:
    """A client that miscounts is broken, not empty — the same UNAVAILABLE path."""

    class Truncating:
        def embed(self, texts):
            return [[1.0, 0.0, 0.0]]

    fields = metric_view_fields(governed_revenue_yaml())
    match = semantic_score(("revenue",), fields, Truncating())
    assert match.cosine == 0.0
    assert match.status == config.MV_SIGNAL_UNAVAILABLE


def test_negative_cosine_is_reported_as_no_match() -> None:
    fields = metric_view_fields(governed_revenue_yaml())
    measure = next(f for f in fields if f.field_name == "discounted_revenue")
    client = FakeEmbeddingClient({"revenue": (-1.0, 0.0, 0.0), measure.text: (1.0, 0.0, 0.0)})

    match = semantic_score(("revenue",), (measure,), client, reference_kind=GOVERNED_FIELDS)
    assert match.cosine == 0.0
    assert match.field is None
    assert match.reference_kind == GOVERNED_FIELDS


# ── S reference kinds (MV-D12) ───────────────────────────────────────────


def test_source_column_metadata_embeds_name_and_comment_not_sql() -> None:
    column = SourceColumnMetadata(
        table=LINEITEM, column="l_extendedprice", comment="Extended price before discount"
    )
    assert column.pointer == f"{LINEITEM}.l_extendedprice"
    # Underscores opened up: 'l_extendedprice' embeds poorly as a single token.
    assert column.text == "l extendedprice Extended price before discount"
    assert SourceColumnMetadata(table=LINEITEM, column="l_discount").text == "l discount"


@pytest.mark.parametrize(
    ("candidate_type", "expected_kind"),
    [
        ("NEW_METRIC_VIEW", COLUMN_METADATA),
        ("REPLACE_RAW_TABLE", GOVERNED_FIELDS),
        ("ADD_MEASURE", GOVERNED_FIELDS),
    ],
)
def test_the_preferred_reference_kind_follows_the_candidate_type(
    candidate_type: str, expected_kind: str
) -> None:
    """MV-D12: the question S answers differs by candidate type, so preference is
    keyed on the type rather than on whatever happens to be available."""
    kind, references = semantic_reference_for(
        candidate(candidate_type=candidate_type, source_column_metadata=SOURCE_COLUMNS),
        metric_view_fields(governed_revenue_yaml()),
    )
    assert kind == expected_kind
    assert references


def test_a_new_metric_view_candidate_scores_s_against_its_source_columns() -> None:
    """The defect MV-D12 fixes: under a governed-fields-only reading this
    candidate scores S = 0.0 and can never exceed 80."""
    client = FakeEmbeddingClient(
        {"discounted revenue": (1.0, 0.0, 0.0), "l extendedprice Extended price": (1.0, 0.0, 0.0)}
    )
    proposal = score_candidate(
        strong_candidate(source_column_metadata=SOURCE_COLUMNS),
        run_id="run_5521",
        mv_fields=(),
        intent_texts=("discounted revenue",),
        embedding_client=client,
    )

    assert proposal.components.S == pytest.approx(1.0)
    assert proposal.evidence["semantic_top_match"] == {
        "field": f"{LINEITEM}.l_extendedprice",
        "cosine": pytest.approx(1.0),
        "reference_kind": COLUMN_METADATA,
        "status": config.MV_SIGNAL_COMPUTED,
    }
    assert proposal.tier == mv_scoring.TIER_HIGH


def test_a_new_metric_view_candidate_is_no_longer_capped_below_high() -> None:
    """Regression guard for MV-D12's capping argument, now enforced by MV-D15.

    MV-D12 rejected a reading under which S was structurally 0.0 for every
    ``NEW_METRIC_VIEW`` candidate, because that caps the class at 80 against
    thresholds calibrated for 100 and makes HIGH structurally unreachable for the
    engine's primary output. MV-D15 removes the ceiling by a different route: an
    unmeasured signal leaves the blend and the rest renormalize, so the ceiling
    is 100 and HIGH does not depend on 0.35 + 0.30 + 0.15 happening to clear 75.

    The EMPTY leg is what keeps this honest. A *measured* zero still costs the
    full 20 points — so the old 80.0 is preserved exactly where it is the truth,
    and only the never-measured case is excused.
    """
    signals = dict(
        lineage=LineageOverlap(
            candidate_columns=frozenset({"a"}), reference_columns=frozenset({"a"})
        ),
        recurrence=RecurrenceSignal(recurrence=config.MV_RECURRENCE_SATURATION),
        demand=DemandSignal(
            frequency=config.MV_DEMAND_FREQUENCY_SATURATION,
            cost_ms=config.MV_DEMAND_COST_SATURATION_MS,
            distinct_users=config.MV_DEMAND_BREADTH_SATURATION,
        ),
    )

    # S never ran (no client): weight leaves the blend, three signals renormalize.
    unavailable_s = score_candidate(candidate(**signals), run_id="r1")
    assert unavailable_s.components.status_of("S") == config.MV_SIGNAL_UNAVAILABLE
    assert unavailable_s.confidence_score == pytest.approx(100.0)
    assert unavailable_s.evidence_coverage == pytest.approx(0.80)
    assert unavailable_s.tier == mv_scoring.TIER_HIGH
    assert unavailable_s.tier_capped_by_coverage is False

    # S ran and found nothing: the old 80.0, because a measured zero keeps its weight.
    empty_s = score_candidate(
        candidate(**signals),
        run_id="r1",
        statuses={"S": config.MV_SIGNAL_EMPTY},
    )
    assert empty_s.confidence_score == pytest.approx(80.0)
    assert empty_s.evidence_coverage == pytest.approx(1.0)

    client = FakeEmbeddingClient({"revenue": (1.0, 0.0, 0.0), "l discount": (1.0, 0.0, 0.0)})
    with_s = score_candidate(
        candidate(source_column_metadata=SOURCE_COLUMNS, **signals),
        run_id="r1",
        intent_texts=("revenue",),
        embedding_client=client,
    )
    assert with_s.confidence_score == pytest.approx(100.0)
    assert with_s.evidence_coverage == pytest.approx(1.0)


def test_a_replace_raw_table_candidate_prefers_governed_field_text() -> None:
    fields = metric_view_fields(governed_revenue_yaml())
    measure = next(f for f in fields if f.kind == mv_scoring.FIELD_MEASURE)
    client = FakeEmbeddingClient({"revenue": (1.0, 0.0, 0.0), measure.text: (1.0, 0.0, 0.0)})

    proposal = score_candidate(
        strong_candidate(
            candidate_type="REPLACE_RAW_TABLE",
            measure_expr=LIST_REVENUE,
            source_column_metadata=SOURCE_COLUMNS,
        ),
        run_id="run_5521",
        mv_fields=fields,
        intent_texts=("revenue",),
        embedding_client=client,
    )

    match = proposal.evidence["semantic_top_match"]
    assert match["reference_kind"] == GOVERNED_FIELDS
    assert match["field"] == f"{GOVERNED_MV}.discounted_revenue"


def test_the_reference_kind_falls_back_and_reports_what_it_actually_used() -> None:
    """A preference is not a requirement: the kind recorded is the one compared,
    never the one that was wanted."""
    kind, references = semantic_reference_for(
        candidate(candidate_type="REPLACE_RAW_TABLE", source_column_metadata=SOURCE_COLUMNS),
        (),  # no governed fields available
    )
    assert kind == COLUMN_METADATA
    assert references == SOURCE_COLUMNS

    kind, references = semantic_reference_for(
        candidate(candidate_type="NEW_METRIC_VIEW"),  # no column metadata
        metric_view_fields(governed_revenue_yaml()),
    )
    assert kind == GOVERNED_FIELDS


def test_both_references_absent_yields_zero_with_a_null_field() -> None:
    """Honest, not imputed: no metric view and no column metadata is genuinely
    no semantic evidence."""
    kind, references = semantic_reference_for(candidate(), ())
    assert kind == mv_scoring.SEMANTIC_REF_NONE
    assert references == ()

    proposal = score_candidate(
        strong_candidate(),
        run_id="run_5521",
        mv_fields=(),
        intent_texts=("discounted revenue",),
        embedding_client=FakeEmbeddingClient(),
    )
    assert proposal.components.S == 0.0
    assert proposal.evidence["semantic_top_match"] == {
        "field": None,
        "cosine": 0.0,
        "reference_kind": mv_scoring.SEMANTIC_REF_NONE,
        "status": config.MV_SIGNAL_EMPTY,
    }


def test_blank_column_metadata_does_not_count_as_a_reference() -> None:
    kind, _ = semantic_reference_for(
        candidate(source_column_metadata=(SourceColumnMetadata(table="", column=""),)), ()
    )
    assert kind == mv_scoring.SEMANTIC_REF_NONE


# ── D ────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("age_days", "expected"),
    [(0.0, 1.0), (30.0, 0.5), (60.0, 0.25), (90.0, 0.125)],
)
def test_demand_decays_on_a_thirty_day_half_life(age_days: float, expected: float) -> None:
    assert demand_decay(age_days) == pytest.approx(expected)


def test_demand_decay_applies_to_the_score() -> None:
    saturated = DemandSignal(
        frequency=config.MV_DEMAND_FREQUENCY_SATURATION,
        cost_ms=config.MV_DEMAND_COST_SATURATION_MS,
        distinct_users=config.MV_DEMAND_BREADTH_SATURATION,
    )
    assert demand_score(saturated) == pytest.approx(1.0)
    assert demand_score(dataclasses.replace(saturated, age_days=30.0)) == pytest.approx(0.5)


def test_demand_uses_a_geometric_mean_because_a_literal_product_cannot_reach_0_80() -> None:
    """MV-D11. Three normalized factors multiplied collapse toward zero, so the
    literal reading of "frequency x cost x distinct users" could not produce the
    0.80 POV's own worked example asserts."""
    busy = DemandSignal(frequency=80, cost_ms=2_880_000, distinct_users=8)
    literal_product = 0.8 * 0.8 * 0.8

    assert demand_score(busy) == pytest.approx(0.8)
    assert literal_product == pytest.approx(0.512)


@pytest.mark.parametrize(
    "signal",
    [
        DemandSignal(frequency=0, cost_ms=3_600_000, distinct_users=10),
        DemandSignal(frequency=100, cost_ms=0, distinct_users=10),
        DemandSignal(frequency=100, cost_ms=3_600_000, distinct_users=0),
        DemandSignal(),
    ],
)
def test_a_zero_factor_zeroes_demand(signal: DemandSignal) -> None:
    """A measure nobody runs, that costs nothing, or that one person uses is not
    demand — the geometric mean must not rescue it."""
    assert demand_score(signal) == 0.0


def test_demand_factors_saturate_rather_than_exceeding_one() -> None:
    enormous = DemandSignal(frequency=10**6, cost_ms=10**12, distinct_users=10**4)
    assert demand_score(enormous) == pytest.approx(1.0)


# ── Metric view field flattening ─────────────────────────────────────────


def test_metric_view_fields_flattens_describe_yaml_into_measures_and_dimensions() -> None:
    fields = metric_view_fields(governed_revenue_yaml())

    assert {f.field_name for f in fields} == {"discounted_revenue", "order_status"}
    measure = next(f for f in fields if f.field_name == "discounted_revenue")
    assert measure.kind == mv_scoring.FIELD_MEASURE
    assert measure.canonical_expr == canonicalize_expr(DISCOUNTED_REVENUE)
    assert measure.pointer == f"{GOVERNED_MV}.discounted_revenue"
    assert measure.source_columns == frozenset({"l_extendedprice", "l_discount"})
    assert "Discounted Revenue" in measure.text and "net revenue" in measure.text


@pytest.mark.parametrize(
    "yamls",
    [{}, None, {GOVERNED_MV: "not a mapping"}, {GOVERNED_MV: {"measures": [{"name": ""}]}}],
)
def test_metric_view_fields_tolerates_unusable_input(yamls) -> None:
    assert metric_view_fields(yamls) == ()


def test_metric_view_field_text_excludes_sql() -> None:
    """The S signal embeds intent, not SQL. Expression text in the embedded
    surface would score two unrelated measures as similar for sharing a column."""
    measure = next(
        f for f in metric_view_fields(governed_revenue_yaml()) if f.kind == mv_scoring.FIELD_MEASURE
    )
    assert "l_extendedprice" not in measure.text


# ── Dedup gate ───────────────────────────────────────────────────────────


def test_exact_match_on_a_governed_measure_blocks_with_a_pointer() -> None:
    outcome = dedup_gate(candidate(), mv_fields=metric_view_fields(governed_revenue_yaml()))

    assert outcome.verdict == mv_scoring.VERDICT_BLOCKED
    assert outcome.blocked_by == f"{GOVERNED_MV}.discounted_revenue"
    assert outcome.conflicts == ()


def test_partial_overlap_becomes_alternatives_not_a_block() -> None:
    partial = {
        GOVERNED_MV: {
            "source": LINEITEM,
            "measures": [
                {"name": "list_revenue", "expr": LIST_REVENUE},
                {"name": "unrelated", "expr": "SUM(o_totalprice)"},
            ],
        }
    }
    outcome = dedup_gate(candidate(), mv_fields=metric_view_fields(partial))

    assert outcome.verdict == mv_scoring.VERDICT_PROPOSE
    assert outcome.blocked_by is None
    assert [entry["field"] for entry in outcome.alternatives] == ["list_revenue"]
    assert outcome.alternatives[0]["shared_columns"] == ["l_extendedprice"]
    assert outcome.alternatives[0]["reason"] == "partial_column_overlap"


def test_alternatives_are_ranked_by_overlap_then_pointer() -> None:
    partial = {
        GOVERNED_MV: {
            "source": LINEITEM,
            "measures": [
                {"name": "one_column", "expr": LIST_REVENUE},
                {"name": "both_columns", "expr": "SUM(l_extendedprice + l_discount)"},
            ],
        }
    }
    outcome = dedup_gate(candidate(), mv_fields=metric_view_fields(partial))

    assert [entry["field"] for entry in outcome.alternatives] == ["both_columns", "one_column"]
    assert [entry["overlap"] for entry in outcome.alternatives] == [2, 1]


def test_a_governed_match_plus_a_divergent_instruction_is_a_conflict() -> None:
    """POV Part 5: never a suggestion, never auto-resolved."""
    instructions = (
        InstructionDefinition(
            source="text_instruction[2]", concept="revenue", expr=LIST_REVENUE
        ),
    )
    outcome = dedup_gate(
        candidate(),
        mv_fields=metric_view_fields(governed_revenue_yaml()),
        instructions=instructions,
    )

    assert outcome.verdict == mv_scoring.VERDICT_CONFLICT
    assert len(outcome.conflicts) == 1
    entry = outcome.conflicts[0]
    assert entry["source"] == "text_instruction[2]"
    assert entry["resolution"] == "requires_human_adjudication"
    assert entry["existing_expr"] == canonicalize_expr(LIST_REVENUE)
    assert entry["proposed_expr"] == canonicalize_expr(DISCOUNTED_REVENUE)
    assert entry["governed_by"] == f"{GOVERNED_MV}.discounted_revenue"


def test_an_instruction_agreeing_with_the_governed_measure_is_not_a_conflict() -> None:
    """Same concept, same canonical expression — that is agreement, and it blocks
    on the governed measure rather than demanding adjudication."""
    instructions = (
        InstructionDefinition(
            source="text_instruction[2]", concept="revenue", expr=DISCOUNTED_REVENUE
        ),
    )
    outcome = dedup_gate(
        candidate(),
        mv_fields=metric_view_fields(governed_revenue_yaml()),
        instructions=instructions,
    )
    assert outcome.verdict == mv_scoring.VERDICT_BLOCKED


def test_a_divergent_instruction_without_a_governed_match_still_proposes() -> None:
    """The conflict state requires both halves. An instruction alone is what the
    advisor is for — it has no governed definition to contradict yet."""
    instructions = (
        InstructionDefinition(
            source="text_instruction[2]", concept="revenue", expr=LIST_REVENUE
        ),
    )
    outcome = dedup_gate(candidate(), mv_fields=(), instructions=instructions)
    assert outcome.verdict == mv_scoring.VERDICT_PROPOSE


def test_conflict_detection_ignores_definitions_of_other_concepts() -> None:
    instructions = (
        InstructionDefinition(source="text_instruction[9]", concept="margin", expr=LIST_REVENUE),
    )
    outcome = dedup_gate(
        candidate(),
        mv_fields=metric_view_fields(governed_revenue_yaml()),
        instructions=instructions,
    )
    assert outcome.verdict == mv_scoring.VERDICT_BLOCKED


def test_dimensions_never_block_a_measure() -> None:
    """Only governed *measures* can already define a measure."""
    dimension_only = {
        GOVERNED_MV: {"dimensions": [{"name": "revenue_bucket", "expr": DISCOUNTED_REVENUE}]}
    }
    outcome = dedup_gate(candidate(), mv_fields=metric_view_fields(dimension_only))
    assert outcome.verdict == mv_scoring.VERDICT_PROPOSE


def test_an_unparseable_candidate_proposes_nothing_and_raises_nothing() -> None:
    outcome = dedup_gate(candidate(measure_expr="not sql at all ((("))
    assert outcome.verdict == mv_scoring.VERDICT_PROPOSE
    assert outcome.alternatives == ()


# ── Proposal assembly ────────────────────────────────────────────────────


def strong_candidate(**overrides) -> MetricViewCandidate:
    """A candidate with strong L, Y and D signals."""
    defaults = {
        "lineage": LineageOverlap(
            candidate_columns=frozenset(f"c{i}" for i in range(9)),
            reference_columns=frozenset(f"c{i}" for i in range(10)),
        ),
        "recurrence": RecurrenceSignal(recurrence=60, ast_equivalent=True),
        "demand": DemandSignal(frequency=80, cost_ms=2_880_000, distinct_users=8),
    }
    defaults.update(overrides)
    return candidate(**defaults)


ADJACENT_MV_YAML = {
    GOVERNED_MV: {
        "source": LINEITEM,
        "measures": [
            {
                "name": "list_revenue",
                "expr": LIST_REVENUE,
                "display_name": "List Revenue",
                "comment": "Revenue before discounts",
                "synonyms": ["gross revenue"],
            }
        ],
    }
}
"""A governed metric view that is semantically adjacent to the candidate but not
an expression match — the shape that lets S contribute without the dedup gate
blocking, which is the only way a candidate reaches HIGH."""


def high_tier_proposal(**overrides):
    fields = metric_view_fields(ADJACENT_MV_YAML)
    client = FakeEmbeddingClient(
        {"discounted revenue": (1.0, 0.0, 0.0), fields[0].text: (2.0, 0.0, 0.0)}
    )
    kwargs = {
        "run_id": "run_5521",
        "mv_fields": fields,
        "intent_texts": ("discounted revenue",),
        "embedding_client": client,
    }
    kwargs.update(overrides)
    return score_candidate(strong_candidate(), **kwargs)


def test_score_candidate_emits_the_pov_part_4_payload() -> None:
    proposal = high_tier_proposal(generated_at="2026-08-22T14:03:00Z")
    payload = proposal.to_payload()

    assert set(payload) == {
        "suggestion_id",
        "type",
        "confidence_score",
        "tier",
        "uncapped_tier",
        "tier_capped_by_coverage",
        "evidence_coverage",
        "target_space_id",
        "proposed_object",
        "score_components",
        "evidence",
        "provenance",
        "dedup_fingerprint",
        "alternatives",
        "conflicts",
    }
    assert payload["type"] == "NEW_METRIC_VIEW"
    assert payload["tier"] == mv_scoring.TIER_HIGH
    assert payload["uncapped_tier"] == mv_scoring.TIER_HIGH
    assert payload["tier_capped_by_coverage"] is False
    assert payload["evidence_coverage"] == pytest.approx(1.0)
    assert payload["confidence_score"] == pytest.approx(91.98, abs=0.01)
    assert payload["target_space_id"] == SPACE_ID
    assert payload["proposed_object"] == "finance.sales.discounted_revenue_metrics"
    assert payload["score_components"]["weights"] == dict(config.MV_SCORE_WEIGHTS)
    assert payload["score_components"]["statuses"] == {
        key: config.MV_SIGNAL_COMPUTED for key in ("L", "Y", "S", "D")
    }
    assert payload["score_components"]["evidence_coverage"] == pytest.approx(1.0)
    assert payload["evidence"]["ast_fingerprint_recurrence"] == 60
    assert payload["evidence"]["lineage_source_tables"] == [LINEITEM, ORDERS]
    assert payload["provenance"] == {
        "generated_by": "gwb-mv-advisor@1.0",
        "auth_identity": "OBO",
        "gso_run_id": "run_5521",
        "gso_task_key": "optimize",
        "generated_at": "2026-08-22T14:03:00Z",
    }
    assert json.dumps(payload)  # the payload must be JSON-serializable as-is


def test_dedup_fingerprint_is_the_mv_d7_key_and_the_suggestion_id_derives_from_it() -> None:
    proposal = score_candidate(strong_candidate(), run_id="run_5521")

    assert len(proposal.dedup_fingerprint) == 64
    assert int(proposal.dedup_fingerprint, 16) >= 0  # bare hex digest (MV-D10)
    assert proposal.suggestion_id == suggestion_id_for(proposal.dedup_fingerprint)
    assert proposal.suggestion_id == f"sug_{proposal.dedup_fingerprint[:12]}"


def test_the_key_is_insensitive_to_source_table_order_so_reruns_upsert_one_row() -> None:
    forward = score_candidate(strong_candidate(source_tables=(LINEITEM, ORDERS)), run_id="r1")
    reversed_ = score_candidate(strong_candidate(source_tables=(ORDERS, LINEITEM)), run_id="r2")

    assert forward.dedup_fingerprint == reversed_.dedup_fingerprint
    assert forward.suggestion_id == reversed_.suggestion_id


def test_alias_spelling_does_not_fork_a_candidate() -> None:
    plain = score_candidate(strong_candidate(), run_id="r1")
    aliased = score_candidate(
        strong_candidate(measure_expr="SUM(li.l_extendedprice * (1 - li.l_discount))"),
        run_id="r1",
    )
    assert plain.dedup_fingerprint == aliased.dedup_fingerprint


def test_a_conflict_is_persisted_but_is_never_a_suggestion() -> None:
    proposal = score_candidate(
        strong_candidate(),
        run_id="run_5521",
        mv_fields=metric_view_fields(governed_revenue_yaml()),
        instructions=(
            InstructionDefinition(
                source="text_instruction[2]", concept="revenue", expr=LIST_REVENUE
            ),
        ),
    )

    assert proposal.verdict == mv_scoring.VERDICT_CONFLICT
    assert proposal.candidate_type == "CONFLICT"
    assert proposal.is_suggestion is False
    assert proposal.is_persistable is True
    assert proposal.conflicts
    assert proposal.to_payload()["type"] == "CONFLICT"


def test_a_blocked_candidate_is_not_persistable_and_skips_the_embedding_call() -> None:
    client = FakeEmbeddingClient()
    proposal = score_candidate(
        strong_candidate(),
        run_id="run_5521",
        mv_fields=metric_view_fields(governed_revenue_yaml()),
        intent_texts=("revenue",),
        embedding_client=client,
    )

    assert proposal.verdict == mv_scoring.VERDICT_BLOCKED
    assert proposal.blocked_by == f"{GOVERNED_MV}.discounted_revenue"
    assert proposal.is_persistable is False
    assert client.calls == []
    # L, Y and D are pure arithmetic, so a blocked row still explains itself.
    assert proposal.components.L == 0.90
    assert proposal.components.S == 0.0


def test_a_sub_25_score_is_suppressed_with_no_tier() -> None:
    proposal = score_candidate(candidate(), run_id="run_5521")

    assert proposal.confidence_score < config.MV_TIER_LOW_MIN
    assert proposal.verdict == mv_scoring.VERDICT_SUPPRESSED
    assert proposal.tier is None
    assert proposal.is_persistable is False


def test_a_low_tier_candidate_is_still_a_persisted_proposal() -> None:
    """LOW is a real tier — only sub-25 suppresses."""
    proposal = score_candidate(
        candidate(
            lineage=LineageOverlap(
                candidate_columns=frozenset({"a", "b"}), reference_columns=frozenset({"a", "b"})
            )
        ),
        run_id="run_5521",
    )
    assert proposal.tier == mv_scoring.TIER_LOW
    assert proposal.verdict == mv_scoring.VERDICT_PROPOSE
    assert proposal.is_persistable is True


def test_an_unknown_candidate_type_is_rejected_at_the_boundary() -> None:
    with pytest.raises(ValueError, match="candidate_type"):
        score_candidate(strong_candidate(candidate_type="INVENT_A_VIEW"), run_id="r1")


def test_score_candidates_ranks_by_confidence_descending() -> None:
    proposals = score_candidates([candidate(), strong_candidate()], run_id="run_5521")

    assert proposals[0].confidence_score > proposals[1].confidence_score
    assert [p.verdict for p in proposals] == [
        mv_scoring.VERDICT_PROPOSE,
        mv_scoring.VERDICT_SUPPRESSED,
    ]


def test_a_conflict_sorts_after_a_suggestion_at_the_same_score() -> None:
    """A queue that leads with items needing adjudication buries the ones a
    reviewer can act on. The two candidates here carry identical signals, so
    only the tiebreak can order them."""
    proposals = score_candidates(
        [
            strong_candidate(measure_expr=DISCOUNTED_REVENUE),
            strong_candidate(measure_expr="SUM(o_totalprice)"),
        ],
        run_id="run_5521",
        mv_fields=metric_view_fields(governed_revenue_yaml()),
        instructions=(
            InstructionDefinition(
                source="text_instruction[2]", concept="revenue", expr=LIST_REVENUE
            ),
        ),
    )

    assert proposals[0].confidence_score == proposals[1].confidence_score
    assert [p.verdict for p in proposals] == [
        mv_scoring.VERDICT_PROPOSE,
        mv_scoring.VERDICT_CONFLICT,
    ]


# ── Firewall: no benchmark question text anywhere ─────────────────────────


def test_the_candidate_contract_has_no_field_for_question_text() -> None:
    """Structural, not filtered: evidence carries ids because there is nothing
    else to carry."""
    names = {f.name for f in dataclasses.fields(MetricViewCandidate)}
    assert "benchmark_question_ids" in names
    assert not any("text" in name for name in names)


def test_intent_text_is_embedded_but_never_stored_in_a_proposal() -> None:
    fields = metric_view_fields(governed_revenue_yaml())
    measure = next(f for f in fields if f.kind == mv_scoring.FIELD_MEASURE)
    client = FakeEmbeddingClient({BENCHMARK_TEXT: (1.0, 1.0, 0.0), measure.text: (1.0, 0.0, 0.0)})

    proposal = score_candidate(
        strong_candidate(measure_expr=LIST_REVENUE),
        run_id="run_5521",
        mv_fields=fields,
        intent_texts=(BENCHMARK_TEXT,),
        embedding_client=client,
    )

    assert client.calls, "the intent text must actually have been embedded"
    assert proposal.components.S > 0.0
    serialized = json.dumps(proposal.to_payload())
    assert BENCHMARK_TEXT not in serialized
    assert "discounted revenue by region" not in serialized
    assert proposal.evidence["benchmark_questions"] == ["bmk_12", "bmk_31"]


def test_conflict_entries_carry_canonical_literal_free_expressions() -> None:
    proposal = score_candidate(
        strong_candidate(measure_expr="SUM(CASE WHEN o_orderstatus = 'F' THEN 1 END)"),
        run_id="run_5521",
        mv_fields=metric_view_fields(
            {
                GOVERNED_MV: {
                    "measures": [
                        {
                            "name": "finished_orders",
                            "expr": "SUM(CASE WHEN o_orderstatus = 'F' THEN 1 END)",
                        }
                    ]
                }
            }
        ),
        instructions=(
            InstructionDefinition(
                source="trusted_asset:orders_q",
                concept="revenue",
                expr="SUM(o_totalprice)",
            ),
        ),
    )

    serialized = json.dumps(proposal.to_payload())
    assert proposal.verdict == mv_scoring.VERDICT_CONFLICT
    assert "'F'" not in serialized
    assert "o_orderstatus = ?s" in serialized


def test_definitions_differing_only_in_an_erased_literal_are_not_flagged() -> None:
    """A documented consequence of canonicalization, not an oversight.

    ``= 'F'`` and ``= 'O'`` both canonicalize to ``= ?s``, so the conflict
    detector cannot see divergence that lives only in a literal. It therefore
    under-reports conflicts, and the failure direction is safe: the candidate
    lands on BLOCKED — no suggestion — rather than being proposed as if the
    contradiction did not exist. Recovering literal-level divergence needs
    profiling values, which is a generator concern, not a scoring one.
    """
    f_orders = "SUM(CASE WHEN o_orderstatus = 'F' THEN 1 END)"
    o_orders = "SUM(CASE WHEN o_orderstatus = 'O' THEN 1 END)"

    outcome = dedup_gate(
        candidate(measure_expr=f_orders),
        mv_fields=metric_view_fields(
            {GOVERNED_MV: {"measures": [{"name": "finished_orders", "expr": f_orders}]}}
        ),
        instructions=(
            InstructionDefinition(source="text_instruction[2]", concept="revenue", expr=o_orders),
        ),
    )

    assert outcome.verdict == mv_scoring.VERDICT_BLOCKED
    assert outcome.conflicts == ()


# ── Persistence through the Prompt 1 accessor ────────────────────────────


def test_persist_proposal_maps_the_payload_onto_the_prompt_1_accessor(monkeypatch) -> None:
    spy = UpsertSpy()
    monkeypatch.setattr(mv_scoring, "upsert_mv_candidate", spy)
    proposal = score_candidate(strong_candidate(), run_id="run_5521")

    returned = persist_proposal(
        object(),
        proposal,
        catalog="main",
        schema="genie_space_optimizer",
        requested_mode="create_and_attach",
        effective_mode="suggest_only",
    )

    assert returned == proposal.dedup_fingerprint
    assert len(spy.calls) == 1
    kwargs = spy.calls[0]
    assert kwargs["catalog"] == "main"
    assert kwargs["schema"] == "genie_space_optimizer"
    assert kwargs["run_id"] == "run_5521"
    assert kwargs["target_space_id"] == SPACE_ID
    assert kwargs["suggestion_id"] == proposal.suggestion_id
    assert kwargs["dedup_fingerprint"] == proposal.dedup_fingerprint
    assert kwargs["candidate_type"] == "NEW_METRIC_VIEW"
    assert kwargs["confidence_score"] == proposal.confidence_score
    # HIGH rather than MEDIUM because this proposal has no embedding client, so S
    # is UNAVAILABLE and the remaining 0.80 of weight renormalizes (MV-D15).
    assert kwargs["tier"] == proposal.tier == mv_scoring.TIER_HIGH
    # MV-D32 as-implemented (Prompt 15.7b): the score-only tier and coverage-cap
    # flag reach Delta too, so the panel can split coverage-capped-strong
    # proposals without re-deriving them. Here S is UNAVAILABLE but the remaining
    # weight renormalizes to HIGH, so coverage did not bind: uncapped == tier and
    # the flag is False. The capped case is pinned in the frontend split tests.
    assert kwargs["uncapped_tier"] == proposal.uncapped_tier == mv_scoring.TIER_HIGH
    assert proposal.tier_capped_by_coverage is False
    assert kwargs["tier_capped_by_coverage"] is False
    assert kwargs["score_components"] == proposal.components.to_dict()
    # Coverage and statuses reach Delta, not just the in-memory payload — a
    # persisted score without its divisor is the unauditable case MV-D15 forbids.
    assert kwargs["score_components"]["evidence_coverage"] == pytest.approx(0.80)
    assert kwargs["score_components"]["statuses"]["S"] == config.MV_SIGNAL_UNAVAILABLE
    assert kwargs["evidence"] == dict(proposal.evidence)
    assert kwargs["provenance"] == dict(proposal.provenance)
    assert kwargs["requested_mode"] == "create_and_attach"
    assert kwargs["effective_mode"] == "suggest_only"
    # Human decision columns are the accessor's business, never this module's.
    assert "decision" not in kwargs
    assert "approved_for_rerun" not in kwargs


def test_persist_proposal_writes_a_conflict(monkeypatch) -> None:
    spy = UpsertSpy()
    monkeypatch.setattr(mv_scoring, "upsert_mv_candidate", spy)
    proposal = score_candidate(
        strong_candidate(),
        run_id="run_5521",
        mv_fields=metric_view_fields(governed_revenue_yaml()),
        instructions=(
            InstructionDefinition(
                source="text_instruction[2]", concept="revenue", expr=LIST_REVENUE
            ),
        ),
    )

    assert persist_proposal(object(), proposal, catalog="main", schema="gso") is not None
    assert spy.calls[0]["candidate_type"] == "CONFLICT"
    assert spy.calls[0]["conflicts"]


@pytest.mark.parametrize("verdict", [mv_scoring.VERDICT_BLOCKED, mv_scoring.VERDICT_SUPPRESSED])
def test_blocked_and_suppressed_verdicts_are_reported_not_written(
    monkeypatch, verdict: str
) -> None:
    """MV-D11: MV_CANDIDATE_TYPES has no state for either, and tier is
    documented HIGH|MEDIUM|LOW. Widening that vocabulary is a later migration,
    not a workaround here."""
    spy = UpsertSpy()
    monkeypatch.setattr(mv_scoring, "upsert_mv_candidate", spy)

    if verdict == mv_scoring.VERDICT_BLOCKED:
        proposal = score_candidate(
            strong_candidate(),
            run_id="run_5521",
            mv_fields=metric_view_fields(governed_revenue_yaml()),
        )
    else:
        proposal = score_candidate(candidate(), run_id="run_5521")

    assert proposal.verdict == verdict
    assert persist_proposal(object(), proposal, catalog="main", schema="gso") is None
    assert spy.calls == []


def test_persisting_without_a_run_id_fails_loudly(monkeypatch) -> None:
    monkeypatch.setattr(mv_scoring, "upsert_mv_candidate", UpsertSpy())
    proposal = score_candidate(strong_candidate())

    with pytest.raises(ValueError, match="run_id"):
        persist_proposal(object(), proposal, catalog="main", schema="gso")


def test_only_propose_and_conflict_are_persistable() -> None:
    assert mv_scoring.PERSISTABLE_VERDICTS == {
        mv_scoring.VERDICT_PROPOSE,
        mv_scoring.VERDICT_CONFLICT,
    }


# ── Module boundaries ────────────────────────────────────────────────────


def _module_ast() -> ast.Module:
    from pathlib import Path

    return ast.parse(Path(mv_scoring.__file__).read_text(encoding="utf-8"))


def _called_names(tree: ast.Module) -> set[str]:
    """Every name invoked as a call, whether bare or as an attribute."""
    out: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        if isinstance(target, ast.Name):
            out.add(target.id)
        elif isinstance(target, ast.Attribute):
            out.add(target.attr)
    return out


def test_this_module_queries_nothing() -> None:
    """POV Part 3 signals arrive precomputed. A DESCRIBE or a system-table read
    here would put a second scanner beside the ones that already own them.

    Asserted against the AST rather than the file text: a substring scan over
    source cannot tell a call from the docstring that documents its absence.
    """
    called = _called_names(_module_ast())
    for forbidden in ("sql", "run_query", "read_table", "toPandas", "createDataFrame"):
        assert forbidden not in called, forbidden


def test_this_module_imports_no_query_layer() -> None:
    imported: set[str] = set()
    for node in ast.walk(_module_ast()):
        if isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
        elif isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)

    assert not any("delta_helpers" in module for module in imported)
    assert not any("warehouse" in module for module in imported)
    assert not any("metric_view_catalog" in module for module in imported)


def test_the_embedding_adapter_borrows_one_symbol_from_the_firewall() -> None:
    """It shares an endpoint mechanism with the firewall and nothing else, so
    exactly one name may cross that boundary."""
    borrowed: set[str] = set()
    for node in ast.walk(_module_ast()):
        if isinstance(node, ast.ImportFrom) and (node.module or "").endswith("leakage"):
            borrowed.update(alias.name for alias in node.names)

    assert borrowed == {"get_embedding"}


def test_the_prod_adapter_l2_normalizes_whatever_the_endpoint_returns() -> None:
    class FakeWorkspace:
        pass

    client = mv_scoring.FoundationModelEmbeddingClient(FakeWorkspace(), endpoint="fake-endpoint")

    import genie_space_optimizer.optimization.leakage as leakage

    original = leakage.get_embedding
    try:
        leakage.get_embedding = lambda text, w, endpoint=None: [3.0, 4.0, 0.0]
        vectors = client.embed(["anything"])
    finally:
        leakage.get_embedding = original

    assert vectors[0] == pytest.approx([0.6, 0.8, 0.0])
    assert math.isclose(math.sqrt(sum(x * x for x in vectors[0])), 1.0)


def test_the_prod_adapter_defaults_to_the_configured_advisor_endpoint() -> None:
    client = mv_scoring.FoundationModelEmbeddingClient(object())
    assert client._endpoint == config.MV_EMBEDDING_ENDPOINT
    assert config.MV_EMBEDDING_ENDPOINT == "databricks-gte-large-en"


# ── Trusted assets as a conflict surface (POV Part 5 step 3) ─────────────
#
# example_question_sqls is a different field from text_instructions, and the gate
# used to read neither it nor anything but governed metric views — so a proposal
# could contradict a curated answer and reach PROPOSE unchallenged. These pin the
# route that closes it, including the firewall boundary: curated SQL is legitimate
# fingerprint input, and the question beside it is not input at all.


REVENUE_COLUMNS = frozenset({"l_extendedprice", "l_discount"})


def _space(*assets: dict) -> dict:
    return {"instructions": {"example_question_sqls": list(assets)}}


def _asset(sql: str, *, asset_id: str = "eq_7") -> dict:
    return {"id": asset_id, "question": BENCHMARK_TEXT, "sql": sql}


def _revenue_candidate(**overrides) -> MetricViewCandidate:
    return candidate(measure_columns=REVENUE_COLUMNS, **overrides)


def test_trusted_asset_definitions_reads_measures_out_of_curated_sql() -> None:
    definitions = trusted_asset_definitions(
        _space(_asset(f"SELECT {LIST_REVENUE} FROM {LINEITEM}"))
    )

    assert len(definitions) == 1
    assert definitions[0].source == "trusted_asset:eq_7"
    assert definitions[0].canonical_expr == canonicalize_expr(LIST_REVENUE)
    assert definitions[0].aggregate.lower() == "sum"
    assert definitions[0].measure_columns == frozenset({"l_extendedprice"})


def test_a_trusted_asset_definition_carries_no_question_text() -> None:
    """The firewall boundary. ``conflicts[]`` is persisted and rendered, so a
    question copied into it would be exactly the leak the oracle exists to stop.
    The asset's id is what a reviewer needs, and all they get."""
    definitions = trusted_asset_definitions(
        _space(_asset(f"SELECT {LIST_REVENUE} FROM {LINEITEM}"))
    )

    blob = repr(definitions)
    assert BENCHMARK_TEXT not in blob
    for word in ("discounted", "region", "quarter"):
        assert word not in blob.lower().replace("discounted_revenue", "")
    # ``concept`` is empty precisely because the only name on offer is that text.
    assert definitions[0].concept == ""


def test_a_divergent_trusted_asset_conflicts_with_no_governed_metric_view() -> None:
    """The case the gate could not previously reach.

    A space with no metric views yet, whose curated answer defines revenue one
    way, and a candidate proposing another. Same aggregate over the same columns,
    different arithmetic: one of the two is wrong, and it is not the shipped one.
    """
    divergent = "SUM(l_extendedprice - l_discount)"
    outcome = dedup_gate(
        _revenue_candidate(measure_expr=divergent),
        mv_fields=(),
        instructions=trusted_asset_definitions(
            _space(_asset(f"SELECT {DISCOUNTED_REVENUE} FROM {LINEITEM}"))
        ),
    )

    assert outcome.verdict == mv_scoring.VERDICT_CONFLICT
    entry = outcome.conflicts[0]
    assert entry["authoritative"] == "trusted_asset:eq_7"
    assert entry["existing_expr"] == canonicalize_expr(DISCOUNTED_REVENUE)
    assert entry["proposed_expr"] == canonicalize_expr(divergent)
    assert entry["resolution"] == "requires_human_adjudication"
    # No governed metric view exists, and the conflict does not pretend one does.
    assert entry["governed_by"] is None
    assert outcome.blocked_by == ""


def test_an_agreeing_trusted_asset_is_not_a_conflict() -> None:
    """Agreement is the good outcome, not something to escalate."""
    outcome = dedup_gate(
        _revenue_candidate(),
        mv_fields=(),
        instructions=trusted_asset_definitions(
            _space(_asset(f"SELECT {DISCOUNTED_REVENUE} FROM {LINEITEM}"))
        ),
    )

    assert outcome.verdict == mv_scoring.VERDICT_PROPOSE


def test_a_trusted_asset_over_other_columns_is_not_a_conflict() -> None:
    """Different columns are a different quantity, however similar the shape."""
    outcome = dedup_gate(
        _revenue_candidate(),
        mv_fields=(),
        instructions=trusted_asset_definitions(
            _space(_asset(f"SELECT SUM(o_totalprice) FROM {ORDERS}"))
        ),
    )

    assert outcome.verdict == mv_scoring.VERDICT_PROPOSE


def test_a_trusted_asset_with_a_different_aggregate_is_not_a_conflict() -> None:
    """``AVG`` over the same columns answers a different question from ``SUM``."""
    outcome = dedup_gate(
        _revenue_candidate(),
        mv_fields=(),
        instructions=trusted_asset_definitions(
            _space(_asset(f"SELECT AVG(l_extendedprice * (1 - l_discount)) FROM {LINEITEM}"))
        ),
    )

    assert outcome.verdict == mv_scoring.VERDICT_PROPOSE


def test_a_qualified_trusted_asset_still_matches_an_unqualified_candidate() -> None:
    """Candidates carry bare column names; curated SQL carries whatever it wrote.

    Without a shared normalization the check would silently never fire on any
    asset that qualified its columns — which is most of them.
    """
    outcome = dedup_gate(
        _revenue_candidate(measure_expr="SUM(l_extendedprice - l_discount)"),
        mv_fields=(),
        instructions=trusted_asset_definitions(
            _space(
                _asset(
                    "SELECT SUM(l.l_extendedprice * (1 - l.l_discount)) "
                    f"FROM {LINEITEM} AS l"
                )
            )
        ),
    )

    assert outcome.verdict == mv_scoring.VERDICT_CONFLICT


def test_a_trusted_asset_conflict_beats_a_governed_block() -> None:
    """Blocking silently on a governed measure a curated answer contradicts hides
    the contradiction, which is the failure POV Part 5 exists to prevent."""
    outcome = dedup_gate(
        _revenue_candidate(),
        mv_fields=metric_view_fields(governed_revenue_yaml()),
        instructions=trusted_asset_definitions(
            _space(_asset(f"SELECT SUM(l_extendedprice + l_discount) FROM {LINEITEM}"))
        ),
    )

    assert outcome.verdict == mv_scoring.VERDICT_CONFLICT
    assert outcome.conflicts[0]["governed_by"] == f"{GOVERNED_MV}.discounted_revenue"


def test_a_candidate_without_resolved_columns_cannot_be_conflicted() -> None:
    """An unknown column set is absence of evidence. Treating it as a match would
    let one asset conflict with every candidate in the space."""
    outcome = dedup_gate(
        candidate(measure_expr="SUM(l_extendedprice - l_discount)", concept=""),
        mv_fields=(),
        instructions=trusted_asset_definitions(
            _space(_asset(f"SELECT {DISCOUNTED_REVENUE} FROM {LINEITEM}"))
        ),
    )

    assert outcome.verdict == mv_scoring.VERDICT_PROPOSE


def test_trusted_asset_definitions_tolerates_unusable_input() -> None:
    assert trusted_asset_definitions(None) == ()
    assert trusted_asset_definitions({}) == ()
    assert trusted_asset_definitions(_space()) == ()
    # Unparseable SQL costs that asset's definitions and nothing else.
    assert trusted_asset_definitions(_space(_asset("SELECT FROM WHERE ("))) == ()
    # A statement with no aggregate defines no measure.
    assert trusted_asset_definitions(_space(_asset("SELECT l_orderkey FROM x"))) == ()


def test_trusted_asset_definitions_deduplicates_within_one_asset() -> None:
    """One asset repeating a measure is one definition, not two identical rows."""
    definitions = trusted_asset_definitions(
        _space(
            _asset(
                f"SELECT {LIST_REVENUE} AS a, {LIST_REVENUE} AS b FROM {LINEITEM}"
            )
        )
    )
    assert len(definitions) == 1


def test_an_asset_without_an_id_is_identified_by_position() -> None:
    """A conflict must be locatable even when the asset carries no id."""
    definitions = trusted_asset_definitions(
        {"instructions": {"example_question_sqls": [{"sql": f"SELECT {LIST_REVENUE} FROM x"}]}}
    )
    assert definitions[0].source == "trusted_asset:index:0"
