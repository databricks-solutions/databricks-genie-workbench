"""Scoring, dedup gate, and proposal payload for the metric view advisor.

POV Part 3 blends four signals into one 0-100 confidence score:

``score = 100 * (0.35*L + 0.30*Y + 0.20*S + 0.15*D)``

- **L** — lineage/schema determinism, the Jaccard of two column sets.
- **Y** — syntactic recurrence: how often the corpus re-derived this measure,
  gated on the statements genuinely collapsing to one canonical form.
- **S** — semantic similarity: max cosine of intent text against a reference set
  whose kind depends on the candidate (MV-D12) — governed metric-view field text
  when the candidate acts on an existing view, otherwise the source columns' own
  names and comments.
- **D** — demand, from frequency, cost and distinct-user breadth, then decayed
  by a 30-day half-life so a measure nobody has asked for in a quarter stops
  arguing for itself.

Weights and every normalization constant live in
:mod:`genie_space_optimizer.common.config` (MV-D11), never inlined here, because
the weights travel inside each proposal's ``score_components`` and a retuned
deployment must be able to say which blend produced a given score.

**What Y multiplies, and what it does not** (MV-D11). The equivalence flag in
``normalized_recurrence * equivalence_flag`` is *corpus-internal*: it asserts
that the statements counted as recurrences really do share one canonical form,
rather than having been bucketed by a looser similarity. It is emphatically not
"does this match a governed metric view" — POV Part 3's first worked example
scores Y = 0.95 for a measure with *no* metric-view equivalent, so an
MV-matching flag would zero the very signal that example is built to
demonstrate. Equivalence against governed metric views is the *dedup gate*
below, which blocks a governed measure outright instead of damping its score.
Gating is strictly stronger than damping: a measure that is already governed is
not a weaker proposal, it is not a proposal.

**Why S has two reference kinds** (MV-D12). Read literally, "max cosine of
intent text vs MV field text" makes S structurally 0.0 for every
``NEW_METRIC_VIEW`` candidate — there is no metric view yet, so there is no
field text. That would retire 20% of the blend for precisely the candidate class
this engine exists to produce, capping it at 80 against thresholds calibrated
for 100, and POV Part 3's own worked example contradicts it by scoring S = 0.40
for a measure with no metric-view equivalent. So S takes the same shape L
already has: a reference set plus a recorded ``reference_kind``. Governed
metric-view field text where the candidate acts on an existing view; the source
columns' names and comments where it does not. Where neither exists S is 0.0
with a null field, reported honestly rather than imputed.

**This module computes; it does not query.** No system table, no DESCRIBE, no
warehouse. Lineage overlap arrives precomputed as :class:`LineageOverlap`,
recurrence as :class:`RecurrenceSignal`, demand as :class:`DemandSignal`, and
existing metric-view definitions as :class:`MetricViewField` values flattened
from ``common.metric_view_catalog.detect_metric_views_via_catalog``, which owns
the ``DESCRIBE ... AS JSON`` parsing. Embeddings arrive through an injected
:class:`EmbeddingClient`. That keeps every signal testable offline and keeps the
scoring rules in one file that a reviewer can read end to end.

**Benchmark question text cannot reach a proposal.** The evidence contract
carries benchmark *ids* only — :class:`MetricViewCandidate` has no field for
question text, so there is nothing to filter. Intent text is passed separately
to :func:`semantic_score`, consumed as vectors, and never stored. Canonical
expressions are safe to persist because canonicalization erases literals; see
``mv_fingerprint``'s inverted contract with the firewall.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol

from genie_space_optimizer.common.config import (
    MV_ADVISOR_GENERATED_BY,
    MV_CURATED_OCCURRENCE_EQUIVALENT,
    MV_DEMAND_BREADTH_SATURATION,
    MV_DEMAND_COST_SATURATION_MS,
    MV_DEMAND_FREQUENCY_SATURATION,
    MV_DEMAND_HALF_LIFE_DAYS,
    MV_COVERAGE_HIGH_MIN,
    MV_COVERAGE_MEDIUM_MIN,
    MV_EMBEDDING_ENDPOINT,
    MV_RECURRENCE_SATURATION,
    MV_SCORE_WEIGHTS,
    MV_SEMANTIC_STATUS_COMPARED,
    MV_SEMANTIC_STATUS_NO_CLIENT,
    MV_SEMANTIC_STATUS_NOTHING_TO_COMPARE,
    MV_SIGNAL_COMPUTED,
    MV_SIGNAL_EMPTY,
    MV_SIGNAL_UNAVAILABLE,
    MV_TIER_HIGH_MIN,
    MV_TIER_LOW_MIN,
    MV_TIER_MEDIUM_MIN,
)

from .mv_fingerprint import canonicalize_expr, extract_measures
from .mv_state import (
    MV_CANDIDATE_TYPES,
    mv_candidate_fingerprint,
    supersede_legacy_mv_candidates,
    upsert_mv_candidate,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# ── Vocabularies ─────────────────────────────────────────────────────────

TIER_HIGH = "HIGH"
TIER_MEDIUM = "MEDIUM"
TIER_LOW = "LOW"

VERDICT_PROPOSE = "PROPOSE"
VERDICT_BLOCKED = "BLOCKED"
VERDICT_CONFLICT = "CONFLICT"
VERDICT_SUPPRESSED = "SUPPRESSED"

PERSISTABLE_VERDICTS: frozenset[str] = frozenset({VERDICT_PROPOSE, VERDICT_CONFLICT})
"""Only these reach ``genie_opt_mv_candidates`` (MV-D11).

``BLOCKED`` and ``SUPPRESSED`` are returned to the caller for run reporting and
deliberately not written: ``MV_CANDIDATE_TYPES`` has no state for either, and
``genie_opt_mv_candidates.tier`` is documented ``HIGH|MEDIUM|LOW``. Should a
later UI need blocked-candidate counts persisted, that is an
``ADDITIVE_COLUMN_MIGRATIONS`` entry plus an enum extension at that time — not
a reason to widen the type vocabulary now.
"""

FIELD_MEASURE = "measure"
FIELD_DIMENSION = "dimension"

REFERENCE_GOVERNED_MV = "governed_mv"
REFERENCE_LINEAGE_FOOTPRINT = "lineage_footprint"

SEMANTIC_REF_GOVERNED_MV_FIELDS = "GOVERNED_MV_FIELDS"
SEMANTIC_REF_SOURCE_COLUMN_METADATA = "SOURCE_COLUMN_METADATA"
SEMANTIC_REF_NONE = "NONE"

_SEMANTIC_REF_BY_CANDIDATE_TYPE: dict[str, str] = {
    "NEW_METRIC_VIEW": SEMANTIC_REF_SOURCE_COLUMN_METADATA,
    "REPLACE_RAW_TABLE": SEMANTIC_REF_GOVERNED_MV_FIELDS,
    "ADD_MEASURE": SEMANTIC_REF_GOVERNED_MV_FIELDS,
    "CONFLICT": SEMANTIC_REF_GOVERNED_MV_FIELDS,
}
"""Which reference kind **S** prefers per candidate type (MV-D12).

Keyed on candidate type rather than on what happens to be available, because the
question S answers differs: for a candidate that acts on an existing metric view
("is this measure already described here?") the governed field text is the right
comparand, while for a brand-new view the only semantic surface that exists yet
is the source columns' own names and comments. Where the preferred set is empty
the other is used and the kind actually used is recorded, so a reader never has
to guess which comparison produced a cosine.
"""


# ── Input contracts ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class LineageOverlap:
    """Precomputed column-set overlap for the **L** signal.

    Both sets are supplied by the caller from ``column_lineage``; this module
    only takes their Jaccard. ``reference_kind`` records what the reference set
    *is*, because it differs by candidate type and a score of 0.9 means two
    different things in the two cases:

    - ``REPLACE_RAW_TABLE`` — reference is a governed metric view's source
      columns, so overlap measures substitutability.
    - ``NEW_METRIC_VIEW`` / ``ADD_MEASURE`` — reference is the lineage
      footprint the space's queries actually touch, so overlap measures how
      squarely the proposed measure sits on established lineage.

    Storing the kind rather than inferring it keeps a reviewer from reading the
    wrong meaning into a number, and keeps this module out of the business of
    deciding which comparison the caller intended.
    """

    candidate_columns: frozenset[str] = frozenset()
    reference_columns: frozenset[str] = frozenset()
    reference_kind: str = REFERENCE_LINEAGE_FOOTPRINT
    source_tables: tuple[str, ...] = ()


@dataclass(frozen=True)
class RecurrenceSignal:
    """Precomputed corpus recurrence for the **Y** signal.

    ``ast_equivalent`` is the corpus-internal flag: True when every counted
    occurrence canonicalizes to ``canonical_expr``. A consumer that bucketed by
    anything looser than canonical equality must pass False, which zeroes Y
    rather than letting a soft match masquerade as a governed-measure case.

    ``curated_provenance_count`` is the subset of ``provenance_count`` whose
    sources were curated (a trusted-asset SQL, a curated snippet, a prior GSO
    patch — see ``mv_fingerprint.CURATED_PROVENANCE_KIND``). It is the MV-D17
    occurrence-equivalent credit :func:`syntactic_score` reads; ``provenance_count``
    itself remains unread by scoring (the distinct-source *breadth* damping it
    would enable is a separate, deferred MV-D17 fix).
    """

    canonical_expr: str = ""
    recurrence: int = 0
    provenance_count: int = 0
    ast_equivalent: bool = True
    curated_provenance_count: int = 0


@dataclass(frozen=True)
class DemandSignal:
    """Precomputed query-history demand for the **D** signal.

    ``cost_ms`` is cumulative execution time attributable to the measure.
    ``age_days`` is measured from the most recent occurrence, so the half-life
    decays staleness rather than the measure's whole history.

    **No producer populates this today, and D is reported ``UNAVAILABLE``**
    (MV-D15). An earlier version of this docstring said ``wide_schema_history``
    supplied the cost proxy; it does not — its ``SELECT`` reads
    ``statement_text`` and ``start_time`` and no duration or billing column at
    all, so it can supply neither ``cost_ms`` nor ``distinct_users``. Corrected
    here rather than left standing, because the false claim would tell whoever
    builds D that half of it already exists. Its ``query_occurrence_count`` is
    also grained per column x normalized query shape, not per measure, so even
    ``frequency`` needs a mapping that does not exist yet. See recon Q3.
    """

    frequency: int = 0
    cost_ms: float = 0.0
    distinct_users: int = 0
    age_days: float = 0.0


@dataclass(frozen=True)
class MetricViewField:
    """One measure or dimension of an existing governed metric view.

    Flattened by :func:`metric_view_fields` from the YAML dicts
    ``detect_metric_views_via_catalog`` returns. ``text`` is the natural-language
    surface (name, display name, comment, synonyms) that the **S** signal embeds;
    ``canonical_expr`` is the SQL surface the dedup gate compares.
    """

    mv_fqn: str
    field_name: str
    kind: str = FIELD_MEASURE
    expr: str = ""
    canonical_expr: str = ""
    text: str = ""
    source_columns: frozenset[str] = frozenset()

    @property
    def pointer(self) -> str:
        """``catalog.schema.view.field`` — what a blocked proposal points at."""
        return f"{self.mv_fqn}.{self.field_name}" if self.mv_fqn else self.field_name


@dataclass(frozen=True)
class SourceColumnMetadata:
    """A source column's own name and comment — the **S** reference of last resort.

    This is the weaker of the two semantic references and knowingly so (MV-D12):
    a column comment describes a column, not a business measure, so a strong
    match here is weaker evidence than a strong match against a curated metric
    view field. It is accepted because it is the only semantic evidence that
    exists before a metric view does, and the alternative — S = 0.0 for every
    new-view candidate — makes a fifth of the score unreachable for the engine's
    primary output.
    """

    table: str
    column: str
    comment: str = ""

    @property
    def pointer(self) -> str:
        return f"{self.table}.{self.column}" if self.table else self.column

    @property
    def text(self) -> str:
        """Embeddable surface: the column name with separators opened up, plus
        its comment. ``l_extendedprice`` embeds poorly as one token and rather
        better as ``l extendedprice``."""
        name = self.column.replace("_", " ").strip()
        return f"{name} {self.comment}".strip() if self.comment else name


@dataclass(frozen=True)
class InstructionDefinition:
    """An instruction or trusted-asset SQL that defines a named concept.

    The conflict path needs both halves: ``concept`` is what the definition
    claims to define (the name a reader would match against a proposed measure)
    and ``canonical_expr`` is how it defines it. A definition of the same concept
    with a different canonical expression is the POV Part 5 conflict.

    ``measure_columns`` and ``aggregate`` carry the second way a definition can
    be matched, which is what makes trusted-asset SQL usable here at all. A
    curated answer has no concept *name* to match on — it has a question, and
    question text is firewall-forbidden as a persisted or compared string. What it
    does have is a measure: an aggregate over a column set. Two measures that
    aggregate the same function over the same columns are claims about the same
    quantity, so when their canonical expressions differ, one of them is wrong.
    Both fields empty means only the concept-name route applies.
    """

    source: str
    concept: str
    expr: str = ""
    canonical_expr: str = ""
    measure_columns: frozenset[str] = frozenset()
    aggregate: str = ""


class SemanticReference(Protocol):
    """Anything **S** can compare intent text against.

    Both :class:`MetricViewField` and :class:`SourceColumnMetadata` satisfy it,
    which is what lets one comparison engine serve both reference kinds instead
    of two near-copies drifting apart.
    """

    @property
    def pointer(self) -> str: ...

    @property
    def text(self) -> str: ...


class EmbeddingClient(Protocol):
    """Batch text embedding. Injected so **S** is testable without a workspace."""

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        """Return one vector per input text. Missing vectors may be ``[]``."""
        ...


class FoundationModelEmbeddingClient:
    """Embeds through the Databricks Foundation Model API.

    Delegates the HTTP call to ``leakage.get_embedding`` so the package keeps one
    FMAPI client rather than two. It **shares an endpoint mechanism with the
    firewall and nothing else** — no corpus, no shingles, no leak verdict, no
    firewall code path is invoked from here.

    Vectors are L2-normalized in our own code: the advisor's endpoint
    (``databricks-gte-large-en``) does not normalize its output, while the
    firewall's default (``databricks-bge-large-en``) does. Normalizing an
    already-unit vector is a no-op, so this is correct against either endpoint
    and does not depend on which one a workspace configured.
    """

    def __init__(self, w: Any, endpoint: str | None = None) -> None:
        self._w = w
        self._endpoint = endpoint or MV_EMBEDDING_ENDPOINT

    def embed(self, texts: Sequence[str]) -> list[list[float]]:
        from .leakage import get_embedding

        out: list[list[float]] = []
        for text in texts:
            raw = get_embedding(text, self._w, endpoint=self._endpoint) if text else None
            out.append(_l2_normalize(raw or []))
        return out


@dataclass(frozen=True)
class SemanticMatch:
    """Best intent-to-reference match found for **S**.

    ``reference_kind`` is carried alongside the cosine, not derived later: a 0.40
    against a governed metric-view field and a 0.40 against a column comment are
    different strengths of evidence, and a payload that reports only the number
    leaves a reviewer unable to tell which they are looking at.

    ``status`` separates the two ways a cosine of 0.0 arises (MV-D15). Before it
    existed, a dead embedding endpoint and a candidate with no reference text
    produced byte-identical payloads, so an operator reading a zero afterwards
    could not tell a missing dependency from a negative finding.
    """

    field: str | None = None
    cosine: float = 0.0
    reference_kind: str = SEMANTIC_REF_NONE
    status: str = MV_SEMANTIC_STATUS_NO_CLIENT

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "cosine": self.cosine,
            "reference_kind": self.reference_kind,
            "status": self.status,
        }


@dataclass(frozen=True)
class MetricViewCandidate:
    """One proposal under consideration, with its precomputed signals.

    Note the absence of any benchmark *text* field: evidence carries
    ``benchmark_question_ids`` only, so verbatim benchmark text has no route
    into a persisted proposal or a shipped comment.
    """

    space_id: str
    candidate_type: str = "NEW_METRIC_VIEW"
    measure_expr: str = ""
    source_tables: tuple[str, ...] = ()
    concept: str = ""
    proposed_object: str | None = None
    measure_columns: frozenset[str] = frozenset()
    source_column_metadata: tuple[SourceColumnMetadata, ...] = ()
    lineage: LineageOverlap = field(default_factory=LineageOverlap)
    recurrence: RecurrenceSignal = field(default_factory=RecurrenceSignal)
    demand: DemandSignal = field(default_factory=DemandSignal)
    benchmark_question_ids: tuple[str, ...] = ()
    query_history_statement_ids: tuple[str, ...] = ()

    @property
    def canonical_measure_expr(self) -> str:
        """The canonical form the MV-D7 dedup key hashes.

        Prefers the recurrence signal's canonical expression when present — that
        is the form the corpus scan already canonicalized, and re-deriving it
        here is how two components end up disagreeing about the key.
        """
        return self.recurrence.canonical_expr or canonicalize_expr(self.measure_expr)


# ── Component scorers ────────────────────────────────────────────────────


def lineage_overlap_score(overlap: LineageOverlap) -> float:
    """**L**: Jaccard of the two column sets. 0.0 when either side is empty.

    An empty side is absence of evidence, not evidence of overlap — scoring it
    as 1.0 (vacuously equal empty sets) would hand a perfect determinism score
    to a candidate whose lineage nobody managed to resolve.
    """
    left = frozenset(overlap.candidate_columns)
    right = frozenset(overlap.reference_columns)
    if not left or not right:
        return 0.0
    union = left | right
    return len(left & right) / len(union)


def normalized_recurrence(recurrence: int, saturation: int | None = None) -> float:
    """Map an occurrence count onto [0, 1], log-scaled and saturating.

    ``log1p(r) / log1p(saturation)``, clamped at 1.0. Diminishing returns are
    the point: the 60th re-derivation of a measure adds less evidence than the
    6th, and a linear curve would let one pathological dashboard saturate the
    signal on its own.
    """
    if recurrence <= 0:
        return 0.0
    cap = MV_RECURRENCE_SATURATION if saturation is None else saturation
    if cap <= 0:
        return 1.0
    return min(1.0, math.log1p(recurrence) / math.log1p(cap))


def syntactic_score(signal: RecurrenceSignal, saturation: int | None = None) -> float:
    """**Y**: log-saturated recurrence over the corpus-internal equivalence flag,
    with curated provenance credited as extra occurrences (MV-D17).

    See the module docstring: the flag says the counted occurrences share one
    canonical form. It does *not* ask whether a governed metric view already
    defines the measure — that question is the dedup gate's, and its answer
    blocks rather than damps.

    Each distinct curated source is credited as
    :data:`config.MV_CURATED_OCCURRENCE_EQUIVALENT` (``k``) generated occurrences,
    added to the count *inside* the saturating curve —
    ``normalized_recurrence(recurrence + k * curated_provenance_count)`` — so the
    credit is monotone throughout Y's range rather than a multiplier that walls at
    the clamp. It is neutral by construction when ``curated_provenance_count`` is 0
    (``k * 0 == 0`` returns the identical float), leaving generated-only
    candidates — the pinned POV worked examples included — and the MV-D15 divisor
    exactly as they were. Whether ``k`` is large enough for one curated source to
    outrank a heavily-recurring generated one is ``k``'s authored value, not a
    property of this function: at the default ``k = 20`` a single curated source
    beats a lightly-recurring generated measure but not sixty derivations of one.
    """
    if not signal.ast_equivalent:
        return 0.0
    effective_recurrence = (
        signal.recurrence
        + MV_CURATED_OCCURRENCE_EQUIVALENT * signal.curated_provenance_count
    )
    return normalized_recurrence(effective_recurrence, saturation)


def _l2_normalize(vector: Sequence[float]) -> list[float]:
    """Scale to unit length. Returns ``[]`` for an empty or zero vector."""
    if not vector:
        return []
    norm = math.sqrt(sum(float(x) * float(x) for x in vector))
    if norm <= 0.0:
        return []
    return [float(x) / norm for x in vector]


def _cosine(a: Sequence[float], b: Sequence[float]) -> float:
    """Cosine similarity of two vectors, normalizing both first.

    Normalizing here rather than trusting the caller makes the result invariant
    to vector magnitude, which is what lets one code path serve a
    self-normalizing endpoint and one that does not.
    """
    left = _l2_normalize(a)
    right = _l2_normalize(b)
    if not left or not right or len(left) != len(right):
        return 0.0
    return sum(x * y for x, y in zip(left, right))


def semantic_score(
    intent_texts: Sequence[str],
    references: Sequence[SemanticReference],
    client: EmbeddingClient | None,
    *,
    reference_kind: str = SEMANTIC_REF_NONE,
) -> SemanticMatch:
    """**S**: the best cosine between any intent text and any reference text.

    The caller chooses the reference set and names its kind — see
    :data:`_SEMANTIC_REF_BY_CANDIDATE_TYPE`. This function does the comparison
    and nothing else, so both kinds are scored by identical code.

    Returns a zero match when there is nothing to compare or no client: a
    missing embedding endpoint costs the advisor one signal out of four rather
    than the whole run, matching how the firewall degrades. A negative maximum is
    reported as 0.0 — opposed vectors mean no semantic match, and a negative
    contribution to a confidence score is meaningless.
    """
    intents = [text for text in intent_texts if text and text.strip()]
    targets = [ref for ref in references if ref.text and ref.text.strip()]
    if client is None:
        return SemanticMatch(status=MV_SEMANTIC_STATUS_NO_CLIENT)
    if not intents or not targets:
        return SemanticMatch(status=MV_SEMANTIC_STATUS_NOTHING_TO_COMPARE)

    try:
        vectors = client.embed([*intents, *[ref.text for ref in targets]])
    except Exception:
        logger.warning(
            "mv_scoring: embedding call failed; S is UNAVAILABLE and leaves the blend",
            exc_info=True,
        )
        return SemanticMatch(status=MV_SEMANTIC_STATUS_NO_CLIENT)

    if len(vectors) != len(intents) + len(targets):
        logger.warning(
            "mv_scoring: embedding client returned %d vectors for %d texts; "
            "S is UNAVAILABLE and leaves the blend",
            len(vectors), len(intents) + len(targets),
        )
        return SemanticMatch(status=MV_SEMANTIC_STATUS_NO_CLIENT)

    intent_vectors = vectors[: len(intents)]
    target_vectors = vectors[len(intents):]

    best_cosine = 0.0
    best_field: str | None = None
    for reference, target_vector in zip(targets, target_vectors):
        for intent_vector in intent_vectors:
            cosine = _cosine(intent_vector, target_vector)
            if cosine > best_cosine:
                best_cosine = cosine
                best_field = reference.pointer
    if best_field is None:
        return SemanticMatch(
            reference_kind=reference_kind,
            status=MV_SEMANTIC_STATUS_NOTHING_TO_COMPARE,
        )
    return SemanticMatch(
        field=best_field,
        cosine=best_cosine,
        reference_kind=reference_kind,
        status=MV_SEMANTIC_STATUS_COMPARED,
    )


def semantic_reference_for(
    candidate: MetricViewCandidate,
    mv_fields: Sequence[MetricViewField] = (),
) -> tuple[str, tuple[SemanticReference, ...]]:
    """Pick **S**'s reference set and name its kind (MV-D12).

    Preference comes from the candidate type; availability decides the rest. If
    the preferred set is empty the other is used, and the kind returned is
    always the one actually compared — never the one that was wanted.
    """
    measures = tuple(f for f in mv_fields if f.text and f.text.strip())
    columns = tuple(
        c for c in candidate.source_column_metadata if c.text and c.text.strip()
    )
    by_kind: dict[str, tuple[SemanticReference, ...]] = {
        SEMANTIC_REF_GOVERNED_MV_FIELDS: measures,
        SEMANTIC_REF_SOURCE_COLUMN_METADATA: columns,
    }
    preferred = _SEMANTIC_REF_BY_CANDIDATE_TYPE.get(
        candidate.candidate_type, SEMANTIC_REF_GOVERNED_MV_FIELDS
    )
    fallback = (
        SEMANTIC_REF_SOURCE_COLUMN_METADATA
        if preferred == SEMANTIC_REF_GOVERNED_MV_FIELDS
        else SEMANTIC_REF_GOVERNED_MV_FIELDS
    )
    for kind in (preferred, fallback):
        if by_kind[kind]:
            return kind, by_kind[kind]
    return SEMANTIC_REF_NONE, ()


def demand_decay(age_days: float, half_life_days: float | None = None) -> float:
    """``0.5 ** (age_days / H)`` — POV Part 3's staleness decay."""
    half_life = MV_DEMAND_HALF_LIFE_DAYS if half_life_days is None else half_life_days
    if half_life <= 0 or age_days <= 0:
        return 1.0
    return 0.5 ** (age_days / half_life)


def demand_score(signal: DemandSignal, half_life_days: float | None = None) -> float:
    """**D**: normalized frequency, cost and breadth, combined then decayed.

    Each factor is saturated against its own config ceiling, then combined as a
    **geometric mean** rather than the literal product POV Part 3's prose
    suggests (MV-D11). Three normalized factors multiplied collapse toward zero
    — 0.8 * 0.8 * 0.8 = 0.51 — so a literal product could not reach the 0.80
    that POV's own worked example asserts for a busy measure. The geometric mean
    keeps D on the same 0-1 scale as L, Y and S, which is the only way the
    weights mean what they say.

    Zero on any factor still yields zero: a measure no one runs, that costs
    nothing, or that one person uses is not demand.
    """
    factors = (
        _saturate(signal.frequency, MV_DEMAND_FREQUENCY_SATURATION),
        _saturate(signal.cost_ms, MV_DEMAND_COST_SATURATION_MS),
        _saturate(signal.distinct_users, MV_DEMAND_BREADTH_SATURATION),
    )
    if any(factor <= 0.0 for factor in factors):
        return 0.0
    geometric_mean = math.exp(sum(math.log(factor) for factor in factors) / len(factors))
    return geometric_mean * demand_decay(signal.age_days, half_life_days)


def _saturate(value: float, ceiling: float) -> float:
    if value <= 0 or ceiling <= 0:
        return 0.0
    return min(1.0, float(value) / float(ceiling))


# ── Blend and tiers ──────────────────────────────────────────────────────


_SIGNAL_KEYS: tuple[str, ...] = ("L", "Y", "S", "D")


@dataclass(frozen=True)
class ScoreComponents:
    """The four signals, the weights they were blended with, and whether each was
    actually measured (POV Part 4 ``score_components``, extended by MV-D15).

    ``statuses`` defaults to all-``COMPUTED`` so a caller that supplies four
    values keeps the pre-MV-D15 behaviour exactly — which is what preserves the
    two pinned POV worked examples at coverage 1.0 with a divisor of 1.0.
    """

    L: float = 0.0
    Y: float = 0.0
    S: float = 0.0
    D: float = 0.0
    weights: Mapping[str, float] = field(default_factory=lambda: dict(MV_SCORE_WEIGHTS))
    statuses: Mapping[str, str] = field(
        default_factory=lambda: {key: MV_SIGNAL_COMPUTED for key in _SIGNAL_KEYS}
    )

    def status_of(self, key: str) -> str:
        """Status for one signal; an unnamed signal is treated as ``COMPUTED``.

        Defaulting to ``COMPUTED`` rather than ``UNAVAILABLE`` keeps a partial
        ``statuses`` map from silently deleting signals from the blend — a caller
        that names only the absent ones gets what it meant.
        """
        return self.statuses.get(key, MV_SIGNAL_COMPUTED)

    def is_counted(self, key: str) -> bool:
        """Whether this signal contributes weight to the divisor (MV-D15).

        ``EMPTY`` counts — it measured zero. ``UNAVAILABLE`` does not.
        """
        return self.status_of(key) != MV_SIGNAL_UNAVAILABLE

    def value_of(self, key: str) -> float:
        """The signal's contribution numerator. ``UNAVAILABLE`` contributes
        nothing regardless of any value that happens to be carried alongside it,
        so a stale number can never leak into a score as though measured."""
        return float(getattr(self, key)) if self.is_counted(key) else 0.0

    @property
    def evidence_coverage(self) -> float:
        """Summed weight of the ``COMPUTED`` and ``EMPTY`` signals (MV-D15).

        This is the blend's divisor and it travels on every proposal, because a
        renormalized score without its divisor cannot be audited: 90 over four
        signals and 90 over one are the same number and different claims.

        **Rounded to six places, and the rounding is load-bearing rather than
        cosmetic.** The default weights make the S-unavailable case — the most
        common one, since a workspace with no embedding endpoint hits it on every
        candidate — sum to ``0.7999999999999999``, one ULP below
        :data:`MV_COVERAGE_HIGH_MIN`. Unrounded, HIGH would be unreachable for
        that entire class by floating-point accident rather than by decision, and
        the cause would be invisible in a payload that prints ``0.8``. Rounding
        is safe here because this sums a handful of authored decimal constants
        rather than measurements, and it makes the all-``COMPUTED`` divisor
        exactly 1.0, which is what keeps the two pinned worked examples on their
        exact IEEE doubles instead of merely near them.
        """
        return round(
            sum(self.weights.get(key, 0.0) for key in _SIGNAL_KEYS if self.is_counted(key)),
            6,
        )

    def weighted_terms(self) -> dict[str, float]:
        """Per-signal contributions before the ``* 100``.

        Rounded to six places for reporting only — the blend itself is not
        rounded. Without this, a term prints as ``0.08000000000000002`` and a
        reviewer reconciling a score against the POV worksheet has to squint.
        """
        return {
            key: round(self.weights.get(key, 0.0) * self.value_of(key), 6)
            for key in _SIGNAL_KEYS
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "L": self.L,
            "Y": self.Y,
            "S": self.S,
            "D": self.D,
            "weights": dict(self.weights),
            "statuses": {key: self.status_of(key) for key in _SIGNAL_KEYS},
            "evidence_coverage": self.evidence_coverage,
        }


def blended_score(components: ScoreComponents) -> float:
    """``100 * (0.35L + 0.30Y + 0.20S + 0.15D)``, renormalized over measured
    signals and unrounded.

    Divides by :attr:`ScoreComponents.evidence_coverage` so the scale stays
    0-100 whatever the availability mix (MV-D15). With all four signals
    ``COMPUTED`` the divisor is exactly 1.0, so this is the pre-MV-D15 arithmetic
    and both POV Part 3 worked examples land on the same IEEE doubles they
    always did.

    Left unrounded on purpose. Rounding to two places would drag a 24.999 across
    the suppression floor into LOW — a rounding rule that changes which
    candidates reach a human is not a presentation detail.

    Returns 0.0 when nothing was measured. A candidate with no coverage at all
    has no score rather than an undefined one, and the coverage cap will hold it
    at or below LOW anyway.
    """
    coverage = components.evidence_coverage
    if coverage <= 0:
        return 0.0
    weights = components.weights
    weighted = sum(
        weights.get(key, 0.0) * components.value_of(key) for key in _SIGNAL_KEYS
    )
    return 100.0 * (weighted / coverage)


def tier_for(score: float) -> str | None:
    """POV Part 3 thresholds. ``None`` means suppress (below 25).

    Score-only: this is the tier the evidence earned, before MV-D15's coverage
    cap is applied. :func:`capped_tier` combines the two.
    """
    if score >= MV_TIER_HIGH_MIN:
        return TIER_HIGH
    if score >= MV_TIER_MEDIUM_MIN:
        return TIER_MEDIUM
    if score >= MV_TIER_LOW_MIN:
        return TIER_LOW
    return None


def coverage_ceiling(coverage: float) -> str:
    """The best tier ``coverage`` permits (MV-D15).

    Never returns ``None``: coverage bounds a tier from above and does not
    suppress. Suppression is a statement about the score being too low to be
    worth a reviewer's time, which is :func:`tier_for`'s judgment to make.
    """
    if coverage >= MV_COVERAGE_HIGH_MIN:
        return TIER_HIGH
    if coverage >= MV_COVERAGE_MEDIUM_MIN:
        return TIER_MEDIUM
    return TIER_LOW


@dataclass(frozen=True)
class TierDecision:
    """A tier, the tier the score alone earned, and whether coverage bound it."""

    tier: str | None
    uncapped_tier: str | None
    capped_by_coverage: bool
    evidence_coverage: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "tier": self.tier,
            "uncapped_tier": self.uncapped_tier,
            "tier_capped_by_coverage": self.capped_by_coverage,
            "evidence_coverage": self.evidence_coverage,
        }


def capped_tier(score: float, coverage: float) -> TierDecision:
    """Apply MV-D15's coverage cap to the tier the score earned.

    Reports the uncapped tier alongside the capped one so a reviewer can see
    both what the evidence said and what the coverage allowed — a candidate
    shown as MEDIUM because it was capped is a different thing from one that
    scored MEDIUM, and collapsing them hides the missing producer that caused it.
    """
    uncapped = tier_for(score)
    if uncapped is None:
        return TierDecision(None, None, False, coverage)

    ceiling = coverage_ceiling(coverage)
    order = (TIER_LOW, TIER_MEDIUM, TIER_HIGH)
    if order.index(uncapped) <= order.index(ceiling):
        return TierDecision(uncapped, uncapped, False, coverage)
    return TierDecision(ceiling, uncapped, True, coverage)


# ── Dedup gate ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DedupOutcome:
    """Result of comparing a candidate against governed metric views and the
    space's own instruction definitions."""

    verdict: str = VERDICT_PROPOSE
    blocked_by: str | None = None
    alternatives: tuple[dict[str, Any], ...] = ()
    conflicts: tuple[dict[str, Any], ...] = ()


def metric_view_fields(yamls: Mapping[str, Mapping[str, Any]]) -> tuple[MetricViewField, ...]:
    """Flatten ``detect_metric_views_via_catalog``'s YAML dicts into fields.

    Consumes that function's second return value — ``{fq_lower: parsed_yaml}``
    — so the ``DESCRIBE ... AS JSON`` parsing stays in ``metric_view_catalog``
    and this module never issues a DESCRIBE of its own.
    """
    out: list[MetricViewField] = []
    for fqn, definition in (yamls or {}).items():
        if not isinstance(definition, Mapping):
            continue
        for kind, key in ((FIELD_MEASURE, "measures"), (FIELD_DIMENSION, "dimensions")):
            for entry in definition.get(key) or ():
                if not isinstance(entry, Mapping):
                    continue
                name = str(entry.get("name") or "").strip()
                if not name:
                    continue
                expr = str(entry.get("expr") or "")
                out.append(
                    MetricViewField(
                        mv_fqn=str(fqn),
                        field_name=name,
                        kind=kind,
                        expr=expr,
                        canonical_expr=canonicalize_expr(expr) if expr else "",
                        text=_field_text(name, entry),
                        source_columns=_expr_columns(expr),
                    )
                )
    return tuple(out)


TRUSTED_ASSET_SOURCE_PREFIX = "trusted_asset"
"""Prefix on a conflict's ``source`` when the authority is a curated answer.

The suffix is the asset's **id**, never its question. ``conflicts[]`` is
persisted and rendered, and a benchmark question copied into it would be exactly
the leak the firewall exists to stop — while the id is enough for a reviewer to
open the asset in the space.
"""


def _coerce_curated_sql(value: Any) -> str:
    """One curated SQL string, whether the field stored a string or a list.

    ``example_question_sqls[].sql`` (and the snippet collections) is ``list[str]``
    in the serialized_space contract but a bare string in some
    programmatic/legacy payloads and in the tests. Both collapse to one
    whitespace-joined statement here so a single reader serves every consumer.
    """
    if isinstance(value, (list, tuple)):
        return " ".join(str(part) for part in value if part).strip()
    return str(value or "").strip()


def example_question_sql_statements(
    config: Mapping[str, Any] | None,
) -> tuple[tuple[str, str], ...]:
    """``(identifier, sql)`` for every ``instructions.example_question_sqls`` entry.

    The single reader of that field. :func:`trusted_asset_definitions` (the
    conflict surface) and the advisor's curated-corpus harvest both consume it, so
    the two cannot drift over what counts as a curated asset or how its id is
    formed — a second reader of the same field is how they would start
    disagreeing. ``identifier`` is the asset id, falling back to ``index:<n>`` so
    an id-less asset stays locatable; the SQL is coerced but not otherwise
    parsed here.
    """
    if not isinstance(config, Mapping):
        return ()
    instructions = config.get("instructions")
    if not isinstance(instructions, Mapping):
        return ()
    out: list[tuple[str, str]] = []
    for ordinal, asset in enumerate(instructions.get("example_question_sqls") or ()):
        if not isinstance(asset, Mapping):
            continue
        sql = _coerce_curated_sql(asset.get("sql") or asset.get("answer"))
        if not sql:
            continue
        identifier = str(asset.get("id") or "").strip() or f"index:{ordinal}"
        out.append((identifier, sql))
    return tuple(out)


def trusted_asset_definitions(
    config: Mapping[str, Any] | None,
) -> tuple[InstructionDefinition, ...]:
    """Measure definitions carried by ``instructions.example_question_sqls``.

    POV Part 5 step 3 makes trusted assets authoritative in a conflict, but they
    live in a different field from ``text_instructions`` and were never read here,
    so a proposal could contradict a curated answer and reach ``PROPOSE``
    unchallenged. This closes that: every curated SQL is parsed through
    ``mv_fingerprint.extract_measures`` — the same extractor the corpus scan uses,
    so a measure means one thing in this codebase — and each aggregate it finds
    becomes a definition the dedup gate can disagree with.

    Only the SQL is read. The question text beside it is not consumed, compared,
    or persisted; ``concept`` stays empty precisely because the only name on offer
    is that text. Matching therefore happens on the measure, which is what
    ``_defines_same_quantity`` is for.
    """
    out: list[InstructionDefinition] = []
    seen: set[tuple[str, str]] = set()
    for identifier, sql in example_question_sql_statements(config):
        source = f"{TRUSTED_ASSET_SOURCE_PREFIX}:{identifier}"
        try:
            measures = extract_measures(sql)
        except Exception:
            logger.warning(
                "mv_scoring: could not parse trusted asset %s for conflict "
                "detection", source, exc_info=True,
            )
            continue
        for measure in measures:
            if not measure.canonical_expr:
                continue
            key = (source, measure.canonical_expr)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                InstructionDefinition(
                    source=source,
                    concept="",
                    expr=measure.canonical_expr,
                    canonical_expr=measure.canonical_expr,
                    measure_columns=frozenset(measure.source_columns),
                    aggregate=measure.aggregate,
                )
            )
    return tuple(out)


def _field_text(name: str, entry: Mapping[str, Any]) -> str:
    """Assemble the natural-language surface of a field for embedding."""
    parts = [name, str(entry.get("display_name") or ""), str(entry.get("comment") or "")]
    synonyms = entry.get("synonyms") or ()
    if isinstance(synonyms, (list, tuple)):
        parts.extend(str(synonym) for synonym in synonyms)
    return " ".join(part.strip() for part in parts if part and str(part).strip())


def _expr_columns(expr: str) -> frozenset[str]:
    """Unqualified column names an aggregate expression reads.

    Delegates to ``mv_fingerprint.extract_measures``, which resolves columns off
    the parsed AST. A text scan over the canonical form would be simpler and
    wrong in a way that matters: it cannot tell the function name ``sum`` from a
    column, so every pair of SUM measures would appear to share a column and
    ``alternatives[]`` would fill with unrelated fields. Unparseable input yields
    an empty set, which costs a partial-overlap ranking and nothing else.
    """
    if not expr:
        return frozenset()
    columns: set[str] = set()
    for measure in extract_measures(expr):
        columns.update(measure.source_columns)
    return frozenset(columns)


def dedup_gate(
    candidate: MetricViewCandidate,
    *,
    mv_fields: Sequence[MetricViewField] = (),
    instructions: Sequence[InstructionDefinition] = (),
) -> DedupOutcome:
    """POV Part 3 and Part 5 gate: block, offer alternatives, or flag a conflict.

    Three outcomes, in precedence order:

    1. An instruction or trusted asset defines the same thing differently ->
       ``CONFLICT``. Never a suggestion, never auto-resolved: the reviewer sees
       both definitions and adjudicates.
    2. The candidate's canonical measure matches a governed measure ->
       ``BLOCKED``, with a pointer to the metric view and field that already
       governs it.
    3. Otherwise -> ``PROPOSE``, with any partially-overlapping governed fields
       ranked into ``alternatives[]``.

    Conflict is checked before block because the two are not exclusive and the
    weaker answer is the wrong one: silently blocking on a governed measure that
    an instruction contradicts hides the contradiction, which is the failure
    POV Part 5 exists to prevent.

    Whether a governed metric view must exist depends on *how* the definition was
    matched, and the difference is the strength of the evidence:

    * Matched by **measure** — same aggregate over the same columns, different
      canonical form. This is direct evidence about the very expression being
      proposed, and it needs no corroboration: a curated trusted asset already
      answers this question differently, and shipping a second answer is the
      silently-wrong-numbers outcome. POV Part 5 step 3 puts the trusted asset on
      the authoritative side.
    * Matched by **concept name** — a text instruction claims the same concept.
      That still requires a governed measure to contradict, because a prose
      instruction naming "revenue" is not yet a definition of *this* expression;
      an instruction alone is what the advisor exists to act on rather than
      escalate.
    """
    canonical = candidate.canonical_measure_expr
    if not canonical:
        return DedupOutcome()

    measures = [f for f in mv_fields if f.kind == FIELD_MEASURE]
    exact = [f for f in measures if f.canonical_expr and f.canonical_expr == canonical]
    by_measure, by_concept = _conflicting_definitions(candidate, canonical, instructions)
    conflicting = by_measure + (by_concept if exact else ())

    if conflicting:
        return DedupOutcome(
            verdict=VERDICT_CONFLICT,
            blocked_by=exact[0].pointer if exact else "",
            conflicts=tuple(
                _conflict_entry(definition, canonical, exact[0] if exact else None)
                for definition in conflicting
            ),
        )
    if exact:
        return DedupOutcome(verdict=VERDICT_BLOCKED, blocked_by=exact[0].pointer)
    return DedupOutcome(
        verdict=VERDICT_PROPOSE,
        alternatives=_partial_alternatives(candidate, measures, canonical),
    )


def _conflicting_definitions(
    candidate: MetricViewCandidate,
    canonical: str,
    instructions: Sequence[InstructionDefinition],
) -> tuple[tuple[InstructionDefinition, ...], tuple[InstructionDefinition, ...]]:
    """Definitions that disagree with the candidate, split by how they matched.

    Returned as ``(by_measure, by_concept)`` because the caller weighs the two
    differently — see :func:`dedup_gate`. Two routes exist because the two
    definition sources identify themselves differently: a text instruction names
    a concept, so it matches on that name, while a trusted-asset SQL has no name.
    Matching an asset on its question text would be both unreliable and a firewall
    violation, so it matches on the measure itself: same aggregate, same columns.

    Divergence is required in either route. An identical canonical expression is
    agreement, and agreement is not something to escalate.
    """
    concept = (candidate.concept or "").strip().lower()
    by_measure: list[InstructionDefinition] = []
    by_concept: list[InstructionDefinition] = []
    for definition in instructions:
        existing = definition.canonical_expr or canonicalize_expr(definition.expr)
        if not existing or existing == canonical:
            continue
        if _defines_same_quantity(definition, candidate):
            by_measure.append(definition)
        elif bool(concept) and (definition.concept or "").strip().lower() == concept:
            by_concept.append(definition)
    return tuple(by_measure), tuple(by_concept)


def _defines_same_quantity(
    definition: InstructionDefinition,
    candidate: MetricViewCandidate,
) -> bool:
    """Whether a definition and a candidate aggregate the same columns the same way.

    Both sides must actually carry a column set: an empty one is unknown
    provenance, and treating unknown as a match would let one unparsed asset
    conflict with every candidate in the space.
    """
    left = _bare_columns(definition.measure_columns)
    right = _bare_columns(candidate.measure_columns)
    if not left or not right or left != right:
        return False
    aggregate = (definition.aggregate or "").strip().lower()
    return bool(aggregate) and aggregate == _leading_aggregate(
        candidate.canonical_measure_expr
    )


def _bare_columns(columns: Iterable[str]) -> frozenset[str]:
    """Unqualified lowercase column names.

    Both sides are reduced the same way because they arrive differently:
    ``candidate_from_measure`` already strips qualifiers, while a definition read
    off a trusted asset carries whatever the SQL wrote. Comparing the two without
    agreeing on the spelling would make ``lineitem.l_extendedprice`` and
    ``l_extendedprice`` look like different columns and quietly disable the check.
    """
    return frozenset(
        str(column).strip().rpartition(".")[2].strip("`").lower()
        for column in columns
        if str(column).strip()
    )


def _leading_aggregate(canonical: str) -> str:
    """The function name a canonical measure expression opens with.

    Canonical forms are rendered lowercase with the function first
    (``sum(l_extendedprice * (?n - l_discount))``), so the name is the text before
    the first parenthesis. Read rather than stored because the candidate's
    aggregate is not a field on it, and re-canonicalizing to recover one would be
    a second implementation of the thing MV-D10 keeps single.
    """
    head = str(canonical or "").split("(", 1)[0].strip().lower()
    return head if head.isidentifier() else ""


def _conflict_entry(
    definition: InstructionDefinition,
    canonical: str,
    governed: MetricViewField | None,
) -> dict[str, Any]:
    """One ``conflicts[]`` entry. Both expressions are canonical, hence
    literal-free and safe to persist and render.

    ``authoritative`` records which side wins if a reviewer just applies the
    house rule: the existing definition, per POV Part 5 step 3. It is stated
    rather than acted on — ``resolution`` stays human adjudication, because the
    engine knowing which side is authoritative is not the same as it knowing
    which side is right.
    """
    return {
        "source": definition.source,
        "concept": definition.concept,
        "existing_expr": definition.canonical_expr or canonicalize_expr(definition.expr),
        "proposed_expr": canonical,
        "governed_by": governed.pointer if governed is not None else None,
        "authoritative": definition.source,
        "resolution": "requires_human_adjudication",
    }


def _partial_alternatives(
    candidate: MetricViewCandidate,
    measures: Sequence[MetricViewField],
    canonical: str,
) -> tuple[dict[str, Any], ...]:
    """Governed measures sharing columns with the candidate but not its shape.

    POV Part 3: when several metric views partially match one fingerprint, the
    primary candidate carries the others under ``alternatives[]`` rather than
    the engine picking silently. Ranked by overlap descending, then by pointer
    so the order is stable across runs.
    """
    candidate_columns = frozenset(candidate.measure_columns) or _expr_columns(
        candidate.measure_expr
    )
    if not candidate_columns:
        return ()

    scored = []
    for measure in measures:
        if measure.canonical_expr == canonical:
            continue
        shared = candidate_columns & measure.source_columns
        if not shared:
            continue
        scored.append((len(shared), measure.pointer, sorted(shared)))

    scored.sort(key=lambda row: (-row[0], row[1]))
    return tuple(
        {
            "metric_view": pointer.rsplit(".", 1)[0],
            "field": pointer.rsplit(".", 1)[-1],
            "shared_columns": shared,
            "overlap": overlap,
            "reason": "partial_column_overlap",
        }
        for overlap, pointer, shared in scored
    )


# ── Proposal assembly ────────────────────────────────────────────────────


@dataclass(frozen=True)
class ScoredProposal:
    """A scored candidate and its POV Part 4 payload."""

    suggestion_id: str
    dedup_fingerprint: str
    target_space_id: str
    candidate_type: str
    verdict: str
    components: ScoreComponents
    confidence_score: float
    tier: str | None = None
    proposed_object: str | None = None
    run_id: str | None = None
    blocked_by: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)
    provenance: Mapping[str, Any] = field(default_factory=dict)
    alternatives: tuple[dict[str, Any], ...] = ()
    conflicts: tuple[dict[str, Any], ...] = ()
    uncapped_tier: str | None = None
    tier_capped_by_coverage: bool = False

    @property
    def evidence_coverage(self) -> float:
        """MV-D15 coverage, read off the components rather than stored twice."""
        return self.components.evidence_coverage

    @property
    def is_persistable(self) -> bool:
        """Whether this reaches ``genie_opt_mv_candidates`` — see
        :data:`PERSISTABLE_VERDICTS`."""
        return self.verdict in PERSISTABLE_VERDICTS

    @property
    def is_suggestion(self) -> bool:
        """A ``CONFLICT`` is persisted for adjudication but is never a suggestion."""
        return self.verdict == VERDICT_PROPOSE

    def to_payload(self) -> dict[str, Any]:
        """The POV Part 4 proposal JSON.

        ``type`` rather than ``candidate_type``: the payload is the API/UI
        surface and POV names the field ``type``; the Delta column carries the
        ``candidate_type`` spelling to avoid shadowing the builtin.
        """
        return {
            "suggestion_id": self.suggestion_id,
            "type": self.candidate_type,
            "confidence_score": self.confidence_score,
            "tier": self.tier,
            "uncapped_tier": self.uncapped_tier,
            "tier_capped_by_coverage": self.tier_capped_by_coverage,
            "evidence_coverage": self.evidence_coverage,
            "target_space_id": self.target_space_id,
            "proposed_object": self.proposed_object,
            "score_components": self.components.to_dict(),
            "evidence": dict(self.evidence),
            "provenance": dict(self.provenance),
            "dedup_fingerprint": self.dedup_fingerprint,
            "alternatives": [dict(entry) for entry in self.alternatives],
            "conflicts": [dict(entry) for entry in self.conflicts],
        }


def suggestion_id_for(dedup_fingerprint: str) -> str:
    """``sug_`` plus the first 12 hex characters of the dedup fingerprint.

    Derived rather than random so a re-proposing run produces the same id for
    the same candidate, which is what lets the id be quoted in a UI, an audit
    row and a decision record without a lookup table.
    """
    return f"sug_{(dedup_fingerprint or '')[:12]}"


def _default_statuses(semantic: SemanticMatch) -> dict[str, str]:
    """Per-signal statuses this module can determine on its own (MV-D15).

    Only **S** is knowable here — :func:`semantic_score` reports whether it
    reached an endpoint. L, Y and D are precomputed by the caller, so only the
    caller knows whether a producer ran; they default to ``COMPUTED`` and the
    caller overrides the ones that did not. The caller's map is merged **over**
    this one, so naming L does not silently reset S.
    """
    return {
        "L": MV_SIGNAL_COMPUTED,
        "Y": MV_SIGNAL_COMPUTED,
        "S": semantic.status,
        "D": MV_SIGNAL_COMPUTED,
    }


def score_candidate(
    candidate: MetricViewCandidate,
    *,
    run_id: str | None = None,
    mv_fields: Sequence[MetricViewField] = (),
    instructions: Sequence[InstructionDefinition] = (),
    intent_texts: Sequence[str] = (),
    embedding_client: EmbeddingClient | None = None,
    weights: Mapping[str, float] | None = None,
    statuses: Mapping[str, str] | None = None,
    auth_identity: str = "OBO",
    generated_at: str | None = None,
) -> ScoredProposal:
    """Run the gate, blend the signals, and assemble the proposal payload.

    The gate runs first because it can change what the score means: a blocked
    candidate skips the embedding call entirely (there is nothing to propose, so
    paying for FMAPI round trips would be waste), while L, Y and D are pure
    arithmetic and are always computed so a blocked row still carries the
    evidence that explains it.
    """
    outcome = dedup_gate(candidate, mv_fields=mv_fields, instructions=instructions)

    if outcome.verdict == VERDICT_BLOCKED:
        semantic = SemanticMatch()
    else:
        reference_kind, references = semantic_reference_for(candidate, mv_fields)
        semantic = semantic_score(
            intent_texts, references, embedding_client, reference_kind=reference_kind
        )
    components = ScoreComponents(
        L=lineage_overlap_score(candidate.lineage),
        Y=syntactic_score(candidate.recurrence),
        S=semantic.cosine,
        D=demand_score(candidate.demand),
        weights=dict(weights) if weights is not None else dict(MV_SCORE_WEIGHTS),
        statuses={**_default_statuses(semantic), **dict(statuses or {})},
    )
    score = blended_score(components)
    decision = capped_tier(score, components.evidence_coverage)
    tier = decision.tier

    verdict = outcome.verdict
    if verdict == VERDICT_PROPOSE and tier is None:
        verdict = VERDICT_SUPPRESSED

    candidate_type = (
        VERDICT_CONFLICT if verdict == VERDICT_CONFLICT else candidate.candidate_type
    )
    if candidate_type not in MV_CANDIDATE_TYPES:
        raise ValueError(
            f"candidate_type must be one of {MV_CANDIDATE_TYPES}, got {candidate_type!r}"
        )

    fingerprint = mv_candidate_fingerprint(
        candidate.space_id,
        candidate.canonical_measure_expr,
        candidate.source_tables,
    )
    return ScoredProposal(
        suggestion_id=suggestion_id_for(fingerprint),
        dedup_fingerprint=fingerprint,
        target_space_id=candidate.space_id,
        candidate_type=candidate_type,
        verdict=verdict,
        components=components,
        confidence_score=score,
        tier=tier if verdict != VERDICT_SUPPRESSED else None,
        uncapped_tier=decision.uncapped_tier if verdict != VERDICT_SUPPRESSED else None,
        tier_capped_by_coverage=decision.capped_by_coverage,
        proposed_object=candidate.proposed_object,
        run_id=run_id,
        blocked_by=outcome.blocked_by,
        evidence=_evidence(candidate, semantic),
        provenance=_provenance(run_id, auth_identity, generated_at),
        alternatives=outcome.alternatives,
        conflicts=outcome.conflicts,
    )


def _evidence(candidate: MetricViewCandidate, semantic: SemanticMatch) -> dict[str, Any]:
    """POV Part 4 ``evidence``. Ids and canonical text only — never question text."""
    return {
        "ast_fingerprint_recurrence": candidate.recurrence.recurrence,
        "ast_equivalent": candidate.recurrence.ast_equivalent,
        "ast_curated_provenance_count": candidate.recurrence.curated_provenance_count,
        "benchmark_questions": list(candidate.benchmark_question_ids),
        "query_history_statement_ids": list(candidate.query_history_statement_ids),
        "lineage_source_tables": list(candidate.source_tables),
        "lineage_reference_kind": candidate.lineage.reference_kind,
        "semantic_top_match": semantic.to_dict(),
    }


def _provenance(
    run_id: str | None, auth_identity: str, generated_at: str | None
) -> dict[str, Any]:
    """POV Part 4 ``provenance``.

    ``gso_task_key`` is ``optimize``: MV-D3 runs the advisor as a gated phase
    inside the existing task, so recording a dedicated ``metric_view_advisor``
    task key would name a task that does not exist in the four-task DAG.
    """
    return {
        "generated_by": MV_ADVISOR_GENERATED_BY,
        "auth_identity": auth_identity,
        "gso_run_id": run_id,
        "gso_task_key": "optimize",
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
    }


# ── Persistence ──────────────────────────────────────────────────────────


def persist_proposal(
    spark: SparkSession,
    proposal: ScoredProposal,
    *,
    catalog: str,
    schema: str,
    run_id: str | None = None,
    requested_mode: str | None = None,
    effective_mode: str | None = None,
) -> str | None:
    """Upsert a proposal through the Prompt 1 accessor. Returns its fingerprint,
    or ``None`` when the verdict is not persistable.

    Blocked and suppressed verdicts are skipped rather than written — see
    :data:`PERSISTABLE_VERDICTS`. Human decision columns are untouched here, so
    a re-proposing run cannot resurrect a candidate a user rejected.
    """
    if not proposal.is_persistable:
        logger.info(
            "mv_scoring: not persisting %s (verdict=%s, blocked_by=%s)",
            proposal.suggestion_id, proposal.verdict, proposal.blocked_by,
        )
        return None

    effective_run_id = run_id or proposal.run_id
    if not effective_run_id:
        raise ValueError("run_id is required to persist a metric view candidate")

    fingerprint = upsert_mv_candidate(
        spark,
        catalog=catalog,
        schema=schema,
        run_id=effective_run_id,
        target_space_id=proposal.target_space_id,
        suggestion_id=proposal.suggestion_id,
        dedup_fingerprint=proposal.dedup_fingerprint,
        candidate_type=proposal.candidate_type,
        confidence_score=proposal.confidence_score,
        tier=proposal.tier,
        uncapped_tier=proposal.uncapped_tier,
        tier_capped_by_coverage=proposal.tier_capped_by_coverage,
        proposed_object=proposal.proposed_object,
        score_components=proposal.components.to_dict(),
        evidence=dict(proposal.evidence),
        provenance=dict(proposal.provenance),
        alternatives=[dict(entry) for entry in proposal.alternatives],
        conflicts=[dict(entry) for entry in proposal.conflicts],
        requested_mode=requested_mode,
        effective_mode=effective_mode,
    )

    # MV-D30 as-implemented (Prompt 15.6): when this run persists a view-grained
    # bundle, retire any legacy per-measure candidate it covers so the in-job
    # advisor and the interactive suggest surface agree on the served grain. The
    # member fingerprints ride in evidence["measures"][].dedup_fingerprint. This
    # mirrors the backend ``mv_suggest._persist`` twin.
    evidence = dict(proposal.evidence)
    if evidence.get("bundle"):
        member_fps = [
            str(m.get("dedup_fingerprint"))
            for m in evidence.get("measures", [])
            if isinstance(m, dict) and m.get("dedup_fingerprint")
        ]
        if member_fps:
            supersede_legacy_mv_candidates(
                spark,
                catalog=catalog,
                schema=schema,
                target_space_id=proposal.target_space_id,
                member_fingerprints=member_fps,
                superseded_by=proposal.dedup_fingerprint,
            )

    return fingerprint


def score_candidates(
    candidates: Iterable[MetricViewCandidate],
    **kwargs: Any,
) -> tuple[ScoredProposal, ...]:
    """Score many candidates, highest confidence first.

    Suggestions sort ahead of conflicts at equal score, because a queue that
    leads with items requiring adjudication buries the ones a reviewer can act
    on immediately.
    """
    scored = [score_candidate(candidate, **kwargs) for candidate in candidates]
    scored.sort(key=lambda p: (-p.confidence_score, not p.is_suggestion, p.suggestion_id))
    return tuple(scored)


__all__ = [
    "FIELD_DIMENSION",
    "FIELD_MEASURE",
    "PERSISTABLE_VERDICTS",
    "REFERENCE_GOVERNED_MV",
    "REFERENCE_LINEAGE_FOOTPRINT",
    "SEMANTIC_REF_GOVERNED_MV_FIELDS",
    "SEMANTIC_REF_NONE",
    "SEMANTIC_REF_SOURCE_COLUMN_METADATA",
    "TIER_HIGH",
    "TIER_LOW",
    "TIER_MEDIUM",
    "VERDICT_BLOCKED",
    "VERDICT_CONFLICT",
    "VERDICT_PROPOSE",
    "VERDICT_SUPPRESSED",
    "DedupOutcome",
    "DemandSignal",
    "EmbeddingClient",
    "FoundationModelEmbeddingClient",
    "InstructionDefinition",
    "LineageOverlap",
    "MetricViewCandidate",
    "MetricViewField",
    "RecurrenceSignal",
    "ScoreComponents",
    "ScoredProposal",
    "SemanticMatch",
    "SemanticReference",
    "SourceColumnMetadata",
    "TierDecision",
    "blended_score",
    "capped_tier",
    "coverage_ceiling",
    "TRUSTED_ASSET_SOURCE_PREFIX",
    "dedup_gate",
    "example_question_sql_statements",
    "trusted_asset_definitions",
    "demand_decay",
    "demand_score",
    "lineage_overlap_score",
    "metric_view_fields",
    "normalized_recurrence",
    "persist_proposal",
    "score_candidate",
    "score_candidates",
    "semantic_reference_for",
    "semantic_score",
    "suggestion_id_for",
    "syntactic_score",
    "tier_for",
]
