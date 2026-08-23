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
    MV_DEMAND_BREADTH_SATURATION,
    MV_DEMAND_COST_SATURATION_MS,
    MV_DEMAND_FREQUENCY_SATURATION,
    MV_DEMAND_HALF_LIFE_DAYS,
    MV_EMBEDDING_ENDPOINT,
    MV_RECURRENCE_SATURATION,
    MV_SCORE_WEIGHTS,
    MV_TIER_HIGH_MIN,
    MV_TIER_LOW_MIN,
    MV_TIER_MEDIUM_MIN,
)

from .mv_fingerprint import canonicalize_expr, extract_measures
from .mv_state import MV_CANDIDATE_TYPES, mv_candidate_fingerprint, upsert_mv_candidate

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
    """

    canonical_expr: str = ""
    recurrence: int = 0
    provenance_count: int = 0
    ast_equivalent: bool = True


@dataclass(frozen=True)
class DemandSignal:
    """Precomputed query-history demand for the **D** signal.

    ``cost_ms`` is cumulative execution time attributable to the measure — the
    repo's available cost proxy (``wide_schema_history`` reads duration, not
    DBUs). ``age_days`` is measured from the most recent occurrence, so the
    half-life decays staleness rather than the measure's whole history.
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
    """

    source: str
    concept: str
    expr: str = ""
    canonical_expr: str = ""


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
    """

    field: str | None = None
    cosine: float = 0.0
    reference_kind: str = SEMANTIC_REF_NONE

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "cosine": self.cosine,
            "reference_kind": self.reference_kind,
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
    """**Y**: normalized recurrence times the corpus-internal equivalence flag.

    See the module docstring: the flag says the counted occurrences share one
    canonical form. It does *not* ask whether a governed metric view already
    defines the measure — that question is the dedup gate's, and its answer
    blocks rather than damps.
    """
    if not signal.ast_equivalent:
        return 0.0
    return normalized_recurrence(signal.recurrence, saturation)


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
    if not intents or not targets or client is None:
        return SemanticMatch()

    try:
        vectors = client.embed([*intents, *[ref.text for ref in targets]])
    except Exception:
        logger.warning("mv_scoring: embedding call failed; S degrades to 0.0", exc_info=True)
        return SemanticMatch()

    if len(vectors) != len(intents) + len(targets):
        logger.warning(
            "mv_scoring: embedding client returned %d vectors for %d texts; S degrades to 0.0",
            len(vectors), len(intents) + len(targets),
        )
        return SemanticMatch()

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
        return SemanticMatch(reference_kind=reference_kind)
    return SemanticMatch(field=best_field, cosine=best_cosine, reference_kind=reference_kind)


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


@dataclass(frozen=True)
class ScoreComponents:
    """The four signals and the weights they were blended with (POV Part 4
    ``score_components``)."""

    L: float = 0.0
    Y: float = 0.0
    S: float = 0.0
    D: float = 0.0
    weights: Mapping[str, float] = field(default_factory=lambda: dict(MV_SCORE_WEIGHTS))

    def weighted_terms(self) -> dict[str, float]:
        """Per-signal contributions before the ``* 100``.

        Rounded to six places for reporting only — the blend itself is not
        rounded. Without this, a term prints as ``0.08000000000000002`` and a
        reviewer reconciling a score against the POV worksheet has to squint.
        """
        return {
            key: round(self.weights.get(key, 0.0) * getattr(self, key), 6)
            for key in ("L", "Y", "S", "D")
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "L": self.L,
            "Y": self.Y,
            "S": self.S,
            "D": self.D,
            "weights": dict(self.weights),
        }


def blended_score(components: ScoreComponents) -> float:
    """``100 * (0.35L + 0.30Y + 0.20S + 0.15D)``, unrounded.

    Left unrounded on purpose. Both POV Part 3 worked examples land on exact
    IEEE doubles (80.0 and 58.75), and rounding to two places would drag a
    24.999 across the suppression floor into LOW — a rounding rule that changes
    which candidates reach a human is not a presentation detail.
    """
    weights = components.weights
    return 100.0 * sum(
        weights.get(key, 0.0) * getattr(components, key) for key in ("L", "Y", "S", "D")
    )


def tier_for(score: float) -> str | None:
    """POV Part 3 thresholds. ``None`` means suppress (below 25)."""
    if score >= MV_TIER_HIGH_MIN:
        return TIER_HIGH
    if score >= MV_TIER_MEDIUM_MIN:
        return TIER_MEDIUM
    if score >= MV_TIER_LOW_MIN:
        return TIER_LOW
    return None


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

    1. The candidate's canonical measure matches a governed measure **and** an
       instruction or trusted asset defines the same concept differently ->
       ``CONFLICT``. Never a suggestion, never auto-resolved: the reviewer sees
       both definitions and adjudicates.
    2. It matches a governed measure -> ``BLOCKED``, with a pointer to the
       metric view and field that already governs it.
    3. Otherwise -> ``PROPOSE``, with any partially-overlapping governed fields
       ranked into ``alternatives[]``.

    Conflict is checked before block because the two are not exclusive and the
    weaker answer is the wrong one: silently blocking on a governed measure that
    an instruction contradicts hides the contradiction, which is the failure
    POV Part 5 exists to prevent.
    """
    canonical = candidate.canonical_measure_expr
    if not canonical:
        return DedupOutcome()

    measures = [f for f in mv_fields if f.kind == FIELD_MEASURE]
    exact = [f for f in measures if f.canonical_expr and f.canonical_expr == canonical]
    conflicting = _conflicting_definitions(candidate, canonical, instructions)

    if exact and conflicting:
        return DedupOutcome(
            verdict=VERDICT_CONFLICT,
            blocked_by=exact[0].pointer,
            conflicts=tuple(
                _conflict_entry(definition, canonical, exact[0]) for definition in conflicting
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
) -> tuple[InstructionDefinition, ...]:
    """Instructions naming the candidate's concept but defining it differently."""
    concept = (candidate.concept or "").strip().lower()
    if not concept:
        return ()
    out = []
    for definition in instructions:
        if (definition.concept or "").strip().lower() != concept:
            continue
        existing = definition.canonical_expr or canonicalize_expr(definition.expr)
        if existing and existing != canonical:
            out.append(definition)
    return tuple(out)


def _conflict_entry(
    definition: InstructionDefinition,
    canonical: str,
    governed: MetricViewField,
) -> dict[str, Any]:
    """One ``conflicts[]`` entry. Both expressions are canonical, hence
    literal-free and safe to persist and render."""
    return {
        "source": definition.source,
        "concept": definition.concept,
        "existing_expr": definition.canonical_expr or canonicalize_expr(definition.expr),
        "proposed_expr": canonical,
        "governed_by": governed.pointer,
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


def score_candidate(
    candidate: MetricViewCandidate,
    *,
    run_id: str | None = None,
    mv_fields: Sequence[MetricViewField] = (),
    instructions: Sequence[InstructionDefinition] = (),
    intent_texts: Sequence[str] = (),
    embedding_client: EmbeddingClient | None = None,
    weights: Mapping[str, float] | None = None,
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
    )
    score = blended_score(components)
    tier = tier_for(score)

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

    return upsert_mv_candidate(
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
        proposed_object=proposal.proposed_object,
        score_components=proposal.components.to_dict(),
        evidence=dict(proposal.evidence),
        provenance=dict(proposal.provenance),
        alternatives=[dict(entry) for entry in proposal.alternatives],
        conflicts=[dict(entry) for entry in proposal.conflicts],
        requested_mode=requested_mode,
        effective_mode=effective_mode,
    )


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
    "blended_score",
    "dedup_gate",
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
