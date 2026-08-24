"""The metric view advisor phase: corpus in, scored proposals and YAML out.

Runs inside the ``optimize`` task after the unified loop, never as a job task of
its own (MV-D3). It is a *phase* in the strict sense the rules use: gated off by
default, wrapped so no failure of its own can reach the task, and communicating
with the rest of the run only through Delta tables keyed by ``run_id``.

What this module is, and is not
-------------------------------
It is an assembler. Every step it performs is owned elsewhere and called here:
``corpus_scan`` fingerprints the SQL, ``metric_view_catalog`` reads the estate,
``mv_scoring`` blends and dedups, ``mv_yaml`` renders and validates, ``state``
and ``mv_state`` persist. It deliberately owns no scoring rule and **no YAML
generation** — the sole-renderer guard in ``test_mv_yaml`` fails if this module
ever builds a definition or a ``CREATE VIEW`` string itself.

The advisor ONLY proposes, in every mode. It issues no ``CREATE``, no ``GRANT``
and no config mutation. Under MV-D1 object creation is a backend/OBO surface, and
the job runs as the service principal.

Signal availability (MV-D15)
----------------------------
Two of the four blend signals have no producer, and this module is where that
fact is recorded rather than hidden:

- **L** is ``UNAVAILABLE``. Nothing in the repository computes column-set
  overlap, and column-level lineage additionally needs a system-tables grant the
  service principal does not hold. No producer is synthesized here.
- **D** is ``UNAVAILABLE``. Approximating it was considered and refused: the
  corpus scored here is *generated SQL from benchmark questions*, while
  ``wide_schema_history``'s counts are *real query-history traffic*, so
  attributing the latter to the former is conflation rather than measurement —
  and the only per-measure recurrence that does exist is already **Y**, so
  routing it into D would spend one piece of evidence twice.
- **Y** is ``COMPUTED`` from the corpus scan.
- **S** reports its own status: ``COMPUTED`` when it reached an endpoint,
  ``EMPTY`` when it ran with nothing to compare, ``UNAVAILABLE`` when the
  endpoint is absent or failed.

So coverage is 0.50 today and nothing can present above MEDIUM. That is the
intended state, and it is legible because the coverage figure rides on every
candidate rather than being inferable only by someone who knows which producers
exist.

The empty-corpus trap
---------------------
``rows_json`` is empty whenever an eval run reached a non-success terminal status
— the runner returns no rows rather than raising. An empty corpus therefore means
"the evaluation did not produce SQL", not "the space has no recurring measures",
and the two must not share an outcome. Every way of arriving at no usable SQL is
a first-class ``SKIP`` with a recorded reason.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from genie_space_optimizer.common.config import (
    MV_ADVISOR_MAX_CANDIDATES,
    MV_ADVISOR_PHASE_NAME,
    MV_SIGNAL_UNAVAILABLE,
)

from .leakage import BenchmarkCorpus, LeakageOracle
from .mv_fingerprint import (
    CURATED_PROVENANCE_KIND,
    CorpusScan,
    FingerprintRecurrence,
    corpus_scan,
)
from .mv_scoring import (
    FIELD_MEASURE,
    DemandSignal,
    LineageOverlap,
    MetricViewCandidate,
    RecurrenceSignal,
    ScoredProposal,
    SourceColumnMetadata,
    example_question_sql_statements,
    metric_view_fields,
    persist_proposal,
    score_candidate,
    trusted_asset_definitions,
)
from .mv_yaml import ColumnFacts, MeasureRequest, MvProfiling, create_ddl, generate, validate
from .state import load_all_full_iterations, load_patches, write_artifact, write_stage

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# ── Outcomes ─────────────────────────────────────────────────────────────

STATUS_COMPLETE = "COMPLETE"
STATUS_SKIPPED = "SKIPPED"
STATUS_FAILED = "FAILED"

SKIP_DISABLED = "DISABLED"
"""``enable_metric_view_suggestions`` was not ``"true"``. Zero cost when off."""

SKIP_NO_ITERATIONS = "NO_ITERATIONS"
"""No full-scope iteration exists for this run — the loop never evaluated."""

SKIP_NO_ITERATION_ZERO = "NO_ITERATION_ZERO"
"""Iterations exist but none is iteration 0, so there is no baseline corpus."""

SKIP_EMPTY_ROWS_JSON = "EMPTY_ROWS_JSON"
"""Iteration 0 exists and carries no rows.

The eval runner returns an empty row list on any non-success terminal status, so
this is the common shape of a failed evaluation and **not** evidence about the
space. Recorded distinctly for exactly that reason.
"""

SKIP_NO_GENERATED_SQL = "NO_GENERATED_SQL"
"""Rows exist but none carries generated SQL — an eval that answered nothing."""

SKIP_NO_PARSEABLE_SQL = "NO_PARSEABLE_SQL"
"""SQL was present and none of it parsed. A scanner problem, not a corpus one."""

SKIP_NO_CANDIDATES = "NO_CANDIDATES"
"""The corpus parsed and contained no aggregate worth proposing. A real finding:
the space's queries do not repeat a measure."""


@dataclass(frozen=True)
class CorpusLoad:
    """Iteration-0 generated SQL, or the reason there is none."""

    entries: tuple[tuple[str, str], ...] = ()
    rows_seen: int = 0
    rows_with_sql: int = 0
    skip_reason: str | None = None
    applied_config: Mapping[str, Any] | None = field(default=None, repr=False)
    """The space configuration the run ended on, for the conflict surface.

    Carried out of the same read rather than fetched separately: the iteration
    rows this function already loaded contain it, and a second Delta read or a
    Genie GET to recover something in hand would be cost for nothing.
    """

    @property
    def usable(self) -> bool:
        return self.skip_reason is None and bool(self.entries)


@dataclass(frozen=True)
class AdvisorOutcome:
    """What the phase did, in a shape that survives into a stage row.

    ``status`` is ``COMPLETE``, ``SKIPPED`` or ``FAILED``. A ``SKIPPED`` outcome
    always carries a ``skip_reason``; a ``FAILED`` one always carries an ``error``
    — the phase is isolated, so a caller reading only the return value must still
    be able to tell a clean skip from a swallowed exception.
    """

    status: str
    skip_reason: str | None = None
    error: str | None = None
    statements_scanned: int = 0
    parse_failures: int = 0
    measures_found: int = 0
    candidates_scored: int = 0
    proposals_persisted: int = 0
    artifacts_written: int = 0
    echo_checks: tuple[str, ...] = ()
    proposals: tuple[ScoredProposal, ...] = field(default=(), repr=False)

    def detail(self) -> dict[str, Any]:
        """The ``genie_opt_stages.detail_json`` payload.

        Ids and counts only — no question text, no SQL, no comment bodies. A
        stage row is operator-facing and is not a leakage exemption.
        """
        return {
            "phase": MV_ADVISOR_PHASE_NAME,
            "status": self.status,
            "skip_reason": self.skip_reason,
            "error": self.error,
            "statements_scanned": self.statements_scanned,
            "parse_failures": self.parse_failures,
            "measures_found": self.measures_found,
            "candidates_scored": self.candidates_scored,
            "proposals_persisted": self.proposals_persisted,
            "artifacts_written": self.artifacts_written,
            "echo_checks": sorted(set(self.echo_checks)),
            "suggestion_ids": [p.suggestion_id for p in self.proposals],
        }


# ── Corpus ───────────────────────────────────────────────────────────────

_SQL_KEYS: tuple[str, ...] = ("generated_sql", "outputs/response")
_ID_KEYS: tuple[str, ...] = ("question_id", "inputs/question_id", "id")


def _generated_sql_of(row: Mapping[str, Any]) -> str:
    """Pull generated SQL out of one eval row.

    Reads the flat aliases first because those are what the active state readers
    consume, then falls back to the nested ``response.response`` shape. Both are
    populated by the official runner; checking both means a row written by either
    path is readable here.
    """
    for key in _SQL_KEYS:
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    response = row.get("response")
    if isinstance(response, Mapping):
        nested = response.get("response")
        if isinstance(nested, str) and nested.strip():
            return nested.strip()
    return ""


def _provenance_of(row: Mapping[str, Any], ordinal: int) -> str:
    """The benchmark question id this SQL came from.

    Falls back to a positional id so two rows never collapse into one
    provenance: ``provenance_count`` is the number that distinguishes "sixty
    occurrences from one query" from a genuinely recurring measure, and silently
    merging unidentified rows would inflate exactly that.
    """
    for key in _ID_KEYS:
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return f"row_{ordinal}"


def load_iteration_zero_corpus(
    spark: SparkSession,
    *,
    run_id: str,
    catalog: str,
    schema: str,
) -> CorpusLoad:
    """Read iteration 0's generated SQL from ``genie_opt_iterations``.

    Iteration 0 specifically: it is the baseline evaluation of the space as the
    user has it, so its SQL reflects what Genie does today rather than what a
    patch made it do. Read from Delta by ``run_id`` rather than from loop state,
    because cross-phase state travels through Delta and a phase that reached into
    the loop's memory would not survive the loop being reordered.
    """
    rows = load_all_full_iterations(spark, run_id, catalog, schema)
    if not rows:
        return CorpusLoad(skip_reason=SKIP_NO_ITERATIONS)

    baseline = next((r for r in rows if _as_int(r.get("iteration")) == 0), None)
    if baseline is None:
        return CorpusLoad(skip_reason=SKIP_NO_ITERATION_ZERO)

    payload = baseline.get("rows_json")
    if isinstance(payload, Mapping):
        payload = payload.get("rows")
    if not isinstance(payload, list) or not payload:
        return CorpusLoad(skip_reason=SKIP_EMPTY_ROWS_JSON)

    entries: list[tuple[str, str]] = []
    for ordinal, row in enumerate(payload):
        if not isinstance(row, Mapping):
            continue
        sql = _generated_sql_of(row)
        if sql:
            entries.append((sql, _provenance_of(row, ordinal)))

    if not entries:
        return CorpusLoad(
            rows_seen=len(payload),
            skip_reason=SKIP_NO_GENERATED_SQL,
        )
    return CorpusLoad(
        entries=tuple(entries),
        rows_seen=len(payload),
        rows_with_sql=len(entries),
        applied_config=_applied_config(rows),
    )


def _applied_config(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    """The configuration the run ended on, from the iteration rows already read.

    Prefers the champion row, since that is the configuration the run stands
    behind, and falls back to the highest iteration for a run whose champion was
    never stamped (a mid-loop failure). Within a row the authoritative observed
    read-back wins over the submitted config, matching how
    ``integration/revert.py`` resolves the same pair.

    Note the consequence for the trusted assets read out of it: the loop can add
    example SQL, so the curated set the conflict surface compares against is the
    set as of the end of the run, not as of iteration 0. That is the right one —
    a proposal has to be consistent with the answers the space actually ships.
    """
    champion = next((r for r in rows if _is_true(r.get("is_champion"))), None)
    if champion is None:
        ranked = sorted(
            (r for r in rows if _as_int(r.get("iteration")) is not None),
            key=lambda r: _as_int(r.get("iteration")) or 0,
        )
        champion = ranked[-1] if ranked else None
    if champion is None:
        return None
    for column in ("observed_config_json", "config_json"):
        parsed = _as_mapping(champion.get(column))
        if parsed:
            return parsed
    return None


def _as_mapping(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return None
        if isinstance(loaded, Mapping):
            return loaded
    return None


def _is_true(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ── Curated corpus (Prompt 6c) ───────────────────────────────────────────
#
# The generated half of the corpus is iteration-0 benchmark SQL. This is the
# *curated* half — SQL a human (or a prior GSO patch) wrote into the space — fed
# through the same ``corpus_scan`` extractors so a curated measure means the same
# thing a generated one does, then tagged ``CURATED_PROVENANCE_KIND`` so
# ``mv_scoring`` can up-weight it (MV-D17). Three sources reach the scan here;
# governed ``data_sources.metric_views`` are handled differently — see
# :func:`_advise`'s post-scan seed filter — because feeding them would only
# produce candidates the dedup gate blocks with the very MV they came from
# (MV-D17 / blocker 4). ``sql_functions`` is deliberately absent: it carries only
# an id and identifier in ``serialized_space`` (no body), so harvesting it would
# need a ``DESCRIBE FUNCTION`` UC read outside this prompt's boundary — its
# curated-measure role is served by ``sql_snippets.measures`` instead (MV-D17).
# ``join_specs`` is also absent: its ``sql`` is a bare predicate fragment that no
# extractor reads without synthetic wrapping, and join keys do not feed Y for
# measure candidates (MV-D17, deferred).

_CURATED_SQL_PATCH_TYPES: frozenset[str] = frozenset(
    {
        "add_sql_snippet_measure",
        "add_sql_snippet_expression",
        "add_sql_snippet_filter",
        "add_example_sql",
        "update_example_sql",
        "add_mv_measure",
        "update_mv_measure",
    }
)
"""GSO-applied patch types whose payload carries curated SQL worth harvesting.

Measure/expression/example patches route through ``extract_measures`` the same
way the config snippets do. These overlap the champion config's snippets and
example SQL (an applied patch is also visible in the config it produced), which
is harmless: a duplicate lands as one more distinct curated source on the same
canonical bucket, and the MV-D17 clamp absorbs the extra count."""

_CURATED_PATCH_SQL_KEYS: tuple[str, ...] = ("sql", "example_sql", "new_text", "expr")


def _curated_provenance(source_id: str) -> dict[str, str]:
    """A provenance mapping ``corpus_scan`` coerces, marked curated (MV-D17).

    ``kind`` is what the bucket reads to count curated sources; the ``id`` prefix
    (``trusted_asset:`` / ``sql_snippet:`` / ``gso_patch:``) is for a reviewer's
    traceability only and is never what makes an occurrence curated.
    """
    return {"id": source_id, "kind": CURATED_PROVENANCE_KIND}


def _curated_sql_text(value: Any) -> str:
    """One curated SQL string whether the field held a string or a ``list[str]``.

    ``example_question_sqls[].sql`` and the ``sql_snippets`` collections are
    ``list[str]`` in the serialized_space contract; both collapse to one
    whitespace-joined statement so the extractors see a single fragment.
    """
    if isinstance(value, (list, tuple)):
        return " ".join(str(part) for part in value if part).strip()
    return str(value or "").strip()


def _snippet_corpus_entries(
    applied_config: Mapping[str, Any] | None,
) -> list[tuple[str, dict[str, str]]]:
    """Curated corpus entries from ``instructions.sql_snippets`` (MV-D17).

    Reads all three collections — ``measures``, ``filters``, ``expressions`` —
    each a list of snippets carrying ``sql: list[str]``. ``measures`` is the one
    that seeds new candidates; the other two rarely yield an aggregate but are
    harvested for completeness and cost nothing when they don't.
    """
    if not isinstance(applied_config, Mapping):
        return []
    instructions = applied_config.get("instructions")
    if not isinstance(instructions, Mapping):
        return []
    snippets = instructions.get("sql_snippets")
    if not isinstance(snippets, Mapping):
        return []
    entries: list[tuple[str, dict[str, str]]] = []
    for collection in ("measures", "filters", "expressions"):
        for ordinal, snippet in enumerate(snippets.get(collection) or ()):
            if not isinstance(snippet, Mapping):
                continue
            sql = _curated_sql_text(snippet.get("sql"))
            if not sql:
                continue
            identifier = str(snippet.get("id") or "").strip() or f"index:{ordinal}"
            entries.append(
                (sql, _curated_provenance(f"sql_snippet:{collection}:{identifier}"))
            )
    return entries


def _patch_records(rows: Any) -> list[Mapping[str, Any]]:
    """Normalize whatever ``load_patches`` returned into a list of row mappings.

    ``load_patches`` returns a pandas ``DataFrame`` in production; tests stub it
    with a list of dicts. Both are accepted so the harvest has no pandas coupling
    in its own logic.
    """
    if rows is None:
        return []
    if hasattr(rows, "empty") and hasattr(rows, "to_dict"):
        return [] if rows.empty else list(rows.to_dict("records"))
    if isinstance(rows, Mapping):
        return [rows]
    if isinstance(rows, Iterable):
        return [row for row in rows if isinstance(row, Mapping)]
    return []


def _patch_corpus_entries(
    spark: SparkSession, *, run_id: str, catalog: str, schema: str
) -> list[tuple[str, dict[str, str]]]:
    """Curated corpus entries from this run's ``genie_opt_patches`` (MV-D17).

    Best-effort, like the estate scan: a Delta read that could not run costs the
    curated backstop and the run nothing. Only SQL-bearing patch types are read,
    and only the SQL payload — never the patch's natural-language rationale.
    """
    try:
        rows = load_patches(spark, run_id, catalog, schema)
    except Exception:
        logger.warning(
            "mv_advisor: could not read genie_opt_patches; curated patch harvest "
            "skipped", exc_info=True,
        )
        return []
    entries: list[tuple[str, dict[str, str]]] = []
    for record in _patch_records(rows):
        patch_type = str(record.get("patch_type") or "")
        if patch_type not in _CURATED_SQL_PATCH_TYPES:
            continue
        patch = _as_mapping(record.get("patch_json")) or {}
        sql = ""
        for key in _CURATED_PATCH_SQL_KEYS:
            sql = _curated_sql_text(patch.get(key))
            if sql:
                break
        if not sql:
            continue
        source_id = (
            f"gso_patch:{record.get('iteration')}:{record.get('lever')}:"
            f"{record.get('patch_index')}"
        )
        entries.append((sql, _curated_provenance(source_id)))
    return entries


def curated_corpus_entries(
    spark: SparkSession,
    *,
    run_id: str,
    catalog: str,
    schema: str,
    applied_config: Mapping[str, Any] | None,
) -> tuple[tuple[str, dict[str, str]], ...]:
    """The curated half of the corpus, tagged for the MV-D17 up-weight.

    Trusted-asset SQL comes through :func:`example_question_sql_statements` — the
    single reader ``trusted_asset_definitions`` also uses, so the conflict surface
    and the harvest cannot drift over what a curated asset is. Snippets and
    patches follow. Governed metric views are intentionally *not* here; they are
    a seed exclusion in :func:`_advise`, not corpus evidence.
    """
    entries: list[tuple[str, dict[str, str]]] = [
        (sql, _curated_provenance(f"trusted_asset:{identifier}"))
        for identifier, sql in example_question_sql_statements(applied_config)
    ]
    entries.extend(_snippet_corpus_entries(applied_config))
    entries.extend(
        _patch_corpus_entries(spark, run_id=run_id, catalog=catalog, schema=schema)
    )
    return tuple(entries)


# ── Estate index and column facts ────────────────────────────────────────


def _refs_from_tables(tables: Iterable[str]) -> list[tuple[str, str, str]]:
    """Turn fully-qualified table names into ``(catalog, schema, name)`` triples.

    Only three-part names are returned. A two-part name has no catalog to
    DESCRIBE against, and guessing one would point the estate scan at a
    different securable than the corpus referenced.
    """
    refs: list[tuple[str, str, str]] = []
    for table in tables:
        parts = [p.strip().strip("`") for p in str(table or "").split(".")]
        if len(parts) == 3 and all(parts):
            triple = (parts[0], parts[1], parts[2])
            if triple not in refs:
                refs.append(triple)
    return refs


def estate_metric_view_yamls(
    spark: SparkSession,
    tables: Iterable[str],
    *,
    w: Any = None,
    warehouse_id: str = "",
) -> dict[str, dict]:
    """The governed metric views defined over the corpus's own tables.

    "In scope" is defined as the tables the corpus actually referenced, which is
    narrower and more defensible than a schema sweep: it is exactly the estate a
    proposal could duplicate, and it needs no new configuration to say what to
    look at.

    Best-effort. A failed DESCRIBE costs the dedup gate its reference set and the
    run nothing — this returns ``{}`` and scoring proceeds, because an estate read
    that could not run is not evidence that the estate is empty.
    """
    refs = _refs_from_tables(tables)
    if not refs:
        return {}
    try:
        from genie_space_optimizer.common.metric_view_catalog import (
            detect_metric_views_via_catalog,
        )

        _detected, yamls = detect_metric_views_via_catalog(
            spark, refs, w=w, warehouse_id=warehouse_id
        )
        return dict(yamls or {})
    except Exception:
        logger.warning(
            "mv_advisor: estate metric view scan failed; dedup runs without a "
            "reference set", exc_info=True,
        )
        return {}


def column_facts_from_inventory(
    inventory: Mapping[str, Any] | None,
) -> dict[str, tuple[ColumnFacts, ...]]:
    """Column names, types and comments per table, from the wide-schema inventory.

    Reuses the inventory the intake task already built rather than issuing a
    second round of catalog reads. Returns an empty map when there is no
    inventory, which makes generation take its conservative branches.
    """
    if not isinstance(inventory, Mapping):
        return {}
    out: dict[str, list[ColumnFacts]] = {}
    for asset in inventory.get("assets") or ():
        if not isinstance(asset, Mapping):
            continue
        key = asset.get("asset_key")
        if not isinstance(key, (list, tuple)) or len(key) != 3:
            continue
        table = ".".join(str(part) for part in key)
        facts = out.setdefault(table, [])
        for column in asset.get("columns") or ():
            if not isinstance(column, Mapping):
                continue
            name = str(column.get("name") or "").strip()
            if not name:
                continue
            facts.append(
                ColumnFacts(
                    name=name,
                    data_type=str(column.get("data_type") or ""),
                    comment=str(column.get("description") or ""),
                )
            )
    return {table: tuple(facts) for table, facts in out.items() if facts}


# ── Candidates ───────────────────────────────────────────────────────────


def _concept_for(measure: FingerprintRecurrence) -> str:
    """A short, literal-free name for what the measure counts.

    Derived from the aggregate and its columns rather than from question text:
    the corpus scan already erased literals, and pulling a concept out of a
    benchmark question would route question text into a shipped comment, which is
    the firewall's whole concern.
    """
    columns = [c.split(".")[-1] for c in measure.source_columns if c]
    stem = "_".join(columns[:2]) if columns else "measure"
    kind = (measure.kind or "measure").lower()
    return f"{kind}_{stem}".strip("_").lower() or "measure"


def candidate_from_measure(
    measure: FingerprintRecurrence,
    *,
    space_id: str,
    table_columns: Mapping[str, tuple[ColumnFacts, ...]] | None = None,
) -> MetricViewCandidate:
    """One scored candidate per recurring measure.

    ``lineage`` and ``demand`` are left at their empty defaults and reported
    ``UNAVAILABLE`` by :func:`advisor_statuses`. They are constructed rather than
    omitted because the dataclass requires them; the status is what carries the
    fact that nothing measured them, which is precisely the confusion MV-D15
    exists to end.

    ``source_column_metadata`` **must** be populated for S to have anything to
    compare. A `NEW_METRIC_VIEW` candidate prefers `SOURCE_COLUMN_METADATA` under
    MV-D12, so leaving it empty would report S as ``EMPTY`` on every candidate —
    which is MV-D12's rejected defect wearing a new label. It is built from the
    inventory's column facts, so an advisor run without an inventory legitimately
    has no reference set and legitimately reports ``EMPTY``.
    """
    concept = _concept_for(measure)
    return MetricViewCandidate(
        space_id=space_id,
        candidate_type="NEW_METRIC_VIEW",
        measure_expr=measure.canonical_expr,
        source_tables=tuple(measure.source_tables),
        concept=concept,
        proposed_object=_proposed_object(measure, concept),
        measure_columns=frozenset(c.split(".")[-1].lower() for c in measure.source_columns),
        source_column_metadata=_source_column_metadata(measure, table_columns or {}),
        lineage=LineageOverlap(),
        recurrence=RecurrenceSignal(
            canonical_expr=measure.canonical_expr,
            recurrence=measure.recurrence,
            provenance_count=measure.provenance_count,
            curated_provenance_count=measure.curated_provenance_count,
            ast_equivalent=True,
        ),
        demand=DemandSignal(),
        benchmark_question_ids=tuple(measure.provenance_ids),
    )


def _source_column_metadata(
    measure: FingerprintRecurrence,
    table_columns: Mapping[str, tuple[ColumnFacts, ...]],
) -> tuple[SourceColumnMetadata, ...]:
    """S's reference set: the measure's own columns, with their UC comments.

    Narrowed to the columns the measure actually aggregates rather than the whole
    table. A wide fact table's other two hundred columns are not evidence about
    this measure, and including them would let an unrelated column's comment
    supply the top cosine.
    """
    wanted = {c.split(".")[-1].lower() for c in measure.source_columns if c}
    if not wanted:
        return ()
    out: list[SourceColumnMetadata] = []
    for table in measure.source_tables:
        for facts in table_columns.get(table, ()):
            if facts.name.lower() in wanted:
                out.append(
                    SourceColumnMetadata(
                        table=table, column=facts.name, comment=facts.comment
                    )
                )
    return tuple(out)


def _proposed_object(measure: FingerprintRecurrence, concept: str) -> str | None:
    """Where the view would live: beside its source data, not beside GSO's tables.

    A metric view over ``sales.orders`` belongs in ``sales``; putting it in the
    optimizer's own schema would make the proposal reference a securable the
    space's users have no reason to be granted.
    """
    refs = _refs_from_tables(measure.source_tables)
    if not refs:
        return None
    catalog, schema, _name = refs[0]
    return f"{catalog}.{schema}.{concept}_metrics"


def advisor_statuses() -> dict[str, str]:
    """The signals this phase cannot measure (MV-D15).

    A function rather than a constant so a caller cannot mutate the shared map,
    and so 6a has one obvious place to delete an entry from when it lands a
    producer. **S is absent on purpose** — ``score_candidate`` derives it from the
    embedding attempt, and naming it here would overwrite the endpoint's own report.
    """
    return {"L": MV_SIGNAL_UNAVAILABLE, "D": MV_SIGNAL_UNAVAILABLE}


def profiling_for(
    candidate: MetricViewCandidate,
    *,
    table_columns: Mapping[str, tuple[ColumnFacts, ...]],
    domain: str = "",
) -> MvProfiling:
    """Assemble generation's input contract for one candidate.

    ``capabilities`` is left empty, and that is not an oversight: there is no
    entitlement probe in the job (MV-D1 puts it on the backend), so every
    capability is ``UNKNOWN`` and MV-D13's step-down contract sends multi-hop
    candidates to rung 3. The YAML this produces is correct on every runtime and
    deliberately more verbose than a probed runtime might require — which is why
    MV-D15 requires Prompt 9 to regenerate rather than replay it.
    """
    source_table = candidate.source_tables[0] if candidate.source_tables else ""
    return MvProfiling(
        source_table=source_table,
        table_columns={source_table: table_columns.get(source_table, ())},
        measures=(
            MeasureRequest(
                name=candidate.concept or "measure",
                expr=candidate.measure_expr,
            ),
        ),
        capabilities={},
        domain=domain,
    )


# ── The phase ────────────────────────────────────────────────────────────


def run_mv_advisor_phase(
    spark: SparkSession,
    *,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    enabled: bool,
    benchmarks: Sequence[Mapping[str, Any]] = (),
    wide_schema_inventory: Mapping[str, Any] | None = None,
    w: Any = None,
    warehouse_id: str = "",
    embedding_client: Any = None,
    intent_texts: Sequence[str] = (),
    domain: str = "",
    max_candidates: int | None = None,
) -> AdvisorOutcome:
    """Run the advisor and write its stage row. **Never raises.**

    Total isolation is the contract: the advisor is an addition to a task whose
    job is optimization, so any failure of its own must cost its own output and
    nothing else. Both the stage write and the exception path are therefore
    swallowed — a phase that could not report its failure must still not fail the
    task by trying to.
    """
    if not enabled:
        outcome = AdvisorOutcome(status=STATUS_SKIPPED, skip_reason=SKIP_DISABLED)
        _record(spark, outcome, run_id=run_id, catalog=catalog, schema=schema)
        return outcome

    try:
        outcome = _advise(
            spark,
            run_id=run_id,
            space_id=space_id,
            catalog=catalog,
            schema=schema,
            benchmarks=benchmarks,
            wide_schema_inventory=wide_schema_inventory,
            w=w,
            warehouse_id=warehouse_id,
            embedding_client=embedding_client,
            intent_texts=intent_texts,
            domain=domain,
            max_candidates=max_candidates,
        )
    except Exception as exc:
        logger.warning("mv_advisor: phase failed; optimization is unaffected", exc_info=True)
        outcome = AdvisorOutcome(
            status=STATUS_FAILED, error=f"{type(exc).__name__}: {exc}"
        )

    _record(spark, outcome, run_id=run_id, catalog=catalog, schema=schema)
    return outcome


def _record(
    spark: SparkSession,
    outcome: AdvisorOutcome,
    *,
    run_id: str,
    catalog: str,
    schema: str,
) -> None:
    try:
        write_stage(
            spark,
            run_id,
            MV_ADVISOR_PHASE_NAME.upper(),
            outcome.status,
            task_key="optimize",
            catalog=catalog,
            schema=schema,
            detail=outcome.detail(),
            error_message=outcome.error,
        )
    except Exception:
        logger.warning("mv_advisor: could not write the phase stage row", exc_info=True)


def _advise(
    spark: SparkSession,
    *,
    run_id: str,
    space_id: str,
    catalog: str,
    schema: str,
    benchmarks: Sequence[Mapping[str, Any]],
    wide_schema_inventory: Mapping[str, Any] | None,
    w: Any,
    warehouse_id: str,
    embedding_client: Any,
    intent_texts: Sequence[str],
    domain: str,
    max_candidates: int | None,
) -> AdvisorOutcome:
    load = load_iteration_zero_corpus(spark, run_id=run_id, catalog=catalog, schema=schema)
    if not load.usable:
        return AdvisorOutcome(
            status=STATUS_SKIPPED,
            skip_reason=load.skip_reason,
            statements_scanned=0,
        )

    # Prompt 6c: the corpus is the generated iteration-0 SQL plus the curated
    # half — trusted-asset SQL, curated snippets, GSO-applied patches — routed
    # through the same extractors and tagged so MV-D17 can up-weight them. One
    # scan over both halves keeps a curated measure and a generated one in the
    # same bucket, which is what makes the provenance count meaningful.
    curated = curated_corpus_entries(
        spark,
        run_id=run_id,
        catalog=catalog,
        schema=schema,
        applied_config=load.applied_config,
    )
    scan = corpus_scan((*load.entries, *curated))
    if not scan.statements_scanned:
        return AdvisorOutcome(
            status=STATUS_SKIPPED,
            skip_reason=SKIP_NO_PARSEABLE_SQL,
            parse_failures=scan.parse_failures,
        )
    if not scan.measures:
        return AdvisorOutcome(
            status=STATUS_SKIPPED,
            skip_reason=SKIP_NO_CANDIDATES,
            statements_scanned=scan.statements_scanned,
            parse_failures=scan.parse_failures,
        )

    # One oracle for the whole phase. Built from the benchmark corpus this run
    # loaded from Delta, so the comment-echo check compares against the questions
    # a reader of the shipped YAML could recognize. Without it every generation
    # reports NOT_COMPARED and the firewall is decorative.
    oracle = LeakageOracle(BenchmarkCorpus.from_benchmarks(list(benchmarks or ())))

    limit = MV_ADVISOR_MAX_CANDIDATES if max_candidates is None else max_candidates
    tables = {t for m in scan.measures for t in m.source_tables}
    mv_fields = metric_view_fields(
        estate_metric_view_yamls(spark, tables, w=w, warehouse_id=warehouse_id)
    )
    # MV-D17 / blocker 4: a governed metric view already defines these, so seeding
    # them would only produce candidates the dedup gate blocks with the very MV
    # they came from. corpus_scan has no evidence-only channel — anything it scans
    # becomes a seed — so the exclusion is applied here, at the assembly site that
    # already holds the estate index, rather than by complicating the scan.
    governed = {
        field_.canonical_expr
        for field_ in mv_fields
        if field_.kind == FIELD_MEASURE and field_.canonical_expr
    }
    seed_measures = tuple(m for m in scan.measures if m.canonical_expr not in governed)
    if not seed_measures:
        return AdvisorOutcome(
            status=STATUS_SKIPPED,
            skip_reason=SKIP_NO_CANDIDATES,
            statements_scanned=scan.statements_scanned,
            parse_failures=scan.parse_failures,
            measures_found=len(scan.measures),
        )
    measures = seed_measures[: max(0, limit)]
    table_columns = column_facts_from_inventory(wide_schema_inventory)
    # POV Part 5 step 3: trusted assets are authoritative in a conflict. They live
    # in example_question_sqls, a different field from text_instructions, so
    # without this the gate could only see governed metric views and a proposal
    # contradicting a curated answer would reach PROPOSE unchallenged.
    trusted_assets = trusted_asset_definitions(load.applied_config)

    persisted = 0
    artifacts = 0
    echo_checks: list[str] = []
    proposals: list[ScoredProposal] = []

    for measure in measures:
        candidate = candidate_from_measure(
            measure, space_id=space_id, table_columns=table_columns
        )
        proposal = score_candidate(
            candidate,
            run_id=run_id,
            mv_fields=mv_fields,
            instructions=trusted_assets,
            intent_texts=intent_texts,
            embedding_client=embedding_client,
            statuses=advisor_statuses(),
            auth_identity="SP",
        )
        if not proposal.is_persistable:
            continue

        rendered = generate(
            candidate,
            profiling_for(candidate, table_columns=table_columns, domain=domain),
            shapes=scan.shapes,
            oracle=oracle,
        )
        echo_checks.append(rendered.echo_check)
        proposal = _with_generation_evidence(proposal, rendered)
        proposals.append(proposal)

        if persist_proposal(
            spark,
            proposal,
            catalog=catalog,
            schema=schema,
            run_id=run_id,
            requested_mode="suggest_only",
            effective_mode="suggest_only",
        ):
            persisted += 1

        if rendered.ok and _write_ddl_artifact(
            spark,
            proposal,
            rendered,
            run_id=run_id,
            catalog=catalog,
            schema=schema,
        ):
            artifacts += 1

    return AdvisorOutcome(
        status=STATUS_COMPLETE,
        statements_scanned=scan.statements_scanned,
        parse_failures=scan.parse_failures,
        measures_found=len(scan.measures),
        candidates_scored=len(measures),
        proposals_persisted=persisted,
        artifacts_written=artifacts,
        echo_checks=tuple(echo_checks),
        proposals=tuple(proposals),
    )


def _with_generation_evidence(proposal: ScoredProposal, rendered: Any) -> ScoredProposal:
    """Fold the join strategy and its evidence onto the proposal.

    Prompt 6 requires the strategy and its evidence to be persisted per
    candidate, and ``genie_opt_mv_candidates`` has no column for either — so they
    ride in ``evidence``, which is the JSON column that exists for facts the
    schema does not name. ``echo_check`` travels with them: a reviewer needs to
    know whether the comment firewall actually compared anything before trusting
    that it found nothing.
    """
    evidence = dict(proposal.evidence)
    evidence["join_strategy"] = rendered.join_strategy
    evidence["join_strategy_reason"] = rendered.strategy_reason
    evidence["join_strategy_evidence"] = dict(rendered.evidence or {})
    evidence["generation_verdict"] = rendered.verdict
    evidence["comment_echo_check"] = rendered.echo_check
    if rendered.rejections:
        evidence["generation_rejections"] = list(rendered.rejections)
    return replace(proposal, evidence=evidence)


def _write_ddl_artifact(
    spark: SparkSession,
    proposal: ScoredProposal,
    rendered: Any,
    *,
    run_id: str,
    catalog: str,
    schema: str,
) -> str | None:
    """Persist the rendered DDL as a ``mv_candidate_ddl`` artifact row.

    ``content_hash`` is the candidate's dedup fingerprint, not the text's hash,
    so ``genie_opt_artifacts`` and ``genie_opt_mv_candidates`` join on one key
    (MV-D7). A content hash would break that join the moment the same candidate
    re-rendered differently — which MV-D15 guarantees will happen, since Prompt 9
    regenerates under probe capabilities the job does not have.

    ``validate`` runs before the write and its findings ride along. The YAML came
    from ``mv_yaml.generate``, so this is not a trust check on the renderer; it
    records which capability downgrades the *reader* of this artifact will face.
    """
    target = proposal.proposed_object or ""
    if not target:
        return None
    report = validate(rendered.yaml_text)
    payload = {
        "suggestion_id": proposal.suggestion_id,
        "dedup_fingerprint": proposal.dedup_fingerprint,
        "target_space_id": proposal.target_space_id,
        "proposed_object": target,
        "join_strategy": rendered.join_strategy,
        "ddl": create_ddl(target, rendered.yaml_text),
        "validation": {
            "ok": report.ok,
            "errors": list(report.errors),
            "warnings": list(report.warnings),
            "downgrade_to": report.downgrade_to,
            "echo_check": report.echo_check,
        },
    }
    return write_artifact(
        spark,
        run_id,
        "mv_candidate_ddl",
        payload,
        catalog=catalog,
        schema=schema,
        stage_name=MV_ADVISOR_PHASE_NAME,
        source_notebook="run_optimize.py",
        content_hash=proposal.dedup_fingerprint,
    )


__all__ = [
    "SKIP_DISABLED",
    "SKIP_EMPTY_ROWS_JSON",
    "SKIP_NO_CANDIDATES",
    "SKIP_NO_GENERATED_SQL",
    "SKIP_NO_ITERATIONS",
    "SKIP_NO_ITERATION_ZERO",
    "SKIP_NO_PARSEABLE_SQL",
    "STATUS_COMPLETE",
    "STATUS_FAILED",
    "STATUS_SKIPPED",
    "AdvisorOutcome",
    "CorpusLoad",
    "advisor_statuses",
    "candidate_from_measure",
    "column_facts_from_inventory",
    "estate_metric_view_yamls",
    "load_iteration_zero_corpus",
    "profiling_for",
    "run_mv_advisor_phase",
]
