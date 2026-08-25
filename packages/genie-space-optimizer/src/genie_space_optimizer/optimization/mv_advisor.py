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
All four blend signals now have a producer, and this module is where each one's
actual status is recorded per candidate rather than hidden:

- **L** is produced by :func:`mv_signals.lineage_signal` (column grain, MV-D19)
  from ``system.access.column_lineage``, scoped to the candidate's source
  tables. It reports ``COMPUTED`` when the space's column footprint resolves,
  ``EMPTY`` when the read ran but resolved nothing, and ``UNAVAILABLE`` (with a
  named reason) when the SP lacks the grant, the table is absent, or no reader
  is injected — the last being the degraded workspace where 6b's producers land
  exactly as the pre-6b advisor did.
- **D** is produced by :func:`mv_signals.demand_signal` from
  ``system.query.history`` — a *distinct population* from **Y** (Y counts the
  benchmark-derived corpus, D counts real query-history traffic), so the two are
  measured separately rather than one attributed to the other. Same
  ``COMPUTED`` / ``EMPTY`` / ``UNAVAILABLE`` semantics.
- **Y** is ``COMPUTED`` from the corpus scan.
- **S** reports its own status: ``COMPUTED`` when it reached an endpoint,
  ``EMPTY`` when it ran with nothing to compare, ``UNAVAILABLE`` when the
  endpoint is absent or failed.

Coverage is therefore a per-workspace fact, not a fixed ceiling: where the grant
and data are present, L and D lift ``evidence_coverage`` toward 1.0 and HIGH
becomes reachable (``>= 0.80``); where they are absent every signal degrades to
``UNAVAILABLE`` with its reason named and the advisor behaves exactly as it did
before 6b — same scores, same MEDIUM cap. It is legible either way because the
coverage figure rides on every candidate rather than being inferable only by
someone who knows which producers ran.

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
from collections.abc import Callable, Iterable, Mapping, Sequence
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
    suggestion_id_for,
    trusted_asset_definitions,
)
from .mv_state import (
    load_mv_suppressed_fingerprints,
    mv_bundle_fingerprint,
    mv_candidate_fingerprint,
)
from .mv_signals import (
    REASON_NO_SCOPE,
    RunQuery,
    SignalResult,
    demand_signal,
    lineage_signal,
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

    ``candidates_render_failed`` / ``render_failures`` carry the MV-D30
    as-implemented (Prompt 15.5) invariant loudly: a suggestion whose body did
    not render is *not persisted* (persist-variant a — the row's existence
    implies a servable body), and the drop rides the run outcome as a count plus
    an operator-facing ``(suggestion_id, verdict)`` pair rather than vanishing
    into a bodyless card that 404s at ``/mv-ddl``. The pair is ids/codes only, so
    the stage row stays a non-exemption.
    """

    status: str
    skip_reason: str | None = None
    error: str | None = None
    statements_scanned: int = 0
    parse_failures: int = 0
    measures_found: int = 0
    candidates_scored: int = 0
    candidates_dropped_for_leakage: int = 0
    candidates_dropped_suppressed: int = 0
    candidates_render_failed: int = 0
    proposals_persisted: int = 0
    artifacts_written: int = 0
    echo_checks: tuple[str, ...] = ()
    render_failures: tuple[tuple[str, str], ...] = field(default=(), repr=False)
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
            "candidates_dropped_for_leakage": self.candidates_dropped_for_leakage,
            "candidates_dropped_suppressed": self.candidates_dropped_suppressed,
            "candidates_render_failed": self.candidates_render_failed,
            "proposals_persisted": self.proposals_persisted,
            "artifacts_written": self.artifacts_written,
            "echo_checks": sorted(set(self.echo_checks)),
            "render_failures": [
                {"suggestion_id": sid, "verdict": verdict}
                for sid, verdict in self.render_failures
            ],
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
    entries: list[tuple[str, dict[str, str]]] = list(space_corpus_entries(applied_config))
    entries.extend(
        _patch_corpus_entries(spark, run_id=run_id, catalog=catalog, schema=schema)
    )
    return tuple(entries)


def space_corpus_entries(
    applied_config: Mapping[str, Any] | None,
) -> tuple[tuple[str, dict[str, str]], ...]:
    """The run-free curated corpus (MV-D23 scope item 1): the SparkSession-free
    half of :func:`curated_corpus_entries` — trusted-asset SQL plus curated
    snippets, tagged for the MV-D17 up-weight. Patches are excluded because they
    are a run's Delta rows; a standalone advice request has no run to read.

    Both callers route through this one accessor so the in-run and no-run paths
    cannot disagree about what a curated corpus entry is — trusted-asset SQL
    comes through :func:`example_question_sql_statements`, the single reader
    ``trusted_asset_definitions`` also uses.
    """
    entries: list[tuple[str, dict[str, str]]] = [
        (sql, _curated_provenance(f"trusted_asset:{identifier}"))
        for identifier, sql in example_question_sql_statements(applied_config)
    ]
    entries.extend(_snippet_corpus_entries(applied_config))
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
    lineage: LineageOverlap | None = None,
    demand: DemandSignal | None = None,
) -> MetricViewCandidate:
    """One scored candidate per recurring measure.

    ``lineage`` and ``demand`` carry the L and D producer payloads the advisor
    ran for this candidate (:func:`mv_signals.lineage_signal` /
    :func:`mv_signals.demand_signal`); their *status* travels separately through
    :func:`advisor_statuses`. When a caller omits them they fall back to the
    empty defaults the dataclass requires — the shape a degraded workspace scores
    with UNAVAILABLE — so this assembler stays callable without a reader and the
    fact that nothing measured them is carried by the status, which is precisely
    the confusion MV-D15 exists to end.

    ``source_column_metadata`` **must** be populated for S to have anything to
    compare. A `NEW_METRIC_VIEW` candidate prefers `SOURCE_COLUMN_METADATA` under
    MV-D12, so leaving it empty would report S as ``EMPTY`` on every candidate —
    which is MV-D12's rejected defect wearing a new label. It is built from the
    inventory's column facts, so an advisor run without an inventory legitimately
    has no reference set and legitimately reports ``EMPTY``.
    """
    concept = _concept_for(measure)
    # MV-D29: the RENDER source is the literal-preserving representative, not the
    # canonical form — the canonical erases `1 - l_discount` to `?n - l_discount`,
    # which cannot be created. Identity/scoring/dedup are unaffected:
    # `recurrence.canonical_expr` below (and `canonical_measure_expr`, which
    # prefers it) still carries the canonical form. The fallback to canonical
    # only fires for a degenerate measure whose representative failed to render,
    # and the placeholder guard in `mv_yaml.validate` catches that body anyway.
    render_source = measure.representative_expr or measure.canonical_expr
    return MetricViewCandidate(
        space_id=space_id,
        candidate_type="NEW_METRIC_VIEW",
        measure_expr=render_source,
        source_tables=tuple(measure.source_tables),
        concept=concept,
        proposed_object=_proposed_object(measure, concept),
        measure_columns=frozenset(c.split(".")[-1].lower() for c in measure.source_columns),
        source_column_metadata=_source_column_metadata(measure, table_columns or {}),
        lineage=lineage if lineage is not None else LineageOverlap(),
        recurrence=RecurrenceSignal(
            canonical_expr=measure.canonical_expr,
            recurrence=measure.recurrence,
            provenance_count=measure.provenance_count,
            curated_provenance_count=measure.curated_provenance_count,
            ast_equivalent=True,
        ),
        demand=demand if demand is not None else DemandSignal(),
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


def _bundle_grain(source_tables: Sequence[str]) -> tuple[str | None, str]:
    """Where a view-grained bundle would live, and its concept name (MV-D30).

    The bundle is one metric view over one source-table set, so it is named for
    the primary (first, sorted) source table's grain — ``{table}_metrics`` in
    that table's own catalog.schema — not per measure. Returns ``(None, concept)``
    when no source table is a three-part name: with no catalog to place the view
    in, the bundle has no resolvable ``proposed_object`` and must be DROPPED as a
    validation failure rather than surfaced as a blank card.
    """
    refs = _refs_from_tables(source_tables)
    if not refs:
        return None, "metrics"
    catalog, schema, name = refs[0]
    concept = f"{name}_metrics"
    return f"{catalog}.{schema}.{concept}", concept


def advisor_statuses(lineage: SignalResult, demand: SignalResult) -> dict[str, str]:
    """The L and D statuses this phase measured for one candidate (MV-D15).

    Reports each producer's *actual* status — ``COMPUTED`` / ``EMPTY`` /
    ``UNAVAILABLE`` — rather than the hardcoded pair 6a's producers replaced, so a
    reader sees which signal ran and which degraded. A fresh dict per call so a
    caller cannot mutate a shared map. **S is absent on purpose** —
    ``score_candidate`` derives it from the embedding attempt, and naming it here
    would overwrite the endpoint's own report.
    """
    return {"L": lineage.status, "D": demand.status}


def _candidate_signals(
    measure: FingerprintRecurrence,
    *,
    space_id: str,
    signal_reader: RunQuery | None,
) -> tuple[SignalResult, SignalResult]:
    """Run the L and D producers for one candidate over the injected reader.

    L is genuinely per-candidate: the footprint read is scoped by this measure's
    own ``source_tables``, so it varies candidate to candidate. D re-reads the
    whole space history and re-fingerprints it per candidate (the read and scan
    are identical across candidates; only the final fingerprint filter differs) —
    a known, bounded cost the ``MV_ADVISOR_MAX_CANDIDATES`` cap keeps small; the
    named fix, a batch ``demand_signals`` that reads once and returns a
    per-fingerprint map, is recorded in the gap report and is the right move only
    if that read ever shows up hot.

    A missing reader is not an error: L self-reports ``UNAVAILABLE`` and D is
    given the symmetric ``no_scope`` result here, so a workspace with no warehouse
    (or no grant) degrades exactly as the pre-6b advisor did.
    """
    lineage = lineage_signal(
        candidate_columns=measure.source_columns,
        source_tables=measure.source_tables,
        space_id=space_id,
        run_query=signal_reader,
    )
    if signal_reader is None:
        demand = SignalResult(
            DemandSignal(),
            MV_SIGNAL_UNAVAILABLE,
            f"{REASON_NO_SCOPE}: demand read needs a reader",
        )
    else:
        demand = demand_signal(
            space_id=space_id,
            candidate_fingerprints=(measure.fingerprint,),
            run_query=signal_reader,
        )
    return lineage, demand


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
    signal_reader: RunQuery | None = None,
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
            signal_reader=signal_reader,
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
    signal_reader: RunQuery | None,
    intent_texts: Sequence[str],
    domain: str,
    max_candidates: int | None,
) -> AdvisorOutcome:
    load = load_iteration_zero_corpus(spark, run_id=run_id, catalog=catalog, schema=schema)
    if not load.usable:
        # In-job, iteration-0 is a genuine gate: no baseline eval means the
        # optimize task itself has nothing to have run. MV-D23's "iteration-0 is
        # a contributor, not a gate" restructure applies to the STANDALONE path,
        # which never loads iteration-0 and calls :func:`advise_from_corpus`
        # directly on a curated-only corpus. The empty-corpus trap
        # (EMPTY_ROWS_JSON / NO_GENERATED_SQL) therefore stays an in-job hard
        # skip, exactly as before — MV-D15 vocabulary preserved.
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
    # MV-D23 scope item 2: the in-job Spark caller. It assembles the corpus and
    # binds the estate reader + persistence to Spark, then delegates the
    # SparkSession-free orchestration to :func:`advise_from_corpus` — the same
    # seam the backend warehouse caller drives. Every helper the loop calls
    # (corpus_scan, score_candidate, generate, persist_proposal, …) stays a
    # module global here so behaviour — and the byte-unchanged in-job test
    # surface — is preserved.
    return advise_from_corpus(
        space_id=space_id,
        run_id=run_id,
        corpus_entries=(*load.entries, *curated),
        applied_config=load.applied_config,
        benchmarks=benchmarks,
        wide_schema_inventory=wide_schema_inventory,
        metric_view_reader=lambda tables: metric_view_fields(
            estate_metric_view_yamls(spark, tables, w=w, warehouse_id=warehouse_id)
        ),
        embedding_client=embedding_client,
        signal_reader=signal_reader,
        intent_texts=intent_texts,
        domain=domain,
        max_candidates=max_candidates,
        persist_proposal=lambda proposal, rendered: persist_proposal(
            spark,
            proposal,
            catalog=catalog,
            schema=schema,
            run_id=run_id,
            requested_mode="suggest_only",
            effective_mode="suggest_only",
        ),
        write_ddl_artifact=lambda proposal, rendered: _write_ddl_artifact(
            spark,
            proposal,
            rendered,
            run_id=run_id,
            catalog=catalog,
            schema=schema,
        ),
        # MV-D30: the in-job advisor honours the same per-measure suppression the
        # IQ Scan user's rejections write, so the job never re-proposes what a
        # user rejected and the two surfaces agree on "rejected".
        read_suppressed_fingerprints=lambda: load_mv_suppressed_fingerprints(
            spark, catalog, schema, target_space_id=space_id
        ),
    )


def _build_bundle(
    *,
    space_id: str,
    source_tables: Sequence[str],
    members: Sequence[tuple[ScoredProposal, MetricViewCandidate]],
    table_columns: Mapping[str, tuple[ColumnFacts, ...]],
    domain: str,
    shapes: Any,
    oracle: Any,
) -> tuple[ScoredProposal, Any] | None:
    """Consolidate one source-table set's PROPOSE members into ONE view (MV-D30).

    The persisted grain becomes the view: members are rendered as one
    multi-measure YAML through the single ``mv_yaml.generate``; the bundle key is
    ``mv_bundle_fingerprint`` (a function of member fingerprints + source set);
    confidence/tier come from the strongest member; evidence is the union,
    carrying ``measures[]`` (each with its per-measure ``dedup_fingerprint`` so
    suppression cross-references survive bundling). Returns ``None`` when the
    grain has no resolvable ``proposed_object`` — a validation failure that is
    DROPPED, never surfaced as a blank card.
    """
    proposed_object, concept = _bundle_grain(source_tables)
    if not proposed_object:
        logger.info(
            "mv_advisor: dropping bundle over %s — no resolvable proposed_object "
            "(MV-D30 validate-don't-render)", tuple(source_tables),
        )
        return None

    # The strongest member drives confidence/tier and is the row we replace from;
    # ties broken on fingerprint so the choice is deterministic across scans.
    strongest_proposal, strongest_candidate = max(
        members, key=lambda m: (m[0].confidence_score, m[0].dedup_fingerprint)
    )

    requests: list[MeasureRequest] = []
    used_names: set[str] = set()
    member_evidence: list[dict[str, Any]] = []
    member_fps: list[str] = []
    question_ids: set[str] = set()
    # Members walked in fingerprint order so bundle rendering + evidence are
    # deterministic regardless of scan order.
    for proposal, candidate in sorted(members, key=lambda m: m[0].dedup_fingerprint):
        base = (candidate.concept or "measure").strip("_") or "measure"
        name = base
        suffix = 2
        while name in used_names:
            name = f"{base}_{suffix}"
            suffix += 1
        used_names.add(name)
        if candidate.measure_expr:
            requests.append(MeasureRequest(name=name, expr=candidate.measure_expr))
        member_fps.append(proposal.dedup_fingerprint)
        rec = candidate.recurrence
        qids = list(candidate.benchmark_question_ids)
        question_ids.update(qids)
        member_evidence.append({
            "display_name": name,
            "expr": candidate.measure_expr,
            "dedup_fingerprint": proposal.dedup_fingerprint,
            "recurrence": rec.recurrence,
            "provenance_count": rec.provenance_count,
            "curated_provenance_count": rec.curated_provenance_count,
            "benchmark_question_ids": qids,
        })

    primary_refs = _refs_from_tables(source_tables)
    primary_table = (
        ".".join(primary_refs[0]) if primary_refs
        else (source_tables[0] if source_tables else "")
    )
    profiling = MvProfiling(
        source_table=primary_table,
        table_columns={t: table_columns.get(t, ()) for t in source_tables},
        measures=tuple(requests),
        capabilities={},
        domain=domain,
    )
    rep_candidate = replace(
        strongest_candidate, proposed_object=proposed_object, concept=concept
    )
    rendered = generate(rep_candidate, profiling, shapes=shapes, oracle=oracle)

    bundle_fp = mv_bundle_fingerprint(space_id, member_fps, source_tables)
    evidence = dict(strongest_proposal.evidence)
    evidence["bundle"] = True
    evidence["measures"] = member_evidence
    evidence["measure_count"] = len(member_evidence)
    evidence["benchmark_question_ids"] = sorted(question_ids)
    evidence["distinct_source_count"] = len(question_ids)
    bundle_proposal = replace(
        strongest_proposal,
        suggestion_id=suggestion_id_for(bundle_fp),
        dedup_fingerprint=bundle_fp,
        proposed_object=proposed_object,
        evidence=evidence,
    )
    return _with_generation_evidence(bundle_proposal, rendered), rendered


def advise_from_corpus(
    *,
    space_id: str,
    run_id: str,
    corpus_entries: Sequence[tuple[str, str]],
    applied_config: Mapping[str, Any] | None,
    benchmarks: Sequence[Mapping[str, Any]],
    wide_schema_inventory: Mapping[str, Any] | None,
    metric_view_reader: Callable[[set[str]], Sequence[Any]],
    embedding_client: Any,
    signal_reader: RunQuery | None,
    intent_texts: Sequence[str],
    domain: str,
    max_candidates: int | None,
    persist_proposal: Callable[[ScoredProposal, Any], bool],
    write_ddl_artifact: Callable[[ScoredProposal, Any], bool],
    read_suppressed_fingerprints: Callable[[], set[str]] | None = None,
) -> AdvisorOutcome:
    """Corpus-agnostic advisor orchestration (MV-D23 scope item 2).

    The one body both callers share: the Spark in-job path (:func:`_advise`) and
    the backend warehouse path (``POST /spaces/{space_id}/mv/suggest``). Its
    inputs are the already-assembled corpus, the estate (via ``metric_view_reader``,
    which the caller binds to ``estate_metric_view_yamls`` under Spark or to a
    warehouse read), profiling context and the signal readers; its persistence is
    injected (``persist_proposal(proposal, rendered)`` /
    ``write_ddl_artifact(proposal, rendered)``). It never touches a
    SparkSession, a run-keyed loader, or a Delta writer directly, so it imports
    cleanly into the FastAPI backend (the ``mv_create``/``mv_yaml`` precedent —
    a PySpark-free optimization leaf, not the job orchestration).

    ``run_id`` is still required, and is a *real* row on both paths: the in-job
    run, or the MV-D23 sentinel advice run the backend writes before calling.
    The MV-D16(b) contamination rule is unweakened — nothing this produces
    re-enters the corpus as recurrence evidence; the corpus is the caller's
    input and is never appended to here.
    """
    scan = corpus_scan(tuple(corpus_entries))
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
    mv_fields = metric_view_reader(tables)
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
    trusted_assets = trusted_asset_definitions(applied_config)

    # MV-D30 as-implemented: a rejected measure never resurfaces inside a bundle.
    # The injected reader returns the per-measure fingerprints suppressed for this
    # space (fan-out ledger ∪ legacy rejected rows); suppressed members are
    # dropped BEFORE scoring so they cannot re-enter through a differently-
    # membered bundle. The reader is injected by BOTH callers (the in-job Spark
    # path and the backend suggest route), so the two surfaces agree on what
    # "rejected" means; None means a caller that opted out of suppression (tests).
    suppressed = read_suppressed_fingerprints() if read_suppressed_fingerprints else set()

    # MV-D29 shape firewall (Prompt 15.5): a recurring shape's components now
    # render literal-preserving (mv_fingerprint.render_components), so a
    # predicate/arithmetic literal inside a shape could reach a shipped body
    # exactly like a primary measure — and the generator's only leakage check is
    # the BEST FOR comment echo, which never inspects the measure body. So gate
    # the shapes through the SAME oracle here, at the assembly site, before any
    # generation: a shape with any leaking render fragment is DROPPED (the base
    # bundle still renders from its non-shape measures). This mirrors the
    # per-measure gate below and keeps "erasure-by-construction no longer
    # protects us" honest.
    shape_leak = 0
    safe_shapes: list[Any] = []
    for shape in scan.shapes:
        fragments = [frag for _, frag in shape.render_components] or [
            frag for _, frag in shape.components
        ]
        if any(oracle.contains_sql(frag) for frag in fragments):
            shape_leak += 1
            logger.info(
                "mv_advisor: dropped recurring shape %s — a render component "
                "matched the benchmark corpus (MV-D29 shape leakage gate)",
                shape.fingerprint,
            )
            continue
        safe_shapes.append(shape)
    safe_shapes = tuple(safe_shapes)

    persisted = 0
    artifacts = 0
    dropped_for_leakage = 0
    dropped_suppressed = 0
    render_failed = 0
    render_failures: list[tuple[str, str]] = []
    echo_checks: list[str] = []
    proposals: list[ScoredProposal] = []
    # PROPOSE suggestions collected for view-grained consolidation, keyed by
    # source-table set (the natural grain key). CONFLICT and any other
    # persistable-but-not-PROPOSE verdict keeps the per-measure path untouched, so
    # its adjudication grain is unaffected by MV-D30.
    bundles: dict[tuple[str, ...], list[tuple[ScoredProposal, MetricViewCandidate]]] = {}

    # ── Pass 1: per-measure scoring (identity/leakage/dedup grain unchanged) ──
    for measure in measures:
        measure_fp = mv_candidate_fingerprint(
            space_id, measure.canonical_expr, measure.source_tables
        )
        if measure_fp in suppressed:
            dropped_suppressed += 1
            logger.info(
                "mv_advisor: dropped measure %s — per-measure fingerprint is "
                "suppressed (MV-D30 rejected-stays-rejected)", measure.fingerprint,
            )
            continue
        lineage_result, demand_result = _candidate_signals(
            measure, space_id=space_id, signal_reader=signal_reader
        )
        candidate = candidate_from_measure(
            measure,
            space_id=space_id,
            table_columns=table_columns,
            lineage=lineage_result.payload,
            demand=demand_result.payload,
        )
        # MV-D29 firewall gate. The render source is now literal-preserving, so a
        # predicate literal that carries a benchmark/PII value could reach a
        # shipped body. Erasure-by-construction no longer protects us here, so the
        # representative must clear the SAME leakage oracle the comment echo check
        # uses before it can be scored, rendered, or persisted. A match DROPS the
        # candidate — it never ships masked and never ships leaked.
        if candidate.measure_expr and oracle.contains_sql(candidate.measure_expr):
            dropped_for_leakage += 1
            logger.info(
                "mv_advisor: dropped candidate %s — representative measure "
                "expression matched the benchmark corpus (MV-D29 leakage gate)",
                measure.fingerprint,
            )
            continue
        proposal = score_candidate(
            candidate,
            run_id=run_id,
            mv_fields=mv_fields,
            instructions=trusted_assets,
            intent_texts=intent_texts,
            embedding_client=embedding_client,
            statuses=advisor_statuses(lineage_result, demand_result),
            auth_identity="SP",
        )
        proposal = _with_signal_evidence(proposal, lineage_result, demand_result)
        if not proposal.is_persistable:
            continue

        if proposal.is_suggestion:
            key = tuple(sorted(candidate.source_tables))
            bundles.setdefault(key, []).append((proposal, candidate))
            continue

        # Non-PROPOSE persistable (e.g. CONFLICT): keep the per-measure path.
        rendered = generate(
            candidate,
            profiling_for(candidate, table_columns=table_columns, domain=domain),
            shapes=safe_shapes,
            oracle=oracle,
        )
        echo_checks.append(rendered.echo_check)
        proposal = _with_generation_evidence(proposal, rendered)
        proposals.append(proposal)
        if persist_proposal(proposal, rendered):
            persisted += 1
        if rendered.ok and write_ddl_artifact(proposal, rendered):
            artifacts += 1

    # ── Pass 2: consolidate each source-table set into ONE view proposal ──
    for source_key, members in bundles.items():
        built = _build_bundle(
            space_id=space_id,
            source_tables=source_key,
            members=members,
            table_columns=table_columns,
            domain=domain,
            shapes=safe_shapes,
            oracle=oracle,
        )
        if built is None:
            # No resolvable proposed_object — validation failure, never renders.
            continue
        bundle_proposal, rendered = built
        echo_checks.append(rendered.echo_check)
        # MV-D30 as-implemented (Prompt 15.5), persist-variant a: a suggestion
        # card surfaces ONLY when its body renders. A bundle whose YAML did not
        # render (``not rendered.ok`` — e.g. a shape that still resolves to a
        # canonicalizer placeholder, or an additive conflict) is DROPPED, not
        # persisted, so the panel never shows a card that 404s at ``/mv-ddl`` and
        # ``mv_create`` never resolves a body-less suggestion to nothing. The drop
        # rides the run outcome loudly rather than silently: a count plus an
        # operator-facing (id, verdict) pair, and the verdict/rejections at INFO.
        if not rendered.ok:
            render_failed += 1
            render_failures.append((bundle_proposal.suggestion_id, rendered.verdict))
            logger.warning(
                "mv_advisor: dropped bundle %s over %s — body did not render "
                "(verdict=%s, rejections=%s); a suggestion without a servable DDL "
                "body never surfaces (MV-D30 as-implemented 15.5)",
                bundle_proposal.suggestion_id,
                tuple(source_key),
                rendered.verdict,
                rendered.rejections,
            )
            continue
        proposals.append(bundle_proposal)
        if persist_proposal(bundle_proposal, rendered):
            persisted += 1
        if write_ddl_artifact(bundle_proposal, rendered):
            artifacts += 1

    return AdvisorOutcome(
        status=STATUS_COMPLETE,
        statements_scanned=scan.statements_scanned,
        parse_failures=scan.parse_failures,
        measures_found=len(scan.measures),
        candidates_scored=len(measures) - dropped_for_leakage - dropped_suppressed,
        candidates_dropped_for_leakage=dropped_for_leakage + shape_leak,
        candidates_dropped_suppressed=dropped_suppressed,
        candidates_render_failed=render_failed,
        proposals_persisted=persisted,
        artifacts_written=artifacts,
        echo_checks=tuple(echo_checks),
        render_failures=tuple(render_failures),
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


def _with_signal_evidence(
    proposal: ScoredProposal,
    lineage: SignalResult,
    demand: SignalResult,
) -> ScoredProposal:
    """Fold each producer's status and UNAVAILABLE reason onto the proposal.

    ``ScoreComponents`` already carries the L and D *values* and their statuses;
    what it has no column for is *why* a signal is UNAVAILABLE. That reason —
    missing grant, missing table, retention, CMK-blank text, no reader — is what
    turns a silent gap into a legible one (MV-D15), so it rides in ``evidence``,
    the JSON column that exists for facts the schema does not name. The reason
    strings are stable codes plus SQL-API error text; ``statement_text`` never
    reaches them (firewall, MV-D10(b)) — the producers only ever fingerprint it.
    """
    evidence = dict(proposal.evidence)
    evidence["signal_status"] = {
        "L": {"status": lineage.status, "reason": lineage.reason},
        "D": {"status": demand.status, "reason": demand.reason},
    }
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
    (MV-D7), independent of how the body is rendered or re-wrapped. The raw
    ``yaml_text`` is persisted next to the wrapped ``ddl`` so the backend can
    replay the immutable body with revalidation at create time (MV-D22,
    superseding MV-D15's regeneration clause) — re-wrapping it for the consented
    target via ``create_ddl`` rather than string-slicing the stored DDL. The
    backend does not regenerate: capabilities derive from the compute, and the
    backend probes via SQL warehouse exactly as the job does, so a regeneration
    would reproduce this same rung-3 body (MV-D13).

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
        # The immutable rendered body (MV-D22). The backend recovers this to
        # re-wrap for the consented target via ``create_ddl`` and revalidate,
        # rather than string-slicing the AS $$…$$ fence out of ``ddl``.
        "yaml_text": rendered.yaml_text,
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
    "advise_from_corpus",
    "advisor_statuses",
    "candidate_from_measure",
    "column_facts_from_inventory",
    "estate_metric_view_yamls",
    "load_iteration_zero_corpus",
    "profiling_for",
    "run_mv_advisor_phase",
    "space_corpus_entries",
]
