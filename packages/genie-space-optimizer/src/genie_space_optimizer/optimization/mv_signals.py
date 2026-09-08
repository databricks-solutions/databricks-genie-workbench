"""Signal producers for the metric view advisor's **L** and **D** signals.

``mv_scoring`` computes but never queries: it takes :class:`LineageOverlap`,
:class:`DemandSignal` and the embedding client as *inputs* so every scoring rule
stays testable offline (see ``mv_scoring``'s "This module computes; it does not
query"). This module is the other half of that seam — the producers that read
the system tables and hand ``mv_scoring`` the two dataclasses it was built to
receive. Prompt 6a builds and tests them here against warehouse-row fixtures;
Prompt 6b wires them into the advisor phase. Keeping them standalone is what lets
6b be a small, reviewable diff over the same injection pattern the embedding
client already uses.

Read identity and path (MV-D1). Both producers read as the **service principal**
through the one warehouse seam every other in-job system-table read uses
(:func:`genie_space_optimizer.common.warehouse.sql_warehouse_query`). There is no
OBO in the job — the job's identity is the SP, and the POV:266 correction settles
it: lineage/demand evidence is *computed under the SP and filtered at
presentation* per the viewing user's grants, never computed under OBO in-job. The
producers depend only on an injected :data:`RunQuery` seam (SQL text in, row
dicts out), so a test supplies fixture rows and the live read path stays a thin
adapter (:func:`warehouse_reader`).

Failure semantics (MV-D15). Every producer returns a :class:`SignalResult`
carrying the payload *and* a status, so the advisor never has to infer why a
signal is missing:

- ``UNAVAILABLE`` — the read could not run (missing grant, missing table, empty
  ``statement_text`` under CMK, retention window exceeded, or an unclassified
  read failure). The reason is recorded on the result. Its weight leaves the
  blend's divisor; it never scores a silent zero.
- ``EMPTY`` — the read ran and found nothing to compare (no lineage footprint
  resolved, no matching measure in the space's traffic). A real measurement of
  zero: it scores 0.0 and keeps its weight.
- ``COMPUTED`` — the producer measured a value.

Firewall (MV-D10(b)). ``system.query.history.statement_text`` is raw user SQL. It
enters this module **only** as :func:`corpus_scan` input, where canonicalization
erases every literal before a fingerprint is formed. No history text — and no
value derived from it beyond a literal-free fingerprint — is placed on a
:class:`DemandSignal`, a :class:`SignalResult`, a log line, or any surface that
could reach a comment, ``display_name`` or synonym. The :class:`DemandSignal`
fields are counts and durations by construction; there is no text field to leak.

Lineage grain (MV-D19 = column grain). The **L** producer reads
``system.access.column_lineage`` and compares column sets, matching what
:class:`LineageOverlap` and :func:`lineage_overlap_score` were written for. Table
grain was rejected as a *correctness* finding, not a cost preference: at table
grain the footprint case degenerates to ``|candidate_tables| / |footprint_tables|``
and inverts the signal (see the MV-D19 record in the playbook). ``column_lineage``
is Public Preview with one-year retention (POV Caveat 8); when the grant or the
data is absent the read fails and L is reported ``UNAVAILABLE`` with the cause
named — the honest landing, never a silent zero.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from genie_space_optimizer.common.config import (
    MV_DEMAND_HISTORY_LOOKBACK_DAYS,
    MV_SIGNAL_COMPUTED,
    MV_SIGNAL_EMPTY,
    MV_SIGNAL_UNAVAILABLE,
)

from .mv_fingerprint import Provenance, corpus_scan
from .mv_scoring import (
    REFERENCE_GOVERNED_MV,
    REFERENCE_LINEAGE_FOOTPRINT,
    DemandSignal,
    LineageOverlap,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


# ── Seam ─────────────────────────────────────────────────────────────────

RunQuery = Callable[[str], Sequence[Mapping[str, Any]]]
"""SQL text in, row dicts out. The one dependency the producers take on the
outside world, so a test injects fixture rows and the live path is a thin
adapter over the SP warehouse read (:func:`warehouse_reader`)."""


# System-table identifiers, named once so a reason string and the SQL cannot
# drift apart.
_COLUMN_LINEAGE_TABLE = "system.access.column_lineage"
_QUERY_HISTORY_TABLE = "system.query.history"


# ── Result contract (MV-D15) ─────────────────────────────────────────────

# UNAVAILABLE reason prefixes. Stable codes a test can pin and a reader can
# grep, followed by a human detail. The advisor records the whole string.
REASON_MISSING_GRANT = "missing_grant"
REASON_MISSING_TABLE = "missing_table"
REASON_EMPTY_STATEMENT_TEXT = "empty_statement_text"
REASON_RETENTION_EXCEEDED = "retention_window_exceeded"
REASON_READ_FAILED = "read_failed"
REASON_NO_SCOPE = "no_scope"
REASON_NO_REFERENCE = "no_reference"


@dataclass(frozen=True)
class SignalResult:
    """A producer's payload plus why it is or is not usable (MV-D15).

    ``status`` is one of the ``MV_SIGNAL_*`` vocabulary. ``reason`` is set only
    for ``UNAVAILABLE`` (and left informative-but-optional for ``EMPTY``); it is
    empty for ``COMPUTED``. The payload is always a fully-formed dataclass — a
    stale or empty :class:`LineageOverlap` / :class:`DemandSignal` — never
    ``None``, so a consumer that ignores the status still gets a shape it can
    score. ``ScoreComponents`` is what actually zeroes an ``UNAVAILABLE``
    payload out of the blend; carrying it here keeps that decision one place.
    """

    payload: Any
    status: str
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "payload": _payload_to_dict(self.payload),
            "status": self.status,
            "reason": self.reason,
        }


def _payload_to_dict(payload: Any) -> Any:
    """Render a payload JSON-safely for logging and persistence.

    Prefers an explicit ``to_dict``; otherwise flattens a dataclass and turns its
    frozenset fields (``LineageOverlap``'s column sets) into sorted lists so the
    result is JSON-serializable and deterministic. This is the only place a
    payload is stringified, and by construction it carries no history text — the
    firewall (MV-D10(b)) holds because there is no text field to render.
    """
    if hasattr(payload, "to_dict"):
        return payload.to_dict()
    if is_dataclass(payload) and not isinstance(payload, type):
        return {
            key: sorted(value) if isinstance(value, (set, frozenset)) else value
            for key, value in asdict(payload).items()
        }
    return payload


def warehouse_reader(ws: WorkspaceClient, warehouse_id: str) -> RunQuery:
    """Bind the SP warehouse read into a :data:`RunQuery`.

    The live adapter Prompt 6b injects. Converts the pandas frame
    ``sql_warehouse_query`` returns into plain row dicts so the producers — and
    their tests — never depend on pandas. Import is local so this module carries
    no import-time dependency on the SDK or pandas.
    """
    from genie_space_optimizer.common.warehouse import sql_warehouse_query

    def _run(sql: str) -> Sequence[Mapping[str, Any]]:
        frame = sql_warehouse_query(ws, warehouse_id, sql)
        if getattr(frame, "empty", True):
            return []
        return list(frame.to_dict("records"))

    return _run


# ── Helpers ──────────────────────────────────────────────────────────────


def _sql_str(value: str) -> str:
    """Quote a string as a SQL literal, escaping embedded quotes."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def _sql_str_list(values: Iterable[str]) -> str:
    """Render an ``IN (...)`` list of quoted, de-duplicated, sorted names."""
    unique = sorted({str(v) for v in values if v})
    return ", ".join(_sql_str(v) for v in unique)


def _classify_read_failure(exc: Exception, table: str) -> str:
    """Map a warehouse read exception onto an MV-D15 UNAVAILABLE reason.

    The message is the only signal the Statement Execution API surfaces, so the
    classification is substring-based and deliberately conservative: a cause it
    cannot recognize is ``read_failed`` with the message attached, never
    silently reshaped into a grant or retention story it did not confirm.
    """
    msg = str(exc)
    upper = msg.upper()
    if "PERMISSION_DENIED" in upper or "ACCESS_DENIED" in upper or "DOES NOT HAVE" in upper:
        return f"{REASON_MISSING_GRANT}: SELECT on {table}"
    if (
        "TABLE_OR_VIEW_NOT_FOUND" in upper
        or "NOT FOUND" in upper
        or "DOES NOT EXIST" in upper
        or "UNRESOLVED" in upper
    ):
        return f"{REASON_MISSING_TABLE}: {table} not found"
    first_line = msg.strip().splitlines()[0] if msg.strip() else exc.__class__.__name__
    return f"{REASON_READ_FAILED}: {table}: {first_line}"


def _bare(column: str) -> str:
    """Bare, lowercased column name — the grain both L sides compare on.

    ``column_lineage.source_column_name`` is already bare; a candidate column may
    arrive table-qualified. Reducing both to the trailing segment matches the
    normalization ``mv_advisor`` already uses for ``measure_columns`` and keeps
    the two sides on one grain, scoped to the candidate's own source tables so a
    shared name like ``id`` cannot collide across the whole estate."""
    return str(column).split(".")[-1].strip().lower()


def _parse_timestamp(value: Any) -> datetime | None:
    """Parse a query-history ``start_time`` into an aware UTC datetime, or ``None``.

    Tolerant of the trailing ``Z`` and of a space-separated form, and of an
    already-parsed datetime. A value it cannot read is a missing age, not an
    error to raise — the caller treats ``None`` as "no age", not "age zero".
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    text = text.replace(" ", "T", 1) if "T" not in text and " " in text else text
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# ── L: lineage overlap (MV-D19 = column grain) ────────────────────────────


def lineage_signal(
    *,
    candidate_columns: Iterable[str],
    source_tables: Sequence[str],
    reference_kind: str = REFERENCE_LINEAGE_FOOTPRINT,
    space_id: str = "",
    run_query: RunQuery | None = None,
    reference_columns: Iterable[str] | None = None,
    lookback_days: int | None = None,
) -> SignalResult:
    """Produce a :class:`LineageOverlap` for one candidate (MV-D19 column grain).

    Two reference kinds, matching :class:`LineageOverlap.reference_kind`:

    - ``REFERENCE_LINEAGE_FOOTPRINT`` (``NEW_METRIC_VIEW`` / ``ADD_MEASURE``):
      the reference is the set of columns the space's queries touch on the
      candidate's source tables, read from ``system.access.column_lineage``
      scoped by ``genie_space_id`` *and* the candidate's source tables. This is
      the footprint question — "which columns, within the tables everyone
      already shares, does this measure touch" — which is inherently column
      grain (MV-D19).
    - ``REFERENCE_GOVERNED_MV`` (``REPLACE_RAW_TABLE``): the reference is a
      governed metric view's source column set, supplied by the caller from the
      estate scan (``reference_columns``). No lineage read is needed or made.

    Both column sets are reduced to bare lowercased names, scoped to the
    candidate's source tables, so the Jaccard :func:`lineage_overlap_score`
    computes means "column overlap within the shared tables" on both sides.
    """
    candidate = frozenset(_bare(c) for c in candidate_columns if c)
    tables = tuple(t for t in source_tables if t)

    if reference_kind == REFERENCE_GOVERNED_MV:
        return _governed_lineage(candidate, tables, reference_columns)

    # Footprint case: the read must be scoped, or it broadens the reference
    # across every consumer of the tables and inverts the signal (MV-D19).
    if run_query is None or not space_id:
        return SignalResult(
            LineageOverlap(candidate, frozenset(), reference_kind, tables),
            MV_SIGNAL_UNAVAILABLE,
            f"{REASON_NO_SCOPE}: footprint L needs a space_id and a reader",
        )
    if not tables:
        return SignalResult(
            LineageOverlap(candidate, frozenset(), reference_kind, tables),
            MV_SIGNAL_UNAVAILABLE,
            f"{REASON_NO_SCOPE}: candidate has no source tables to scope lineage",
        )

    sql = _footprint_sql(space_id=space_id, tables=tables, lookback_days=lookback_days)
    try:
        rows = run_query(sql)
    except Exception as exc:  # noqa: BLE001 - a read failure is a status, not a crash
        reason = _classify_read_failure(exc, _COLUMN_LINEAGE_TABLE)
        logger.info("mv_signals: lineage read unavailable (%s)", reason)
        return SignalResult(
            LineageOverlap(candidate, frozenset(), reference_kind, tables),
            MV_SIGNAL_UNAVAILABLE,
            reason,
        )

    reference = frozenset(
        _bare(row.get("source_column_name"))
        for row in rows
        if row.get("source_column_name")
    )
    overlap = LineageOverlap(candidate, reference, reference_kind, tables)
    if not reference or not candidate:
        # Ran, resolved nothing to compare. A measurement of zero (keeps weight),
        # not an absence of measurement — column_lineage's preview/retention gaps
        # land here honestly rather than as an UNAVAILABLE we cannot prove.
        return SignalResult(overlap, MV_SIGNAL_EMPTY, "no column-lineage footprint resolved")
    return SignalResult(overlap, MV_SIGNAL_COMPUTED)


def _governed_lineage(
    candidate: frozenset[str],
    tables: tuple[str, ...],
    reference_columns: Iterable[str] | None,
) -> SignalResult:
    """L against a governed metric view's source columns (caller-supplied)."""
    if reference_columns is None:
        return SignalResult(
            LineageOverlap(candidate, frozenset(), REFERENCE_GOVERNED_MV, tables),
            MV_SIGNAL_UNAVAILABLE,
            f"{REASON_NO_REFERENCE}: no governed metric view source columns supplied",
        )
    reference = frozenset(_bare(c) for c in reference_columns if c)
    overlap = LineageOverlap(candidate, reference, REFERENCE_GOVERNED_MV, tables)
    if not reference or not candidate:
        return SignalResult(overlap, MV_SIGNAL_EMPTY, "empty governed reference or candidate")
    return SignalResult(overlap, MV_SIGNAL_COMPUTED)


def _footprint_sql(*, space_id: str, tables: Sequence[str], lookback_days: int | None) -> str:
    """The column-lineage footprint read, scoped by space and source tables.

    Scoped by ``entity_metadata.genie_space_id`` (the same field
    ``table_lineage`` carries and ``watch/system_tables`` reads) so the footprint
    is *this space's* usage, not every consumer of the tables — the scoping the
    MV-D19 correctness argument turns on. An optional bounded window bounds the
    scan; lineage is already space- and table-scoped, so the default is
    unbounded within the one-year retention.
    """
    where = [
        f"entity_metadata.genie_space_id = {_sql_str(space_id)}",
        f"source_table_full_name IN ({_sql_str_list(tables)})",
        "source_column_name IS NOT NULL",
    ]
    if lookback_days is not None:
        where.append(f"event_time >= current_date() - {int(lookback_days)}")
    return (
        "SELECT DISTINCT source_table_full_name, source_column_name "
        f"FROM {_COLUMN_LINEAGE_TABLE} "
        f"WHERE {' AND '.join(where)}"
    )


# ── D: demand from query history ──────────────────────────────────────────


def demand_signal(
    *,
    space_id: str,
    candidate_fingerprints: Iterable[str],
    run_query: RunQuery,
    lookback_days: int = MV_DEMAND_HISTORY_LOOKBACK_DAYS,
    now: datetime | None = None,
) -> SignalResult:
    """Produce a :class:`DemandSignal` for one candidate from real traffic.

    Reads the space's ``system.query.history`` over a bounded window, fingerprints
    each statement through the one canonicalizer (:func:`corpus_scan`, MV-D10 — no
    second parser), and joins the candidate's measure fingerprints against the
    result. From the matched statements: ``frequency`` = summed recurrence,
    ``distinct_users`` = distinct ``executed_by``, ``cost_ms`` = summed
    ``total_duration_ms``, ``age_days`` from the most recent occurrence.

    This is a *distinct population* from the **Y** signal (MV-D15): Y counts the
    benchmark-derived corpus scored by the advisor, D counts real query-history
    traffic. Attributing one to the other would spend one piece of evidence
    twice; reading history directly is what keeps them separate. ``statement_text``
    is used only as fingerprint input and never stored (firewall, MV-D10(b)).
    """
    fingerprints = frozenset(str(f) for f in candidate_fingerprints if f)
    if not fingerprints:
        return SignalResult(DemandSignal(), MV_SIGNAL_EMPTY, "no candidate fingerprints to match")
    if not space_id:
        return SignalResult(
            DemandSignal(),
            MV_SIGNAL_UNAVAILABLE,
            f"{REASON_NO_SCOPE}: demand read needs a space_id",
        )

    sql = _history_sql(space_id=space_id, lookback_days=lookback_days)
    try:
        rows = run_query(sql)
    except Exception as exc:  # noqa: BLE001 - a read failure is a status, not a crash
        reason = _classify_read_failure(exc, _QUERY_HISTORY_TABLE)
        logger.info("mv_signals: demand read unavailable (%s)", reason)
        return SignalResult(DemandSignal(), MV_SIGNAL_UNAVAILABLE, reason)

    rows = list(rows)
    if not rows:
        # The space ran no queries in the window: a real measurement of no demand.
        return SignalResult(DemandSignal(), MV_SIGNAL_EMPTY, "no query history in window")

    by_statement: dict[str, Mapping[str, Any]] = {}
    entries: list[tuple[str, Provenance]] = []
    for row in rows:
        sid = str(row.get("statement_id") or "")
        if not sid:
            continue
        by_statement[sid] = row
        text = row.get("statement_text")
        if not text or not str(text).strip():
            continue
        entries.append((str(text), Provenance(id=sid, seen_at=_seen_at(row.get("start_time")))))

    if not entries:
        # Rows came back but every statement_text was empty/null — the CMK
        # redaction case. The read could not measure demand; that is UNAVAILABLE
        # with a named reason, not an EMPTY that would claim demand is zero.
        return SignalResult(
            DemandSignal(),
            MV_SIGNAL_UNAVAILABLE,
            f"{REASON_EMPTY_STATEMENT_TEXT}: statement_text blank on all rows (CMK redaction?)",
        )

    scan = corpus_scan(entries)
    matched = [m for m in scan.measures if m.fingerprint in fingerprints]
    if not matched:
        # Traffic exists, but none of it re-derives this candidate's measure.
        return SignalResult(DemandSignal(), MV_SIGNAL_EMPTY, "no matching measure in history")

    frequency = sum(m.recurrence for m in matched)
    matched_ids = {sid for m in matched for sid in m.provenance_ids}
    matched_rows = [by_statement[sid] for sid in matched_ids if sid in by_statement]

    cost_ms = sum(_as_float(r.get("total_duration_ms")) for r in matched_rows)
    distinct_users = len({str(r.get("executed_by")) for r in matched_rows if r.get("executed_by")})
    age_days = _age_days(matched_rows, now=now)

    demand = DemandSignal(
        frequency=frequency,
        cost_ms=cost_ms,
        distinct_users=distinct_users,
        age_days=age_days,
    )
    return SignalResult(demand, MV_SIGNAL_COMPUTED)


def _history_sql(*, space_id: str, lookback_days: int) -> str:
    """The per-space query-history read, mirroring watch/system_tables' shape.

    Scoped by ``query_source.genie_space_id`` — the field ``system.query.history``
    carries for Genie traffic (not ``entity_metadata``) — over a bounded window.
    """
    return (
        "SELECT statement_id, executed_by, start_time, total_duration_ms, statement_text "
        f"FROM {_QUERY_HISTORY_TABLE} "
        f"WHERE query_source.genie_space_id = {_sql_str(space_id)} "
        f"AND start_time >= current_date() - {int(lookback_days)} "
        "AND statement_text IS NOT NULL"
    )


def _seen_at(start_time: Any) -> str | None:
    parsed = _parse_timestamp(start_time)
    return parsed.isoformat() if parsed else None


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _age_days(rows: Sequence[Mapping[str, Any]], *, now: datetime | None) -> float:
    """Days since the most recent matched statement, floored at zero.

    ``age_days`` feeds ``demand_decay`` (half-life staleness), so it is measured
    from the *most recent* occurrence, not the mean — a measure run yesterday and
    a year ago is not stale. A row whose timestamp will not parse contributes no
    age rather than a spurious zero-age (which would read as maximally fresh).
    """
    reference = now or datetime.now(timezone.utc)
    stamps = [ts for ts in (_parse_timestamp(r.get("start_time")) for r in rows) if ts]
    if not stamps:
        return 0.0
    most_recent = max(stamps)
    return max(0.0, (reference - most_recent).total_seconds() / 86_400.0)
