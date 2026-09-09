"""Ontology materializer (Phase 2) — SP reads → shared transforms → idempotent
Delta MERGE of the three snapshot tables.

The orchestration (:func:`run_materialize`) takes an injectable ``reader`` (raw
system-table rows) and ``writer`` (Delta MERGE + run ledger) so the whole path is
unit-testable offline with fakes; the job wires the real Spark/warehouse-backed
reader + :class:`SparkSnapshotWriter`.

Idempotency (§7.2, re-grained MV-D49): snapshot writes go through
``writer.merge(...)`` — upsert on the metastore-led derived key + delete rows of
this metastore no longer in the source. A re-run yields the same rows, never
duplicates, and two installs sharing a metastore converge on one row set.
``workspace_id`` rides along as provenance only. Phase 2 writes ONLY the snapshot
tables and the run ledger; the empty Phase-3 tables are never written.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import date, datetime, timezone
from typing import Any, Protocol

from genie_space_optimizer.ontology import cluster, ddl, er, graph, layout, pages, rank, similarity, transforms

logger = logging.getLogger(__name__)

# Derived keys / update columns for the snapshot MERGEs (§7.2, re-grained MV-D49:
# every key leads with metastore_id; workspace_id rides along as provenance).
TAG_GRAPH_KEYS = ["metastore_id", "tag_key"]
TAG_GRAPH_UPDATE_COLS = [
    "workspace_id", "allowed_values", "assignment_count", "acts_as_domain",
    "acts_as_subdomain", "dedupe_verdicts", "run_id", "as_of",
]
TAXONOMY_KEYS = ["metastore_id"]
TAXONOMY_UPDATE_COLS = ["workspace_id", "tree", "run_id", "as_of"]
# Identity map (Phase 3a) — derived PK per §7.2.
IDENTITY_KEYS = ["metastore_id", "canonical_id", "member_ref"]
IDENTITY_UPDATE_COLS = ["workspace_id", "member_kind", "verdict", "method", "score", "reason", "run_id", "as_of"]
# Domain / member proposals (Phase 3b) — derived PKs per §7.
DOMAIN_KEYS = ["metastore_id", "domain_id"]
MEMBER_KEYS = ["metastore_id", "domain_id", "asset_fqn"]
# Page proposals (Phase 3c) — concept-anchored derived PK, metastore-scoped (§7).
PAGE_KEYS = ["metastore_id", "page_id"]
GRAPH_SNAPSHOT_KEYS = ["metastore_id"]


class MaterializeReader(Protocol):
    """Raw system-table reads (SP), scoped by the catalog allowlist."""

    def governed_tags(self) -> list[dict[str, Any]]: ...
    def assignments(self, allowlist: list[str]) -> list[dict[str, Any]]: ...
    def metric_view_fqns(self, allowlist: list[str]) -> list[str]: ...
    def agents(self) -> list[str]: ...
    # Stage-2 Genie-Agent placement (MV-D91) — all optional; the wheel calls them
    # defensively (an older reader degrades: no agent nodes / no applied placement,
    # byte-identical, MV-D43). ``entity_tag_assignments`` reads the applied governed tag
    # off each Genie space via the entity-tag-assignments API; ``agent_scopes`` are the
    # space→table read edges; ``agent_names`` map space id → display name for the label.
    def entity_tag_assignments(self, entity_type: str) -> list[dict[str, Any]]: ...
    def agent_scopes(self, allowlist: list[str]) -> dict[str, list[str]]: ...
    def agent_names(self) -> dict[str, str]: ...
    # Stage-3 Dashboard placement (MV-D92) — all optional, same posture as the Stage-2
    # agent methods above. ``entity_tag_assignments("dashboards")`` reads the applied
    # governed tag off each Lakeview dashboard; ``dashboard_scopes`` are the
    # dashboard→table read edges (from dataset lineage); ``dashboard_names`` map dashboard
    # id → display name for the label. An older reader degrades: no dashboard nodes / no
    # applied placement, byte-identical (MV-D43).
    def dashboard_scopes(self, allowlist: list[str]) -> dict[str, list[str]]: ...
    def dashboard_names(self) -> dict[str, str]: ...
    # Stage-1 asset typing (MV-D90) — optional; the wheel calls it defensively (a reader
    # without it degrades to every tagged member typed ``table``, byte-identical to the
    # pre-Stage-1 output, MV-D43). ``{fqn -> table_type}`` from ``information_schema.tables``.
    def table_types(self, allowlist: list[str]) -> dict[str, str]: ...
    def lineage_edges(self, allowlist: list[str]) -> list[tuple[str, str]]: ...
    # Stage-1 structural signals (MV-D52) — optional; the wheel calls them defensively
    # (an older reader without them degrades to the lineage-only graph, MV-D43).
    def join_key_edges(self, allowlist: list[str]) -> list[tuple]: ...
    def mv_membership(self, allowlist: list[str]) -> dict[str, list[str]]: ...
    def schema_affinity(self, allowlist: list[str]) -> dict[str, list[str]]: ...
    # Phase 3d (17g) L6 inputs — optional; the wheel calls them defensively (an older
    # reader without them degrades to empty, MV-D43). ``suppressions`` is a READ-ONLY
    # fetch of the ledger the backend writes; the wheel never writes it.
    def usage_signals(self, allowlist: list[str]) -> dict[str, float]: ...
    def suppressions(self, metastore_id: str) -> list[dict[str, Any]]: ...


class SnapshotWriter(Protocol):
    """Delta writer for the snapshot tables + the run ledger."""

    def ensure_tables(self) -> None: ...
    def upsert_run(self, row: dict[str, Any]) -> None: ...
    def merge(self, table: str, rows: list[dict[str, Any]], key_cols: list[str], metastore_id: str) -> None: ...


def build_snapshot(
    graph_struct: dict[str, Any],
    tree: dict[str, Any],
    *,
    metastore_id: str,
    workspace_id: str,
    run_id: str,
    as_of: str,
    collisions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the Delta rows for the two data snapshots from the shared transforms.

    Uses the SAME transforms the Phase-1 routes call (parity). ``dedupe_verdicts``
    carries the per-tag collisions + cleanup flags as JSON. When ``collisions`` is
    supplied (the Phase-3a embedding-backed ER verdicts) it replaces the string-only
    ``find_collisions_dict`` — same shape, richer content; otherwise the Phase-2
    string-only behavior is preserved.
    """
    if collisions is None:
        collisions = transforms.find_collisions_dict(graph_struct)
    cleanup = transforms.find_cleanup_dict(graph_struct)

    tag_graph_rows: list[dict[str, Any]] = []
    for gt in transforms.governed_tag_rows(graph_struct):
        key = gt["tag_key"]
        verdicts = {
            "collisions": [c for c in collisions if key in c["members"]],
            "cleanup": [c for c in cleanup if c["tag_key"] == key],
        }
        tag_graph_rows.append({
            "metastore_id": metastore_id,
            "workspace_id": workspace_id,
            "tag_key": key,
            "allowed_values": gt["allowed_values"],
            "assignment_count": gt["assignment_count"],
            "acts_as_domain": gt["acts_as_domain"],
            "acts_as_subdomain": gt["acts_as_subdomain"],
            "dedupe_verdicts": json.dumps(verdicts, sort_keys=True),
            "run_id": run_id,
            "as_of": as_of,
        })

    taxonomy_rows = [{
        "metastore_id": metastore_id,
        "workspace_id": workspace_id,
        "tree": json.dumps(tree, sort_keys=True),
        "run_id": run_id,
        "as_of": as_of,
    }]

    ungrouped = tree.get("ungrouped", {})
    counts = {
        "tag_count": len(tag_graph_rows),
        "domain_count": len(tree.get("domains", [])),
        "ungrouped_count": len(ungrouped.get("metric_views", [])) + len(ungrouped.get("genie_agents", [])),
    }
    return {"tag_graph_rows": tag_graph_rows, "taxonomy_rows": taxonomy_rows, "counts": counts}


def build_domain_rows(
    proposals: list[cluster.DomainProposal],
    *,
    metastore_id: str,
    workspace_id: str,
    run_id: str,
    as_of: str,
    asset_type_by_fqn: dict[str, str] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Expand L4 :class:`~cluster.DomainProposal`s into ``genie_ont_domains`` +
    ``genie_ont_members`` rows (§7). ``score`` is ``0.0`` — L6 ranking is 17g. Sub-
    domain CREATE keys are qualified into the ``Domain/Sub`` convention first."""
    proposals = cluster.qualify_subdomain_keys(proposals)
    types = asset_type_by_fqn or {}
    domain_rows: list[dict[str, Any]] = []
    member_rows: list[dict[str, Any]] = []
    for p in proposals:
        domain_rows.append({
            "metastore_id": metastore_id,
            "domain_id": p.domain_id,
            "workspace_id": workspace_id,
            "parent_id": p.parent_id,
            "name": p.name,
            "description": p.description,
            "tag_decision": p.tag_decision,
            "tag_key": p.tag_key,
            "tag_value": p.tag_value,
            "evidence": json.dumps(p.evidence, sort_keys=True),
            "score": 0.0,
            "run_id": run_id,
            "as_of": as_of,
        })
        for fqn in p.members:
            member_rows.append({
                "metastore_id": metastore_id,
                "domain_id": p.domain_id,
                "asset_fqn": fqn,
                "asset_type": types.get(fqn, "table"),
                "workspace_id": workspace_id,
                "run_id": run_id,
                "as_of": as_of,
            })
    return {"domain_rows": domain_rows, "member_rows": member_rows}


def agent_domain_placement(
    entity_rows: list[dict[str, Any]],
    proposals: list[cluster.DomainProposal],
) -> dict[str, str]:
    """Place workspace-entity assignments (Genie spaces, Stage 2 MV-D91) into the
    Domain/sub-domain their APPLIED governed tag names — the ``{member_id -> domain_id}``
    the layout uses to colour an ``agent:<id>`` node ``origin=applied``.

    ``entity_rows`` are the reader's kept, aboutness-only entity-tag rows
    (``{tag_name, tag_value?, member_id}``, ``member_id`` = ``agent:<id>``). The
    placement is the deterministic dual of how a tagged TABLE lands in its domain: an
    entity tag is matched to the proposal that **reused/reassigned that same governed
    tag** (``tag_decision in {reuse, reassign}`` — the ``origin=applied`` domains). The
    clustering engine cannot carry a structure-less tag-only member into a community (a
    tag never solo-creates a Domain, MV-D52), so this post-cluster attach is what maps
    the agent's applied tag onto the very domain_id its tables produced — semantically
    "lands in its domain via the same governed tag", with no phantom ``asset:agent:<id>``
    node and no change to table clustering/ranking.

    **Scope reconciliation (MV-D91, live-probe finding):** an entity tag naming a domain
    NOT present as an applied proposal in this run (an out-of-scope workspace tag —
    ``SupplyChain`` on the airline snapshot) matches nothing and the agent is left
    UNGROUPED (absent from the returned map) — never a fabricated domain. A slash key
    (``Domain/Sub``) nests into the sub-domain when that exact sub is an applied proposal,
    else onto its top-level domain; a value-less top-level key (the common case,
    ``Alaska Airlines Commercial``) attaches to the top-level rollup. Deterministic
    (MV-D82): per member, the most specific match wins, ties break by domain_id asc.
    Pure — no I/O, no proposal mutation.
    """
    applied_top: dict[str, str] = {}
    applied_sub: dict[str, str] = {}
    for p in proposals:
        if p.tag_decision not in ("reuse", "reassign") or not p.tag_key:
            continue
        (applied_sub if p.parent_id is not None else applied_top)[p.tag_key] = p.domain_id

    placement: dict[str, str] = {}
    # (member_id -> best (specificity, domain_id)); higher specificity + lower id wins.
    best: dict[str, tuple[int, str]] = {}
    for r in sorted(entity_rows, key=lambda x: (str(x.get("member_id") or ""), str(transforms.tag_key_of(x) or ""))):
        member_id = r.get("member_id")
        key = transforms.tag_key_of(r)
        if not member_id or not key:
            continue
        cand: tuple[int, str] | None = None
        if key in applied_sub:                       # exact slash sub-domain match
            cand = (2, applied_sub[key])
        elif key in applied_top:                     # exact top-level domain key
            cand = (1, applied_top[key])
        else:                                        # slash key whose sub is absent → top domain
            dom = transforms.domain_part(key)
            if dom in applied_top:
                cand = (1, applied_top[dom])
        if cand is None:
            continue                                 # out-of-scope domain ⇒ ungrouped
        prev = best.get(str(member_id))
        if prev is None or cand[0] > prev[0] or (cand[0] == prev[0] and cand[1] < prev[1]):
            best[str(member_id)] = cand
    for member_id, (_spec, domain_id) in best.items():
        placement[member_id] = domain_id
    return dict(sorted(placement.items()))


def build_page_rows(
    candidates: list[pages.PageCandidate],
    *,
    metastore_id: str,
    workspace_id: str,
    run_id: str,
    as_of: str,
) -> list[dict[str, Any]]:
    """Expand L5 :class:`~pages.PageCandidate`s into ``genie_ont_pages`` rows (§7).

    ``canonical_id`` + ``corroboration`` + ``confidence`` ride in the ``evidence`` JSON
    (no new DDL, §4); ``score`` is ``0.0`` — L6 ranking is 17g. ``workspace_id`` rides
    as provenance only; the key is ``(metastore_id, page_id)`` (MV-D49)."""
    rows: list[dict[str, Any]] = []
    for c in candidates:
        rows.append({
            "metastore_id": metastore_id,
            "page_id": c.page_id,
            "workspace_id": workspace_id,
            "domain_id": c.domain_id,
            "archetype": c.archetype,
            "title": c.title,
            "body": c.body,
            "synonyms": list(c.synonyms),
            "related_fqns": list(c.related_fqns),
            "source_fqns": list(c.source_fqns),
            "certify": c.certify,
            "evidence": json.dumps({**c.evidence, "confidence": c.confidence}, sort_keys=True),
            "score": 0.0,
            "run_id": run_id,
            "as_of": as_of,
        })
    return rows


def _gather_page_inputs(reader: Any, allowlist: list[str]) -> dict[str, Any]:
    """Collect the L5 miner inputs from the reader if it surfaces them, else empty
    (MV-D43 degrade — an estate with no measure/column signal mines zero Pages). The
    Phase-2/3b reader does not carry these, so a run without them still succeeds."""
    def _call(name: str, *args):
        fn = getattr(reader, name, None)
        try:
            return list(fn(*args)) if fn is not None else []
        except Exception as exc:  # noqa: BLE001 — a missing signal never fails the run
            logger.info("ontology page input %s unavailable (%s)", name, exc)
            return []

    return {
        "measures": _call("measure_signals", allowlist),
        "columns": _call("coded_column_signals", allowlist),
        "comments": _call("comment_signals", allowlist),
        "instructions": _call("space_instructions"),
    }


def _gather_structural_signals(reader: Any, allowlist: list[str]) -> dict[str, Any]:
    """Collect the Stage-1 structural grouping signals (MV-D52) from the reader if it
    surfaces them, else empty (MV-D43 degrade). ``join_key_edges`` (FK + shared-join
    proxy), ``mv_membership`` (MV → source tables), and ``schema_affinity`` (shared
    schema) are opt-in ``build_signal_graph`` kwargs; a Phase-2/3b reader without them
    (or a failed information_schema read) still yields a valid run over lineage alone."""
    def _call(name: str, default: Any):
        fn = getattr(reader, name, None)
        if fn is None:
            return default
        try:
            return fn(allowlist)
        except Exception as exc:  # noqa: BLE001 — a missing signal never fails the run
            logger.info("ontology structural signal %s unavailable (%s)", name, exc)
            return default

    return {
        "join_key_edges": list(_call("join_key_edges", [])),
        "mv_membership": dict(_call("mv_membership", {})),
        "schema_affinity": dict(_call("schema_affinity", {})),
    }


def _gather_asset_types(reader: Any, allowlist: list[str]) -> dict[str, str]:
    """The Stage-1 asset-type map (fqn → table_type) for per-member typing (MV-D90), if the
    reader surfaces ``table_types``, else empty (MV-D43 degrade — an older reader, or a
    failed ``information_schema.tables`` read, leaves every tagged member a ``table``,
    byte-identical to the pre-Stage-1 output)."""
    fn = getattr(reader, "table_types", None)
    if fn is None:
        return {}
    try:
        got = fn(allowlist)
        return {str(k): str(v) for k, v in dict(got or {}).items() if k and v}
    except Exception as exc:  # noqa: BLE001 — a missing signal never fails the run
        logger.info("ontology asset-type map unavailable (%s)", exc)
        return {}


def _gather_entity_tags(reader: Any, entity_type: str) -> list[dict[str, Any]]:
    """Stage 2 (MV-D91): the reader's kept, aboutness-only entity-tag rows for a workspace
    entity type (``geniespaces``) via the entity-tag-assignments API — ``{tag_name,
    tag_value?, member_id}``. Defensive: an older reader without ``entity_tag_assignments``,
    or any Beta/permission failure, degrades to [] (the applied placement is then simply
    absent, MV-D43) — never blocks the run."""
    fn = getattr(reader, "entity_tag_assignments", None)
    if fn is None:
        return []
    try:
        return list(fn(entity_type) or [])
    except Exception as exc:  # noqa: BLE001 — a missing/Beta entity-tag API never fails the run
        logger.info("ontology entity-tag read (%s) unavailable (%s)", entity_type, exc)
        return []


def _gather_agent_scopes(reader: Any, allowlist: list[str]) -> dict[str, list[str]]:
    """Stage 2 (MV-D91) read-edge overlay: ``{space_id -> sorted[table_fqn]}`` for the
    ``agent_scopes`` kwarg of ``build_signal_graph``. Defensive: an older reader, or any
    failure, degrades to {} ⇒ byte-identical graph (MV-D43)."""
    fn = getattr(reader, "agent_scopes", None)
    if fn is None:
        return {}
    try:
        got = fn(allowlist) or {}
        return {str(k): [str(x) for x in (v or [])] for k, v in dict(got).items()}
    except Exception as exc:  # noqa: BLE001 — a missing scope read never fails the run
        logger.info("ontology agent_scopes unavailable (%s)", exc)
        return {}


def _gather_agent_names(reader: Any) -> dict[str, str]:
    """Stage 2 (MV-D91): ``{space_id -> display_name}`` so ``layout`` labels an agent node
    with its space name (a raw id is not human-readable). Defensive: absent/failed ⇒ {}
    ⇒ the label degrades to the id (MV-D43)."""
    fn = getattr(reader, "agent_names", None)
    if fn is None:
        return {}
    try:
        return {str(k): str(v) for k, v in dict(fn() or {}).items() if k and v}
    except Exception as exc:  # noqa: BLE001 — a missing name map never fails the run
        logger.info("ontology agent_names unavailable (%s)", exc)
        return {}


def _gather_dashboard_scopes(reader: Any, allowlist: list[str]) -> dict[str, list[str]]:
    """Stage 3 (MV-D92) read-edge overlay: ``{dashboard_id -> sorted[table_fqn]}`` for the
    ``dashboard_scopes`` kwarg of ``build_signal_graph``. Defensive: an older reader, or any
    failure, degrades to {} ⇒ byte-identical graph (MV-D43)."""
    fn = getattr(reader, "dashboard_scopes", None)
    if fn is None:
        return {}
    try:
        got = fn(allowlist) or {}
        return {str(k): [str(x) for x in (v or [])] for k, v in dict(got).items()}
    except Exception as exc:  # noqa: BLE001 — a missing scope read never fails the run
        logger.info("ontology dashboard_scopes unavailable (%s)", exc)
        return {}


def _gather_dashboard_names(reader: Any) -> dict[str, str]:
    """Stage 3 (MV-D92): ``{dashboard_id -> display_name}`` so ``layout`` labels a dashboard
    node with its name (a raw id is not human-readable). Defensive: absent/failed ⇒ {}
    ⇒ the label degrades to the id (MV-D43)."""
    fn = getattr(reader, "dashboard_names", None)
    if fn is None:
        return {}
    try:
        return {str(k): str(v) for k, v in dict(fn() or {}).items() if k and v}
    except Exception as exc:  # noqa: BLE001 — a missing name map never fails the run
        logger.info("ontology dashboard_names unavailable (%s)", exc)
        return {}


def _gather_usage(reader: Any, allowlist: list[str]) -> dict[str, float]:
    """The L2 usage/cost signal (fqn → pre-normalized [0,1] demand) for the L6 blend,
    if the reader surfaces it, else empty (MV-D43 degrade — a reader without it just
    leaves the usage factor absent, lowering coverage rather than faking a zero)."""
    fn = getattr(reader, "usage_signals", None)
    if fn is None:
        return {}
    try:
        got = fn(allowlist)
        return {str(k): float(v) for k, v in dict(got or {}).items()}
    except Exception as exc:  # noqa: BLE001 — a missing signal never fails the run
        logger.info("ontology usage signal unavailable (%s)", exc)
        return {}


def _gather_suppressions(reader: Any, metastore_id: str) -> list[dict[str, Any]]:
    """READ-ONLY fetch of the suppression-ledger rows for this metastore (the backend
    is the ONLY writer, MV-D26). The read goes through the injected reader method (the
    SELECT lives in the job's system-table reader, never here) so the wheel holds no
    ledger table literal and no ledger write. Defensive: an older reader without it, or
    a failed read, degrades to an empty ledger (everything surfaces per its score)
    rather than blocking the run."""
    fn = getattr(reader, "suppressions", None)
    if fn is None:
        return []
    try:
        return list(fn(metastore_id) or [])
    except Exception as exc:  # noqa: BLE001 — a missing ledger never fails the run
        logger.info("ontology suppression ledger unavailable (%s)", exc)
        return []


def _governance_map(graph_struct: dict[str, Any]) -> dict[str, str]:
    """Governance rung (fqn → ``governed``) for the L6 blend, from the governed-tag
    graph: an asset carrying ≥1 governed tag is ``governed``. ``curated`` (certified)
    is a richer rung not read in the offline slice; an untagged asset is simply absent
    (the governance factor then leaves the blend rather than scoring it ``ungoverned``
    — the honest-gap discipline, architecture §5)."""
    out: dict[str, str] = {}
    for t in graph_struct.get("tags", []):
        for m in t.get("members", []):
            fqn = m.get("fqn")
            if fqn:
                out[str(fqn)] = "governed"
    return out


def _has_scope(allowlist: list[str] | None) -> bool:
    """True iff the catalog allowlist names at least one catalog.

    An empty allowlist means "no scope selected" (never "scan everything"): every
    allowlist-scoped reader call — assignments, metric views, lineage, page inputs —
    returns nothing, so the derived Domain/Member/Page snapshots MERGE to zero and the
    metastore-scoped NOT-MATCHED-BY-SOURCE delete wipes a good snapshot. The guard in
    ``run_materialize`` uses this to refuse a destructive empty-scope run.
    """
    return any((c or "").strip() for c in (allowlist or []))


def run_materialize(
    reader: MaterializeReader,
    writer: SnapshotWriter,
    *,
    metastore_id: str,
    workspace_id: str,
    trigger: str,
    allowlist: list[str],
    run_id: str | None = None,
    now: datetime | None = None,
    similarity_backend: Any | None = None,
    embedder: Any | None = None,
    adjudicator: Any | None = None,
    namer: Any | None = None,
    company: str | None = None,
    page_drafter: Any | None = None,
    routing_validator: Any | None = None,
    page_oracle: Any | None = None,
    facet_denylist: list[str] | frozenset[str] | None = None,
    domain_min_tables: int = transforms.DOMAIN_MIN_TABLES,
    domain_min_schemas: int = transforms.DOMAIN_MIN_SCHEMAS,
    domain_require_connection: bool = transforms.DOMAIN_REQUIRE_CONNECTION,
    domain_max_diffuse_schemas: int = transforms.DOMAIN_MAX_DIFFUSE_SCHEMAS,
    domain_min_home_concentration: float = transforms.DOMAIN_MIN_HOME_CONCENTRATION,
    page_require_domain: bool = True,
    page_autodraft_min_corroboration: int = pages.PAGE_AUTODRAFT_MIN_CORROBORATION,
    page_autodraft_max_pages: int = pages.PAGE_AUTODRAFT_MAX_PAGES,
    page_autodraft_max_workers: int = pages.PAGE_AUTODRAFT_MAX_WORKERS,
    rename_max_workers: int = cluster.RENAME_MAX_WORKERS,
    rename_max_domains: int | None = None,
    er_adjudicate_max_pairs: int | None = None,
    er_adjudicate_min_score: float = er.ESCALATE_LOW,
    er_adjudicate_max_workers: int = 1,
) -> dict[str, Any]:
    """Materialize the governed-tag graph + taxonomy snapshots for one metastore
    (MV-D49 grain), then resolve identity (L3 ER) and MERGE the identity map +
    embedding-backed dedupe verdicts.

    ``metastore_id`` is the storage/serving grain and the MERGE delete scope; a run
    only ever deletes/updates rows of its own metastore, so two installs sharing a
    metastore converge on one row set. ``workspace_id`` rides along as provenance
    ("which install triggered this run") and is never part of a key or delete
    predicate.

    Writes a ``running`` run-ledger row, reads → transforms → MERGEs tag_graph +
    taxonomy + identity, then flips the run row to ``succeeded`` (or ``failed``).
    ``embedder`` (mv_scoring EmbeddingClient shape) and ``adjudicator`` are optional;
    without them ER runs string-only and skips escalation (MV-D43 degrade). Returns
    the terminal run row.
    """
    run_id = run_id or uuid.uuid4().hex
    started = now or datetime.now(timezone.utc)
    as_of = started.isoformat()

    writer.ensure_tables()
    run_row: dict[str, Any] = {
        "run_id": run_id,
        "metastore_id": metastore_id,
        "workspace_id": workspace_id,
        "trigger": trigger,
        "state": "running",
        "scope_allowlist": list(allowlist),
        "started_at": as_of,
        "finished_at": None,
        "as_of": as_of,
        "tag_count": None,
        "domain_count": None,
        "ungrouped_count": None,
        "error": None,
    }
    writer.upsert_run(run_row)

    # Empty-scope guard (MV-D49 safety). An empty catalog allowlist scans nothing, so
    # every derived snapshot would MERGE to zero and the metastore-scoped delete would
    # wipe the last good mirror (the failure mode behind an empty nightly run clearing
    # a curated ontology). Refuse to run destructively: record a terminal ``skipped``
    # header and return WITHOUT issuing any MERGE, so the prior snapshot — and the
    # prior ``succeeded`` run the UI serves from — is preserved untouched. Clearing the
    # estate is a deliberate, scoped action, never an accidental empty refresh.
    if not _has_scope(allowlist):
        logger.warning(
            "ontology materialize skipped: empty catalog allowlist (run_id=%s, "
            "metastore_id=%s) — snapshot preserved",
            run_id, metastore_id,
        )
        run_row = {
            **run_row,
            "state": "skipped",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error": "empty catalog allowlist — no scope selected; snapshot preserved",
            "tag_count": 0,
            "domain_count": 0,
            "ungrouped_count": 0,
        }
        writer.upsert_run(run_row)
        return run_row

    try:
        catalog_rows = reader.governed_tags()
        assign_rows = reader.assignments(allowlist)
        # Stage 1 (MV-D90): type tagged members by their real relation type so a tagged
        # Metric View becomes a first-class ``metric_view`` node (Measures then expand),
        # not a generic ``table``. Absent/failed read ⇒ {} ⇒ byte-identical (MV-D43).
        asset_type_map = _gather_asset_types(reader, allowlist)
        graph_struct = transforms.assemble_tag_graph(
            catalog_rows, assign_rows, as_of, asset_type_map=asset_type_map,
        )
        metric_views = reader.metric_view_fqns(allowlist)
        agents = reader.agents()
        tree = transforms.build_taxonomy_dict(graph_struct, metric_views, agents)

        # L2 fused signal graph — the clustering input (17d builds it; 17e consumes it).
        # Stage 1 (MV-D52) feeds in the strong structural signals (FK/shared-join, MV
        # membership, shared schema) when the reader surfaces them; a reader without them
        # degrades to the lineage-only graph unchanged (MV-D43).
        structural = _gather_structural_signals(reader, allowlist)
        # Stage 2 (MV-D91) read-edge overlay: the space→table scopes emit ``agent:<id>``
        # nodes + ``agent_scope`` edges (a relational overlay, NOT the domain mechanism —
        # that is the applied-tag placement below). ``agent_names`` labels each node with
        # its Genie space display name. Empty scopes ⇒ byte-identical graph (MV-D43).
        agent_scopes = _gather_agent_scopes(reader, allowlist)
        agent_names = _gather_agent_names(reader)
        # Stage 3 (MV-D92) read-edge overlay: the dashboard→table scopes emit
        # ``dashboard:<id>`` nodes + ``dashboard_scope`` edges (a relational overlay, NOT
        # the domain mechanism — that is the applied-tag placement below), mirroring the
        # agent overlay. Empty scopes ⇒ byte-identical graph (MV-D43).
        dashboard_scopes = _gather_dashboard_scopes(reader, allowlist)
        dashboard_names = _gather_dashboard_names(reader)
        signal_graph = graph.build_signal_graph(
            graph_struct, reader.lineage_edges(allowlist),
            agent_scopes=agent_scopes,
            agent_names=agent_names,
            dashboard_scopes=dashboard_scopes,
            dashboard_names=dashboard_names,
            join_key_edges=structural["join_key_edges"],
            mv_membership=structural["mv_membership"],
            schema_affinity=structural["schema_affinity"],
        )

        # L3 ER / dedupe over the (tag) candidate inventory. Similarity is behind
        # the one interface (in-process cosine default); embeddings + LLM are
        # optional and degrade to string-only / skip-escalation (MV-D43).
        backend = similarity_backend or similarity.get_similarity_backend(None)
        candidates = er.candidates_from_graph(graph_struct)
        vectors: dict[str, Any] = {}
        if embedder is not None and candidates:
            try:
                vecs = embedder.embed([c.text for c in candidates])
                vectors = {c.ref: v for c, v in zip(candidates, vecs) if v}
            except Exception as e:  # noqa: BLE001 — degrade to string-only ER
                logger.info("ontology ER embedding failed (%s); string-only", e)
        er_verdicts = er.run_er(
            candidates, backend=backend, vectors=vectors, adjudicator=adjudicator,
            # Bounded batch adjudication (MV-D68): cap + fan out the near-tie LLM calls so
            # ER's cost is a small constant; deferred near-ties degrade to escalate.
            adjudicate_max_pairs=er_adjudicate_max_pairs,
            adjudicate_min_score=er_adjudicate_min_score,
            adjudicate_max_workers=er_adjudicate_max_workers,
        )
        counts_by_ref = {t["tag_key"]: int(t.get("assignment_count") or 0) for t in graph_struct.get("tags", [])}
        collisions = transforms.collisions_from_er_verdicts(er_verdicts, counts_by_ref)
        identity_rows = transforms.identity_map_rows(
            er_verdicts, metastore_id=metastore_id, workspace_id=workspace_id,
            run_id=run_id, as_of=as_of,
            member_kind_by_ref={c.ref: c.kind for c in candidates},
        )

        snap = build_snapshot(
            graph_struct, tree, metastore_id=metastore_id, workspace_id=workspace_id,
            run_id=run_id, as_of=as_of, collisions=collisions,
        )
        writer.merge("genie_ont_tag_graph", snap["tag_graph_rows"], TAG_GRAPH_KEYS, metastore_id)
        writer.merge("genie_ont_taxonomy_snapshot", snap["taxonomy_rows"], TAXONOMY_KEYS, metastore_id)
        writer.merge("genie_ont_identity", identity_rows, IDENTITY_KEYS, metastore_id)

        # L4 clustering (Phase 3b) — the ADDITIVE FINAL step (§8). Runs over the fused
        # graph mapped to 17d's CANONICAL entities; MERGEs Domain/Sub-Domain proposals
        # + membership. Deterministic + offline; naming degrades (MV-D43). The snapshot
        # writes above are already committed, so a clustering error records `failed`
        # without corrupting them.
        # MV-D67: cluster with DETERMINISTIC names (namer=None). The LLM namer is deferred
        # to `cluster.rename_surfaced` below, run only over gate-survivors — naming per raw
        # cluster (pre-gate) was the batch-timeout hog.
        proposals = cluster.cluster(
            signal_graph, identity=er_verdicts, namer=None, company=company,
            facet_denylist=facet_denylist,
        )
        expanded = build_domain_rows(
            proposals, metastore_id=metastore_id, workspace_id=workspace_id,
            run_id=run_id, as_of=as_of,
        )
        writer.merge(ddl.TABLE_ONT_DOMAINS, expanded["domain_rows"], DOMAIN_KEYS, metastore_id)
        writer.merge(ddl.TABLE_ONT_MEMBERS, expanded["member_rows"], MEMBER_KEYS, metastore_id)

        # L5 Page mining (Phase 3c) — the ADDITIVE-FINAL step (§8). Runs over the just-
        # MERGEd Sub-Domain membership + the 17d identity map (the concept anchor);
        # detectors are deterministic, the drafting LLM + ask_genie validation are
        # injected + degrade (MV-D43). The MERGE is always issued (even with zero
        # Pages) so a concept that lost all signal is deleted metastore-scoped. Every
        # write above is already committed, so a mining error records `failed` without
        # corrupting the 17d/17e snapshots.
        page_in = _gather_page_inputs(reader, allowlist)
        member_fqns = {r["asset_fqn"] for r in expanded["member_rows"]}
        # Source-majority attachment (MV-D55): the asset → sub-domain map computed THIS
        # run (the just-MERGEd Sub-Domain membership). A Page attaches to the domain of
        # the majority of its Source assets; an asset absent from this map (or an empty
        # map) falls back to the signal home inside the wheel.
        asset_domain = {r["asset_fqn"]: r["domain_id"] for r in expanded["member_rows"]}
        page_cands = pages.mine_pages(
            measures=page_in["measures"], columns=page_in["columns"],
            comments=page_in["comments"], identity_verdicts=er_verdicts,
            members=sorted(member_fqns | set(agents)), instructions=page_in["instructions"],
            asset_domain=asset_domain, workspace_id=workspace_id,
            drafter=page_drafter, routing_validator=routing_validator, oracle=page_oracle,
            page_autodraft_min_corroboration=page_autodraft_min_corroboration,
            page_autodraft_max_pages=page_autodraft_max_pages,
            page_autodraft_max_workers=page_autodraft_max_workers,
        )
        page_rows = build_page_rows(
            page_cands, metastore_id=metastore_id, workspace_id=workspace_id,
            run_id=run_id, as_of=as_of,
        )
        # Step 2 (MV-D66): preserve curator-authored bodies across re-materialize.
        # For curator rows (body_source ∈ {llm_ondemand, llm_bulk, human}), keep the
        # existing body + evidence; for batch-owned rows (stub/llm_auto), refresh normally.
        writer.merge(
            ddl.TABLE_ONT_PAGES, page_rows, PAGE_KEYS, metastore_id,
            preserve_cols=["body", "evidence"],
            preserve_when="get_json_object(t.evidence,'$.body_source') IN ('llm_ondemand','llm_bulk','human')",
        )

        # L6 rank & trust gate (Phase 3d) — the ADDITIVE-LAST step (§8). Score +
        # firewall the just-written Domain/Page rows, READ the suppression ledger
        # (never write it), then re-MERGE with score + surfaced. The re-MERGE source
        # carries the FULL metastore proposal set (every row 17e/17f just wrote), so
        # the metastore-scoped NOT-MATCHED-BY-SOURCE delete prunes nothing it
        # shouldn't. Ranking is additive/idempotent — it never corrupts the snapshots
        # committed above, so a rank error records `failed` without losing them.
        signals = rank.RankSignals(
            usage=_gather_usage(reader, allowlist),
            centrality=graph.lineage_centrality(signal_graph),
            governance=_governance_map(graph_struct),
        )
        members_by_domain: dict[str, list[str]] = {}
        for m in expanded["member_rows"]:
            members_by_domain.setdefault(m["domain_id"], []).append(m["asset_fqn"])
        rank.score_proposals(
            expanded["domain_rows"], page_rows,
            members_by_domain=members_by_domain, signals=signals,
            min_tables=domain_min_tables, min_schemas=domain_min_schemas,
            require_connection=domain_require_connection,
            max_diffuse_schemas=domain_max_diffuse_schemas,
            min_home_concentration=domain_min_home_concentration,
            page_require_domain=page_require_domain,
        )
        report = rank.mark_surfaced(
            expanded["domain_rows"], page_rows, _gather_suppressions(reader, metastore_id),
        )
        # MV-D67: LLM-name ONLY the Domains that passed the gate (naming was the batch
        # timeout hog — it fired per raw cluster pre-gate on a slow model). Deterministic
        # names were written in the first domain MERGE above; upgrade the surfaced few here,
        # before the re-MERGE persists them. Bounded fan-out; degrades per Domain (MV-D43).
        cluster.rename_surfaced(
            proposals, expanded["domain_rows"], namer=namer, company=company,
            max_workers=rename_max_workers, max_domains=rename_max_domains,
        )
        writer.merge(ddl.TABLE_ONT_DOMAINS, expanded["domain_rows"], DOMAIN_KEYS, metastore_id)
        # Step 2 (MV-D66): re-merge pages with preserved bodies for curator rows.
        writer.merge(
            ddl.TABLE_ONT_PAGES, page_rows, PAGE_KEYS, metastore_id,
            preserve_cols=["body", "evidence"],
            preserve_when="get_json_object(t.evidence,'$.body_source') IN ('llm_ondemand','llm_bulk','human')",
        )

        # L7 estate-graph feed (Phase 3e, MV-D48) — the ADDITIVE-LAST step: lay out the
        # fused signal graph mapped to 17e's domains + 17g's scores, and MERGE the single
        # pre-computed snapshot the Ontology Map serves (read-only, off the request path).
        # Additive/idempotent like ranking: a layout error records `failed` without
        # corrupting the snapshots committed above (MV-D43). An empty graph → an empty
        # snapshot, run still succeeds.
        # Domain meta (MV-D71 + MV-D73 §2.1): {domain_id → {name, parent_id, origin}}
        # from the rows just MERGEd, so the estate map labels rollup nodes with human
        # names, links Sub-Domain → Domain for the hierarchy LOD, and stamps provenance —
        # ``applied`` iff the domain is backed by a governed-tag decision (reuse/reassign),
        # else ``proposed`` (a pure engine cluster).
        domain_meta = {
            r["domain_id"]: {
                "name": r.get("name"),
                "parent_id": r.get("parent_id"),
                "origin": "applied" if r.get("tag_decision") in ("reuse", "reassign") else "proposed",
                # MV-D86 (Lane D2b): forward the rollup description (already on the
                # domain row, materialize.py L159) so layout surfaces genie_ont_domains
                # descriptions on domain/sub-domain nodes live. Additive; None stays None.
                "description": r.get("description"),
            }
            for r in expanded["domain_rows"]
        }
        # Business-snippet index (MV-D73 §2.3): the bounded expand-on-demand source, baked
        # here in the deterministic batch (no request-path warehouse). Measures grouped by
        # mv_fqn (re-keyed to the mv:<fqn> hub node in layout); Pages grouped by their home
        # sub-domain (domain_id). Sorted so the blob is byte-stable across runs.
        measures_by_mv: dict[str, list[dict[str, Any]]] = {}
        for m in page_in["measures"]:
            measures_by_mv.setdefault(m.mv_fqn, []).append(
                {"ref": m.ref, "name": m.name, "expression": m.expression, "fmt": m.fmt}
            )
        pages_by_domain: dict[str, list[dict[str, Any]]] = {}
        for c in page_cands:
            if not c.domain_id:
                continue
            pages_by_domain.setdefault(c.domain_id, []).append(
                {"page_id": c.page_id, "title": c.title, "archetype": c.archetype, "domain_id": c.domain_id}
            )
        snippets_in = {
            "measures": {k: measures_by_mv[k] for k in sorted(measures_by_mv)},
            "pages": {k: pages_by_domain[k] for k in sorted(pages_by_domain)},
        }
        # Stage 2 (MV-D91) APPLIED placement: map each Genie space's applied governed tag
        # (read off the entity-tag-assignments API — NOT information_schema, which is why
        # the materializer was blind to it) onto the domain_id its tables produced, so the
        # ``agent:<id>`` node rolls up ``origin=applied`` in its Domain/sub-domain. An
        # out-of-scope or untagged space is simply absent here ⇒ ungrouped (never a
        # fabricated domain). Additive: the table ``asset_domain`` (rank/member rows) is
        # untouched — agents ride only the layout's node→domain map (MV-D43/D49/D82).
        entity_tags = _gather_entity_tags(reader, "geniespaces")
        agent_domain = agent_domain_placement(entity_tags, proposals)
        # Stage 3 (MV-D92) APPLIED placement: identical mechanism to the agent placement
        # above — each Lakeview dashboard's applied governed tag (read off the
        # entity-tag-assignments API for the ``dashboards`` entity, NOT information_schema)
        # maps onto the domain_id its tables produced, so the ``dashboard:<id>`` node rolls
        # up ``origin=applied``. Out-of-scope/untagged ⇒ absent here ⇒ ungrouped (MV-D43).
        # ``agent_domain_placement`` is generic on ``member_id`` (``dashboard:<id>`` works
        # unchanged); dashboards ride only the layout node→domain map (MV-D49/D82).
        dashboard_tags = _gather_entity_tags(reader, "dashboards")
        dashboard_domain = agent_domain_placement(dashboard_tags, proposals)
        graph_row = layout.build_graph_snapshot(
            signal_graph, {**asset_domain, **agent_domain, **dashboard_domain}, node_scores=None, domain_meta=domain_meta,
            snippets_in=snippets_in,
            metastore_id=metastore_id, workspace_id=workspace_id, run_id=run_id, as_of=as_of,
        )
        writer.merge(ddl.TABLE_ONT_GRAPH_SNAPSHOT, [graph_row], GRAPH_SNAPSHOT_KEYS, metastore_id)

        counts = {**snap["counts"], "domain_count": len(proposals)}
        run_row = {
            **run_row,
            "state": "succeeded",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            **counts,
            "identity_count": len({r["canonical_id"] for r in identity_rows}),
            "page_count": len(page_rows),
            # L6 report (§8.3). Returned + logged for the run summary; NOT persisted
            # columns (no DDL, MV-D49) — the writer drops keys absent from the schema.
            "surfaced_count": report["surfaced"],
            "suppressed_count": report["suppressed"],
            "blocked_count": report["blocked"],
        }
        writer.upsert_run(run_row)
        return run_row
    except Exception as exc:  # noqa: BLE001 — record failure, keep last good mirror
        run_row = {
            **run_row,
            "state": "failed",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        }
        writer.upsert_run(run_row)
        raise


# ─────────────────────────────────────────────────────────────────────────
# Real Spark-backed writer (used by the job; not exercised in offline tests).
# ─────────────────────────────────────────────────────────────────────────


def _coerce_scalar(value: Any, type_name: str) -> Any:
    """Coerce a row value to fit an explicit Spark column type.

    The row builders emit ISO-8601 strings for timestamp/date columns (JSON-safe),
    but ``createDataFrame`` with an explicit schema requires native ``datetime`` /
    ``date`` objects for ``TIMESTAMP`` / ``DATE`` fields. Everything else (arrays,
    ints, bools, strings, ``None``) passes through unchanged. ``type_name`` is the
    Spark ``DataType.typeName()`` (e.g. ``"timestamp"``, ``"date"``) so this helper
    stays import-free and unit-testable without pyspark.
    """
    if value is None or not isinstance(value, str):
        return value
    if type_name == "timestamp":
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    if type_name == "date":
        return date.fromisoformat(value)
    return value


def _project_to_schema(row: dict[str, Any], struct) -> dict[str, Any]:
    """Keep only the keys that are real columns of the target table.

    The run-ledger terminal row carries report-only counts (``surfaced_count`` /
    ``suppressed_count`` / ``blocked_count``) that are intentionally NOT DDL columns
    (MV-D49, no-DDL), and a live table created before a column was added (e.g.
    ``page_count``) legitimately lacks it. Because ``update_cols`` is derived from the
    row's keys, an un-projected row would build a MERGE that references ``s.<col>`` /
    ``t.<col>`` for columns that exist on neither side → ``UNRESOLVED_COLUMN`` at
    execute. Projecting to the schema honours the "writer drops keys absent from the
    schema" contract so the generated MERGE stays resolvable.
    """
    names = {f.name for f in struct}
    return {k: v for k, v in row.items() if k in names}


class SparkSnapshotWriter:
    """Writes the snapshots to Delta via Spark, using the idempotent MERGE."""

    def __init__(self, spark, catalog: str, schema: str):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    def ensure_tables(self) -> None:
        ddl.ensure_ontology_tables(self.spark, self.catalog, self.schema)

    def _df_for(self, table: str, rows: list[dict[str, Any]], struct=None):
        """Build a source DataFrame with the TARGET table's schema.

        Spark Connect (serverless) raises ``CANNOT_DETERMINE_TYPE`` when it must
        infer a column that is entirely ``None`` (e.g. ``error`` on a running row).
        Reading the schema from the already-created Delta table makes every column
        explicitly typed, aligns values by field name (missing keys → ``None``),
        and coerces ISO-string timestamps/dates to native objects. ``struct`` may be
        passed in to reuse a schema the caller already fetched (avoids a second read).
        """
        if struct is None:
            struct = self.spark.table(f"{self.catalog}.{self.schema}.{table}").schema
        data = [
            tuple(_coerce_scalar(r.get(f.name), f.dataType.typeName()) for f in struct)
            for r in rows
        ]
        return self.spark.createDataFrame(data, struct)

    def upsert_run(self, row: dict[str, Any]) -> None:
        struct = self.spark.table(f"{self.catalog}.{self.schema}.genie_ont_runs").schema
        row = _project_to_schema(row, struct)  # drop non-DDL report keys / absent cols
        df = self._df_for("genie_ont_runs", [row], struct)
        view = "_ont_run_src"
        df.createOrReplaceTempView(view)
        update_cols = [c for c in row.keys() if c != "run_id"]
        # The run ledger is UPSERT-ONLY (delete_unmatched=False). Its key is run_id,
        # which is unique per run, so the source-diff delete would treat every prior
        # run as "not matched by source" and wipe it — collapsing the ledger to the
        # latest run. Upsert-only preserves history; the two calls per run (running →
        # terminal) share a run_id and update in place.
        sql = ddl.build_snapshot_merge_sql(
            catalog=self.catalog, schema=self.schema, table="genie_ont_runs",
            source_view=view, key_cols=["run_id"], update_cols=update_cols,
            metastore_id=row.get("metastore_id", ""), delete_unmatched=False,
        )
        self.spark.sql(sql)

    def merge(
        self, table: str, rows: list[dict[str, Any]], key_cols: list[str], metastore_id: str,
        preserve_cols: list[str] = (), preserve_when: str = "",
    ) -> None:
        """Merge source rows into a snapshot table (idempotent MERGE).

        Step 2 (MV-D66): when preserve_cols and preserve_when are both set, emit
        a guarded clause that preserves specified columns for rows matching the
        predicate. Example: preserve body for curator rows.
        """
        view = f"_ont_src_{table}"
        if rows:
            struct = self.spark.table(f"{self.catalog}.{self.schema}.{table}").schema
            rows = [_project_to_schema(r, struct) for r in rows]
            update_cols = [c for c in rows[0].keys() if c not in key_cols]
            self._df_for(table, rows, struct).createOrReplaceTempView(view)
        else:
            # Empty source: create an empty view with the table's schema so the
            # NOT-MATCHED-BY-SOURCE delete still clears this metastore's stale rows.
            self.spark.sql(
                f"SELECT * FROM {self.catalog}.{self.schema}.{table} WHERE 1=0"
            ).createOrReplaceTempView(view)
            update_cols = []
        sql = ddl.build_snapshot_merge_sql(
            catalog=self.catalog, schema=self.schema, table=table,
            source_view=view, key_cols=key_cols, update_cols=update_cols,
            metastore_id=metastore_id,
            preserve_cols=preserve_cols, preserve_when=preserve_when,
        )
        self.spark.sql(sql)
