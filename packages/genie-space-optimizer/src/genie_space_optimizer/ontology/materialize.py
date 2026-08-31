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

from genie_space_optimizer.ontology import cluster, ddl, er, graph, pages, rank, similarity, transforms

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


class MaterializeReader(Protocol):
    """Raw system-table reads (SP), scoped by the catalog allowlist."""

    def governed_tags(self) -> list[dict[str, Any]]: ...
    def assignments(self, allowlist: list[str]) -> list[dict[str, Any]]: ...
    def metric_view_fqns(self, allowlist: list[str]) -> list[str]: ...
    def agents(self) -> list[str]: ...
    def lineage_edges(self, allowlist: list[str]) -> list[tuple[str, str]]: ...
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
        "instructions": _call("space_instructions"),
    }


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
        graph_struct = transforms.assemble_tag_graph(catalog_rows, assign_rows, as_of)
        metric_views = reader.metric_view_fqns(allowlist)
        agents = reader.agents()
        tree = transforms.build_taxonomy_dict(graph_struct, metric_views, agents)

        # L2 fused signal graph — the clustering input (17d builds it; 17e consumes it).
        signal_graph = graph.build_signal_graph(graph_struct, reader.lineage_edges(allowlist))

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
        er_verdicts = er.run_er(candidates, backend=backend, vectors=vectors, adjudicator=adjudicator)
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
        proposals = cluster.cluster(signal_graph, identity=er_verdicts, namer=namer, company=company)
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
        page_cands = pages.mine_pages(
            measures=page_in["measures"], columns=page_in["columns"],
            identity_verdicts=er_verdicts, members=sorted(member_fqns | set(agents)),
            instructions=page_in["instructions"], workspace_id=workspace_id,
            drafter=page_drafter, routing_validator=routing_validator, oracle=page_oracle,
        )
        page_rows = build_page_rows(
            page_cands, metastore_id=metastore_id, workspace_id=workspace_id,
            run_id=run_id, as_of=as_of,
        )
        writer.merge(ddl.TABLE_ONT_PAGES, page_rows, PAGE_KEYS, metastore_id)

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
        )
        report = rank.mark_surfaced(
            expanded["domain_rows"], page_rows, _gather_suppressions(reader, metastore_id),
        )
        writer.merge(ddl.TABLE_ONT_DOMAINS, expanded["domain_rows"], DOMAIN_KEYS, metastore_id)
        writer.merge(ddl.TABLE_ONT_PAGES, page_rows, PAGE_KEYS, metastore_id)

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

    def merge(self, table: str, rows: list[dict[str, Any]], key_cols: list[str], metastore_id: str) -> None:
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
        )
        self.spark.sql(sql)
