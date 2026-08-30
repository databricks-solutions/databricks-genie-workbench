"""Ontology materializer (Phase 2) — SP reads → shared transforms → idempotent
Delta MERGE of the three snapshot tables.

The orchestration (:func:`run_materialize`) takes an injectable ``reader`` (raw
system-table rows) and ``writer`` (Delta MERGE + run ledger) so the whole path is
unit-testable offline with fakes; the job wires the real Spark/warehouse-backed
reader + :class:`SparkSnapshotWriter`.

Idempotency (§7.2): snapshot writes go through ``writer.merge(...)`` — upsert on
the derived key + delete rows of this workspace no longer in the source. A re-run
yields the same rows, never duplicates. Phase 2 writes ONLY the snapshot tables
and the run ledger; the empty Phase-3 tables are never written.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Protocol

from genie_space_optimizer.ontology import ddl, er, graph, similarity, transforms

# Derived keys / update columns for the snapshot MERGEs (§7.2).
TAG_GRAPH_KEYS = ["workspace_id", "tag_key"]
TAG_GRAPH_UPDATE_COLS = [
    "allowed_values", "assignment_count", "acts_as_domain",
    "acts_as_subdomain", "dedupe_verdicts", "run_id", "as_of",
]
TAXONOMY_KEYS = ["workspace_id"]
TAXONOMY_UPDATE_COLS = ["tree", "run_id", "as_of"]
# Identity map (Phase 3a) — derived PK per §7.2.
IDENTITY_KEYS = ["workspace_id", "canonical_id", "member_ref"]
IDENTITY_UPDATE_COLS = ["member_kind", "verdict", "method", "score", "reason", "run_id", "as_of"]


class MaterializeReader(Protocol):
    """Raw system-table reads (SP), scoped by the catalog allowlist."""

    def governed_tags(self) -> list[dict[str, Any]]: ...
    def assignments(self, allowlist: list[str]) -> list[dict[str, Any]]: ...
    def metric_view_fqns(self, allowlist: list[str]) -> list[str]: ...
    def agents(self) -> list[str]: ...
    def lineage_edges(self, allowlist: list[str]) -> list[tuple[str, str]]: ...


class SnapshotWriter(Protocol):
    """Delta writer for the snapshot tables + the run ledger."""

    def ensure_tables(self) -> None: ...
    def upsert_run(self, row: dict[str, Any]) -> None: ...
    def merge(self, table: str, rows: list[dict[str, Any]], key_cols: list[str], workspace_id: str) -> None: ...


def build_snapshot(
    graph_struct: dict[str, Any],
    tree: dict[str, Any],
    *,
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


def run_materialize(
    reader: MaterializeReader,
    writer: SnapshotWriter,
    *,
    workspace_id: str,
    trigger: str,
    allowlist: list[str],
    run_id: str | None = None,
    now: datetime | None = None,
    similarity_backend: Any | None = None,
    embedder: Any | None = None,
    adjudicator: Any | None = None,
) -> dict[str, Any]:
    """Materialize the governed-tag graph + taxonomy snapshots for one workspace,
    then resolve identity (L3 ER) and MERGE the identity map + embedding-backed
    dedupe verdicts.

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

    try:
        catalog_rows = reader.governed_tags()
        assign_rows = reader.assignments(allowlist)
        graph_struct = transforms.assemble_tag_graph(catalog_rows, assign_rows, as_of)
        metric_views = reader.metric_view_fqns(allowlist)
        agents = reader.agents()
        tree = transforms.build_taxonomy_dict(graph_struct, metric_views, agents)

        # L2 fused-graph scaffold — built as a Phase-3 dependency, not persisted.
        graph.build_signal_graph(graph_struct, reader.lineage_edges(allowlist))

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
            er_verdicts, workspace_id=workspace_id, run_id=run_id, as_of=as_of,
            member_kind_by_ref={c.ref: c.kind for c in candidates},
        )

        snap = build_snapshot(
            graph_struct, tree, workspace_id=workspace_id, run_id=run_id,
            as_of=as_of, collisions=collisions,
        )
        writer.merge("genie_ont_tag_graph", snap["tag_graph_rows"], TAG_GRAPH_KEYS, workspace_id)
        writer.merge("genie_ont_taxonomy_snapshot", snap["taxonomy_rows"], TAXONOMY_KEYS, workspace_id)
        writer.merge("genie_ont_identity", identity_rows, IDENTITY_KEYS, workspace_id)

        run_row = {
            **run_row,
            "state": "succeeded",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            **snap["counts"],
            "identity_count": len({r["canonical_id"] for r in identity_rows}),
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


class SparkSnapshotWriter:
    """Writes the snapshots to Delta via Spark, using the idempotent MERGE."""

    def __init__(self, spark, catalog: str, schema: str):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    def ensure_tables(self) -> None:
        ddl.ensure_ontology_tables(self.spark, self.catalog, self.schema)

    def upsert_run(self, row: dict[str, Any]) -> None:
        df = self.spark.createDataFrame([row])
        view = "_ont_run_src"
        df.createOrReplaceTempView(view)
        update_cols = [c for c in row.keys() if c != "run_id"]
        sql = ddl.build_snapshot_merge_sql(
            catalog=self.catalog, schema=self.schema, table="genie_ont_runs",
            source_view=view, key_cols=["run_id"], update_cols=update_cols,
            workspace_id=row.get("workspace_id", ""),
        )
        # The run ledger is keyed on run_id (one header per run); the
        # workspace-scoped NOT-MATCHED-BY-SOURCE delete never fires here because
        # every prior run of this workspace keeps a distinct run_id — history is
        # preserved. Reuse the same MERGE builder for a single code path.
        self.spark.sql(sql)

    def merge(self, table: str, rows: list[dict[str, Any]], key_cols: list[str], workspace_id: str) -> None:
        view = f"_ont_src_{table}"
        if rows:
            update_cols = [c for c in rows[0].keys() if c not in key_cols]
            self.spark.createDataFrame(rows).createOrReplaceTempView(view)
        else:
            # Empty source: create an empty view with the table's schema so the
            # NOT-MATCHED-BY-SOURCE delete still clears this workspace's stale rows.
            self.spark.sql(
                f"SELECT * FROM {self.catalog}.{self.schema}.{table} WHERE 1=0"
            ).createOrReplaceTempView(view)
            update_cols = []
        sql = ddl.build_snapshot_merge_sql(
            catalog=self.catalog, schema=self.schema, table=table,
            source_view=view, key_cols=key_cols, update_cols=update_cols,
            workspace_id=workspace_id,
        )
        self.spark.sql(sql)
