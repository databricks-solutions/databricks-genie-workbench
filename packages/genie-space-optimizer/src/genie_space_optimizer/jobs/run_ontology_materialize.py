# Databricks notebook source
# MAGIC %md
# MAGIC # Ontology Materialize (Phase 2 — nightly + on-demand)
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | `ontology_materialize` |
# MAGIC | **Grain** | **metastore** (MV-D49) — resolved `metastore_id` is the storage/serving key; `workspace_id` is provenance only |
# MAGIC | **Reads** | job params, `system.tags.governed_tags` + `information_schema.*_tags` + lineage (as the job's `run_as` identity — a metastore-admin user or an SP — allowlist-scoped) |
# MAGIC | **Writes** | `genie_ont_runs`, `genie_ont_tag_graph`, `genie_ont_taxonomy_snapshot`, `genie_ont_identity`, `genie_ont_domains`, `genie_ont_members`, `genie_ont_pages` (idempotent MERGE) |
# MAGIC | **Never writes** | any governed-tag DDL; `genie_ont_consents` / `_suppressions` (17g) |
# MAGIC | **Log label** | `[TASK ONTOLOGY]` |
# MAGIC
# MAGIC ## 🎯 Purpose (Phase-2 §8, re-grained MV-D49)
# MAGIC
# MAGIC Materialize the Phase-1 live outputs (governed-tag graph + taxonomy tree) to
# MAGIC Delta so the page reads a stable, sub-second snapshot. The heavy logic lives
# MAGIC in the wheel (`genie_space_optimizer.ontology.*`) using the SAME pure
# MAGIC transforms the Phase-1 routes call, so mirror output == live output. This
# MAGIC notebook is thin glue: resolve metastore_id → read system tables as the job's
# MAGIC `run_as` identity (a metastore-admin user or an SP, MV-D50) → `run_materialize`.
# MAGIC No app-SP system-table grant is required; the run_as identity's own grants back
# MAGIC the reads.
# MAGIC
# MAGIC **Run once per metastore.** The MERGE is idempotent and metastore-scoped, so
# MAGIC several workspace installs sharing a metastore are convergent (they land the
# MAGIC same rows) — but the operational recommendation is a single scheduled runner
# MAGIC per metastore to avoid churn. No code lock is added; the MERGE is the net.
# MAGIC
# MAGIC Read-only w.r.t. UC governance: the ONLY UC writes are the `genie_ont_*`
# MAGIC Delta MERGEs. No `SET`/`CREATE` governed-tag statements anywhere.

# COMMAND ----------

import json
import os
from typing import Any, cast

from genie_space_optimizer._workspace_client import make_workspace_client
from genie_space_optimizer.ontology import materialize

dbutils = cast(Any, globals().get("dbutils"))
spark = cast(Any, globals().get("spark"))

_TASK_LABEL = "[TASK ONTOLOGY]"


def _log(msg: str, **kw: Any) -> None:
    extra = " ".join(f"{k}={v}" for k, v in kw.items())
    print(f"{_TASK_LABEL} {msg}{(' ' + extra) if extra else ''}")


# COMMAND ----------

dbutils.widgets.text("metastore_id", "")
dbutils.widgets.text("workspace_id", "")
dbutils.widgets.text("trigger", "nightly")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "genie_space_optimizer")
dbutils.widgets.text("catalog_allowlist", "[]")
dbutils.widgets.text("run_id", "")

metastore_id = dbutils.widgets.get("metastore_id").strip()
workspace_id = dbutils.widgets.get("workspace_id").strip()
trigger = dbutils.widgets.get("trigger").strip() or "nightly"
catalog = dbutils.widgets.get("catalog").strip() or os.environ.get("GSO_CATALOG", "")
schema = dbutils.widgets.get("schema").strip() or os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
run_id = dbutils.widgets.get("run_id").strip() or None
try:
    allowlist = [str(c).strip() for c in json.loads(dbutils.widgets.get("catalog_allowlist") or "[]") if str(c).strip()]
except (TypeError, ValueError):
    allowlist = []


def _resolve_metastore_id() -> str:
    """Resolve the metastore grain (MV-D49), degrading rather than hanging (MV-D43).

    Tiers: the SDK ``metastores.current()`` → the system-table ``CURRENT_METASTORE()``
    → the stable literal ``"default"``. A missing id never blocks the run; a run
    scoped to a stable id still MERGEs onto the same rows on the next run.
    """
    try:
        current = make_workspace_client().metastores.current()
        mid = getattr(current, "metastore_id", None)
        if mid:
            return str(mid)
    except Exception as e:  # noqa: BLE001 — degrade to the system-table read
        _log("metastores.current() failed; trying CURRENT_METASTORE()", error=str(e))
    try:
        rows = [r.asDict(recursive=True) for r in spark.sql("SELECT CURRENT_METASTORE() AS metastore_id").collect()]
        mid = rows[0].get("metastore_id") if rows else None
        if mid:
            return str(mid)
    except Exception as e:  # noqa: BLE001 — degrade to the stable literal
        _log("CURRENT_METASTORE() read failed; using 'default'", error=str(e))
    return "default"


# workspace_id is PROVENANCE (which install triggered this run), never a key.
if not workspace_id:
    w0 = make_workspace_client()
    try:
        workspace_id = str(w0.get_workspace_id())
    except Exception:
        workspace_id = "default"

if not metastore_id:
    metastore_id = _resolve_metastore_id()

_log("Resolved params", metastore_id=metastore_id, workspace_id=workspace_id,
     trigger=trigger, catalog=catalog, schema=schema, allowlist=allowlist)

# COMMAND ----------


def _rows(sql: str) -> list[dict[str, Any]]:
    return [r.asDict(recursive=True) for r in spark.sql(sql).collect()]


def _in_list(allowlist: list[str]) -> str:
    return ", ".join("'" + c.replace("'", "''") + "'" for c in allowlist)


class SparkSystemTableReader:
    """SP/Spark reads of the same system tables Phase 1 reads live (allowlist-scoped)."""

    def governed_tags(self) -> list[dict[str, Any]]:
        try:
            return _rows("SELECT * FROM system.tags.governed_tags")
        except Exception as e:  # noqa: BLE001
            _log("governed_tags read failed", error=str(e))
            return []

    def assignments(self, allowlist: list[str]) -> list[dict[str, Any]]:
        if not allowlist:
            return []
        cats = _in_list(allowlist)
        sql = (
            "SELECT tag_name, catalog_name, schema_name, table_name FROM ("
            f" SELECT tag_name, catalog_name, schema_name, table_name FROM system.information_schema.table_tags WHERE catalog_name IN ({cats})"
            " UNION ALL"
            f" SELECT tag_name, catalog_name, schema_name, CAST(NULL AS STRING) table_name FROM system.information_schema.schema_tags WHERE catalog_name IN ({cats})"
            " UNION ALL"
            f" SELECT tag_name, catalog_name, CAST(NULL AS STRING) schema_name, CAST(NULL AS STRING) table_name FROM system.information_schema.catalog_tags WHERE catalog_name IN ({cats})"
            ")"
        )
        try:
            return _rows(sql)
        except Exception as e:  # noqa: BLE001
            _log("assignments read failed", error=str(e))
            return []

    def metric_view_fqns(self, allowlist: list[str]) -> list[str]:
        if not allowlist:
            return []
        cats = _in_list(allowlist)
        sql = (
            "SELECT table_catalog, table_schema, table_name FROM system.information_schema.tables "
            f"WHERE table_catalog IN ({cats}) AND table_type = 'METRIC_VIEW'"
        )
        try:
            return [
                ".".join(str(v) for v in (r.get("table_catalog"), r.get("table_schema"), r.get("table_name")) if v)
                for r in _rows(sql)
            ]
        except Exception as e:  # noqa: BLE001
            _log("metric_view_fqns read failed", error=str(e))
            return []

    def agents(self) -> list[str]:
        try:
            w = make_workspace_client()
            from genie_space_optimizer.common.genie_client import list_spaces  # optional
            return [f"{s.get('display_name') or s.get('title') or 'Genie Agent'} · {s.get('id')}" for s in list_spaces(w)]
        except Exception as e:  # noqa: BLE001 — agents are best-effort in the ungrouped bucket
            _log("agents read skipped", error=str(e))
            return []

    def lineage_edges(self, allowlist: list[str]) -> list[tuple[str, str]]:
        # Structural adjacency only (used by the L2 scaffold; never invents a domain).
        return []

    # ── Phase 3c (17f) L5 Page-miner inputs — best-effort, degrade to [] ─────
    def measure_signals(self, allowlist: list[str]) -> list[Any]:
        """Governed metric-view measures as :class:`pages.MeasureSignal`s (the concept
        signals for [Routing]/[Guardrail]/[Disambiguation]). Reuses the MV-advisor's
        estate YAML read + ``metric_view_fields`` flatten — no new DESCRIBE path. Any
        failure degrades to [] (MV-D43); measure format / serving-Agent / home-domain
        enrichment is layered in at serve time (17g)."""
        try:
            from genie_space_optimizer.optimization.mv_advisor import estate_metric_view_yamls
            from genie_space_optimizer.optimization.mv_scoring import FIELD_MEASURE, metric_view_fields
            from genie_space_optimizer.ontology.pages import MeasureSignal
        except Exception as e:  # noqa: BLE001
            _log("page measure imports unavailable; mining zero Routing/Guardrail pages", error=str(e))
            return []
        mv_fqns = self.metric_view_fqns(allowlist)
        if not mv_fqns:
            return []
        try:
            yamls = estate_metric_view_yamls(
                spark, mv_fqns, w=make_workspace_client(),
                warehouse_id=os.environ.get("GSO_WAREHOUSE_ID", ""),
            )
            fields = metric_view_fields(yamls)
        except Exception as e:  # noqa: BLE001 — a failed estate read is not evidence of none
            _log("metric-view measure read failed; mining zero measure pages", error=str(e))
            return []
        return [
            MeasureSignal(mv_fqn=f.mv_fqn, name=f.field_name, expression=f.expr, comment=f.text)
            for f in fields if getattr(f, "kind", "") == FIELD_MEASURE and f.field_name and f.expr
        ]

    def coded_column_signals(self, allowlist: list[str]) -> list[Any]:
        """Low-cardinality coded columns ([Taxonomy] signals). Deferred to the serve
        pass (17g) — column value-profiling is not read here — so this degrades to []
        (no [Taxonomy] pages) rather than issue a profiling sweep in the offline slice."""
        return []

    def space_instructions(self) -> list[str]:
        """Existing Agent ``text_instructions`` (READ-ONLY, for the contradiction gate).
        Best-effort; absence simply means no contradiction downgrade (never blocks)."""
        return []


# COMMAND ----------

# L3 ER wiring: the in-process similarity backend by default (Lakebase Search stays
# OFF — enabling it is the §12 human gate), GTE embeddings via the shared FMAPI
# client, and the near-tie LLM adjudicator (degrades if the endpoint is down).
# L4 clustering (Phase 3b) uses the same LLM path for cluster NAMING only (degrades
# to anchor-derived names — MV-D43).
from genie_space_optimizer.ontology import cluster, er, pages, similarity  # noqa: E402

try:
    from genie_space_optimizer.optimization.mv_scoring import FoundationModelEmbeddingClient
    _embedder = FoundationModelEmbeddingClient(make_workspace_client())
except Exception as _e:  # noqa: BLE001 — degrade to string-only ER
    _log("Embedding client unavailable; ER runs string-only", error=str(_e))
    _embedder = None

writer = materialize.SparkSnapshotWriter(spark, catalog, schema)
run = materialize.run_materialize(
    SparkSystemTableReader(),
    writer,
    metastore_id=metastore_id,
    workspace_id=workspace_id,
    trigger=trigger,
    allowlist=allowlist,
    run_id=run_id,
    similarity_backend=similarity.get_similarity_backend(None),  # in-process (Lakebase Search off)
    embedder=_embedder,
    adjudicator=er.default_adjudicator(),
    namer=cluster.default_namer(),  # LLM cluster naming; degrades to anchor names
    # L5 Page mining (Phase 3c) — deterministic detectors + LLM BODY PROSE only
    # (degrades to a deterministic stub + certify=false, MV-D43). Routing ask_genie
    # confirmation degrades to unvalidated (no concept→Agent map wired here).
    page_drafter=pages.default_page_drafter(),
    routing_validator=None,
)
_log("Materialize complete", metastore_id=metastore_id, state=run["state"], tags=run.get("tag_count"),
     domains=run.get("domain_count"), identities=run.get("identity_count"), pages=run.get("page_count"))
dbutils.notebook.exit(json.dumps({"run_id": run["run_id"], "state": run["state"]}, default=str))
