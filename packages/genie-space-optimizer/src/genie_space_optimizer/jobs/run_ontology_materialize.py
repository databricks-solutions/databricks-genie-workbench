# Databricks notebook source
# MAGIC %md
# MAGIC # Ontology Materialize (Phase 2 — nightly + on-demand)
# MAGIC
# MAGIC | Quick Reference | |
# MAGIC |---|---|
# MAGIC | **Task** | `ontology_materialize` |
# MAGIC | **Reads** | job params, `system.tags.governed_tags` + `information_schema.*_tags` + lineage (SP, allowlist-scoped) |
# MAGIC | **Writes** | `genie_ont_runs`, `genie_ont_tag_graph`, `genie_ont_taxonomy_snapshot`, `genie_ont_identity`, `genie_ont_domains`, `genie_ont_members` (idempotent MERGE) |
# MAGIC | **Never writes** | any governed-tag DDL; `genie_ont_pages` / `_consents` / `_suppressions` (17f/17g) |
# MAGIC | **Log label** | `[TASK ONTOLOGY]` |
# MAGIC
# MAGIC ## 🎯 Purpose (Phase-2 §8)
# MAGIC
# MAGIC Materialize the Phase-1 live outputs (governed-tag graph + taxonomy tree) to
# MAGIC Delta so the page reads a stable, sub-second snapshot. The heavy logic lives
# MAGIC in the wheel (`genie_space_optimizer.ontology.*`) using the SAME pure
# MAGIC transforms the Phase-1 routes call, so mirror output == live output. This
# MAGIC notebook is thin glue: read params → SP reads → `run_materialize`.
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

dbutils.widgets.text("workspace_id", "")
dbutils.widgets.text("trigger", "nightly")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "genie_space_optimizer")
dbutils.widgets.text("catalog_allowlist", "[]")
dbutils.widgets.text("run_id", "")

workspace_id = dbutils.widgets.get("workspace_id").strip()
trigger = dbutils.widgets.get("trigger").strip() or "nightly"
catalog = dbutils.widgets.get("catalog").strip() or os.environ.get("GSO_CATALOG", "")
schema = dbutils.widgets.get("schema").strip() or os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
run_id = dbutils.widgets.get("run_id").strip() or None
try:
    allowlist = [str(c).strip() for c in json.loads(dbutils.widgets.get("catalog_allowlist") or "[]") if str(c).strip()]
except (TypeError, ValueError):
    allowlist = []

if not workspace_id:
    w0 = make_workspace_client()
    try:
        workspace_id = str(w0.get_workspace_id())
    except Exception:
        workspace_id = "default"

_log("Resolved params", workspace_id=workspace_id, trigger=trigger, catalog=catalog, schema=schema, allowlist=allowlist)

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


# COMMAND ----------

# L3 ER wiring: the in-process similarity backend by default (Lakebase Search stays
# OFF — enabling it is the §12 human gate), GTE embeddings via the shared FMAPI
# client, and the near-tie LLM adjudicator (degrades if the endpoint is down).
# L4 clustering (Phase 3b) uses the same LLM path for cluster NAMING only (degrades
# to anchor-derived names — MV-D43).
from genie_space_optimizer.ontology import cluster, er, similarity  # noqa: E402

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
    workspace_id=workspace_id,
    trigger=trigger,
    allowlist=allowlist,
    run_id=run_id,
    similarity_backend=similarity.get_similarity_backend(None),  # in-process (Lakebase Search off)
    embedder=_embedder,
    adjudicator=er.default_adjudicator(),
    namer=cluster.default_namer(),  # LLM cluster naming; degrades to anchor names
)
_log("Materialize complete", state=run["state"], tags=run.get("tag_count"),
     domains=run.get("domain_count"), identities=run.get("identity_count"))
dbutils.notebook.exit(json.dumps({"run_id": run["run_id"], "state": run["state"]}, default=str))
