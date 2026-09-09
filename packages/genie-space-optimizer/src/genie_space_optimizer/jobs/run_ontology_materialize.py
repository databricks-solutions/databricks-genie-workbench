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
# MAGIC | **Reads (17g)** | `genie_ont_suppressions` (READ-ONLY, so the L6 gate skips curator-dismissed proposals, MV-D26) |
# MAGIC | **Never writes** | any governed-tag DDL; `genie_ont_consents` / `_suppressions` (the backend OBO route is their only writer) |
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

# TABLESAMPLE size for coded-column value profiling (MV-D63). Comfortably above the
# cardinality ceiling so a genuinely wide column profiles wide (→ dropped) while a small
# code list is fully captured.
_CODED_COLUMN_SAMPLE_ROWS = 200


def _log(msg: str, **kw: Any) -> None:
    extra = " ".join(f"{k}={v}" for k, v in kw.items())
    print(f"{_TASK_LABEL} {msg}{(' ' + extra) if extra else ''}")


# MLflow OpenAI autolog — token/cost/latency spans on every ontology LLM call
# (drafter / namer / adjudicator, MV-D65). Best-effort: a failure here just means
# no tracing; the run continues (degrade-not-hang, MV-D43).
try:
    import mlflow  # noqa: E402

    mlflow.openai.autolog()
    _log("mlflow.openai.autolog enabled for ontology LLM calls")
except Exception as _e:  # noqa: BLE001 — tracing is optional, never blocks the run
    _log("mlflow.openai.autolog unavailable; ontology LLM calls run untraced", error=str(_e))


# COMMAND ----------

dbutils.widgets.text("metastore_id", "")
dbutils.widgets.text("workspace_id", "")
dbutils.widgets.text("trigger", "nightly")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "genie_space_optimizer")
dbutils.widgets.text("catalog_allowlist", "[]")
dbutils.widgets.text("run_id", "")
# Warehouse for the metric-view YAML read (measure_signals → estate_metric_view_yamls).
# Serverless Spark Connect cannot expose MV metadata, so DESCRIBE ... AS JSON needs a SQL
# warehouse; threaded as a job parameter (the env var is not set on serverless jobs).
dbutils.widgets.text("warehouse_id", "")
# LLM endpoint for the batch enrichers (page-body drafts, cluster naming, ER adjudication).
# Threaded as a job parameter like the optimization jobs (run_optimize / run_intake) because
# the env var is NOT set on serverless jobs; empty ⇒ the wheel's default endpoint. The job
# run_as identity needs CAN QUERY on this endpoint, else the drafts degrade to stub +
# certify=false (MV-D43 degrade-not-hang; MV-D65 injected identity).
dbutils.widgets.text("llm_model", "")
# Stage 3 curation policy (MV-D57) — job_parameters with in-code defaults so a
# param-less run (nightly, or an older launcher) still works (MV-D43).
dbutils.widgets.text("domain_facet_denylist", "[]")
dbutils.widgets.text("domain_min_tables", "3")
dbutils.widgets.text("domain_min_schemas", "2")
dbutils.widgets.text("domain_require_connection", "true")
# Stage 3.2 edge hygiene + Gate-B (MV-D61/62) — job_parameters with in-code defaults so
# a param-less run keeps today's behavior except the two principled cuts (spec §3).
dbutils.widgets.text("domain_schema_denylist", "[\"information_schema\"]")
dbutils.widgets.text("domain_join_col_suffixes", "[\"_id\", \"_key\"]")
dbutils.widgets.text("domain_join_col_max_schemas", "2")
dbutils.widgets.text(
    "domain_join_col_denylist",
    "[\"id\", \"user_id\", \"workspace_id\", \"category_id\", \"tenant_id\", \"account_id\"]",
)
dbutils.widgets.text("domain_max_diffuse_schemas", "6")
dbutils.widgets.text("domain_min_home_concentration", "0.5")
# Stage 4.1b coded-column batch feed + Page-attachment gate (MV-D63/64) — job_parameters
# with in-code defaults so a param-less run (nightly, older launcher) still works (MV-D43).
dbutils.widgets.text("coded_column_max_columns", "300")
dbutils.widgets.text("coded_column_max_cardinality", "40")
dbutils.widgets.text("page_require_domain", "true")
# Stage 4.1d bounded auto-drafting (MV-D66) — the "super sure" cap: certify + corroboration
# ≥ MIN, top-N by confidence. A param-less run applies the shipped defaults (aligned with
# pages.PAGE_AUTODRAFT_*). max_pages=0 ⇒ a pure-stub (zero page-LLM) batch.
dbutils.widgets.text("page_autodraft_min_corroboration", "3")
dbutils.widgets.text("page_autodraft_max_pages", "50")
# Stage 4.1e (MV-D67) — bounded worker cap for the LLM enrichers (surfaced-Domain renames +
# super-sure Page drafts). k<=1 ⇒ sequential; the shipped default keeps fan-out small.
dbutils.widgets.text("enrich_max_workers", "4")
# Stage 4.1f (MV-D68) — the pages pattern generalized to the last two unbounded LLM paths.
# ER near-tie adjudication: LLM only the top-N (score desc) near-ties clearing the floor,
# capped; the rest degrade to `escalate` (curator dedupe proposals). Domain renames: LLM
# only the top-N surfaced create Domains. Both keep the batch LLM cost a small CONSTANT.
dbutils.widgets.text("er_adjudicate_max_pairs", "30")
dbutils.widgets.text("er_adjudicate_min_score", "0.85")
dbutils.widgets.text("rename_max_domains", "10")

metastore_id = dbutils.widgets.get("metastore_id").strip()
workspace_id = dbutils.widgets.get("workspace_id").strip()
trigger = dbutils.widgets.get("trigger").strip() or "nightly"
catalog = dbutils.widgets.get("catalog").strip() or os.environ.get("GSO_CATALOG", "")
schema = dbutils.widgets.get("schema").strip() or os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
run_id = dbutils.widgets.get("run_id").strip() or None
warehouse_id = dbutils.widgets.get("warehouse_id").strip() or os.environ.get("GSO_WAREHOUSE_ID", "")
# Set LLM_MODEL before the enrichers resolve get_llm_endpoint() below (mirrors the
# optimization jobs). Empty ⇒ leave env untouched so the wheel default applies (MV-D43).
llm_model = dbutils.widgets.get("llm_model").strip()
if llm_model:
    os.environ["LLM_MODEL"] = llm_model
try:
    allowlist = [str(c).strip() for c in json.loads(dbutils.widgets.get("catalog_allowlist") or "[]") if str(c).strip()]
except (TypeError, ValueError):
    allowlist = []

# Stage 3 curation policy (MV-D57), each parsed defensively → in-code default (MV-D43).
try:
    facet_denylist = [str(c).strip() for c in json.loads(dbutils.widgets.get("domain_facet_denylist") or "[]") if str(c).strip()]
except (TypeError, ValueError):
    facet_denylist = []
try:
    domain_min_tables = int(dbutils.widgets.get("domain_min_tables").strip() or "3")
except (TypeError, ValueError):
    domain_min_tables = 3
try:
    domain_min_schemas = int(dbutils.widgets.get("domain_min_schemas").strip() or "2")
except (TypeError, ValueError):
    domain_min_schemas = 2
domain_require_connection = (dbutils.widgets.get("domain_require_connection").strip().lower() or "true") != "false"

# Stage 3.2 edge hygiene + Gate-B (MV-D61/62), each parsed defensively → in-code default
# (MV-D43). A param-less run still applies the shipped default cuts (spec §3).
from genie_space_optimizer.ontology import schema_signals as _ss  # noqa: E402


def _parse_str_list(name: str, default: list[str]) -> list[str]:
    try:
        parsed = [str(c).strip() for c in json.loads(dbutils.widgets.get(name) or "[]") if str(c).strip()]
    except (TypeError, ValueError):
        return list(default)
    return parsed


def _parse_int(name: str, default: int) -> int:
    try:
        return int(dbutils.widgets.get(name).strip() or str(default))
    except (TypeError, ValueError):
        return default


def _parse_float(name: str, default: float) -> float:
    try:
        return float(dbutils.widgets.get(name).strip() or str(default))
    except (TypeError, ValueError):
        return default


schema_denylist = _parse_str_list("domain_schema_denylist", ["information_schema"])
join_col_suffixes = tuple(_parse_str_list("domain_join_col_suffixes", list(_ss.JOIN_COLUMN_SUFFIXES))) or _ss.JOIN_COLUMN_SUFFIXES
join_col_max_schemas = _parse_int("domain_join_col_max_schemas", _ss.MAX_SCHEMAS_PER_SHARED_COLUMN)
join_col_denylist = frozenset(_parse_str_list("domain_join_col_denylist", list(_ss.GENERIC_JOIN_COLUMN_DENYLIST)))
max_diffuse_schemas = _parse_int("domain_max_diffuse_schemas", 6)
min_home_concentration = _parse_float("domain_min_home_concentration", 0.5)

# Stage 4.1b (MV-D63/64) — coded-column bounds + the Page-attachment gate toggle, each
# parsed defensively → in-code default (MV-D43). Cardinality default = the miner's own
# _TAXONOMY_MAX_CARDINALITY (kept aligned via the wheel constant).
from genie_space_optimizer.ontology import coded_columns as _cc  # noqa: E402

coded_column_max_columns = _parse_int("coded_column_max_columns", _cc.CODED_COLUMN_MAX_COLUMNS)
coded_column_max_cardinality = _parse_int("coded_column_max_cardinality", _cc.CODED_COLUMN_MAX_CARDINALITY)
page_require_domain = (dbutils.widgets.get("page_require_domain").strip().lower() or "true") != "false"
# Stage 4.1d (MV-D66) — bounded auto-draft cap, parsed defensively → in-code default
# (aligned with the wheel's pages.PAGE_AUTODRAFT_* constants).
page_autodraft_min_corroboration = _parse_int("page_autodraft_min_corroboration", 3)
page_autodraft_max_pages = _parse_int("page_autodraft_max_pages", 50)
enrich_max_workers = _parse_int("enrich_max_workers", 4)
# Stage 4.1f (MV-D68) — ER near-tie cap + rename cap, parsed defensively → in-code default
# (aligned with the wheel's er.ER_ADJUDICATE_* / cluster.RENAME_MAX_DOMAINS constants).
er_adjudicate_max_pairs = _parse_int("er_adjudicate_max_pairs", 30)
er_adjudicate_min_score = _parse_float("er_adjudicate_min_score", 0.85)
rename_max_domains = _parse_int("rename_max_domains", 10)


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
    """SP/Spark reads of the same system tables Phase 1 reads live (allowlist-scoped).

    The Stage-3.2 edge-hygiene knobs (MV-D61) are held on the instance: the non-business
    ``schema_denylist`` is applied to EVERY row input before any edge is built, and the
    proxy join knobs (suffixes / span cap / name denylist) thread into
    ``schema_signals.join_key_edges`` (a declared FK is never span-capped)."""

    def __init__(
        self,
        *,
        schema_denylist: list[str] | None = None,
        join_col_suffixes: tuple[str, ...] = None,
        join_col_max_schemas: int = None,
        join_col_denylist: frozenset[str] = None,
        warehouse_id: str = "",
        coded_column_max_columns: int = None,
        coded_column_max_cardinality: int = None,
    ) -> None:
        from genie_space_optimizer.ontology import coded_columns as cc
        from genie_space_optimizer.ontology import schema_signals as ss
        self._schema_denylist = list(schema_denylist or ["information_schema"])
        # SQL warehouse for the metric-view YAML DESCRIBE (measure_signals) AND the coded-
        # column value profiling (coded_column_signals, MV-D63); empty ⇒ both short-circuit
        # (MV detection → OUTCOME_NO_WAREHOUSE, coded columns → []) and degrade to [].
        self._warehouse_id = warehouse_id or ""
        # Coded-column batch feed bounds (MV-D57/D63), in-code defaults from the wheel.
        self._coded_max_columns = (
            int(coded_column_max_columns) if coded_column_max_columns is not None else cc.CODED_COLUMN_MAX_COLUMNS
        )
        self._coded_max_cardinality = (
            int(coded_column_max_cardinality) if coded_column_max_cardinality is not None else cc.CODED_COLUMN_MAX_CARDINALITY
        )
        self._join_suffixes = tuple(join_col_suffixes) if join_col_suffixes else ss.JOIN_COLUMN_SUFFIXES
        self._join_max_schemas = (
            int(join_col_max_schemas) if join_col_max_schemas is not None else ss.MAX_SCHEMAS_PER_SHARED_COLUMN
        )
        self._join_name_denylist = (
            frozenset(join_col_denylist) if join_col_denylist is not None else ss.GENERIC_JOIN_COLUMN_DENYLIST
        )

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
        # ``tag_value`` (Stage 2, MV-D54): information_schema *_tags carry the per-
        # assignment value; a value-carrying tag (``mvm_subdomain``) names a sub-domain.
        # A value-free tag simply reads NULL — the member dict stays byte-identical.
        sql = (
            "SELECT tag_name, catalog_name, schema_name, table_name, tag_value FROM ("
            f" SELECT tag_name, catalog_name, schema_name, table_name, tag_value FROM system.information_schema.table_tags WHERE catalog_name IN ({cats})"
            " UNION ALL"
            f" SELECT tag_name, catalog_name, schema_name, CAST(NULL AS STRING) table_name, tag_value FROM system.information_schema.schema_tags WHERE catalog_name IN ({cats})"
            " UNION ALL"
            f" SELECT tag_name, catalog_name, CAST(NULL AS STRING) schema_name, CAST(NULL AS STRING) table_name, tag_value FROM system.information_schema.catalog_tags WHERE catalog_name IN ({cats})"
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

    def table_types(self, allowlist: list[str]) -> dict[str, str]:
        # Stage-1 asset typing (MV-D90): {fqn -> table_type} for every relation in the
        # allowlisted catalogs, so ``assemble_tag_graph`` can type a tagged Metric View as
        # ``metric_view`` (not ``table``). Same allowlist-scoped ``information_schema.tables``
        # source as ``metric_view_fqns``; degrades to {} on any missing grant (MV-D43).
        if not allowlist:
            return {}
        cats = _in_list(allowlist)
        sql = (
            "SELECT table_catalog, table_schema, table_name, table_type "
            "FROM system.information_schema.tables "
            f"WHERE table_catalog IN ({cats})"
        )
        try:
            out: dict[str, str] = {}
            for r in _rows(sql):
                fqn = ".".join(
                    str(v) for v in (r.get("table_catalog"), r.get("table_schema"), r.get("table_name")) if v
                )
                ttype = r.get("table_type")
                if fqn and ttype:
                    out[fqn] = str(ttype)
            return out
        except Exception as e:  # noqa: BLE001
            _log("table_types read failed", error=str(e))
            return {}

    def agents(self) -> list[str]:
        try:
            w = make_workspace_client()
            from genie_space_optimizer.common.genie_client import list_spaces  # optional
            return [f"{s.get('display_name') or s.get('title') or 'Genie Agent'} · {s.get('id')}" for s in list_spaces(w)]
        except Exception as e:  # noqa: BLE001 — agents are best-effort in the ungrouped bucket
            _log("agents read skipped", error=str(e))
            return []

    # ── Stage 2 (MV-D91): Genie Agents placed by their APPLIED governed tag ──────
    def entity_tag_assignments(self, entity_type: str = "geniespaces") -> list[dict[str, Any]]:
        """Applied governed-tag rows for each Genie space, read off the ENTITY-TAG-
        ASSIGNMENTS API (NOT information_schema — governed tags on workspace objects live
        there, which is why the materializer was blind to them). Enumerate spaces via
        ``list_spaces`` and, per space, call
        ``w.workspace_entity_tag_assignments.list_tag_assignments("geniespaces", id)``;
        if the pinned ``databricks-sdk==0.117.0`` lacks the method, fall back to REST
        ``api_client.do("GET", /api/2.0/entity-tag-assignments/geniespaces/{id}/tags)``
        (NO dependency bump). Keep a tag ONLY when ``transforms.is_domain_entity_tag`` says
        aboutness (drops facets — ``certified``/``contains_synthetic`` — and reserved
        ``system.*``/``class.*``/``sap.*`` tags). Returns rows shaped like the
        ``assignments()`` union — ``{tag_name, tag_value, member_id: "agent:<id>"}`` —
        sorted/deterministic. Bounded fan-out (~17 calls); ANY API/permission failure
        degrades to [] (MV-D43), and scope reconciliation is done in the wheel."""
        from genie_space_optimizer.ontology import transforms
        et = entity_type or "geniespaces"
        try:
            w = make_workspace_client()
            entities = self._enumerate_tag_entities(w, et)
        except Exception as e:  # noqa: BLE001 — no entities / no client ⇒ no applied rows
            _log("entity_tag_assignments enumerate skipped", entity_type=et, error=str(e))
            return []
        out: list[dict[str, Any]] = []
        for entity_id, member_id in entities:
            if not entity_id:
                continue
            for tag_key, tag_value in self._list_entity_tags(w, et, str(entity_id)):
                if not transforms.is_domain_entity_tag(tag_key):
                    continue
                row: dict[str, Any] = {"tag_name": tag_key, "member_id": member_id}
                if tag_value is not None and str(tag_value) != "":
                    row["tag_value"] = str(tag_value)
                out.append(row)
        # Deterministic (MV-D82): stable by (member, tag, value).
        return sorted(out, key=lambda r: (r["member_id"], r["tag_name"], str(r.get("tag_value") or "")))

    def _enumerate_tag_entities(self, w: Any, entity_type: str) -> list[tuple[str, str]]:
        """``[(entity_id, member_id)]`` for the entity-tag fan-out — dispatched by entity
        type so the SAME per-entity tag read (``_list_entity_tags``) + keep-filter serves
        both stages. ``geniespaces`` enumerates Genie spaces (``list_spaces`` → member
        ``agent:<id>``, Stage 2); ``dashboards`` enumerates Lakeview dashboards
        (``w.lakeview.list()`` → member ``dashboard:<id>``, Stage 3, MV-D92). An id is coerced
        to str; a missing id is dropped by the caller."""
        if entity_type == "dashboards":
            return [(did, f"dashboard:{did}") for did, _name in self._list_dashboards(w)]
        from genie_space_optimizer.common.genie_client import list_spaces
        return [(str(s.get("id")), f"agent:{s.get('id')}") for s in list_spaces(w) if s.get("id")]

    def _list_dashboards(self, w: Any) -> list[tuple[str, str]]:
        """``[(dashboard_id, display_name)]`` from ``w.lakeview.list()`` (Stage 3, MV-D92) —
        the single enumeration reused by the entity-tag fan-out, ``dashboard_scopes`` and
        ``dashboard_names``. Tolerates SDK objects or dicts; degrades to [] on any failure
        (MV-D43)."""
        try:
            listing = w.lakeview.list()
        except Exception as e:  # noqa: BLE001 — no dashboards / no access ⇒ none
            _log("lakeview list skipped", error=str(e))
            return []
        out: list[tuple[str, str]] = []
        for d in listing or []:
            did = getattr(d, "dashboard_id", None) if not isinstance(d, dict) else d.get("dashboard_id")
            if not did:
                continue
            name = getattr(d, "display_name", None) if not isinstance(d, dict) else d.get("display_name")
            out.append((str(did), str(name or did)))
        return out

    def _list_entity_tags(self, w: Any, entity_type: str, entity_id: str) -> list[tuple[str, str | None]]:
        """One entity's ``(tag_key, tag_value)`` pairs — SDK first, REST fallback, else []
        (per-entity degrade so one unreadable space never sinks the batch, MV-D43)."""
        svc = getattr(w, "workspace_entity_tag_assignments", None)
        if svc is not None and hasattr(svc, "list_tag_assignments"):
            try:
                return [
                    (str(getattr(a, "tag_key", "") or ""), getattr(a, "tag_value", None))
                    for a in svc.list_tag_assignments(entity_type, entity_id)
                    if getattr(a, "tag_key", None)
                ]
            except Exception as e:  # noqa: BLE001 — fall through to REST
                _log("list_tag_assignments SDK failed; trying REST", entity=entity_id, error=str(e))
        try:
            resp = w.api_client.do("GET", f"/api/2.0/entity-tag-assignments/{entity_type}/{entity_id}/tags")
        except Exception as e:  # noqa: BLE001 — no access to this entity's tags
            _log("entity-tag REST read skipped", entity=entity_id, error=str(e))
            return []
        items = []
        if isinstance(resp, dict):
            for k in ("tag_assignments", "tags", "assignments"):
                v = resp.get(k)
                if isinstance(v, list):
                    items = v
                    break
        return [
            (str(it.get("tag_key") or ""), it.get("tag_value"))
            for it in items
            if isinstance(it, dict) and it.get("tag_key")
        ]

    def agent_scopes(self, allowlist: list[str]) -> dict[str, list[str]]:
        """Read-edge overlay (MV-D91 Build B): ``{space_id -> sorted[table_fqn]}`` — the
        tables/MVs each Genie space reads, reusing ``fetch_space_config``'s resolved
        ``_tables`` + ``_metric_views`` identifiers (the create/scan resolution). Scoped to
        the allowlisted catalogs so an out-of-scope table never draws an edge. EVERY
        enumerated space is a key (a scopeless space maps to ``[]`` so it still gets an
        ``agent:<id>`` node). ANY failure degrades to {} ⇒ byte-identical graph (MV-D43)."""
        try:
            w = make_workspace_client()
            from genie_space_optimizer.common.genie_client import fetch_space_config, list_spaces
            spaces = list_spaces(w)
        except Exception as e:  # noqa: BLE001
            _log("agent_scopes enumerate skipped", error=str(e))
            return {}
        cats = {str(c).strip() for c in (allowlist or []) if str(c).strip()}

        def _in_scope(fqn: str) -> bool:
            return not cats or (str(fqn).split(".", 1)[0] in cats)

        out: dict[str, list[str]] = {}
        for s in spaces:
            sid = s.get("id")
            if not sid:
                continue
            try:
                cfg = fetch_space_config(w, str(sid))
                refs = list(cfg.get("_tables") or []) + list(cfg.get("_metric_views") or [])
            except Exception as e:  # noqa: BLE001 — a space we cannot read has no edges
                _log("agent_scopes space read skipped", space=str(sid), error=str(e))
                refs = []
            fqns = sorted({str(r) for r in refs if r and _in_scope(str(r))})
            out[str(sid)] = fqns
        return dict(sorted(out.items()))

    def agent_names(self) -> dict[str, str]:
        """``{space_id -> display_name}`` so the map labels an agent node with its space
        name (a raw id is not human-readable). Degrades to {} ⇒ id fallback (MV-D43)."""
        try:
            w = make_workspace_client()
            from genie_space_optimizer.common.genie_client import list_spaces
            return {
                str(s["id"]): str(s.get("title") or s.get("display_name") or s["id"])
                for s in list_spaces(w) if s.get("id")
            }
        except Exception as e:  # noqa: BLE001
            _log("agent_names read skipped", error=str(e))
            return {}

    # ── Stage 3 (MV-D92): Lakeview Dashboards as first-class nodes ───────────────
    def dashboard_scopes(self, allowlist: list[str]) -> dict[str, list[str]]:
        """Read-edge overlay (MV-D92 Build B): ``{dashboard_id -> sorted[table_fqn]}`` — the
        tables/MVs each Lakeview dashboard's datasets read, resolved from dataset lineage
        (``system.access.table_lineage`` keyed by ``entity_metadata.dashboard_id``, the same
        table GenieWatch reads). Scoped to the allowlisted catalogs so an out-of-scope table
        never draws an edge. EVERY enumerated dashboard is a key (a dashboard with no readable
        lineage maps to ``[]`` so it still gets a ``dashboard:<id>`` node). ANY failure degrades
        to {} ⇒ byte-identical graph (MV-D43). Mirrors ``agent_scopes``."""
        try:
            w = make_workspace_client()
            dashboards = self._list_dashboards(w)
        except Exception as e:  # noqa: BLE001
            _log("dashboard_scopes enumerate skipped", error=str(e))
            return {}
        cats = {str(c).strip() for c in (allowlist or []) if str(c).strip()}

        def _in_scope(fqn: str) -> bool:
            return not cats or (str(fqn).split(".", 1)[0] in cats)

        lineage: dict[str, set[str]] = {}
        try:
            for r in _rows(
                "SELECT entity_metadata.dashboard_id AS dashboard_id, "
                "source_table_full_name AS full_name "
                "FROM system.access.table_lineage "
                "WHERE entity_metadata.dashboard_id IS NOT NULL "
                "AND source_table_full_name IS NOT NULL "
                "GROUP BY 1, 2"
            ):
                did, fqn = r.get("dashboard_id"), r.get("full_name")
                if did and fqn and _in_scope(str(fqn)):
                    lineage.setdefault(str(did), set()).add(str(fqn))
        except Exception as e:  # noqa: BLE001 — no lineage grant ⇒ nodes with no read edges
            _log("dashboard_scopes lineage read skipped", error=str(e))
            lineage = {}

        out: dict[str, list[str]] = {}
        for did, _name in dashboards:
            out[str(did)] = sorted(lineage.get(str(did), set()))
        return dict(sorted(out.items()))

    def dashboard_names(self) -> dict[str, str]:
        """``{dashboard_id -> display_name}`` so the map labels a dashboard node with its name
        (a raw id is not human-readable). Degrades to {} ⇒ id fallback (MV-D43)."""
        try:
            w = make_workspace_client()
            return {did: name for did, name in self._list_dashboards(w) if did}
        except Exception as e:  # noqa: BLE001
            _log("dashboard_names read skipped", error=str(e))
            return {}

    def lineage_edges(self, allowlist: list[str]) -> list[tuple[str, str]]:
        # Structural adjacency only (used by the L2 scaffold; never invents a domain).
        return []

    # ── Stage 1 (MV-D52) structural grouping signals — read as run_as, allowlist-
    # scoped, degrade to empty on any missing grant (MV-D43). All parsing is the PURE
    # schema_signals module; this class only issues the queries and hands over rows. ──
    def _rows_safe(self, sql: str, what: str) -> list[dict[str, Any]]:
        try:
            return _rows(sql)
        except Exception as e:  # noqa: BLE001 — a missing/unreadable grant never blocks
            _log(f"{what} read skipped", error=str(e))
            return []

    def _per_catalog(self, allowlist: list[str], relation: str, cols: str, what: str) -> list[dict[str, Any]]:
        """Read ``<catalog>.information_schema.<relation>`` for each allowlisted catalog
        (constraint/column metadata is per-catalog, not in system.information_schema),
        degrading each catalog independently."""
        out: list[dict[str, Any]] = []
        for cat in allowlist:
            ident = "`" + str(cat).replace("`", "``") + "`"
            out += self._rows_safe(f"SELECT {cols} FROM {ident}.information_schema.{relation}", f"{relation}({cat})")
        return out

    def join_key_edges(self, allowlist: list[str]) -> list[tuple]:
        if not allowlist:
            return []
        from genie_space_optimizer.ontology import schema_signals
        rc = self._per_catalog(
            allowlist, "referential_constraints",
            "constraint_catalog, constraint_schema, constraint_name, "
            "unique_constraint_catalog, unique_constraint_schema, unique_constraint_name",
            "referential_constraints",
        )
        kcu = self._per_catalog(
            allowlist, "key_column_usage",
            "constraint_catalog, constraint_schema, constraint_name, "
            "table_catalog, table_schema, table_name, column_name",
            "key_column_usage",
        )
        ccu = self._per_catalog(
            allowlist, "constraint_column_usage",
            "constraint_catalog, constraint_schema, constraint_name, "
            "table_catalog, table_schema, table_name, column_name",
            "constraint_column_usage",
        )
        cols = self._per_catalog(
            allowlist, "columns", "table_catalog, table_schema, table_name, column_name", "columns",
        )
        # MV-D61: drop non-business schemas from EVERY row input BEFORE any edge is built
        # (spec §2.1.1), then thread the proxy knobs (fk_edges is never span-capped).
        dl = self._schema_denylist
        return schema_signals.join_key_edges(
            schema_signals.filter_denylisted_schemas(rc, denylist=dl),
            schema_signals.filter_denylisted_schemas(kcu, denylist=dl),
            schema_signals.filter_denylisted_schemas(ccu, denylist=dl),
            schema_signals.filter_denylisted_schemas(cols, denylist=dl),
            join_suffixes=self._join_suffixes,
            max_schemas=self._join_max_schemas,
            name_denylist=self._join_name_denylist,
        )

    def mv_membership(self, allowlist: list[str]) -> dict[str, list[str]]:
        from genie_space_optimizer.ontology import schema_signals
        mv_fqns = self.metric_view_fqns(allowlist)
        if not mv_fqns:
            return {}
        try:
            from genie_space_optimizer.optimization.mv_advisor import estate_metric_view_yamls
            yamls = estate_metric_view_yamls(
                spark, mv_fqns, w=make_workspace_client(),
                warehouse_id=self._warehouse_id or os.environ.get("GSO_WAREHOUSE_ID", ""),
            )
        except Exception as e:  # noqa: BLE001 — a failed MV-YAML read yields no membership
            _log("mv_membership read skipped", error=str(e))
            return {}
        mapped = schema_signals.mv_membership_map(yamls)
        dl = self._schema_denylist
        if not dl:
            return mapped

        def _ok(fqn: str) -> bool:
            parts = str(fqn).split(".")
            row = {"table_catalog": parts[0] if parts else None,
                   "table_schema": parts[1] if len(parts) > 1 else None}
            return bool(schema_signals.filter_denylisted_schemas([row], denylist=dl))

        # MV-D61: drop an MV (or a source) that lives in a denylisted schema.
        out: dict[str, list[str]] = {}
        for mv_fqn, sources in mapped.items():
            if not _ok(mv_fqn):
                continue
            kept = [s for s in sources if _ok(s)]
            if kept:
                out[mv_fqn] = kept
        return out

    def schema_affinity(self, allowlist: list[str]) -> dict[str, list[str]]:
        if not allowlist:
            return {}
        from genie_space_optimizer.ontology import schema_signals
        cats = _in_list(allowlist)
        rows = self._rows_safe(
            "SELECT table_catalog, table_schema, table_name FROM system.information_schema.tables "
            f"WHERE table_catalog IN ({cats}) AND table_type IN ('MANAGED', 'EXTERNAL', 'MANAGED_SHALLOW_CLONE')",
            "schema_affinity",
        )
        # MV-D61: a denylisted schema contributes no shared-schema affinity either.
        rows = schema_signals.filter_denylisted_schemas(rows, denylist=self._schema_denylist)
        return schema_signals.schema_affinity_map(rows)

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
                warehouse_id=self._warehouse_id or os.environ.get("GSO_WAREHOUSE_ID", ""),
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
        """Low-cardinality coded columns ([Taxonomy] signals) via a BOUNDED two-pass read
        (MV-D63). Pass 1 is a metadata-only prefilter over the SAME allowlist-scoped,
        denylist-filtered ``information_schema.columns`` read ``comment_signals`` issues
        (+ ``data_type``): keep STRING/CHAR/small-INT columns that look coded (name or
        enum-like comment). Pass 2 profiles at most ``self._coded_max_columns`` survivors
        (sorted FQN) against ``self._warehouse_id`` by REUSING the ``wide_schema_profile``
        builders (``approx_count_distinct`` + value-list), keeping a column iff its
        distinct count is within ``self._coded_max_cardinality``. The pure prefilter /
        cap / assembly is ``ontology.coded_columns``; this method is the warehouse I/O.
        ``self._warehouse_id == ""`` ⇒ [] (no profiling); ANY failure ⇒ [] (MV-D43)."""
        if not allowlist or not self._warehouse_id:
            return []
        try:
            from genie_space_optimizer.ontology import coded_columns, schema_signals
        except Exception as e:  # noqa: BLE001 — wheel/imports unavailable → mine zero
            _log("coded-column imports unavailable; mining zero taxonomy pages", error=str(e))
            return []
        col_rows = schema_signals.filter_denylisted_schemas(
            self._per_catalog(
                allowlist, "columns",
                "table_catalog, table_schema, table_name, column_name, data_type, comment",
                "coded_columns",
            ),
            denylist=self._schema_denylist,
        )
        governed_refs = self._governed_coded_refs(allowlist)
        w = make_workspace_client()

        def _profiler(cand: Any) -> tuple[Any, list[str]]:
            return self._profile_coded_column(w, cand)

        return coded_columns.coded_column_signals(
            col_rows, warehouse_id=self._warehouse_id, profiler=_profiler,
            governed_refs=governed_refs,
            max_columns=self._coded_max_columns, max_cardinality=self._coded_max_cardinality,
        )

    def _governed_coded_refs(self, allowlist: list[str]) -> set[str]:
        """Best-effort set of coded-column refs (``cat.sch.tbl.col``) that carry a
        GOVERNED tag — the ``governed=True`` half of MV-D63 (the CHECK-constraint enum
        half stays a later best-effort). Reads column-level tag assignments per catalog
        and keeps only those whose ``tag_name`` is in ``system.tags.governed_tags``. Any
        missing grant / unreadable relation degrades to ``set()`` (governed=False)."""
        try:
            governed_names = {str(t.get("tag_name")) for t in self.governed_tags() if t.get("tag_name")}
            if not governed_names:
                return set()
            rows = self._per_catalog(
                allowlist, "column_tags",
                "catalog_name, schema_name, table_name, column_name, tag_name",
                "column_tags",
            )
            return {
                f"{r.get('catalog_name')}.{r.get('schema_name')}.{r.get('table_name')}.{r.get('column_name')}"
                for r in rows
                if str(r.get("tag_name")) in governed_names
                and r.get("catalog_name") and r.get("schema_name") and r.get("table_name") and r.get("column_name")
            }
        except Exception as e:  # noqa: BLE001 — governed enrichment is best-effort
            _log("governed coded-column tags read skipped", error=str(e))
            return set()

    def _profile_coded_column(self, w: Any, cand: Any) -> tuple[Any, list[str]]:
        """Profile one candidate against the warehouse, REUSING the ``wide_schema_profile``
        builders (MV-D63, "reuse don't fork"): an ``approx_count_distinct`` aggregate for
        the cardinality gate + a ``collect_set`` value list, each a TABLESAMPLE-bounded
        statement executed as the job ``run_as`` identity (MV-D50). Returns
        ``(distinct_count, values)``; ``(None, [])`` when the cardinality read did not
        succeed, so the column is dropped rather than kept without a bound."""
        import uuid as _uuid

        from genie_space_optimizer.optimization import wide_schema_profile as wsp

        parts = str(cand.table_fqn).split(".")
        if len(parts) != 3:
            return None, []
        asset_key = (parts[0], parts[1], parts[2])
        asset = {"asset_key": asset_key, "asset_type": "table"}
        column = {"name": cand.column, "column_key": (*asset_key, cand.column), "data_type": cand.data_type or "string"}
        run_id = _uuid.uuid4().hex

        agg = wsp._aggregate_item(asset, [column], sample_size=_CODED_COLUMN_SAMPLE_ROWS)
        ares = wsp._execute(w, self._warehouse_id, agg, run_id=run_id)
        distinct = None
        if ares.state == "succeeded" and ares.rows:
            for spec in agg.metrics:
                if spec.metric == "cardinality":
                    raw = ares.rows[0].get(spec.alias)
                    distinct = int(raw) if raw is not None else None
                    break
        if distinct is None:
            return None, []
        vitem = wsp._value_list_item(asset, column, sample_size=_CODED_COLUMN_SAMPLE_ROWS)
        vres = wsp._execute(w, self._warehouse_id, vitem, run_id=run_id)
        values = wsp._parse_values(vres.rows[0].get("values")) if (vres.state == "succeeded" and vres.rows) else []
        return distinct, values

    def comment_signals(self, allowlist: list[str]) -> list[Any]:
        """Business terms carried in table/column COMMENTs as :class:`pages.CommentSignal`s
        — the broadened Page trigger (MV-D55). A best-effort ``information_schema`` read
        scoped to the allowlist (per-catalog, denylist-filtered like every other schema
        read, MV-D61); term = the asset's own name (column/table) so a comment corroborates
        that asset's concept and its text enriches the synonym vocabulary. A missing grant
        or unreadable relation degrades to [] (mine zero, MV-D43). Genie history is NOT
        read — that trigger stays a dormant seam."""
        if not allowlist:
            return []
        try:
            from genie_space_optimizer.ontology import schema_signals
            from genie_space_optimizer.ontology.pages import CommentSignal
        except Exception as e:  # noqa: BLE001 — wheel/imports unavailable → mine zero
            _log("page comment imports unavailable; mining zero comment pages", error=str(e))
            return []
        dl = self._schema_denylist
        col_rows = schema_signals.filter_denylisted_schemas(
            self._per_catalog(
                allowlist, "columns",
                "table_catalog, table_schema, table_name, column_name, comment",
                "column_comments",
            ),
            denylist=dl,
        )
        tbl_rows = schema_signals.filter_denylisted_schemas(
            self._per_catalog(
                allowlist, "tables",
                "table_catalog, table_schema, table_name, comment",
                "table_comments",
            ),
            denylist=dl,
        )
        out: list[Any] = []
        for r in col_rows:
            comment = str(r.get("comment") or "").strip()
            col = r.get("column_name")
            if not comment or not col:
                continue
            fqn = f"{r.get('table_catalog')}.{r.get('table_schema')}.{r.get('table_name')}.{col}"
            out.append(CommentSignal(fqn=fqn, term=str(col), comment=comment))
        for r in tbl_rows:
            comment = str(r.get("comment") or "").strip()
            tbl = r.get("table_name")
            if not comment or not tbl:
                continue
            fqn = f"{r.get('table_catalog')}.{r.get('table_schema')}.{tbl}"
            out.append(CommentSignal(fqn=fqn, term=str(tbl), comment=comment))
        return out

    def space_instructions(self) -> list[str]:
        """Existing Agent ``text_instructions`` (READ-ONLY, for the contradiction gate).
        Best-effort; absence simply means no contradiction downgrade (never blocks)."""
        return []

    # ── Phase 3d (17g) L6 inputs — READ-ONLY, degrade to empty (MV-D43) ──────
    def suppressions(self, metastore_id: str) -> list[dict[str, Any]]:
        """READ the consent/suppression ledger's suppression rows for this metastore
        so the L6 gate can mark a curator-dismissed proposal ``surfaced=false`` (MV-D26).
        This is the ONLY ledger access in the run and it is a read: the backend (OBO) is
        the sole writer. Any failure degrades to [] (nothing suppressed) rather than
        blocking the run."""
        if not catalog or not metastore_id:
            return []
        ms = metastore_id.replace("'", "''")
        try:
            return _rows(
                f"SELECT proposal_kind, proposal_id FROM {catalog}.{schema}.genie_ont_suppressions "
                f"WHERE metastore_id = '{ms}'"
            )
        except Exception as e:  # noqa: BLE001 — a missing/unreadable ledger never blocks
            _log("suppression ledger read skipped", error=str(e))
            return []

    def usage_signals(self, allowlist: list[str]) -> dict[str, float]:
        """The L2 usage/cost signal for the L6 blend. Not wired in the offline slice
        (query.history/billing demand normalization is a serve-pass concern), so this
        degrades to {} — the usage factor is simply absent, lowering coverage rather
        than faking a zero (the honest-gap discipline)."""
        return {}


# COMMAND ----------

# L3 ER wiring: the in-process similarity backend by default (Lakebase Search stays
# OFF — enabling it is the §12 human gate), GTE embeddings via the shared FMAPI
# client, and the near-tie LLM adjudicator (degrades if the endpoint is down).
# L4 clustering (Phase 3b) uses the same LLM path for cluster NAMING only (degrades
# to anchor-derived names — MV-D43).
from genie_space_optimizer.ontology import cluster, er, pages, similarity  # noqa: E402

# The run_as identity (MV-D50) injected into the LLM enrichers — the wheel never
# resolves identity itself (MV-D65). Needs CAN QUERY on the LLM_MODEL endpoint.
_ont_llm_w = make_workspace_client()

try:
    from genie_space_optimizer.optimization.mv_scoring import FoundationModelEmbeddingClient
    _embedder = FoundationModelEmbeddingClient(make_workspace_client())
except Exception as _e:  # noqa: BLE001 — degrade to string-only ER
    _log("Embedding client unavailable; ER runs string-only", error=str(_e))
    _embedder = None

writer = materialize.SparkSnapshotWriter(spark, catalog, schema)
run = materialize.run_materialize(
    SparkSystemTableReader(
        schema_denylist=schema_denylist,
        join_col_suffixes=join_col_suffixes,
        join_col_max_schemas=join_col_max_schemas,
        join_col_denylist=join_col_denylist,
        warehouse_id=warehouse_id,
        coded_column_max_columns=coded_column_max_columns,
        coded_column_max_cardinality=coded_column_max_cardinality,
    ),
    writer,
    metastore_id=metastore_id,
    workspace_id=workspace_id,
    trigger=trigger,
    allowlist=allowlist,
    run_id=run_id,
    similarity_backend=similarity.get_similarity_backend(None),  # in-process (Lakebase Search off)
    embedder=_embedder,
    adjudicator=er.default_adjudicator(w=_ont_llm_w),
    namer=cluster.default_namer(w=_ont_llm_w),  # LLM cluster naming; degrades to anchor names
    # L5 Page mining (Phase 3c) — deterministic detectors + LLM BODY PROSE only
    # (degrades to a deterministic stub + certify=false, MV-D43). Routing ask_genie
    # confirmation degrades to unvalidated (no concept→Agent map wired here).
    page_drafter=pages.default_page_drafter(w=_ont_llm_w),
    routing_validator=None,
    # Stage 3 curation policy (MV-D57) — from job_parameters, in-code defaults above.
    facet_denylist=facet_denylist,
    domain_min_tables=domain_min_tables,
    domain_min_schemas=domain_min_schemas,
    domain_require_connection=domain_require_connection,
    # Stage 3.2 Gate-B (MV-D62) — the diffuseness presentation net (spec §2.2/§3).
    domain_max_diffuse_schemas=max_diffuse_schemas,
    domain_min_home_concentration=min_home_concentration,
    # Stage 4.1b Page-attachment gate (MV-D64) — hide a Page with no surfaced home.
    page_require_domain=page_require_domain,
    # Stage 4.1d bounded auto-drafting (MV-D66) — the super-sure cap for batch LLM prose.
    page_autodraft_min_corroboration=page_autodraft_min_corroboration,
    page_autodraft_max_pages=page_autodraft_max_pages,
    # Stage 4.1e (MV-D67) — bound the LLM enricher fan-out (renames + super-sure drafts).
    page_autodraft_max_workers=enrich_max_workers,
    rename_max_workers=enrich_max_workers,
    # Stage 4.1f (MV-D68) — cap the last two unbounded LLM paths so the batch LLM budget is
    # a small constant: top-N surfaced Domain renames + top-N near-tie ER adjudications
    # (fanned out); every deferred near-tie degrades to a curator dedupe proposal.
    rename_max_domains=rename_max_domains,
    er_adjudicate_max_pairs=er_adjudicate_max_pairs,
    er_adjudicate_min_score=er_adjudicate_min_score,
    er_adjudicate_max_workers=enrich_max_workers,
)
_log("Materialize complete", metastore_id=metastore_id, state=run["state"], tags=run.get("tag_count"),
     domains=run.get("domain_count"), identities=run.get("identity_count"), pages=run.get("page_count"))
dbutils.notebook.exit(json.dumps({"run_id": run["run_id"], "state": run["state"]}, default=str))
