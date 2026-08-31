"""Delta DDL for the ontology tables (Phase 2).

Mirrors the GSO ``optimization/ddl.py`` pattern: one CREATE-TABLE-IF-NOT-EXISTS
template per table (``{catalog}.{schema}`` substituted), CDF enabled so the tables
can later become Lakebase synced tables. The job calls
:func:`ensure_ontology_tables` at startup.

The materializer writes the snapshot tables (``genie_ont_runs`` /
``genie_ont_tag_graph`` / ``genie_ont_taxonomy_snapshot`` / ``genie_ont_identity``)
and — starting Phase 3b — the Domain/Member PROPOSAL tables (``genie_ont_domains`` /
``genie_ont_members``). The remaining Phase-3 tables (``genie_ont_pages`` /
``_consents`` / ``_suppressions``) are created EMPTY here so later phases never
re-DDL — nothing writes them yet (17f/17g own them).

The ONLY UC writes anywhere in this package are the ``genie_ont_*`` Delta MERGEs
(:func:`build_snapshot_merge_sql`). There is no governed-tag DDL of any kind.
"""

from __future__ import annotations

# ── Written snapshot tables ────────────────────────────────────────────────

_GENIE_ONT_RUNS_DDL = """\
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.genie_ont_runs (
    run_id           STRING    COMMENT 'UUID for this materialization run; PK (one header per run)',
    metastore_id     STRING    COMMENT 'Metastore grain (MV-D49) this run materialized',
    workspace_id     STRING    COMMENT 'provenance — which install/workspace triggered the run; NOT a key',
    trigger          STRING    COMMENT 'nightly | on_demand',
    state            STRING    COMMENT 'running | succeeded | failed',
    scope_allowlist  ARRAY<STRING> COMMENT 'Catalog allowlist the run was scoped to (MV-D42)',
    started_at       TIMESTAMP COMMENT 'When the run started',
    finished_at      TIMESTAMP COMMENT 'When the run reached a terminal state',
    as_of            TIMESTAMP COMMENT 'Logical snapshot time (= started_at)',
    tag_count        INT       COMMENT 'Governed tags snapshotted',
    domain_count     INT       COMMENT 'Domains in the taxonomy tree',
    ungrouped_count  INT       COMMENT 'Assets under no domain tag',
    identity_count   INT       COMMENT 'Canonical entities resolved by L3 ER (Phase 3a)',
    page_count       INT       COMMENT 'Archetype Page proposals mined by L5 (Phase 3c)',
    error            STRING    COMMENT 'Failure detail when state = failed'
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"""

_GENIE_ONT_TAG_GRAPH_DDL = """\
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.genie_ont_tag_graph (
    metastore_id      STRING     COMMENT 'Metastore grain (MV-D49); (metastore_id, tag_key) is the derived PK',
    workspace_id      STRING     COMMENT 'provenance — which install/workspace triggered the run; NOT a key',
    tag_key           STRING     COMMENT 'Governed tag key',
    allowed_values    ARRAY<STRING> COMMENT 'Allowed values (policy), if any',
    assignment_count  INT        COMMENT 'In-scope assignments',
    acts_as_domain    BOOLEAN    COMMENT 'Top-level tag that parents a sub-domain (the / convention)',
    acts_as_subdomain BOOLEAN    COMMENT 'A {{parent}}/{{child}} sub-domain tag',
    dedupe_verdicts   STRING     COMMENT 'JSON: collisions + cleanup flags for this tag',
    run_id            STRING     COMMENT 'FK to genie_ont_runs.run_id',
    as_of             TIMESTAMP  COMMENT 'Materialization time'
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"""

_GENIE_ONT_TAXONOMY_SNAPSHOT_DDL = """\
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.genie_ont_taxonomy_snapshot (
    metastore_id  STRING     COMMENT 'Derived PK (MV-D49) — one serialized tree per metastore',
    workspace_id  STRING     COMMENT 'provenance — which install/workspace triggered the run; NOT a key',
    tree          STRING     COMMENT 'JSON: the OntologyTaxonomy payload (Phase-1 contract)',
    run_id        STRING     COMMENT 'FK to genie_ont_runs.run_id',
    as_of         TIMESTAMP  COMMENT 'Materialization time'
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"""

# ── Empty Phase-3 tables (schema only; NOT written in Phase 2) ──────────────

_GENIE_ONT_DOMAINS_DDL = """\
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.genie_ont_domains (
    metastore_id  STRING     COMMENT 'Metastore grain (MV-D49); (metastore_id, domain_id) is the derived PK',
    domain_id     STRING     COMMENT 'Derived id (sug_<fingerprint>); PK with metastore_id',
    workspace_id  STRING     COMMENT 'provenance — which install/workspace triggered the run; NOT a key',
    parent_id     STRING     COMMENT 'NULL = domain; set = sub-domain (self-ref to domain_id)',
    name          STRING,
    description   STRING,
    tag_decision  STRING     COMMENT 'reuse | create | reassign',
    tag_key       STRING     COMMENT 'Governed tag mapped (reuse) or proposed (create)',
    tag_value     STRING     COMMENT 'Sub-domain value in the Domain/Sub convention',
    evidence      STRING     COMMENT 'JSON: signals behind the proposal',
    score         DOUBLE     COMMENT 'L6 rank',
    run_id        STRING,
    as_of         TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"""

_GENIE_ONT_MEMBERS_DDL = """\
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.genie_ont_members (
    metastore_id  STRING     COMMENT 'Metastore grain (MV-D49); (metastore_id, domain_id, asset_fqn) is the derived PK',
    domain_id     STRING     COMMENT 'FK -> genie_ont_domains.domain_id',
    asset_fqn     STRING     COMMENT '(metastore_id, domain_id, asset_fqn) is the derived PK',
    asset_type    STRING     COMMENT 'table | metric_view | dashboard | genie_agent',
    workspace_id  STRING     COMMENT 'provenance — which install/workspace triggered the run; NOT a key',
    run_id        STRING,
    as_of         TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"""

_GENIE_ONT_PAGES_DDL = """\
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.genie_ont_pages (
    metastore_id  STRING     COMMENT 'Metastore grain (MV-D49); (metastore_id, page_id) is the derived PK',
    page_id       STRING     COMMENT 'Derived id; PK with metastore_id',
    workspace_id  STRING     COMMENT 'provenance — which install/workspace triggered the run; NOT a key',
    domain_id     STRING     COMMENT 'The sub-domain this Page elevates a concept for',
    archetype     STRING     COMMENT 'Routing | Disambiguation | Guardrail | Taxonomy',
    title         STRING,
    body          STRING     COMMENT 'The draft Page body (curator-facing)',
    synonyms      ARRAY<STRING>,
    related_fqns  ARRAY<STRING> COMMENT 'Discover Related assets',
    source_fqns   ARRAY<STRING> COMMENT 'Discover Sources (incl. the originating Genie Agent)',
    certify       BOOLEAN,
    evidence      STRING     COMMENT 'JSON',
    score         DOUBLE,
    run_id        STRING,
    as_of         TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"""

# The consent/suppression ledger makes Phase-3 re-runs idempotent — a resolved
# proposal is never re-surfaced (MV-D26). Empty until Phase 3.
_GENIE_ONT_CONSENTS_DDL = """\
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.genie_ont_consents (
    metastore_id  STRING     COMMENT 'Metastore grain (MV-D49); (metastore_id, proposal_kind, proposal_id) is the PK',
    workspace_id  STRING     COMMENT 'provenance — which install/workspace triggered the run; NOT a key',
    proposal_kind STRING     COMMENT 'domain | member | page',
    proposal_id   STRING     COMMENT '(metastore_id, proposal_kind, proposal_id) is the PK',
    state         STRING     COMMENT 'applied',
    decided_by    STRING     COMMENT 'Consenting user email (OBO)',
    decided_at    TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"""

_GENIE_ONT_SUPPRESSIONS_DDL = """\
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.genie_ont_suppressions (
    metastore_id  STRING     COMMENT 'Metastore grain (MV-D49); (metastore_id, proposal_kind, proposal_id) is the PK',
    workspace_id  STRING     COMMENT 'provenance — which install/workspace triggered the run; NOT a key',
    proposal_kind STRING     COMMENT 'domain | member | page',
    proposal_id   STRING     COMMENT '(metastore_id, proposal_kind, proposal_id) is the PK',
    reason        STRING     COMMENT 'Optional dismissal note',
    dismissed_by  STRING,
    dismissed_at  TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"""

# Identity map (canonical entity -> members) — NEW in 17d (Phase 3a). One row per
# (metastore_id, canonical_id, member_ref): the ER output that L4 clustering (17e)
# forms communities over. Written idempotently by the materializer.
_GENIE_ONT_IDENTITY_DDL = """\
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.genie_ont_identity (
    metastore_id  STRING     COMMENT 'Metastore grain (MV-D49); (metastore_id, canonical_id, member_ref) is the derived PK',
    workspace_id  STRING     COMMENT 'provenance — which install/workspace triggered the run; NOT a key',
    canonical_id  STRING     COMMENT 'Derived id (dedupe_<fingerprint of sorted members>)',
    member_ref    STRING     COMMENT '(metastore_id, canonical_id, member_ref) is the derived PK',
    member_kind   STRING     COMMENT 'tag | measure | metric_view | agent | page_name',
    verdict       STRING     COMMENT 'merge | reject | escalate | distinct',
    method        STRING     COMMENT 'exact | string | embedding | llm',
    score         DOUBLE,
    reason        STRING     COMMENT 'LLM reason for near-tie adjudications only',
    run_id        STRING,
    as_of         TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"""

# Tables the batch job WRITES (snapshots + run ledger + identity map).
TABLE_ONT_RUNS = "genie_ont_runs"
TABLE_ONT_TAG_GRAPH = "genie_ont_tag_graph"
TABLE_ONT_TAXONOMY_SNAPSHOT = "genie_ont_taxonomy_snapshot"
TABLE_ONT_IDENTITY = "genie_ont_identity"

SNAPSHOT_TABLES: tuple[str, ...] = (
    TABLE_ONT_RUNS,
    TABLE_ONT_TAG_GRAPH,
    TABLE_ONT_TAXONOMY_SNAPSHOT,
    TABLE_ONT_IDENTITY,
)

# Proposal tables — WRITTEN starting Phase 3b (17e): the clustering engine MERGEs
# Domain/Sub-Domain rows + their asset membership here.
TABLE_ONT_DOMAINS = "genie_ont_domains"
TABLE_ONT_MEMBERS = "genie_ont_members"

PROPOSAL_TABLES: tuple[str, ...] = (
    TABLE_ONT_DOMAINS,
    TABLE_ONT_MEMBERS,
)

# Page proposal table — WRITTEN starting Phase 3c (17f): the L5 miners MERGE
# canonical-concept-anchored, corroboration-gated archetype Page proposals here
# (metastore-scoped, MV-D49). It left PHASE3_TABLES when 17f began populating it.
TABLE_ONT_PAGES = "genie_ont_pages"

PAGE_TABLES: tuple[str, ...] = (
    TABLE_ONT_PAGES,
)

# Tables still created EMPTY and NEVER written in this phase (consent/suppression
# ledger = 17g). The firewall test asserts these stay empty.
PHASE3_TABLES: tuple[str, ...] = (
    "genie_ont_consents",
    "genie_ont_suppressions",
)

_ONT_ALL_DDL: dict[str, str] = {
    TABLE_ONT_RUNS: _GENIE_ONT_RUNS_DDL,
    TABLE_ONT_TAG_GRAPH: _GENIE_ONT_TAG_GRAPH_DDL,
    TABLE_ONT_TAXONOMY_SNAPSHOT: _GENIE_ONT_TAXONOMY_SNAPSHOT_DDL,
    TABLE_ONT_IDENTITY: _GENIE_ONT_IDENTITY_DDL,
    "genie_ont_domains": _GENIE_ONT_DOMAINS_DDL,
    "genie_ont_members": _GENIE_ONT_MEMBERS_DDL,
    "genie_ont_pages": _GENIE_ONT_PAGES_DDL,
    "genie_ont_consents": _GENIE_ONT_CONSENTS_DDL,
    "genie_ont_suppressions": _GENIE_ONT_SUPPRESSIONS_DDL,
}


def all_ddl(catalog: str, schema: str) -> dict[str, str]:
    """Return {table_name: rendered CREATE TABLE} for every ontology table."""
    return {name: ddl.format(catalog=catalog, schema=schema) for name, ddl in _ONT_ALL_DDL.items()}


def ensure_ontology_tables(spark, catalog: str, schema: str) -> None:
    """Idempotently create all ontology tables (snapshots + empty Phase-3)."""
    for stmt in all_ddl(catalog, schema).values():
        spark.sql(stmt)


def build_snapshot_merge_sql(
    *,
    catalog: str,
    schema: str,
    table: str,
    source_view: str,
    key_cols: list[str],
    update_cols: list[str],
    metastore_id: str,
    delete_unmatched: bool = True,
) -> str:
    """Build the idempotent MERGE for a snapshot table (§7.2, re-grained MV-D49).

    Update matched, insert new, and (when ``delete_unmatched``) delete rows of THIS
    metastore that are no longer in the source (``WHEN NOT MATCHED BY SOURCE`` scoped
    to ``metastore_id`` so a tag that disappeared is removed without touching other
    metastores). A re-run therefore yields the same rows, never duplicates; a row
    written by a different metastore is never deleted by this run. ``workspace_id``
    rides along as provenance and is never part of the key or the delete predicate.

    ``delete_unmatched=False`` makes the MERGE **upsert-only** (no source-diff delete).
    The run ledger (``genie_ont_runs``) needs this: its key is ``run_id``, which is
    unique per run, so a source-diff delete would wipe every *prior* run's header —
    the opposite of a ledger. Snapshot/proposal tables keep the default (True) because
    their keys are stable across runs, so the delete correctly prunes stale entities.
    """
    target = f"{catalog}.{schema}.{table}"
    on = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
    insert_cols = key_cols + [c for c in update_cols if c not in key_cols]
    insert_names = ", ".join(insert_cols)
    insert_vals = ", ".join(f"s.{c}" for c in insert_cols)
    ms = metastore_id.replace("'", "''")
    clauses = [
        f"MERGE INTO {target} AS t",
        f"USING {source_view} AS s",
        f"ON {on}",
    ]
    if update_cols:
        set_clause = ", ".join(f"t.{c} = s.{c}" for c in update_cols)
        clauses.append(f"WHEN MATCHED THEN UPDATE SET {set_clause}")
    clauses.append(f"WHEN NOT MATCHED THEN INSERT ({insert_names}) VALUES ({insert_vals})")
    if delete_unmatched:
        clauses.append(f"WHEN NOT MATCHED BY SOURCE AND t.metastore_id = '{ms}' THEN DELETE")
    return "\n".join(clauses)
