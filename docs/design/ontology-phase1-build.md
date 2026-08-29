# Ontology — Phase 1: the read-only spine (build spec)

**Status:** build-ready · **Owner directive:** MV-D36 / MV-D37 / MV-D38, decisions
closed by MV-D39–D47 (see `mv-advisor-playbook.md`). **Design source of truth:**
`ontology-engine-architecture.md` (this doc is a *buildable slice* of it, not a new
design). **UI target:** mockup frames `17.0a` (permission banner), `17.0b`
(taxonomy), `17.0c` (Governed-Tags / dedupe lens) in
`frontend/src/components/auto-optimize/mockups/OntologyPageMockups.tsx`.

This is the first Goal-Mode-runnable deliverable. It is deliberately small: **a
thin, read-only page that renders the estate's existing ontology and the
governed-tag substrate — no proposals engine, no embeddings, no clustering, no
external context, no writes.** Everything harder is a later phase and is called out
in §12 so the agent does not silently pull it forward.

---

## 1. Scope

### In (Phase 1)

- **L0 preflight** → serve `17.0a`: resolve the permission tiers actually
  exercised in Phase 1 and drive the tiered banner (degrade-not-hang, MV-D43).
- **L1 inventory (OBO)** → the cheap `information_schema` counts that render
  in-request on first load (MV-D43 fast-path).
- **Governed-tag graph (SP)** → enumerate `system.tags.governed_tags` +
  assignments (MV-D37), the substrate for both `17.0b` and `17.0c`.
- **Deterministic taxonomy** → serve `17.0b`: the Domain → Sub-Domain → Page tree
  **as it already exists in governed tags** (the `Domain` / `Domain/Sub` `/`
  convention), member assets + counts, plus an **ungrouped** bucket (metric views
  / Agents under no domain tag) as a coverage signal — **read straight from tags +
  assignments, no clustering, no LLM.**
- **Deterministic tags lens** → serve `17.0c`: existing governed tags, reuse-vs-
  create **collisions by exact + fuzzy match** (case / plural / token — **no
  embeddings**, MV-D40 is a later phase), and cleanup flags (orphan / near-empty /
  deprecated-but-assigned).
- **Settings** → company name + **catalog allowlist** (MV-D42), durable.
- **Top-level admin-gated nav entry** `Ontology` in `App.tsx`, lazy-loaded.
- **Grants** → extend `scripts/grant_permissions.py` (the grant source of truth)
  with the governed-tag read.

### Out (deferred — see §12)

Proposal engine (L3 ER / L4 clustering / L5 Page miners / L6 rank), the batch job
+ Lakebase mirror materialization, Lakebase Search similarity (MV-D40, Phase 3),
external enrichment / Context Pack via AI Gateway MCP context sources (MV-D38 / MV-D44 / MV-D46 / MV-D47), the `17.0d/e`
**drafts**, and the
**`SET TAG` apply** (L9). Phase 1 renders **what exists**; it proposes **nothing
new** and writes **nothing**.

---

## 2. Decisions honored (and which sleep in Phase 1)

| Decision | Phase-1 posture |
|---|---|
| MV-D36 standalone admin-gated estate page | **Active** — new top-level nav, admin-gated. |
| MV-D37 governed-tag substrate + Tags lens | **Active (read-only)** — enumerate/dedupe; **no** `CREATE/SET TAG`. |
| MV-D38 external enrichment | **Dormant** — absent entirely (not even toggled). |
| MV-D39 in-job `igraph` clustering | **Dormant** — taxonomy is tag-derived, no Louvain. |
| MV-D40 Lakebase Search similarity | **Dormant** — dedupe is exact + fuzzy in-process only; no embeddings, Lakebase Search not enabled (Phase 3). |
| MV-D41 nightly batch | **Dormant** — Phase 1 is live-read + TTL cache (GenieWatch model). |
| MV-D42 catalog allowlist | **Active** — scopes every reader. |
| MV-D43 OBO inventory fast-path | **Active** — inventory renders in-request; heavier reads are cached/async. |
| MV-D44 enrichment OFF by default | **Active (trivially)** — no web-search/enrichment path exists yet. |
| MV-D45 minimal install footprint | **Active** — Phase 1 adds **zero** new services; its only new provisioning is one SP `SELECT system.tags.governed_tags` grant. |
| MV-D46 governed web-search MCP | **Dormant** — no web-search path yet (arrives with enrichment, Phase 4). |
| MV-D47 AI Gateway MCP context-source registry | **Dormant** — no external context sources wired; the tier-5 Context Sources panel is a mockup only until Phase 4. |

**Load-bearing consequence:** because MV-D41's batch is dormant, Phase 1 does **not**
read a Lakebase mirror. It reads system tables **live as the SP** with an in-process
TTL cache — the exact GenieWatch pattern (`backend/watch/services/system_tables.py`).
When the batch lands (Phase 2), the same routers switch their reader to the mirror;
the contracts in §4 do not change.

---

## 3. Subsystem layout (mirror GenieWatch)

Ontology is a self-contained subsystem like `backend/watch/`, mounted under
`/api/ontology/*` and registered separately in `main.py`.

```
backend/ontology/
  __init__.py
  models.py                 # Pydantic contracts (§4)
  routers/
    __init__.py
    preflight.py            # GET /api/ontology/preflight, /health
    inventory.py            # GET /api/ontology/inventory   (OBO fast-path)
    taxonomy.py             # GET /api/ontology/taxonomy
    tags.py                 # GET /api/ontology/tags
    settings.py             # GET/PUT /api/ontology/settings
  services/
    __init__.py
    inventory.py            # OBO information_schema counts (MV-D43)
    tag_graph.py            # SP governed-tag + assignment reads (MV-D37)
    taxonomy.py             # deterministic tree from tag_graph + lineage adjacency
    dedupe.py               # exact + fuzzy collision + cleanup flags (no embeddings)
    ont_settings.py         # company_name + catalog_allowlist persistence
frontend/src/ontology/       # own api.ts + types.ts (namespaced, like watch/)
```

Reuse, do not fork: `backend/services/auth.py`
(`get_workspace_client` OBO / `get_service_principal_client` SP),
`backend/services/lakebase.py` (`_ensure_schema`, `is_available`, in-memory
fallback), and the TTL-cache + permission-error-detection shape from
`backend/watch/services/system_tables.py`.

---

## 4. Backend contracts (`backend/ontology/models.py`)

Pydantic v2, mirroring the repo convention (models are shared, TS mirrors in §5).
Every payload carries an `as_of` ISO-8601 stamp (the `Provenanced<T>` discipline,
§6 of the architecture doc, reduced to a timestamp for Phase 1).

```python
from __future__ import annotations
from typing import Literal
from pydantic import BaseModel, Field

TierStatus = Literal["ok", "degraded", "blocked", "not_exercised"]
TierId = Literal["inventory", "signals", "tag_graph", "membership_write", "external_enrichment"]

class PermissionTier(BaseModel):
    id: TierId
    label: str
    identity: Literal["obo", "sp", "batch"]
    status: TierStatus
    grants: list[str] = Field(default_factory=list)   # copy-ready grant/entitlement lines
    reason: str | None = None                         # why blocked/degraded, plain language

class OntologyPreflight(BaseModel):
    tiers: list[PermissionTier]                       # 5 rows; Phase 1 exercises 1–3
    can_render_taxonomy: bool                          # tier "tag_graph" == ok
    company_name: str | None = None
    catalog_allowlist: list[str] = Field(default_factory=list)
    as_of: str

class OntologyInventory(BaseModel):
    catalogs_scanned: list[str]
    metric_view_count: int
    genie_agent_count: int
    governed_tag_count: int
    as_of: str

class MemberAsset(BaseModel):
    fqn: str
    asset_type: Literal["table", "metric_view", "dashboard", "genie_agent"]

class SubDomainNode(BaseModel):
    tag_value: str                                    # e.g. "Tax" in Finance/Tax
    name: str
    member_count: int
    members: list[MemberAsset] = Field(default_factory=list)

class DomainNode(BaseModel):
    tag_key: str                                      # the governed tag acting as domain
    name: str
    member_count: int
    subdomains: list[SubDomainNode] = Field(default_factory=list)
    members: list[MemberAsset] = Field(default_factory=list)   # direct, un-sub-domained

class UngroupedBucket(BaseModel):
    metric_views: list[MemberAsset] = Field(default_factory=list)
    genie_agents: list[MemberAsset] = Field(default_factory=list)

class OntologyTaxonomy(BaseModel):
    domains: list[DomainNode]
    ungrouped: UngroupedBucket
    as_of: str

class GovernedTag(BaseModel):
    tag_key: str
    allowed_values: list[str] = Field(default_factory=list)
    assignment_count: int
    acts_as_domain: bool
    acts_as_subdomain: bool

CollisionKind = Literal["exact", "fuzzy_case", "fuzzy_plural", "fuzzy_token"]
CleanupFlag = Literal["orphan", "near_empty", "deprecated_but_assigned"]

class TagCollision(BaseModel):
    kind: CollisionKind
    members: list[str]                                # tag keys that collide
    suggestion: str                                   # "reuse `finance` instead of creating `Finance`"

class TagCleanup(BaseModel):
    tag_key: str
    flag: CleanupFlag
    detail: str

class TagLens(BaseModel):
    tags: list[GovernedTag]
    collisions: list[TagCollision]
    cleanup: list[TagCleanup]
    as_of: str

class OntologySettings(BaseModel):
    company_name: str | None = None
    catalog_allowlist: list[str] = Field(default_factory=list)
```

---

## 5. TypeScript mirrors (`frontend/src/ontology/types.ts`)

Keep 1:1 with §4 (the repo's Pydantic ↔ TS mirror rule). Sketch:

```typescript
export type TierStatus = "ok" | "degraded" | "blocked" | "not_exercised";
export type TierId = "inventory" | "signals" | "tag_graph" | "membership_write" | "external_enrichment";

export interface PermissionTier {
  id: TierId;
  label: string;
  identity: "obo" | "sp" | "batch";
  status: TierStatus;
  grants: string[];
  reason?: string | null;
}
export interface OntologyPreflight {
  tiers: PermissionTier[];
  can_render_taxonomy: boolean;
  company_name?: string | null;
  catalog_allowlist: string[];
  as_of: string;
}
// OntologyInventory, MemberAsset, SubDomainNode, DomainNode, UngroupedBucket,
// OntologyTaxonomy, GovernedTag, TagCollision, TagCleanup, TagLens,
// OntologySettings — mirror §4 field-for-field.
```

`frontend/src/ontology/api.ts` mirrors `frontend/src/watch/api.ts` (base
`/api/ontology`), one typed fetch per route.

---

## 6. Routes (all read-only except settings PUT)

| Method + path | Returns | Identity | Notes |
|---|---|---|---|
| `GET /api/ontology/preflight` | `OntologyPreflight` | OBO + SP probes | Resolves tiers; cheap; drives `17.0a`. |
| `GET /api/ontology/inventory` | `OntologyInventory` | **OBO** | In-request fast-path (MV-D43); `information_schema` counts. |
| `GET /api/ontology/taxonomy` | `OntologyTaxonomy` | **SP** | Tag-derived tree; TTL-cached; drives `17.0b`. |
| `GET /api/ontology/tags` | `TagLens` | **SP** | Enumerate + dedupe + cleanup; drives `17.0c`. |
| `GET /api/ontology/settings` | `OntologySettings` | OBO | Read company name + allowlist. |
| `PUT /api/ontology/settings` | `OntologySettings` | OBO (admin) | The **only** Phase-1 write, and it writes **our** config, never UC. |
| `GET /api/ontology/health` | health dict | — | Mirror `backend/watch/routers/settings.py::health`. |

No `/drafts`, no `/apply` in Phase 1 (those are `17.0d/e` + L9, deferred).

**Preflight tier resolution (Phase 1):**

- `inventory` (OBO) → always `ok` (auto-filtered, no grant).
- `signals` (SP: `system.access.*`, `billing.usage`, `query.history`) → `ok` if the
  GenieWatch SP grants are present, else `degraded` (ranking weaker; page still
  renders). Reuse `system_tables.system_tables_status()`.
- `tag_graph` (SP: `SELECT system.tags.governed_tags`) → `ok` / `blocked`. If
  `blocked`, `can_render_taxonomy=false` and `17.0b`/`17.0c` show the banner's
  grant CTA instead of an error.
- `membership_write` → always `not_exercised` in Phase 1 (informational row).
- `external_enrichment` → always `not_exercised` in Phase 1 (informational row).

---

## 7. Persistence / DDL (Phase-1 minimum)

Phase 1 stands up **one** durable table — settings — via the app's existing
`_ensure_schema()` at startup (`backend/services/lakebase.py`), with the in-memory
fallback when `LAKEBASE_HOST` is unset. The `genie_ont_*` proposal/mirror/audit
tables from architecture §7 are **NOT** created in Phase 1 (nothing writes them
yet).

```sql
-- genie schema (same schema the app already owns)
CREATE TABLE IF NOT EXISTS genie_ont_settings (
  workspace_id     TEXT PRIMARY KEY,   -- one row per workspace/app instance
  company_name     TEXT,
  catalog_allowlist JSONB NOT NULL DEFAULT '[]',
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Governed-tag reads (`17.0c`) are served **live + TTL-cached** (GenieWatch model),
not persisted, in Phase 1. `genie_ont_tag_graph` as a materialized snapshot is a
Phase-2 optimization; the `tag_graph` service is written so its output can later be
read from that table without changing the route contract.

---

## 8. Reader implementation notes

**Inventory (OBO, MV-D43 fast-path)** — `services/inventory.py`. Count metric
views + governed tags within the allowlist via `system.information_schema`, run as
the user (`get_workspace_client`), so results are auto-filtered to what they may
see, no grant required. Agent count reuses the existing space list
(`genie_client.list_genie_spaces` / GenieWatch's list). Sketch (verify the
metric-view `table_type` filter against a live workspace before relying on it):

```sql
-- metric views in scope (OBO; auto-filtered)
SELECT count(*) FROM system.information_schema.tables
WHERE table_catalog IN (:allowlist) AND table_type = 'METRIC_VIEW';
```

**Governed-tag graph (SP, MV-D37)** — `services/tag_graph.py`. Run as the SP
(`get_service_principal_client`), TTL-cache like `system_tables.py`. Enumerate tags
+ allowed values (`system.tags.governed_tags`) and join assignments from
`system.information_schema.{catalog,schema,table,column}_tags` on
`tag_name = tag_key` filtered to `is_governed`. A tag `acts_as_domain` when it (or
a `parent/child` `/`-convention pair) is assigned as a Discover domain; treat the
`/` split as the domain→sub-domain boundary.

**Taxonomy (deterministic)** — `services/taxonomy.py`. Pure function of the
tag-graph output: group assignments by domain tag → sub-domain (`/` split) →
member assets; anything in the allowlist not under a domain tag falls into
`ungrouped`. Lineage adjacency (`system.access.table_lineage`, SP) is used **only**
to order/annotate the ungrouped bucket, never to invent a new domain (that is L4,
deferred).

**Dedupe (deterministic, no embeddings)** — `services/dedupe.py`. Collisions from
normalized-key equality after case-fold, singular/plural fold, and token-set
compare; cleanup flags from assignment counts (`0` → orphan, below a small floor →
near-empty) and a `deprecated` allowed-value/policy marker still assigned. MV-D40
Lakebase Search similarity (embeddings + BM25) is explicitly **out** of this phase.

---

## 9. Frontend wiring

- **Nav (`frontend/src/App.tsx`)** — add an `Ontology` top-level view alongside the
  existing five (SpaceList | SpaceDetail | AdminDashboard | CreateSpace |
  HowItWorks), admin-gated (reuse whatever gate `AdminDashboard` uses). Lazy-load
  the page, like the GenieWatch sub-tabs.
- **Page** — render `17.0a` (from `/preflight`), `17.0b` (from `/taxonomy`), `17.0c`
  (from `/tags`). The mockup components in `OntologyPageMockups.tsx` are the visual
  contract; wire them to live data, keeping the **zero-burden** copy already in the
  mockups. No `17.0d/e` drafts and no Apply button in Phase 1.
- **Settings** — a small company-name + catalog-allowlist form backed by
  `/settings`.

---

## 10. Grants

Add the governed-tag read to `scripts/grant_permissions.py` (the source of truth,
per AGENTS.md): the SP needs `USE CATALOG system`, `USE SCHEMA system.tags`, and
`SELECT ON system.tags.governed_tags` (in addition to the existing GenieWatch
`system.{query,billing,access}` grants). The banner's `tag_graph` tier surfaces the
exact copy-ready grant line when it resolves `blocked`.

---

## 11. Tests (offline, `backend/tests/`, run via `./scripts/test.sh`)

Follow the repo's offline-unit discipline — no live workspace, fixtures for the SP
reads.

- **Contracts** — every model in §4 round-trips `model_dump(mode="json")`; enums
  reject unknown values.
- **Preflight** — with SP grants present → `signals`/`tag_graph` `ok` and
  `can_render_taxonomy=true`; with a permission error injected on the tag read →
  `tag_graph` `blocked`, `can_render_taxonomy=false`, page-does-not-raise.
- **Taxonomy** — a fixture tag-graph with `Finance`, `Finance/Tax`, and an untagged
  metric view produces one `DomainNode` with one `SubDomainNode` and one entry in
  `ungrouped.metric_views`.
- **Dedupe** — `{"finance","Finance","finances"}` collapses to one collision group
  with a reuse suggestion; a zero-assignment tag flags `orphan`.
- **Scope** — an empty allowlist yields empty inventory/taxonomy and a preflight
  hint to choose catalogs (MV-D42), never a scan of everything.
- **Read-only guarantee** — a test asserts no route module imports a `SET TAG` /
  `CREATE GOVERNED TAG` / `manage_uc_tags` write path (Phase-1 firewall).
- **Frontend** — extend `mockups.test.tsx` only if copy changes; add a light api.ts
  shape test if the harness supports it.

---

## 12. Definition of done & explicit deferrals

**Done when:** the admin opens `Ontology`, the banner (`17.0a`) shows tiers 1–3
resolved (4–5 informational), the taxonomy (`17.0b`) renders the estate's existing
domains + ungrouped coverage from governed tags, and the tags lens (`17.0c`) lists
tags with reuse-vs-create collisions and cleanup flags — all read-only, scoped by
the catalog allowlist, with `./scripts/test.sh` green.

**Explicitly deferred (do NOT pull forward):**

- **Phase 2** — the batch job + `genie_ont_*` materialization + Lakebase mirror
  (MV-D41); routers switch reader from live-SP to mirror, contracts unchanged.
- **Phase 3** — the proposal engine: L3 ER + **MV-D40 Lakebase Search dedupe**
  (`lakebase_vector` + `lakebase_text` on the existing Lakebase; **enables Lakebase
  Search** — beta, irreversible; degrades to in-process cosine), L4 `igraph`
  clustering (MV-D39), L5 Page miners, L6 rank/trust; unlocks the `17.0d/e`
  **drafts** of *new* domains/pages.
- **Phase 4** — external context / Context Pack (MV-D38 / MV-D44) via the **MV-D47
  AI Gateway MCP context-source registry** (MV-D46 governed `system.ai.web_search`
  + You.com / Model-Serving fallback ladder; Confluence / Drive / M365 + internal
  Genie / SQL MCPs), `EXECUTE`-granted + service-policy governed, as an opt-in tier.
- **Phase 5** — the consented **`SET TAG` apply** (L9), the single write, dry-run →
  preview → consent → audit (`genie_ont_applied`).
