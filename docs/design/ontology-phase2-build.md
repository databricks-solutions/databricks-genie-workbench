# Ontology — Phase 2: batch materialization + Lakebase mirror (build spec)

**Status:** build-ready (offline slice) · **Owner directive:** MV-D41 (nightly +
on-demand), inheriting MV-D39 / MV-D42 / MV-D43 / MV-D45 (see
`mv-advisor-playbook.md`, Prompt 17c). **Design source of truth:**
`ontology-engine-architecture.md` §2 (thin page reads the mirror, job materializes
it), §7 (data model), and the **L7 persistence** + **L8 serving** building-block
subsections. This doc is a *buildable slice* of that design, not a new design.
**Builds on:** `ontology-phase1-build.md` (the read-only spine, already shipped on
the `ontology` branch).

This is the **second** Goal-Mode deliverable. It is deliberately narrow: **stand up
the batch path and the Lakebase mirror for the two snapshots Phase 1 already
computes live — the governed-tag graph and the taxonomy tree — and swap the routers
to read the mirror when it is fresh, falling back to the Phase-1 live path when it
is not.** It adds **no new proposals, no embeddings, no clustering, no external
context, and no UC writes.** Everything harder is a later phase and is called out in
§12 so the agent does not silently pull it forward.

> **The one-line contract:** Phase 1 = *see* the estate's existing ontology by
> reading system tables live. Phase 2 = *materialize* that same view nightly (and
> on demand) to Delta + a Lakebase synced-table mirror, so the page reads a stable,
> sub-second snapshot — **without changing any Phase-1 route response contract.**

---

## 1. Scope

### In (Phase 2)

- **L7 persistence (batch-written)** → a new GSO-style job task materializes the
  Phase-1 outputs to **Delta** (system-of-record) in the GSO catalog/schema:
  `genie_ont_tag_graph` (governed tags + assignment counts + dedupe verdicts) and
  `genie_ont_taxonomy_snapshot` (the serialized Domain → Sub-Domain → member tree),
  plus a `genie_ont_runs` ledger row per run. **Idempotent** — a re-run produces the
  same rows (derived keys + `MERGE`), so a manual run between nightlies is safe.
- **Lakebase mirror (synced tables)** → the three Delta tables are registered as
  **Databricks synced tables** (the exact GSO pattern) and read back through a
  `gso_lakebase.py`-style reader for fast page loads.
- **L2 signal-graph scaffold** → the job builds the fused signal graph **structure
  only** (nodes/edges from tags + lineage adjacency) as a dependency for Phase 3;
  **no clustering, no Louvain** (MV-D39 is a dependency here, not an algorithm).
- **Reader swap** → `taxonomy.py` / `tags.py` read the mirror when it exists and is
  fresh; otherwise they fall back to the Phase-1 live-SP + TTL path (MV-D43). The
  route **response models do not change** (§4).
- **Refresh model (MV-D41)** → a **nightly schedule** (DABs) + an **on-demand**
  `POST /api/ontology/refresh` trigger that runs the job now (reuse the GSO
  `run_now` launcher). A new `GET /api/ontology/refresh` returns freshness + last-run
  state for the "Refresh ontology" button and the freshness chip.
- **Empty Phase-3 tables** → `genie_ont_domains`, `genie_ont_members`,
  `genie_ont_pages`, `genie_ont_consents`, `genie_ont_suppressions` are **created
  empty** (schema only, in the same Delta DDL module) so Phase 3 has them; Phase 2
  writes **nothing** to them.

### Out (deferred — see §12)

Proposal engine (L3 ER / L4 clustering / L5 Page miners / L6 rank), MV-D40 Lakebase
Search similarity (Phase 3), external enrichment / Context Pack (MV-D38 / MV-D44 /
MV-D46 / MV-D47, Phase 4), the `17.0d/e` **drafts**, and the **`SET TAG` apply**
(L9, Phase 5). Phase 2 materializes **what exists**; it proposes **nothing new** and
writes **nothing to UC**.

---

## 2. Decisions honored (and which sleep in Phase 2)

| Decision | Phase-2 posture |
|---|---|
| MV-D36 standalone admin-gated estate page | **Active** — unchanged from Phase 1. |
| MV-D37 governed-tag substrate + Tags lens | **Active (read-only)** — now **snapshotted** to Delta + mirror; still **no** `CREATE/SET TAG`. |
| MV-D38 external enrichment | **Dormant** — absent entirely. |
| MV-D39 in-job `igraph` clustering | **Scaffolded** — `igraph` becomes a job dependency and L2 builds the fused graph; **no Louvain / no communities** (that is 17e). |
| MV-D40 Lakebase Search similarity | **Dormant** — dedupe verdicts are still exact + fuzzy in-process; **Lakebase Search is NOT enabled** (Phase 3, irreversible). |
| MV-D41 nightly batch + on-demand | **Active — this is the phase.** Nightly schedule + `POST /refresh`; both write the same tables through the same idempotent path. |
| MV-D42 catalog allowlist | **Active** — the allowlist scopes the run (the job reads it from `genie_ont_settings`). |
| MV-D43 OBO inventory fast-path / degrade-not-hang | **Active** — the live path is now the **fallback** when the mirror is cold or stale; the page never blocks on the job. |
| MV-D44 enrichment OFF by default | **Active (trivially)** — no enrichment path exists yet. |
| MV-D45 minimal install footprint | **Active** — reuses the **existing GSO job packaging + wheel + synced-table + Lakebase** machinery; adds **one scheduled job task**, **zero new managed services**. |
| MV-D46 governed web-search MCP | **Dormant** — no web-search path yet. |
| MV-D47 AI Gateway MCP context-source registry | **Dormant** — no external context sources wired. |

**Load-bearing consequence:** the routers now **prefer the mirror**. When the mirror
is present and fresh, `taxonomy`/`tags` read Lakebase (sub-second) and every payload's
`as_of` reflects the **materialization time**. When the mirror is cold (before the
first run) or stale (beyond the freshness window), the routers **degrade to the
Phase-1 live-SP + TTL path** and `as_of` reflects the live read. The §4 response
schemas are **byte-identical** to Phase 1; only the *source* of the data (and the
meaning of `as_of`) widens.

---

## 3. Subsystem layout (extends Phase 1 + the GSO wheel)

Two homes: the **batch/materializer** lives in the **GSO wheel** (it runs on a job
cluster and must not import `backend.*`); the **reader swap + trigger** live in the
existing `backend/ontology/` subsystem.

```
packages/genie-space-optimizer/src/genie_space_optimizer/
  ontology/                       # NEW — batch-side, wheel-importable by backend
    __init__.py
    transforms.py                 # PURE transforms extracted from Phase-1 services
                                  #   (raw tag rows → GovernedTag/collisions; tag_graph → tree)
    materialize.py                # SP reads → transforms → build Delta rows (idempotent)
    graph.py                      # L2 fused signal-graph scaffold (nodes/edges only)
    ddl.py                        # Delta DDL for ALL genie_ont_* (snapshots + empty Phase-3)
  jobs/
    run_ontology_materialize.py   # NEW job task (notebook-source, like run_intake_and_snapshot.py)

backend/ontology/
  services/
    mirror.py                     # NEW — gso_lakebase-style reads of the synced tag_graph/taxonomy
    refresh.py                    # NEW — run_now trigger + freshness/run-status (reuse job_launcher)
    tag_graph.py                  # MODIFIED — mirror-first, Phase-1 live path as fallback
    taxonomy.py                   # MODIFIED — mirror-first, Phase-1 live path as fallback
  routers/
    refresh.py                    # NEW — GET/POST /api/ontology/refresh
    preflight.py                  # (unchanged contract; may add a batch tier status read)
  models.py                       # +OntologyRefreshStatus only (existing models unchanged)
```

**Reuse, do not fork:**

- `genie_space_optimizer/optimization/ddl.py` (`_ALL_DDL`) — the Delta-DDL pattern
  the new `ontology/ddl.py` mirrors, and the module `scripts/grant_permissions.py`
  already imports.
- `genie_space_optimizer/jobs/run_intake_and_snapshot.py` — the job-task shape
  (notebook-source header, `make_workspace_client`, job-params in, Delta out).
- `genie_space_optimizer/integration/trigger.py` + `backend/services/… job_launcher`
  — the `jobs.run_now` trigger the `POST /refresh` route reuses (the `GSO_JOB_ID`
  precedent → new `GSO_ONT_JOB_ID`).
- `backend/services/gso_lakebase.py` — synced-table reads from Lakebase; `mirror.py`
  mirrors this shape for the ontology tables.
- `scripts/setup_synced_tables.py` + `scripts/deploy_lib/` — register the new
  `genie_ont_*` snapshot tables as synced tables.
- **The Phase-1 transform logic** in `backend/ontology/services/{tag_graph,taxonomy,
  dedupe}.py` — its **pure** parts move to `genie_space_optimizer/ontology/transforms.py`
  and the Phase-1 services import them back (a contract-preserving refactor; the §4
  route models do not change; the Phase-1 tests still pass unchanged).

---

## 4. Backend contracts (`backend/ontology/models.py`)

**Existing Phase-1 models are FROZEN** — `OntologyPreflight`, `OntologyInventory`,
`OntologyTaxonomy`, `TagLens`, `OntologySettings`, and all nested models keep their
exact Phase-1 shape (that is the "17a contracts MUST NOT change" rule). Phase 2 adds
**one** new model for the refresh/freshness surface:

```python
RefreshState = Literal["cold", "queued", "running", "fresh", "stale", "failed"]

class OntologyRefreshStatus(BaseModel):
    state: RefreshState                 # cold=never run; fresh=within window; stale=beyond window
    source: Literal["mirror", "live"]   # what the read routes are currently serving
    mirror_as_of: str | None = None     # materialization time of the current mirror (ISO-8601)
    last_run_id: str | None = None
    last_run_state: Literal["succeeded", "failed", "running", "none"] = "none"
    freshness_window_hours: int = 24    # how old the mirror may be before "stale"
    message: str | None = None          # plain-language, zero-burden (e.g. "Updated 3 hours ago")
```

`as_of` semantics (no schema change): on the existing `taxonomy`/`tags`/`inventory`
payloads, `as_of` now reports the **mirror materialization time** when served from
the mirror, and the **live read time** when served from the fallback. The field type
is unchanged.

---

## 5. TypeScript mirrors (`frontend/src/ontology/types.ts`)

Add the mirror of the one new model; leave the rest 1:1 with Phase 1.

```typescript
export type RefreshState = "cold" | "queued" | "running" | "fresh" | "stale" | "failed";

export interface OntologyRefreshStatus {
  state: RefreshState;
  source: "mirror" | "live";
  mirror_as_of?: string | null;
  last_run_id?: string | null;
  last_run_state: "succeeded" | "failed" | "running" | "none";
  freshness_window_hours: number;
  message?: string | null;
}
```

`frontend/src/ontology/api.ts` gains two calls: `getRefreshStatus()` (`GET
/api/ontology/refresh`) and `triggerRefresh()` (`POST /api/ontology/refresh`).

---

## 6. Routes

**Unchanged (Phase 1):** `GET /preflight`, `GET /inventory`, `GET /taxonomy`,
`GET /tags`, `GET/PUT /settings`, `GET /health` — same response models, now
mirror-first for `taxonomy`/`tags`.

**New (Phase 2):**

| Method + path | Returns | Identity | Notes |
|---|---|---|---|
| `GET /api/ontology/refresh` | `OntologyRefreshStatus` | OBO | Mirror freshness + last-run state; drives the freshness chip + button label. Cheap; reads the `genie_ont_runs` mirror header. |
| `POST /api/ontology/refresh` | `OntologyRefreshStatus` | **OBO (admin)** | Triggers the materialize job via `jobs.run_now` (reuse the GSO launcher). Returns `state="queued"`. Idempotent — a second call while `running` returns the in-flight run, never a duplicate. |

Still **no** `/drafts`, **no** `/apply` (Phase 3 / Phase 5). `POST /refresh` triggers
a job; it does **not** itself write UC or the snapshot tables — the **job** does.

**Reader-swap contract (`taxonomy.py` / `tags.py`):**

1. Ask `mirror.py` for the snapshot. If present and `as_of` within
   `freshness_window_hours` → serve it (`source="mirror"`).
2. Else → run the Phase-1 live-SP path (`source="live"`), TTL-cached, and (if a run
   is not already in flight) it is fine to leave materialization to the schedule /
   the user's button — **never block the request on a job** (MV-D43).
3. `preflight` continues to resolve tiers exactly as Phase 1; it may additionally
   report the `batch` tier's status (job configured / last run ok) but its response
   model is unchanged.

---

## 7. Persistence / DDL

Two tiers, the **GSO `genie_opt_mv_*` pattern reused verbatim**: Delta is the
system-of-record (written by the job); Lakebase is a **synced-table mirror** (read by
the page). No new app-created Postgres tables — the mirror is automatic via synced
tables, exactly as GSO does it.

### 7.1 Delta (job-written, GSO catalog/schema) — `genie_space_optimizer/ontology/ddl.py`

```sql
-- Run ledger (append-only; one row per materialization) ----------------------
CREATE TABLE IF NOT EXISTS ${GSO_CATALOG}.${GSO_SCHEMA}.genie_ont_runs (
  run_id           STRING,
  workspace_id     STRING,
  trigger          STRING,            -- 'nightly' | 'on_demand'
  state            STRING,            -- 'running' | 'succeeded' | 'failed'
  scope_allowlist  ARRAY<STRING>,
  started_at       TIMESTAMP,
  finished_at      TIMESTAMP,
  as_of            TIMESTAMP,         -- logical snapshot time (= started_at)
  tag_count        INT,
  domain_count     INT,
  ungrouped_count  INT,
  error            STRING
) USING DELTA;

-- Governed-tag snapshot (backs 17.0c) ---------------------------------------
CREATE TABLE IF NOT EXISTS ${GSO_CATALOG}.${GSO_SCHEMA}.genie_ont_tag_graph (
  workspace_id      STRING,
  tag_key           STRING,           -- (workspace_id, tag_key) is the derived PK
  allowed_values    ARRAY<STRING>,
  assignment_count  INT,
  acts_as_domain    BOOLEAN,
  acts_as_subdomain BOOLEAN,
  dedupe_verdicts   STRING,           -- JSON: collisions + cleanup flags for this tag
  run_id            STRING,
  as_of             TIMESTAMP
) USING DELTA;

-- Taxonomy snapshot (backs 17.0b) — one serialized tree per workspace --------
CREATE TABLE IF NOT EXISTS ${GSO_CATALOG}.${GSO_SCHEMA}.genie_ont_taxonomy_snapshot (
  workspace_id      STRING,           -- derived PK
  tree              STRING,           -- JSON: the OntologyTaxonomy payload (§4, Phase 1)
  run_id            STRING,
  as_of             TIMESTAMP
) USING DELTA;

-- Phase-3 tables — created EMPTY here, populated later (do NOT write in Phase 2)
CREATE TABLE IF NOT EXISTS ${GSO_CATALOG}.${GSO_SCHEMA}.genie_ont_domains       (...) USING DELTA;
CREATE TABLE IF NOT EXISTS ${GSO_CATALOG}.${GSO_SCHEMA}.genie_ont_members       (...) USING DELTA;
CREATE TABLE IF NOT EXISTS ${GSO_CATALOG}.${GSO_SCHEMA}.genie_ont_pages         (...) USING DELTA;
CREATE TABLE IF NOT EXISTS ${GSO_CATALOG}.${GSO_SCHEMA}.genie_ont_consents      (...) USING DELTA;
CREATE TABLE IF NOT EXISTS ${GSO_CATALOG}.${GSO_SCHEMA}.genie_ont_suppressions  (...) USING DELTA;
```

(The empty Phase-3 tables follow architecture §7 column intent; give them their
final columns now so the synced-table + grant walk never has to re-DDL. They stay
empty until Phase 3.)

### 7.2 Idempotency (the load-bearing rule)

- **Derived keys, not run-scoped rows.** `genie_ont_tag_graph` is keyed on
  `(workspace_id, tag_key)`; `genie_ont_taxonomy_snapshot` on `(workspace_id)`. The
  job writes with **`MERGE`**: update matched, insert new, and
  **`WHEN NOT MATCHED BY SOURCE ... DELETE`** scoped to the run's `workspace_id` so a
  tag that disappeared is removed. Result: a re-run yields the **same rows**, never
  duplicates.
- `genie_ont_runs` is the only append-only table (one header per run); `as_of` and
  freshness come from its latest `succeeded` row.
- Because Phase 2 writes **no proposals**, the MV-D26 suppression ledger has nothing
  to suppress yet — the `genie_ont_suppressions` table exists but the idempotency it
  guards (never resurrect a dismissed proposal) is exercised from Phase 3 on.

### 7.3 Lakebase mirror (synced tables — no app DDL)

`backend/ontology/services/mirror.py` **mirrors `gso_lakebase.py` exactly** — do not
invent a new read path. Note the current GSO reality: in `gso_lakebase.py`,
`_SYNCED_TABLES_ENABLED = False`, so reads **fall through to the Delta table via the
SQL warehouse today**; true Lakebase **synced tables** are the future flip (registered
via `scripts/setup_synced_tables.py` + `scripts/deploy_lib/`, the same mechanism GSO
uses for `genie_opt_mv_*`) that activates when the flag flips and the tables are
provisioned. So `mirror.py` reads `genie_ont_runs`, `genie_ont_tag_graph`, and
`genie_ont_taxonomy_snapshot` through that one interface — **Delta-via-warehouse now,
auto-upgrading to synced tables later** — and the routers never see the difference.
Register the three tables in `setup_synced_tables.py` so they light up when the flip
happens. The Phase-1 `genie_ont_settings` Postgres table (created by `_ensure_schema`)
is unchanged.

---

## 8. Batch job implementation notes

**Job task (`jobs/run_ontology_materialize.py`)** — a notebook-source task like
`run_intake_and_snapshot.py`. It:

1. Reads job parameters (`workspace_id`, `trigger`, and the catalog allowlist —
   resolved from `genie_ont_settings` or passed through), and writes a `running`
   row to `genie_ont_runs`.
2. **SP reads** the same system tables Phase 1 reads live (`system.tags.governed_tags`
   + `information_schema.*_tags` + lineage adjacency), scoped by the allowlist.
3. Applies the **shared pure transforms** (`ontology/transforms.py`) to produce the
   `GovernedTag`/collision/cleanup rows and the `OntologyTaxonomy` tree — **the same
   functions the Phase-1 routes call**, so mirror output == live output (enforced by
   the parity test, §11).
4. Builds the **L2 fused signal graph** (`ontology/graph.py`) as a structural
   dependency only — nodes (tag/table/mv/agent) + edges (tag_assignment, lineage
   adjacency). **No clustering.** It is not persisted as proposals; it exists so 17e
   can attach Louvain without a new builder.
5. **`MERGE`-writes** the Delta snapshots (§7.2), then flips the `genie_ont_runs`
   row to `succeeded` with `as_of`, counts. On any failure → `failed` with `error`;
   the page keeps serving the last good mirror (or the live fallback).

**On-demand trigger (`services/refresh.py`)** — reuse the GSO `jobs.run_now`
launcher (`integration/trigger.py` / `job_launcher`) keyed on a new
`GSO_ONT_JOB_ID` env var (deploy-injected, like `GSO_JOB_ID`). Concurrency: if a run
is already `running`, return it; do not launch a duplicate.

**Nightly schedule** — a DABs `schedule` (quartz cron) on the new job (see §10).

---

## 9. Frontend wiring

- **No new frames.** Reuse `17.0b` (taxonomy) and `17.0c` (tags lens); wire them to
  the mirror-backed routes (unchanged response shape).
- **Freshness chip** — on the taxonomy/tags panels, show a small, zero-burden status
  from `GET /refresh`: e.g. *"Updated 3 hours ago"* (`source="mirror"`), or *"Live
  view"* (`source="live"`, mirror cold/stale). No jargon about jobs, Delta, or
  synced tables.
- **"Refresh ontology" button** (admin only) → `POST /refresh`, then poll `GET
  /refresh` until `succeeded`/`failed`; show *"Refreshing…"* while `running`. Keep
  the copy plain — a curator sees a recommendation surface, never the machinery.

---

## 10. Grants / deploy (DABs)

- **No new system-table grant** beyond Phase 1 — the job's SP reads use the grants
  already added in Phase 1 (`system.tags.governed_tags`) plus the existing GenieWatch
  `system.{query,billing,access}` grants. The SP additionally needs the GSO
  catalog/schema write it already has for `genie_opt_mv_*`.
- **New job in `databricks.yml`** — one task (`ontology_materialize`) on the GSO
  wheel + `run_ontology_materialize.py`, with a **nightly `schedule`** (quartz cron;
  GSO currently has no schedule block, so this is the new bit) and a `workspace_id`
  parameter. The deploy script injects `GSO_ONT_JOB_ID` into `app.yaml` from bundle
  state, the same way `GSO_JOB_ID` is injected.
- **Synced tables** — extend `scripts/setup_synced_tables.py` (+ the notebook path in
  `scripts/deploy_lib/`) with the three new `genie_ont_*` snapshot tables.

---

## 11. Tests (offline, `backend/tests/` + GSO `tests/unit/`, run via `./scripts/test.sh`)

All acceptance is **offline** — the job's pure logic runs without a cluster; the
mirror runs against the in-memory Lakebase fallback.

- **Contract-frozen guard** — assert the Phase-1 response models
  (`OntologyPreflight`, `OntologyTaxonomy`, `TagLens`, `OntologyInventory`,
  `OntologySettings`) are **byte-identical** to Phase 1 (field set + types
  unchanged); only `OntologyRefreshStatus` is added.
- **Mirror-vs-live parity** — for a fixture set of raw tag/lineage rows, the job's
  materialized `tree` + `tag_graph` equal what the Phase-1 live path produces from
  the same fixture (same shared transforms → byte-identical JSON).
- **Idempotent re-run** — running the materializer twice over the same fixture yields
  the **same rows** (no duplicates); a tag removed between runs is deleted
  (`NOT MATCHED BY SOURCE`); a tag added appears once.
- **Reader swap** — with a fresh mirror fixture → route serves `source="mirror"` and
  `as_of` = mirror time; with a cold/stale mirror → route serves `source="live"` and
  does not raise; a permission error on the live fallback still degrades (Phase-1
  firewall behavior preserved).
- **Freshness** — `GET /refresh` reports `cold` before any run, `fresh` within the
  window, `stale` beyond it; `POST /refresh` while `running` returns the in-flight
  run (no duplicate launch — mock the launcher).
- **Read-only firewall (extended)** — the Phase-1 firewall test now also scans the
  **GSO `ontology/` package**: no `SET TAG` / `CREATE GOVERNED TAG` /
  `manage_uc_tags` write path anywhere; the only UC writes are the `genie_ont_*`
  **Delta snapshot** `MERGE`s (never a governed-tag DDL). Assert the empty Phase-3
  tables are created but never written.
- **DDL shape** — `ontology/ddl.py` creates exactly the snapshot tables + the empty
  Phase-3 tables and nothing else; no `lakebase_vector` / `lakebase_text` /
  `web_search` tokens anywhere (Phase 3/4 firewall).

---

## 12. Definition of done & explicit deferrals

**Offline done (the agent stops here) when:** the materializer + `ontology/`
transforms/ddl/graph land in the GSO wheel; the job task exists; the routers read the
mirror with a live fallback; `GET/POST /refresh` + the freshness chip + button are
wired with zero-burden copy; the Phase-1 contracts are provably unchanged; and
`./scripts/test.sh` + `cd frontend && npm run lint` + `tsc` are all green — including
the parity, idempotency, reader-swap, freshness, and extended-firewall tests.

**Deploy-gated (human, after the offline run — the agent must NOT do these):**
register/deploy the DABs job (`databricks bundle deploy -t app`), confirm the nightly
schedule + `GSO_ONT_JOB_ID` injection, run the job once against the live workspace,
verify the **synced tables** populate Lakebase and the page flips from `source="live"`
to `source="mirror"` with a real `as_of`. OBO auth, the SP system-table reads, the
Delta write, and the synced-table mirror can only be validated in a deployed app —
that boundary is why Phase 2's offline slice stops before deploy.

**Explicitly deferred (do NOT pull forward):**

- **Phase 3** — the proposal engine: L3 ER + **MV-D40 Lakebase Search dedupe**
  (`lakebase_vector` + `lakebase_text`; **enables Lakebase Search** — beta,
  irreversible; degrades to in-process cosine), L4 `igraph` **clustering / Louvain**
  (MV-D39, using the L2 graph this phase scaffolds), L5 Page miners, L6 rank/trust;
  populates the empty `genie_ont_domains/members/pages` and unlocks the `17.0d/e`
  **drafts**.
- **Phase 4** — external context / Context Pack (MV-D38 / MV-D44) via the MV-D47 AI
  Gateway MCP context-source registry (MV-D46 governed web search), opt-in tier.
- **Phase 5** — the consented **`SET TAG` apply** (L9), dry-run → preview → consent →
  audit (`genie_ont_applied`).
