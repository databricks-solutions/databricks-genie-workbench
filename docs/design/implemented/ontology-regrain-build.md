# Ontology — Re-grain to metastore (MV-D49) — build spec

**Status:** build-ready (offline slice) · **Owner directive:** MV-D49 — the ontology
grain is the **metastore** (account-level governance boundary), not the workspace;
`workspace_id` becomes provenance, never a key. **Design source of truth:**
`ontology-engine-architecture.md` §7 (grain statement) + §8 (identity map — the reads
are already account-level). **Reconciles:** the shipped `ontology-phase3a-build.md` +
`ontology-phase3b-build.md` code (currently `workspace_id`-keyed). **Runs BEFORE:**
`ontology-phase3c-build.md` (17f is authored at metastore grain on top of this).

This is a **pure re-grain refactor**, not a feature. It changes the **partition key**
of every `genie_ont_*` table from `workspace_id` to `metastore_id`, re-scopes the
idempotent MERGE delete, makes the batch run **once per metastore**, and demotes
`workspace_id` to a provenance column. **No new proposal logic, no new archetype, no
behaviour change beyond the grain.** All acceptance is offline.

> **The one-line contract:** the pipeline already *reads* account-level data
> (`system.tags.governed_tags` + `system.information_schema.*`, catalog-allowlist
> scoped, no workspace filter — the only workspace-bound input is the best-effort
> Genie-agents list). This phase makes the *storage + serving grain* match that
> reality: one ontology per metastore, `workspace_id` kept only as provenance.

---

## 1. Scope

### In (re-grain)

- **Wheel key constants** (`ontology/materialize.py`): every `*_KEYS` list leads with
  `metastore_id` instead of `workspace_id`:
  - `TAG_GRAPH_KEYS = ["metastore_id", "tag_key"]`
  - `TAXONOMY_KEYS = ["metastore_id"]`
  - `IDENTITY_KEYS = ["metastore_id", "canonical_id", "member_ref"]`
  - `DOMAIN_KEYS = ["metastore_id", "domain_id"]`
  - `MEMBER_KEYS = ["metastore_id", "domain_id", "asset_fqn"]`
  - the run ledger stays keyed on `run_id` (one header per run), but carries
    `metastore_id` + a provenance `workspace_id`.
- **MERGE delete scope** (`ontology/ddl.py` `build_snapshot_merge_sql`, `materialize.py`
  `SparkSnapshotWriter.merge` + `run_materialize`): the `WHEN NOT MATCHED BY SOURCE …
  DELETE` predicate and the `merge(..., scope_id)` argument switch from `workspace_id`
  to `metastore_id`. Rename the parameter (`workspace_id` → `metastore_id`) through
  `run_materialize`, `build_snapshot`, `build_domain_rows`, `identity_map_rows`, and the
  writer interface.
- **DDL** (`ontology/ddl.py`): every `genie_ont_*` table gains `metastore_id STRING`
  as the leading PK column and **retains `workspace_id STRING` as a provenance column**
  (nullable; "which install triggered this run"; for members/agents, which workspace an
  Agent lives in). Update the PK comments accordingly. (Delta has no enforced PK; the
  "PK" is the MERGE key — so this is a column + comment change, not a constraint.)
- **Batch job** (`jobs/run_ontology_materialize.py`): resolve **`metastore_id`**
  (`make_workspace_client().metastores.current().metastore_id`, else the system-table
  `CURRENT_METASTORE()`/`system.information_schema` fallback, else `"default"`), add a
  `metastore_id` widget, keep `workspace_id` as a resolved provenance value, and pass
  `metastore_id=…` to `run_materialize`. The job is **scheduled once per metastore**
  (see §8); the idempotent MERGE makes a duplicate/concurrent install safe.
- **Backend mirror reads** (`backend/ontology/services/mirror.py`): every read scopes by
  `metastore_id` instead of `workspace_id` (`latest_run`, `latest_succeeded_run`,
  `read_taxonomy_tree`, `read_tag_graph`, and any domain/member/identity reader). Resolve
  the app's metastore once (`WorkspaceClient().metastores.current().metastore_id`, cached)
  and thread it through `refresh.py`, `taxonomy.py`, `tags.py` in place of the current
  `workspace_id` argument. The per-workspace app reads the **one** metastore ontology.
- **Synced tables** (`scripts/setup_synced_tables.py`): every registered PK leads with
  `metastore_id` (e.g. `genie_ont_domains` → `["metastore_id", "domain_id"]`).
- **Tests** — re-grain every workspace-scoped assertion to metastore; add the "two
  installs / same metastore converge on one row set" test (see §11).

### Out (deferred / unchanged)

- **No API RESPONSE-contract change.** The routes and Pydantic models are unchanged;
  only the internal *scope argument* the mirror uses changes (workspace_id → metastore_id
  resolved server-side). `OntologyRefreshStatus` and the taxonomy/tags shapes are
  byte-identical to callers.
- **No new table, no clustering/ER/miner logic change, no dependency** (`uv.lock`
  untouched). 17f's canonical-concept + corroboration work is a **separate** phase on top.
- **Cross-metastore merge** (one physical row spanning metastores) is explicitly out —
  the grain is the single metastore (MV-D49).
- **Deploy-gated:** running the job live per metastore + confirming the mirror repopulates
  is the human step after this offline slice.

---

## 2. Decisions honored

| Decision | Posture |
|---|---|
| MV-D49 grain = metastore | **Active — the whole phase.** `metastore_id` is the key; `workspace_id` is provenance. |
| MV-D42 catalog allowlist | **Active — unchanged.** Still the scoping knob; it already bounds the account-level reads. |
| MV-D41 nightly + on-demand | **Active** — but the schedule is now **one runner per metastore** (§8). |
| MV-D37 / D39 / D40 / D45 | **Consumed, not changed.** Same tags/clustering/similarity/deps; only the key moves. |
| MV-D43 degrade-not-hang | **Active** — metastore resolution degrades to a stable fallback id; a missing id never blocks. |
| MV-D26 suppression ledger | **Active** — derived ids are unchanged (they never included `workspace_id`), so suppression still keys correctly once the scope is metastore. |

---

## 3. Subsystem layout (wheel + backend + scripts; no frontend, no new module)

```
packages/genie-space-optimizer/src/genie_space_optimizer/
  ontology/
    ddl.py                # MODIFIED — metastore_id PK col (+ workspace_id provenance col) on all genie_ont_*; MERGE delete predicate
    materialize.py        # MODIFIED — *_KEYS lead metastore_id; run_materialize(metastore_id=…); writer scope arg rename
    transforms.py         # MODIFIED — identity_map_rows / domain rows carry metastore_id (+ provenance workspace_id)
  jobs/
    run_ontology_materialize.py   # MODIFIED — resolve metastore_id; per-metastore run; workspace_id kept as provenance widget
backend/ontology/
  services/mirror.py      # MODIFIED — all reads scope by metastore_id; resolve app metastore once (cached)
  services/refresh.py     # MODIFIED — thread metastore_id
  routers/taxonomy.py     # MODIFIED — resolve + pass metastore_id (no response-shape change)
  routers/tags.py         # MODIFIED — resolve + pass metastore_id (no response-shape change)
scripts/setup_synced_tables.py   # MODIFIED — synced-table PKs lead metastore_id
backend/tests/…           # MODIFIED — workspace-scoped assertions re-grained; convergence test added
packages/genie-space-optimizer/tests/unit/…   # MODIFIED — materialize/idempotency tests re-grained
```

**Reuse, do not fork:** the `build_snapshot_merge_sql` MERGE builder, the reader/writer
seams, and the metastore resolver pattern already used elsewhere in the app
(`WorkspaceClient().metastores.current()`); do **not** introduce a new persistence path.

---

## 4. Contracts

**No API response model changes.** The mirror's internal scope argument changes
(`workspace_id` → `metastore_id`, resolved server-side from the app's `WorkspaceClient`),
but every route returns the same shape. `OntologyRefreshStatus`, the taxonomy tree, and
the tag lens are byte-identical to callers. This is the frozen-contract guard §11 asserts.

---

## 5. TypeScript / frontend

**None.** No route, model, or component changes.

---

## 6. Metastore resolution (the one new rule)

- **Job side:** resolve `metastore_id` from `make_workspace_client().metastores.current()`;
  on failure fall back to a system-table `CURRENT_METASTORE()` read, then to `"default"`
  (degrade-not-hang, MV-D43). The resolved value is stable for a given workspace/metastore,
  so re-runs MERGE onto the same rows.
- **Backend side:** resolve the app's metastore once at startup / first read and cache it
  (the app runs in one workspace → one metastore); thread it as the scope id. Never derive
  scope from the request unless a future multi-metastore admin view needs it (out of scope).
- **Provenance:** keep `workspace_id` on every row (resolved as today) so lineage of "which
  install wrote this" survives; it is **never** part of a key or a delete predicate.

---

## 7. Persistence / DDL

- Every `genie_ont_*` `CREATE TABLE` gains `metastore_id STRING` as the **leading**
  column with the "PK with …" comment, and keeps `workspace_id STRING` (comment:
  `provenance — which install/workspace triggered the run; NOT a key`).
- CDF stays on. No table added or removed. No column dropped (workspace_id demoted, not
  deleted — safe for any already-written rows).
- The MERGE builder's delete predicate uses `metastore_id`; the run ledger's per-`run_id`
  MERGE is unchanged except it now carries `metastore_id` + provenance `workspace_id`.

---

## 8. Batch job / scheduling

- The `ontology_materialize` task resolves `metastore_id` and runs **once per metastore**.
- If several workspace installs share a metastore, the idempotent, metastore-scoped MERGE
  makes concurrent/duplicate runs **convergent** (they land the same rows) — but the
  operational recommendation is **one scheduled runner per metastore** to avoid churn.
  Document this in the job header; no code lock is added (the MERGE is the safety net).
- `GSO_ONT_JOB_ID` and the existing trigger path are unchanged.

---

## 9. Frontend wiring

**None.**

---

## 10. Grants / deploy

- **No new grant** — `system.tags.governed_tags` + `system.information_schema.*` are
  already SP-readable (the reads were always account-level). Metastore resolution needs
  no extra grant (`metastores.current()` is available to the app's client).
- **No dependency change** — `uv.lock` untouched.
- **Synced tables** — re-register PKs to lead `metastore_id`.

---

## 11. Tests (offline, `./scripts/test.sh`)

- **Contract-frozen** — every route returns the same shape; `OntologyRefreshStatus`,
  taxonomy, tag lens byte-identical (the scope arg change is internal only).
- **Key re-grain** — `*_KEYS` lead `metastore_id`; the MERGE SQL delete predicate is
  `metastore_id`-scoped (assert on the generated SQL string).
- **Convergence (the load-bearing new test)** — running the materializer twice with the
  **same `metastore_id` but two different provenance `workspace_id`s** yields **one** row
  set (no duplicate `Domain=Finance`), and the second run's provenance is recorded without
  forking the key.
- **Idempotency re-grain** — the existing "re-run yields identical rows / NOT-MATCHED-BY-
  SOURCE deletes stale rows" tests now assert **metastore-scoped** deletion (a row from a
  *different* metastore is NOT deleted by this metastore's run).
- **Provenance retained** — `workspace_id` is present on rows but absent from every key /
  delete predicate.
- **Metastore resolver degrade** — resolver failure falls back to a stable id, run still
  succeeds (MV-D43).
- **Backend mirror scope** — mirror reads filter by `metastore_id`; the per-workspace app
  reads the one metastore ontology.
- **Firewall unchanged** — still no `SET/UNSET/CREATE GOVERNED TAG` / `manage_uc_tags` /
  `web_search`; write set unchanged (snapshots + domains/members; pages/consents/
  suppressions still never written here).

---

## 12. Definition of done & explicit deferrals

**Offline done (the agent stops here) when:** every `genie_ont_*` table is keyed by
`metastore_id` with `workspace_id` demoted to provenance; the MERGE delete is
metastore-scoped; the job resolves + runs per metastore; the backend mirror + routes
resolve and scope by `metastore_id` with **no response-shape change**; synced-table PKs
lead `metastore_id`; and `./scripts/test.sh` + `npm run lint` + `tsc` are green —
including the convergence, metastore-scoped idempotency, provenance-retained, resolver-
degrade, and frozen-contract tests. `uv lock --check` green (lockfile untouched).

**Deploy-gated (human, after the offline run):** run the `ontology_materialize` job once
per metastore and confirm the mirror repopulates one ontology (no per-workspace
duplicates). Then 17f builds on this grain.

**Deferred:** the full **cross-metastore** merge (one row spanning metastores) — out of
scope; the grain is the single metastore (MV-D49). 17f's canonical-concept + corroboration
gating is a separate phase.
