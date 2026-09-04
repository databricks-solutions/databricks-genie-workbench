# Ontology — OBO-first foundations; batch via job `run_as`; SP optional (MV-D50) — build spec

**Status:** build-ready (offline slice) · **Owner directive:** MV-D50 — the ontology
foundation reads default to **OBO (the admin viewer's identity)**, not the app service
principal; the SP is an **optional upgrade** (shared cache / consumer serving), never a
prerequisite; the batch materialize reads system tables as a configurable **`run_as`
identity** (a metastore admin **user** or an SP), so **no app-SP system-table grant is
required anywhere**. **Design source of truth:** `ontology-engine-architecture.md` §2
(thin reader over the mirror) + the MV-D37/D43/D44 read tiers. **Builds on:** the shipped
Phase-1/2/3a/3b spine **and the metastore re-grain (MV-D49)** — grain and every response
contract are unchanged; this phase changes only the *identity* of the reads.

This is an **identity refactor**, not a feature. It swaps the two SP-only foundation
reads (governed-tag graph + usage/lineage signals) to OBO by default, makes the SP an
opt-in via a single `read_identity` setting, adds a DABs `run_as` knob to the materialize
job, and rewrites the preflight banner copy so the SP tiers read **"OBO (admin) or SP"**
instead of implying an SP grant is mandatory. **No grain change (MV-D49 holds), no new
table, no new dependency, no response-shape change.** All acceptance is offline.

> **The one-line contract:** the Ontology page is admin-gated, so the viewing admin
> already holds the system-table access the taxonomy needs — read as **them** (OBO) and
> nothing must be granted to the app SP to render. The SP path stays available for shared
> caching / consumer serving when an admin *chooses* to grant it; scheduled work (the
> mirror) can never be OBO, so it reads as the job's `run_as` identity instead.

---

## 1. Scope

### In (identity refactor)

- **Live governed-tag graph read** (`backend/ontology/services/tag_graph.py`): the `_run`
  helper resolves its client from the selected `read_identity` (below) instead of
  hard-coding `get_service_principal_client()`. Default **OBO** via
  `require_obo_workspace_client()` (never a silent SP fallback — an OBO read that can't
  authorize must degrade the tier, not quietly widen to the SP). `probe`,
  `sp_assignment_count` (rename semantics: "assignment_count for the active identity"),
  and `build_graph` all read through the same resolver.
- **Live usage/lineage signals** (`backend/watch/services/system_tables.py`, consumed by
  the ontology `signals` tier): same resolver seam. Signals stays **optional** (MV-D44) —
  if the active identity can't read `system.access/billing/query`, the tier degrades; it
  never blocks the page.
- **Per-identity cache key**: the in-process TTL caches in `tag_graph.py` (and the watch
  system-tables cache, for ontology reads) key on the **resolved principal** (OBO user id
  / SP app id) so a privilege-filtered OBO view is never served to a different user.
- **`read_identity` setting** (`OntologySettings`): additive field
  `read_identity: Literal["obo", "sp", "auto"] = "obo"`. `obo` = always the viewer;
  `sp` = always the app SP (requires the grants); `auto` = SP when the SP probe is
  accessible (shared cache), else OBO. Stored beside `company_name`/`catalog_allowlist`.
- **Batch `run_as` knob** (`databricks.yml` `ontology-materialize-runner`): a bundle var
  `ontology_job_run_as` that, when set, emits `run_as` on the job resource
  (`user_name: <metastore admin>` **or** `service_principal_name: <sp>`). Unset = current
  behaviour (runs as the deploy identity). Update the job **description + notebook header**
  from "Reads system tables as the SP" to "Reads system tables as the job's `run_as`
  identity (a metastore admin user or an SP)".
- **Preflight tiers + banner** (`backend/ontology/routers/preflight.py`,
  `frontend/src/ontology/components/PermissionBanner.tsx`): the `signals` and `tag_graph`
  tiers report the **active** identity; the banner renders the identity label as
  **"OBO (admin) or SP"** for those two tiers, the reason text frames the SP grants as an
  *optional upgrade* (shared cache / consumer serving), and the **copy button renders on
  every tier that has grant lines** (not only blocked/degraded) — fixing the "no copy
  button on green tiers" gap.

### Out (deferred / unchanged)

- **No grain change.** MV-D49 metastore grain, `metastore_id` keys, `workspace_id`
  provenance — all unchanged. This phase never touches keys, DDL, or the MERGE.
- **No new API RESPONSE shape.** `OntologySettings` gains one additive, defaulted field;
  `OntologyPreflight`, taxonomy, and tag-lens shapes are byte-identical. The tier
  `identity` enum already carries `obo|sp|batch`; no enum change (the "OBO (admin) or SP"
  wording is a frontend label, not a new value).
- **No new table, no new dependency** (`uv.lock` untouched), no clustering/ER/miner change.
- **No consumer-facing exposure.** OBO is privilege-filtered → the page stays
  **admin/curator-gated**; this phase does not open the ontology to consumers.
- **Deploy-gated:** setting `ontology_job_run_as` to a real admin and confirming the
  nightly mirror populates without any app-SP system grant is the human step after the
  offline slice.

---

## 2. Decisions honored

| Decision | Posture |
|---|---|
| **MV-D50 OBO-first; SP optional; job run_as** | **Active — the whole phase.** |
| MV-D49 grain = metastore | **Unchanged.** Identity changes; grain does not. |
| MV-D43 degrade-not-hang | **Reinforced.** An OBO read that can't authorize degrades the tier; it never falls back to the SP or blocks the page. |
| MV-D44 signals optional / enrichment OFF | **Unchanged.** Signals still optional and degradable. |
| MV-D42 catalog allowlist | **Unchanged.** Still the scope; OBO reads are auto-filtered *and* allowlist-bounded. |
| MV-D45 minimal footprint | **Active.** No new table, no new dependency; one settings field + one DABs var + client-seam swaps. |
| MV-D37 governed-tag graph | **Unchanged substrate;** only the read identity is selectable. |

---

## 3. Subsystem layout (backend + DABs + frontend; no new module)

```
backend/ontology/
  services/tag_graph.py     # MODIFIED — client resolver from read_identity; OBO default (require_obo); per-identity cache key
  services/ont_settings.py  # MODIFIED — read_identity accessor (default "obo")
  services/refresh.py       # (unchanged — trigger still SP run_now; execution identity is the job run_as)
  routers/preflight.py      # MODIFIED — tiers report active identity; grant lines framed as optional upgrade
  models.py                 # MODIFIED — OntologySettings.read_identity (additive, defaulted)
backend/watch/services/system_tables.py  # MODIFIED — resolver seam for the ontology signals read (per-identity cache)
databricks.yml              # MODIFIED — ontology_job_run_as var → run_as on ontology-materialize-runner; description reworded
packages/.../jobs/run_ontology_materialize.py  # MODIFIED — header reworded (run_as identity, not "the SP")
frontend/src/ontology/
  components/PermissionBanner.tsx  # MODIFIED — "OBO (admin) or SP" label; always-on copy button; optional-upgrade copy
  components/SettingsTab (or equiv)# MODIFIED (optional) — read-identity toggle (default OBO; SP shows required grants)
  types/index.ts (ontology types) # MODIFIED — OntologySettings.read_identity mirror
backend/tests/…             # MODIFIED — identity-resolution + per-identity-cache + banner-copy tests
```

**Reuse, do not fork:** `get_workspace_client` / `require_obo_workspace_client` /
`get_service_principal_client` already exist (`backend/services/auth.py`) — the resolver
is a 3-line switch over `read_identity`, not a new auth path. The TTL cache and
permission-error detection in `tag_graph.py` / `system_tables.py` are reused as-is, only
the cache **key** gains the principal.

---

## 4. Contracts

- **`OntologySettings`** gains `read_identity: Literal["obo","sp","auto"] = "obo"`
  (additive, defaulted → old clients and stored rows read as `"obo"`). The TypeScript
  mirror in `frontend/src/types` adds the same optional field.
- **`OntologyPreflight` / `PermissionTier`** shapes are unchanged; the `identity` field
  still ∈ `{obo,sp,batch}`. The "OBO (admin) or SP" text is a **frontend rendering** of
  the two read tiers, not a new enum value. `grants` still carries the copy-ready SP lines
  (now surfaced as an optional upgrade).
- Taxonomy tree and tag-lens response shapes: **byte-identical**.

---

## 5. TypeScript / frontend

- **`PermissionBanner.tsx`**: for the `signals` and `tag_graph` tiers, render the identity
  badge as **"OBO (admin) or SP"**; move the **copy button out of the `needsGrant`
  condition** so any tier with `grants.length > 0` shows it (label stays "Copy GRANT SQL"
  for SP lines / "Copy entitlement request" for OBO lines); reword the reason to state the
  SP grants are an optional upgrade for shared caching / consumer serving, not required to
  view.
- **Settings (optional, minimal):** a "Read as" control — *My identity (admin)* [default]
  vs *Service principal* — writing `read_identity`. If skipped, the backend default
  (`obo`) governs; the toggle is a convenience, not the source of truth.
- No other component changes; no route changes.

---

## 6. The one new rule — identity resolution

A single resolver (backend) selects the read client for the two foundation reads:

- `read_identity == "obo"` (default) → `require_obo_workspace_client()`. No OBO context
  (e.g. a background call) → the read is skipped/degraded, **never** widened to the SP.
- `read_identity == "sp"` → `get_service_principal_client()` (requires the banner's grants).
- `read_identity == "auto"` → SP when the SP probe last succeeded (enables the shared
  cross-user cache), else OBO.
- **Cache key** always includes the resolved principal id so an OBO (privilege-filtered)
  result is never served to another user, and an SP result never leaks into an OBO view.
- **Grain/scope unchanged:** whichever identity reads, the rows are still allowlist-bounded
  (MV-D42) and stamped at metastore grain (MV-D49).

---

## 7. Persistence / DDL

**None.** No table, column, key, or MERGE change. The mirror (Delta) reads are unaffected
because they never touch system tables — only the *live/probe* reads change identity.

---

## 8. Batch job / `run_as`

- The materialize job reads system tables via `spark.sql` on the job cluster; those reads
  are governed by the job's **`run_as`** identity ([Jobs privileges]).
- Add a bundle var `ontology_job_run_as` (default empty). When set, the
  `ontology-materialize-runner` resource emits `run_as`:
  - `run_as: { user_name: "<metastore-admin>@company.com" }` — the documented default when
    the app SP has no system grants (the admin's grants back the read), **or**
  - `run_as: { service_principal_name: "<sp-app-id>" }` — when an SP has been granted.
- Unset ⇒ current behaviour (runs as the deploy identity), so this is backward-compatible.
- Reword the job description + notebook header away from "Reads system tables as the SP".
- **Trigger path unchanged:** `refresh._launch` still calls `jobs.run_now` via the SP —
  `run_now` only *starts* the job; execution identity is `run_as`. No change there.
- Databricks recommends SP `run_as` for durability (survives an owner leaving); **user
  `run_as` is the supported bridge** when SP system grants aren't available.

---

## 9. Frontend wiring

Banner copy + labels (§5) and the optional Settings toggle. No new views, no lazy routes.

---

## 10. Grants / deploy — the whole point

- **Default path (no SP system grant at all):** `read_identity="obo"` renders the taxonomy
  as the viewing admin; `ontology_job_run_as = <metastore admin user>` backs the nightly
  mirror. **Nothing is granted to the app SP.**
- **Optional SP upgrade (unchanged grants):** an account admin *may* still grant the app SP
  the banner's `USE CATALOG system` + `USE SCHEMA system.tags` + `SELECT system.tags.
  governed_tags` (and, for ranking, the sensitive `system.access/billing/query` set) to get
  a shared cross-user cache / consumer-safe serving — then set `read_identity` to `sp`/`auto`.
- **Completeness caveat (document in banner reason):** OBO is privilege-filtered, so a
  metastore-complete ontology requires the setup identity (live admin viewer, or the job
  `run_as`) to be an **account/metastore admin**. Non-admin OBO viewers see a partial
  estate — hence the page stays admin-gated.
- **No dependency change** — `uv.lock` untouched.

---

## 11. Tests (offline, `./scripts/test.sh` + vitest)

- **Default is OBO** — with `read_identity` unset/`"obo"`, `tag_graph._run` and the signals
  read resolve `require_obo_workspace_client()`; assert the SP client is **not** used.
- **No silent SP fallback** — `read_identity="obo"` with no OBO context degrades the tier
  (empty/`_TAG_GRAPH_ACCESSIBLE` handling) and never calls `get_service_principal_client()`.
- **SP path on demand** — `read_identity="sp"` resolves the SP client; `"auto"` picks SP
  when the SP probe succeeded, else OBO (table-driven).
- **Per-identity cache** — two principals hitting the same allowlist get **separate** cache
  entries; an OBO result for user A is never returned to user B or to an SP read.
- **Settings contract** — `OntologySettings` round-trips `read_identity`; an old row with
  the field absent reads as `"obo"` (additive/defaulted).
- **Preflight framing** — `signals`/`tag_graph` tiers still return copy-ready `grants`, and
  a blocked SP grant no longer implies "required" (reason text asserts "optional upgrade").
- **Banner (vitest)** — the two read tiers render the **"OBO (admin) or SP"** label and a
  **copy button even when `status==="ok"`** (grant lines present); clicking copies the
  joined lines.
- **`run_as` wiring** — a bundle-validate / snapshot test: with `ontology_job_run_as` set,
  the rendered `ontology-materialize-runner` carries `run_as.user_name` (or
  `service_principal_name`); unset ⇒ no `run_as` block.
- **Grain/contract-frozen** — taxonomy/tag-lens/refresh-status shapes byte-identical; no
  key/DDL/MERGE change; firewall unchanged (still no governed-tag DDL / `web_search`).
- **Deps** — `uv lock --check` green (untouched).

---

## 12. Definition of done & explicit deferrals

**Offline done (the agent stops here) when:** the two foundation reads default to OBO and
resolve identity from `read_identity` (`obo` default, `sp`, `auto`) with **no silent SP
fallback**; caches key on the resolved principal; `OntologySettings.read_identity` is
additive/defaulted and mirrored in TS; the preflight tiers + banner render **"OBO (admin)
or SP"** with an **always-on copy button** and optional-upgrade wording; the DABs
`ontology_job_run_as` var emits `run_as` on the materialize job (user or SP) and the job
description/header no longer say "as the SP"; and `./scripts/test.sh` + `npm run lint` +
`tsc` are green including every §11 case. `uv lock --check` green. **No grain, table, or
response-shape change.**

**Deploy-gated (human, after the offline slice):** set `ontology_job_run_as` to a real
metastore admin (or a granted SP), deploy, and confirm the nightly + on-demand mirror
populate the metastore ontology with **no app-SP system-table grant**; confirm the live
page renders the taxonomy for an admin with `read_identity="obo"`.

**Deferred:** consumer-facing (non-admin) ontology serving — OBO is privilege-filtered, so
opening the page to consumers needs the SP/mirror path and is out of scope here. Any
account-level (cross-metastore) admin view remains out of scope (MV-D49).

[Jobs privileges]: https://docs.databricks.com/aws/en/jobs/privileges
