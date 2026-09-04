# Ontology — Curation Redesign · Stage 3.2 build spec (diffuse-FK root cause + Gate-B)

Wheel-first follow-up to **Stage 3.1**. Stage 3.1 fixed the two correctness bugs the
Stage-3 gate exposed (curated Domains wrongly gated; curated tag not absorbing its FK
twin). Its deploy-verify (§A.3 of the consolidated build) confirmed **Fix 1 fully** and
**Fix 2 partially**: the curated `Alaska Airlines Maintenance and Engineering` gained FK
corroboration, but a **separate `Airline Demo Mvm Maintenance` FK Domain still
surfaced** — a *diffuse FK hairball* spanning 9 schemas with home-concentration 0.32.

Stage 3.1's own analysis showed why widening the absorb match (share-anchor-schema /
home-set-overlap — the "Stage-3.2 small" option) is the **wrong** fix: the hairball's
true center of mass is a **migration/infra schema**, not maintenance, so relaxing absorb
would over-merge unrelated same-schema groups and pollute clean curated Domains. The
defect is not in absorption — it is that the hairball **should never have formed**.

This spec attacks the **root cause** (the mechanism that fuses unrelated stars into one
component) and adds a **presentation safety net** (Gate-B). Consolidated context:
`docs/design/ontology-curation-redesign-build.md` (§5.1 signals, §7 gates/config, §A.3).
Decisions: `docs/design/mv-advisor-playbook.md` — **new MV-D61 / MV-D62**; honor
MV-D35/43/45/49/50/52/53/56/57.

---

## 1. Root cause — grounded in the Step-0 bridging-column probe

The `shared_join_column` proxy (`schema_signals.shared_join_column_edges`) links any two
tables sharing a column whose name ends `_id` / `_key` / `_code`, capped only at **≤ 15
tables per column** (`MAX_TABLES_PER_SHARED_COLUMN`). That cap bounds fan-out **per
column** but does nothing about **cross-schema reach**, so a column touching 8 tables in
6 schemas still stitches all 6 schemas into one component. Chained across columns, small
per-schema stars fuse into an estate-wide blob.

**Probe (live, `serverless_stable_6t92c3_catalog`, 2026-09-01) — columns that pass the
≤15-table cap yet bridge ≥ 3 schemas:**

| column | schemas | tables | class |
|---|---|---|---|
| `flight_leg_id` | 6 | 8 | business key, but reaches infra |
| `postal_code` | 5 | 8 | reference/enum (`_code`) |
| `route_id` | 4 | 10 | business key, reaches infra |
| `ffp_member_id` | 4 | 8 | business key, reaches infra |
| `origin_station_id` / `station_id` | 4 | 7 | business key |
| `operating_carrier_code` | 4 | 7 | reference/enum (`_code`) |
| `iata_code` | 4 | 6 | reference/enum (`_code`) |
| `region_code` / `reason_code` / `state_or_province_code` / `passenger_type_code` / … | 3 | 3–13 | reference/enum (`_code`) |

**Airline → infra bridges (a business schema joined to an infra/demo schema, ≤15
tables):** `postal_code`, `question_id`, `carrier_code`, `ticket_id`, `employee_id`,
`region_code`, `reason_code`, `target_id`, `status_code`, `profile_id`, `chunk_id`,
`audit_id` — i.e. `genie_space_optimizer` / `metadata_results` / `migration` infra tables
carry generic `_id`/`_code` columns that collide with airline tables.

**Estate composition:** of the catalog's schemas, only `airline_demo*` are the business
estate. The rest — `migration`, `genie_space_optimizer`, `cost_attribution`,
`metadata_results`, `prashanth_wanderbricks_*`, `skyloyalty_dev`, `e2e_*`, `bakehouse`,
`information_schema`, … — are **infra / pipeline / demo** schemas that are not business
domains at all, yet they are the hairball's mass.

**Two mechanisms, therefore two root-cause levers (MV-D61):**
1. **Reference `_code` columns and generic `_id`s are not join keys.** `_code` columns
   are enum/lookup values (`country_code`, `currency_code`, `region_code`, `postal_code`,
   `reason_code`, `status_code`); generic `_id`s (`user_id`, `workspace_id`,
   `category_id`) are surrogate/audit keys. They create spurious bridges.
2. **A shared-join column that spans many schemas is a generic bridge, not a domain
   key.** A real bounded-context key stays within a domain's few schemas.

---

## 2. Design

### 2.1 Root cause A — structural edge hygiene · `schema_signals.py` (pure, MV-D61)

All changes are **pure** and **offline-testable** (no I/O, no new dep — MV-D45). All new
limits are **config-thresholded** (MV-D57) with the shipped defaults below; a param-less
call keeps today's behavior *except* the two principled cuts (`_code` drop, schema-span
cap) that are the fix.

1. **Non-business schema denylist (the biggest lever).** Add a pure
   `filter_denylisted_schemas(rows, *, denylist)` applied to **every** row input
   (`referential_*`, `key_column_*`, `constraint_column_*`, `column_rows`, `table_rows`,
   MV-YAML sources) **before** any edge is built, dropping any FQN whose `catalog.schema`
   matches a denylist entry (exact `catalog.schema`, bare `schema`, or a `fnmatch` glob
   like `e2e_*`, `*_dev`). Applied first, so a legit key that only *looked* cross-schema
   because it also touched infra collapses back to its true 1–2 business schemas.
   **Shipped default:** `["information_schema"]` only (always non-business; estate-neutral
   — MV-D57 leaves the rest to per-enterprise config so we never hard-code demo names).
2. **`_code` is not a join key.** Change `JOIN_COLUMN_SUFFIXES` default `("_id","_key",
   "_code")` → `("_id","_key")`. Configurable via `domain_join_col_suffixes`.
3. **Per-column schema-span cap.** `shared_join_column_edges` gains
   `max_schemas: int = 2`: after grouping tables by column, **skip a column whose tables
   span more than `max_schemas` distinct `catalog.schema`** (computed from the FQN). A
   declared FK (`fk_edges`) is **exempt** — it is decisive and explicit; only the
   *proxy* is capped.
4. **Generic-name denylist.** `shared_join_column_edges` gains
   `name_denylist: frozenset[str]` (casefolded exact column names) — skip a denylisted
   column outright. **Shipped default seed:** `{"id","user_id","workspace_id",
   "category_id","tenant_id","account_id"}` (surrogate/audit keys). Threaded from config.
5. **Thread through `join_key_edges(...)`** so the job reader passes `join_suffixes`,
   `max_tables`, `max_schemas`, `name_denylist`. `fk_edges` unchanged.

Reader wiring (`jobs/run_ontology_materialize.py`): read the four new job params (below),
apply `filter_denylisted_schemas` to the collected rows, and pass the knobs into
`join_key_edges`. Degrade-to-empty on any missing grant stays (MV-D43).

### 2.2 Gate-B — diffuseness presentation gate · `rank.py` + `transforms.py` (MV-D62)

A last-resort net so the UI never shows a hairball even on an estate that has not tuned
its denylist. Mirrors `_apply_legitimacy_gate` exactly (same shape, same curated-exempt
rule from Stage 3.1).

- `transforms.is_diffuse(n_schemas, home_concentration, *, max_schemas, min_home_concentration) -> (diffuse, reason)`
  — pure; `diffuse` when `n_schemas >= max_schemas AND home_concentration < min_home_concentration`.
  `home_concentration` = (members in the single most common `catalog.schema`) / (total
  members). Defaults **`max_schemas=6`, `min_home_concentration=0.5`** (from A.3: the
  hairball was 9 schemas / 0.32; every real Domain in the estate is ≤ 3 schemas / ≥ 0.7).
- `rank._apply_diffuseness_gate(...)` — runs **only** on a **top-level, non-curated**
  Domain whose origin is structural (`create` / FK-component; reuse `_is_curated_domain`
  to exempt curated, and skip sub-domains/reassign/pages exactly like the legitimacy
  gate). Below the net → row **KEPT** but `evidence["surfaced"]=False`, `rank["diffuse"]
  =True`, `rank["diffuse_reason"]="too broad — spans N schemas, home concentration X;
  split or attach to a specific area"`. Curated Domains are **never** diffuse-gated.
- Wire it in `_score_row` right after `_apply_legitimacy_gate` (Domains only), reading
  the two thresholds threaded from config with in-code defaults.

### 2.3 Options considered and **declined** (honest engineering call)

- **(a) Relax `absorb_curated_into_structural`** to fire on shared-anchor-schema /
  home-set-overlap. **Declined** — over-merge risk (A.3 analysis): the diffuse twin's
  home is a *migration* schema, so a schema-anchor match would fold infra into the
  curated maintenance Domain. With Root-cause A the twin loses its cross-schema mass and
  its members collapse into the real maintenance schema, at which point the **existing**
  Stage-3.1 shared-home absorb fires cleanly. Widening absorb is both unnecessary and
  unsafe.
- **(c) Component split (finer-γ Leiden on oversized FK components).** **Declined as
  redundant** — Stage-2 already runs Leiden-on-remainder (MV-D53/54), and after Root-cause
  A no giant *cross-schema* component forms. A large *single-schema* star is a legitimate
  Domain, not a hairball; splitting it would fragment a real domain. Gate-B covers the
  residual-diffuse case without the fragmentation risk.

If, after deploy-verify, a hairball still forms **within a single business schema** (not
observed today), revisit (c) as a scoped fast-follow.

---

## 3. Config (additive/defaulted; MV-D50 `ADD COLUMN IF NOT EXISTS`)

`OntologySettings` (+ `ont_settings.py`, `lakebase.ont_upsert_settings`), all additive so
an old stored row reads the shipped defaults:

| field | type | default |
|---|---|---|
| `domain_schema_denylist` | `list[str]` | `["information_schema"]` |
| `domain_join_col_suffixes` | `list[str]` | `["_id","_key"]` |
| `domain_join_col_max_schemas` | `int` | `2` |
| `domain_join_col_denylist` | `list[str]` | `["id","user_id","workspace_id","category_id","tenant_id","account_id"]` |
| `domain_max_diffuse_schemas` | `int` | `6` |
| `domain_min_home_concentration` | `float` | `0.5` |

`refresh._launch` threads all six as `job_parameters` (like `catalog_allowlist` /
`domain_min_tables`); `run_ontology_materialize` reads them with in-code defaults so a
param-less run still works (MV-D43). `SettingsForm.tsx` MAY expose them for parity
(additive, optional — no new route/frame); no new render is required by Gate-B (below-net
rows are already not shown).

---

## 4. Data-model impact

**None new.** No table, no column, no DDL. Gate-B writes only additive keys into the
existing `evidence` JSON (`rank.diffuse`, `rank.diffuse_reason`). Contracts (routes,
`OntologyRefreshStatus` / taxonomy / tag-lens / drafts response keys) stay byte-identical.

## 5. Guardrails / invariants

- Metastore grain (MV-D49); OBO-default reads (MV-D50); degrade-not-hang (MV-D43); no new
  dependency (MV-D45) — `uv.lock` untouched (git-verified).
- No `SET`/`UNSET`/`CREATE GOVERNED TAG`, no `manage_uc_tags`, no `web_search`; the wheel
  writes no ledger. Firewall (`test_ontology_firewall`) stays green over the changed
  modules.
- Deterministic + offline wheel; `LEIDEN_SEED` unchanged. Do **not** change Stage-1/2
  grouping *outputs* beyond the intended edge-hygiene effect; do **not** touch absorb
  (declined (a)); do **not** pull forward Stage 4 / §9 / §10.
- Root-cause A defaults are conservative: only `information_schema` is denylisted by
  default and only the two principled edge cuts (`_code` drop, span cap) change default
  behavior — a real business FK/declared-FK edge is never dropped.

## 6. Acceptance (offline; `./scripts/test.sh` green)

Root cause A (`test_ontology_schema_signals.py`):
- a `_code` column produces **no** proxy edge under the default suffixes; still produced
  when `_code` is passed back in via config.
- a shared column spanning 3 schemas (≤15 tables) is **skipped** at `max_schemas=2`;
  a 2-schema column still yields its star.
- a denylisted schema's tables are dropped from **every** edge kind before building;
  a denylisted column name yields no edge; a **declared FK** across schemas is **kept**
  (span cap is proxy-only).

Gate-B (`test_ontology_rank.py` / `transforms`):
- `is_diffuse(9, 0.32)` → diffuse; `is_diffuse(3, 0.8)` → not; boundary
  `is_diffuse(6, 0.5)` → not (strict `<`).
- a non-curated top-level FK Domain at 7 schemas / 0.3 home → kept, `surfaced=false`,
  `rank.diffuse=True`; a **curated** Domain at the same shape → **surfaced** (exempt);
  a sub-domain / reassign / page is untouched.

Config (`test_ontology_settings.py` / `test_ontology_phase2.py`):
- settings round-trip (old row → the six defaults); the job runs **both** param-less
  (defaults) and param-driven; contract-frozen (existing keys byte-identical).

Frozen: full backend suite, `npm run test`/`lint`/`build`/`tsc` green (zero or additive-
only frontend), `uv.lock` untouched.

## 7. Deploy-verify (human-gated, after offline lands)

`./scripts/deploy.sh --update` on `fevm-serverless` (offline path:
`SKIP_FRONTEND_BUILD=1 UV_OFFLINE=1` when a build already exists) → Settings: set
`domain_schema_denylist` to the estate's infra schemas (`migration`,
`genie_space_optimizer`, `cost_attribution`, `metadata_results`, `prashanth_wanderbricks_*`,
`skyloyalty_dev`, `e2e_*`, `bakehouse`) → Refresh scoped to
`serverless_stable_6t92c3_catalog`. Confirm:
1. `Airline Demo Mvm Maintenance` no longer surfaces as a standalone Domain (absorbed
   into the curated maintenance Domain, or gated by Gate-B).
2. The three curated Domains still surface (Stage-3.1 regression guard).
3. Member-schema histogram of any surfaced non-curated Domain is home-concentrated
   (≥ 0.5) and ≤ 5 schemas.

```sql
-- surfaced top-level domains + their schema spread
SELECT d.name,
       get_json_object(d.evidence,'$.surfaced') AS surfaced,
       get_json_object(d.evidence,'$.rank.diffuse') AS diffuse,
       count(DISTINCT split(m.asset_fqn,'[.]')[0]||'.'||split(m.asset_fqn,'[.]')[1]) AS n_schemas
FROM <gso_catalog>.genie_space_optimizer.genie_ont_domains d
LEFT JOIN <gso_catalog>.genie_space_optimizer.genie_ont_members m ON m.domain_id = d.domain_id
WHERE d.parent_id IS NULL
GROUP BY 1,2,3 ORDER BY n_schemas DESC;
```

---

## Appendix — Step-0 probe (verbatim, for threshold provenance)

`information_schema.columns`, suffixes `_id`/`_key`/`_code`, grouped by column:
- **≥ 3 schemas, any table count:** `country_code` (12 sch / 26 tbl), `category_id`
  (11/128), `aircraft_id` (7/20), `flight_leg_id` (6/8), `currency_code` (5/26),
  `postal_code` (5/8), `user_id` (4/28), `route_id` (4/10), `ffp_member_id` (4/8),
  `origin_station_id` (4/7), `operating_carrier_code` (4/7), `station_id` (4/7),
  `iata_code` (4/6), … (37 columns total ≥ 3 schemas).
- **airline→infra bridges, ≤15 tables:** `postal_code`, `question_id`, `carrier_code`,
  `ticket_id`, `employee_id`, `region_code`, `reason_code`, `target_id`, `status_code`,
  `profile_id`, `chunk_id`, `audit_id`.
- **non-`airline_demo*` schemas present:** `bakehouse, cost_attribution, default,
  digital, e2e_*, ebm_demo, ed_azure_costs, exception_management_schema,
  genie_space_optimizer, information_schema, master, metadata_results, migration,
  northpeak, sales_reports, shop_reports_*_dev, skyloyalty_dev,
  prashanth_wanderbricks_*`.
