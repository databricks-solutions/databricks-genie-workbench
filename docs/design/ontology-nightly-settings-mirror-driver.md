# Nightly scan honours Settings (Delta settings mirror) · Goal-Mode driver

> **On the `ontology` branch.** This is **P5** of MV-D109 (`ontology-ga-hardening-driver.md`),
> pulled ahead of P2–P4 by the human on 2026-09-30. ONE phase, ONE commit, then a STOP at the
> deploy-verify gate. Closes PRD Appendix D #1 ("The nightly schedule does no work by default") and
> #2 ("Scheduled runs ignore Settings"), and unblocks CUJ-11 ("Keep the ontology fresh without
> clicking"). Where this driver and the MV-D109 driver's "P5 — Nightly scan honours Settings" spec
> disagree, this driver wins (it refines that spec; see "Refinements to the MV-D109 P5 spec").
>
> **Doc freeze.** Uncommitted until the P5 code commit; it rides inside it (MV-D9 / doc-freeze).

## Status
| Step | Scope | Status |
|---|---|---|
| **S1** | Delta table DDL in the wheel, registered in `_ONT_ALL_DDL` | ✅ offline-green (deploy-verify pending) |
| **S2** | Pure encoder + resolver in the wheel (`ontology/settings_mirror.py`) | ✅ offline-green (deploy-verify pending) |
| **S3** | App writes the mirror (SP) after every successful Settings save | ✅ offline-green (deploy-verify pending) |
| **S4** | Job reads the mirror on a param-less nightly run | ✅ offline-green (deploy-verify pending) |
| **S5** | Tests, docs refresh, VERIFY, commit (not pushed) | ✅ offline-green (deploy-verify pending) |

## The problem in one paragraph
Settings (catalogs, curation policy, alignment, company name) live in **Lakebase**, keyed by
workspace. Only the app can read Lakebase. An on-demand scan works because the app reads Settings
and forwards them as `run_now` job parameters. The 07:00 UTC schedule passes only the job's
declared defaults, and the allowlist default is `"[]"`, so the scope guard records `skipped`
every night and preserves the old snapshot. Freshness counts only succeeded runs, so Review goes
cold 24 hours after the last manual scan. Fix: the app also writes Settings to a Delta row that the
job can read, and a nightly run with no allowlist reads that row.

```
Settings PUT ──► Lakebase (source of truth, unchanged)
            └──► Delta genie_ont_settings_mirror (SP, best-effort)      [S3]
07:00 UTC run (trigger=nightly, catalog_allowlist="[]")
            └──► read this workspace's mirror row ──► resolver ──► widget overrides   [S4]
                 └── no row / unreadable / bad JSON ──► today's behaviour (skipped)
```

---

## Grounded facts (code, read 2026-09-30)

### The schedule and its parameters
- Schedule `quartz_cron_expression: "0 0 7 * * ?"`, UTC, UNPAUSED (`databricks.yml:260-263`).
- Eight declared job parameters (`databricks.yml:268-288`): `workspace_id` `""`, `trigger`
  `"nightly"`, `catalog`, `schema`, `catalog_allowlist` `"[]"` (`:277-278`), `run_id`,
  `warehouse_id`, `llm_model`. They pass to the notebook as `base_parameters` (`:293-301`).
- The job is DABs-only (`scripts/deploy_lib/install.py:87-89`, per the MV-D109 driver), so the GSO
  four-mirror parameter rule does not apply. **P5 adds no job parameter.**

### The notebook (`packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_ontology_materialize.py`)
- Every widget is declared with an in-code default at `:73-141`.
- Parsing reads `dbutils.widgets.get(...)` directly: `trigger` `:145`, `catalog`/`schema` `:146-147`,
  allowlist `:155-158`, Stage-3 curation `:160-173`, Stage-3.2 edge hygiene via `_parse_str_list`
  `:180`, `_parse_int` `:188`, `_parse_float` `:195` (values at `:202-207`), page/coded knobs
  `:214-221`, ER/rename caps `:224-226`, external context `:230-238`, industry alignment `:241-242`.
- `workspace_id` is resolved only AFTER all parsing: `:270-275` (`make_workspace_client().get_workspace_id()`,
  fallback `"default"`); `metastore_id` at `:277-278`; logged at `:280-281`.
- The precedent for a job-side read of an app-written table is the suppression read
  `SparkSystemTableReader.suppressions` `:1009-1025`: `spark.sql` SELECT, any failure logs and
  degrades to empty.
- The run call threads the parsed values at `:1179-1235` (`allowlist=allowlist` at `:1193`).

### The scope guard (`.../ontology/materialize.py`)
- `_has_scope` `:567-576`; the guard in `run_materialize` `:661-677` writes a terminal `skipped`
  row and returns with no MERGE. `writer.ensure_tables()` runs first, at `:636`.

### DDL registry (`.../ontology/ddl.py`)
- `_ONT_ALL_DDL` `:338-353`; `all_ddl` `:356`; `ensure_ontology_tables` `:361-364` (called by the
  job's writer, `materialize.py:1152`).
- The table set is pinned at 14 by `test_ddl_shape_all_tables_no_deferred_tokens`
  (`packages/genie-space-optimizer/tests/unit/test_ontology_materialize.py:1020-1032`), as the union
  of the `*_TABLES` tuples. A new table needs a new tuple there and 14 → 15.

### App-side Settings (`backend/ontology/services/ont_settings.py`)
- Lakebase table is `genie.genie_ont_settings` (module docstring `:3-4`). **Name clash:** the Delta
  mirror must not reuse that name (see decision MD-1).
- `_workspace_id` `:44-58` (SP `get_workspace_id()`, cached, fallback `"default"`); `_metastore_id`
  `:61-78`.
- `get_settings` `:120-177`; `save_settings` `:180-231` returns the normalized `OntologySettings`.
- Route: `PUT /api/ontology/settings` `write_settings` (`backend/ontology/routers/settings.py:28-31`),
  admin-gated at the router (`:20`). It has no `Request` today.
- OBO email helper precedent: `_obo_email(request)` (`backend/ontology/routers/drafts.py:76`,
  duplicated at `routers/apply.py:28`); the route offloads blocking warehouse work with
  `asyncio.to_thread` (`drafts.py:96`).

### What on-demand forwards today (the parity target)
- `_launch` `backend/ontology/services/refresh.py:201-259`, `job_parameters` at `:231-257`.
  Settings-derived keys (16): `catalog_allowlist`, `domain_facet_denylist`, `domain_min_tables`,
  `domain_min_schemas`, `domain_require_connection`, `domain_schema_denylist`,
  `domain_join_col_suffixes`, `domain_join_col_max_schemas`, `domain_join_col_denylist`,
  `domain_max_diffuse_schemas`, `domain_min_home_concentration`, `page_autodraft_min_corroboration`,
  `page_autodraft_max_pages`, `industry_alignment_enabled`, `industry_alignment_reference_model`,
  `company_name`. Infra keys (not Settings): `metastore_id`, `workspace_id`, `trigger`, `catalog`,
  `schema`.
- Encodings: lists `json.dumps(list(x or []))`; ints `str(int(x))`; float `str(float(x))`; bools
  `"true"`/`"false"`; strings `str(x or "")`.
- `trigger()` reads Settings and calls `_launch` at `refresh.py:277-296`. Existing test:
  `test_trigger_launches_when_idle` (`backend/tests/test_ontology_phase2.py:167`) asserts the kwarg set.
- **Not forwarded today:** `external_context_*` (that is MV-D109 P4). `read_identity` is app-only.

### Writing app-state Delta from the app
- The SQL-warehouse MERGE with bound `StatementParameterListItem` values and a poll loop:
  `record_decision` (`backend/ontology/services/decisions.py:80-137`, MERGE builder `:47-77`).
- SP-attributed bookkeeping (identity precedent for this write): `_write_audit_row` imports the wheel
  DDL (`backend/ontology/services/apply.py:586`) and executes as `get_service_principal_client()`
  (`apply.py:622-625`). The firewall pins that split (`backend/tests/test_ontology_firewall.py:252-273`).
- The app SP holds `USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE, … MANAGE` on the GSO schema
  (`SP_SCHEMA_PRIVILEGES` `scripts/grant_permissions.py:37-47`).

### Firewall tests that must keep passing
- `test_wheel_writes_snapshots_proposals_pages_never_consents_suppressions` and
  `test_wheel_reads_but_never_writes_the_ledger` (`backend/tests/test_ontology_firewall.py:144-194`).
- `test_backend_router_verbs_are_read_only_plus_settings_put_refresh_and_decision_post`
  (`:123-141`): no new route verb (P5 adds none).

---

## Decisions (MD-n; all decided by the human, 2026-09-30)

- **MD-1 ✅ Table name `genie_ont_settings_mirror`** (not `genie_ont_settings`): the Lakebase table
  is already `genie.genie_ont_settings`, and two different stores with one name would mislead every
  reader of a query or a log.
- **MD-2 ✅ Key is `workspace_id` alone; `metastore_id` is a provenance column.** Settings are
  per-install (`ont_settings.py` keys Lakebase by workspace), not per-metastore. Keying on
  `(metastore_id, workspace_id)` adds a second id that can resolve differently on the two sides
  (either side can degrade to `"default"`) and turn a valid row into a miss. This refines the MV-D109
  P5 text "one row per `(metastore_id, workspace_id)`".
- **MD-3 ✅ The gate is all-or-nothing.** The mirror is consulted only when `trigger == "nightly"`
  AND the `catalog_allowlist` widget parses to empty. When consulted and valid, the mirror supplies
  **every** Settings-derived knob; otherwise every widget is used as today. Reason: the notebook
  cannot tell an explicit widget value from its in-code default (`"3"` is `"3"`), so a per-key
  "explicit widget wins" rule is not implementable. A manual "Run now" with an allowlist keeps full
  control. This refines the MV-D109 text "explicit widgets win".
- **MD-4 ✅ Nightly = on-demand, exactly.** The mirror maps the 16 Settings-derived keys `_launch`
  forwards today, through the same encodings, and nothing else. In particular **external context
  stays off nightly** until P4 extends both paths together. Otherwise a nightly run would turn on
  web egress that on-demand runs do not, before the HIPAA control (P4) exists. A parity test makes
  that drift impossible (S5).
- **MD-5 ✅ Writer identity is the SP** (the SP owns app-state bookkeeping, as in `apply.py`);
  `updated_by` records the OBO email of the admin who saved. The job only reads the table and
  never writes it.
- **MD-6 ✅ Degrade, never block (MV-D43).** A mirror write failure logs a warning and the PUT
  still returns 200 with the saved Settings. A job read failure (no table, no row, no grant, bad
  JSON) logs one line naming the reason and falls through to today's behaviour (`skipped`).
- **MD-7 ✅ Lakebase stays the source of truth.** The app never reads the mirror. A stale mirror
  (a write failed after a save) is visible in the job log line, which prints `updated_at`.

### Refinements to the MV-D109 P5 spec
MD-1 renames the table, MD-2 narrows the key, and MD-3 replaces per-key precedence with the
all-or-nothing gate. When this phase lands, the MV-D109 driver's P5 spec and status row are
refreshed to point here (same commit).

---

## Build steps

### S1 — DDL (`packages/genie-space-optimizer/src/genie_space_optimizer/ontology/ddl.py`)
```sql
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.genie_ont_settings_mirror (
    workspace_id   STRING    COMMENT 'Install key: one row per workspace (Settings are per-install); the PK',
    metastore_id   STRING    COMMENT 'provenance: the app''s metastore at write time; NOT a key',
    settings_json  STRING    COMMENT 'JSON: OntologySettings.model_dump(mode="json") as saved',
    updated_by     STRING    COMMENT 'OBO email of the admin who saved (the write itself is SP)',
    updated_at     TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
```
Add `TABLE_ONT_SETTINGS_MIRROR = "genie_ont_settings_mirror"`, `SETTINGS_TABLES = (TABLE_ONT_SETTINGS_MIRROR,)`,
and register it in `_ONT_ALL_DDL`. The job's `ensure_tables` will then also create it (harmless,
idempotent). Update the table-count test 14 → 15 and its union.

### S2 — pure seam (`.../ontology/settings_mirror.py`, new)
- `settings_to_widgets(settings: Mapping[str, Any]) -> dict[str, str]`: the 16 keys, `_launch`'s
  encodings, in-code defaults for any missing key (an old row degrades, never raises).
- `resolve_nightly_settings(widget_values: Mapping[str, str], mirror_row: Mapping[str, Any] | None)
  -> dict[str, str] | None`: returns the overrides when MD-3's gate is open and the row parses, else
  `None`. No I/O, no Spark, no SDK.
- `mirror_read_sql(catalog, schema, workspace_id) -> str`: the SELECT (`settings_json`, `updated_at`
  WHERE `workspace_id = '<escaped>'`), with the same quote escaping the suppression read uses.

### S3 — app writes (`backend/ontology/services/settings_mirror.py`, new; route edit)
- `write_mirror(settings: OntologySettings, *, workspace_id, metastore_id, updated_by) -> None`
  (sync): `CREATE TABLE IF NOT EXISTS` from `ddl.all_ddl(...)[TABLE_ONT_SETTINGS_MIRROR]` (the app may save
  before the job ever ran), then a single-row MERGE on `workspace_id`, **every value bound** as
  `StatementParameterListItem`, executed as `get_service_principal_client()` on `SQL_WAREHOUSE_ID`
  with the `decisions.py` poll loop. Raises on failure; the caller decides.
- `routers/settings.py` `write_settings`: take `request: Request`; after `save_settings` succeeds,
  `await asyncio.to_thread(write_mirror, saved, …, updated_by=<OBO email>)` inside `try/except` that
  logs and continues (MD-6). For the email, reuse the `_obo_email` shape; do not add a third copy:
  move it to one shared helper and import it in the three routers, or import it from `drafts.py`.
- `save_settings` itself is unchanged (Lakebase only).

### S4 — job reads (`run_ontology_materialize.py`)
1. Move the `workspace_id` resolution block (`:270-275`) up, before the first Settings-derived
   parse (it only needs `make_workspace_client`).
2. Before parsing: if `trigger == "nightly"` and the allowlist widget is empty, read the row with
   `mirror_read_sql` through `spark.sql` in `try/except` (as the `suppressions` read does), then
   `_overrides = resolve_nightly_settings({...widget values...}, row) or {}`.
3. Add `_w(name)` = `_overrides.get(name, dbutils.widgets.get(name))`, and switch every
   Settings-derived parse (the 16 keys) to `_w`. Infra widgets keep `dbutils.widgets.get`.
4. Log one line in all cases: `Nightly settings source` with `source=mirror|widgets`, `reason=`
   (`not_nightly` / `allowlist_given` / `no_row` / `read_failed` / `bad_row`), and `updated_at`
   when the mirror was used.
5. The notebook never writes `genie_ont_settings_mirror`, and no other job logic changes.

### S5 — tests, docs, VERIFY
Tests (all on pure seams, no I/O):
- **Wheel, resolver:** `on_demand` → None; nightly + allowlist given → None; nightly + empty + no
  row → None; bad JSON → None; valid row → all 16 keys; row with an empty allowlist → overrides with
  `"[]"` (the job then still records `skipped`, which is correct); a row missing newer keys →
  defaults, no raise.
- **Backend, parity (the drift guard):** for a non-default `OntologySettings`, capture `_launch`'s
  `job_parameters` with a fake `jobs.run_now`; assert
  `settings_to_widgets(settings.model_dump(mode="json"))` equals that dict minus the infra keys.
  P4 then cannot forward `external_context_*` on demand without also teaching the mirror.
- **Backend, write path:** the PUT calls `write_mirror` with the OBO email; a raising
  `write_mirror` still yields 200 and the saved body; `write_mirror`'s SQL contains no interpolated
  value (all `:name` params) and its source names `get_service_principal_client` and not
  `require_obo_workspace_client`.
- **Firewall:** no MERGE/INSERT/UPDATE targeting `genie_ont_settings_mirror` in
  `run_ontology_materialize.py` or `ontology/materialize.py`; the existing firewall tests pass unchanged.
- **DDL shape:** 15 tables, `SETTINGS_TABLES` in the union.

Docs in the same commit (MV-D9; `git add -f` under `docs/design/`):
- PRD Appendix D #1 and #2 → resolved (cite the new symbols by file:line); CUJ-11 status → shipped
  pending deploy-verify; CUJ-05 "Only on-demand runs use these values" → updated.
- MV-D109 driver: P5 row ✅ offline-green, its P5 spec points here; this driver's status table flipped.
- Playbook MV-D109 entry: one P5 sentence. Rules floor bump in BOTH parity-mirrored copies.

---

## GOAL PROMPT (paste verbatim into Goal Mode; under the 4000-character limit)

```text
Execute docs/design/ontology-nightly-settings-mirror-driver.md (MV-D109 P5: the nightly ontology scan honours Settings via a Delta settings mirror) on the `ontology` branch, then STOP.

Binding sections: "Grounded facts" (anchors), "Decisions" (MD-1..MD-7; do not re-decide), "Build steps" S1-S5. If any MD is still marked proposed, STOP and ask before building.

1. PLAN first (workspace READ-BEFORE-WRITE contract): files to touch, fresh quotes of the current code at each change site, every symbol with file:line. If an anchor drifted or a symbol is missing, STOP and report; never scaffold a stand-in.
2. Build S1-S4 only. Additive: no new job parameter, no new route or verb, Lakebase stays the source of truth, the job never writes the mirror table, the mirror write is SP with every value bound, a mirror failure never fails the Settings save, a job read failure falls through to today's `skipped`. Nightly maps exactly the 16 Settings-derived keys `_launch` forwards today; external_context_* stays off (P4 owns it). No new npm/py dependency.
3. S5 tests ship in the same commit, including the _launch parity test.
4. VERIFY: ./scripts/test.sh (report backend + GSO counts; bump the floor in BOTH parity-mirrored rule copies); frontend untouched (confirm by git status); genie_space_optimizer import path resolves inside this checkout and the sqlglot version is reported, via uv run --frozen --extra dev; ruff on touched files adds no new finding; repo-wide grep (paste output) shows zero writers of genie_ont_settings_mirror outside backend/ontology/services/settings_mirror.py and zero reads of it outside the notebook and the wheel seam; git status shows only intended changes (uv.lock, frontend/package-lock.json untouched; .env.deploy.* never staged).
5. Same commit: flip this driver's status rows and the MV-D109 driver's P5 row, close PRD Appendix D #1 and #2, update CUJ-11 and CUJ-05, add the playbook MV-D109 P5 sentence. docs/design/ is gitignored: git add -f.
6. Commit, do not push. STOP and hand over the "Deploy-verify gate" checklist.
```

---

## Deploy-verify gate (human, after offline-green)
`./scripts/deploy.sh --update` (profile from `.env.deploy`). Then:
1. In Settings, choose at least one catalog and Save. In SQL, `SELECT workspace_id, updated_by,
   updated_at FROM <catalog>.genie_space_optimizer.genie_ont_settings_mirror` shows one row with your
   email.
2. Jobs UI → `genie-ontology-materialize-job` → **Run now** with the defaults (that is
   `trigger=nightly`, `catalog_allowlist=[]`). The log shows `Nightly settings source source=mirror`,
   and the new `genie_ont_runs` row is `succeeded` with `scope_allowlist` = your saved catalogs (not
   `skipped`).
3. **Run now with different parameters**, `catalog_allowlist=["<other>"]`: the log shows
   `source=widgets reason=allowlist_given` and the run scopes to `<other>`.
4. Negative: delete the mirror row, Run now with defaults → `source=widgets reason=no_row`, the run
   is `skipped`, and the prior snapshot is untouched.
5. The next morning, Overview freshness reflects the 07:00 UTC run.

## Risks and how this design handles them
- **The job's `run_as` cannot read the SP-created table.** The job runs as the deploy identity, or as
  whoever `ontology_job_run_as` names. If that identity lacks SELECT, the read fails, the log says
  `read_failed`, and the run is `skipped` (today's behaviour). Gate step 2 catches it, and the fix
  is a `GRANT SELECT` (copy it into the log message).
- **Id mismatch (`"default"` fallback on one side).** The read misses, and the log says `no_row`. It
  is safe and visible.
- **Two workspaces sharing one metastore.** Each install's nightly now does real work, and each
  scopes to its own allowlist. With different allowlists the metastore-scoped MERGE would alternate
  the snapshot nightly. This was latent before (nightly did nothing). The notebook header already
  recommends a single scheduled runner per metastore; the deploy note must repeat it. Out of scope
  to enforce.
- **Stale mirror after a failed write.** The log prints `updated_at`; Lakebase is unaffected.

## Non-goals
Surfacing mirror-sync state in the UI (a P2-messages follow-on); external context on nightly (P4);
schedule or cadence controls; enforcing one runner per metastore; any change to on-demand runs.
