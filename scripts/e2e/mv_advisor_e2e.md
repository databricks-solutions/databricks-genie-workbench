# Metric View Advisor — end-to-end runbook (Prompt 15)

Live, opt-in E2E for the MV advisor. Runs the **real** FastAPI route code
in-process against a **real** Databricks workspace, driving the four scenarios
A–D (including D's BYO leg).

- **Suite:** `tests/e2e/test_mv_advisor_e2e.py` (fixtures in `tests/e2e/conftest.py`).
- **Marker:** `e2e` (also `slow`). Registered in root `pyproject.toml`.
- **Not** in the offline testpaths — `./scripts/test.sh` never collects it, so the
  637 + 1452 offline baseline is untouched. It is opt-in only.

## Invocation model (why there is no server here)

The routes run in-process via FastAPI `TestClient`. Identity is injected exactly
as Databricks Apps does it: the real `OBOAuthMiddleware` reads
`x-forwarded-access-token` and calls `set_obo_user_token` inside the request
context, so both the router and the services layer see the OBO client. The
`no-local-server` rule exists because OBO / Lakebase / serving aren't available
locally — here OBO is a real token, the MV routes read through **Statement
Execution** (not Lakebase), and nothing serves HTTP. So the rule is satisfied for
the reason it exists. (A deployed-app PAT can't reliably populate
`x-forwarded-access-token`, which is why option B was rejected.)

## Rate discipline (hard workspace limit)

Native benchmark eval is capped at **~20 questions/min**. The suite is
**serialized** by a process-wide lock and **refuses to run under `pytest-xdist`**
(import-time guard). Every scenario is marked `slow`. **Do not** add `-n` / xdist
to "save time" — you'll blow the ceiling and get throttled failures, not speed.

## Prerequisites (dev workspace)

Per the playbook "Before you start": the TPC-H samples catalog (`samples.tpch`),
a scratch schema you own (e.g. `main.mv_advisor_dev`), a small Genie Agent with
10–15 benchmark questions, and a **second** test identity that lacks
`CREATE TABLE` / `USE SCHEMA` on the scratch schema.

## Configuration — what each value is and where it comes from

Every variable is env-gated. A missing one **skips** the tests it gates with a
message naming the variable and the scenario — e.g.
`skipped: MV_E2E_LOWPRIV_TOKEN unset (Scenario B)` — never a bare skip count. A
credential that is *set but invalid* fails the gate loudly (a bad token surfaces
as a clear gate error, not a mid-scenario 401).

| Variable | What it is | Where it comes from |
|---|---|---|
| `DATABRICKS_HOST` | Workspace URL | Your dev workspace |
| `DATABRICKS_TOKEN` | PAT for the **signed-in user** | `databricks auth token` / a PAT; this identity is `created_by` for Scenario C |
| `GSO_CATALOG` | GSO Delta catalog | `.env.deploy` (written by `scripts/install.sh`) or the deployed app's `app.yaml` |
| `GSO_SCHEMA` | GSO Delta schema (default `genie_space_optimizer`) | `.env.deploy` / `app.yaml` |
| `GSO_WAREHOUSE_ID` (or `SQL_WAREHOUSE_ID`) | SQL warehouse for Delta reads + DDL | `.env.deploy` / `app.yaml` |
| `GSO_JOB_ID` | The optimization job id | `.env.deploy` / `app.yaml`. **Tier 1 needs it present only as the `_is_configured` gate — suggest never triggers it.** |
| `MV_E2E_SUGGEST_SPACE_ID` | A never-optimized space **with** curated SQL | You create it by hand (recipe below) |
| `MV_E2E_EMPTY_SPACE_ID` | A genuinely bare space (no curated SQL, no history) | You create it by hand (recipe below) |
| `MV_E2E_SPACE_ID` | An eval-capable space with 10–15 benchmark questions | Playbook "Before you start" step 3 |
| `MV_E2E_SCRATCH_CATALOG` / `MV_E2E_SCRATCH_SCHEMA` | The consented scratch schema you own | You own it (e.g. `main` / `mv_advisor_dev`) |
| `MV_E2E_LOWPRIV_TOKEN` | PAT for a user deliberately lacking `USE SCHEMA`/`CREATE TABLE` on the scratch schema | A second workspace user (playbook step 3) |

### Recipes for the two Scenario-D spaces

- **Curated (`MV_E2E_SUGGEST_SPACE_ID`):** create a Genie Agent over `samples.tpch`
  and add 2–3 example question/SQL pairs whose SQL contains aggregate measures
  (e.g. `SELECT SUM(l_extendedprice * (1 - l_discount)) ...`) in
  `example_question_sqls` (or `sql_snippets.measures`). **Do not** run
  optimization on it — the advisor reads the curated corpus with no run present.
- **Bare (`MV_E2E_EMPTY_SPACE_ID`):** create a Genie Agent with a single table and
  **no** example SQLs, **no** `sql_snippets`, and never query it. No parseable SQL
  and no history is exactly the EMPTY-with-a-reason case a customer demo hits first.

## Tiers — cheapest first

Assemble config incrementally; each tier is a meaningful checkpoint on its own.

### Tier 1 — Scenario D (no job triggered, no eval budget)

Needs: `DATABRICKS_HOST/TOKEN`, `GSO_CATALOG/GSO_SCHEMA`, a warehouse id,
`GSO_JOB_ID` (gate only), the two D space ids, and — for the BYO leg — the scratch
schema. Proves: suggest **COMPLETE** on curated SQL, suggest **EMPTY-with-reason**
on a bare space, and the BYO cheap path (register → `USER_CREATED`, drop **refused
409**, route 10 reports provenance).

```bash
uv run --frozen --extra dev pytest -m e2e tests/e2e -k scenario_d -v
```

### Tier 2 — Scenarios A and B (adds `GSO_JOB_ID` as a real job + `MV_E2E_LOWPRIV_TOKEN`)

Needs Tier 1 plus `MV_E2E_SPACE_ID` and `MV_E2E_LOWPRIV_TOKEN`. Proves: a
`suggest_only` run completes with ≥1 candidate, a parseable/structurally-valid
DDL artifact, DDL + GRANT on the UI endpoints, and no MV created (A); and the
denied-permission **INSUFFICIENT** probe plus the auto-downgrade run that creates
nothing and records a `downgrade_reason` (B). These trigger real optimization
runs (bounded with `max_attempts=1`).

```bash
uv run --frozen --extra dev pytest -m e2e tests/e2e -k "scenario_a or scenario_b" -v
```

### Tier 3 — Scenario C (full consent chain; the eval spender)

Needs Tier 2 plus `MV_E2E_SCRATCH_CATALOG/SCHEMA` and the primary identity
holding `USE SCHEMA` + `CREATE TABLE` on the scratch schema. Proves the whole
two-run create-and-attach-with-lift chain (MV-D1): approve → probe SUFFICIENT →
create under the user's identity in the consented schema → `Type: METRIC_VIEW` →
YAML validates → mv_attach patch → mv_lift eval to DONE with both eval run ids →
`lift_report` + audit rows. This is the only scenario that spends the extra
mv_lift subset-eval budget.

```bash
uv run --frozen --extra dev pytest -m e2e tests/e2e -k scenario_c -v
```

### Everything (once all config is assembled)

```bash
uv run --frozen --extra dev pytest -m e2e tests/e2e -v
```

## Teardown

Scenario C detaches/reverts via the run and **the test drops the scratch MV**
(registered as a finalizer). Scenario D-BYO **drops its view manually** in the
test — the app must never drop a `USER_CREATED` object. Finalizers run LIFO even
on failure. If a run is interrupted, manually `DROP VIEW IF EXISTS` any
`mv_e2e_*` view left in the scratch schema.

## Manual UI smoke checklist (do these in the deployed app, ≤10 items)

1. **Consent panel** — enabling create-and-attach shows the schema-scoped consent
   with the target catalog/schema and a materialize checkbox that is *separate*.
2. **Denial banner** — as the low-priv user, the probe surfaces INSUFFICIENT with
   the missing privilege and a copy-ready GRANT; the run falls back to suggest_only.
3. **DDL panel** — the suggest_only output shows the metric-view DDL and the
   `GRANT SELECT ON VIEW … TO` line, both copyable.
4. **Created-object panel** — after create_and_attach, the object shows
   `created_by` = you, status, and the lift report (pre/post accuracy).
5. **Drop affordance (OBO_CREATED)** — an app-created, DETACHED object offers Drop.
6. **Drop affordance (USER_CREATED)** — a registered BYO view shows the
   `USER_CREATED` badge and **no** Drop button (route 10 provenance; Prompt 14.1).
7. **Semantic model graph** — the space's tables/measures/joins render with the
   governance ladder and coverage lens.
8. **IQ Scan panel — populated** — the curated space's suggest surfaces proposal
   cards with evidence chips.
9. **IQ Scan panel — empty** — the bare space shows the honest empty state (what
   was read, what would change the answer), not an error toast.
10. **Downgrade transparency** — a downgraded run shows the `downgrade_reason`
    and that nothing was created.
