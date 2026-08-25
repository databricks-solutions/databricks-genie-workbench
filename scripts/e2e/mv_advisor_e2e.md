# Metric View Advisor — end-to-end runbook (Prompt 15)

Live, opt-in E2E for the MV advisor. Runs the **real** FastAPI route code
in-process against a **real** Databricks workspace, driving the four scenarios
A–D (including D's BYO leg).

- **Suite:** `tests/e2e/test_mv_advisor_e2e.py` (fixtures in `tests/e2e/conftest.py`).
- **Marker:** `e2e` (also `slow`). Registered in root `pyproject.toml`.
- **Not** in the offline testpaths — `./scripts/test.sh` never collects it, so the
  638 + 1461 offline baseline is untouched. It is opt-in only.

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
a scratch schema you own (e.g. `main.mv_advisor_dev`), and a small Genie Agent
with 10–15 benchmark questions. **No second identity is required** — the
simplified Scenario B denies the *primary* identity by pointing it at a
read-only schema (`samples.tpch` by default) it cannot create in, rather than
minting a low-privilege token.

## Configuration — what each value is and where it comes from

Every variable is env-gated. A missing one **skips** the tests it gates with a
message naming the variable and the scenario — e.g.
`skipped: MV_E2E_SPACE_ID unset (Scenario B)` — never a bare skip count. A
credential that is *set but invalid* fails the gate loudly (a bad token surfaces
as a clear gate error, not a mid-scenario 401).

| Variable | What it is | Where it comes from |
|---|---|---|
| `DATABRICKS_HOST` | Workspace URL | Your dev workspace (or the host of the `.env.deploy` `GENIE_DEPLOY_PROFILE` CLI profile) |
| `DATABRICKS_TOKEN` | PAT for the **signed-in user** | `databricks auth token` / a PAT; this identity is `created_by` for Scenario C |
| `GSO_CATALOG` | GSO Delta catalog | `.env.deploy` — as the `GENIE_CATALOG` key (remapped); or the deployed app's `app.yaml` as `GSO_CATALOG` |
| `GSO_SCHEMA` | GSO Delta schema (default `genie_space_optimizer`) | Not in `.env.deploy` — default it, or read `GSO_SCHEMA` from `app.yaml` |
| `GSO_WAREHOUSE_ID` (or `SQL_WAREHOUSE_ID`) | SQL warehouse for Delta reads + DDL | `.env.deploy` — as the `GENIE_WAREHOUSE_ID` key (remapped); or `app.yaml` as `GSO_WAREHOUSE_ID` |
| `GSO_JOB_ID` | The optimization job id | **Not** in `.env.deploy`. Read it from `app.yaml`, or resolve from the deployed job (`genie-workbench-gso-optimization-job`). **Tier 1 needs it present only as the `_is_configured` gate — suggest never triggers it.** |
| `MV_E2E_SUGGEST_SPACE_ID` | A never-optimized space **with** curated SQL | You create it by hand (recipe below) |
| `MV_E2E_EMPTY_SPACE_ID` | A genuinely bare space (no curated SQL, no history) | You create it by hand (recipe below) |
| `MV_E2E_SPACE_ID` | An eval-capable space with 10–15 benchmark questions | You create it by hand (Tier 2 prep; recorded in the run record) |
| `MV_E2E_SCRATCH_CATALOG` / `MV_E2E_SCRATCH_SCHEMA` | The consented scratch schema you own | You own it (e.g. `main` / `mv_advisor_dev`) |
| `MV_E2E_DENIED_CATALOG` / `MV_E2E_DENIED_SCHEMA` | A schema the **primary** identity cannot create in (Scenario B) | **Optional** — defaults to the read-only `samples` / `tpch` every workspace ships. Override only if samples is absent or writable. |

### Recipes for the two Scenario-D spaces

- **Curated (`MV_E2E_SUGGEST_SPACE_ID`):** create a Genie Agent over `samples.tpch`
  and add 2–3 example question/SQL pairs whose SQL contains aggregate measures
  (e.g. `SELECT SUM(l_extendedprice * (1 - l_discount)) ...`) in
  `example_question_sqls` (or `sql_snippets.measures`). **Do not** run
  optimization on it — the advisor reads the curated corpus with no run present.
- **Bare (`MV_E2E_EMPTY_SPACE_ID`):** create a Genie Agent with a single table and
  **no** example SQLs, **no** `sql_snippets`, and never query it. No parseable SQL
  and no history is exactly the EMPTY-with-a-reason case a customer demo hits first.
- **Eval-capable (`MV_E2E_SPACE_ID`, Tier 2+):** create a Genie Agent over
  `samples.tpch` with 10–15 benchmark questions, each with exactly one SQL answer,
  and at least one literal-bearing measure (an arithmetic constant *and* a
  predicate literal such as `o_orderstatus = 'F'`, to exercise both halves of
  MV-D29). **Grant the app service principal `CAN_MANAGE` on this space — and on
  any fixture space a real job run will touch.** Tiers 2 and 3 launch the real
  optimization job, which runs as the **SP**, not your OBO identity, so the SP
  must be able to read the space. Tier 1's in-process OBO masks this: Scenario D
  never triggers the job, so a missing SP grant only surfaces the first time a
  real job runs — as an `INTERNAL_ERROR` job failure with a `PermissionDenied`
  reading the space, not as a config error you can catch locally.

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

### Tier 2 — Scenarios A and B (adds `GSO_JOB_ID` as a real job + `MV_E2E_SPACE_ID`)

Needs Tier 1 plus `MV_E2E_SPACE_ID` (an eval-capable space). Scenario B needs no
extra identity or token — it denies the **primary** identity by pointing the
probe at `MV_E2E_DENIED_CATALOG/SCHEMA` (default `samples.tpch`, read-only).
Proves: a `suggest_only` run completes with ≥1 candidate, a
parseable/structurally-valid DDL artifact, DDL + GRANT on the UI endpoints, and
no MV created (A); and the denied-permission probe (**INSUFFICIENT** on
not-granted, or **UNKNOWN** on unreadable — both downgrade) plus the
auto-downgrade run that creates nothing and records a `downgrade_reason` (B).
These trigger real optimization runs (bounded with `max_attempts=1`).

> **Scenario B tradeoff.** Denying the primary identity on a read-only schema
> covers what matters — the INSUFFICIENT/UNKNOWN verdict and the downgrade — but
> it does **not** exercise the SP-fallback path a genuinely low-privilege OBO
> token would (where the probe reads succeed but the create is denied at a
> different layer). That path is pinned **offline** by the backend unit suite;
> the live tier verifies the boundary and the downgrade only.

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

> **Redeploy before Tier 3 — it validates the *deployed* app, not your tree.**
> Scenarios A/C run approve → create → attach against the deployed app over
> Statement Execution, so a Tier 3 run measures whatever code is currently
> deployed. Any prompt that changes approval or generation semantics (e.g.
> Prompt 15.3 changed the proposal grain to view-grained bundles, bundle-grained
> approval, multi-measure YAML through `generate`, and fan-out on reject) must be
> **redeployed first**, or Tier 3 spends the branch's one eval budget validating
> code you have already replaced. The same rule already governs the `scenario_d`
> rerun; it applies to Scenario C for the same reason.

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
2. **Denial banner** — probing a schema you cannot create in (e.g. `samples.tpch`)
   surfaces INSUFFICIENT/UNKNOWN with the missing privilege and a copy-ready GRANT;
   the run falls back to suggest_only.
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

## Run record

Live results against the dev workspace. Config is recorded by **name only** —
no hosts, tokens, or other values. A failure is recorded verbatim and mirrored
as a gap-report row in `docs/design/mv-advisor-gap-report.md` (MV-D9).

### 2026-08-24 — Tier 1 (Scenario D)

**Workspace / identity:** `fevm-serverless` profile, primary identity
`prashanth.subrahmanyam@databricks.com` (OAuth token exported in-shell only;
`current_user.me()` credential gate passed).

**Config present (names only):** `DATABRICKS_HOST`, `DATABRICKS_TOKEN`,
`GSO_CATALOG`, `GSO_SCHEMA`, `GSO_WAREHOUSE_ID`, `GSO_JOB_ID` (config gate only —
never triggered), `MV_E2E_SUGGEST_SPACE_ID`, `MV_E2E_EMPTY_SPACE_ID`,
`MV_E2E_SCRATCH_CATALOG`, `MV_E2E_SCRATCH_SCHEMA`.

**Env-provenance finding (setup):** `.env.deploy` on this machine uses `GENIE_*`
keys (`GENIE_CATALOG`, `GENIE_WAREHOUSE_ID`, `GENIE_DEPLOY_PROFILE`), **not** the
`GSO_*` / `DATABRICKS_*` names the config table names. Mapping used:
`GSO_CATALOG ← GENIE_CATALOG`, `GSO_WAREHOUSE_ID ← GENIE_WAREHOUSE_ID`,
host+token ← the `GENIE_DEPLOY_PROFILE` CLI profile; `GSO_SCHEMA` defaulted to
`genie_space_optimizer`; `GSO_JOB_ID` is not in `.env.deploy` and was resolved
from the deployed job (`genie-workbench-gso-optimization-job`). The runbook's
env-provenance table should say "from `.env.deploy`'s `GENIE_*` keys (remapped)"
rather than imply the `GSO_*` names live there verbatim.

**Fixtures created via API (exact config, since the assertions depend on it):**
- `MV_E2E_SUGGEST_SPACE_ID = 01f1a02f907314728c3fc05d5118b516` — title
  `mv-e2e-suggest-curated`, parent `/Users/prashanth.subrahmanyam@databricks.com`,
  one table `samples.tpch.lineitem`, **4** `instructions.example_question_sqls`
  each aggregating the same measure
  `SUM(l_extendedprice * (1 - l_discount)) AS discounted_revenue` (total, by
  ship mode, by return flag, by line status) plus **1**
  `instructions.sql_snippets.measures` entry
  `SUM(lineitem.l_extendedprice * (1 - lineitem.l_discount))`. Never optimized.
  The repeated measure is deliberate: the advisor is recurrence-ranked, so one
  fingerprint recurring across four curated statements clears candidate emission.
- `MV_E2E_EMPTY_SPACE_ID = 01f1a02f90151e9abc8b8a8914707dab` — title
  `mv-e2e-empty-bare`, one table `samples.tpch.region`, **no**
  `example_question_sqls`, **no** `sql_snippets`, no benchmarks, never queried.
- Scratch schema `serverless_stable_6t92c3_catalog.mv_advisor_e2e` created for
  the BYO leg (empty; no views leaked — see teardown).

**Command:** `uv run --frozen --extra dev pytest -m e2e tests/e2e -k scenario_d -v`
(171.34s; serialized; xdist refused by design).

**Results (2 passed, 1 failed):**

| Scenario D leg | Test | Result |
|---|---|---|
| suggest COMPLETE on curated SQL | `test_scenario_d_suggest_with_curated_sql` | **PASS** — status `COMPLETE`, ≥1 proposal, each proposal carries `evidence`. |
| suggest EMPTY-with-reason on a bare space | `test_scenario_d_suggest_empty_with_reason` | **PASS** — HTTP 200, status `SKIPPED`, non-empty `skip_reason`, `proposals == []`, `error is None`. Not a 500, not silence. |
| BYO register → `USER_CREATED`, drop refused 409, route-10 provenance | `test_scenario_d_byo_register_refuse_and_provenance` | **FAIL** — blocked at setup; see verbatim below. The register / 409 / provenance assertions were **not reached**. |

**Verbatim failure (`test_scenario_d_byo_register_refuse_and_provenance`):**

```
    suggest_run = suggest["run_id"]
    ddl_resp = api_primary.get(f"/api/auto-optimize/runs/{suggest_run}/mv-ddl")
>   assert ddl_resp.status_code == 200, ddl_resp.text
E   AssertionError: {"detail":"No metric view DDL artifact for this run."}
E   assert 404 == 200
E    +  where 404 = <Response [404 Not Found]>.status_code

tests/e2e/test_mv_advisor_e2e.py:153: AssertionError
```

**Root cause (characterized, not fixed — a finding per the ground rules):** the
space-scoped suggest route writes a *born-terminal sentinel advice run* and
persists proposals (each `MvProposal` carries `proposed_object`, the rendered
body), but it does **not** write a run-scoped `mv_candidate_ddl` artifact. The
`GET /runs/{run_id}/mv-ddl` endpoint reads exactly that artifact
(`_load_latest_artifact(run_id, "mv_candidate_ddl")`), which only the full
optimization/create path emits — so it 404s for a suggest-only run. The BYO test
sources its manually-created view's DDL from that endpoint, so its precondition
is incompatible with the suggest path. The fix belongs in the suite (source the
BYO DDL from `proposal.proposed_object`, or drive the BYO leg from a real run),
or in the route (have suggest persist a candidate-DDL artifact for its sentinel
run) — a design call for review, not made in this run.

**Eval budget spent:** none. Tier 1 triggers no optimization job and no native
benchmark eval; `GSO_JOB_ID` was a config gate only.

**Teardown confirmed:** the BYO test failed *before* its `CREATE VIEW` step, so
the app created nothing and the test's finalizer had nothing to drop. Verified
directly: `SHOW VIEWS IN serverless_stable_6t92c3_catalog.mv_advisor_e2e`
returned `[]`. The two Genie spaces and the empty scratch schema are left in
place for Tier 2/3 reuse (ids above); delete them when the suite is retired.

**Retries:** none (the failure is deterministic, not transient).

**Manual UI smoke checklist:** not yet run — it is a human step against the
deployed app (browser). Pending the reviewer.

### 2026-08-24 — Tier 1 rerun (Scenario D), after the Prompt 15.1 DDL fix

**Same workspace / identity / D fixtures as the Tier 1 entry above** (`fevm-serverless`
profile; `MV_E2E_SUGGEST_SPACE_ID`/`MV_E2E_EMPTY_SPACE_ID`/scratch ids unchanged;
`GENIE_*` remap; `GSO_JOB_ID` resolved from the deployed job; config gate only).

**Command:** `uv run --frozen --extra dev pytest -m e2e tests/e2e -k scenario_d -v`
(170.06s; serialized; xdist refused).

**Results (2 passed, 1 failed — 2/3):**

| Scenario D leg | Test | Result |
|---|---|---|
| suggest COMPLETE on curated SQL | `test_scenario_d_suggest_with_curated_sql` | **PASS** (unchanged). |
| suggest EMPTY-with-reason on a bare space | `test_scenario_d_suggest_empty_with_reason` | **PASS** (unchanged). |
| BYO register → `USER_CREATED`, drop 409, provenance | `test_scenario_d_byo_register_refuse_and_provenance` | **FAIL — new blocker.** The 404 is GONE (the leg cleared `assert ddl_resp.status_code == 200` and received `yaml_text`, confirming the Prompt 15.1 fallback works live); it then failed one step later at the manual `CREATE VIEW`. |

**What the rerun proved about Prompt 15.1.** The fix works: `GET /runs/{id}/mv-ddl`
now returns 200 for a suggest-only sentinel run and serves the candidate row's
`yaml_text`. The prior 404 (`tests/…:153`) is resolved; the leg advanced to
`tests/…:162`.

**New finding (recorded, not fixed) — masked numeric literal in the rendered expr:**

```
E   RuntimeError: SQL warehouse query failed:
E   [INVALID_IDENTIFIER] The unquoted identifier n-l_discount is invalid and must be back quoted as: `n-l_discount`.
E   == SQL of METRIC VIEW …mv_e2e_byo [measures.`measure_l_discount_l_extendedprice`.expr] …
```

The dumped `yaml_text` (read from `genie_opt_mv_candidates`) shows the culprit —
the curated measure `SUM(l_extendedprice * (1 - l_discount))` was persisted as:

```
measures:
  - name: measure_l_discount_l_extendedprice
    expr: sum(l_extendedprice * (?n - l_discount))
```

The numeric literal `1` was masked to a placeholder `?n` upstream (a
literal-normalization/parameterization step), so the served "copy-ready" DDL is
not executable for any literal-bearing measure. Mirrored as a gap-report row
(MV-D9, 2026-08-24 Tier 1 rerun). This is a product defect in the measure-expr
rendering, orthogonal to Prompt 15.1 (whose read route faithfully served what was
stored) and to the Scenario-B simplification.

**Eval budget spent:** none (Scenario D triggers no job / no native eval).

**Teardown confirmed:** the BYO test failed *before* a successful CREATE (the
CREATE was rejected), so nothing was created; the finalizer's `DROP VIEW IF
EXISTS` was a no-op. D fixtures left in place for reuse.

**Retries:** one targeted rerun of the BYO leg alone (`-k byo`) to capture the
full traceback — same deterministic failure, not transient.

### 2026-08-24 — Scenario B simplification (no live run)

Scenario B was simplified to drop the second low-privilege identity. It now denies
the **primary** identity by probing a schema it cannot create in
(`MV_E2E_DENIED_CATALOG/SCHEMA`, default the read-only `samples.tpch`), and the
probe assertion accepts `verdict in {INSUFFICIENT, UNKNOWN}` — denied and
unreadable are indistinguishable at the UC boundary (the MV-D13 reasoning), and
`_verdict` returns INSUFFICIENT only on DENIED while UNKNOWN short-circuits
(`mv_entitlement.py:400-402`). The downgrade assertion stays strict: `verify()`
treats anything short of SUFFICIENT as a downgrade, so the run-half test holds for
either verdict. Fixtures `lowpriv_token`/`lowpriv_email`/`api_lowpriv` and the
`MV_E2E_LOWPRIV_TOKEN` variable are retired.

**Tradeoff (recorded).** This covers the denial verdict and the downgrade, but
does **not** exercise live the SP-fallback detection a genuinely low-privilege
OBO token would (probe reads succeed, create denied at a different layer). That
path stays pinned **offline** by the backend unit suite. If the live UNKNOWN
outcome is what a future Tier-2 run observes, record it there — it is information
about the workspace's `samples` grants, not a defect.

**MV_E2E_SPACE_ID (Tier 2 prep):** not yet built. The rerun finding above (numeric
literals mask to `?n` in the rendered measure expr) directly shapes how the
benchmark corpus's measures should be authored, so the build is deferred pending
the reviewer's call on whether to fix the masking first or author literal-free
measures around it. **Resolved 2026-08-24 by Prompt 15.2 (MV-D29) — see the next
entry; the masking is fixed, so `MV_E2E_SPACE_ID` measures may be authored
literal-bearing.** **Built 2026-08-24 (Tier 2 prep entry below).**

### 2026-08-24 — Tier 1 re-rerun (Scenario D), after Prompt 15.2 (MV-D29)

**Same workspace / identity / D fixtures as the entries above** (`fevm-serverless`
profile; `MV_E2E_SUGGEST_SPACE_ID`/`MV_E2E_EMPTY_SPACE_ID`/scratch ids unchanged;
`GENIE_*` remap; `GSO_JOB_ID` config gate only). Offline baseline moved to
638 backend + **1461** GSO (+9 GSO at Prompt 15.2).

**Command:** `uv run --frozen --extra dev pytest -m e2e tests/e2e -k scenario_d -v`
(206.90s; serialized; xdist refused).

**Results (3 passed — 3/3, the exit criterion):**

| Scenario D leg | Test | Result |
|---|---|---|
| suggest COMPLETE on curated SQL | `test_scenario_d_suggest_with_curated_sql` | **PASS** (unchanged). |
| suggest EMPTY-with-reason on a bare space | `test_scenario_d_suggest_empty_with_reason` | **PASS** (unchanged). |
| BYO register → `USER_CREATED`, drop 409, provenance | `test_scenario_d_byo_register_refuse_and_provenance` | **PASS — the `?n` blocker is gone.** The manual `CREATE VIEW … WITH METRICS LANGUAGE YAML` that failed the prior rerun with `INVALID_IDENTIFIER` now executes: the served DDL carries `SUM(source.l_extendedprice * (1 - source.l_discount))` (literal preserved) instead of `(?n - l_discount)`. Register → `USER_CREATED`, drop refused 409, and route 10's provenance assertion all hold. |

**What this proved about Prompt 15.2.** MV-D29's render source works end to end
against a live warehouse: the advisor now renders from `representative_expr`
(literal-preserving), the `LeakageOracle` gate did not drop the structural-constant
measure, and the copy-ready DDL is executable. The gap-report row
(2026-08-24 Tier 1 rerun) is resolved; the "copy-ready DDL" claim is no longer
qualified for literal-bearing measures.

**Eval budget spent:** none (Scenario D triggers no job / no native eval).

**Teardown confirmed:** the BYO leg now creates a real view; the finalizer's
`DROP VIEW IF EXISTS` in the scratch schema removed it (the test asserts the drop
route refuses and drops manually — the app never dropped it). D fixtures left in
place for reuse.

**Retries:** none — a single clean 3/3 run.

### 2026-08-24 — MV_E2E_SPACE_ID built (Tier 2 prep)

**Workspace / identity:** `fevm-serverless` profile, primary identity
`prashanth.subrahmanyam@databricks.com`. Same `GENIE_*→GSO_*` env remap and
`GSO_JOB_ID` resolution as the Tier 1 entries.

**Fixture created via API** (`POST /api/2.0/genie/spaces`,
`warehouse_id=41cfe645e10807a4`, parent
`/Users/prashanth.subrahmanyam@databricks.com`; serialized_space 8015 bytes):

- `MV_E2E_SPACE_ID = 01f1a04003201e38b6f66cddc8d7d0ee` — title
  `mv-e2e-tier2-tpch`, two tables `samples.tpch.lineitem` + `samples.tpch.orders`
  (with a join_spec `lineitem.l_orderkey = orders.o_orderkey`,
  `FROM_RELATIONSHIP_TYPE_MANY_TO_ONE`).

**Corpus authored to exercise both MV-D29 halves** (config recorded by shape, so
the assertions are reproducible):

- **12 benchmark questions**, each with exactly one SQL answer (for iteration-0
  eval → the in-job advisor corpus).
  - **Arithmetic-constant recurring measure** `SUM(l_extendedprice * (1 -
    l_discount))` across **six distinct** benchmark questions (Q1 by return
    flag/line status, Q2 by ship mode, Q3 by ship year, Q4 top-10 parts, Q5 large
    quantity, Q6 finished-orders join). Each answer is deliberately **multi-clause**
    (GROUP BY dims / JOIN / WHERE) so the ~40-char representative is a small
    fraction of the answer text → low char-3-gram Jaccard vs the benchmark-only
    LeakageOracle → the candidate is expected to **survive** the gate and render
    executable DDL with the `(1 - l_discount)` literal preserved.
  - **Predicate literal `o_orderstatus = 'F'`** in a `WHERE` clause (Q6) — the
    reviewer's exact example — exercises the erasure that keeps the measure
    fingerprint stable (the literal is not in the measure expr, so Q6's discounted
    revenue buckets with Q1-Q5).
  - **Predicate-literal CASE measure** `SUM(CASE WHEN o_orderstatus = 'F' THEN
    o_totalprice ELSE 0 END)` across **two distinct** questions (Q7 total, Q8 by
    priority). This is the literal-bearing measure whose representative carries
    `'F'`; Q7's answer is short/measure-dominated so the representative gate has a
    candidate whose text overlaps the benchmark corpus. Whether the gate fires
    (`candidates_dropped_for_leakage ≥ 1`) is a **reported Tier-2 outcome**, not an
    assumption — if it does not fire, that is a gap row, not a silent fix.
  - Q9-Q12 are single-occurrence measures (`AVG(o_totalprice)`, `COUNT(*)` by
    status, `SUM(l_quantity)`, `COUNT(DISTINCT o_custkey)`) — realism; they should
    not clear recurrence, confirming provenance breadth (not raw count) is what
    makes a candidate.
- **Curated corpus** for the advisor (MV-D17 up-weight, feeds recurrence
  alongside the generated half): **4** `example_question_sqls` (three discounted-
  revenue slices distinct from the benchmarks — monthly trend, by return flag, air
  shipments — plus one finished/open CASE comparison) and **2**
  `sql_snippets.measures` (`Discounted Revenue` = `SUM(lineitem.l_extendedprice *
  (1 - lineitem.l_discount))`; `Finished Order Value` = the CASE measure).
- One `text_instructions` PURPOSE block; three `sample_questions`.

**Not yet run:** Tier 2 (`-k "scenario_a or scenario_b"`, `max_attempts=1`) — the
next entry records the live A/B results. This entry records only the fixture
config so the run is reproducible.

### 2026-08-25 — Tier 2 (Scenarios A + B), against redeployed Prompt 15.3 (`e65cc5a6`)

**Code under test.** Committed `e65cc5a6` (Prompt 15.3 — view-grained bundles,
per-measure suppression fan-out, justified EMPTY / MV-D30), **redeployed** to the
`fevm-serverless-stable` app before this run (the runbook's own redeploy rule —
15.3 changed the proposal grain and generation semantics, so a Tier that
validates the deployed app must run against the redeployed code). An earlier Tier
2 attempt on 2026-08-25 (job `893007930871503`, then `run2`) is **VOID** — it ran
mid-edit against a tree where `TABLE_MV_SUPPRESSIONS` was not yet importable, so
`mv_state` failed on import and the result was a stale-tree artifact, not an A/B
signal. This entry is the clean re-run after commit + redeploy.

**Workspace / identity.** `fevm-serverless-stable-6t92c3.cloud.databricks.com`,
primary identity `prashanth.subrahmanyam@databricks.com` (PAT; `current_user.me()`
gate passed). GSO `serverless_stable_6t92c3_catalog.genie_space_optimizer`,
warehouse `41cfe645e10807a4`.

**Config present (names only):** Tier 1 config plus `MV_E2E_SPACE_ID =
01f1a04003201e38b6f66cddc8d7d0ee` (the Tier 2-prep `mv-e2e-tier2-tpch` fixture,
built in the entry above). `GSO_JOB_ID` is now a **real trigger** (not a gate).
Scenario B denies the primary identity via `MV_E2E_DENIED_CATALOG/SCHEMA` default
`samples.tpch`. The app service principal holds `CAN_MANAGE` on the fixture space
(granted after the first real job surfaced the missing-SP-grant `PermissionDenied`
— see the fixture recipe note).

**Command:** `uv run --frozen --extra dev pytest -m e2e tests/e2e -k "scenario_a
or scenario_b" -v -s` (3094.49s = **51m34s**; serialized; xdist refused).

**Results (1 passed, 2 failed):**

| Scenario | Test | Result |
|---|---|---|
| A — `suggest_only` run serves DDL + GRANT, creates no MV | `test_scenario_a_suggest_only` | **FAIL.** The optimization run reached `SUCCESS` and persisted ≥1 candidate (both asserts passed), but `GET /runs/{run_id}/mv-ddl` returned **404**. Verbatim below. |
| B — denied-permission probe verdict | `test_scenario_b_probe_insufficient` | **PASS** — the probe on the read-only denied schema returns a non-SUFFICIENT verdict, as designed. |
| B — auto-downgrade run creates nothing, records a reason | `test_scenario_b_run_auto_downgrades` | **FAIL.** The downgraded run reached `SUCCESS` and created **no** MV (that assert passed), but `downgrade_reason` was `None` on `/runs/{run_id}/mv-created`. Verbatim below. |

**Verbatim failure — `test_scenario_a_suggest_only`:**

```
    ddl_resp = api_primary.get(f"/api/auto-optimize/runs/{run_id}/mv-ddl")
>       assert ddl_resp.status_code == 200, ddl_resp.text
E       AssertionError: {"detail":"No metric view DDL artifact for this run."}
E       assert 404 == 200
E        +  where 404 = <Response [404 Not Found]>.status_code

tests/e2e/test_mv_advisor_e2e.py:227: AssertionError
```

**Verbatim failure — `test_scenario_b_run_auto_downgrades`:**

```
        created = api_primary.get(f"/api/auto-optimize/runs/{run_id}/mv-created").json()
        assert created["created"] == [], "a downgraded run must create NO metric view"
>       assert created["downgrade_reason"], "downgrade left no downgrade_reason on the run"
E       AssertionError: downgrade left no downgrade_reason on the run
E       assert None

tests/e2e/test_mv_advisor_e2e.py:332: AssertionError
```

**Characterization (findings, not fixed — per the ground rules). Both mirrored as
gap-report rows (MV-D9) in `docs/design/mv-advisor-gap-report.md`.**

- **Scenario A — the in-job twin of the Prompt 15.1 finding, reborn at the view
  grain.** The exact `404 {"detail":"No metric view DDL artifact for this run."}`
  that 15.1 resolved for the *standalone suggest* path has reappeared on the
  *in-job* `suggest_only` path. 15.1's read-side fallback (`get_mv_ddl` →
  candidate `yaml_text`) still works for Scenario D because `mv_suggest._persist`
  writes `yaml_text` unconditionally on `rendered.ok`; but Scenario A goes through
  the in-job engine, where **both** `write_ddl_artifact` and the candidate's
  `yaml_text` are gated on `rendered.ok` (`mv_advisor.py:1346-1348`). Prime
  suspect introduced by MV-D30: the view-grained **bundle** body does not render
  `ok` on the in-job path (or the bundle's rendered body is not propagated to the
  in-job persist), so neither the run artifact nor the fallback resolves and
  `/mv-ddl` 404s even though the candidate row exists. To confirm in the fix
  prompt: dump the Scenario-A candidate row's `yaml_text` and the bundle
  `rendered.ok` on the in-job path; do **not** assume — the unit suite is green
  because it covers the standalone `_persist` writer, not the in-job bundle
  render. This is a real 15.3 regression signal, not a fixture issue.

- **Scenario B — downgrade recorded no `downgrade_reason`.** The probe half is
  green (verdict is non-SUFFICIENT), and the run correctly created no MV, but the
  reason the create was abandoned is absent on `/runs/{run_id}/mv-created`. Two
  candidate loci, to be told apart in triage: (a) the downgrade path wrote no
  `downgrade_reason` (`mv_create.verify_consent` / `mv_entitlement.verify`), or
  (b) it was written but the `/mv-created` response does not surface it. This is
  the **first live Tier-2 observation of B's run-half** (the prior Tier 2 was
  VOID), so whether it is a 15.3 regression or pre-existing is **undetermined** —
  the create/downgrade path is largely untouched by MV-D30 (only the proposal
  grain changed), which points more toward pre-existing or environmental than a
  15.3 regression. Flag for the reviewer's triage; not fixed here.

**Eval budget spent.** Both scenarios triggered real optimization jobs
(`max_attempts=1`), which consume the workspace's bounded native benchmark eval
for the optimization loop's iteration-0. The **MV-lift subset eval is Tier 3
only** and was not spent here.

**Teardown.** Neither scenario creates a persisted UC object (A is `suggest_only`;
B downgraded and created nothing), so there was nothing to drop. The fixture space
and scratch schema are left in place for reuse.

**Retries:** none — this is the single clean post-redeploy run; both failures are
deterministic (the earlier VOID run is not counted as a retry, it tested a
different, stale tree).

**Manual UI smoke checklist:** pending — the reviewer runs it against the deployed
app after the 15.4 / 12c-Part-2 surfaces land, before Prompt 16.

**Tier 3:** held behind the reviewer's explicit go (and behind the redeploy rule),
unchanged by this run. The two findings above are triage input for that go, not a
blocker recorded as fixed.
