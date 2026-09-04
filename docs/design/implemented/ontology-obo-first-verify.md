# Ontology — deploy-and-verify runbook (MV-D49 re-grain + MV-D50 OBO-first)

Copy-paste runbook for the **one deploy pass** that verifies both shipped-but-never-verified
changes live: the **metastore re-grain (MV-D49)** and **OBO-first foundations (MV-D50)**.
Run it **after** Goal Mode merges MV-D50 and `./scripts/test.sh` + `npm run lint`/`tsc` are
green on the `ontology` branch. Everything here is the **human deploy-gate** — the agent
does not deploy.

## Locked decisions

| Knob | Value |
|---|---|
| `ontology_job_run_as` | **`prashanth.subrahmanyam@databricks.com`** (run the materialize job as me — bridge; SP later) |
| `read_identity` | **`obo`** (default — live reads use the admin viewer's identity; no app-SP system grant) |

## Environment (from `.env.deploy` / terminal 33)

```bash
PROFILE=fevm-serverless
APP=genie-workbench
CATALOG=serverless_stable_6t92c3_catalog
GSO_SCHEMA=serverless_stable_6t92c3_catalog.genie_space_optimizer
WAREHOUSE_ID=41cfe645e10807a4
RUN_AS=prashanth.subrahmanyam@databricks.com
```

> **Helper used below.** `q() { databricks api post /api/2.0/sql/statements --profile "$PROFILE" \`
> `  --json "{\"warehouse_id\":\"$WAREHOUSE_ID\",\"statement\":\"$1\",\"wait_timeout\":\"50s\"}"; }`
> (Paste that once so the `q '<SQL>'` calls below work. Or run the SQL in a SQL editor.)

---

## Step 0 — set the `run_as` identity, then deploy

`ontology_job_run_as` is a **complex** bundle var, which the CLI **cannot** set via `--var`
or `BUNDLE_VAR_` (those take strings only). The deploy wiring (this repo) reads a `.env.deploy`
key and writes it to the sanctioned `.databricks/bundle/app/variable-overrides.json` for you.
So you only set the env key:

```bash
# Already set in .env.deploy for this workspace:
#   GENIE_ONTOLOGY_JOB_RUN_AS_USER=prashanth.subrahmanyam@databricks.com
# (or GENIE_ONTOLOGY_JOB_RUN_AS_SP=<sp-app-id> to run as a granted SP; empty ⇒ deploy identity)
grep ONTOLOGY_JOB_RUN_AS .env.deploy   # confirm it's present

./scripts/deploy.sh --update
```

Watch the config banner print `Ont run_as: prashanth.subrahmanyam@databricks.com` and, during
Step "Deploy", the line `✓ Ontology job run_as → variable-overrides.json (…)`. Deploy must
reach **7/7** (build → sync → configure → redeploy). Do **not** deploy while Goal Mode is
still editing the branch.

**Confirm the job now carries `run_as`:**

```bash
# The app target uses mode: development, so the ONE ontology job is named
# "[dev prashanth_subrahmanyam] genie-ontology-materialize-job" (id 529504954941024).
# That is NOT an orphan — it's the app-target job this deploy manages.
JOB_ID=529504954941024
databricks jobs list --profile "$PROFILE" -o json \
  | python3 -c "import sys,json;[print(j['job_id'],'|',j['settings']['name']) for j in json.load(sys.stdin) if 'ontology' in j['settings']['name'].lower()]"

# NOTE: this CLI (v1.10) takes JOB_ID positionally — NOT --job-id.
databricks jobs get "$JOB_ID" --profile "$PROFILE" -o json \
  | python3 -c "import sys,json;print(json.dumps(json.load(sys.stdin)['settings'].get('run_as')))"
# EXPECT: {"user_name": "prashanth.subrahmanyam@databricks.com"}
```

---

## Step 1 — run one materialization

Set a **catalog allowlist** first (Settings → Ontology), or pass it directly on the CLI run.
This job has **job-level parameters**, so a CLI run MUST use `--json` with `job_parameters`
(NOT `notebook_params`), and `run-now` **waits** for the run to terminate.

```bash
# Easiest: app UI → Ontology → "Refresh ontology".
# Or via CLI (runs under run_as = you), passing the allowlist inline:
databricks jobs run-now --profile "$PROFILE" -o json --json '{
  "job_id": 529504954941024,
  "job_parameters": {
    "catalog": "serverless_stable_6t92c3_catalog",
    "schema": "genie_space_optimizer",
    "catalog_allowlist": "[\"serverless_stable_6t92c3_catalog\"]",
    "trigger": "manual"
  }
}'
```

---

## Step 2 — verify MV-D49 (metastore grain)

```sql
-- 2a. Every genie_ont_* table LEADS with metastore_id (workspace_id demoted to provenance)
-- NOTE: Databricks information_schema.columns.ordinal_position is 0-BASED here (the first
-- column is position 0), so filter = 0, not 1.
SELECT table_name, column_name
FROM serverless_stable_6t92c3_catalog.information_schema.columns
WHERE table_schema = 'genie_space_optimizer'
  AND table_name LIKE 'genie_ont_%'
  AND ordinal_position = 0
ORDER BY table_name;
-- EXPECT: metastore_id on every table EXCEPT genie_ont_runs, whose PK is run_id
-- (metastore_id is its 2nd column, ordinal_position = 1, by design).

-- 2b. One metastore; workspace_id present only as provenance
SELECT count(*)                    AS rows,
       count(DISTINCT metastore_id) AS metastores,   -- EXPECT 1
       count(DISTINCT workspace_id) AS workspaces      -- provenance; >=1 is fine
FROM serverless_stable_6t92c3_catalog.genie_space_optimizer.genie_ont_taxonomy_snapshot;

-- 2c. Exactly one tree per metastore (not per workspace)
SELECT metastore_id, count(*) AS tree_rows
FROM serverless_stable_6t92c3_catalog.genie_space_optimizer.genie_ont_taxonomy_snapshot
GROUP BY metastore_id;                        -- EXPECT: a single row
```

Via the helper: `q 'SELECT table_name, column_name FROM serverless_stable_6t92c3_catalog.information_schema.columns WHERE table_schema=\'genie_space_optimizer\' AND table_name LIKE \'genie_ont_%\' AND ordinal_position=1 ORDER BY table_name'`

---

## Step 3 — verify MV-D50 (OBO-first; no app-SP system grant)

1. **Render as an admin, OBO:** open the app as `prashanth.subrahmanyam@databricks.com`,
   go to **Ontology**. The **Taxonomy** tab must render the domains/sub-domains and the
   **Tags** lens with member counts — with `read_identity` at its `obo` default.
2. **Banner reads "OBO (admin) or SP":** the two read tiers ("Usage / lineage / cost
   ranking", "Governed-tag graph") show the identity as **OBO (admin) or SP**, with the SP
   grants framed as an *optional upgrade*, and a **copy button on every tier** (including
   green ones).
3. **Prove OBO is carrying it (optional, the strong check):** confirm the app SP does **not**
   have the system grant, yet the page still rendered. Read the SP id from the banner (or
   `GET /api/ontology/preflight` → any `grants[]` line has it substituted), then:

```sql
-- Replace <app-sp> with the app service principal id from the banner:
SHOW GRANTS `<app-sp>` ON TABLE system.tags.governed_tags;   -- EXPECT: no SELECT for the SP
```

   If the taxonomy rendered while the SP has no SELECT here, OBO is doing the work.

---

## Step 4 — verify `run_as` on the actual run

```bash
# Primary: the run's task ran under your identity.
databricks jobs list-runs --job-id 529504954941024 --limit 3 --profile "$PROFILE" -o json \
  | python3 -c "import sys,json;[print(r['run_id'], r.get('state',{}).get('result_state'),'|',r.get('run_as_user_name')) for r in json.load(sys.stdin).get('runs',[])]"
```

```sql
-- Secondary (audit has minutes of latency): run_by = Jobs SP, run_as = you (normal user run_as)
SELECT event_time, service_name, action_name,
       identity_metadata.run_by  AS run_by,
       identity_metadata.run_as  AS run_as
FROM system.access.audit
WHERE identity_metadata.run_as = 'prashanth.subrahmanyam@databricks.com'
  AND event_date >= current_date() - 1
ORDER BY event_time DESC
LIMIT 20;                                     -- EXPECT: rows for the materialize reads
```

---

## Step 5 — idempotency (optional, proves the metastore-scoped MERGE)

```bash
databricks jobs run-now --profile "$PROFILE" -o json --json '{"job_id":529504954941024,"job_parameters":{"catalog":"serverless_stable_6t92c3_catalog","schema":"genie_space_optimizer","catalog_allowlist":"[\"serverless_stable_6t92c3_catalog\"]","trigger":"manual"}}'   # 2nd run
```

```sql
-- Row counts must be UNCHANGED after the second run (no duplicate Domain rows)
SELECT count(*) AS domains
FROM serverless_stable_6t92c3_catalog.genie_space_optimizer.genie_ont_domains;
```

---

## Green = done

- **MV-D49:** 2a all `metastore_id`; 2b `metastores = 1`; 2c one tree row.
- **MV-D50:** taxonomy renders OBO as admin; banner shows "OBO (admin) or SP" + copy button;
  (optional) SP has no `system.tags.governed_tags` SELECT yet the page rendered.
- **run_as:** job `run_as.user_name = prashanth.subrahmanyam@databricks.com`; audit shows it.
- **Idempotency:** second run leaves row counts unchanged.

Then hand off **Prompt 17f** (`ontology-phase3c-{build,driver}.md`) — build-ready on this grain.

## If something's red

- **Materialize job fails `[CANNOT_DETERMINE_TYPE]` at `createDataFrame`:** known blocker on
  serverless (Spark Connect) — `SparkSnapshotWriter.upsert_run`/`merge` call
  `spark.createDataFrame(rows)` with no explicit schema, and an all-`None` column can't be
  inferred. Fix = pass the target Delta table's schema (and coerce ISO-string timestamps to
  `datetime`). Offline tests don't exercise real Spark Connect, so this only surfaces live.
- **Taxonomy empty:** catalog allowlist unset (Settings) — the scan queries nothing by design.
- **Tier "blocked" under OBO:** your account lacks metastore-wide governed-tag read → the
  mirror is *partial, not wrong*; grant yourself the reads or accept partial for now.
- **`run_as` missing on the job:** the override didn't get written — confirm
  `GENIE_ONTOLOGY_JOB_RUN_AS_USER` is in `.env.deploy`, that the deploy printed
  `✓ Ontology job run_as → variable-overrides.json`, and that
  `.databricks/bundle/app/variable-overrides.json` contains `ontology_job_run_as`; re-deploy.
- **Rollback:** `git revert` the MV-D50 merge and `./scripts/deploy.sh --update`; the grain
  (MV-D49) is independent and stays intact.
