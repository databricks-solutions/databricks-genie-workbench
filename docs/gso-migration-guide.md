# GSO Integration — Migration Guide

> Step-by-step guide for integrating the Genie Space Optimizer (GSO) into the
> Genie Workbench. See [gso-architecture.md](gso-architecture.md) for the
> full architecture rationale.

---

## Prerequisites

Before starting, ensure the following are in place:

- [ ] GSO standalone app is deployed and functional in the target workspace
- [ ] GSO optimization Databricks Job exists and can run successfully
- [ ] GSO Delta tables are created and populated (at least one completed run)
- [ ] Genie Workbench is deployed with Lakebase configured
      (`LAKEBASE_HOST`, `LAKEBASE_INSTANCE_NAME` set in `app.yaml`)
- [ ] Both apps point to the **same Databricks workspace**
- [ ] GitHub CLI (`gh`) authenticated with access to both repos
- [ ] Databricks CLI authenticated with the target workspace profile

---

## Phase 1: Git Subtree Setup

**Goal:** Embed the GSO repository into the Workbench monorepo at
`packages/genie-space-optimizer/`.

### 1.1 Add the GSO remote

```bash
cd databricks-genie-workbench
git remote add gso https://github.com/prashsub/Genie_Space_Optimizer.git
```

Verify:

```bash
git remote -v
# Should show: gso  https://github.com/prashsub/Genie_Space_Optimizer.git (fetch)
#              gso  https://github.com/prashsub/Genie_Space_Optimizer.git (push)
```

### 1.2 Pull the subtree

```bash
git subtree add --prefix=packages/genie-space-optimizer gso main --squash
```

This creates the `packages/genie-space-optimizer/` directory with the full
GSO codebase as a single squashed commit.

### 1.3 Add as an editable Python dependency

The GSO package makes `databricks-connect` an optional extra (`[spark]`),
so the standard install does not pull it in. Append to `requirements.txt`:

```
-e ./packages/genie-space-optimizer
```

### 1.4 Configure `.databricksignore` for the GSO subtree

The GSO Python package needs to be synced to the workspace for imports to
work at runtime. Verify that `.databricksignore` does **not** exclude
`packages/` itself, but **does** exclude the GSO's build artifacts, test
files, and frontend assets that are not needed at runtime:

```
# GSO subtree — exclude non-runtime files
packages/genie-space-optimizer/node_modules/
packages/genie-space-optimizer/.build/
packages/genie-space-optimizer/.databricks/
packages/genie-space-optimizer/src/genie_space_optimizer/__dist__/
packages/genie-space-optimizer/browser-test-output/
packages/genie-space-optimizer/tests/
packages/genie-space-optimizer/.git/
```

### 1.5 Verify the import works

```bash
pip install -e ./packages/genie-space-optimizer
python -c "from genie_space_optimizer.integration import trigger_optimization; print('OK')"
```

### 1.6 Commit

```bash
git add -A
git commit -m "Add GSO as git subtree at packages/genie-space-optimizer"
```

---

## Phase 1.5: Bootstrap and Initialization

**Goal:** Ensure the GSO's Delta tables, UC grants, and optimization job
exist before the first trigger. These are normally handled by the GSO's
Databricks Asset Bundle deployment.

### 1.5.1 Deploy the GSO bundle (if not already deployed)

The GSO bundle creates the optimization job, Delta tables, and UC grants.
If the GSO standalone app is already deployed in the workspace, these
resources already exist and this step is a no-op.

```bash
cd packages/genie-space-optimizer
databricks bundle deploy -t dev --profile fevm-prashanth
```

This creates:
- The optimization Databricks Job (note the job ID for `GSO_JOB_ID`)
- Delta tables in the GSO catalog/schema (via the first job run)
- UC grants for the app's service principal

### 1.5.2 Record the GSO job ID

After bundle deploy, note the job ID:

```bash
databricks jobs list --profile fevm-prashanth | grep genie-space-optimizer
```

Set this as `GSO_JOB_ID` in `app.yaml` (Phase 3.4).

### 1.5.3 Grant Workbench SP access to GSO resources

The Workbench app runs under its own service principal (different from the
GSO's SP). The Workbench SP needs:

```sql
-- Grant access to GSO Delta tables
GRANT USE CATALOG ON CATALOG <GSO_CATALOG> TO `<workbench-sp>`;
GRANT USE SCHEMA ON SCHEMA <GSO_CATALOG>.<GSO_SCHEMA> TO `<workbench-sp>`;
GRANT SELECT ON SCHEMA <GSO_CATALOG>.<GSO_SCHEMA> TO `<workbench-sp>`;
GRANT MODIFY ON SCHEMA <GSO_CATALOG>.<GSO_SCHEMA> TO `<workbench-sp>`;

-- Grant ability to trigger the optimization job
-- (done via Databricks Jobs UI: Jobs > [gso job] > Permissions > Add SP)
```

Alternatively, if both apps share the same SP (same Databricks App), no
additional grants are needed.

### 1.5.4 Verify bootstrap

```bash
# Verify Delta tables exist
databricks experimental aitools tools query \
  "SELECT count(*) FROM <catalog>.<schema>.genie_opt_runs" \
  --profile fevm-prashanth

# Verify job exists and is healthy
databricks jobs get --job-id <GSO_JOB_ID> --profile fevm-prashanth
```

---

## Phase 2: Synced Tables Setup

**Goal:** Replicate the 8 GSO Delta tables into the Workbench's Lakebase
instance as read-only synced tables.

### 2.1 Enable Change Data Feed

For Triggered sync mode, the source Delta tables must have Change Data Feed
(CDF) enabled. Run the following for each table:

```sql
ALTER TABLE <catalog>.<schema>.genie_opt_runs
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE <catalog>.<schema>.genie_opt_stages
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE <catalog>.<schema>.genie_opt_iterations
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE <catalog>.<schema>.genie_opt_patches
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE <catalog>.<schema>.genie_eval_asi_results
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE <catalog>.<schema>.genie_opt_provenance
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE <catalog>.<schema>.genie_opt_suggestions
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

ALTER TABLE <catalog>.<schema>.genie_opt_data_access_grants
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

### 2.2 Create the `gso` schema in Lakebase

The synced tables will be created in a `gso` schema. Create it first:

```sql
-- Connect to Lakebase via psql or SQL editor
CREATE SCHEMA IF NOT EXISTS gso;
```

### 2.3 Create synced tables

Run the following script (adjust `SOURCE_CATALOG`, `SOURCE_SCHEMA`, and
`LAKEBASE_INSTANCE` for your environment):

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (
    SyncedDatabaseTable,
    SyncedTableSpec,
    NewPipelineSpec,
    SyncedTableSchedulingPolicy,
)

w = WorkspaceClient()

SOURCE_CATALOG = "<your_catalog>"
SOURCE_SCHEMA = "genie_space_optimizer"
LAKEBASE_CATALOG = "<your_lakebase_catalog>"
LAKEBASE_SCHEMA = "gso"
STORAGE_CATALOG = SOURCE_CATALOG
STORAGE_SCHEMA = SOURCE_SCHEMA

TABLES = [
    ("genie_opt_runs",               ["run_id"]),
    ("genie_opt_stages",             ["run_id", "stage", "started_at"]),
    ("genie_opt_iterations",         ["run_id", "iteration", "eval_scope"]),
    ("genie_opt_patches",            ["run_id", "iteration", "lever", "patch_index"]),
    ("genie_eval_asi_results",       ["run_id", "iteration", "question_id", "judge"]),
    ("genie_opt_provenance",         ["run_id", "iteration", "lever", "question_id", "judge"]),
    ("genie_opt_suggestions",        ["suggestion_id"]),
    ("genie_opt_data_access_grants", ["grant_id"]),
]

for table_name, pk_cols in TABLES:
    print(f"Creating synced table for {table_name}...")
    synced = w.database.create_synced_database_table(
        SyncedDatabaseTable(
            name=f"{LAKEBASE_CATALOG}.{LAKEBASE_SCHEMA}.{table_name}",
            spec=SyncedTableSpec(
                source_table_full_name=f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table_name}",
                primary_key_columns=pk_cols,
                scheduling_policy=SyncedTableSchedulingPolicy.TRIGGERED,
                new_pipeline_spec=NewPipelineSpec(
                    storage_catalog=STORAGE_CATALOG,
                    storage_schema=STORAGE_SCHEMA,
                ),
            ),
        )
    )
    print(f"  Created: {synced.name}")

print("Done. Wait for initial snapshots to complete.")
```

### 2.4 Verify sync

Check that data appears in Lakebase:

```sql
-- Connect to Lakebase via psql or SQL editor
SELECT count(*) FROM gso.genie_opt_runs;
SELECT count(*) FROM gso.genie_opt_stages;
```

### 2.5 Configure sync triggers (optional)

For near-real-time updates during active optimization runs, configure
**table update triggers** in Lakeflow Jobs:

1. Go to **Workflows > Create Job**.
2. Add a **Database Table Sync pipeline** task for each synced table's
   pipeline.
3. Under **Schedules & Triggers**, add a **Table update** trigger pointing
   to the source Delta table.
4. Save.

Alternatively, trigger syncs programmatically from within the GSO job tasks:

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
table = w.database.get_synced_database_table(
    name="<lakebase_catalog>.gso.genie_opt_runs"
)
pipeline_id = table.data_synchronization_status.pipeline_id
w.pipelines.start_update(pipeline_id=pipeline_id)
```

---

## Phase 3: Backend Integration

**Goal:** Create the thin FastAPI router and Lakebase read functions.

### 3.1 Create `backend/services/gso_lakebase.py`

Create the file with async read functions for each synced table. Each
function should:

1. Check `_lakebase_available` and `_pool` from `backend.services.lakebase`
2. Return `None` or `[]` if unavailable
3. Use `async with _pool.acquire() as conn:` for queries
4. Return plain dicts (not Pydantic models)

Functions needed:

| Function | Query pattern |
|----------|--------------|
| `load_gso_run(run_id)` | `SELECT * FROM gso.genie_opt_runs WHERE run_id = $1` |
| `load_gso_runs_for_space(space_id)` | `SELECT * FROM gso.genie_opt_runs WHERE space_id = $1 ORDER BY started_at DESC` |
| `load_gso_stages(run_id)` | `SELECT * FROM gso.genie_opt_stages WHERE run_id = $1 ORDER BY started_at` |
| `load_gso_iterations(run_id)` | `SELECT * FROM gso.genie_opt_iterations WHERE run_id = $1 ORDER BY iteration` |
| `load_gso_patches(run_id)` | `SELECT * FROM gso.genie_opt_patches WHERE run_id = $1 ORDER BY iteration, lever, patch_index` |
| `load_gso_asi_results(run_id, iteration)` | `SELECT * FROM gso.genie_eval_asi_results WHERE run_id = $1 AND iteration = $2` |
| `load_gso_suggestions(run_id)` | `SELECT * FROM gso.genie_opt_suggestions WHERE run_id = $1 ORDER BY created_at` |

### 3.2 Create `backend/routers/auto_optimize.py`

Create the router with 10 endpoints. See
[gso-architecture.md](gso-architecture.md) Section 3.1 for the full
endpoint table and Section 9 for the auth bridge pattern.

The router imports from the GSO **integration module** (see
[gso-upstream-rfc.md](gso-upstream-rfc.md) RFC-1):

```python
from genie_space_optimizer.integration import (
    trigger_optimization,
    apply_optimization,
    discard_optimization,
    get_lever_info,
    IntegrationConfig,
)
from backend.services.auth import get_workspace_client, get_service_principal_client
from backend.services import gso_lakebase
```

Key design decisions:

1. **Trigger** calls `trigger_optimization()` — uses warehouse-first mode
   (no Spark Connect). Passes OBO + SP clients.
2. **Apply** calls `apply_optimization()` — OBO client only (no SP needed).
3. **Discard** calls `discard_optimization()` — both OBO + SP clients
   (SP is used by `_genie_client()` for the rollback REST call).
4. **Monitoring endpoints** (run detail, status, iterations, ASI results,
   run history) read from Lakebase via `gso_lakebase` functions. No GSO
   imports needed for reads.
5. **Levers** calls `get_lever_info()` — returns metadata for levers 1-5.
6. **Health** checks whether `GSO_CATALOG` and `GSO_JOB_ID` are configured.

### 3.3 Mount the router in `backend/main.py`

Add the import and `app.include_router()` call after the existing routers.

### 3.4 Add GSO env vars to `app.yaml`

Add the following under the `env:` section:

```yaml
  # ---------------------------------------------------------------------------
  # Auto-Optimize (GSO engine)
  # ---------------------------------------------------------------------------
  - name: GSO_CATALOG
    value: ""
  - name: GSO_SCHEMA
    value: "genie_space_optimizer"
  - name: GSO_JOB_ID
    value: ""
  - name: GSO_WAREHOUSE_ID
    valueFrom: sql-warehouse
```

### 3.5 Test (after deployment)

The Workbench cannot run locally — it depends on Databricks-managed
resources (OBO auth, Lakebase, serving endpoints). Test after deploying
to the target workspace (see Phase 5):

```bash
# Health check (should return configured: true after setting env vars)
curl https://<app-url>/api/auto-optimize/health

# List levers (no auth needed for read-only)
curl https://<app-url>/api/auto-optimize/levers

# Verify import works in the deployed environment
# (check app logs for import errors at startup)
databricks apps logs genie-workbench --profile fevm-prashanth
```

---

## Phase 4: Frontend Integration

**Goal:** Add the "Auto-Optimize" tab to the SpaceDetail page with
configuration and monitoring sub-views.

### 4.1 Three-Layer Frontend Design

The Auto-Optimize UI has three levels of depth. See
[gso-architecture.md](gso-architecture.md) Section 11 for the full
rationale.

| Layer | Component | Purpose |
|-------|-----------|---------|
| Layer 1 | `AutoOptimizeTab.tsx` | Trigger + history (in SpaceDetail tab) |
| Layer 2 | `RunDetailView.tsx` | Benchmark evaluation page (question list, SQL comparison) |
| Layer 3 | `PipelineDetailsModal.tsx` | Full GSO pipeline transparency (gear icon) |

**Design intent:** Layer 2 is intentionally modeled after the Genie native
Benchmarks page (question list + SQL comparison), not the GSO's
pipeline-centric run detail page. The GSO's pipeline view is available as
Layer 3 for power users.

### 4.2 Install missing UI primitives

The Workbench currently has 8 shadcn/ui components. The Auto-Optimize tab
requires additional primitives:

```bash
cd frontend
npx shadcn@latest add checkbox table collapsible tooltip skeleton alert-dialog
```

### 4.3 Create component directory

```
frontend/src/components/auto-optimize/
├── AutoOptimizeTab.tsx          # Layer 1: tab container
├── OptimizationConfig.tsx       # Layer 1: levers, apply mode, start
├── RunHistoryTable.tsx          # Layer 1: past runs table
├── RunDetailView.tsx            # Layer 2: benchmark eval page
├── QuestionList.tsx             # Layer 2: sidebar question list
├── QuestionDetail.tsx           # Layer 2: SQL comparison + results
├── PipelineDetailsModal.tsx     # Layer 3: shell (gear icon opens modal)
├── PipelineStepCard.tsx         # Layer 3: individual step card
└── ScoreSummary.tsx             # Shared: baseline vs optimized display
```

### 4.4 Layer 1: AutoOptimizeTab.tsx

State-driven container with three sub-views:

- **"configure"** — Shows `OptimizationConfig` (levers, apply mode, start
  button) and `RunHistoryTable` (past runs).
- **"monitoring"** — Shows active run status with score summary and
  auto-polling (5s interval). Shows "View Details" link.
- **"detail"** — Shows `RunDetailView` (Layer 2) for a selected run.

Transitions: configure → monitoring (after trigger), monitoring → detail
(user clicks "View Details"), detail → configure (user clicks back).

**Polling pattern:** Use `useEffect` with `setInterval` (5s) calling the
lightweight `/status` endpoint. Stop polling when status reaches a terminal
state: `CONVERGED`, `STALLED`, `MAX_ITERATIONS`, `FAILED`, `CANCELLED`,
`APPLIED`, `DISCARDED`. Clean up the interval on unmount.

**Graceful degradation:** On mount, call `GET /api/auto-optimize/health`.
If `configured: false`, show an informational card instead of the form.

### 4.5 Layer 1: OptimizationConfig.tsx

Reference the GSO's `spaces/$spaceId.tsx` (lines 89-96 for LEVERS, lines
433-557 for the config form). Elements:

- **Apply mode toggle**: "Config Only" (default) vs "Config + UC Write
  Backs" (disabled / coming soon). A third mode `"uc_artifact"` exists
  but is not yet exposed in the UI.
- **Lever checkboxes** (uses the `checkbox` UI primitive): 5 levers (1-5),
  all checked by default. Lever 0 ("Proactive Enrichment") always runs
  and is NOT shown as a checkbox.
  1. Tables & Columns
  2. Metric Views
  3. Table-Valued Functions
  4. Join Specifications
  5. Genie Space Instructions
- **Deploy target**: Optional collapsible text input.
- **Start button**: Calls `triggerAutoOptimize()`. Disabled when:
  - An active run already exists
  - No levers selected

### 4.6 Layer 2: RunDetailView.tsx (Benchmark Evaluation Page)

Designed to resemble the Genie native Benchmarks page. Opened by clicking
"View Details" on a run. Layout:

```
┌────────────────────────────────────────────────────────────┐
│  ← Back    Mar 9, 2026  ● Applied  92% (23/25)  [gear]   │
│                                                            │
│  ┌──────────────────┬─────────────────────────────────────┐│
│  │ Question list    │ Selected question detail             ││
│  │ (sidebar)        │                                     ││
│  │                  │ Question text                       ││
│  │ ✓ Question 1     │ Assessment: Good / Bad              ││
│  │ ✓ Question 2     │                                     ││
│  │ ✗ Question 3     │ ┌─────────────┬─────────────┐      ││
│  │ ✓ Question 4     │ │ Model SQL   │ Ground Truth│      ││
│  │ ...              │ │ SELECT ...  │ SELECT ...  │      ││
│  │                  │ └─────────────┴─────────────┘      ││
│  │                  │                                     ││
│  │                  │ Results table                       ││
│  └──────────────────┴─────────────────────────────────────┘│
└────────────────────────────────────────────────────────────┘
```

Data sources:
- `getAutoOptimizeRun(runId)` — run status and scores
- `getAutoOptimizeIterations(runId)` — evaluation results per question
- `getAutoOptimizeAsiResults(runId, iteration)` — per-judge verdicts

### 4.7 Layer 3: PipelineDetailsModal.tsx

Opened by clicking the gear icon in the Layer 2 header. Shows a modal or
side panel with the full GSO pipeline view:

- 6 pipeline step cards (status, duration, summary)
- Score summary (baseline → optimized)
- Apply / Discard buttons (when run is complete)

Built natively in the Workbench using `components/ui/*` primitives. Uses
the GSO subtree code as a reference for data shapes, but does not import
from it directly. Start simple (step cards + scores), add iteration
explorer and judge details incrementally.

### 4.8 Layer 1: RunHistoryTable.tsx

Table of past optimization runs for the current space (uses the `table`
UI primitive):

| Column | Source |
|--------|--------|
| Date | `started_at` |
| Status | `status` (badge) |
| Baseline | `best_accuracy` % |
| Optimized | `best_accuracy` % (latest iteration) |
| Action | "View Details" link |

Data: `getAutoOptimizeRunsForSpace(spaceId)` — calls
`GET /api/auto-optimize/spaces/{spaceId}/runs`.

### 4.9 Add API functions to `frontend/src/lib/api.ts`

Add 9 functions following the existing `fetchWithTimeout` pattern:

1. `triggerAutoOptimize(request)` — POST /trigger (use LONG_TIMEOUT)
2. `getAutoOptimizeRun(runId)` — GET /runs/{runId}
3. `getAutoOptimizeStatus(runId)` — GET /runs/{runId}/status
4. `getAutoOptimizeLevers()` — GET /levers
5. `applyAutoOptimize(runId)` — POST /runs/{runId}/apply (use LONG_TIMEOUT)
6. `discardAutoOptimize(runId)` — POST /runs/{runId}/discard
7. `getAutoOptimizeRunsForSpace(spaceId)` — GET /spaces/{spaceId}/runs
8. `getAutoOptimizeIterations(runId)` — GET /runs/{runId}/iterations
9. `getAutoOptimizeAsiResults(runId, iteration)` — GET /runs/{runId}/asi-results

### 4.10 Add types to `frontend/src/types/index.ts`

Add the GSO-specific TypeScript interfaces. See
[gso-architecture.md](gso-architecture.md) Section 4.3 for the full list.
Note: `apply_mode` supports three values: `"genie_config" | "uc_artifact" | "both"`.

### 4.11 Wire into SpaceDetail.tsx

1. Extend the `Tab` type union with `"auto-optimize"`.
2. Add the tab entry to the `tabs` array (between "optimize" and "history").
3. Add `{activeTab === "auto-optimize" && <AutoOptimizeTab spaceId={spaceId} />}`.

### 4.12 Test

1. Open the Workbench in a browser.
2. Navigate to a Genie Space detail page.
3. Click the "Auto-Optimize" tab.
4. Verify lever checkboxes appear (5 checkboxes, all checked).
5. Click "Start Optimization".
6. Verify the pipeline monitor appears with step cards.
7. Wait for completion, then test Apply / Discard.
8. Verify "GSO not configured" message when env vars are empty.

---

## Phase 5: Deploy and Validate

### 5.1 Build the frontend

```bash
cd frontend && npm install && npm run build && cd ..
```

### 5.2 Switch to the correct GitHub account

```bash
gh auth switch --user prashsub
```

### 5.3 Sync to workspace

```bash
databricks sync . /Workspace/Users/<email>/genie-workbench \
  --profile fevm-prashanth
```

### 5.4 Deploy

```bash
databricks apps deploy genie-workbench \
  --source-code-path /Workspace/Users/<email>/genie-workbench \
  --profile fevm-prashanth
```

### 5.5 Post-deployment configuration

1. **Set GSO env vars** in the Databricks Apps UI:
   - `GSO_CATALOG` → your Unity Catalog name
   - `GSO_SCHEMA` → `genie_space_optimizer`
   - `GSO_JOB_ID` → the optimization job ID

2. **Verify OAuth scopes** (same as existing):
   - `sql`, `dashboards.genie`, `serving.serving-endpoints`
   - `catalog.catalogs:read`, `catalog.schemas:read`, `catalog.tables:read`

3. **Verify SP permissions**:
   - CAN_MANAGE on target Genie Spaces
   - SELECT on data schemas
   - Can Query on LLM serving endpoint

### 5.6 Validation checklist

- [ ] Workbench loads without errors (existing tabs still work)
- [ ] "Auto-Optimize" tab appears in SpaceDetail
- [ ] Health endpoint returns `{"configured": true}` when env vars are set
- [ ] Health endpoint returns `{"configured": false}` when env vars are empty
- [ ] Lever checkboxes render with correct labels (5 levers, 1-5)
- [ ] "Start Optimization" triggers a Databricks Job
- [ ] Pipeline monitor shows step progress
- [ ] Status polls correctly (updates every 5s, stops on terminal status)
- [ ] Apply/Discard buttons appear after run completes
- [ ] Apply writes optimized config to the Genie Space
- [ ] Discard rolls back to pre-optimization state
- [ ] Score, Analysis, and Optimize tabs are unaffected
- [ ] No `databricks-connect` import errors in app logs

---

## Ongoing Maintenance

### Pushing GSO changes back to the standalone repo

```bash
git subtree push --prefix=packages/genie-space-optimizer gso <branch-name>
```

### Pulling upstream GSO updates

```bash
git subtree pull --prefix=packages/genie-space-optimizer gso main --squash
```

### Synced table architecture (same-schema with `_synced` suffix)

GSO synced tables now live **in the same catalog/schema** as the source Delta
tables, with a `_synced` suffix. For example:

| Source Table | Synced Table |
|---|---|
| `{CATALOG}.genie_space_optimizer.genie_opt_runs` | `{CATALOG}.genie_space_optimizer.genie_opt_runs_synced` |

In Lakebase PostgreSQL, these appear under the `genie_space_optimizer` schema:

```sql
SELECT * FROM "genie_space_optimizer"."genie_opt_runs_synced" WHERE run_id = $1;
```

**Why not a separate Lakebase catalog?** The Databricks SDK's
`create_synced_database_table` does not support Lakebase Autoscaling
project/branch fields — only the legacy `database_instance_name`. The raw REST
API has a proto3 serialization bug where `databaseProjectName`/`databaseBranchName`
require camelCase but `spec` fields require snake_case, and mixing both fails.
Creating synced tables via the Catalog Explorer UI (which uses the correct
internal API) works reliably.

**Creating synced tables:**

For each of the 8 source tables in `{CATALOG}.genie_space_optimizer`:

1. Open Catalog Explorer → navigate to the source table
2. Click **Create** → **Synced table**
3. Name: `{table_name}_synced` (same schema)
4. Database type: **Lakebase Serverless (Autoscaling)**
5. Project: `genie-workbench-db`, Branch: `production`
6. Sync mode: **Triggered**
7. Verify primary key detection, then create

Tables: `genie_opt_runs`, `genie_opt_stages`, `genie_opt_iterations`,
`genie_opt_patches`, `genie_eval_asi_results`, `genie_opt_provenance`,
`genie_opt_suggestions`, `genie_opt_data_access_grants`

Docs: https://docs.databricks.com/aws/en/oltp/projects/sync-tables

### Adding new GSO Delta tables

If the GSO adds new Delta tables in future versions:

1. Enable CDF on the new table.
2. Create a synced table via Catalog Explorer UI (same schema, `_synced` suffix).
3. Add a read function to `backend/services/gso_lakebase.py` (uses `_tbl()` helper).
4. Add/update the corresponding endpoint in `backend/routers/auto_optimize.py`.
5. Update frontend types and API functions if the new table surfaces in the UI.

### Monitoring synced table health

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
for table_name in ["genie_opt_runs", "genie_opt_stages", ...]:
    synced_name = f"<catalog>.genie_space_optimizer.{table_name}_synced"
    status = w.database.get_synced_database_table(name=synced_name)
    print(f"{table_name}: {status.data_synchronization_status.detailed_state}")
```
