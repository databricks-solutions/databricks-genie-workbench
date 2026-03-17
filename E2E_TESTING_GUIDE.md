# End-to-End Testing Guide (for LLM Agents)

This guide is written for an LLM agent that will execute a full end-to-end test of the Genie Space Optimizer. It describes every API call, UI interaction, expected response, failure mode, and diagnostic step. Follow the journeys sequentially -- each builds on the prior.

> **Related documentation:** [Deployment Guide](DEPLOYMENT.md) | [Architecture Overview](docs/genie-space-optimizer-design/02-architecture-overview.md) | [API Reference](docs/genie-space-optimizer-design/07-api-reference.md) | [Permissions Guide](docs/genie-space-optimizer-design/08-permissions-and-security.md) | [Troubleshooting](docs/genie-space-optimizer-design/appendices/B-troubleshooting.md) | [Full Documentation Index](docs/genie-space-optimizer-design/00-index.md)

---

## Table of Contents

1. [System Architecture](#1-system-architecture)
2. [Prerequisites and Verification](#2-prerequisites-and-verification)
3. [Journey 1: Dashboard Load](#3-journey-1-dashboard-load)
4. [Journey 2: Browse and Search Spaces](#4-journey-2-browse-and-search-spaces)
5. [Journey 3: Inspect a Space](#5-journey-3-inspect-a-space)
6. [Journey 4: Start an Optimization](#6-journey-4-start-an-optimization)
7. [Journey 5: Monitor a Running Pipeline](#7-journey-5-monitor-a-running-pipeline)
8. [Journey 6: Review Optimization Results](#8-journey-6-review-optimization-results)
9. [Journey 7: Apply or Discard Changes](#9-journey-7-apply-or-discard-changes)
10. [Journey 8: Post-Action Verification](#10-journey-8-post-action-verification)
11. [Journey 9: Navigation Paths](#11-journey-9-navigation-paths)
12. [Journey 10: Settings & Data Access](#12-journey-10-settings--data-access)
13. [Journey 11: Programmatic Trigger API](#13-journey-11-programmatic-trigger-api)
14. [Troubleshooting Databricks Job Failures](#14-troubleshooting-databricks-job-failures)
15. [Edge Cases and Failure Scenarios](#15-edge-cases-and-failure-scenarios)
16. [Issue Reporting Template](#16-issue-reporting-template)
17. [Full E2E Checklist](#17-full-e2e-checklist)

---

## 1. System Architecture

Understanding the architecture is critical for diagnosing failures. For a comprehensive architecture overview with mermaid diagrams, see [02 -- Architecture Overview](docs/genie-space-optimizer-design/02-architecture-overview.md).

### Components

```
Browser (React)  ──API──▶  FastAPI Backend  ──triggers──▶  Databricks Job (6-task DAG)
                            │                                │
                            ├─ OBO user token (if available) ├─ preflight
                            ├─ SP fallback (if not)          ├─ baseline_eval
                            ├─ Serverless Spark              ├─ enrichment
                            └─ Delta state tables            ├─ lever_loop
                                                             ├─ finalize
                                                             └─ deploy_check → deploy
```

### Authentication Flow

The app uses two clients:
- **Service Principal (SP)** -- `Dependencies.Client` -- always available, used for job submission and fallback
- **OBO User Client** -- `Dependencies.UserClient` -- uses `X-Forwarded-Access-Token` header injected by Databricks Apps runtime

When the OBO token is missing or lacks the `dashboards.genie` scope, the backend falls back to the SP client transparently. The function `_genie_client(ws, sp_ws)` tests the OBO token first, catches `PermissionDenied`, and returns the SP client.

### Job Launcher

The app does NOT look up a pre-deployed job by name. Instead, `job_launcher.py`:
1. Finds/builds the project wheel (with content hash embedded in the filename for cache busting)
2. Uploads wheel + all 6 job notebooks to `/Workspace/Shared/genie-space-optimizer/`
3. Cleans up stale `.whl` files from the workspace dist directory (`_cleanup_stale_wheels`)
4. Creates or updates a persistent job named `genie-space-optimizer-runner` (idempotent)
5. Submits the run via `jobs.run_now()` with widget parameters

At app startup, a `_WheelHealthCheck` logs the resolved wheel name and size for deploy verification.

### State Management

8 Delta tables in `{catalog}.{schema}`:
- `genie_opt_runs` -- one row per optimization run (status, scores, config snapshot)
- `genie_opt_stages` -- per-stage transitions (PREFLIGHT_STARTED, LEVER_1_EVAL_DONE, etc.)
- `genie_opt_iterations` -- per-iteration evaluation results (9 judges: 7 quality dimensions + response quality + arbiter)
- `genie_opt_patches` -- individual patches (type, lever, old/new values, rolled_back flag, provenance chain)
- `genie_eval_asi_results` -- failure assessments from LLM judges (with `mlflow_run_id` for trace linking)
- `genie_opt_provenance` -- end-to-end provenance linking patches to judge verdicts, clusters, and gate outcomes
- `genie_opt_queued_patches` -- high-risk patches pending human approval
- `genie_opt_flagged_questions` -- questions flagged for human review after exhausting automated approaches

### UC Permissions Grant

During `databricks bundle deploy`, the build step runs `resources/grant_app_uc_permissions.py` which:
1. Looks up the app's service principal
2. Grants `USE_CATALOG` on the catalog
3. Grants `USE_SCHEMA`, `CREATE_TABLE`, `CREATE_FUNCTION`, `CREATE_MODEL`, `SELECT`, `MODIFY`, `EXECUTE`, `MANAGE` on the schema
4. Verifies all grants were applied
5. On first deploy (app not yet created): logs a warning and returns 0 (idempotent)

---

## 2. Prerequisites and Verification

Before starting the test, verify each item programmatically.

### Deployment Check

```bash
# Confirm the app is deployed
databricks apps get genie-space-optimizer -o json

# Confirm the app is running (status should be "RUNNING")
databricks apps get genie-space-optimizer -o json | jq '.status'

# Get the app URL
databricks apps get genie-space-optimizer -o json | jq -r '.url'

# Verify the correct wheel is deployed (via Makefile)
make verify WAREHOUSE_ID=<warehouse-id>
```

### Wheel Health Check

After deployment, verify the app loaded the correct wheel by checking the startup logs:

```bash
databricks apps logs genie-space-optimizer -p <profile> | grep "App wheel:"
# Expected: "App wheel: genie_space_optimizer-x.y.z.whl (size=NNNNN bytes)"
```

If the wheel size is unexpectedly small or the log line is missing, redeploy:

```bash
make deploy WAREHOUSE_ID=<warehouse-id>
```

### SQL Warehouse Check

```bash
# Verify the warehouse is running
databricks warehouses get <warehouse_id> -p <profile> -o json | jq '.state'
# Expected: "RUNNING"
```

### Genie Space Availability

```bash
# List available Genie Spaces (requires dashboards.genie scope)
databricks api get /api/2.0/genie/spaces -p <profile> | jq '.spaces | length'
# Expected: >= 1
```

### API Health Check

```bash
# Check the app API is responding
curl -s https://<app-url>/api/genie/version | jq
# Expected: {"version": "x.y.z"}
```

### Delta Tables Check

```bash
# These tables are created automatically on first app startup
# If optimization has never run, tables may be empty
databricks api post /api/2.0/sql/statements \
  -p <profile> \
  --json '{"warehouse_id": "<wh_id>", "statement": "SHOW TABLES IN <catalog>.<schema> LIKE '\''genie_opt_*'\''"}' \
  | jq '.result.data_array'
```

---

## 3. Journey 1: Dashboard Load

### API Calls Made

When the dashboard loads, the frontend makes these parallel requests:

| Request | Endpoint | Expected Status |
|---------|----------|----------------|
| List spaces | `GET /api/genie/spaces` | 200 |
| Get activity | `GET /api/genie/activity` | 200 |
| Current user | `GET /api/genie/current-user` | 200 |

### What the Agent Should Verify

**`GET /api/genie/spaces`** returns:
```json
[
  {
    "id": "string (space UUID)",
    "name": "string",
    "description": "string",
    "tableCount": "int >= 0",
    "lastModified": "string (ISO timestamp or empty)",
    "qualityScore": "float | null"
  }
]
```

**`GET /api/genie/activity`** returns:
```json
[
  {
    "runId": "string (run UUID)",
    "spaceId": "string",
    "spaceName": "string",
    "status": "string (QUEUED|IN_PROGRESS|CONVERGED|STALLED|MAX_ITERATIONS|FAILED|APPLIED|DISCARDED)",
    "initiatedBy": "string (email or 'system')",
    "baselineScore": "float | null",
    "optimizedScore": "float | null",
    "timestamp": "string (ISO timestamp)"
  }
]
```

### UI Elements to Verify

| Element | Selector/Identifier | Expected Content |
|---------|---------------------|------------------|
| Header title | Text "Genie Space Optimizer" | Visible in dark header bar |
| Stats row | 3 cards with icons | Total Spaces count, Recent Runs count, Avg Quality Score |
| Search input | Placeholder "Search spaces..." | Empty, focusable |
| Space cards | Grid of clickable cards | One per space from API response |
| Activity table | Table below grid | One row per activity item (hidden if empty array) |
| User avatar | Top-right of header | Initials or fallback user icon |

### Failure Scenarios

| Scenario | API Response | UI Behavior | Root Cause |
|----------|-------------|-------------|------------|
| No spaces visible | `200 []` | "No Genie Spaces found." | SP lacks access to Genie Spaces, or no spaces exist |
| Genie API down | `502` with error detail | Red error alert "Failed to load dashboard" | Genie API unreachable or all auth failed |
| Delta tables missing | `200 []` for activity | Activity section hidden, all scores null | First deployment, no optimization runs yet |
| OBO fallback active | `200` with all spaces (unfiltered) | All spaces shown regardless of user permissions | OBO token missing -- SP used instead |
| NaN in quality scores | Scores scrubbed to null | "No score" badge on affected cards | Edge case in Delta data |

---

## 4. Journey 2: Browse and Search Spaces

### Test Actions

1. Verify all cards from `GET /api/genie/spaces` render
2. Type a search term that matches a space name → verify filtered grid
3. Type a search term that matches no spaces → verify "No spaces match your search."
4. Clear search → verify full grid restored

### Verification Points

- Search is client-side (no API call), debounced ~300ms
- Filters on `name` and `description` fields (case-insensitive)
- Quality score badge colors: green (>=80), orange (60-79), red (<60), gray outline ("No score") for null

---

## 5. Journey 3: Inspect a Space

### Navigation

Click a space card → navigates to `/spaces/{spaceId}`.

### API Call

`GET /api/genie/spaces/{space_id}` returns `SpaceDetail`:
```json
{
  "id": "string",
  "name": "string",
  "description": "string",
  "instructions": "string (may be multi-line, or empty)",
  "sampleQuestions": ["string"],
  "benchmarkQuestions": ["string (from Delta benchmark table, may be empty)"],
  "tables": [
    {"name": "catalog.schema.table", "catalog": "string", "schema_name": "string", "description": "string", "columnCount": 0}
  ],
  "joins": [
    {"leftTable": "string", "rightTable": "string", "relationshipType": "string|null", "joinColumns": ["left_col = right_col"]}
  ],
  "functions": [
    {"name": "catalog.schema.function", "catalog": "string", "schema_name": "string"}
  ],
  "optimizationHistory": [
    {"runId": "string", "status": "string", "baselineScore": "float|null", "optimizedScore": "float|null", "timestamp": "string"}
  ],
  "hasActiveRun": false
}
```

### UI Elements to Verify

| Element | Condition | Content |
|---------|-----------|---------|
| Back button | Always | Arrow icon + "Back", navigates to `/` |
| Title | Always | `name` from response |
| Description | Always | `description` from response |
| "Start Optimization" button | `hasActiveRun === false` | Red button with rocket icon |
| "Start Optimization" button | `hasActiveRun === true` | Button disabled or shows "Optimization in progress" |
| Instructions card | Always | `instructions` text or "No instructions configured." |
| Sample Questions card | Always | Bulleted list of `sampleQuestions` or "No sample questions configured." |
| Benchmark Questions card | `benchmarkQuestions.length > 0` | List of benchmark questions |
| Tables card | Always | Table with columns: Name, Catalog, Schema, Columns |
| Joins card | `joins.length > 0` | Table showing join relationships |
| Functions card | `functions.length > 0` | Table with columns: Name, Catalog, Schema |
| Optimization History card | `optimizationHistory.length > 0` | Table with clickable rows |

### Stale Run Reconciliation

When the space detail loads, the backend calls `_reconcile_active_runs()` which:
- Finds any runs in `QUEUED` or `IN_PROGRESS` status
- Checks the actual Databricks job state via `ws.jobs.get_run()`
- If the job is `TERMINATED`, `SKIPPED`, or `INTERNAL_ERROR` but the Delta status is still active: marks it `FAILED` with convergence reason like `job_terminated_without_state_update:failed`
- If a run has been `QUEUED` for >10 minutes with no `job_run_id`: marks it `FAILED` with `stale_queued_no_job_run`

This means: if you see a stuck "in progress" run, loading the space detail page should auto-fix it.

---

## 6. Journey 4: Start an Optimization

### Trigger

Click "Start Optimization" on the space detail page.

### API Call

`POST /api/genie/spaces/{space_id}/optimize?apply_mode=genie_config`

### What the Backend Does (in order)

1. Validates `apply_mode` is one of: `genie_config`, `uc_artifact`, `both`
2. Ensures Delta state tables exist (`ensure_optimization_tables`)
3. Loads existing runs for this space
4. Runs `_reconcile_active_runs()` to clean up stale runs
5. Checks for active runs → returns **409** if one exists
6. Looks up previous experiment name (for MLflow experiment reuse)
7. Generates a new `run_id` (UUID)
8. Fetches space config snapshot via best-available client
9. Prefetches UC metadata via OBO SQL (columns, tags, routines scoped to Genie space tables)
10. Creates `QUEUED` run in Delta with config snapshot and `triggered_by` (current user email)
11. Calls `submit_optimization()` which:
    - Finds/builds the project wheel
    - Uploads wheel + job notebooks to `/Workspace/Shared/genie-space-optimizer/`
    - Creates or updates persistent job `genie-space-optimizer-runner`
    - Calls `jobs.run_now()` with all parameters
12. Updates run to `IN_PROGRESS` with `job_run_id`
13. Returns `{runId, jobRunId, jobUrl}`

### Expected Response

```json
{
  "runId": "UUID string",
  "jobRunId": "integer as string",
  "jobUrl": "https://<host>/jobs/<job_id>?o=<workspace_id> (or null)"
}
```

### Failure Scenarios

| Status | Body | Root Cause | Resolution |
|--------|------|------------|------------|
| **400** | `"Unsupported apply_mode '...'"` | Invalid query parameter | Use `genie_config`, `uc_artifact`, or `both` |
| **409** | `"An optimization is already in progress for this space (run ...)"` | Another run is `QUEUED` or `IN_PROGRESS` | Wait for it to finish, or reconciliation will clean it up |
| **500** | `"Job submission failed: ..."` | Wheel not found, workspace permission denied, job creation failed | Check app logs; see [Job Troubleshooting](#12-troubleshooting-databricks-job-failures) |
| **502** | `"Could not fetch Genie spaces: ..."` | Genie API unreachable | Check workspace connectivity |

---

## 7. Journey 5: Monitor a Running Pipeline

### Navigation

After successful optimization start, browser navigates to `/runs/{runId}`.

### API Call (Polling)

`GET /api/genie/runs/{run_id}` -- polled every 5 seconds until terminal status.

### Expected Response Structure

```json
{
  "runId": "string",
  "spaceId": "string",
  "spaceName": "string",
  "status": "QUEUED|RUNNING|CONVERGED|STALLED|MAX_ITERATIONS|FAILED|CANCELLED|APPLIED|DISCARDED",
  "startedAt": "ISO timestamp",
  "completedAt": "ISO timestamp | null",
  "initiatedBy": "email string",
  "baselineScore": "float | null",
  "optimizedScore": "float | null",
  "steps": [
    {
      "stepNumber": 1,
      "name": "Configuration Analysis",
      "status": "pending|running|completed|failed",
      "durationSeconds": "float | null",
      "summary": "string | null"
    }
  ],
  "levers": [
    {
      "lever": 1,
      "name": "Tables & Columns",
      "status": "pending|running|evaluating|accepted|rolled_back|skipped|failed",
      "patchCount": 0,
      "scoreBefore": "float | null",
      "scoreAfter": "float | null",
      "scoreDelta": "float | null",
      "rollbackReason": "string | null"
    }
  ],
  "convergenceReason": "string | null"
}
```

### Pipeline Steps

| Step | Name | Maps to Job Task | Typical Duration |
|------|------|-----------------|-----------------|
| 1 | Configuration Analysis | `preflight` (first half) | 30s - 2min |
| 2 | Metadata Collection | `preflight` (second half) | 30s - 2min |
| 3 | Baseline Evaluation | `baseline_eval` | 5 - 20min |
| 3.5 | Prompt Matching Auto-Config | `lever_loop` (Stage 2.5, before levers) | 30s - 3min |
| 4 | Configuration Generation | `lever_loop` (Stage 2.5 + 5-lever iteration with tiered failure analysis) | 10 - 45min |
| 5 | Optimized Evaluation | `finalize` (2 repeatability runs + model promotion) | 5 - 15min |

### Lever Status Lifecycle

```
pending → running → evaluating → accepted (patch passed 3-gate evaluation)
                               → rolled_back (patch caused regression > noise-floor-adjusted threshold, default 10%)
                               → skipped (no applicable patches, or all failures filtered by arbiter)
                               → failed (exception during evaluation)
```

### Terminal Statuses (polling stops)

`CONVERGED`, `STALLED`, `MAX_ITERATIONS`, `FAILED`, `CANCELLED`, `APPLIED`, `DISCARDED`

### UI Behavior

| Status | UI Shows |
|--------|----------|
| `QUEUED` / `RUNNING` | Progress bar advancing, steps transitioning, 5s polling |
| `CONVERGED` | Score cards visible, "View Results" button at bottom |
| `STALLED` | Same as CONVERGED (score may not have improved much) |
| `MAX_ITERATIONS` | Same as CONVERGED (5 iterations ran, thresholds not met) |
| `FAILED` | Red destructive alert with `convergenceReason`, "Return to Space Detail" link |
| `CANCELLED` | Orange warning alert, "Return to Space Detail" link |

---

## 8. Journey 6: Review Optimization Results

### Navigation

Click "View Results" button → navigates to `/runs/{runId}/comparison`.

### API Call

`GET /api/genie/runs/{run_id}/comparison`

### Expected Response Structure

```json
{
  "runId": "string",
  "spaceId": "string",
  "spaceName": "string",
  "baselineScore": 72.5,
  "optimizedScore": 88.3,
  "improvementPct": 21.79,
  "perDimensionScores": [
    {"dimension": "schema_accuracy", "baseline": 70.0, "optimized": 90.0, "delta": 20.0}
  ],
  "original": {
    "instructions": "string",
    "sampleQuestions": ["string"],
    "tableDescriptions": [{"tableName": "string", "description": "string"}]
  },
  "optimized": {
    "instructions": "string",
    "sampleQuestions": ["string"],
    "tableDescriptions": [{"tableName": "string", "description": "string"}]
  }
}
```

### 9 Evaluation Judges (7 Quality Dimensions + Response Quality + Arbiter)

| Dimension Key | Display Name | Target Threshold |
|--------------|-------------|-----------------|
| `syntax_validity` | Syntax Validity | 98% |
| `schema_accuracy` | Schema Accuracy | 95% |
| `logical_accuracy` | Logical Accuracy | 90% |
| `semantic_equivalence` | Semantic Equivalence | 90% |
| `completeness` | Completeness | 90% |
| `response_quality` | Response Quality | N/A (advisory) |
| `result_correctness` | Result Correctness | 85% |
| `asset_routing` | Asset Routing | 95% |
| `arbiter` | Arbiter (tie-breaker) | N/A (advisory; verdicts: `genie_correct`, `ground_truth_correct`, `both_correct`, `neither_correct`) |

**Response quality** evaluates whether Genie's natural language analysis/explanation accurately describes the SQL query and answers the user's question. Returns `unknown` when no analysis text is available (Genie does not always include the text attachment), so this scorer never penalizes runs where the NL response is absent.

All judges (except `syntax_validity` and `asset_routing`) now return **structured JSON** with `failure_type`, `blame_set`, `counterfactual_fix`, `wrong_clause`, and `rationale` fields. These flow directly into ASI metadata and provenance without separate rationale-pattern extraction.

The **arbiter** judge does not contribute to the overall quality score but its verdicts drive three mechanisms: (1) benchmark correction before the lever loop (rewriting stale gold SQL for `genie_correct` verdicts), (2) hard failure filtering (excluding `genie_correct` and `both_correct` verdicts from the main failure cluster), and (3) tiered soft signal extraction (`genie_correct`/`both_correct` rows where individual judges still failed are clustered separately as best-practice signals for Levers 4 and 5).

### Config Diff Tabs

3 tabs: **Instructions**, **Sample Questions**, **Table Descriptions**.
- Left panel = original, right panel = optimized
- Red highlight = removed lines, green highlight = added lines

---

## 9. Journey 7: Apply or Discard Changes

### Apply

`POST /api/genie/runs/{run_id}/apply`

**Backend behavior:**
1. Verifies run is in terminal state (`CONVERGED`, `STALLED`, or `MAX_ITERATIONS`)
2. If `apply_mode` includes `uc_artifact` or `both`:
   - Loads patches from Delta where `scope in (uc_artifact, both)` and `rolled_back = false`
   - Converts to UC SQL statements (e.g., `ALTER TABLE ... ALTER COLUMN ... COMMENT '...'`)
   - Executes via OBO user's SQL warehouse connection
   - Records `UC_OBO_WRITE` stage in Delta
3. Updates run status to `APPLIED`

**Possible responses:**

| Status | Body | Cause |
|--------|------|-------|
| 200 | `{"status": "applied", "runId": "...", "message": "..."}` | Success |
| 400 | `"Cannot apply run in status ..."` | Run not in terminal state |
| 409 | `"Run already applied/discarded"` | Already actioned |
| 409 | `"Deferred UC writes require proven improvement"` | Baseline score >= optimized score |
| 500 | `"Deferred UC write execution failed: ..."` | OBO SQL execution error |

### Discard

`POST /api/genie/runs/{run_id}/discard`

**Backend behavior:**
1. Verifies run is not already `DISCARDED` or `APPLIED`
2. Loads original config snapshot from run record
3. Calls `rollback(apply_log, client, space_id)` to restore original Genie Space config
4. Updates run status to `DISCARDED` with `convergence_reason="user_discarded"`

**Possible responses:**

| Status | Body | Cause |
|--------|------|-------|
| 200 | `{"status": "discarded", "runId": "...", "message": "..."}` | Success |
| 409 | `"Run already ..."` | Already applied or discarded |

### UI Behavior

- Apply success → green toast "Optimization applied!" → redirect to `/`
- Discard success → info toast "Optimization discarded." → redirect to `/`
- Either failure → red error toast, stays on comparison page

---

## 10. Journey 8: Post-Action Verification

After apply or discard, verify:

1. **Dashboard activity table** shows the run with correct status (`APPLIED` or `DISCARDED`)
2. **Space quality score** updated on the space card
3. **Genie Space config** in workspace matches expected state:
   - After Apply: instructions/descriptions match "Optimized" column
   - After Discard: instructions/descriptions match "Original" column

---

## 11. Journey 9: Navigation Paths

| From | Action | Destination |
|------|--------|-------------|
| Dashboard | Click space card | `/spaces/{spaceId}` |
| Dashboard | Click activity row | `/runs/{runId}` |
| Dashboard | Click logo/title | `/` |
| Space Detail | Click back button | `/` |
| Space Detail | Click "Start Optimization" | `/runs/{newRunId}` |
| Space Detail | Click history row | `/runs/{runId}` |
| Pipeline View | Click back button | `/spaces/{spaceId}` |
| Pipeline View | Click "View Results" | `/runs/{runId}/comparison` |
| Pipeline View (failed) | Click "Return to Space Detail" | `/spaces/{spaceId}` |
| Comparison | Click back button | `/runs/{runId}` |
| Comparison | Apply (success) | `/` |
| Comparison | Discard (success) | `/` |

---

## 12. Journey 10: Settings & Data Access (Advisor-Only)

> **Reference:** For the complete permissions model, see [08 -- Permissions and Security](docs/genie-space-optimizer-design/08-permissions-and-security.md). For deployment configuration, see the [Deployment Guide](DEPLOYMENT.md).

### Navigation

Click the **gear icon** in the navbar → navigates to `/settings`.

### API Calls

| Request | Endpoint | Expected Status |
|---------|----------|----------------|
| Permission metadata (fast) | `GET /api/genie/settings/permissions?metadata_only=true` | 200 |
| Per-page access check (lazy) | `POST /api/genie/spaces/check-access` | 200 |

The Settings page is **advisor-only** -- it reads permissions and displays what's missing with copyable SQL commands and sharing instructions, but never executes GRANT/REVOKE on the user's behalf.

### `GET /api/genie/settings/permissions` Response

```json
{
  "spIdentity": {
    "displayName": "string",
    "applicationId": "string (UUID)"
  },
  "frameworkCatalog": "string (e.g. main)",
  "frameworkSchema": "string (e.g. genie_optimization)",
  "workspaceHost": "string (URL)",
  "jobUrl": "string | null",
  "schemaPermissions": [
    {
      "catalog": "string",
      "schema": "string",
      "hasRead": true,
      "hasWrite": false,
      "readGrantCommand": "GRANT USE CATALOG ON CATALOG ...",
      "writeGrantCommand": "GRANT MODIFY ON SCHEMA ..."
    }
  ],
  "spacePermissions": [
    {
      "spaceId": "string",
      "spaceName": "string",
      "spHasManage": false,
      "spGrantInstructions": "Open the Genie Space > Share > add ..."
    }
  ]
}
```

### `POST /api/genie/spaces/check-access` Request/Response

```json
// Request body
{"ids": ["space-id-1", "space-id-2"]}

// Response
[
  {"spaceId": "space-id-1", "accessLevel": "CAN_MANAGE"},
  {"spaceId": "space-id-2", "accessLevel": "CAN_EDIT"}
]
```

Access checks are performed **lazily per page** -- only the spaces visible on the current page are checked, not all spaces at once.

### Test Actions

1. Navigate to Settings page → verify SP identity card loads (display name + application ID)
2. Verify **Framework Resources** shows the correct `frameworkCatalog` and `frameworkSchema` matching deploy-time config
3. Verify per-space access badges load lazily (spinner → resolved badge) as you paginate
4. Verify copyable SQL `GRANT` commands appear for schemas with missing read/write access
5. Verify sharing instructions appear for spaces where SP lacks `CAN_MANAGE`
6. Verify **no** GRANT/REVOKE buttons exist -- the page is read-only (advisor model)
7. Filter by "Editable Only" → verify only spaces with CAN_MANAGE/CAN_EDIT remain visible

### What the Backend Does

**Permissions metadata (`GET`):**
1. Resolves the app's service principal from `Dependencies.Client`
2. Reads the deploy-time `catalog` and `schema_name` from `AppConfig` (environment variables injected by `patch_app_yml.py`)
3. Checks UC effective grants on each detected schema
4. Generates copyable SQL strings for missing grants
5. Returns the full permission dashboard as read-only data

**Access check (`POST`):**
1. Receives a list of space IDs (typically the current page)
2. Parallelizes access checks via `ThreadPoolExecutor` (max 5 workers)
3. Returns the access level per space (`CAN_MANAGE`, `CAN_EDIT`, `VIEW_ONLY`, or `null`)

### Failure Scenarios

| Status | Body | Root Cause |
|--------|------|------------|
| **200** with stale `frameworkCatalog` | Config shows `main` instead of deployed catalog | Environment variables not injected -- redeploy with `make deploy WAREHOUSE_ID=...` |
| **500** | SQL execution error | Warehouse unavailable or SP lacks UC access |
| Access badges stuck on "Checking..." | `check-access` call timing out | Too many spaces; verify pagination is limiting the batch size |

---

## 13. Journey 11: Programmatic Trigger API

> **Reference:** For the complete API specification with request/response schemas, see [07 -- API Reference](docs/genie-space-optimizer-design/07-api-reference.md).

### Overview

The `/trigger` endpoint allows headless optimization — trigger runs programmatically without the UI. This is designed for CI/CD pipelines, scheduled workflows, or external automation tools.

### Trigger an Optimization

`POST /api/genie/trigger`

**Request body:**
```json
{
  "space_id": "string (Genie Space UUID)",
  "apply_mode": "genie_config"
}
```

**Response (200):**
```json
{
  "runId": "UUID string",
  "jobRunId": "integer as string",
  "jobUrl": "https://<host>/jobs/<job_id>?o=<workspace_id>",
  "status": "IN_PROGRESS"
}
```

**Backend behavior:** Delegates to the same `do_start_optimization()` function used by the UI-driven `POST /spaces/{space_id}/optimize` endpoint. The Databricks Apps proxy injects OBO tokens automatically for any authenticated request.

### Poll Run Status

`GET /api/genie/trigger/status/{run_id}`

**Response (200):**
```json
{
  "runId": "string",
  "status": "QUEUED|RUNNING|CONVERGED|STALLED|MAX_ITERATIONS|FAILED|APPLIED|DISCARDED",
  "baselineScore": "float | null",
  "optimizedScore": "float | null",
  "convergenceReason": "string | null",
  "completedAt": "ISO timestamp | null"
}
```

### Test Actions

1. **Trigger via curl:**
```bash
curl -X POST https://<app-url>/api/genie/trigger \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"space_id": "<genie-space-id>"}'
```

2. **Poll until terminal status:**
```bash
# Repeat every 30s until status is terminal
curl https://<app-url>/api/genie/trigger/status/<run_id> \
  -H "Authorization: Bearer $TOKEN"
```

3. **Verify in UI:** Navigate to the Dashboard — the triggered run should appear in the activity table and be viewable from the space detail page.

### Failure Scenarios

| Status | Body | Root Cause |
|--------|------|------------|
| **400** | Missing or invalid `space_id` | Request body validation failed |
| **409** | Active optimization already in progress | Another run is QUEUED or IN_PROGRESS for this space |
| **500** | Job submission failed | Same causes as Journey 4 (wheel not found, permissions, etc.) |

### Integration Testing Pattern

```bash
# Full headless E2E test
RUN_ID=$(curl -s -X POST https://<app-url>/api/genie/trigger \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"space_id": "'$SPACE_ID'"}' | jq -r '.runId')

# Poll loop
while true; do
  STATUS=$(curl -s https://<app-url>/api/genie/trigger/status/$RUN_ID \
    -H "Authorization: Bearer $TOKEN" | jq -r '.status')
  echo "Status: $STATUS"
  case $STATUS in
    CONVERGED|STALLED|MAX_ITERATIONS|APPLIED|DISCARDED) echo "Done"; break ;;
    FAILED) echo "FAILED"; exit 1 ;;
    *) sleep 30 ;;
  esac
done
```

---

## 14. Troubleshooting Databricks Job Failures

> **Reference:** For the complete error-solution matrix and convergence reason decoder, see [Appendix B -- Troubleshooting](docs/genie-space-optimizer-design/appendices/B-troubleshooting.md). For operational diagnostic queries, see [10 -- Operations Guide](docs/genie-space-optimizer-design/10-operations-guide.md).

This is the most common source of issues. The optimization pipeline runs as a multi-task Databricks Job with 5 sequential notebook tasks. Each task can fail independently.

### How to Access Job Logs

```bash
# Get the job run ID from the run detail API
curl -s https://<app-url>/api/genie/runs/<run_id> | jq '.jobRunId'

# Or from the optimize response when you triggered it:
# {"runId": "...", "jobRunId": "12345", "jobUrl": "https://..."}

# Get job run details
databricks runs get <job_run_id> -p <profile> -o json | jq '.tasks[] | {task_key, state}'

# Get output/error for a specific task
databricks runs get-output <job_run_id> -p <profile> -o json | jq '.notebook_output'
```

### Task-by-Task Failure Guide

#### Task 1: `preflight`

**What it does:** Fetches Genie Space config, collects UC metadata (columns, tags, routines) via REST API with Spark SQL fallback, validates config with `genie_schema.py` (lenient mode), generates benchmark questions via LLM, creates MLflow experiment.

**Metadata collection strategy:** Preflight tries three sources in order: (1) prefetched data from the optimization trigger, (2) UC REST API via `WorkspaceClient` (`get_columns_for_tables_rest`, `get_routines_for_schemas_rest`), (3) Spark SQL queries against `information_schema`. Tags always use Spark SQL (no REST API for `table_tags`).

| Error Pattern | Likely Cause | Resolution |
|--------------|-------------|------------|
| `PermissionDenied` on Genie API | App SP lacks Genie access | Grant the app SP access to the Genie Space |
| `SCHEMA_NOT_FOUND` or `CATALOG_NOT_FOUND` | Config vars point to nonexistent catalog/schema | Fix `GENIE_SPACE_OPTIMIZER_CATALOG` / `GENIE_SPACE_OPTIMIZER_SCHEMA` in `databricks.yml` |
| `permission denied` on information_schema | App SP lacks UC grants; REST API also failed | Re-run `databricks bundle deploy` to trigger grant script, or use the Settings page to grant access, or run `resources/grant_app_uc_permissions.py` manually |
| `Could not create MLflow experiment` | Shared experiment path not writable | Create `/Shared/genie-optimization/` in workspace or grant app SP write access |
| `benchmark_count = 0` | LLM benchmark generation returned empty | Check Foundation Model API endpoint availability; verify `databricks-claude-opus-4-6` endpoint exists |
| Timeout (>600s in DAB job, >7200s in launcher job) | Slow UC metadata queries or LLM calls | Increase timeout or investigate warehouse performance |

**Diagnostic query (check preflight stages in Delta):**
```sql
SELECT stage, status, error_message, duration_seconds
FROM <catalog>.<schema>.genie_opt_stages
WHERE run_id = '<run_id>' AND stage LIKE 'PREFLIGHT%'
ORDER BY started_at
```

#### Task 2: `baseline_eval`

**What it does:** Runs all benchmark questions through Genie (with exponential backoff on HTTP 429 rate limits), scores responses with 9 LLM judges via MLflow, records baseline scores. Uses robust JSON extraction (`_extract_json`) to handle markdown fences and prose-wrapped LLM responses.

| Error Pattern | Likely Cause | Resolution |
|--------------|-------------|------------|
| `AttributeError: 'NoneType' object has no attribute 'info'` | Transient MLflow harness bug | Automatic retry up to 4 times with exponential backoff (10s, 20s, 30s, 40s). Falls back to single-worker mode on retry 2+. |
| `ResourceExhausted` / HTTP 429 from Genie API | Rate limit hit | Built-in exponential backoff retries (configurable via `GENIE_RATE_LIMIT_BASE_DELAY` and `GENIE_RATE_LIMIT_RETRIES`). Should self-heal. |
| `TABLE_OR_VIEW_NOT_FOUND` in evaluation | Benchmark SQL references missing table | Check benchmark questions reference valid tables; regenerate benchmarks |
| `Infrastructure SQL errors detected during evaluation` | Multiple benchmark SQLs hit infra errors | Default: fails fast (`FAIL_ON_INFRA_EVAL_ERRORS=true`). Set to `false` to continue past infra errors. |
| `_multithreadedrendezvous` | gRPC/Spark Connect timeout | Automatic retry with backoff |
| `permission denied` on data tables | App SP can't SELECT from Genie space tables | Grant SELECT on the data tables to app SP |
| Timeout (>3600s in DAB / >7200s in launcher) | 20 benchmarks x 9 judges = 180 LLM calls + Genie queries | Reduce `TARGET_BENCHMARK_COUNT` in `common/config.py` or increase timeout |

**Diagnostic query (check baseline iteration scores):**
```sql
SELECT iteration, eval_scope, overall_accuracy, scores_json, thresholds_met
FROM <catalog>.<schema>.genie_opt_iterations
WHERE run_id = '<run_id>' AND iteration = 0
```

**Important:** If `thresholds_met = true` after baseline, the lever loop will skip early (this is correct behavior, not a failure).

**Temporal-stale benchmark handling:** Before scoring, `_flag_stale_temporal_benchmarks()` runs each benchmark's GT SQL via Spark. Benchmarks whose GT SQL returns 0 rows (due to stale temporal references like "this year" or "last quarter") are flagged `temporal_stale=True` and excluded from the accuracy denominator.

**`both_empty` comparison handling:** When both the GT SQL and Genie's generated SQL return 0 rows, the comparison result is `both_empty`. These results, along with `genie_result_unavailable` errors, are excluded from the accuracy denominator so they don't inflate or deflate scores.

#### Task 3: `lever_loop`

**What it does:** Runs in three phases:

1. **Stage 2.5: Prompt Matching Auto-Config** — Before the lever loop, applies format assistance (`get_example_values`) on all visible columns and entity matching (`build_value_dictionary`) on prioritized STRING columns (up to 120-column cap). This is a deterministic, no-LLM step. Propagation wait: 90s if entity matching changes were applied, 30s otherwise.

2. **Arbiter Benchmark Corrections** — Extracts `genie_correct` arbiter verdicts from baseline evaluation. When the count meets `ARBITER_CORRECTION_TRIGGER` (default 3), rewrites those benchmarks' `expected_sql` to match Genie's correct SQL via `apply_benchmark_corrections()`. This prevents chasing false failures from stale gold SQL.

3. **5-Lever Optimization Loop** — Iterates through 5 levers (Tables & Columns, Metric Views, TVFs, Join Specifications, Genie Space Instructions). For each lever:
   - **Tiered failure analysis**: loads failure rows and splits them into:
     - **Hard failures** (`ground_truth_correct` / `neither_correct` arbiter verdicts) — drive targeted fixes
     - **Soft signals** (`genie_correct` / `both_correct` verdicts where individual judges still failed) — inform best-practice guidance
   - Clusters hard failures and soft signals separately (soft clusters tagged `signal_type: soft`)
   - Generates proposals via LLM, converts to patches
   - Applies patches in risk order (low → medium → high)
   - Propagation wait: 90s if value dictionary changes, 30s otherwise
   - Runs **3-gate evaluation** (slice → P0 → full) with noise-floor-adjusted tolerances
   - Rolls back if any gate detects regression beyond the effective threshold (default `REGRESSION_THRESHOLD=5.0%`, adjusted upward for small benchmark sets)
   - On acceptance: updates best scores, registers instruction version snapshot, refreshes reference SQLs
   - **Lever 4** always runs its join discovery path (even without join failure clusters), detecting implicit joins from successful Genie queries and proposing explicit documentation
   - **Lever 5** generates a holistic instruction rewrite using `LEVER_5_HOLISTIC_PROMPT` (inspired by [AgentSkills.io](https://agentskills.io/specification)), considering the space's purpose, all benchmark evaluation learnings, prior lever tweaks, and both hard failure clusters and soft signal clusters. Produces a `rewrite_instruction` patch that replaces the full instruction body.

**Connection pool initialization:** At job startup, `optimize_genie_space()` calls `configure_connection_pool(w, CONNECTION_POOL_SIZE)` (default 20) to increase the urllib3 pool on the `WorkspaceClient`, preventing "Connection pool is full" warnings under concurrent evaluation load.

**Resume state restoration:** On task retry, `_resume_lever_loop()` restores `reflection_buffer`, `tried_patches`, `tried_root_causes`, and `skill_exemplars` from the `reflection_json` column on `genie_opt_iterations` Delta table. This ensures the adaptive strategist resumes with full context (DO NOT RETRY list, proven patterns) rather than starting from scratch.

| Error Pattern | Likely Cause | Resolution |
|--------------|-------------|------------|
| `thresholds_met` → early exit | Baseline already met targets | Not an error. Run exits with `SKIPPED: baseline meets thresholds`. |
| All levers rolled back | Every patch caused regression | Run completes as `STALLED`. Review failure analysis in `genie_eval_asi_results` table. |
| `Genie API rate limit (429)` | Too many API calls | Built-in rate limiting at 12s intervals + automatic exponential backoff retries on `ResourceExhausted`. If persistent, increase `RATE_LIMIT_SECONDS` or `GENIE_RATE_LIMIT_BASE_DELAY`. |
| `LLM endpoint not found` | Foundation Model API endpoint misconfigured | Verify `databricks-claude-opus-4-6` endpoint exists in workspace model serving |
| Patch application error | Invalid patch command generated by LLM | Patch is rolled back, lever is skipped. Check `genie_opt_patches` table for the failed patch. |
| `SQL context lost` | Spark session reset between evaluations | Built-in `_ensure_sql_context()` should prevent this. If persists, check Spark Connect stability. |
| Timeout (>5400s in DAB / >7200s in launcher) | Many levers x many iterations | Reduce `MAX_ITERATIONS` in config or limit levers via `levers` parameter |
| Arbiter corrections applied but scores worsen | Benchmark gold SQL was actually correct | Check `genie_correct` verdicts in `genie_eval_asi_results`; manually verify questionable benchmarks |
| ASI data shows `failure_type: None` | QID extraction failed — `question_id` not merging with eval rows | Check that `_row_qid()` is parsing the MLflow `request` column correctly; verify wheel is up to date |
| All clusters are `wrong_table` with overlapping questions | Clustering by per-judge instead of by question | Verify the deployed wheel contains the question-level clustering fix; redeploy with `make deploy` |
| Soft signals not appearing in logs | Arbiter filter not splitting hard/soft rows | Verify `_has_individual_judge_failure()` is present in the deployed code; check wheel health |

**Diagnostic query (check lever outcomes):**
```sql
SELECT lever, stage, status, detail_json, error_message
FROM <catalog>.<schema>.genie_opt_stages
WHERE run_id = '<run_id>' AND stage LIKE 'LEVER_%'
ORDER BY lever, started_at
```

**Diagnostic query (check what patches were applied/rolled back):**
```sql
SELECT lever, patch_type, target_object, risk_level, rolled_back, rollback_reason
FROM <catalog>.<schema>.genie_opt_patches
WHERE run_id = '<run_id>'
ORDER BY lever, patch_index
```

**Diagnostic query (check arbiter verdicts for benchmark corrections):**
```sql
SELECT question_id, arbiter_verdict, question_text
FROM <catalog>.<schema>.genie_eval_asi_results
WHERE run_id = '<run_id>' AND iteration = 0 AND arbiter_verdict = 'genie_correct'
```

**Diagnostic query (trace provenance from patch back to judge verdicts):**
```sql
SELECT p.question_id, p.judge, p.resolved_root_cause, p.signal_type,
       p.cluster_id, p.proposal_id, p.gate_type, p.gate_result
FROM <catalog>.<schema>.genie_opt_provenance p
WHERE p.run_id = '<run_id>'
ORDER BY p.iteration, p.lever, p.question_id
```

#### Task 4: `finalize`

**What it does:** Runs exactly **2 repeatability evaluation passes** (re-asks all benchmark questions to check SQL consistency), averages the results, promotes the best model in MLflow, generates a summary report, and writes the final terminal stage records.

The finalize task uses a **heartbeat + soft timeout** mechanism:
- Emits `FINALIZE_HEARTBEAT` stage events every 30s (configurable via `GENIE_SPACE_OPTIMIZER_FINALIZE_HEARTBEAT_SECONDS`) so stale-run reconciliation doesn't prematurely mark the run as dead.
- Enforces a soft timeout of 6600s (configurable via `GENIE_SPACE_OPTIMIZER_FINALIZE_TIMEOUT_SECONDS`). If exceeded, raises `TimeoutError` and records `finalize_timeout` as the convergence reason.

| Error Pattern | Likely Cause | Resolution |
|--------------|-------------|------------|
| Repeatability % very low (<50%) | Genie gives inconsistent SQL for same question | Not a code bug -- indicates the Genie Space config is non-deterministic. Report to Genie team. |
| MLflow model promotion failure | Best model not loadable or experiment missing | Check MLflow experiment manually; ensure experiment was created in preflight |
| `finalize_timeout` convergence reason | Finalize exceeded soft timeout (default 6600s) | Increase `GENIE_SPACE_OPTIMIZER_FINALIZE_TIMEOUT_SECONDS` env var, or reduce benchmark count |
| `finalize_error` convergence reason | Unhandled exception during finalize | Check error_message in `genie_opt_stages` for the FINALIZE_TERMINAL stage |
| Timeout (>1800s in DAB / >7200s in launcher) | Repeatability testing with many benchmarks | Increase DAB/launcher job timeout, and/or the soft timeout env var |

**Diagnostic query (check finalize result):**
```sql
SELECT status, best_accuracy, best_repeatability, best_iteration, convergence_reason
FROM <catalog>.<schema>.genie_opt_runs
WHERE run_id = '<run_id>'
```

**Diagnostic query (check finalize heartbeats and phases):**
```sql
SELECT stage, status, detail_json, error_message, duration_seconds
FROM <catalog>.<schema>.genie_opt_stages
WHERE run_id = '<run_id>' AND stage IN ('FINALIZE_STARTED', 'FINALIZE_HEARTBEAT', 'FINALIZE_TERMINAL', 'REPEATABILITY_TEST')
ORDER BY started_at
```

#### Task 5: `deploy_check` + `deploy`

**`deploy_check`** is a condition task (no compute) that evaluates `deploy_target != ""`. If empty (default), deploy is skipped entirely.

**`deploy`** only runs if `deploy_check` passes (outcome = "true"). Currently a placeholder for future DABs integration.

### Common Cross-Task Issues

| Issue | Symptom | Resolution |
|-------|---------|------------|
| **Wheel not found** | Job fails immediately with `RuntimeError: Could not find or build the genie_space_optimizer wheel` | Re-deploy the app: `make deploy`. The build step creates and uploads the wheel. |
| **Stale wheel (old code running)** | Optimization runs show old behavior despite code changes | `.build/.gitignore` blocked wheel sync. Run `make deploy` which removes `.gitignore`, runs `bundle deploy` (sync) + `apps deploy` (snapshot + restart), and verifies. Check app startup logs for `App wheel:` line to confirm correct version. |
| **Notebook not found** | Job task fails with path not found error | The job launcher uploads notebooks to `/Workspace/Shared/genie-space-optimizer/`. Check workspace permissions on `/Workspace/Shared/`. |
| **Stale run stuck in QUEUED** | Run shows QUEUED forever, no job_run_id | Wait 10 minutes -- reconciliation will mark it FAILED. Or load the space detail page to trigger reconciliation immediately. |
| **Job terminated but run still shows IN_PROGRESS** | Delta state not updated | Load the space detail page -- `_reconcile_active_runs()` will check job state and update Delta. |
| **`convergence_reason: job_submission_error`** | Run created but job never started | Check app logs for the specific error. Common causes: SP lacks job creation permission, workspace at compute limits. |
| **NaN/Inf in scores** | JSON serialization error in frontend | Backend scrubs NaN/Inf to null via `_scrub_nan()`. If you see NaN in API responses, this is a bug -- report it. |
| **Connection pool is full** | urllib3 warnings during concurrent evaluation | Automatically configured at startup via `configure_connection_pool(w, CONNECTION_POOL_SIZE=20)`. If warnings persist, increase `CONNECTION_POOL_SIZE` in `config.py`. |

### How to Read the `convergence_reason` Field

The `convergence_reason` on a run tells you exactly what happened:

| Value | Meaning |
|-------|---------|
| `threshold_met` | All quality thresholds met (CONVERGED) |
| `no_further_improvement` | No levers improved score enough (STALLED) |
| `max_iterations` | Hit 3 iteration limit (MAX_ITERATIONS) |
| `baseline_meets_thresholds` | No optimization needed — baseline already converged |
| `finalize_timeout` | Finalize exceeded soft timeout (default 6600s, FAILED) |
| `finalize_error` | Unhandled exception during finalize (FAILED) |
| `error_in_PREFLIGHT_STARTED` | Preflight task crashed |
| `error_in_BASELINE_EVAL` | Baseline evaluation crashed |
| `error_in_LEVER_{N}_*` | Lever N task crashed |
| `error_in_FINALIZE_*` | Finalize task crashed |
| `job_submission_error: {detail}` | Job couldn't be submitted |
| `job_terminated_without_state_update:failed` | Job crashed before writing terminal state |
| `job_{lifecycle}_detected_on_poll:{result_state}` | Run poller detected job termination (e.g. `job_terminated_detected_on_poll:failed`) |
| `job_run_lookup_failed` | Couldn't check job status (API error) |
| `stale_queued_no_job_run` | QUEUED for >10 min with no job_run_id |
| `user_discarded` | User clicked Discard |

---

## 15. Edge Cases and Failure Scenarios

### Authentication Edge Cases

| Scenario | Behavior |
|----------|----------|
| OBO token present with `dashboards.genie` scope | Full user-scoped access; spaces filtered by user permissions |
| OBO token present without `dashboards.genie` scope | `_genie_client()` catches `PermissionDenied`, falls back to SP |
| OBO token completely missing | `_get_user_ws()` logs warning, returns SP client |
| SP client also has no Genie access | `GET /spaces` returns `502` |

### Concurrent Run Prevention

| Scenario | API Response |
|----------|-------------|
| One run `IN_PROGRESS`, user clicks optimize again | `409: An optimization is already in progress for this space (run ...)` |
| One run `QUEUED` but job never started (stale) | After 10 min, reconciliation marks it FAILED; next optimize succeeds |
| Two browser tabs click optimize simultaneously | First request succeeds, second gets `409` (Delta write + reconciliation prevents race) |

### Data Edge Cases

| Case | API Behavior | UI Behavior |
|------|-------------|-------------|
| Space with 0 tables | `tables: []` in response | "No tables referenced." |
| Space deleted mid-optimization | Job fails to fetch config | Pipeline shows FAILED with convergence reason |
| NaN/Inf in evaluation scores | `_scrub_nan()` converts to null | Score cards show "--" or are hidden |
| Very long instructions (>10KB) | Full text returned | Instructions card scrolls |
| Benchmark table doesn't exist | `benchmarkQuestions: []` | Benchmark section hidden |

### Deferred UC Writes (apply_mode = uc_artifact)

When `apply_mode` includes UC artifact writes:
- Pipeline runs with `genie_config` mode (patches applied to Genie config only)
- On Apply, backend replays applicable patches as UC SQL statements:
  - `add_column_description` / `update_column_description` → `ALTER TABLE ... ALTER COLUMN ... COMMENT '...'`
  - `add_description` / `update_description` → `COMMENT ON TABLE ... IS '...'`
  - `update_tvf_sql` → Direct SQL execution
- Requires OBO user's warehouse access
- Fails if baseline score >= optimized score (safety check)

---

## 16. Issue Reporting Template

When reporting issues found during testing, structure the report as follows. Include all available diagnostic data so the issue can be reproduced and debugged.

```
## Issue Report

### Summary
[One sentence: what failed and where]

### Journey
[Which journey number and step, e.g., "Journey 4, Step 11: job submission"]

### Identifiers
- Space ID: [from API response]
- Run ID: [from API response]
- Job Run ID: [from optimize response or run detail]
- Job URL: [if available]

### API Request & Response
- Method: [GET/POST]
- URL: [full path]
- Request body: [if POST]
- Response status: [e.g., 500]
- Response body:
```json
[paste full response]
```

### Expected vs Actual
- Expected: [what should have happened]
- Actual: [what happened instead]

### Delta State (if applicable)
Run this SQL to capture state:
```sql
-- Run record
SELECT * FROM <catalog>.<schema>.genie_opt_runs WHERE run_id = '<run_id>';

-- Stage history
SELECT stage, status, error_message, duration_seconds
FROM <catalog>.<schema>.genie_opt_stages
WHERE run_id = '<run_id>' ORDER BY started_at;

-- Iteration scores
SELECT iteration, lever, eval_scope, overall_accuracy, thresholds_met
FROM <catalog>.<schema>.genie_opt_iterations
WHERE run_id = '<run_id>' ORDER BY iteration;
```

### Job Logs (if applicable)
```bash
databricks runs get <job_run_id> -p <profile> -o json | jq '.tasks[] | {task_key, state}'
```

### Convergence Reason
[Value of convergenceReason from run detail API]

### App Logs
```bash
databricks apps logs genie-space-optimizer -p <profile>
```

### Severity
- Blocker: Cannot proceed with testing
- Critical: Core feature broken
- Major: Feature broken but workaround exists
- Minor: Cosmetic or non-blocking
```

---

## 17. Full E2E Checklist

### Pre-deployment

- [ ] `make setup WAREHOUSE_ID=<warehouse-id>` succeeds for first-time install (or `make deploy WAREHOUSE_ID=<warehouse-id>` for updates)
- [ ] Grant script ran (check output for `[grant-app-sp] Ensured UC grants for principal=...`)
- [ ] `make verify WAREHOUSE_ID=<warehouse-id>` confirms wheel is on workspace
- [ ] App is RUNNING: `databricks apps get genie-space-optimizer -o json`
- [ ] App startup logs show `App wheel: genie_space_optimizer-x.y.z.whl (size=... bytes)`
- [ ] SQL Warehouse is RUNNING
- [ ] At least one Genie Space exists and is accessible

### Dashboard

- [ ] `GET /api/genie/spaces` returns 200 with space list
- [ ] `GET /api/genie/activity` returns 200 (may be empty array)
- [ ] `GET /api/genie/current-user` returns 200
- [ ] Stats cards render with correct counts
- [ ] Search filters spaces client-side
- [ ] Space cards show name, description, table count, score badge
- [ ] Activity table renders (if data exists) with clickable rows

### Space Detail

- [ ] `GET /api/genie/spaces/{id}` returns 200 with full detail
- [ ] Instructions, sample questions, tables, joins, functions render correctly
- [ ] Empty sections show appropriate messages
- [ ] Benchmark questions section appears (if benchmarks exist)
- [ ] Optimization history renders with clickable rows
- [ ] `hasActiveRun` correctly enables/disables optimize button
- [ ] Stale run reconciliation works (load page when a stuck run exists)

### Start Optimization

- [ ] `POST /api/genie/spaces/{id}/optimize` returns 200 with runId + jobRunId
- [ ] Run created in Delta with status QUEUED, then IN_PROGRESS
- [ ] Job visible in Databricks Workflows UI
- [ ] Second optimize attempt returns 409
- [ ] Invalid apply_mode returns 400

### Pipeline Monitoring

- [ ] `GET /api/genie/runs/{id}` returns 200 with steps and levers
- [ ] Steps transition through pending → running → completed
- [ ] Lever statuses update (accepted/rolled_back/skipped)
- [ ] Score cards appear on completion
- [ ] "View Results" button appears on terminal status
- [ ] Failed run shows convergence reason
- [ ] Polling stops on terminal status
- [ ] Iteration chart renders score progression across iterations
- [ ] ASI panel shows failure breakdown by judge and type
- [ ] Provenance panel shows judge → cluster → patch → gate chain
- [ ] `GET /runs/{id}/iterations` returns per-iteration score data
- [ ] `GET /runs/{id}/asi` returns ASI failure analysis
- [ ] `GET /runs/{id}/provenance` returns provenance records

### Job Execution (check each task)

- [ ] Preflight completes: config fetched, benchmarks generated, experiment created, experiment-level tags set
- [ ] Preflight `_validate_core_access()` uses REST API (`w.tables.get`) instead of `information_schema`
- [ ] Preflight returns `human_corrections` from prior labeling session (if available)
- [ ] Preflight `_validate_write_access()`: fails fast if `apply_mode=both/uc_artifact` and MODIFY missing
- [ ] Baseline eval completes: scores recorded in `genie_opt_iterations` with iteration=0
- [ ] Baseline temporal-stale benchmarks flagged (GT SQL returns 0 rows) and excluded from accuracy
- [ ] Baseline `both_empty` comparisons (both GT and Genie return 0 rows) excluded from accuracy
- [ ] Baseline labeling session auto-created for failure trace IDs
- [ ] Baseline traces tagged with `genie.optimization_run_id`, `genie.iteration`, `genie.eval_scope`
- [ ] Connection pool configured at job startup (`configure_connection_pool`, `CONNECTION_POOL_SIZE=20`)
- [ ] `fetch_genie_result_df` retries up to 3 times on PENDING/RUNNING statements with linear backoff
- [ ] Lever loop resume restores `reflection_buffer`, `tried_patches`, `tried_root_causes`, `skill_exemplars` from Delta
- [ ] Lever loop applies human benchmark corrections from prior labeling session (if any)
- [ ] Lever loop Stage 2.5: prompt matching auto-config applied (format assistance + entity matching)
- [ ] Lever loop Stage 2.75: proactive description enrichment for insufficient columns (< 10 chars)
- [ ] Lever loop Stage 2.75: table description enrichment for tables with no/insufficient descriptions
- [ ] Lever loop Stage 2.75: example SQL mining with 0-row validation, applied proactively via Genie API
- [ ] Lever loop Stage 2.95: proactive instruction seeding for spaces with < 50 char instructions
- [ ] Lever loop noise floor: score improvements < 5.0 pts rejected as cosmetic noise
- [ ] Adaptive loop: re-clusters failures from fresh eval each iteration (not stale baseline)
- [ ] Adaptive loop: `rank_clusters()` scores clusters by impact (question_count × causal × severity × fixability)
- [ ] Adaptive strategist: produces exactly 1 action group per iteration (not batch)
- [ ] Adaptive strategist prompt includes priority ranking and reflection buffer with DO NOT RETRY list
- [ ] Holistic fallback on iteration 1: if adaptive strategist returns 0 AGs, falls back to holistic strategy
- [ ] Reflection buffer: each iteration records accepted/rolled-back, score deltas, target objects, patches
- [ ] Reflection buffer persisted in `genie_opt_iterations.reflection_json` column
- [ ] Tried-patch filtering: clusters with already-tried-and-rolled-back (failure_type, blame_set) excluded
- [ ] Diminishing returns: loop stops when last 2 accepted iterations each improved < 2%
- [ ] Lever loop arbiter corrections: applied if ≥3 `genie_correct` verdicts in baseline
- [ ] Lever loop arbiter filter: `both_correct` AND `genie_correct` excluded from hard failure rows
- [ ] Lever loop tiered arbiter: soft signal rows extracted (individual judge failures on correct verdicts)
- [ ] Lever loop soft clusters: tagged `signal_type: soft`, passed to Levers 4 and 5
- [ ] Lever 4: join discovery runs even without explicit join failure clusters
- [ ] Lever 5: uses `instruction_guidance` from lever directives for instruction rewrite
- [ ] Full-eval gate: confirmation double-run (two evaluations averaged) to smooth Genie non-determinism
- [ ] Lever loop iteration completes (or skips if thresholds met): patches in `genie_opt_patches`
- [ ] ASI data present on clusters (failure_type not None for applicable questions)
- [ ] ASI results written to `genie_eval_asi_results` with `mlflow_run_id` for trace linking
- [ ] Provenance rows written to `genie_opt_provenance` linking judges → clusters → proposals → gates
- [ ] ASI Feedback logged on MLflow traces (`asi_{judge}` entries)
- [ ] Gate Feedback logged on MLflow traces (`gate_{type}` entries with pass/fail + regression details)
- [ ] Expected SQL logged as `Expectation` assessments on evaluation traces (baseline + each accepted iteration)
- [ ] Instruction slot budget enforced: proposals capped at remaining budget, post-apply trim, strict validation
- [ ] Cross-iteration verdict history: per-question arbiter verdicts tracked across all full-scope evaluations
- [ ] Question persistence summary: INTERMITTENT / PERSISTENT / ADDITIVE_LEVERS_EXHAUSTED classification
- [ ] Escalation: `remove_tvf` triggers TVF schema overlap analysis and confidence tiering
- [ ] Escalation: `gt_repair` triggers LLM-assisted ground-truth SQL correction for `neither_correct`
- [ ] Escalation: `flag_for_review` writes to `genie_opt_flagged_questions` Delta table
- [ ] Quarantined questions excluded from accuracy denominator after repeated `neither_correct` + failed GT repair
- [ ] Queued patches written to `genie_opt_queued_patches` for high-risk patches pending human approval
- [ ] `GET /pending-reviews/{space_id}` returns flagged questions, queued patches, labeling session URL
- [ ] Pending review badge shown on run detail page and space card
- [ ] Reflection entries track `affected_question_ids`, `fixed_questions`, `still_failing`, `new_regressions`
- [ ] Label schemas created with `overwrite=True` (no defensive existence-check needed)
- [ ] Console `print()` diagnostics visible for schema creation, session creation, and trace population steps
- [ ] Labeling session created with `mlflow.set_experiment()` called before session creation
- [ ] Trace population uses batched `add_traces()` — priority traces first, then backfill (not single concat)
- [ ] Labeling session created with eval-run-based trace population (flagged trace IDs prioritized)
- [ ] Labeling session URL persisted to `genie_opt_runs.labeling_session_url`
- [ ] Labeling session URL surfaced as "Human Review" link in run detail UI
- [ ] Baseline labeling session also captures and persists session URL
- [ ] Finalize: repeatability eval prints per-judge scores to console; falls back to benchmark `expected_sql` if no reference SQLs
- [ ] Finalize heartbeats visible in `genie_opt_stages` (FINALIZE_HEARTBEAT events)
- [ ] Deploy skipped (deploy_target empty): deploy_check condition evaluates to false
- [ ] Labeling schemas created/reused without overwrite conflicts (handles "referenced by labeling sessions")

### Comparison

- [ ] `GET /api/genie/runs/{id}/comparison` returns 200 with scores and configs
- [ ] Per-dimension scores show all 7 quality dimensions (+ response quality + arbiter if applicable)
- [ ] Original and optimized configs differ (if patches were applied)
- [ ] Improvement percentage calculated correctly

### Apply

- [ ] `POST /api/genie/runs/{id}/apply` returns 200
- [ ] Run status updated to APPLIED in Delta
- [ ] If apply_mode=uc_artifact: UC_OBO_WRITE stage recorded
- [ ] Genie Space config in workspace reflects optimized version

### Discard

- [ ] `POST /api/genie/runs/{id}/discard` returns 200
- [ ] Run status updated to DISCARDED in Delta
- [ ] Genie Space config in workspace reverted to original

### Settings & Permission Dashboard (Advisor-Only)

- [ ] `GET /api/genie/settings/permissions?metadata_only=true` returns 200 with SP identity, framework config, schema permissions, and space permissions
- [ ] Service principal ID and display name shown in SP identity card
- [ ] `frameworkCatalog` and `frameworkSchema` match deploy-time config (not hardcoded defaults)
- [ ] `workspaceHost` and `jobUrl` fields populated
- [ ] Schema permissions show read/write status per schema with copyable `readGrantCommand`/`writeGrantCommand`
- [ ] Space permissions show `spHasManage` status and `spGrantInstructions`
- [ ] Per-space access badges load lazily via `POST /api/genie/spaces/check-access` (per page, not all at once)
- [ ] No GRANT/REVOKE buttons — advisor model provides copyable SQL and sharing instructions only
- [ ] "Editable Only" filter toggle works (hides View Only / No Access spaces)
- [ ] Activity feed filtered: only shows runs for spaces user has CAN_MANAGE or CAN_EDIT on

### Programmatic Trigger API

- [ ] `POST /api/genie/trigger` with valid space_id returns 200 with runId, jobRunId, status
- [ ] `GET /api/genie/trigger/status/{run_id}` returns 200 with current status
- [ ] Triggered run appears in dashboard activity table
- [ ] Triggered run visible from space detail history
- [ ] Second trigger for same space returns 409 (active run exists)
- [ ] Poll loop correctly detects terminal status
- [ ] Invalid space_id returns 400

### Error Handling

- [ ] Nonexistent space ID → 404
- [ ] Nonexistent run ID → 404
- [ ] Apply on non-terminal run → 400
- [ ] Apply on already-applied run → 409
- [ ] Discard on already-discarded run → 409
- [ ] OBO fallback works (SP used when token missing)
- [ ] Stale run reconciliation marks dead runs as FAILED

### Navigation

- [ ] All back buttons navigate correctly
- [ ] Browser back/forward works
- [ ] Direct URL access works (deep linking)
- [ ] Page refresh preserves state
- [ ] Logo click returns to dashboard
