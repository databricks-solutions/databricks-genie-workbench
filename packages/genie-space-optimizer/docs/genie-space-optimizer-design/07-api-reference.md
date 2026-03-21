# 07 -- API Reference

[Back to Index](00-index.md) | Previous: [06 State Management](06-state-management.md) | Next: [08 Permissions and Security](08-permissions-and-security.md)

---

## Overview

All API endpoints are prefixed with `/api/genie`. The API serves two consumption patterns:

1. **UI-driven** -- the React frontend calls these endpoints to render the dashboard, trigger optimizations, and display results
2. **Headless / programmatic** -- the `/trigger` endpoints allow CI/CD pipelines, scheduled workflows, or scripts to trigger and monitor optimizations without the UI

Authentication is handled by the Databricks Apps proxy, which injects an OBO (on-behalf-of) token via the `X-Forwarded-Access-Token` header. For headless access, pass a `Bearer` token in the `Authorization` header.

---

## Triggering Optimization

### Via the UI

1. Navigate to the dashboard at `https://<app-url>/`
2. Click a Genie Space card to open the space detail page
3. Click **Start Optimization**
4. The app calls `POST /api/genie/spaces/{space_id}/optimize?apply_mode=genie_config`
5. On success, the browser navigates to `/runs/{runId}` to monitor progress
6. The run detail page polls `GET /api/genie/runs/{run_id}` every 5 seconds

### Via the Headless API

The `/trigger` endpoint provides the same optimization pipeline without requiring the UI.

**Step 1: Trigger an optimization**

```bash
curl -X POST "https://<app-url>/api/genie/trigger" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"space_id": "<genie-space-id>"}'
```

Response:

```json
{
  "runId": "abc123-...",
  "jobRunId": "12345",
  "jobUrl": "https://<workspace>/jobs/<job_id>?o=<workspace_id>",
  "status": "IN_PROGRESS"
}
```

**Step 2: Poll until completion**

```bash
curl "https://<app-url>/api/genie/trigger/status/<run_id>" \
  -H "Authorization: Bearer $TOKEN"
```

Response:

```json
{
  "runId": "abc123-...",
  "status": "CONVERGED",
  "spaceId": "<space-id>",
  "baselineScore": 72.5,
  "optimizedScore": 91.3,
  "convergenceReason": "threshold_met",
  "completedAt": "2026-03-06T15:30:00Z"
}
```

**Step 3: Full polling loop (bash example)**

```bash
APP_URL="https://<app-url>"
TOKEN="$(databricks auth token -p <profile> | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")"
SPACE_ID="<genie-space-id>"

# Trigger
RUN_ID=$(curl -s -X POST "${APP_URL}/api/genie/trigger" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{\"space_id\": \"${SPACE_ID}\"}" | python3 -c "import sys,json; print(json.load(sys.stdin)['runId'])")

echo "Run ID: ${RUN_ID}"

# Poll
while true; do
  RESP=$(curl -s "${APP_URL}/api/genie/trigger/status/${RUN_ID}" \
    -H "Authorization: Bearer ${TOKEN}")
  STATUS=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
  echo "Status: ${STATUS}"
  case $STATUS in
    CONVERGED|STALLED|MAX_ITERATIONS|APPLIED|DISCARDED) echo "Done"; break ;;
    FAILED) echo "FAILED"; echo "$RESP" | python3 -m json.tool; exit 1 ;;
    *) sleep 30 ;;
  esac
done
```

---

## Complete Endpoint Reference

### System Endpoints

| Method | Path | Operation ID | Description |
|--------|------|-------------|-------------|
| `GET` | `/version` | `getVersion` | App version |
| `GET` | `/current-user` | `getCurrentUser` | Authenticated user info |

### Space Endpoints

| Method | Path | Operation ID | Description |
|--------|------|-------------|-------------|
| `GET` | `/spaces` | `listSpaces` | List Genie Spaces with quality scores |
| `GET` | `/spaces/{space_id}` | `getSpaceDetail` | Full space config, tables, instructions, history |
| `POST` | `/spaces/{space_id}/optimize` | `startOptimization` | Trigger optimization via UI |

### Run Endpoints

| Method | Path | Operation ID | Description |
|--------|------|-------------|-------------|
| `GET` | `/runs/{run_id}` | `getRun` | Run status with pipeline steps and lever detail |
| `GET` | `/runs/{run_id}/comparison` | `getComparison` | Side-by-side original vs optimized config |
| `POST` | `/runs/{run_id}/apply` | `applyOptimization` | Confirm and keep optimized config |
| `POST` | `/runs/{run_id}/discard` | `discardOptimization` | Rollback to original config |

### Transparency Endpoints

| Method | Path | Operation ID | Description |
|--------|------|-------------|-------------|
| `GET` | `/runs/{run_id}/iterations` | `getIterations` | Per-iteration scores for charts |
| `GET` | `/runs/{run_id}/asi` | `getAsiResults` | ASI failure analysis breakdown |
| `GET` | `/runs/{run_id}/provenance` | `getProvenance` | Judge-to-patch provenance chain |
| `GET` | `/runs/{run_id}/iteration-detail` | `getIterationDetail` | Full iteration breakdown (gates, patches, questions, reflections) |

### Trigger Endpoints (Headless API)

| Method | Path | Operation ID | Description |
|--------|------|-------------|-------------|
| `POST` | `/trigger` | `triggerOptimization` | Trigger optimization programmatically |
| `GET` | `/trigger/status/{run_id}` | `getTriggerStatus` | Poll run status |

### Other Endpoints

| Method | Path | Operation ID | Description |
|--------|------|-------------|-------------|
| `GET` | `/activity` | `getActivity` | Recent runs (permission-filtered to user's spaces) |
| `GET` | `/settings/permissions` | `getPermissionDashboard` | Advisor-only permission dashboard |
| `GET` | `/pending-reviews/{space_id}` | `getPendingReviews` | Flagged questions, queued patches, labeling URL |

---

## Request and Response Schemas

### `POST /spaces/{space_id}/optimize`

**Query parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `apply_mode` | string | `genie_config` | One of: `genie_config`, `uc_artifact`, `both` |

**Response (200):**

```json
{
  "runId": "UUID string",
  "jobRunId": "integer as string",
  "jobUrl": "https://<host>/jobs/<job_id>?o=<workspace_id>"
}
```

**Errors:**

| Status | Body | Cause |
|--------|------|-------|
| 400 | `"Unsupported apply_mode '...'"` | Invalid query parameter |
| 409 | `"An optimization is already in progress..."` | Another run is QUEUED or IN_PROGRESS |
| 500 | `"Job submission failed: ..."` | Wheel not found, permissions, job creation failed |

### `POST /trigger`

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

**Errors:**

| Status | Body | Cause |
|--------|------|-------|
| 400 | Missing or invalid `space_id` | Request validation |
| 409 | Active optimization already in progress | Concurrent run prevention |
| 500 | Job submission failed | Same causes as UI trigger |

### `GET /trigger/status/{run_id}`

**Response (200):**

```json
{
  "runId": "string",
  "status": "QUEUED|IN_PROGRESS|CONVERGED|STALLED|MAX_ITERATIONS|FAILED|APPLIED|DISCARDED",
  "spaceId": "string",
  "startedAt": "ISO timestamp",
  "completedAt": "ISO timestamp | null",
  "baselineScore": "float | null",
  "optimizedScore": "float | null",
  "convergenceReason": "string | null"
}
```

### `GET /runs/{run_id}`

**Response (200):**

```json
{
  "runId": "string",
  "spaceId": "string",
  "spaceName": "string",
  "status": "string",
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
  "convergenceReason": "string | null",
  "links": [
    {"label": "string", "url": "string", "category": "string"}
  ]
}
```

### `GET /runs/{run_id}/comparison`

**Response (200):**

```json
{
  "runId": "string",
  "spaceId": "string",
  "spaceName": "string",
  "baselineScore": 72.5,
  "optimizedScore": 91.3,
  "improvementPct": 25.93,
  "perDimensionScores": [
    {"dimension": "schema_accuracy", "baseline": 70.0, "optimized": 95.0, "delta": 25.0}
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

### `POST /runs/{run_id}/apply`

**Response (200):**

```json
{
  "status": "applied",
  "runId": "string",
  "message": "Optimization applied successfully."
}
```

**Errors:**

| Status | Body | Cause |
|--------|------|-------|
| 400 | `"Cannot apply run in status ..."` | Run not in terminal state |
| 409 | `"Run already applied/discarded"` | Already actioned |
| 409 | `"Deferred UC writes require proven improvement"` | Baseline >= optimized (safety check) |
| 500 | `"Deferred UC write execution failed: ..."` | OBO SQL execution error |

### `POST /runs/{run_id}/discard`

**Response (200):**

```json
{
  "status": "discarded",
  "runId": "string",
  "message": "Optimization discarded. Config restored."
}
```

**Errors:**

| Status | Body | Cause |
|--------|------|-------|
| 409 | `"Run already ..."` | Already applied or discarded |

### `GET /settings/permissions`

**Response (200):**

```json
{
  "spaces": [
    {
      "spaceId": "string",
      "title": "string",
      "spHasManage": true,
      "schemas": [
        {
          "catalog": "string",
          "schema_name": "string",
          "readGranted": true,
          "writeGranted": false,
          "readGrantCommand": "GRANT USE CATALOG ON CATALOG `cat` TO `sp`; ...",
          "writeGrantCommand": "GRANT MODIFY ON SCHEMA `cat`.`sch` TO `sp`; ..."
        }
      ],
      "status": "ready|missing_read|missing_write|missing_manage",
      "spGrantInstructions": "Share the Genie Space with the SP..."
    }
  ],
  "spPrincipalId": "string",
  "spPrincipalDisplayName": "string | null",
  "frameworkCatalog": "string",
  "frameworkSchema": "string",
  "experimentBasePath": "string",
  "jobName": "string",
  "workspaceHost": "string | null",
  "jobUrl": "string | null"
}
```

### `GET /pending-reviews/{space_id}`

**Response (200):**

```json
{
  "flaggedQuestions": 2,
  "queuedPatches": 1,
  "totalPending": 3,
  "labelingSessionUrl": "https://<workspace>/ml/labeling/...",
  "items": [
    {
      "questionId": "string",
      "questionText": "string",
      "reason": "string",
      "confidenceTier": "high|medium|low",
      "itemType": "flagged_question|queued_patch"
    }
  ]
}
```

---

## Apply Modes

The `apply_mode` parameter controls where optimized metadata is written:

| Mode | During Optimization | On Apply |
|------|-------------------|----------|
| `genie_config` (default) | Patches applied to Genie Space config only | No additional action |
| `uc_artifact` | Patches applied to Genie Space config | UC DDL replayed (ALTER TABLE COMMENT, etc.) via OBO user identity |
| `both` | Patches applied to Genie Space config | UC DDL replayed via OBO user identity |

For `uc_artifact` and `both` modes:
- Preflight validates `MODIFY` privilege on target schemas (fails fast if missing)
- Apply only succeeds if the optimized score exceeds the baseline (safety check)
- UC writes are recorded as a `UC_OBO_WRITE` stage in Delta
- DDL statements include: `ALTER TABLE ... ALTER COLUMN ... COMMENT`, `COMMENT ON TABLE ... IS`, direct SQL execution for TVF updates

---

## Pipeline Steps in the UI

The run detail page maps job tasks to 5 pipeline steps:

| Step | Name | Maps to Job Task(s) |
|------|------|---------------------|
| 1 | Configuration Analysis | `preflight` (first half) |
| 2 | Metadata Collection | `preflight` (second half) |
| 3 | Baseline Evaluation | `baseline_eval` |
| 4 | Configuration Generation | `lever_loop` (preparatory stages + adaptive loop) |
| 5 | Optimized Evaluation | `finalize` (repeatability + report + model promotion) |

---

## Terminal Statuses

Polling stops when the status is one of:

| Status | Meaning |
|--------|---------|
| `CONVERGED` | All quality thresholds met |
| `STALLED` | No further improvement possible |
| `MAX_ITERATIONS` | Hit iteration limit |
| `FAILED` | Error during execution |
| `APPLIED` | User confirmed optimization |
| `DISCARDED` | User rolled back |

---

Next: [08 -- Permissions and Security](08-permissions-and-security.md)
