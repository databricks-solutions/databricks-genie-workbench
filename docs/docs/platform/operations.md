---
sidebar_position: 2
description: "Lakebase, MLflow, monitoring, and GSO job management for a deployed instance."
---

# Operations Guide

This guide covers day-to-day operations for a deployed Genie Workbench instance: Lakebase management, MLflow configuration, monitoring, and GSO job management.

## Lakebase

### Schema and Tables

The app creates the `genie` schema and all tables on first startup (the SP owns everything it creates). Data is stored in the `databricks_postgres` database:

| Table | Purpose |
|-------|---------|
| `scan_results` | IQ scan history: score, maturity, checks, findings, timestamps |
| `starred_spaces` | User-starred agents for quick access |
| `seen_spaces` | Tracks which agents the user has visited |
| `optimization_runs` | Legacy optimization accuracy records (used by scanner checks 11–12) |
| `agent_sessions` | Create agent session persistence (message history, step state) |

### Credential Refresh

Lakebase credentials are auto-generated via the Databricks SDK (`postgres.generate_database_credential` for autoscaling, `database.generate_database_credential` for provisioned). These OAuth tokens expire after ~1 hour, so the app recreates the asyncpg connection pool every **50 minutes** to stay ahead of expiration.

### Graceful Degradation

If `LAKEBASE_HOST` is not configured (no Lakebase attached), the app falls back to **in-memory dictionaries**. The app remains fully functional but:

- Scan results are lost on restart
- Starred Genie Agents are lost on restart
- Agent sessions are lost on restart
- The Admin Dashboard shows no historical data

### Troubleshooting Lakebase

Re-running your install path re-provisions the Lakebase resource, SP role, and
grants: rerun `notebooks/install.py` (notebook path) or
`./scripts/deploy.sh --update` (local terminal path).

| Symptom | Cause | Fix |
|---------|-------|-----|
| "Failed to list spaces" | Lakebase not attached | Re-run your install path to auto-attach the postgres resource |
| Connection errors after ~1 hour | Token refresh failed | Check app logs for credential generation errors |
| Tables not created | SP lacks CONNECT or CREATE ON DATABASE | Re-run your install path to re-create the SP role and grants |

## MLflow

### Experiment Tracking

LLM calls in the create agent and optimization pipeline are traced via MLflow. Tracing is **optional** — controlled by the `MLFLOW_EXPERIMENT_ID` environment variable in `app.yaml`.

At startup, the app validates that the experiment ID exists in the workspace. If it doesn't, tracing is silently disabled (the variable is cleared).

Tracing is the **only** MLflow dependency. Auto-Optimize does not use MLflow for
dataset persistence, run tracking, model registration, or evaluation — the
benchmark corpus is written directly to Delta and evaluation uses Genie's native
Eval-Run API. MLflow Prompt Registry is not required, and there is no
`MLFLOW_REGISTRY_URI` setting.

### Configuration

```yaml
# In app.yaml
- name: MLFLOW_TRACKING_URI
  value: "databricks"
- name: MLFLOW_EXPERIMENT_ID
  value: "<your-experiment-id>"
```

The experiment ID is workspace-specific. The installer can create one during setup, or you can create one manually and update `app.yaml`.

## Monitoring

### App Logs

```bash
databricks apps logs <app-name> --profile <profile>
```

### App Status

```bash
databricks apps get <app-name> --profile <profile>
```

### Verify Workspace Files

```bash
databricks workspace list /Workspace/Users/<email>/<app-name>/backend --profile <profile>
```

### Key Log Patterns

| Log Pattern | Meaning |
|-------------|---------|
| `OBO: using user token for /api/...` | Request authenticated via user's OBO token |
| `OBO: no x-forwarded-access-token, using SP` | No user token — using SP (expected for health checks) |
| `OBO token lacks genie scope, retrying with service principal` | Genie API scope fallback triggered |
| `Lakebase pool created` | Database connection established |
| `Lakebase pool re-created (credential refresh)` | Scheduled 50-minute token refresh |
| `Failed to persist scan result` | Lakebase write failed (check connectivity) |

## GSO Job Management

### Job Creation

The optimization job (`<app-name>-gso-optimization-job`) is created automatically by both install paths:

- **Notebook path (recommended):** `notebooks/install.py` creates or updates the job through the SDK/Jobs API (`jobs/reset` upsert semantics via `scripts/deploy_lib/gso_job.py`). No Terraform state is involved.
- **Local terminal path:** `deploy.sh` uses DABs (`databricks bundle deploy -t app`), with Terraform state scoped to the deployer.

### Job Reuse

If a job with matching settings already exists, it is reused rather than duplicated. To force recreation:

1. Delete the job in the Databricks UI
2. Rerun `notebooks/install.py`, or `./scripts/deploy.sh --update` for the local terminal path

### Job `run_as` Verification

At app startup, `_verify_gso_job_run_as()` in `backend/main.py` checks that the optimization job's `run_as` is the current app SP. It only verifies — it never updates the job. The deployer sets `run_as` (`databricks.yml` on the local terminal path, `scripts/deploy_lib/gso_job.py` on the notebook path), so a mismatch is fixed by redeploying the job (rerun `notebooks/install.py`, or `./scripts/deploy.sh --update`).

- **Readable and wrong** (e.g., the app was redeployed with a different SP): the app fails to boot until the job is redeployed.
- **Unreadable** (Jobs API error): the app still boots, after three reads with 2 s and 4 s backoff (each read has a 1 s SDK retry window and a 30 s HTTP timeout, so this can delay boot by up to about 100 s), and logs `Optimizer Job run_as unreadable after 3 attempt(s)`. Only `POST /api/auto-optimize/trigger` is disabled: it returns 503 `The optimizer job's identity could not be verified. Try again shortly.` and re-checks once per attempt, so it recovers without a restart. If the re-check then finds the wrong `run_as`, it returns 503 `The optimizer job's run_as does not match the app service principal` — redeploy the job.
- **`GSO_JOB_ID` empty or not a positive integer**: the app boots, and `/trigger` returns 503 `The optimizer job is not configured` — redeploy so the job id is injected.

### Bundle Management (local terminal path only)

On the local terminal path, the GSO job is managed by Databricks Asset Bundles (DABs):

```bash
# Deploy/update the job (done automatically by deploy.sh)
databricks bundle deploy -t app --profile <profile>
```

**Important:** Do NOT run `databricks bundle deploy -t dev` for production deployments — it creates `[dev username]` prefixed orphan jobs with separate Terraform state.

The `app` target uses `mode: development` for per-deployer Terraform state with `presets.name_prefix: ""` for clean job names.

The notebook installer does not use DABs at all — it manages the job through the Jobs API, so there is no bundle or Terraform state to maintain. Do not mix the two paths for the same app instance.

### Post-Deploy: Genie Agent Access

After deploying, the app's SP needs access to Genie Agents for API fallback and optimization:

1. Both installers grant SP access to your existing visible Genie Agents
2. For agents created after install, share them with the SP (`CAN_MANAGE`)
3. Grant SP `SELECT` on referenced schemas:

```sql
GRANT SELECT ON SCHEMA <catalog>.<schema> TO `<service-principal-name>`;
```

See [Authentication & Permissions](/docs/platform/authentication) for the full permission model.

## Related Documentation

- [Deployment Guide](/docs/getting-started/deployment-guide) — initial setup and deploy commands
- [Authentication & Permissions](/docs/platform/authentication) — SP permissions
- [Auto-Optimize](/docs/features/auto-optimize) — the pipeline managed by the GSO job
- [Troubleshooting](/docs/reference/troubleshooting) — common issues
