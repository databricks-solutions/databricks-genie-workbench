# Operations Guide

This guide covers day-to-day operations for a deployed Genie Workbench instance: Lakebase management, MLflow configuration, monitoring, and GSO job management.

## Lakebase

### Schema and Tables

The app creates the `genie` schema and all tables on first startup (the SP owns everything it creates). Data is stored in the `databricks_postgres` database:

| Table | Purpose |
|-------|---------|
| `scan_results` | IQ scan history: score, maturity, checks, findings, timestamps |
| `starred_spaces` | User-starred spaces for quick access |
| `seen_spaces` | Tracks which spaces the user has visited |
| `optimization_runs` | Legacy optimization accuracy records (used by scanner checks 11–12) |
| `agent_sessions` | Create agent session persistence (message history, step state) |

Lakebase state is tied to the Databricks App service principal that created
these objects. For normal updates, keep the same app instance and run
`./scripts/deploy.sh --update`. If you create a new app instance, use a fresh
Lakebase project instead of pointing the new app at the old app's `genie`
schema.

### Credential Refresh

Lakebase credentials are auto-generated via the Databricks SDK (`postgres.generate_database_credential` for autoscaling, `database.generate_database_credential` for provisioned). These OAuth tokens expire after ~1 hour, so the app recreates the asyncpg connection pool every **50 minutes** to stay ahead of expiration.

### Graceful Degradation

If `LAKEBASE_HOST` is not configured (no Lakebase attached), the app falls back to **in-memory dictionaries**. The app remains fully functional but:

- Scan results are lost on restart
- Starred spaces are lost on restart
- Agent sessions are lost on restart
- The Admin Dashboard shows no historical data

### Troubleshooting Lakebase

| Symptom | Cause | Fix |
|---------|-------|-----|
| "Failed to list spaces" | Lakebase not attached | Re-run `deploy.sh --update` to auto-attach the postgres resource |
| Connection errors after ~1 hour | Token refresh failed | Check app logs for credential generation errors |
| Tables not created | SP lacks CONNECT or CREATE ON DATABASE | Re-run `deploy.sh --update` to re-create the SP role and grants |
| `permission denied for sequence scan_results_id_seq` | New app is reusing Lakebase objects owned by an older app SP | Reuse the original app instance or move the new app to a fresh Lakebase project |

## MLflow

### Experiment Tracking

LLM calls in the fix agent, create agent, and optimization pipeline are traced via MLflow. Tracing is **optional** — controlled by the `MLFLOW_EXPERIMENT_ID` environment variable in `app.yaml`.

At startup, the app validates that the experiment ID exists in the workspace. If it doesn't, tracing is silently disabled (the variable is cleared).

### Prompt Registry

Auto-Optimize requires MLflow Prompt Registry for versioned judge prompts. If Prompt Registry is not enabled on the workspace, the optimization preflight task will fail with `FEATURE_DISABLED`.

### Configuration

```yaml
# In app.yaml
- name: MLFLOW_TRACKING_URI
  value: "databricks"
- name: MLFLOW_REGISTRY_URI
  value: "databricks-uc"
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

In Databricks Web Terminal, omit `--profile <profile>` from these commands
because the CLI uses current-user auth from the environment.

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

The optimization job is created automatically during `deploy.sh` via `databricks bundle deploy -t app`. It uses Terraform state scoped to the deployer.

### Job Reuse

If the job already exists (from a previous deploy), it is reused. To force recreation:

1. Delete the job in the Databricks UI
2. Re-run `./scripts/deploy.sh --update`

### `ensure_job_run_as` Self-Healing

At app startup, `_ensure_gso_job_run_as()` checks that the optimization job's `run_as` matches the current app SP. If they don't match (e.g., the app was redeployed with a different SP), the job is automatically updated. This avoids manual reconfiguration when the app identity changes.

### Bundle Management

The GSO job is managed by Databricks Asset Bundles (DABs):

```bash
# Deploy/update the job (done automatically by deploy.sh)
databricks bundle deploy -t app --profile <profile>
```

In Databricks Web Terminal, omit `--profile <profile>`.

**Important:** Do NOT run `databricks bundle deploy -t dev` for production deployments — it creates `[dev username]` prefixed orphan jobs with separate Terraform state.

The `app` target uses `mode: development` for per-deployer Terraform state with `presets.name_prefix: ""` for clean job names.

### Post-Deploy: Genie Space Access

After deploying, the app's SP needs access to Genie Spaces for API fallback and optimization:

1. The installer grants SP access to your existing Genie Spaces
2. For spaces created after install, share them with the SP (`CAN_MANAGE`)
3. Grant SP `SELECT` on referenced schemas:

```sql
GRANT SELECT ON SCHEMA <catalog>.<schema> TO `<service-principal-name>`;
```

See [Authentication & Permissions](03-authentication-and-permissions.md) for the full permission model.

## Related Documentation

- [Deployment Guide](08-deployment-guide.md) — initial setup and deploy commands
- [Authentication & Permissions](03-authentication-and-permissions.md) — SP permissions
- [Auto-Optimize](07-auto-optimize.md) — the pipeline managed by the GSO job
- [Troubleshooting](appendices/B-troubleshooting.md) — common issues
