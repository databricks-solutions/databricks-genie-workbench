---
sidebar_position: 3
description: "Common issues, causes, and fixes."
---

# Troubleshooting

## Common Issues

"Re-run your install path" below means rerunning `notebooks/install.py` from the
top (notebook path, recommended) or `./scripts/deploy.sh --update` (local
terminal path). Use whichever path you originally installed with — don't mix
them for the same app instance.

| Symptom | Cause | Fix |
|---------|-------|-----|
| App shows blank page | `frontend/dist/` missing (gitignored, not synced) | Re-run your install path |
| `Could not import module "backend.main"` | Source files missing on workspace | Re-run your install path (full sync) |
| `No dependencies file found` | `pyproject.toml` / `uv.lock` not on workspace | Re-run your install path |
| "Failed to list spaces" | Lakebase not attached | Attach a `postgres` resource in Apps UI, or re-run your install path |
| `Catalog 'X' is not accessible` | Wrong catalog or missing permissions | Verify the `catalog` widget value (notebook) or `GENIE_CATALOG` (terminal); `databricks catalogs list --profile <profile>` |
| `Invalid SQL warehouse resource` | Warehouse doesn't exist or no CAN_USE | `databricks warehouses list --profile <profile>` |
| `Maximum number of apps` | Workspace hit the 300-app limit | Delete unused apps |
| Unresolved `__GSO_*__` placeholders | The installer couldn't patch `app.yaml` | Ensure the catalog is set; check installer output for warnings |
| GSO job creation fails during deploy | Notebook path: Jobs API error or wheel build failure. Terminal path: bundle deploy failed (CLI version, auth, build) | Notebook: check the install cell output. Terminal: check `databricks bundle deploy -t app` output; ensure CLI >= 0.297.2 and `pip install build` |
| Notebook upload fails (`RESOURCE_DOES_NOT_EXIST`) | Upload path not writable by deployer | Check workspace-level permissions on the upload path |

## Permission Errors

| Symptom | Cause | Fix |
|---------|-------|-----|
| "You need CAN_EDIT or CAN_MANAGE permission" on optimize trigger | User lacks permission on the Genie Agent | Share the agent with the user (CAN_EDIT or CAN_MANAGE) |
| "The service principal does not have CAN_MANAGE" | SP not shared on the Genie Agent | Share the agent with the app's SP (CAN_MANAGE) |
| "OBO token lacks genie scope, retrying with service principal" (in logs) | User token missing `dashboards.genie` scope | This is handled automatically via SP fallback — no action needed unless SP also fails |
| Optimization job fails with catalog/schema access errors | SP lacks UC permissions on referenced data | Grant `SELECT` on referenced schemas to the SP |
| "Permission denied" on scan | User lacks access to the Genie Agent | Share the agent with the user |

## Lakebase Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| "Failed to list spaces" on first load | Lakebase not attached | Re-run your install path to auto-attach the postgres resource |
| Connection timeouts after ~1 hour | Credential refresh failed | Check logs for `generate_database_credential` errors |
| Tables not created on startup | SP lacks CONNECT or CREATE ON DATABASE | Re-run your install path to re-create the SP role and grants |
| Scan results not persisting | Lakebase write failed | Check logs for `Failed to persist scan result` |
| Agent sessions lost on restart | Lakebase not configured | Without Lakebase, sessions use in-memory storage (ephemeral) |

## GSO / Auto-Optimize Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| "GSO not configured" in health check | `GSO_JOB_ID` or `GSO_CATALOG` not set | Re-run your install path (patches `app.yaml` with the resolved job ID and catalog) |
| Optimization job never starts | Job doesn't exist or SP can't run it | Check job exists in workspace; verify SP has CAN_MANAGE on job |
| Job stuck in QUEUED | No available cluster or warehouse | Check cluster policies and warehouse availability |
| Baseline evaluation fails | Benchmark questions reference inaccessible tables | Grant SP `SELECT` on all referenced schemas |
| Run ends `SKIPPED` with `INSUFFICIENT_VALID_BENCHMARKS` | Fewer than 15 valid benchmarks remained after QC and the bounded top-up budget | Review the top-up attempt telemetry and rejected findings, then add or repair benchmark questions before rerunning |
| Run ends `FAILED` with `EVAL_INVALID` | Native Eval-Run API returned an unusable result after retries | Check the Eval-Run status in the run page; verify SP access to referenced tables |
| Patches generated but accuracy doesn't improve | Optimization strategy exhausted | Run may reach `STALLED` status — review suggestions for manual improvements |
| `__GSO_*__` values in running app | The installer didn't patch `app.yaml` before deploy | Verify the catalog value and re-run your install path |

## Debug Commands

```bash
# View app logs
databricks apps logs <app-name> --profile <profile>

# Check app status
databricks apps get <app-name> --profile <profile>

# List workspace files to verify sync
databricks workspace list /Workspace/Users/<email>/<app-name>/backend --profile <profile>

# Check GSO job status
databricks jobs get <job-id> --profile <profile>

# List GSO job runs
databricks jobs list-runs --job-id <job-id> --profile <profile>

# Check SP identity
databricks apps get <app-name> --profile <profile> | grep service_principal
```

## MLflow Tracing

:::note
`MLFLOW_EXPERIMENT_ID` is workspace-specific. The app validates it at startup and silently disables tracing if the experiment doesn't exist. To enable tracing, create an MLflow experiment and update the value in `app.yaml` before deploying.
:::

## Related Documentation

- [Deployment Guide](/docs/getting-started/deployment-guide) — deploy commands and configuration
- [Operations Guide](/docs/platform/operations) — monitoring and management
- [Authentication & Permissions](/docs/platform/authentication) — permission model
- [Environment Variables](/docs/reference/environment-variables) — full variable reference
