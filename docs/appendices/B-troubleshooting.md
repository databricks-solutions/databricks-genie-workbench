# Appendix B: Troubleshooting

## Common Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| App shows blank page | `frontend/dist/` missing (gitignored, not synced) | Re-run `./scripts/deploy.sh --update` |
| `Could not import module "backend.main"` | Source files missing on workspace | Re-run `./scripts/deploy.sh --update` (full sync) |
| `No dependencies file found` | `requirements.txt` not on workspace | Re-run `./scripts/deploy.sh --update` |
| "Failed to list spaces" | Lakebase not attached | Set `GENIE_LAKEBASE_INSTANCE` and re-run `./scripts/deploy.sh --update` |
| `Catalog 'X' is not accessible` | Wrong catalog or missing permissions | `databricks catalogs list --profile <profile>` |
| `Invalid SQL warehouse resource` | Warehouse doesn't exist or no CAN_USE | `databricks warehouses list --profile <profile>` |
| `Maximum number of apps` | Workspace hit the 300-app limit | Delete unused apps |
| Auto-Optimize fails at "Baseline Evaluation" with `FEATURE_DISABLED` | Prompt Registry not enabled | Contact workspace admin to enable MLflow Prompt Registry |
| Unresolved `__GSO_*__` placeholders | `deploy.sh` couldn't patch `app.yaml` | Ensure `GENIE_CATALOG` is set; check deploy output for warnings |
| GSO job creation fails during deploy | Bundle deploy failed (CLI version, auth, or build issue) | Check `databricks bundle deploy -t app` output; ensure CLI >= 0.297.2 and `pip install build` |
| Notebook upload fails (`RESOURCE_DOES_NOT_EXIST`) | `/Workspace/Shared/` not writable by deployer | Check workspace-level permissions on the upload path |

## Permission Errors

| Symptom | Cause | Fix |
|---------|-------|-----|
| "You need CAN_EDIT or CAN_MANAGE permission" on optimize trigger | User lacks permission on the Genie Space | Share the space with the user (CAN_EDIT or CAN_MANAGE) |
| "The service principal does not have CAN_MANAGE" | SP not shared on the Genie Space | Share the space with the app's SP (CAN_MANAGE) |
| "OBO token lacks genie scope, retrying with service principal" (in logs) | User token missing `dashboards.genie` scope | This is handled automatically via SP fallback — no action needed unless SP also fails |
| Optimization job fails with catalog/schema access errors | SP lacks UC permissions on referenced data | Grant `SELECT` on referenced schemas to the SP |
| "Permission denied" on scan | User lacks access to the Genie Space | Share the space with the user |

## Lakebase Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| "Failed to list spaces" on first load | Lakebase not attached | Re-run `deploy.sh --update` to auto-attach the postgres resource |
| Connection timeouts after ~1 hour | Credential refresh failed | Check logs for `generate_database_credential` errors |
| Tables not created on startup | SP lacks CONNECT or CREATE ON DATABASE | Re-run `deploy.sh --update` to re-create the SP role and grants |
| `permission denied for sequence scan_results_id_seq` | New app is reusing a Lakebase `genie` schema owned by an older app SP | Reuse the original app instance or move the new app to a fresh Lakebase project |
| Scan results not persisting | Lakebase write failed | Check logs for `Failed to persist scan result` |
| Agent sessions lost on restart | Lakebase not configured | Without Lakebase, sessions use in-memory storage (ephemeral) |

### Cross-App Lakebase Reuse

Lakebase persistence is app-instance scoped in normal operation. The app
service principal that first creates the `genie` schema owns its tables and
sequences. If a different Databricks App instance is later pointed at that
same Lakebase project, the new app service principal may not be able to write
to the existing sequences.

For product updates, keep `GENIE_APP_NAME` unchanged and run:

```bash
./scripts/deploy.sh --update
```

For a new app instance, use a fresh `GENIE_LAKEBASE_INSTANCE`. Cross-app reuse
of an existing Lakebase project is not a supported install path unless a
Lakebase project owner or workspace admin deliberately migrates ownership of
the existing `genie` schema, tables, and sequences.

## GSO / Auto-Optimize Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| "GSO not configured" in health check | `GSO_JOB_ID` or `GSO_CATALOG` not set | Re-run `./scripts/deploy.sh` (patches `app.yaml` with values from bundle state) |
| Optimization job never starts | Job doesn't exist or SP can't run it | Check job exists in workspace; verify SP has CAN_MANAGE on job |
| Job stuck in QUEUED | No available cluster or warehouse | Check cluster policies and warehouse availability |
| "Baseline Evaluation" fails | Benchmark questions reference inaccessible tables | Grant SP `SELECT` on all referenced schemas |
| "FEATURE_DISABLED" during preflight | MLflow Prompt Registry not enabled | Contact workspace admin to enable it |
| Patches generated but accuracy doesn't improve | Optimization strategy exhausted | Run may reach `STALLED` status — review suggestions for manual improvements |
| `__GSO_*__` values in running app | `deploy.sh` didn't patch `app.yaml` before deploy | Check `GENIE_CATALOG` in `.env.deploy`; re-run deploy |

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

> `MLFLOW_EXPERIMENT_ID` is workspace-specific. The app validates it at startup and silently disables tracing if the experiment doesn't exist. To enable tracing, create an MLflow experiment and update the value in `app.yaml` before deploying.

## Related Documentation

- [Deployment Guide](../08-deployment-guide.md) — deploy commands and configuration
- [Operations Guide](../09-operations-guide.md) — monitoring and management
- [Authentication & Permissions](../03-authentication-and-permissions.md) — permission model
- [Environment Variables](C-environment-variables.md) — full variable reference
