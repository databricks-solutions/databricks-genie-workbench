# Web Terminal Install Path

Install Genie Workbench from inside Databricks when local CLI usage is
blocked or inconvenient. This path still uses the Databricks CLI, but the
CLI runs in Databricks Web Terminal with current-user workspace auth.

For the laptop-driven path, see [08-deployment-guide.md](08-deployment-guide.md).

## When to pick which path

| Constraint | Local CLI (`install.sh`) | Web Terminal |
|---|---|---|
| Can install Databricks CLI + Node.js + `uv` locally | Yes | Works too |
| Local VM blocks Databricks CLI | No | Yes |
| Want fastest redeploy after local edits | Yes | No |
| Need everything to run inside the Databricks workspace | No | Yes |
| Web Terminal is disabled by workspace policy | Yes | No |

Both paths run the same installer and deploy script. They produce the same
Databricks App, optimization job, UC schema/tables, Lakebase configuration,
and app resources.

## Prerequisites

- A Databricks workspace with Databricks Apps enabled.
- Web Terminal enabled on supported compute. See the Databricks Web
  Terminal docs for compute requirements and limitations.
- Permission to create Databricks Apps.
- A SQL Warehouse ID.
- A Unity Catalog where you have `CREATE SCHEMA`.
- Node.js, npm, Python, `uv`, and the Databricks CLI available in the
  Web Terminal environment.
- Permission to create or use a Lakebase Autoscaling project for persistent
  app state. The guided installer can select an existing project, create a new
  one during deploy, or skip persistence.
- Optional: an MLflow experiment ID for tracing.

## Step 1 - Open Web Terminal

1. Open or create supported Databricks compute.
2. Launch Web Terminal from the compute page or an attached notebook.
3. Confirm current-user CLI auth works:

```bash
databricks current-user me
```

Web Terminal uses environment-provided current-user auth. Do not run
`databricks auth login` or configure profiles for this path.

## Step 2 - Clone Or Enter The Repo

Use a Git folder or clone from the terminal:

```bash
cd /Workspace/Users/<your-email>
git clone https://github.com/databricks-solutions/databricks-genie-workbench.git
cd databricks-genie-workbench
```

If the repo already exists as a Databricks Git folder, `cd` into that
folder instead.

## Step 3 - Run The Installer

Tell the deploy scripts to use current-user auth by leaving the profile
empty:

```bash
export GENIE_DEPLOY_PROFILE=""
./scripts/install.sh
```

The installer writes `.env.deploy` with `GENIE_DEPLOY_PROFILE=""`, then
runs `scripts/deploy.sh`. The deploy performs the same steps as the local
CLI path:

- Builds the frontend with `npm ci && npm run build`.
- Creates or updates the Databricks App.
- Syncs app code to the workspace.
- Deploys the GSO optimization job with `databricks bundle deploy -t app`.
- Runs `scripts/setup_workbench.py` for UC grants, Lakebase, app resources,
  `app.yaml` patching, job permissions, and optional Genie Space grants.
- Deploys the app from the synced workspace folder.

## Updating Later

From the same Web Terminal checkout:

```bash
git pull
./scripts/deploy.sh --update
```

For normal updates, keep the same `GENIE_APP_NAME` and
`GENIE_LAKEBASE_INSTANCE` in `.env.deploy`. If you intentionally create a new
Databricks App instance, use a fresh Lakebase project name instead of pointing
the new app at an older app's Lakebase project.

If you change workspace resources such as catalog, warehouse, or MLflow
settings, edit `.env.deploy` or re-run `./scripts/install.sh`.

## Troubleshooting

### `Cannot authenticate with Databricks CLI`

Run:

```bash
databricks current-user me
```

If that fails, confirm you are in Databricks Web Terminal on supported
compute. If you are running locally instead, set `GENIE_DEPLOY_PROFILE` to
a configured local profile.

### `databricks bundle deploy` fails

Confirm the Web Terminal environment has `uv`, Python, and the required
Databricks CLI version. Then re-run:

```bash
./scripts/deploy.sh --update
```

### Web Terminal session disconnects

Databricks Web Terminal sessions can time out. Reopen the terminal, return
to the repo directory, and re-run the last command. The deploy scripts are
idempotent.

### App reaches RUNNING but cannot access data

The app service principal still needs access to the data schemas referenced
by your Genie Spaces. Open the app's Auto-Optimize settings to see which
schemas need grants, then ask a catalog owner to grant the required
permissions.

### IQ scan fails to persist

If the app logs show `permission denied for sequence scan_results_id_seq`,
the new app instance is probably using a Lakebase project whose `genie`
schema was created by an older app service principal. Reuse the original app
instance for updates, or move the new app to a fresh Lakebase project. See
[Appendix B: Troubleshooting](appendices/B-troubleshooting.md#cross-app-lakebase-reuse).

## References

- Databricks docs: [Run shell commands in Databricks web terminal](https://docs.databricks.com/aws/en/compute/web-terminal)
- Databricks docs: [Create and manage Git folders](https://docs.databricks.com/aws/en/repos/git-operations-with-repos.html)
- Databricks docs: [Deploy a Databricks app](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/deploy)
- Local CLI install path: [08-deployment-guide.md](08-deployment-guide.md)
