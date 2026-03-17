# Deployment Guide

Deploy the Genie Space Optimizer to any Databricks workspace. The only required input is your **SQL Warehouse ID**.

> **Related docs:** [README](README.md) | [Quickstart Guide](QUICKSTART.md) | [E2E Testing Guide](E2E_TESTING_GUIDE.md) | [Permissions Guide](docs/genie-space-optimizer-design/08-permissions-and-security.md)

---

## 1. Prerequisites

### Local Tools

| Tool           | Install                                                                                  |
| -------------- | ---------------------------------------------------------------------------------------- |
| Python 3.11+   | [python.org](https://www.python.org/downloads/)                                          |
| uv             | `curl -LsSf https://astral.sh/uv/install.sh \| sh`                                      |
| bun            | `curl -fsSL https://bun.sh/install \| bash`                                              |
| apx            | `uv tool install apx`                                                                    |
| Databricks CLI | `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh \| sh` |
| make           | Pre-installed on macOS and Linux                                                         |

### Databricks Workspace

- **Unity Catalog** enabled
- A **SQL Warehouse** (serverless recommended)
- At least one **Genie Space** to optimize
- Permission to create **Databricks Apps** and **Jobs**

---

## 2. Find Your Warehouse ID

Navigate to your Databricks workspace > **SQL Warehouses** > click a warehouse. The ID is the last segment of the URL:

```
https://<workspace>.cloud.databricks.com/sql/warehouses/<WAREHOUSE_ID>
```

Copy this value -- it is the only required input for deployment.

---

## 3. Authenticate the Databricks CLI

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

Verify with:

```bash
databricks current-user me
```

If you use a named CLI profile, pass `PROFILE=<name>` to all `make` commands below.

---

## 4. Clone and Install

```bash
git clone https://github.com/prashsub/Genie_Space_Optimizer.git
cd Genie_Space_Optimizer
uv sync && apx bun install
```

---

## 5. Deploy (Single Command)

```bash
make setup WAREHOUSE_ID=<your-warehouse-id>
```

This single command:

1. Validates your CLI authentication and `WAREHOUSE_ID`
2. Builds the Python wheel and React frontend
3. Patches the app config with your warehouse/catalog/schema values
4. Grants the app's service principal access to Unity Catalog
5. Deploys the bundle to your workspace
6. Starts the app compute (if stopped)
7. Creates a deployment snapshot and starts the app
8. Verifies the wheel is present on the workspace
9. Prints the app URL

Wait ~2-3 minutes for the full pipeline to complete.

---

## 6. Complete UC Grants (Second Deploy)

On the first deploy, the UC grant script skips because the app doesn't exist yet:

```
[grant-app-sp] App 'genie-space-optimizer' not found yet, skipping grants for now.
```

After the first deploy creates the app, run a second deploy to apply grants:

```bash
make deploy WAREHOUSE_ID=<your-warehouse-id>
```

This applies the 8 UC privileges the service principal needs on the operational schema (`USE_CATALOG`, `USE_SCHEMA`, `SELECT`, `MODIFY`, `CREATE_TABLE`, `CREATE_FUNCTION`, `CREATE_MODEL`, `EXECUTE`).

---

## 7. Open the App

The app URL is printed at the end of `make setup`. You can also find it with:

```bash
databricks apps get genie-space-optimizer -o json | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))"
```

### First-Time Setup in the App

1. Go to the **Settings** page (gear icon in the header)
2. Verify **Framework Resources** shows the correct catalog and schema
3. For each Genie Space you want to optimize:
   - **Grant SP access to data schemas** -- the Settings page shows which schemas are missing access and provides copyable SQL `GRANT` commands. Run them in a SQL editor.
   - **Share the Genie Space with the SP** -- the Settings page shows the SP name. Open the Genie Space > Share > add the SP with **CAN_MANAGE** permission.

---

## 8. Subsequent Deploys

After code changes or updates:

```bash
make deploy WAREHOUSE_ID=<your-warehouse-id>
```

To see all available targets and current variable values:

```bash
make help
```

---

## 9. Advanced Options

These variables have sensible defaults and rarely need overriding:

| Variable  | Default                | Override when...                            |
| --------- | ---------------------- | ------------------------------------------- |
| `CATALOG` | `main`                 | Your optimizer tables live in another catalog |
| `PROFILE` | `DEFAULT`              | You use a named Databricks CLI profile       |

Example with overrides:

```bash
make setup WAREHOUSE_ID=abc123 CATALOG=my_catalog PROFILE=my-profile
```

---

## 10. Troubleshooting

### `WAREHOUSE_ID is required`

You forgot to pass it. Every `make setup` and `make deploy` command requires `WAREHOUSE_ID=...`.

### `Could not detect deployer email`

The Databricks CLI is not authenticated or the profile doesn't exist. Run `databricks auth login` and try again.

### `PERMISSION_DENIED: User does not have CREATE SCHEMA`

Safe to ignore. This means the app's service principal tried to create the operational schema but lacks `CREATE SCHEMA` on the catalog. If the schema already exists, the app starts normally. If it doesn't exist, create it manually:

```sql
CREATE SCHEMA IF NOT EXISTS main.genie_optimization;
```

### `App Not Available` in the browser

The app compute may be stopped. Start it with:

```bash
databricks apps start genie-space-optimizer
```

Then wait ~1 minute for it to become active. `make setup` handles this automatically; `make deploy` does not (use `make setup` for first-time installs).

### UC grants skipped on first deploy

Expected behavior. Run `make deploy WAREHOUSE_ID=...` a second time after the app is created.

### `App name must be between 2 and 30 characters`

If you override `APP_NAME`, keep it between 2 and 30 characters with only lowercase letters, numbers, and hyphens.

---

## Makefile Reference

| Target         | Description                                                |
| -------------- | ---------------------------------------------------------- |
| `make help`    | Show usage, all targets, and current variable values       |
| `make setup`   | First-time install: deploy + start app + verify            |
| `make deploy`  | Subsequent deploys: clean + bundle + app deploy + verify   |
| `make verify`  | Check that the workspace has the expected wheel            |
| `make clean-wheels` | Remove stale wheels from the workspace                |
