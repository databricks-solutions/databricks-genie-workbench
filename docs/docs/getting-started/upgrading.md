---
sidebar_position: 4
description: "Check your version, upgrade an existing install, and fix common upgrade problems."
---

# Upgrading

Upgrading keeps your app, its settings, its Lakebase project, and its optimizer history. You rerun the installer you used the first time, against newer code.

## 1. Find your version

**0.2.1 and later:** open the **Admin** tab, then **Settings**. The App version card shows the version, commit, install method, and deploy date.

**Earlier versions** don't show a version in the app. If Admin → Settings has no App version card, you're on a version older than 0.2.1 and should upgrade. To see exactly which code you installed:

- **Notebook install:** open the Git folder in your workspace and click the branch name to open the Git dialog. Compare the latest commit with the [releases](https://github.com/databricks-solutions/databricks-genie-workbench/releases).
- **Terminal install:** in your clone, run `git describe --tags`. Output such as `0.2-5-g1a2b3c4` means 5 commits after 0.2.

## 2. Upgrade

### Notebook install

1. In the Git folder, click **Pull**. If Pull reports a conflict in `notebooks/install.py`, discard your changes to `notebooks/install.py` and Pull again. Databricks edits the notebook when it runs, so Git sees a local change; discarding it loses nothing.
2. Open `notebooks/install.py` and check that the widgets hold the values you installed with (`app_name`, `catalog`, `warehouse_id`, and the Lakebase settings). With `lakebase_mode` set to `create` and `lakebase_project_name` left blank, the installer keeps the Lakebase project the app already uses, so leave the name blank rather than guessing it.
3. Click **Run All**.

Always Pull before Run All. Running the installer without pulling redeploys the code you already have.

### Terminal install

```bash
git checkout main
git pull
./scripts/deploy.sh --update
```

`git checkout main` matters if you checked out a release tag: on a tag, `git pull` fails with "You are not currently on a branch", and `deploy.sh --update` then redeploys the old code. To upgrade to a specific release instead of the latest `main`, run `git fetch --tags` and `git checkout <tag>` in place of the first two commands.

After either method, restart any Auto-Optimize run that was in progress during the redeploy.

## 3. What changes, by starting version

Read every section from your starting version up.

### From 0.2

- **Release check.** The Admin tab can tell you when a newer release is available. After upgrading, the first person to open the Admin tab is asked whether to turn it on. With Lakebase, the answer applies to everyone using the app. To decide at install time, use the `release_check` notebook widget or `GENIE_UPDATE_CHECK` in `.env.deploy`. See [Release update check](./deployment-guide.md#release-update-check).
- **Version Control on notebook installs.** The notebook installer now creates the Version Control tables, so the Version Control tab works on notebook installs.
- **certifi 2026.10.19.** The app now pins `certifi==2026.10.19`. From November 12, 2026, Databricks Apps blocks deployments that install certifi older than 2026.10.19, so versions before 0.2.1 can no longer be redeployed. A running app keeps running until you redeploy it.

### From 0.1

- **`ai-gateway` scope.** All LLM calls go through Unity AI Gateway by default. Each user is asked to consent to the new `ai-gateway` scope on their next visit.
- **Gateway availability.** Unity AI Gateway must be available in your workspace's region, or the installer stops at its gateway check. To keep calling serving endpoints directly, set `llm_route` to `classic` (notebook) or `GENIE_LLM_ROUTE=classic` (terminal).
- **Custom serving endpoints (terminal installs).** The gateway only knows the `databricks-*` foundation models (as `system.ai.<name>`). If `GENIE_LLM_MODEL` in `.env.deploy` names a custom serving endpoint, `deploy.sh --update` stops at the gateway check, and granting permissions won't fix it. Before upgrading, set `GENIE_LLM_ROUTE=classic` in `.env.deploy`, or switch `GENIE_LLM_MODEL` to a foundation model.
- **Model access.** If your workspace restricts who can use the `system.ai` models, grant access to both the app's users and the app's service principal. See [Model access for LLM calls](../platform/authentication.md#model-access-for-llm-calls).

### From 0.0.x

- **New optimizer engine.** Auto-Optimize runs on the second-generation optimizer engine.
- **MLflow Prompt Registry no longer required** for Auto-Optimize.
- **New name.** "Genie Space" is now "Genie Agent" throughout the app and docs.

## 4. Verify

1. Open **Admin → Settings** and confirm the version is 0.2.1 or later and the commit matches what you pulled.
2. Start an Auto-Optimize run on a test Genie Agent to confirm the optimization job works. Don't use an agent people depend on: a run applies the changes it accepts to the live agent. If you do, you can undo the changes from the run's History (see [History, revert, and discard](../features/auto-optimize.md#history-revert-and-discard)).

## 5. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Pull reports a conflict in `notebooks/install.py` | Databricks edited the notebook when it last ran | Discard your changes to that file and Pull again |
| App deploy fails installing `certifi`, from November 12, 2026 | A version older than 0.2.1 was redeployed, usually by running the installer without Pull | Pull, then Run All (or `git pull`, then `./scripts/deploy.sh --update`) |
| Installer stops at the Unity AI Gateway check | Gateway not available in the region, or no model access | Install with `llm_route=classic`, or grant model access |
| "Model unavailable or access not granted" | The model isn't reachable through the gateway for this user or the app | See [Model access for LLM calls](../platform/authentication.md#model-access-for-llm-calls) |
| Auto-Optimize won't start a run for an agent | The app's service principal can't read that agent's data schemas | Grant access as in [Per referenced data schema](../platform/authentication.md#per-referenced-data-schema) |

For anything else, see [Troubleshooting](../reference/troubleshooting.md).
