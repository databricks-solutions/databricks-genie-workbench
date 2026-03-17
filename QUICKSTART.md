# Quickstart Guide

Get the Genie Space Optimizer running in your Databricks workspace in under 15 minutes.

> **Detailed docs:** [Deployment Guide](DEPLOYMENT.md) | [Full Documentation](docs/genie-space-optimizer-design/00-index.md) | [Permissions Guide](docs/genie-space-optimizer-design/08-permissions-and-security.md) | [API Reference](docs/genie-space-optimizer-design/07-api-reference.md) | [Troubleshooting](docs/genie-space-optimizer-design/appendices/B-troubleshooting.md)

---

## 1. Prerequisites

Before you start, ensure you have the following installed and configured:


| Tool           | Version | Install                                                                                  |
| -------------- | ------- | ---------------------------------------------------------------------------------------- |
| Python         | 3.11+   | [python.org](https://www.python.org/downloads/)                                          |
| uv             | latest  | `curl -LsSf https://astral.sh/uv/install.sh | sh`                                        |
| bun            | latest  | `curl -fsSL https://bun.sh/install | bash`                                               |
| apx            | latest  | `uv tool install apx`                                                                    |
| Databricks CLI | latest  | `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh` |


### Databricks Workspace Requirements

- A Databricks workspace with **Unity Catalog** enabled
- A **SQL Warehouse** (serverless recommended) with `CAN_USE` permission
- At least one **Genie Space** to optimize
- Workspace permissions to create **Databricks Apps** and **Jobs**

### Authenticate the Databricks CLI

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

Verify with:

```bash
databricks auth env
```

---

## 2. Clone and Install Dependencies

```bash
git clone https://github.com/prashsub/Genie_Space_Optimizer.git
cd Genie_Space_Optimizer
```

Install Python dependencies:

```bash
uv sync
```

Install frontend dependencies:

```bash
apx bun install
```

---

## 3. Configure Your Workspace

No manual editing of `databricks.yml` is needed. All configuration is passed via `make` command variables at deploy time. The only required variable is `WAREHOUSE_ID`.

**Finding your Warehouse ID:** Navigate to SQL Warehouses in your workspace. The ID is the last segment of the URL: `https://<workspace>/sql/warehouses/<WAREHOUSE_ID>`.

Run `make help` to see current variable values and all available overrides:

```bash
make help
```

The defaults (`CATALOG=main`, `SCHEMA=genie_optimization`) work for most workspaces. Override only if your optimizer tables need to live elsewhere. See the [Deployment Guide](DEPLOYMENT.md#9-advanced-options) for the full list.

---

## 4. Deploy to Databricks

**First-time install** (single command):

```bash
make setup WAREHOUSE_ID=<your-warehouse-id>
```

**Subsequent deploys** after code changes:

```bash
make deploy WAREHOUSE_ID=<your-warehouse-id>
```

The `setup` target handles everything: clean stale wheels, build the bundle, start the app compute (if stopped), deploy a snapshot, and verify. The `deploy` target does the same but skips the app-start step (for existing installs).

> See the [Deployment Guide](DEPLOYMENT.md) for the complete walkthrough including prerequisites, UC grants, and troubleshooting.

This provisions:

- A **Databricks App** at `https://<workspace>/apps/genie-space-optimizer`
- A **PostgreSQL database** instance (Lakebase, CU_1 capacity)
- Two **Databricks Jobs**: the main optimization pipeline and a standalone evaluation job
- **Environment variables** automatically wired from your `databricks.yml` variables

Wait for the deployment to complete (~2-3 minutes).

### What to Expect During Deploy

Each step produces visible output:

1. **clean-wheels** -- `Removing stale wheels from workspace...` followed by `Cleanup done.`
2. **bundle deploy** -- `apx build` compiles the wheel, then `databricks bundle deploy` syncs files. Look for `Bundle deployed successfully` (or similar) at the end.
3. **apps deploy** -- `databricks apps deploy genie-space-optimizer` creates a new snapshot. Look for `Deployment started` in the output.
4. **verify** -- Prints the `.whl` filename on the workspace and `Verification passed.`

### First-Time Deploy: UC Grants Require a Second Deploy

The `make deploy` pipeline automatically runs `resources/grant_app_uc_permissions.py` as part of the build step to grant the app's service principal access to the operational schema (the schema where Delta state tables, MLflow experiments, and benchmarks are stored).

**On a brand-new deployment** (the app does not exist yet), the grant script detects this, prints a warning, and exits cleanly:

```
[grant-app-sp] App 'genie-space-optimizer' not found yet, skipping grants for now.
```

After this first deploy creates the app, **run `make deploy` a second time** so the grant script can resolve the app's service principal and apply the required UC privileges:

```bash
# First deploy: creates the app (grants are skipped)
make setup WAREHOUSE_ID=<your-warehouse-id>

# Second deploy: grants are now applied to the SP
make deploy WAREHOUSE_ID=<your-warehouse-id>
```

Alternatively, run the grant script manually after the first deploy:

```bash
python resources/grant_app_uc_permissions.py \
  --profile <your-profile> \
  --app-name genie-space-optimizer \
  --catalog <your-catalog> \
  --schema <your-schema>
```

### Verify Deployment

After deploy completes, confirm everything is healthy:

```bash
# 1. Confirm the app exists and is RUNNING
databricks apps get genie-space-optimizer -o json

# 2. Check the wheel was synced
make verify WAREHOUSE_ID=<your-warehouse-id>

# 3. Check app startup logs (look for "App wheel: genie_space_optimizer-x.y.z.whl")
databricks apps logs genie-space-optimizer
```

If the app status shows `DEPLOYING`, wait a minute and re-check. If it shows `ERROR`, inspect the logs for details.

### Grant Data Access

After deployment, the app's service principal needs access to your Unity Catalog schemas. The app operates as an **advisor** -- it shows you exactly what permissions are missing and provides **copyable SQL commands** on the Settings page. Run those commands in a SQL editor or use the grant script:

```bash
python resources/grant_app_uc_permissions.py --catalog your_catalog --schema your_schema
```

See [08 -- Permissions and Security](docs/genie-space-optimizer-design/08-permissions-and-security.md) for the complete permissions model, including required OBO scopes, SP UC grants, and Genie Space CAN_MANAGE requirements.

---

## 5. Open the App

After deployment, find your app URL:

```bash
databricks apps list
```

Or navigate to **Apps** in the Databricks workspace sidebar and click **genie-space-optimizer**.

---

## 6. Optimize Your First Genie Space

### Step 0: Configure Permissions (First Time Only)

Navigate to the **Settings** page (gear icon in the navbar). The app is an **advisor** — it reads current permissions and tells you exactly what's missing, with **copyable SQL commands** and **sharing instructions**. You run those yourself:

- **Schema Permissions** — shows per-schema read/write status for the app service principal. Copy the provided `GRANT USE CATALOG/USE SCHEMA/SELECT/MODIFY` SQL into a query editor to grant missing access.
- **Space Permissions** — shows whether the SP has CAN_MANAGE on each Genie Space. Use the provided sharing instructions to grant access from the Genie Space UI.

If you prefer using `apply_mode = both` (writes UC artifacts), ensure MODIFY is granted on the relevant schemas — the preflight stage will fail fast with a clear error if it's missing.

#### Complete Permission Setup Guide

There are **three separate permission categories** that must be satisfied before the optimizer can run. Each targets a different scope:

**A. Operational Schema Grants (automatic on deploy)**

These grants give the app's service principal access to its own state schema (`{catalog}.{schema}`, e.g. `main.genie_optimization`) where Delta state tables, MLflow experiments, and benchmark data are stored. They are applied automatically by `resources/grant_app_uc_permissions.py` during `make deploy`.

The 8 privileges granted:

| Privilege | Purpose |
|-----------|---------|
| `USE_CATALOG` | Access the catalog |
| `USE_SCHEMA` | Access the schema |
| `SELECT` | Read state tables |
| `MODIFY` | Write state tables |
| `CREATE_TABLE` | Create state tables on first run |
| `CREATE_FUNCTION` | Create UDFs if needed |
| `CREATE_MODEL` | Create MLflow models |
| `EXECUTE` | Execute stored procedures |

> If you see grant errors on the first deploy, this is expected -- see [First-Time Deploy](#first-time-deploy-uc-grants-require-a-second-deploy) above.

**B. Data Schema Grants (manual, per Genie Space)**

Your Genie Space references tables in one or more data schemas (e.g. `analytics.sales`, `finance.revenue`). The app's SP needs read access to every such schema. If you use `apply_mode = both` (writing UC artifacts like table/column descriptions), it also needs write access.

The easiest way: open the app's **Settings** page -- it detects which schemas are missing access and provides **copyable SQL** you can paste into a query editor:

```sql
-- Read access (required for all spaces)
GRANT USE CATALOG ON CATALOG `<catalog>` TO `<sp_application_id>`;
GRANT USE SCHEMA ON SCHEMA `<catalog>`.`<schema>` TO `<sp_application_id>`;
GRANT SELECT ON SCHEMA `<catalog>`.`<schema>` TO `<sp_application_id>`;
GRANT EXECUTE ON SCHEMA `<catalog>`.`<schema>` TO `<sp_application_id>`;

-- Write access (only needed if apply_mode = both)
GRANT MODIFY ON SCHEMA `<catalog>`.`<schema>` TO `<sp_application_id>`;
```

Repeat for every catalog/schema referenced by your Genie Space tables.

**C. Genie Space CAN_MANAGE (manual, per Space)**

The SP needs `CAN_MANAGE` permission on each Genie Space it will optimize. This allows it to read and modify the space configuration (instructions, sample questions, table descriptions, join specs).

To grant:

1. Open the Genie Space in the Databricks workspace
2. Click the **Share** button (or permissions icon)
3. Search for the service principal by name
4. Set the permission level to **CAN_MANAGE**

The Settings page shows which spaces are missing this permission and provides the exact SP name to search for.

**D. How to Find the Service Principal Name**

You need the SP identity to run SQL GRANT commands and to share Genie Spaces. Two ways to find it:

```bash
# Via CLI: look for "service_principal_client_id" in the output
databricks apps get genie-space-optimizer -p <your-profile> -o json
```

Or open the app's **Settings** page -- the SP identity (display name and application ID) is shown at the top.

> For SQL GRANT commands, use the `application_id` (a UUID). For Genie Space sharing dialogs, search by the SP's `display_name`.

### Step 1: Browse Your Spaces

The dashboard shows all Genie Spaces you have edit access to. Each card displays the space name, table count, and quality score (if previously evaluated).

### Step 2: Select a Space

Click a space card to view its details:

- **Tables** -- Registered tables with column counts and descriptions
- **Instructions** -- Current Genie instructions
- **Sample Questions** -- Configured example queries
- **History** -- Previous optimization runs (if any)

### Step 3: Start Optimization

Click **Optimize** to kick off the pipeline. This submits a multi-task Databricks Job with 5 stages:


| Stage         | Duration   | What Happens                                                                                         |
| ------------- | ---------- | ---------------------------------------------------------------------------------------------------- |
| Preflight     | ~1 min     | Validates config, collects UC metadata                                                               |
| Baseline Eval | ~5-15 min  | Generates 20 benchmark questions, runs through Genie, scores with 9 LLM judges                       |
| Lever Loop    | ~10-30 min | Preparatory stages (2.5/2.75/2.85/2.95), then adaptive loop: re-cluster → rank → strategist (1 AG) → apply → gate → reflect |
| Finalize      | ~5-10 min  | Repeatability testing, final scoring, report generation                                              |
| Deploy        | ~1 min     | (Optional) Deploys optimized config to version control                                               |


**Total time: 20-60 minutes** depending on space complexity and number of tables.

The lever loop now uses an **adaptive iteration cycle**: each iteration re-clusters failures, priority-scores them by impact, calls the adaptive strategist for a single targeted action group, applies patches, evaluates through 3 gates, and reflects on the outcome. A **100-instruction slot budget** is enforced (each example SQL = 1 slot, SQL function = 1, text instructions = 1) to stay within the Genie API limit. **Per-question failure persistence** is tracked across iterations — questions classified as ADDITIVE_LEVERS_EXHAUSTED trigger escalation: TVF removal (with schema overlap confidence analysis), LLM-assisted ground-truth repair, or flagging for human review. High-risk patches are queued for approval in the pending reviews panel. At the end of the loop, an MLflow labeling session is created for human review with a clickable URL link in the run detail page. Schema creation uses `overwrite=True` for simplicity, and console-visible `print()` diagnostics track each step of the labeling pipeline (schema creation, session creation, trace population, and errors).

### Step 4: Monitor Progress

Click through to the run detail page to watch the pipeline in real-time:

- **Pipeline Steps** -- 5-step progress tracker with status, duration, and summaries
- **Lever Detail** -- Per-lever status: accepted, rolled back (with reason), or skipped
- **Score Progression** -- Baseline vs. optimized scores across 7 quality dimensions

The page auto-polls every 5 seconds.

### Step 5: Review and Apply

When the pipeline completes (status: `CONVERGED`, `STALLED`, or `MAX_ITERATIONS`):

1. Navigate to the **Comparison** view
2. Review the side-by-side diff:
  - Original vs. optimized **instructions**
  - Original vs. optimized **sample questions**
  - Original vs. optimized **table descriptions**
  - Per-dimension score improvements
3. Choose an action:
  - **Apply** -- Keep the optimized configuration (marks run as `APPLIED`)
  - **Discard** -- Roll back all patches to the original configuration (marks run as `DISCARDED`)

### Step 6: Verify Optimization Quality

After the pipeline completes, use the following to evaluate whether the optimization was successful:

#### Understanding Convergence Reasons

The pipeline terminates with one of these statuses:

| Status | Meaning | What to Do |
|--------|---------|------------|
| `CONVERGED` | All 7 quality dimensions met their target thresholds | Review the comparison and **Apply** -- this is the best outcome |
| `STALLED` | No meaningful score improvement for 2+ consecutive iterations | Review the comparison -- partial improvements may still be valuable |
| `MAX_ITERATIONS` | Hit the iteration limit (default 3) without converging | Review what was improved; consider re-running with tuned thresholds |
| `FAILED` | A pipeline error occurred | Check the job logs for the error and fix the root cause |

#### Interpreting Scores

The comparison view shows before/after scores across 7 quality dimensions. Each dimension has a target threshold:

| Dimension | Target | What It Tells You |
|-----------|--------|-------------------|
| Syntax Validity | 98% | Are generated SQL queries syntactically correct? |
| Schema Accuracy | 95% | Does the SQL reference the right tables/columns? |
| Logical Accuracy | 90% | Are aggregations, filters, and GROUP BY correct? |
| Semantic Equivalence | 90% | Does the query answer the same business question? |
| Completeness | 90% | Are all requested dimensions/measures included? |
| Result Correctness | 85% | Do the query results match the expected values? |
| Asset Routing | 95% | Is the right asset type (table, metric view, TVF) selected? |

A score increase of 5%+ in any dimension is generally meaningful. Scores below the threshold are highlighted in the comparison view.

#### Apply vs. Discard

- **Apply**: The optimized instructions, sample questions, and table descriptions are already live in the Genie Space (they were patched during the lever loop). Clicking Apply marks the run as `APPLIED` and preserves the changes. If `apply_mode = both` was used, deferred UC artifact writes (table/column descriptions in Unity Catalog) are also applied at this point using your OBO credentials.
- **Discard**: Every patch applied during the lever loop is **rolled back** to restore the original Genie Space configuration. The run is marked as `DISCARDED`.

#### After Applying

Once you apply an optimization:

1. Open the Genie Space in the Databricks workspace
2. Ask the same questions you used as benchmarks (or new questions)
3. Compare the quality of responses before and after -- you should see improvements in the dimensions that were scored low at baseline
4. The optimization is idempotent: you can re-run the optimizer on the same space at any time to further improve quality

---

## 7. Deploy & Run (Quick Reference)

One-liner to build, deploy, and trigger an optimization run:

```bash
# 1. Deploy (build wheel + sync + restart app)
make deploy WAREHOUSE_ID=<your-warehouse-id>

# 2. Trigger optimization for a Genie Space
APP_URL=$(databricks apps get genie-space-optimizer -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])")
TOKEN=$(databricks auth token | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s -X POST "${APP_URL}/api/genie/trigger" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"space_id": "<GENIE_SPACE_ID>"}' | python3 -m json.tool
```

> Replace `<GENIE_SPACE_ID>` with the UUID of your Genie Space. Find it in the Genie Space URL or on the app's Dashboard.

**Poll run status:**

```bash
curl -s "${APP_URL}/api/genie/trigger/status/<RUN_ID>" \
  -H "Authorization: Bearer ${TOKEN}" | python3 -m json.tool
```

---

## 8. Programmatic API (Headless Trigger)

You can trigger optimization runs programmatically without the UI, using the `/trigger` endpoint. See [07 -- API Reference](docs/genie-space-optimizer-design/07-api-reference.md) for the complete endpoint reference with request/response schemas and CI/CD integration patterns.

### Three Ways to Initiate an Optimization

**Method 1: Via the Web UI**

1. Open the app at `https://<workspace>/apps/genie-space-optimizer`
2. Click a **Space card** on the Dashboard
3. Click the **Optimize** button on the Space detail page
4. The run detail page opens automatically -- monitor progress there

**Method 2: Via curl / CLI**

Step-by-step for shell-based triggering:

```bash
# Step 1: Get the app URL
APP_URL=$(databricks apps get genie-space-optimizer -p <your-profile> -o json \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])")
echo "App URL: ${APP_URL}"

# Step 2: Get an access token
TOKEN=$(databricks auth token -p <your-profile> \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Step 3: Trigger optimization (returns runId and jobUrl)
RESPONSE=$(curl -s -X POST "${APP_URL}/api/genie/trigger" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"space_id": "<GENIE_SPACE_ID>"}')
echo "${RESPONSE}" | python3 -m json.tool

# Step 4: Extract the run ID from the response
RUN_ID=$(echo "${RESPONSE}" | python3 -c "import sys,json; print(json.load(sys.stdin)['runId'])")

# Step 5: Poll until completion (CONVERGED, STALLED, MAX_ITERATIONS, or FAILED)
curl -s "${APP_URL}/api/genie/trigger/status/${RUN_ID}" \
  -H "Authorization: Bearer ${TOKEN}" | python3 -m json.tool
```

The trigger response includes:

| Field | Description |
|-------|-------------|
| `runId` | Unique optimization run ID (UUID) |
| `jobRunId` | Databricks Job run ID (for viewing in the Jobs UI) |
| `jobUrl` | Direct link to the running job |
| `status` | Initial status (`IN_PROGRESS`) |

The status response includes `baselineScore`, `optimizedScore`, and `convergenceReason` once available.

**Method 3: Via Python (requests)**

```python
import requests
import subprocess
import json
import time

profile = "<your-profile>"
space_id = "<GENIE_SPACE_ID>"

# Get app URL and token
app_info = json.loads(subprocess.check_output(
    ["databricks", "apps", "get", "genie-space-optimizer", "-p", profile, "-o", "json"]
))
app_url = app_info["url"]

token_info = json.loads(subprocess.check_output(
    ["databricks", "auth", "token", "-p", profile]
))
token = token_info["access_token"]

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Trigger
resp = requests.post(f"{app_url}/api/genie/trigger", json={"space_id": space_id}, headers=headers)
run_id = resp.json()["runId"]
print(f"Started run: {run_id}")

# Poll until done
while True:
    status = requests.get(f"{app_url}/api/genie/trigger/status/{run_id}", headers=headers).json()
    print(f"Status: {status['status']} | Baseline: {status.get('baselineScore')} | Optimized: {status.get('optimizedScore')}")
    if status["status"] in ("CONVERGED", "STALLED", "MAX_ITERATIONS", "FAILED"):
        break
    time.sleep(30)
```

This is useful for CI/CD pipelines or scheduled optimization workflows.

---

## 9. Local Development

For making changes to the app itself:

### Start the Dev Server

```bash
apx dev start
```

This starts three processes in the background:

- **Backend** (FastAPI) on port 8000
- **Frontend** (Vite) on port 5173
- **OpenAPI watcher** that regenerates `ui/lib/api.ts` on backend changes

### Check Your Code

```bash
apx dev check
```

Runs TypeScript and Python type checks in parallel.

### View Logs

```bash
apx dev logs -f     # Stream logs live
```

### Stop the Dev Server

```bash
apx dev stop
```

---

## 10. Key Configuration Knobs

All tunable parameters are in `src/genie_space_optimizer/common/config.py`. See [Appendix A -- Configuration Reference](docs/genie-space-optimizer-design/appendices/A-configuration-reference.md) for the complete list with defaults, environment variable overrides, and tuning guidance.

### Quality Thresholds

If the baseline evaluation already meets all thresholds, the lever loop is skipped entirely.

```python
DEFAULT_THRESHOLDS = {
    "syntax_validity": 98.0,
    "schema_accuracy": 95.0,
    "logical_accuracy": 90.0,
    "semantic_equivalence": 90.0,
    "completeness": 90.0,
    "result_correctness": 85.0,
    "asset_routing": 95.0,
}
```

### Iteration Limits

```python
MAX_ITERATIONS = 3            # Max adaptive loop iterations
REGRESSION_THRESHOLD = 5.0    # % drop that triggers rollback
SLICE_GATE_TOLERANCE = 15.0   # % tolerance for per-dimension slice gate
PLATEAU_ITERATIONS = 2        # Consecutive no-improvement before stopping
MAX_NOISE_FLOOR = 5.0         # Min score improvement to accept (below = cosmetic noise)
DIMINISHING_RETURNS_EPSILON = 2.0  # Stop when last N accepted iters each < this %
DIMINISHING_RETURNS_LOOKBACK = 2   # How many recent accepted iterations to check
GENIE_CORRECT_CONFIRMATION_THRESHOLD = 2  # Evals confirming genie_correct before auto-correction
NEITHER_CORRECT_REPAIR_THRESHOLD = 2      # neither_correct verdicts before GT repair attempt
NEITHER_CORRECT_QUARANTINE_THRESHOLD = 3  # Consecutive neither_correct + failed repair → quarantine
PERSISTENCE_MIN_FAILURES = 2              # Evals before question appears in persistence summary
TVF_REMOVAL_MIN_ITERATIONS = 2            # Consecutive failing iterations before TVF removal considered
TVF_REMOVAL_BLAME_THRESHOLD = 2           # Iterations TVF blamed in ASI for auto-removal
```

### Benchmark Generation

```python
TARGET_BENCHMARK_COUNT = 20  # Number of evaluation questions to generate
```

### Rate Limiting

```python
RATE_LIMIT_SECONDS = 12                         # Delay between Genie API calls
PROPAGATION_WAIT_SECONDS = 30                   # Wait after config patch before evaluation
PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS = 90   # Extra wait for entity matching propagation
```

These can also be overridden via environment variables (`GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT`, `GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT_ENTITY_MATCHING`).

### LLM

```python
LLM_ENDPOINT = "databricks-claude-opus-4-6"  # Foundation Model API endpoint
LLM_TEMPERATURE = 0                           # Deterministic output
```

---

## 11. Reviewing Results in MLflow

Every optimization run is tracked in MLflow with full traceability -- from benchmark questions to judge verdicts to applied patches. This section explains how to navigate the MLflow experiment, inspect traces, and use labeling sessions for human review.

### Finding the MLflow Experiment

Each Genie Space gets its own MLflow experiment at a predictable path:

```
/Shared/genie-space-optimizer/<space_id>/<domain>
```

For example: `/Shared/genie-space-optimizer/01f10e84df3b14d993c30773abde7f44/revenue_property`

Three ways to navigate to it:

1. **From the app**: On the run detail page, click the **"MLflow Experiment"** link in the Resource Links section at the bottom of the page.
2. **From the workspace sidebar**: Navigate to **Machine Learning** > **Experiments** > browse to `/Shared/genie-space-optimizer/`.
3. **Direct URL**: `https://<workspace>/ml/experiments/<experiment_id>` (the experiment ID is shown in the run detail page).

### What's Inside the Experiment

Each evaluation iteration (baseline, lever loop iterations, repeatability) creates an **MLflow run** within the experiment. Each run contains:

| Artifact | Description |
|----------|-------------|
| **Traces** | One trace per benchmark question -- contains the question, Genie-generated SQL, expected SQL, and all judge verdicts |
| **Metrics** | Per-dimension accuracy scores (syntax_validity, schema_accuracy, etc.) |
| **Model Versions** | A `LoggedModel` snapshot of the Genie Space config at each iteration |
| **Assessments** | Scorer feedback (rationale, failure type, blame set, counterfactual fix) attached to each trace |

The baseline run (iteration 0) establishes the "before" scores. Subsequent runs from the lever loop show how scores changed with each optimization iteration. The **best model** (highest overall accuracy) is promoted at the end of the pipeline.

### Navigating Traces

To inspect individual benchmark evaluations:

1. Open the MLflow experiment
2. Click an MLflow run (e.g. `genie_eval_iter0_...` for baseline, `genie_eval_iter1_...` for first iteration)
3. Go to the **Traces** tab
4. Each trace shows:
   - **Input**: The benchmark question
   - **Output**: The Genie-generated SQL and results
   - **Assessments**: All 9 judge verdicts with pass/fail, rationale, failure type, and blame set

Assessments follow a structured format: each judge returns `failure_type` (e.g. `WRONG_TABLE`, `MISSING_FILTER`), `blame_set` (which metadata element caused the failure), and `counterfactual_fix` (what change would fix it). This metadata feeds directly into the adaptive strategist's next iteration.

### Labeling Sessions (Human Review)

When the lever loop completes and persistent failures remain (questions that failed across multiple iterations despite optimization attempts), the optimizer automatically creates an **MLflow Labeling Session** for human review.

**Where to find it:**

- The labeling session URL is displayed on the **run detail page** in the app (in the Resource Links section).
- It is also printed in the job logs: `[MLflow Review] Labeling session created for human review`.
- In MLflow, labeling sessions appear under the experiment's **Labeling** tab.

**What you can review:**

The labeling session includes three review schemas:

| Schema | What You Review |
|--------|-----------------|
| `judge_verdict_accuracy` | Were the LLM judges correct? Mark verdicts as accurate or inaccurate |
| `corrected_expected_sql` | Is the benchmark's expected SQL correct? Provide corrected SQL if not |
| `improvement_suggestions` | Free-form suggestions for improving the Genie Space |

**How to use it:**

1. Click the labeling session link from the run detail page
2. Review each flagged trace -- the session pre-loads traces for questions with persistent failures
3. Add your labels (verdict corrections, SQL fixes, suggestions)
4. On the next optimization run, the optimizer ingests your human feedback and uses it to improve benchmark accuracy and optimization strategy

### Cross-Run Comparison

To compare results across multiple optimization runs on the same space:

1. Open the MLflow experiment
2. Select multiple runs (checkboxes)
3. Use the **Compare** view to see how scores evolved over time
4. The app's **Iteration Chart** (on the run detail page) also visualizes score progression across iterations within a single run

---

## Troubleshooting

### "Optimization job not found"

The Databricks Job hasn't been deployed yet. Run:

```bash
databricks bundle deploy -p <your-profile>
```

### "Space not found" on the dashboard

You need **CAN_EDIT** permission on the Genie Space. The app filters to spaces you can modify.

### Delta tables not available

On the first run, Delta state tables are created automatically in the configured catalog/schema. If you see "Delta tables not yet available" warnings, this is normal for first use -- they will appear after the first optimization completes.

### Preflight fails with "information_schema permission" errors

The preflight stage now uses the Unity Catalog **REST API** by default to fetch column and routine metadata, bypassing `system.information_schema` permission requirements. If the REST API fails, it falls back to Spark SQL automatically. If both fail, ensure the app's service principal has `USE CATALOG` and `USE SCHEMA` grants on the target catalog/schema (see the **Settings** page or `resources/grant_app_uc_permissions.py`).

### Benchmark dates look wrong (stale "this year" / "last quarter")

The optimizer auto-detects relative time references in benchmark questions ("this year", "YTD", "last quarter", "last N months/days") and rewrites the ground-truth SQL date literals to match the current date. This is automatic and requires no manual intervention. If you still see date mismatches, the question may contain an explicit year (e.g., "for 2024") which is intentionally not rewritten.

### Evaluation takes a long time

Each benchmark question requires a round-trip to the Genie API (with rate limiting and exponential backoff on 429 rate limits) plus LLM judge evaluation. For 20 questions with 9 judges, expect ~15 minutes for a full evaluation pass. You can reduce `TARGET_BENCHMARK_COUNT` in `config.py` for faster iteration during testing.

### Spark session errors ("InvalidAccessKeyId", "ExpiredToken")

The backend automatically detects stale AWS STS credentials and recreates the Spark session. If you see transient credential errors in logs, they should self-heal within one retry. Persistent errors indicate the workspace's credential vending is misconfigured.

### Delta "SCHEMA_CHANGE_SINCE_ANALYSIS" errors

The Delta state tables may be dropped and recreated by upstream pipeline tasks. The `read_table` helper automatically retries with `REFRESH TABLE` on schema change errors. If persistent, check that concurrent optimization runs aren't conflicting.

### Stale wheel after deployment (optimization runs old code)

The `apx build` step generates a `.build/.gitignore` with `*` that blocks wheel sync. The `make deploy` pipeline handles this by removing the `.gitignore`, running `databricks bundle deploy` (file sync), and then `databricks apps deploy` (snapshot + restart). Always use `make deploy` to avoid stale wheels:

```bash
# Full redeploy (clean + bundle deploy + app deploy + verify)
make deploy WAREHOUSE_ID=<your-warehouse-id>

# Or just verify the current state
make verify WAREHOUSE_ID=<your-warehouse-id>
```

The app logs the resolved wheel at startup (`App wheel: genie_space_optimizer-x.y.z.whl (size=... bytes)`). Check app logs to confirm the correct wheel is loaded:

```bash
databricks apps logs genie-space-optimizer
```

### Many benchmarks flagged as "temporal-stale"

The optimizer auto-detects benchmarks whose ground-truth SQL returns 0 rows (typically due to stale temporal references like "this year" or "last quarter" whose date ranges no longer match any data). These benchmarks are flagged `temporal_stale=True` and excluded from the accuracy denominator so they don't penalize the score. If you see many benchmarks flagged, consider regenerating benchmarks with fresh date-relative queries, or check that the underlying data actually contains records for the referenced time periods.

### "Connection pool is full, discarding connection" warnings

The optimizer automatically configures a larger urllib3 connection pool (`CONNECTION_POOL_SIZE = 20`) at job startup to handle concurrent API calls during evaluation. If you still see these warnings, increase `CONNECTION_POOL_SIZE` in `src/genie_space_optimizer/common/config.py`.

### Score didn't improve

Not all Genie Spaces benefit from metadata optimization. If the baseline score is already high (>90%), there may be limited room for improvement. The optimizer will report `STALLED` or `CONVERGED` and you can review the comparison to see what (if any) changes were attempted.

---

## Architecture Overview

```
┌─────────────────────┐       ┌─────────────────────┐
│   Browser (React)   │◄─────▶│   FastAPI Backend    │
│                     │  API   │   /api/genie/*       │
│  Dashboard          │       │                     │
│  Space Detail       │       │  ┌─────────────────┐│
│  Run Monitor        │       │  │ Dependencies     ││
│  Comparison Diff    │       │  │ .UserClient (OBO)││
│                     │       │  │ .Config (env)    ││
└─────────────────────┘       │  │ .Spark (serverless)│
                              │  └─────────────────┘│
                              └──────────┬──────────┘
                                         │ triggers
                              ┌──────────▼──────────┐
                              │  Databricks Job      │
                              │  (Multi-task)        │
                              │                     │
                              │  preflight           │
                              │    ↓                 │
                              │  baseline_eval       │
                              │    ↓                 │
                              │  lever_loop          │
                              │    ↓                 │
                              │  finalize            │
                              │    ↓                 │
                              │  deploy (optional)   │
                              └──────────┬──────────┘
                                         │ reads/writes
                    ┌────────────────────┼────────────────────┐
                    │                    │                    │
           ┌────────▼───────┐   ┌────────▼───────┐   ┌──────▼───────┐
           │  Delta Tables   │   │  Genie API     │   │  MLflow      │
           │  (5 state       │   │  (Space CRUD,  │   │  (Experiment │
           │   tables)       │   │   query exec)  │   │   tracking)  │
           └────────────────┘   └────────────────┘   └──────────────┘
```

**Data flow:**

1. User clicks **Optimize** in the React frontend
2. Backend creates a `QUEUED` run in Delta and submits the Databricks Job
3. Job tasks execute sequentially: preflight → baseline → lever loop → finalize
4. Each task reads/writes Delta state tables and calls the Genie API
5. LLM judges (via Foundation Model API) score each evaluation
6. Frontend polls the backend every 5 seconds for updated pipeline status
7. On completion, user reviews the comparison and applies or discards

