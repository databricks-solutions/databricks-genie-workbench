# Genie Space Optimizer

A full-stack Databricks App that automatically optimizes [Genie Spaces](https://docs.databricks.com/en/genie/index.html) through iterative, LLM-driven metadata improvement. It evaluates your Genie Space configuration against benchmark questions, identifies quality gaps across 7 dimensions, and applies targeted patches through 5 optimization levers -- all with full rollback support.

Built with [apx](https://github.com/databricks-solutions/apx) (React + FastAPI).

> **Quick links:** [Deployment Guide](DEPLOYMENT.md) | [Quickstart Guide](QUICKSTART.md) | [E2E Testing Guide](E2E_TESTING_GUIDE.md) | [Detailed Documentation](docs/genie-space-optimizer-design/00-index.md) | [Changelog](CHANGELOG.md)

---

## How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Genie Space Optimizer                           │
│                                                                     │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐       │
│  │ Preflight│──▶│ Baseline │──▶│  Lever   │──▶│ Finalize │       │
│  │ Analysis │   │ Eval     │   │  Loop    │   │ & Report │       │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘       │
│       │              │              │               │              │
│  Config scan    9 LLM judges   5 levers ×3     Repeatability      │
│  UC metadata    20 benchmarks  iterations       Final scores       │
│  Validation     7 dimensions   Auto-rollback    Deploy (opt.)      │
└─────────────────────────────────────────────────────────────────────┘
```

### Optimization Pipeline (5 Steps)

1. **Preflight** -- Validates config, collects UC metadata (REST API with Spark SQL fallback), generates/loads benchmark questions
2. **Baseline Evaluation** -- Runs ~20 benchmarks through Genie, scores with 9 judges across 7 quality dimensions, establishes "before" score
3. **Preparatory Stages + Adaptive Lever Loop** -- 4 preparatory stages (prompt matching, description enrichment, join discovery, instruction seeding), then an adaptive loop that re-clusters failures, targets the highest-impact root cause with one action group per iteration, and evaluates through a 3-gate quality check
4. **Finalize** -- 2 repeatability evaluation passes, model promotion, report generation
5. **Deploy** -- (Optional) Deploy optimized config to version control

> **Deep dive:** See [03 -- Optimization Pipeline](docs/genie-space-optimizer-design/03-optimization-pipeline.md) for the complete end-to-end walkthrough of every stage.

### Optimization Levers

| Lever | Name | What It Optimizes |
|-------|------|-------------------|
| 1 | Tables & Columns | Descriptions, visibility, column aliases |
| 2 | Metric Views | Measures, dimensions, MV YAML definitions |
| 3 | Table-Valued Functions | Parameters, TVF SQL, function signatures |
| 4 | Join Specifications | Table relationships, join columns, cardinality |
| 5 | Genie Instructions | Holistic instruction rewrite (routing, disambiguation, best practices) |

Each iteration, an **adaptive strategist** re-clusters failures, priority-scores them, and produces exactly one action group targeting the highest-impact root cause. A **reflection buffer** prevents repeating failed approaches. The loop stops on convergence, diminishing returns, or max iterations.

> **Deep dive:** See [05 -- Optimization Levers](docs/genie-space-optimizer-design/05-optimization-levers.md) for detailed coverage of each lever and preparatory stage.

### 7 Quality Dimensions (9 Scorers)

| Dimension | Target | What It Measures |
|-----------|--------|------------------|
| Syntax Validity | 98% | Generated SQL parses correctly |
| Schema Accuracy | 95% | Correct tables, columns, and joins referenced |
| Logical Accuracy | 90% | Correct aggregations, filters, GROUP BY, ORDER BY |
| Semantic Equivalence | 90% | Same business metric as expected answer |
| Completeness | 90% | All requested dimensions/measures included |
| Result Correctness | 85% | Correct final result values |
| Asset Routing | 95% | Correct asset type (table, metric view, TVF) selected |

Plus **Response Quality** (LLM analysis accuracy), **Repeatability** (variance detection across repeated runs), and an **Arbiter** (tiebreaker judge for conflicting scorer verdicts). All judges return structured JSON with `failure_type`, `blame_set`, `counterfactual_fix`, and `rationale` for direct ASI metadata flow.

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| **Backend** | Python 3.11+, FastAPI, Pydantic, Uvicorn |
| **Frontend** | React 19, TypeScript, TanStack Router & Query, Vite |
| **UI Components** | shadcn/ui, Radix UI, Tailwind CSS 4 |
| **Data** | Delta Lake (state), PostgreSQL (Lakebase), Unity Catalog |
| **ML/AI** | MLflow 3.4+, Claude Opus 4.6 (via Databricks Foundation Model API), MLflow GenAI scorers, MLflow Labeling Sessions |
| **Infrastructure** | Databricks Apps, Databricks Jobs, SQL Warehouse |
| **SDK** | Databricks SDK 0.40+, Databricks Connect 15+ |
| **Build** | apx, uv (Python), bun (JavaScript) |

---

## Project Structure

```
Genie_Space_Optimizer/
├── pyproject.toml                    # Python project config & apx metadata
├── databricks.yml                    # Databricks Asset Bundle definition
├── Makefile                          # Deployment helpers (setup, deploy, verify, clean-wheels, help)
├── app.yml                           # Databricks App entry point (uvicorn)
├── resources/
│   ├── grant_app_uc_permissions.py   # Script to grant app SP access to UC schemas
│   └── patch_app_yml.py             # Injects deploy-time env vars into .build/app.yml
├── docs/                             # Reference documentation & config samples
│
├── src/genie_space_optimizer/
│   ├── backend/                      # FastAPI backend
│   │   ├── app.py                    # App factory (registers routes, serves frontend)
│   │   ├── models.py                 # Pydantic response models
│   │   ├── router.py                 # System routes (/version, /current-user)
│   │   ├── job_launcher.py           # Databricks Jobs submission helper
│   │   ├── constants.py              # Backend-specific constants
│   │   ├── utils.py                  # Backend utility functions
│   │   ├── _spark.py                 # Serverless Spark session factory (auto-recreate on credential errors)
│   │   ├── core/                     # Dependency injection & infrastructure
│   │   │   ├── dependencies.py       # Dependencies.Client, .UserClient, .Config, etc.
│   │   │   ├── _config.py            # AppConfig (env vars → typed config)
│   │   │   ├── _factory.py           # create_app(), create_router()
│   │   │   ├── lakebase.py           # PostgreSQL/SQLModel setup
│   │   │   └── sql.py                # SQL Warehouse statement execution
│   │   └── routes/
│   │       ├── spaces.py             # GET /spaces, GET /spaces/{id}, POST /spaces/{id}/optimize
│   │       ├── runs.py               # GET /runs/{id}, comparison, iterations, ASI, provenance, apply/discard
│   │       ├── activity.py           # GET /activity (permission-filtered recent runs)
│   │       ├── settings.py           # Advisor-only permission dashboard (read-only, copyable grants)
│   │       └── trigger.py            # POST /trigger, GET /trigger/status (programmatic API)
│   │
│   ├── ui/                           # React + Vite frontend
│   │   ├── main.tsx                  # React entry point
│   │   ├── routes/                   # File-based TanStack Router pages
│   │   │   ├── __root.tsx            # Root layout (navbar, theme)
│   │   │   ├── index.tsx             # Dashboard (spaces grid, activity, stats)
│   │   │   ├── settings.tsx          # Advisor-only settings (read permissions, copyable grants)
│   │   │   ├── spaces/$spaceId.tsx   # Space detail & optimization trigger
│   │   │   ├── runs/$runId.tsx       # Run monitoring (pipeline steps, levers)
│   │   │   └── runs/$runId/comparison.tsx  # Side-by-side config diff
│   │   ├── components/               # React components
│   │   │   ├── SpaceCard.tsx         # Space summary card
│   │   │   ├── ScoreCard.tsx         # Metric display widget
│   │   │   ├── PipelineStepCard.tsx  # Pipeline step visualization
│   │   │   ├── LeverProgress.tsx     # Lever progress bar
│   │   │   ├── ConfigDiff.tsx        # Configuration comparison viewer
│   │   │   ├── ResourceLinks.tsx     # Workspace resource links
│   │   │   ├── IterationChart.tsx    # Score-over-iterations line chart
│   │   │   ├── AsiResultsPanel.tsx   # ASI failure analysis breakdown
│   │   │   ├── ProvenancePanel.tsx   # Judge → cluster → patch provenance viewer
│   │   │   ├── StageTimeline.tsx     # Pipeline stage timeline visualization
│   │   │   ├── CrossRunChart.tsx     # Cross-run score comparison chart
│   │   │   ├── ProcessFlow.tsx       # Pipeline process flow visualization
│   │   │   └── ui/                   # shadcn/ui components (incl. chart, accordion, alert-dialog)
│   │   └── lib/
│   │       ├── api.ts                # Auto-generated OpenAPI client (DO NOT edit)
│   │       ├── transparency-api.ts   # Transparency API hooks (iterations, ASI, provenance)
│   │       └── selector.ts           # Query selector helper
│   │
│   ├── common/                       # Shared utilities
│   │   ├── config.py                 # All constants (thresholds, prompts, taxonomy, noise floor)
│   │   ├── genie_client.py           # Genie Space API wrapper (list, fetch, patch, query, result DFs)
│   │   ├── genie_schema.py           # Genie Space config schema validation (lenient + strict), instruction slot budget (100 max)
│   │   ├── uc_metadata.py            # Unity Catalog introspection (REST API + Spark SQL fallback + FK extraction + TVF overlap analysis)
│   │   └── delta_helpers.py          # Delta table read/write operations
│   │
│   ├── optimization/                 # Core optimization engine
│   │   ├── optimizer.py              # Strategist, failure analysis, proposal generation, table/column enrichment, example SQL mining
│   │   ├── evaluation.py             # Benchmark generation, temporal date resolution, 9-judge scoring, MLflow tracking, expected SQL on traces
│   │   ├── applier.py                # Patch application & rollback
│   │   ├── harness.py                # Full pipeline orchestration
│   │   ├── preflight.py              # Pre-flight validation
│   │   ├── labeling.py               # MLflow labeling sessions (overwrite-based schema creation, batched trace population, console diagnostics)
│   │   ├── state.py                  # Delta-backed state machine (8 tables incl. provenance)
│   │   ├── benchmarks.py             # Benchmark question definitions
│   │   ├── repeatability.py          # Repeatability testing & variance classification
│   │   ├── report.py                 # Run report generation
│   │   ├── models.py                 # MLflow LoggedModel snapshots & metric linking
│   │   └── scorers/                  # 10 quality scorers
│   │       ├── syntax_validity.py
│   │       ├── schema_accuracy.py
│   │       ├── logical_accuracy.py
│   │       ├── semantic_equivalence.py
│   │       ├── completeness.py
│   │       ├── response_quality.py   # NL analysis accuracy judge
│   │       ├── result_correctness.py
│   │       ├── asset_routing.py
│   │       ├── repeatability.py      # Variance detection across repeated runs
│   │       └── arbiter.py            # Tiebreaker judge
│   │
│   └── jobs/                         # Databricks Job entry points
│       ├── run_preflight.py          # Task 1: config analysis & metadata collection
│       ├── run_baseline.py           # Task 2: baseline evaluation & benchmark generation
│       ├── run_lever_loop.py         # Task 3: iterative optimization (5 levers × 3 iterations)
│       ├── run_evaluation_only.py    # Standalone evaluation (called by other tasks)
│       ├── run_optimization.py       # Single-entry-point optimization runner
│       ├── run_finalize.py           # Task 4: repeatability tests & final report
│       └── run_deploy.py             # Task 5: deploy to version control (conditional)
```

---

## Documentation

Comprehensive documentation lives in [`docs/genie-space-optimizer-design/`](docs/genie-space-optimizer-design/00-index.md):

| Document | Description |
|----------|-------------|
| [00 -- Index](docs/genie-space-optimizer-design/00-index.md) | Document index and quick navigation |
| [01 -- Introduction](docs/genie-space-optimizer-design/01-introduction.md) | Purpose, audience, prerequisites |
| [02 -- Architecture](docs/genie-space-optimizer-design/02-architecture-overview.md) | System architecture and data flows |
| [03 -- Optimization Pipeline](docs/genie-space-optimizer-design/03-optimization-pipeline.md) | End-to-end optimizer deep dive |
| [04 -- Evaluation and Scoring](docs/genie-space-optimizer-design/04-evaluation-and-scoring.md) | 9 judges, benchmarks, MLflow |
| [05 -- Optimization Levers](docs/genie-space-optimizer-design/05-optimization-levers.md) | 5 levers and preparatory stages |
| [06 -- State Management](docs/genie-space-optimizer-design/06-state-management.md) | Delta tables and run lifecycle |
| [07 -- API Reference](docs/genie-space-optimizer-design/07-api-reference.md) | All endpoints, headless API, CI/CD |
| [08 -- Permissions](docs/genie-space-optimizer-design/08-permissions-and-security.md) | OBO auth, UC grants, advisor model |
| [09 -- Deployment](docs/genie-space-optimizer-design/09-deployment-guide.md) | Build, deploy, and configure |
| [10 -- Operations](docs/genie-space-optimizer-design/10-operations-guide.md) | Monitoring, diagnostics, troubleshooting |
| [Appendix A](docs/genie-space-optimizer-design/appendices/A-configuration-reference.md) | All tunable parameters |
| [Appendix B](docs/genie-space-optimizer-design/appendices/B-troubleshooting.md) | Error-solution matrix |
| [Appendix C](docs/genie-space-optimizer-design/appendices/C-references.md) | Glossary and external links |

---

## API Reference

All endpoints are prefixed with `/api/genie`. For full request/response schemas, see [07 -- API Reference](docs/genie-space-optimizer-design/07-api-reference.md).

| Method | Endpoint | Operation ID | Description |
|--------|----------|-------------|-------------|
| `GET` | `/spaces` | `listSpaces` | List Genie Spaces with quality scores |
| `GET` | `/spaces/{space_id}` | `getSpaceDetail` | Full space config, tables, instructions, history |
| `POST` | `/spaces/{space_id}/optimize` | `startOptimization` | Trigger multi-task optimization job |
| `GET` | `/runs/{run_id}` | `getRun` | Run status with 5 pipeline steps & lever detail |
| `GET` | `/runs/{run_id}/comparison` | `getComparison` | Side-by-side original vs optimized config |
| `POST` | `/runs/{run_id}/apply` | `applyOptimization` | Confirm and keep optimized config |
| `POST` | `/runs/{run_id}/discard` | `discardOptimization` | Rollback to original config |
| `GET` | `/activity` | `getActivity` | Recent optimization runs (permission-filtered to user's spaces) |
| `GET` | `/settings/permissions` | `getPermissionDashboard` | Advisor-only dashboard: schema read/write, space ACLs, copyable grant commands |
| `POST` | `/trigger` | `triggerOptimization` | Trigger optimization programmatically (headless API) |
| `GET` | `/trigger/status/{run_id}` | `getTriggerStatus` | Poll status of a triggered optimization run |
| `GET` | `/runs/{run_id}/iterations` | `getIterations` | Per-iteration scores for iteration chart |
| `GET` | `/runs/{run_id}/asi` | `getAsiResults` | ASI failure analysis breakdown |
| `GET` | `/runs/{run_id}/provenance` | `getProvenance` | End-to-end provenance (judge → cluster → patch → gate) |
| `GET` | `/pending-reviews/{space_id}` | `getPendingReviews` | Flagged questions, queued patches, labeling session URL |
| `GET` | `/version` | `getVersion` | App version |
| `GET` | `/current-user` | `getCurrentUser` | Authenticated user info |

---

## State Management

The optimizer maintains state across 8 Delta tables (partitioned by `run_id` or `space_id`):

| Table | Purpose |
|-------|---------|
| `genie_opt_runs` | Run lifecycle: status, scores, config snapshots, convergence reason, labeling session URL |
| `genie_opt_stages` | Per-stage tracking: preflight, lever iterations, finalize |
| `genie_opt_iterations` | Per-iteration scores across all 7 quality dimensions, adaptive loop reflection entries |
| `genie_opt_patches` | Individual patches: type, lever, old/new values, applied/rolled-back, provenance chain |
| `genie_eval_asi_results` | Failure assessments: type, severity, blame set, counterfactual fixes, MLflow run ID for trace linking |
| `genie_opt_provenance` | End-to-end provenance: links every patch to originating judge verdicts, clusters, and gate outcomes |
| `genie_opt_queued_patches` | High-risk patches (e.g. TVF removal) pending human approval, with confidence tier and coverage analysis |
| `genie_opt_flagged_questions` | Questions flagged for human review after exhausting automated approaches |

Run statuses: `QUEUED` → `IN_PROGRESS` → `CONVERGED` | `STALLED` | `MAX_ITERATIONS` | `FAILED` → `APPLIED` | `DISCARDED`

---

## Reviewing Results in MLflow

Every optimization run is fully tracked in MLflow for transparency and auditability.

### Experiment Location

Each Genie Space gets its own MLflow experiment at:

```
/Shared/genie-space-optimizer/<space_id>/<domain>
```

Navigate to it from the workspace sidebar (**Machine Learning** > **Experiments**), or click the **MLflow Experiment** link on the run detail page in the app.

### What's Tracked

| Artifact | Description |
|----------|-------------|
| **Traces** | One per benchmark question -- includes question, Genie SQL, expected SQL, and all 9 judge verdicts |
| **Metrics** | Per-dimension accuracy scores for each evaluation iteration |
| **Model Versions** | `LoggedModel` snapshots of the Genie Space config at each iteration; best model is promoted |
| **Assessments** | Structured scorer feedback (failure type, blame set, counterfactual fix) on each trace |

### Labeling Sessions (Human Review)

When persistent failures remain after the lever loop, an **MLflow Labeling Session** is auto-created with three review schemas: `judge_verdict_accuracy`, `corrected_expected_sql`, and `improvement_suggestions`. The session URL is displayed on the run detail page and printed in job logs. Human labels from the session are ingested on the next optimization run.

### Verifying Results

After an optimization completes:

1. **In the app**: Open the run detail page > **Comparison** view for side-by-side score diffs and config changes
2. **In MLflow**: Compare baseline (iteration 0) vs. final iteration traces to see which benchmark questions improved
3. **In the Genie Space**: Ask the same benchmark questions and compare response quality before/after

Terminal statuses: `CONVERGED` (all thresholds met), `STALLED` (no improvement), `MAX_ITERATIONS` (hit limit). After reviewing, **Apply** keeps changes live; **Discard** rolls back every patch.

> **Full guide:** See [Quickstart -- Reviewing Results in MLflow](QUICKSTART.md#11-reviewing-results-in-mlflow) for detailed navigation instructions, trace inspection, and labeling session workflows.

---

## Databricks Resources

The `databricks.yml` bundle provisions:

- **Databricks App** -- Serves the full-stack web application with OBO (on-behalf-of) user authentication. The app operates as an **advisor**: it reads permissions and shows copyable grant commands but never executes GRANT/REVOKE on the user's behalf
- **PostgreSQL Database** (Lakebase) -- `CU_1` capacity instance
- **SQL Warehouse** -- For statement execution and UC metadata queries
- **Optimization Job** -- Triggered on-demand via UI or `/trigger` API; runs preflight → baseline → lever loop → finalize pipeline

### Required OBO API Scopes

- `dashboards.genie` -- Read/write Genie Space configurations
- `files.files` -- File operations (workspace artifacts)
- `catalog.catalogs:read` -- List Unity Catalog catalogs
- `catalog.schemas:read` -- List UC schemas
- `catalog.tables:read` -- Read UC table/column metadata via REST
- `sql` -- Execute SQL statements
- `iam.access-control:read` -- Check space permissions
- `iam.current-user:read` -- Identify the authenticated user

---

## Configuration

Environment variables (set via `databricks.yml`):

| Variable | Description | Default |
|----------|-------------|---------|
| `GENIE_SPACE_OPTIMIZER_CATALOG` | Unity Catalog name for Delta state tables | `main` |
| `GENIE_SPACE_OPTIMIZER_SCHEMA` | Schema name for Delta state tables | `genie_optimization` |
| `GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID` | SQL Warehouse ID for query execution | (workspace-specific) |
| `GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT` | Seconds to wait after config patch before re-evaluation | `30` |
| `GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT_ENTITY_MATCHING` | Extra wait for entity matching propagation | `90` |
| `GENIE_SPACE_OPTIMIZER_EVAL_DEBUG` | Enable verbose evaluation logging | `true` |
| `GENIE_SPACE_OPTIMIZER_EVAL_MAX_ATTEMPTS` | Max retry attempts per evaluation query | `4` |

All optimization parameters (thresholds, rate limits, iterations, LLM config, connection pool size) are centralized in `src/genie_space_optimizer/common/config.py` and can be tuned without code changes. Notable code-level constants include `CONNECTION_POOL_SIZE` (default `20`, controls urllib3 pool for concurrent API calls) and `ENABLE_SLICE_GATE` (default `False`, per-dimension regression gate). See [Appendix A -- Configuration Reference](docs/genie-space-optimizer-design/appendices/A-configuration-reference.md) for the full parameter list.

### Permissions

The app requires three categories of permissions to run optimizations:

| Category | Scope | How to Grant |
|----------|-------|--------------|
| **Operational Schema** | SP needs 8 privileges (`USE_CATALOG`, `USE_SCHEMA`, `SELECT`, `MODIFY`, `CREATE_TABLE`, `CREATE_FUNCTION`, `CREATE_MODEL`, `EXECUTE`) on the optimizer's own schema (e.g. `main.genie_optimization`) | **Automatic** -- applied by `resources/grant_app_uc_permissions.py` during `make deploy` |
| **Data Schema** | SP needs `USE_CATALOG`, `USE_SCHEMA`, `SELECT` (and `MODIFY` if `apply_mode=both`) on every catalog/schema referenced by your Genie Space tables | **Manual** -- open the app's Settings page for copyable SQL GRANT commands |
| **Genie Space** | SP needs `CAN_MANAGE` on each Genie Space to read/modify its configuration | **Manual** -- share the Space with the SP from the Genie Space UI |

The app operates as an **advisor** -- it shows what permissions are missing with copyable grant commands on the Settings page but never executes GRANT/REVOKE itself. To find the SP identity, run `databricks apps get genie-space-optimizer -p <profile> -o json` and look for `service_principal_client_id`, or check the Settings page.

See [Quickstart -- Permission Setup Guide](QUICKSTART.md#complete-permission-setup-guide) for step-by-step instructions, or [08 -- Permissions and Security](docs/genie-space-optimizer-design/08-permissions-and-security.md) for the full permissions model.

---

## Development

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- [bun](https://bun.sh/) (JavaScript runtime)
- [apx](https://github.com/databricks-solutions/apx) CLI
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) (authenticated)

### Start Development Server

```bash
apx dev start
```

This launches backend (FastAPI), frontend (Vite), and OpenAPI watcher in detached mode. The API client auto-regenerates on backend changes.

### Common Commands

```bash
apx dev status       # Check running server status and ports
apx dev logs         # View recent logs (default: last 10m)
apx dev logs -f      # Stream logs in real-time
apx dev stop         # Stop all servers
apx dev check        # Run TypeScript + Python type checks
apx build            # Create production build
```

### Deploy to Databricks

> **Full walkthrough:** See the [Deployment Guide](DEPLOYMENT.md) for prerequisites, first-time setup, and troubleshooting.

**First-time install** (single command -- only `WAREHOUSE_ID` is required):

```bash
make setup WAREHOUSE_ID=<your-warehouse-id>
```

**Subsequent deploys** after code changes:

```bash
make deploy WAREHOUSE_ID=<your-warehouse-id>
```

Run `make help` to see all available targets and current variable values. Optional overrides: `CATALOG` (default `main`), `PROFILE` (default `DEFAULT`).

> **First-time deploy note:** On a brand-new deployment, the UC grant script skips because the app doesn't exist yet. Run `make deploy WAREHOUSE_ID=...` a second time to apply the operational schema grants. See the [Deployment Guide](DEPLOYMENT.md#6-complete-uc-grants-second-deploy) for details.

---

## License

Internal Databricks project.
