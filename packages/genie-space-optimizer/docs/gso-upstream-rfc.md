# RFC: GSO Changes for Genie Workbench Integration

> **Status:** Draft
> **Author:** Genie Workbench team
> **Date:** 2026-03-16
> **GSO Repo:** https://github.com/prashsub/Genie_Space_Optimizer

## Summary

The Genie Workbench is integrating the Genie Space Optimizer (GSO) as its
third feature alongside GenieRx and GenieIQ. The integration embeds the GSO
package via git subtree at `packages/genie-space-optimizer/` and calls GSO
functions from a thin FastAPI proxy router.

During the integration audit, we identified five changes needed in the
upstream GSO repo to support a clean, maintainable integration. These changes
benefit both the standalone GSO app (no regressions) and the embedded
integration (no workarounds).

See `docs/gso-architecture.md` in the Workbench repo for the full
integration architecture.

---

## RFC-1: Integration API Module

### Problem

The Workbench needs to call GSO functions for trigger, apply, and discard
operations. Currently, the only entry points are:

- `do_start_optimization()` in `backend/routes/spaces.py` — usable but
  tightly coupled to Spark Connect (see RFC-2)
- `apply_optimization()` in `backend/routes/runs.py` (line ~1542) — a
  FastAPI route handler decorated with `@router.post` and using DI params
  (`Dependencies.UserClient`, `Dependencies.Config`)
- `discard_optimization()` in `backend/routes/runs.py` (line ~1636) — same
  problem: FastAPI DI params, not callable as a plain function

The Workbench cannot import and call these route handlers because FastAPI's
dependency injection (`Dependencies.UserClient`, `Dependencies.Config`) will
not resolve outside the GSO's own app context. The Workbench has its own
auth system (ContextVar-based OBO middleware) and config (env vars, no
pydantic-settings prefix).

### Proposed Change

Create a new `integration` subpackage that exports standalone functions
with explicit arguments:

```
src/genie_space_optimizer/
├── integration/
│   ├── __init__.py           # Public API surface
│   ├── trigger.py            # trigger_optimization()
│   ├── apply.py              # apply_optimization()
│   ├── discard.py            # discard_optimization()
│   ├── levers.py             # get_lever_info()
│   └── config.py             # IntegrationConfig dataclass
└── ...existing modules...
```

Public API:

```python
# genie_space_optimizer/integration/__init__.py
from .trigger import trigger_optimization
from .apply import apply_optimization
from .discard import discard_optimization
from .levers import get_lever_info, get_default_lever_order
from .config import IntegrationConfig
```

### `IntegrationConfig` Dataclass

A lightweight config object that callers construct directly, without
pydantic-settings env-prefix magic:

```python
from dataclasses import dataclass

@dataclass
class IntegrationConfig:
    """Minimal configuration for GSO integration callers."""
    catalog: str
    schema_name: str
    warehouse_id: str
    job_id: int | None = None
```

This replaces the need for the caller to construct a full `AppConfig`
(which requires `GENIE_SPACE_OPTIMIZER_`-prefixed env vars and
pydantic-settings).

### Function Signatures

```python
# integration/trigger.py
def trigger_optimization(
    space_id: str,
    ws: WorkspaceClient,
    sp_ws: WorkspaceClient,
    config: IntegrationConfig,
    *,
    user_email: str | None = None,
    user_name: str | None = None,
    apply_mode: str = "genie_config",
    levers: list[int] | None = None,
    deploy_target: str | None = None,
) -> TriggerResult: ...

# integration/apply.py
def apply_optimization(
    run_id: str,
    ws: WorkspaceClient,
    config: IntegrationConfig,
) -> ActionResult: ...

# integration/discard.py
def discard_optimization(
    run_id: str,
    ws: WorkspaceClient,
    sp_ws: WorkspaceClient,
    config: IntegrationConfig,
) -> ActionResult: ...

# integration/levers.py
def get_lever_info() -> list[dict]:
    """Return lever metadata: [{id, name, description}, ...]."""

def get_default_lever_order() -> list[int]:
    """Return [1, 2, 3, 4, 5]."""
```

Return types:

```python
@dataclass
class TriggerResult:
    run_id: str
    job_run_id: str
    job_url: str | None
    status: str  # "IN_PROGRESS"

@dataclass
class ActionResult:
    status: str   # "applied" or "discarded"
    run_id: str
    message: str
```

### Impact on Standalone App

None. The existing route handlers continue to work unchanged. The
integration module extracts and reuses the same internal logic
(`_sql_warehouse_*` helpers, `rollback()`, `submit_optimization()`, etc.)
through a new public surface.

---

## RFC-2: Warehouse-First Mode for Trigger

### Problem

`do_start_optimization()` (spaces.py line 751) requires Spark Connect for
Delta state management:

- `ensure_optimization_tables(spark, ...)` — DDL via `spark.sql()`
- `load_runs_for_space(spark, ...)` — Delta read via `spark.sql()`
- `create_run(spark, ...)` — Delta insert via `spark.sql()`
- `update_run_status(spark, ...)` — Delta update via `spark.sql()`

The function already has a warehouse fallback (lines 801-866): when Spark
credential errors occur, it falls back to `_sql_warehouse_query()`,
`_wh_create_run()`, and `_sql_warehouse_execute()`, which use the
Statement Execution API. This fallback works correctly.

The Workbench runtime does NOT have `databricks-connect` installed and
cannot create Spark sessions. The Workbench has a SQL Warehouse available
via `SQL_WAREHOUSE_ID` / `GSO_WAREHOUSE_ID`.

### Proposed Change

In `integration/trigger.py`, implement `trigger_optimization()` using
the **warehouse path as the primary path** (not a fallback):

```python
def trigger_optimization(
    space_id: str,
    ws: WorkspaceClient,
    sp_ws: WorkspaceClient,
    config: IntegrationConfig,
    *,
    user_email: str | None = None,
    user_name: str | None = None,
    apply_mode: str = "genie_config",
    levers: list[int] | None = None,
    deploy_target: str | None = None,
) -> TriggerResult:
    """Trigger a GSO optimization run using SQL Warehouse for state management.

    This function does NOT require Spark Connect. All Delta state operations
    use the Statement Execution API via the configured SQL Warehouse.
    """
    # 1. Validate apply_mode
    # 2. Check user permission (OBO) — user_can_edit_space(ws, space_id, ...)
    # 3. Check active runs — _sql_warehouse_query(ws, warehouse_id, SELECT ...)
    # 4. Reconcile stale runs — _sql_warehouse_execute(ws, warehouse_id, UPDATE ...)
    # 5. Fetch space config snapshot — fetch_space_config(ws, space_id) [REST, no Spark]
    # 6. Fetch UC metadata — _fetch_uc_metadata_obo(ws, ...) [Statement Execution API]
    # 7. Check SP permissions — sp_can_manage_space(sp_ws, ...) [REST]
    # 8. Create QUEUED run — _wh_create_run(ws, warehouse_id, ...)
    # 9. Submit Databricks Job — submit_optimization(sp_ws, ...) [Jobs API]
    # 10. Update to IN_PROGRESS — _sql_warehouse_execute(ws, warehouse_id, UPDATE ...)
    # 11. Return TriggerResult
```

The existing helper functions can be reused directly:

| Helper | Location | Spark Required? |
|--------|----------|-----------------|
| `_sql_warehouse_query()` | `backend/routes/spaces.py:45` | No |
| `_sql_warehouse_execute()` | `backend/routes/spaces.py:80` | No |
| `_wh_create_run()` | `backend/routes/spaces.py:100` | No |
| `fetch_space_config()` | `common/genie_client.py` | No |
| `user_can_edit_space()` | `common/genie_client.py` | No |
| `sp_can_manage_space()` | `common/genie_client.py` | No |
| `submit_optimization()` | `backend/job_launcher.py` | No |

These helpers should be moved to or re-exported from the integration module
so they are part of the public API.

### Impact on Standalone App

None. `do_start_optimization()` in `spaces.py` is unchanged. The
standalone app continues using Spark Connect as the primary path. The
integration module provides a parallel warehouse-first path.

---

## RFC-3: Extract Apply/Discard Business Logic

### Problem

The apply and discard route handlers in `runs.py` use Spark Connect for
all Delta operations and have no warehouse fallback:

**`apply_optimization`** (line 1542):
```python
spark = get_spark()
run_data = load_run(spark, run_id, ...)              # Spark
iters_df = load_iterations(spark, run_id, ...)       # Spark
stages_df = load_stages(spark, run_id, ...)          # Spark
uc_apply_result = _apply_deferred_uc_writes_obo(...) # Spark
update_run_status(spark, run_id, ..., "APPLIED")     # Spark
```

**`discard_optimization`** (line 1636):
```python
spark = get_spark()
run_data = load_run(spark, run_id, ...)              # Spark
rollback(apply_log, client, space_id)                # REST (no Spark)
update_run_status(spark, run_id, ..., "DISCARDED")   # Spark
```

The business logic is straightforward but entangled with Spark. The
`rollback()` call itself is already REST-based (calls `patch_space_config()`
via Genie API), proving the core logic doesn't inherently need Spark.

### Proposed Change

Create warehouse-backed equivalents in the integration module:

**`integration/apply.py`:**
```python
def apply_optimization(
    run_id: str,
    ws: WorkspaceClient,
    config: IntegrationConfig,
) -> ActionResult:
    """Apply an optimization run. OBO client only (no SP needed).

    Uses SQL Warehouse for Delta state reads/writes.
    """
    # 1. Read run from Delta via warehouse
    run_data = _wh_load_run(ws, config.warehouse_id, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise ValueError(f"Run not found: {run_id}")

    # 2. Validate terminal status
    status = run_data.get("status", "")
    if status not in {"CONVERGED", "STALLED", "MAX_ITERATIONS"}:
        raise ValueError(f"Cannot apply run in status {status}")

    # 3. If deferred UC writes requested, handle via OBO
    requested_mode = str(run_data.get("apply_mode") or "genie_config").lower()
    if requested_mode in {"uc_artifact", "both"}:
        # Read iterations to check improvement
        # Apply deferred UC writes via OBO (Statement Execution API)
        pass

    # 4. Update status to APPLIED via warehouse
    _sql_warehouse_execute(ws, config.warehouse_id,
        f"UPDATE {config.catalog}.{config.schema_name}.genie_opt_runs "
        f"SET status = 'APPLIED', updated_at = current_timestamp() "
        f"WHERE run_id = '{run_id}'"
    )
    return ActionResult(status="applied", run_id=run_id, message="Optimization applied.")
```

**`integration/discard.py`:**
```python
def discard_optimization(
    run_id: str,
    ws: WorkspaceClient,
    sp_ws: WorkspaceClient,
    config: IntegrationConfig,
) -> ActionResult:
    """Discard an optimization run and rollback config. Needs OBO + SP.

    Uses SQL Warehouse for Delta state reads/writes.
    Rollback uses Genie REST API (no Spark needed).
    """
    from genie_space_optimizer.optimization.applier import rollback

    # 1. Read run from Delta via warehouse
    run_data = _wh_load_run(ws, config.warehouse_id, run_id, config.catalog, config.schema_name)
    if not run_data:
        raise ValueError(f"Run not found: {run_id}")

    # 2. Validate status
    status = run_data.get("status", "")
    if status in ("DISCARDED", "APPLIED"):
        raise ValueError(f"Run already {status.lower()}")

    # 3. Rollback via Genie REST API (no Spark)
    original_snapshot = run_data.get("config_snapshot")
    if isinstance(original_snapshot, str):
        original_snapshot = json.loads(original_snapshot)
    if original_snapshot and isinstance(original_snapshot, dict):
        apply_log = {"pre_snapshot": original_snapshot}
        client = _pick_genie_client(ws, sp_ws)
        rollback(apply_log, client, run_data.get("space_id", ""))

    # 4. Update status to DISCARDED via warehouse
    _sql_warehouse_execute(ws, config.warehouse_id,
        f"UPDATE {config.catalog}.{config.schema_name}.genie_opt_runs "
        f"SET status = 'DISCARDED', "
        f"convergence_reason = 'user_discarded', "
        f"updated_at = current_timestamp() "
        f"WHERE run_id = '{run_id}'"
    )
    return ActionResult(status="discarded", run_id=run_id, message="Discarded and rolled back.")
```

Shared warehouse helper:
```python
def _wh_load_run(ws, warehouse_id, run_id, catalog, schema_name) -> dict | None:
    """Read a single run from Delta via SQL Warehouse."""
    df = _sql_warehouse_query(
        ws, warehouse_id,
        f"SELECT * FROM {catalog}.{schema_name}.genie_opt_runs WHERE run_id = '{run_id}'"
    )
    if df.empty:
        return None
    return df.iloc[0].to_dict()
```

### Impact on Standalone App

None. The existing route handlers in `runs.py` are unchanged. The
standalone app continues using Spark. The integration module provides
warehouse-only equivalents that reuse `rollback()` and the SQL warehouse
helpers.

---

## RFC-4: Make `databricks-connect` an Optional Dependency

### Problem

The GSO's `pyproject.toml` lists `databricks-connect>=15.0.0` as a core
dependency:

```toml
dependencies = [
    "fastapi>=0.115.0",
    "pydantic-settings>=2.11.0",
    ...
    "databricks-connect>=15.0.0",   # <-- heavyweight, ~hundreds of MB
    ...
]
```

The Workbench installs the GSO package via `-e ./packages/genie-space-optimizer`.
This pulls in `databricks-connect` and all its transitive dependencies, even
though the Workbench integration uses only the warehouse-based code paths
(RFC-2/RFC-3) that do not require Spark Connect.

`databricks-connect` is large (~500MB+ installed), adds significant
cold-start time to the Databricks App, and can conflict with other packages
in the Workbench environment.

### Proposed Change

Move `databricks-connect` to an optional extra in `pyproject.toml`:

```toml
[project]
dependencies = [
    "fastapi>=0.115.0",
    "pydantic-settings>=2.11.0",
    "uvicorn>=0.34.0",
    "databricks-sdk>=0.40.0",
    # databricks-connect moved to [spark] extra
    "mlflow[databricks]>=3.4.0",
    "pyyaml>=6.0",
    "pydantic>=2.0",
    "pandas>=2.0",
    "sqlmodel>=0.0.27",
    "psycopg[binary,pool]>=3.2.11",
    "sqlglot>=29.0.1",
]

[project.optional-dependencies]
spark = ["databricks-connect>=15.0.0"]
```

Guard Spark imports with a try/except in `backend/_spark.py`:

```python
try:
    from databricks.connect import DatabricksSession
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

def get_spark():
    if not SPARK_AVAILABLE:
        raise RuntimeError(
            "databricks-connect is not installed. "
            "Install with: pip install genie-space-optimizer[spark]"
        )
    return DatabricksSession.builder.serverless(True).getOrCreate()
```

### Install Patterns

| Context | Command | databricks-connect? |
|---------|---------|---------------------|
| Standalone GSO app | `pip install -e .[spark]` | Yes |
| GSO Databricks Job | `pip install -e .[spark]` | Yes |
| Workbench integration | `pip install -e ./packages/genie-space-optimizer` | No |
| Workbench (if Spark needed later) | `pip install -e ./packages/genie-space-optimizer[spark]` | Yes |

### Impact on Standalone App

Minimal. The standalone app's `app.yml` or install script adds `[spark]`:

```yaml
# In databricks.yml or Makefile
pip install -e .[spark]
```

All existing code paths continue to work. The `get_spark()` guard only
raises if code actually tries to use Spark without the extra installed.

---

## RFC-5: Document Lever 0 and Apply Mode Values

### Problem

The `LEVER_NAMES` dictionary in `common/config.py` (line 2094) includes
lever 0 ("Proactive Enrichment"):

```python
LEVER_NAMES = {
    0: "Proactive Enrichment",
    1: "Tables & Columns",
    2: "Metric Views",
    3: "Table-Valued Functions",
    4: "Join Specifications",
    5: "Genie Space Instructions",
}
DEFAULT_LEVER_ORDER = [1, 2, 3, 4, 5]
```

The Workbench integration needs to know:
1. That lever 0 is a preparatory stage that always runs and is NOT
   user-selectable
2. That `DEFAULT_LEVER_ORDER = [1, 2, 3, 4, 5]` is the correct set of
   levers to show in the UI

Additionally, `_SUPPORTED_APPLY_MODES` in `spaces.py` (line 41) defines
three valid values:

```python
_SUPPORTED_APPLY_MODES = {"genie_config", "uc_artifact", "both"}
```

But external documentation and the integration docs only mention
`"genie_config"` and `"both"`, omitting `"uc_artifact"`.

### Proposed Change

1. **Export lever info from the integration module** (see RFC-1):

```python
# integration/levers.py
from genie_space_optimizer.common.config import (
    DEFAULT_LEVER_ORDER,
    LEVER_NAMES,
)

LEVER_DESCRIPTIONS = {
    1: "Update table descriptions, column descriptions, and synonyms",
    2: "Update metric view column descriptions",
    3: "Remove underperforming TVFs",
    4: "Add, update, or remove join relationships between tables",
    5: "Rewrite global routing instructions and add domain-specific guidance",
}

def get_lever_info() -> list[dict]:
    """Return metadata for user-selectable levers (1-5).

    Lever 0 ('Proactive Enrichment') is a preparatory stage that always
    runs and is not exposed in the UI.
    """
    return [
        {
            "id": lever_id,
            "name": LEVER_NAMES[lever_id],
            "description": LEVER_DESCRIPTIONS.get(lever_id, ""),
        }
        for lever_id in DEFAULT_LEVER_ORDER
    ]

def get_default_lever_order() -> list[int]:
    """Return the default lever execution order: [1, 2, 3, 4, 5]."""
    return list(DEFAULT_LEVER_ORDER)
```

2. **Add docstring to `LEVER_NAMES`** in `common/config.py`:

```python
LEVER_NAMES = {
    0: "Proactive Enrichment",   # Always runs; NOT user-selectable
    1: "Tables & Columns",
    2: "Metric Views",
    3: "Table-Valued Functions",
    4: "Join Specifications",
    5: "Genie Space Instructions",
}
"""Lever ID → display name mapping.

Lever 0 is a preparatory stage that always runs before the adaptive lever
loop. It is not included in DEFAULT_LEVER_ORDER and should not be shown
in the UI as a toggleable option.
"""
```

3. **Document apply_mode values** in the `do_start_optimization` docstring
   and in the integration module:

```python
# Supported apply modes:
#   "genie_config"  — Apply changes only to the Genie Space config (default)
#   "uc_artifact"   — Apply UC-level changes only (DDL on tables/columns)
#   "both"          — Apply both Genie Space config and UC-level changes
SUPPORTED_APPLY_MODES = {"genie_config", "uc_artifact", "both"}
```

### Impact on Standalone App

Documentation-only change plus a new `integration/levers.py` module. No
behavioral changes to existing code.

---

## Implementation Order

| Order | RFC | Effort | Dependencies |
|-------|-----|--------|-------------|
| 1 | RFC-4 (optional deps) | Small | None |
| 2 | RFC-5 (lever/apply_mode docs) | Small | None |
| 3 | RFC-1 (integration module) | Medium | RFC-4, RFC-5 |
| 4 | RFC-2 (warehouse-first trigger) | Medium | RFC-1 |
| 5 | RFC-3 (extract apply/discard) | Medium | RFC-1, RFC-2 |

Total estimated effort: ~2-3 days of implementation + testing.

## Testing

- The standalone GSO app's existing E2E tests must pass unchanged
- The integration module functions should have unit tests with mocked
  `WorkspaceClient` and SQL warehouse responses
- The Workbench integration (downstream) will add E2E tests against a
  deployed app

## Questions for GSO Team

1. Are there plans to add more levers beyond 1-5? If so, the integration
   module's `get_lever_info()` should read dynamically from config rather
   than hardcoding descriptions.

2. Should the `_sql_warehouse_*` helpers be promoted to a shared utility
   (e.g., `common/warehouse.py`) since they're now used by both the
   fallback path and the integration module?

3. Is there interest in making the warehouse-first path the default for the
   standalone app as well (removing the Spark Connect dependency entirely)?
   The warehouse path is already proven stable via the existing fallback.
