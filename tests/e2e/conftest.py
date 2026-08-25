"""Fixtures + env gate for the MV Advisor live E2E suite (Prompt 15).

OPT-IN and ENV-GATED. Nothing here touches a workspace unless the tier's config
is present. A missing variable skips with a message naming the variable AND the
scenario it gates ("skipped: MV_E2E_LOWPRIV_TOKEN unset (Scenario B)") — never a
bare skip count. See ``scripts/e2e/mv_advisor_e2e.md`` for the tiered runbook.

Invocation model (Prompt 15, decision A). The real FastAPI route code runs
IN-PROCESS via ``TestClient``; the caller's identity is injected exactly as
production does it — the real ``OBOAuthMiddleware`` reads
``x-forwarded-access-token`` and calls ``set_obo_user_token`` inside the request
context, so both the router and the services layer see the OBO ``ContextVar``
(and ``asyncio.to_thread`` offloads copy the context). No uvicorn, no deployed
URL: the MV routes read through Statement Execution, not Lakebase, so a real OBO
token plus a SQL warehouse is the whole dependency surface. This is compliant
with the no-local-server rule for the reason that rule exists.

Rate discipline (your reminder): the whole suite is serialized by an in-process
lock and refuses to run under pytest-xdist, so the native-eval ~20 q/min ceiling
is never blown by parallel workers. Every live test is also marked ``slow``.
"""

from __future__ import annotations

import contextlib
import os
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import pytest

# Refuse parallel execution: the ~20 questions/min native-eval ceiling (Prompt
# 15, Scenario C) is a hard workspace limit, not a local one. xdist workers would
# each drive their own eval runs and blow it. Fail loudly at import rather than
# silently under-serialize.
if os.environ.get("PYTEST_XDIST_WORKER"):
    raise RuntimeError(
        "tests/e2e is serialized by design (Prompt 15, ~20 q/min native-eval "
        "ceiling). Do not run it under pytest-xdist / -n."
    )

# One process-wide lock serializes every live test regardless of runner order.
_SERIAL_LOCK = threading.Lock()

pytestmark = [pytest.mark.e2e]


# ── env helpers ─────────────────────────────────────────────────────────────


def _env(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def require(var: str, scenario: str) -> str:
    """Return ``var``'s value, or skip naming the variable AND the scenario.

    The skip message is the contract: it always names the missing variable and
    the scenario it gates, so a partial config produces an actionable skip line
    rather than a bare count.
    """
    value = _env(var)
    if not value:
        pytest.skip(f"{var} unset ({scenario})")
    return value


def _warehouse_id() -> str | None:
    # Same precedence the GSO warehouse resolver and the router use.
    return _env("GSO_WAREHOUSE_ID") or _env("SQL_WAREHOUSE_ID")


# ── the identity gate (decision A addition) ─────────────────────────────────


def _resolve_identity(token: str, *, label: str, scenario: str) -> str:
    """Build a token-backed client and confirm ``current_user.me()`` resolves.

    A bad/expired token fails the gate HERE with a clear reason, instead of
    surfacing later as a confusing mid-scenario 401 from a route. Unset is a
    skip (the tier is simply not configured); set-but-invalid is a hard failure
    (the operator intended to run and must fix the credential).
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.config import Config

    host = require("DATABRICKS_HOST", scenario)
    try:
        client = WorkspaceClient(config=Config(host=host, token=token, auth_type="pat"))
        email = client.current_user.me().user_name
    except Exception as exc:  # noqa: BLE001 - surface any auth failure as a clear gate error
        pytest.fail(
            f"{label} token is set but current_user.me() failed against {host}: "
            f"{exc}. Fix the token/host before running the live suite "
            f"({scenario})."
        )
    if not email:
        pytest.fail(f"{label} token resolved but returned no user_name ({scenario}).")
    return email


# ── config ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class GsoConfig:
    """The GSO Delta location + warehouse the routes and direct reads share."""

    catalog: str
    schema: str
    warehouse_id: str


@pytest.fixture(scope="session")
def gso() -> GsoConfig:
    """GSO catalog/schema/warehouse — sourced from .env.deploy / app.yaml.

    Required by every tier (the routes read the same Delta tables the suite
    asserts against). ``GSO_JOB_ID`` is validated separately per tier: Tier 1
    needs it PRESENT only as the shared ``_is_configured`` gate, never triggered.
    """
    catalog = require("GSO_CATALOG", "all tiers")
    warehouse_id = _warehouse_id()
    if not warehouse_id:
        pytest.skip("GSO_WAREHOUSE_ID (or SQL_WAREHOUSE_ID) unset (all tiers)")
    return GsoConfig(
        catalog=catalog,
        schema=_env("GSO_SCHEMA") or "genie_space_optimizer",
        warehouse_id=warehouse_id,
    )


@pytest.fixture(scope="session")
def scratch() -> tuple[str, str]:
    """The consented scratch schema you own (catalog, schema).

    A metric view is created here under your identity (Scenario C) or by hand
    (Scenario D BYO). Never anywhere else — the consent target is this schema.
    """
    catalog = require("MV_E2E_SCRATCH_CATALOG", "Scenarios C/D-BYO")
    schema = require("MV_E2E_SCRATCH_SCHEMA", "Scenarios C/D-BYO")
    return catalog, schema


# ── identities ──────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def primary_token() -> str:
    return require("DATABRICKS_TOKEN", "all tiers")


@pytest.fixture(scope="session")
def primary_email(primary_token: str) -> str:
    """The signed-in user email; also the suite's credential gate."""
    return _resolve_identity(primary_token, label="DATABRICKS_TOKEN", scenario="all tiers")


@pytest.fixture(scope="session")
def lowpriv_token() -> str:
    return require("MV_E2E_LOWPRIV_TOKEN", "Scenario B")


@pytest.fixture(scope="session")
def lowpriv_email(lowpriv_token: str) -> str:
    return _resolve_identity(
        lowpriv_token, label="MV_E2E_LOWPRIV_TOKEN", scenario="Scenario B"
    )


# ── the in-process API client (faithful OBO via the real middleware) ─────────


class E2EClient:
    """A ``TestClient`` that injects the caller's identity on every request.

    Wraps the real ``auto_optimize`` router + the real ``OBOAuthMiddleware`` so
    each request sets the OBO ``ContextVar`` from ``x-forwarded-access-token``
    exactly as Databricks Apps does, and tags the caller via
    ``x-forwarded-email`` (what ``trigger`` / ``suggest`` / decision read).
    """

    def __init__(self, token: str, email: str):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from backend.main import OBOAuthMiddleware
        from backend.routers import auto_optimize

        app = FastAPI()
        app.add_middleware(OBOAuthMiddleware)
        app.include_router(auto_optimize.router)
        self._client = TestClient(app)
        self._headers = {
            "x-forwarded-access-token": token,
            "x-forwarded-email": email,
            "x-forwarded-preferred-username": email,
        }

    def _merge(self, headers: dict | None) -> dict:
        merged = dict(self._headers)
        if headers:
            merged.update(headers)
        return merged

    def get(self, url: str, **kw):
        kw["headers"] = self._merge(kw.get("headers"))
        return self._client.get(url, **kw)

    def post(self, url: str, **kw):
        kw["headers"] = self._merge(kw.get("headers"))
        return self._client.post(url, **kw)


@pytest.fixture
def api_primary(primary_token: str, primary_email: str, gso: GsoConfig) -> E2EClient:
    """API client as the signed-in user. Requires the primary credential gate."""
    return E2EClient(primary_token, primary_email)


@pytest.fixture
def api_lowpriv(lowpriv_token: str, lowpriv_email: str, gso: GsoConfig) -> E2EClient:
    """API client as the low-privilege identity (Scenario B)."""
    return E2EClient(lowpriv_token, lowpriv_email)


# ── direct workspace access for setup / assertions / teardown ────────────────


@pytest.fixture(scope="session")
def ws(primary_email: str):
    """A real ``WorkspaceClient`` as the signed-in user (for SQL + Jobs reads).

    Depends on ``primary_email`` so the credential gate runs first.
    """
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.config import Config

    host = require("DATABRICKS_HOST", "all tiers")
    token = require("DATABRICKS_TOKEN", "all tiers")
    return WorkspaceClient(config=Config(host=host, token=token, auth_type="pat"))


@pytest.fixture
def run_sql(ws, gso: GsoConfig) -> Callable[[str], object]:
    """Run one statement against the GSO warehouse (reads AND DDL).

    Uses the GSO warehouse helper (no read-only guard) so DESCRIBE EXTENDED,
    manual CREATE (BYO), and teardown DROP all go through one path.
    """
    from genie_space_optimizer.common.warehouse import sql_warehouse_query

    def _run(sql: str):
        return sql_warehouse_query(ws, gso.warehouse_id, sql)

    return _run


@pytest.fixture
def cleanup() -> Iterator[list[Callable[[], None]]]:
    """Register teardown callables run LIFO in a finally block.

    Scenario C detaches+drops its scratch MV; Scenario D-BYO drops its view
    MANUALLY here — the app must never drop a USER_CREATED object.
    """
    finalizers: list[Callable[[], None]] = []
    try:
        yield finalizers
    finally:
        for fn in reversed(finalizers):
            with contextlib.suppress(Exception):
                fn()


# ── job trigger/poll helper (real GSO run, serialized) ───────────────────────


@dataclass(frozen=True)
class JobRunResult:
    life_cycle_state: str
    result_state: str | None


@pytest.fixture
def poll_job_run(ws) -> Callable[..., JobRunResult]:
    """Poll a Databricks job run to a terminal life-cycle state.

    ``run_now`` returns immediately; there is no wait-until-terminal helper in
    the launcher, so callers poll ``jobs.get_run`` themselves (Prompt 15).
    """

    def _poll(job_run_id: str | int, *, timeout_s: int = 2400, interval_s: int = 20) -> JobRunResult:
        terminal = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
        deadline = time.monotonic() + timeout_s
        while True:
            run = ws.jobs.get_run(run_id=int(job_run_id))
            state = run.state
            life = getattr(state, "life_cycle_state", None)
            life_name = getattr(life, "value", None) or str(life or "")
            if life_name in terminal:
                result = getattr(state, "result_state", None)
                result_name = getattr(result, "value", None) or (
                    str(result) if result is not None else None
                )
                return JobRunResult(life_cycle_state=life_name, result_state=result_name)
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"job run {job_run_id} did not reach a terminal state within "
                    f"{timeout_s}s (last life_cycle_state={life_name})"
                )
            time.sleep(interval_s)

    return _poll


# ── serialization (autouse) ──────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _serialize() -> Iterator[None]:
    """Hold a process-wide lock for the duration of every live test."""
    with _SERIAL_LOCK:
        yield
