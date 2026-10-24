"""Unit tests for the shared platform adapters (``platform.adapters``).

Covers the governed-free infrastructure the observe runtime is built on: the REST
Statements API row coercion, the deployment clock's dual contract, and the three
``WorkspaceClient`` construction paths (local profile / in-Job M2M / ambient
run_as) with DATABRICKS_* env isolation. These moved out of the (now-removed)
governed ``live_seams`` module with the observe-only pivot.
"""

from datetime import UTC


def test_rows_coerces_statement_api_strings_to_store_native_types():
    """The REST Statements API returns every cell as a string; `_rows` must hand the
    durable stores typed fences/booleans/timestamps like the typed connector."""
    from datetime import datetime

    from backend.services.version_control.platform.adapters import _rows

    response = {
        "manifest": {"schema": {"columns": [
            {"name": "binding_revision", "type_name": "LONG"},
            {"name": "state", "type_name": "STRING"},
            {"name": "unresolved", "type_name": "BOOLEAN"},
            {"name": "updated_at", "type_name": "TIMESTAMP"},
            {"name": "observed_sequence", "type_name": "INT"},
        ]}},
        "result": {"data_array": [
            ["1", "idle", "false", "2026-09-09 19:30:39.233", None],
        ]},
    }
    (row,) = _rows(response)
    assert row["binding_revision"] == 1 and isinstance(row["binding_revision"], int)
    assert row["state"] == "idle"
    assert row["unresolved"] is False
    # Naive UTC: `row_from_columns` reattaches tz via `_from_naive_utc`.
    assert row["updated_at"] == datetime(2026, 9, 9, 19, 30, 39, 233000)  # noqa: DTZ001
    assert row["observed_sequence"] is None


def test_clock_satisfies_both_callable_and_now_contracts():
    """One `_Clock` backs the whole graph: CoordinationService calls `clock.now()`;
    the durable stores (DeltaCoordinationStore/DeltaRegistry) call `clock()`. Both
    must return an aware UTC datetime."""

    from backend.services.version_control.platform.adapters import _Clock

    clock = _Clock()
    assert callable(clock)
    called, vianow = clock(), clock.now()
    assert called.tzinfo == UTC and vianow.tzinfo == UTC


def _fake_workspace_client(monkeypatch):
    """Replace databricks.sdk.WorkspaceClient with a recorder that captures its
    construction kwargs and the DATABRICKS_* env visible at construction time, so
    the three auth modes (local profile / in-Job M2M / ambient run_as) can be
    pinned offline without any real credential resolution."""
    import os

    from databricks import sdk

    class Recorder:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.env_seen = {k: v for k, v in os.environ.items() if k.startswith("DATABRICKS_")}

    monkeypatch.setattr(sdk, "WorkspaceClient", Recorder)


def test_client_uses_named_profile_and_isolates_ambient_env(monkeypatch):
    """Local path: a role with a `profile` builds a profile-pinned client and pops
    every DATABRICKS_* var during construction so ambient env cannot shadow the
    named profile; the env is restored afterwards."""
    import os

    from backend.services.version_control.platform.adapters import PlatformAdapters

    _fake_workspace_client(monkeypatch)
    monkeypatch.setenv("DATABRICKS_HOST", "https://ambient.example.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "ambient-tok")
    adapters = PlatformAdapters({"config_file": "/tmp/cfg",
                                 "roles": {"executor": {"profile": "vc-exec", "host": "h"}}})
    client = adapters.client("executor")
    assert client.kwargs == {"profile": "vc-exec", "config_file": "/tmp/cfg"}
    assert client.env_seen == {}  # DATABRICKS_* popped during construction
    assert os.environ["DATABRICKS_HOST"] == "https://ambient.example.com"  # restored after


def test_client_builds_m2m_from_injected_secrets_in_job(monkeypatch):
    """In-Job governed credential storage: a role with an m2m `auth` block (no
    profile) builds an OAuth M2M client from Job-injected secret env vars, pinned
    to the role's host. Ambient DATABRICKS_* is isolated during construction so the
    run_as identity cannot conflict with the explicit M2M credentials."""
    import os

    from backend.services.version_control.platform.adapters import PlatformAdapters

    _fake_workspace_client(monkeypatch)
    monkeypatch.setenv("DATABRICKS_HOST", "https://runas.example.com")  # ambient run_as present
    monkeypatch.setenv("VC_SRC_ID", "src-client-id")
    monkeypatch.setenv("VC_SRC_SECRET", "src-secret")
    cfg = {"roles": {"source": {
        "host": "https://source.example.com",
        "auth": {"mode": "m2m", "client_id_env": "VC_SRC_ID", "client_secret_env": "VC_SRC_SECRET"}}}}
    client = PlatformAdapters(cfg).client("source")
    assert client.kwargs == {"host": "https://source.example.com",
                             "client_id": "src-client-id", "client_secret": "src-secret"}
    assert client.env_seen == {}  # ambient env isolated so M2M creds are authoritative
    assert os.environ["DATABRICKS_HOST"] == "https://runas.example.com"  # restored after


def test_client_uses_ambient_runas_when_no_profile_or_auth(monkeypatch):
    """Executor path in-Job: a role with neither a profile nor an auth block builds a
    bare WorkspaceClient() that resolves the Job's ambient run_as identity -- and it
    must NOT pop DATABRICKS_*, since that env is exactly how the run_as credential is
    injected."""
    from backend.services.version_control.platform.adapters import PlatformAdapters

    _fake_workspace_client(monkeypatch)
    monkeypatch.setenv("DATABRICKS_HOST", "https://runas.example.com")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "runas-sp")
    cfg = {"roles": {"executor": {"host": "https://target.example.com"}}}  # no profile, no auth
    client = PlatformAdapters(cfg).client("executor")
    assert client.kwargs == {}  # ambient WorkspaceClient() picks up run_as env
    assert client.env_seen == {"DATABRICKS_HOST": "https://runas.example.com",
                               "DATABRICKS_CLIENT_ID": "runas-sp"}  # preserved, not popped


def test_client_prefers_m2m_auth_over_profile_routing_key_in_job(monkeypatch):
    """In-Job role configs carry BOTH a `profile` (identity-provider routing key into
    `_profiles`) AND an m2m `auth` block (client construction). client() must use the
    auth block and NOT try to load the non-existent local profile inside the Job."""
    from backend.services.version_control.platform.adapters import PlatformAdapters

    _fake_workspace_client(monkeypatch)
    monkeypatch.setenv("VC_EXEC_ID", "exec-client-id")
    monkeypatch.setenv("VC_EXEC_SECRET", "exec-secret")
    cfg = {"roles": {"executor": {
        "host": "https://target.example.com", "profile": "vc-primary-executor",  # routing key only
        "auth": {"mode": "m2m", "client_id_env": "VC_EXEC_ID", "client_secret_env": "VC_EXEC_SECRET"}}}}
    client = PlatformAdapters(cfg).client("executor")
    assert client.kwargs == {"host": "https://target.example.com",
                             "client_id": "exec-client-id", "client_secret": "exec-secret"}
