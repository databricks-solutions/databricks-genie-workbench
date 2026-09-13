"""Regression tests for create-on-timeout duplicate spaces.

Field report: creating a Genie Space produced many duplicates while the app
showed "failed" and kept retrying (error: HTTPSConnectionPool ... Read timed
out). Root cause: the create POST is non-idempotent and the app never recorded
space_id on timeout, so every retry created another space. Genie appends a
timestamp to the title on name collision, so duplicates looked distinct.

These tests cover the backend half of the fix:
  - _is_timeout_error detects "Read timed out"
  - _title_matches_requested matches exact + collision variants, rejects siblings
  - reconcile adopts the timed-out-but-created space, scoped to THIS attempt
    (create_time >= request start) so it never binds to a pre-existing space
  - a non-retrying client is used for the create POST
"""

import backend.genie_creator as gc


class _FakeApiClient:
    def __init__(self, created_title, created_id="sp_reconciled", create_time="2026-08-12T12:03:55Z"):
        self._title = created_title
        self._id = created_id
        self._ct = create_time
        self.post_calls = 0

    def do(self, method, path, body=None, query=None):
        if method == "POST" and path == "/api/2.0/genie/spaces":
            self.post_calls += 1
            raise Exception(
                "HTTPSConnectionPool(host='8259553952209604.4.gcp.databricks.com', "
                "port=443): Read timed out."
            )
        if method == "GET" and path == "/api/2.0/genie/spaces":
            return {"spaces": [
                {"space_id": self._id, "title": self._title, "create_time": self._ct},
                {"space_id": "sp_other", "title": "Some Unrelated Space", "create_time": self._ct},
            ], "next_page_token": None}
        raise AssertionError(f"unexpected: {method} {path}")


class _FakeClient:
    def __init__(self, api):
        self.api_client = api


def _patch(monkeypatch, api):
    monkeypatch.setattr(gc, "get_workspace_client", lambda: _FakeClient(api))
    monkeypatch.setattr(gc, "get_databricks_host", lambda: "https://example.cloud.databricks.com")
    monkeypatch.setattr(gc, "get_sql_warehouse_id", lambda: "wh_123")
    monkeypatch.setattr(gc, "_non_retrying_client", lambda base: base)
    import backend.services.genie_client as gcli
    monkeypatch.setattr(gcli, "list_genie_spaces",
                        lambda: api.do("GET", "/api/2.0/genie/spaces")["spaces"])


# ── _is_timeout_error ────────────────────────────────────────────────────────

def test_is_timeout_error_detects_read_timed_out():
    assert gc._is_timeout_error(Exception("HTTPSConnectionPool(...): Read timed out."))
    assert gc._is_timeout_error(Exception("connect timeout"))
    assert gc._is_timeout_error(TimeoutError("x"))


def test_is_timeout_error_false_for_non_timeout():
    assert not gc._is_timeout_error(Exception("403 permission denied"))
    assert not gc._is_timeout_error(ValueError("bad config"))


# ── _title_matches_requested ─────────────────────────────────────────────────

def test_title_matcher_exact_and_variants():
    assert gc._title_matches_requested("Sales", "Sales")
    assert gc._title_matches_requested("Sales 2026-08-12 12:03:55", "Sales")
    assert gc._title_matches_requested("Sales 2026-08-10 13:17", "Sales")           # no seconds
    assert gc._title_matches_requested("Sales [2026-08-10 13:17]", "Sales")         # brackets
    assert gc._title_matches_requested("Sales 2026-08-12T12:03:55.123Z", "Sales")   # iso+frac+Z


def test_title_matcher_rejects_prefix_siblings():
    for t in ["Sales Report", "Sales Agent", "SalesX", "Sales v2", "Sales 2026"]:
        assert not gc._title_matches_requested(t, "Sales"), t


# ── reconcile behavior ───────────────────────────────────────────────────────

def test_timeout_reconciles_to_created_space(monkeypatch):
    title = "Ops_Collection_team_SQL_Agent"
    api = _FakeApiClient(created_title=title, created_id="sp_ok", create_time="2026-08-12T12:03:55Z")
    _patch(monkeypatch, api)
    # request_start is computed inside create_genie_space (now - 5s); the fake's
    # create_time is fixed in the past, so freeze "now" is unnecessary — instead
    # give the created space a create_time far in the future to sit in-window.
    api._ct = "2999-01-01T00:00:00Z"
    result = gc.create_genie_space(display_name=title, merged_config={"data_sources": {"tables": []}})
    assert result["genie_space_id"] == "sp_ok"
    assert result.get("reconciled") is True
    assert api.post_calls == 1


def test_reconcile_matches_timestamp_renamed_variant(monkeypatch):
    requested = "Ops_Collection_team_SQL_Agent"

    class _RenamedApi(_FakeApiClient):
        def do(self, method, path, body=None, query=None):
            if method == "POST":
                self.post_calls += 1
                raise Exception("HTTPSConnectionPool(host='x'): Read timed out.")
            return {"spaces": [
                {"space_id": "sp_renamed", "title": f"{requested} 2999-08-10 13:04:12",
                 "create_time": "2999-08-10T13:04:12Z"},
                {"space_id": "sp_unrelated", "title": "Something Else", "create_time": "2999-01-01T00:00:00Z"},
            ], "next_page_token": None}

    api = _RenamedApi(created_title=requested)
    _patch(monkeypatch, api)
    result = gc.create_genie_space(display_name=requested, merged_config={"data_sources": {"tables": []}})
    assert result["genie_space_id"] == "sp_renamed"
    assert result.get("reconciled") is True


def test_reconcile_ignores_preexisting_space_outside_window(monkeypatch):
    """CRITICAL (data-loss guard): a same-named space created BEFORE this attempt
    must NOT be adopted — otherwise a later update_space overwrites it. With only
    a pre-existing (old) match and no in-window space, reconcile must raise."""
    requested = "Sales Agent"

    class _OldOnlyApi(_FakeApiClient):
        def do(self, method, path, body=None, query=None):
            if method == "POST":
                self.post_calls += 1
                raise Exception("Read timed out.")
            # Only a pre-existing space with an OLD create_time exists.
            return {"spaces": [
                {"space_id": "sp_preexisting", "title": requested, "create_time": "2000-01-01T00:00:00Z"},
            ], "next_page_token": None}

    api = _OldOnlyApi(created_title=requested)
    _patch(monkeypatch, api)
    monkeypatch.setattr(gc, "_RECONCILE_ATTEMPTS", 1)  # no long polling in test

    import pytest
    with pytest.raises(TimeoutError):
        gc.create_genie_space(display_name=requested, merged_config={"data_sources": {"tables": []}})


def test_reconcile_prefers_earliest_in_window(monkeypatch):
    requested = "Sales Agent"

    class _BothApi(_FakeApiClient):
        def do(self, method, path, body=None, query=None):
            if method == "POST":
                self.post_calls += 1
                raise Exception("Read timed out.")
            return {"spaces": [
                {"space_id": "sp_dup", "title": f"{requested} 2999-08-10 13:05:00",
                 "create_time": "2999-08-10T13:05:00Z"},
                {"space_id": "sp_original", "title": requested,
                 "create_time": "2999-08-10T13:04:00Z"},
            ], "next_page_token": None}

    api = _BothApi(created_title=requested)
    _patch(monkeypatch, api)
    result = gc.create_genie_space(display_name=requested, merged_config={"data_sources": {"tables": []}})
    assert result["genie_space_id"] == "sp_original"  # earliest in-window


def test_timeout_without_match_raises(monkeypatch):
    class _NoMatchApi(_FakeApiClient):
        def do(self, method, path, body=None, query=None):
            if method == "POST":
                self.post_calls += 1
                raise Exception("Read timed out.")
            return {"spaces": [{"space_id": "x", "title": "A Different Title",
                                "create_time": "2999-01-01T00:00:00Z"}], "next_page_token": None}

    api = _NoMatchApi(created_title="whatever")
    _patch(monkeypatch, api)
    monkeypatch.setattr(gc, "_RECONCILE_ATTEMPTS", 1)
    import pytest
    with pytest.raises(TimeoutError):
        gc.create_genie_space(display_name="Ops_Collection_team_SQL_Agent",
                              merged_config={"data_sources": {"tables": []}})


def test_uses_non_retrying_client_for_create(monkeypatch):
    """The create POST must go through a NON-retrying client so the SDK cannot
    silently re-fire the non-idempotent POST on timeout."""
    title = "Client Selection Space"
    used = _FakeApiClient(created_title=title, created_id="sp_ok")
    base = _FakeApiClient(created_title="unused", created_id="sp_wrong")

    class _C:
        def __init__(self, api): self.api_client = api

    monkeypatch.setattr(gc, "get_workspace_client", lambda: _C(base))
    monkeypatch.setattr(gc, "get_databricks_host", lambda: "https://example.cloud.databricks.com")
    monkeypatch.setattr(gc, "get_sql_warehouse_id", lambda: "wh_123")

    calls = {"n": 0}
    def fake_non_retrying(b):
        calls["n"] += 1
        return _C(used)
    monkeypatch.setattr(gc, "_non_retrying_client", fake_non_retrying)

    def do(method, path, body=None, query=None):
        if method == "POST":
            used.post_calls += 1
            return {"space_id": "sp_ok"}
        raise AssertionError(method)
    used.do = do

    result = gc.create_genie_space(display_name=title, merged_config={"data_sources": {"tables": []}})
    assert calls["n"] == 1, "_non_retrying_client not used for the create POST"
    assert result["genie_space_id"] == "sp_ok"
    assert used.post_calls == 1
    assert base.post_calls == 0
