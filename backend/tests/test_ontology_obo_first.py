"""OBO-first foundation reads (MV-D50, spec §11).

The two foundation reads (governed-tag graph + usage/lineage signals) default to
OBO (the viewing admin), resolve their identity from ``read_identity``, and never
silently fall back to the SP. Caches partition on the resolved principal so an
OBO (privilege-filtered) result is never served to another identity.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.services import auth
from backend.ontology.services import tag_graph
from backend.watch.services import system_tables


class _FakeClient:
    """Minimal stand-in with the ``.config`` fields the resolver reads."""

    def __init__(self, *, token: str | None = None, client_id: str | None = None):
        self.config = SimpleNamespace(token=token, client_id=client_id)


def _no_sp():
    raise AssertionError("the service principal client must not be used")


def _no_obo():
    raise AssertionError("the OBO client must not be used")


# ── resolve_read_client: the identity switch (table-driven) ─────────────────


def test_resolve_default_obo_never_touches_sp(monkeypatch):
    obo = _FakeClient(token="viewer-token")
    monkeypatch.setattr(auth, "require_obo_workspace_client", lambda: obo)
    monkeypatch.setattr(auth, "get_service_principal_client", _no_sp)

    client, principal = auth.resolve_read_client()  # default "obo"
    assert client is obo
    assert principal.startswith("obo:")


def test_resolve_sp_on_demand(monkeypatch):
    sp = _FakeClient(client_id="app-123")
    monkeypatch.setattr(auth, "get_service_principal_client", lambda: sp)
    monkeypatch.setattr(auth, "require_obo_workspace_client", _no_obo)

    client, principal = auth.resolve_read_client("sp")
    assert client is sp
    assert principal == "sp:app-123"


@pytest.mark.parametrize("sp_probe_ok,expect_sp", [(True, True), (False, False)])
def test_resolve_auto_picks_sp_only_when_probe_ok(monkeypatch, sp_probe_ok, expect_sp):
    sp = _FakeClient(client_id="app-123")
    obo = _FakeClient(token="viewer-token")
    monkeypatch.setattr(auth, "get_service_principal_client", lambda: sp)
    monkeypatch.setattr(auth, "require_obo_workspace_client", lambda: obo)

    client, principal = auth.resolve_read_client("auto", sp_probe_ok=sp_probe_ok)
    if expect_sp:
        assert client is sp and principal.startswith("sp:")
    else:
        assert client is obo and principal.startswith("obo:")


def test_resolve_obo_no_context_raises_without_sp_fallback(monkeypatch):
    monkeypatch.setattr(
        auth, "require_obo_workspace_client",
        lambda: (_ for _ in ()).throw(RuntimeError("requires user authorization")),
    )
    monkeypatch.setattr(auth, "get_service_principal_client", _no_sp)

    with pytest.raises(RuntimeError):
        auth.resolve_read_client("obo")


def test_principal_isolation_between_users(monkeypatch):
    a = auth.read_principal_id(_FakeClient(token="user-A"), is_obo=True)
    b = auth.read_principal_id(_FakeClient(token="user-B"), is_obo=True)
    sp = auth.read_principal_id(_FakeClient(client_id="app-9"), is_obo=False)
    assert a != b  # distinct users → distinct cache partitions
    assert a != sp and b != sp  # OBO never collides with the SP partition


# ── tag_graph: OBO default, no silent SP fallback, per-identity cache ───────


def _capture_run(monkeypatch, sink: list):
    """Replace tag_graph._run with a stub that records the client it was handed."""
    def _fake(client, sql, parameters, *, track_health, poll_total_seconds=120):
        sink.append(client)
        return []
    monkeypatch.setattr(tag_graph, "_run", _fake)


def test_build_graph_default_reads_as_viewer_not_sp(monkeypatch):
    tag_graph._CACHE.clear()
    obo = _FakeClient(token="viewer-1")
    monkeypatch.setattr(auth, "require_obo_workspace_client", lambda: obo)
    monkeypatch.setattr(auth, "get_service_principal_client", _no_sp)
    seen: list = []
    _capture_run(monkeypatch, seen)

    tag_graph.build_graph(["finance"])  # default "obo"
    assert seen and all(c is obo for c in seen)


def test_build_graph_no_obo_context_degrades_not_sp(monkeypatch):
    tag_graph._CACHE.clear()
    monkeypatch.setattr(
        auth, "require_obo_workspace_client",
        lambda: (_ for _ in ()).throw(RuntimeError("requires user authorization")),
    )
    monkeypatch.setattr(auth, "get_service_principal_client", _no_sp)
    monkeypatch.setattr(
        tag_graph, "_run",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("no query should run")),
    )

    graph = tag_graph.build_graph(["finance"])  # obo default, no OBO context
    assert graph["tags"] == []  # degraded to empty, never widened to the SP


def test_build_graph_cache_is_per_identity(monkeypatch):
    tag_graph._CACHE.clear()
    a, b = _FakeClient(token="A"), _FakeClient(token="B")
    calls: list[str] = []

    def _fake(client, sql, parameters, *, track_health, poll_total_seconds=120):
        calls.append(client.config.token)
        return []

    monkeypatch.setattr(tag_graph, "_run", _fake)
    monkeypatch.setattr(auth, "get_service_principal_client", _no_sp)

    monkeypatch.setattr(auth, "require_obo_workspace_client", lambda: a)
    tag_graph.build_graph(["finance"])  # A: catalog + assignment reads
    tag_graph.build_graph(["finance"])  # A again: served from cache (no new reads)
    assert calls.count("A") == 2

    monkeypatch.setattr(auth, "require_obo_workspace_client", lambda: b)
    tag_graph.build_graph(["finance"])  # B: a different principal must NOT reuse A's entry
    assert "B" in calls


def test_build_graph_sp_and_auto_resolution(monkeypatch):
    tag_graph._CACHE.clear()
    sp = _FakeClient(client_id="app-1")
    obo = _FakeClient(token="viewer-1")
    monkeypatch.setattr(auth, "get_service_principal_client", lambda: sp)
    monkeypatch.setattr(auth, "require_obo_workspace_client", lambda: obo)
    seen: list = []
    _capture_run(monkeypatch, seen)

    tag_graph.build_graph(["finance"], "sp")
    assert seen and all(c is sp for c in seen)

    seen.clear()
    tag_graph._CACHE.clear()  # isolate from the prior SP-partition entry
    tag_graph.build_graph(["finance"], "auto", sp_probe_ok=True)
    assert seen and all(c is sp for c in seen)

    seen.clear()
    tag_graph._CACHE.clear()
    tag_graph.build_graph(["finance"], "auto", sp_probe_ok=False)
    assert seen and all(c is obo for c in seen)


def test_probe_no_obo_context_blocks_not_sp(monkeypatch):
    monkeypatch.setattr(
        auth, "require_obo_workspace_client",
        lambda: (_ for _ in ()).throw(RuntimeError("requires user authorization")),
    )
    monkeypatch.setattr(auth, "get_service_principal_client", _no_sp)
    assert tag_graph.probe("obo") is False


# ── system_tables signals seam: OBO reads for ontology, SP for GenieWatch ───


def test_system_tables_resolves_obo_for_signals_not_sp(monkeypatch):
    obo = _FakeClient(token="viewer-1")
    monkeypatch.setattr(auth, "require_obo_workspace_client", lambda: obo)
    monkeypatch.setattr(auth, "get_service_principal_client", _no_sp)

    client, principal = system_tables._resolve_client("obo", False)
    assert client is obo and principal.startswith("obo:")


def test_system_tables_default_is_sp_for_geniewatch(monkeypatch):
    sp = _FakeClient(client_id="app-1")
    monkeypatch.setattr(auth, "get_service_principal_client", lambda: sp)
    monkeypatch.setattr(auth, "require_obo_workspace_client", _no_obo)

    client, principal = system_tables._resolve_client("sp", False)
    assert client is sp and principal == "sp:app-1"


def test_system_tables_obo_read_with_no_context_degrades(monkeypatch):
    monkeypatch.setattr(
        auth, "require_obo_workspace_client",
        lambda: (_ for _ in ()).throw(RuntimeError("requires user authorization")),
    )
    monkeypatch.setattr(auth, "get_service_principal_client", _no_sp)

    # No context under an obo read → degrade to [] (MV-D44), never touch the SP.
    assert system_tables._run("SELECT 1", [], read_identity="obo") == []


def test_system_tables_cache_key_partitions_by_principal():
    k_sp = system_tables._cache_key("SELECT 1", [], "sp:app-1")
    k_obo = system_tables._cache_key("SELECT 1", [], "obo:deadbeef")
    assert k_sp != k_obo  # an SP result never collides with an OBO partition
