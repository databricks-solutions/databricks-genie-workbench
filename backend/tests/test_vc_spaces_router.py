"""Space-keyed VC bridge router: history reads + auto-enroll capture for a Genie space."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.version_control import contracts as vc

SPACE_ID = "space-1"
_BINDING = vc.BindingRef(str(UUID(int=5)), 1, SPACE_ID, "target", SPACE_ID, "prod")


def _summary():
    fingerprints = vc.Fingerprints("a" * 64, "b" * 64, "c" * 64, "vc-c14n/1")
    return vc.VersionSummary(str(UUID(int=9)), _BINDING.binding_id, datetime.now(timezone.utc),
                             vc.Origin.EXTERNAL, "user@x", None, None, fingerprints, "run-1", None)


def _status():
    return vc.BindingStatus(_BINDING.binding_id, 1, vc.Heads(str(UUID(int=9)), None, None),
                            vc.DriftState.CLEAN, False, None, datetime.now(timezone.utc),
                            datetime.now(timezone.utc), False, (), ())


def _runtime(*, history=True, writes=True, restore=True, existing=_BINDING):
    ledger = Mock()
    ledger.history.return_value = vc.VersionPage((_summary(),), "cursor-2")
    # get_version -> a snapshot whose serialized_space/restorable_metadata are real JSON
    # (the restore path json.dumps the serialized_space onto the live space).
    ledger.get_version.return_value = SimpleNamespace(
        snapshot=SimpleNamespace(serialized_space={"config": {}}, restorable_metadata={"description": "d"}))
    registry = Mock()
    registry.find_active_by_space_key.return_value = existing
    observer = Mock()
    observer.capture_on_open.return_value = vc.ObservationResult(_status(), _summary(), False)
    observer.capture.return_value = vc.ObservationResult(_status(), _summary(), False)
    actor = vc.ActorContext("user@x", "target", "human")
    identity = Mock()
    identity.actor.return_value = actor
    canonicalizer = Mock()
    canonicalizer.observe.return_value = object()
    canonicalizer.compare.return_value = vc.Comparison.EQUAL
    flags = Mock()
    flags.enabled.side_effect = lambda switch: {
        "vc_history_enabled": history, "vc_writes_enabled": writes,
        "vc_restore_enabled": restore}.get(switch, False)
    return SimpleNamespace(
        ledger=ledger, registry=registry, observer=observer, identity=identity, flags=flags,
        canonicalizer=canonicalizer, authorize_history=Mock(return_value=True), actor=actor,
        workspace_id="target", environment="prod", reader_selection=object(),
        tag_store=_FakeTagStore())


class _FakeTagStore:
    """Sync in-memory twin of DeltaVersionTagStore for router tests (fresh per runtime, so
    no cross-test bleed). Same method surface the router calls: set_tag/delete_tag/get_tags."""

    def __init__(self):
        self._rows: dict[str, dict] = {}

    def set_tag(self, space_id, version_id, label, note, author):
        self._rows[version_id] = {"space_id": space_id, "label": label,
                                  "note": note, "author": author}

    def delete_tag(self, space_id, version_id):
        row = self._rows.get(version_id)
        if row is not None and row["space_id"] == space_id:
            del self._rows[version_id]

    def get_tags(self, space_id):
        return {vid: {"label": r["label"], "note": r["note"], "author": r["author"]}
                for vid, r in self._rows.items() if r["space_id"] == space_id}


def _client(runtime, *, authenticated=True):
    from backend.routers.vc_spaces import build_router

    app = FastAPI()

    @app.middleware("http")
    async def authenticate(request, call_next):
        if authenticated:
            request.state.vc_auth = vc.AuthenticatedRequest("user@x", "target")
        return await call_next(request)

    app.include_router(build_router(runtime=runtime))
    return TestClient(app)


def test_space_versions_returns_history_when_enrolled():
    runtime = _runtime()
    response = _client(runtime).get(f"/api/version-control/spaces/{SPACE_ID}/versions?limit=5")
    assert response.status_code == 200
    assert response.json()["items"][0]["optimizer_run_id"] == "run-1"
    runtime.ledger.history.assert_called_once_with(_BINDING, None, 5)


def test_space_versions_is_empty_when_not_enrolled():
    runtime = _runtime(existing=None)
    response = _client(runtime).get(f"/api/version-control/spaces/{SPACE_ID}/versions")
    assert response.status_code == 200
    assert response.json() == {"items": [], "next_cursor": None}
    runtime.ledger.history.assert_not_called()


def test_space_versions_fail_closed_when_history_disabled():
    runtime = _runtime(history=False)
    response = _client(runtime).get(f"/api/version-control/spaces/{SPACE_ID}/versions")
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "vc_history_disabled"


def test_space_versions_requires_authentication():
    runtime = _runtime()
    response = _client(runtime, authenticated=False).get(
        f"/api/version-control/spaces/{SPACE_ID}/versions")
    assert response.status_code == 401


def test_space_observe_captures_current_state(monkeypatch):
    runtime = _runtime()
    envelope = {"serialized_space": {}}
    seen = {}
    monkeypatch.setattr("backend.services.genie_client.get_genie_space",
                        lambda sid: seen.update(sid=sid) or envelope)
    response = _client(runtime).post(f"/api/version-control/spaces/{SPACE_ID}/observe")
    assert response.status_code == 200
    assert response.json()["captured_version"]["version_id"] == str(UUID(int=9))
    # The authenticated human is recorded as the ledger actor (authorship), not the SP.
    call = runtime.observer.capture_on_open.call_args
    assert call.args == (_BINDING, runtime.actor)
    assert call.kwargs["actor_override"] == runtime.actor
    # The live serialized space is read under the SAME user (OBO): the injected reader
    # resolves get_genie_space for this space (the SP may lack access to a user-owned space).
    assert call.kwargs["live_reader"]() is envelope
    assert seen["sid"] == SPACE_ID


def test_space_observe_fail_closed_when_writes_disabled():
    runtime = _runtime(writes=False)
    response = _client(runtime).post(f"/api/version-control/spaces/{SPACE_ID}/observe")
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "vc_writes_disabled"
    runtime.observer.capture_on_open.assert_not_called()


def test_spaces_router_exposes_exact_routes():
    from backend.routers.vc_spaces import build_router

    routes = {(r.path, tuple(sorted(r.methods))) for r in build_router(runtime=_runtime()).routes}
    assert routes == {
        ("/api/version-control/config", ("GET",)),
        ("/api/version-control/spaces/{space_id}/versions", ("GET",)),
        ("/api/version-control/spaces/{space_id}/observe", ("POST",)),
        ("/api/version-control/spaces/{space_id}/restore", ("POST",)),
        ("/api/version-control/spaces/{space_id}/tags", ("GET",)),
        ("/api/version-control/spaces/{space_id}/versions/{version_id}/tag", ("PUT",)),
        ("/api/version-control/spaces/{space_id}/versions/{version_id}/tag", ("DELETE",)),
    }


def test_config_reports_flag_state():
    runtime = _runtime(history=True, writes=True, restore=False)
    response = _client(runtime).get("/api/version-control/config")
    assert response.status_code == 200
    assert response.json() == {"history_enabled": True, "writes_enabled": True,
                               "restore_enabled": False}


def test_config_needs_no_authentication():
    runtime = _runtime()
    response = _client(runtime, authenticated=False).get("/api/version-control/config")
    assert response.status_code == 200


_RESTORE_BODY = {"version_id": str(UUID(int=1)), "expected_current_version_id": str(UUID(int=9))}


def _obo_patch(monkeypatch):
    """Patch the OBO seams the restore route reaches for the live GET/PATCH."""
    client = Mock()
    monkeypatch.setattr("backend.services.auth.get_workspace_client", lambda: client)
    monkeypatch.setattr("backend.services.genie_client.get_genie_space", lambda sid: {"serialized_space": {}})
    return client


def test_space_restore_applies_snapshot_and_records_version(monkeypatch):
    runtime = _runtime()
    client = _obo_patch(monkeypatch)
    response = _client(runtime).post(f"/api/version-control/spaces/{SPACE_ID}/restore", json=_RESTORE_BODY)
    assert response.status_code == 200
    assert response.json()["captured_version"]["version_id"] == str(UUID(int=9))
    # Recorded as a restore linked to the requested source version, attributed to the
    # human who clicked Restore (actor_override), not the SP executor.
    _binding, reason, _executor = runtime.observer.capture.call_args.args
    assert reason == "restore"
    assert runtime.observer.capture.call_args.kwargs["origin"] == vc.Origin.RESTORE
    assert runtime.observer.capture.call_args.kwargs["restored_from_version_id"] == str(UUID(int=1))
    assert runtime.observer.capture.call_args.kwargs["actor_override"] == runtime.actor
    # The live space was PATCHed as the OBO user.
    assert client.api_client.do.call_args_list[0].args[0] == "PATCH"


def test_space_restore_409_when_space_drifted(monkeypatch):
    runtime = _runtime()
    runtime.canonicalizer.compare.return_value = vc.Comparison.DIFFERENT
    _obo_patch(monkeypatch)
    response = _client(runtime).post(f"/api/version-control/spaces/{SPACE_ID}/restore", json=_RESTORE_BODY)
    assert response.status_code == 409
    runtime.observer.capture.assert_not_called()


def test_space_restore_404_when_not_enrolled(monkeypatch):
    runtime = _runtime(existing=None)
    _obo_patch(monkeypatch)
    response = _client(runtime).post(f"/api/version-control/spaces/{SPACE_ID}/restore", json=_RESTORE_BODY)
    assert response.status_code == 404


def test_space_restore_fail_closed_when_restore_disabled(monkeypatch):
    runtime = _runtime(restore=False)
    _obo_patch(monkeypatch)
    response = _client(runtime).post(f"/api/version-control/spaces/{SPACE_ID}/restore", json=_RESTORE_BODY)
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "vc_restore_disabled"
    runtime.observer.capture.assert_not_called()


# -- restore_space_version service (branches not reached via the router) -----

def test_restore_service_raises_value_error_when_current_version_is_stale():
    from backend.services.version_control.restore_local import restore_space_version

    runtime = _runtime()
    runtime.ledger.get_version.side_effect = [object(), KeyError("gone")]  # historical ok, expected missing
    with pytest.raises(ValueError, match="stale"):
        restore_space_version(runtime, space_id=SPACE_ID, version_id=str(UUID(int=1)),
                              expected_current_version_id=str(UUID(int=2)), actor=runtime.actor,
                              live_reader=lambda sid: {}, live_writer=lambda *a: None)
    runtime.observer.capture.assert_not_called()


def test_restore_service_denies_actor_outside_binding_workspace():
    from backend.services.version_control.restore_local import restore_space_version

    runtime = _runtime()
    intruder = vc.ActorContext("user@x", "other-ws", "human")
    with pytest.raises(PermissionError):
        restore_space_version(runtime, space_id=SPACE_ID, version_id=str(UUID(int=1)),
                              expected_current_version_id=str(UUID(int=2)), actor=intruder,
                              live_reader=lambda sid: {}, live_writer=lambda *a: None)
    runtime.ledger.get_version.assert_not_called()


# -- version tags (§19 Task 4) ------------------------------------------------
# The tag store is the Delta-backed DeltaVersionTagStore in prod; router tests inject a
# fresh sync in-memory twin per runtime (_FakeTagStore), so there is no cross-test bleed.
_TAG_VID = "22222222-2222-2222-2222-222222222222"


def test_put_get_delete_version_tag():
    c = _client(_runtime())
    put = c.put(f"/api/version-control/spaces/{SPACE_ID}/versions/{_TAG_VID}/tag",
                json={"label": "Golden", "note": "keep"})
    assert put.status_code == 200
    assert put.json()["author"] == "user@x"  # stamped with the actor's subject_id

    got = c.get(f"/api/version-control/spaces/{SPACE_ID}/tags").json()
    assert got[_TAG_VID]["label"] == "Golden"

    assert c.delete(
        f"/api/version-control/spaces/{SPACE_ID}/versions/{_TAG_VID}/tag").status_code == 200
    assert _TAG_VID not in c.get(f"/api/version-control/spaces/{SPACE_ID}/tags").json()


def test_put_tag_404_when_not_enrolled():
    c = _client(_runtime(existing=None))
    response = c.put(f"/api/version-control/spaces/{SPACE_ID}/versions/{_TAG_VID}/tag",
                     json={"label": "Golden"})
    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "resource_not_found"


def test_put_tag_fail_closed_when_writes_disabled():
    c = _client(_runtime(writes=False))
    response = c.put(f"/api/version-control/spaces/{SPACE_ID}/versions/{_TAG_VID}/tag",
                     json={"label": "Golden"})
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "vc_writes_disabled"


def test_put_tag_denies_actor_outside_binding_workspace():
    # Mirror `test_restore_service_denies_actor_outside_binding_workspace`, which forces the
    # scope gate to fail via a mismatched workspace_id on the actor (the binding lives in
    # "target"; the intruder authenticates from "other-ws"). Here the actor comes from
    # `identity.actor`, so we mismatch it there: `resolve_readable` then raises
    # PermissionError, which the router maps to 403 for the tag WRITE — proving the write is
    # denied (not merely that a read returns empty). DELETE enforces the same gate.
    runtime = _runtime()
    runtime.identity.actor.return_value = vc.ActorContext("user@x", "other-ws", "human")
    c = _client(runtime)
    put = c.put(f"/api/version-control/spaces/{SPACE_ID}/versions/{_TAG_VID}/tag",
                json={"label": "Golden"})
    assert put.status_code == 403
    assert put.json()["detail"]["code"] == "scope_denied"
    delete = c.delete(f"/api/version-control/spaces/{SPACE_ID}/versions/{_TAG_VID}/tag")
    assert delete.status_code == 403
    assert delete.json()["detail"]["code"] == "scope_denied"
