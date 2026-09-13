"""Tests for the fail-soft initial-capture hook wired into the REST create endpoint.

Task 3: ``create_space_endpoint`` fires ``capture_initial_version`` after a
successful space creation. The hook must (A) receive the new space id, and
(B) never affect the endpoint's response — including the default/unintegrated
case where no ``vc_observe`` runtime is on ``app.state``.
"""
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import create


# CreateSpaceRequest (backend/models.py): display_name (required, min_length=1),
# serialized_space (required dict), parent_path (optional str).
_VALID_BODY = {
    "display_name": "D",
    "serialized_space": {"data_sources": {"tables": []}},
    "parent_path": "/Shared/genie",
}

_FAKE_RESULT = {
    "genie_space_id": "space-xyz",
    "display_name": "D",
    "space_url": "http://x",
}


def test_hook_invoked_with_new_space_id(monkeypatch):
    """Test A: the hook is called once with the new space id."""
    monkeypatch.setattr(create, "create_genie_space", lambda **kwargs: _FAKE_RESULT)
    hook = Mock()
    monkeypatch.setattr(create, "capture_initial_version", hook)

    app = FastAPI()
    app.include_router(create.router)

    with TestClient(app) as client:
        resp = client.post("/api/create", json=_VALID_BODY)

    assert resp.status_code == 200, resp.text
    assert resp.json()["space_id"] == "space-xyz"
    assert hook.call_count == 1
    # new space id is the second positional arg (request, space_id)
    assert hook.call_args.args[1] == "space-xyz"


def test_creation_unaffected_when_hook_is_real_and_unintegrated(monkeypatch):
    """Test B: with the REAL (unpatched) helper and no ``vc_observe`` on
    app.state, the helper no-ops and creation still returns 200."""
    monkeypatch.setattr(create, "create_genie_space", lambda **kwargs: _FAKE_RESULT)

    app = FastAPI()
    app.include_router(create.router)
    # No app.state.vc_observe is set → real capture_initial_version returns early.

    with TestClient(app) as client:
        resp = client.post("/api/create", json=_VALID_BODY)

    assert resp.status_code == 200, resp.text
    assert resp.json()["space_id"] == "space-xyz"
