"""Shared admin client for route tests behind the workspace-admin gate (MV-D109).

The Ontology routers carry ``dependencies=[Depends(require_admin)]``. Route tests
pass the gate the way a real admin request does — the ``X-Forwarded-Groups``
header — rather than overriding the dependency, so the gate itself stays under
test.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

ADMIN_HEADERS = {"X-Forwarded-Groups": "users,admins"}


def admin_client(app: FastAPI) -> TestClient:
    """A ``TestClient`` whose every request carries the admin group header."""
    return TestClient(app, headers=ADMIN_HEADERS)
