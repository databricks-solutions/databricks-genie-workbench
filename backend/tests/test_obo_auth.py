from __future__ import annotations

import pytest

from backend.services import auth


def test_require_obo_client_never_falls_back_to_default(monkeypatch) -> None:
    auth.clear_obo_user_token()
    monkeypatch.setattr(
        auth,
        "_get_default_client",
        lambda: (_ for _ in ()).throw(AssertionError("must not use app identity")),
    )

    with pytest.raises(RuntimeError, match="user authorization"):
        auth.require_obo_workspace_client()


def test_require_obo_client_returns_request_client() -> None:
    marker = object()
    token = auth._obo_client.set(marker)
    try:
        assert auth.require_obo_workspace_client() is marker
    finally:
        auth._obo_client.reset(token)
