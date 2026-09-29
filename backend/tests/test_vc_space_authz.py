"""VC-D-authz1: version control asks the one space-access resolver."""

from uuid import UUID

import pytest
from fastapi import HTTPException

from backend.services import space_access
from backend.services.space_access import SpaceAccessLevel as L
from backend.services.version_control import contracts as vc
from backend.services.version_control import space_authz

_ACTOR = vc.ActorContext("user@x", "target", "human")
_BINDING = vc.BindingRef(str(UUID(int=1)), 1, "space-1", "target", "space-1", "prod")


@pytest.fixture
def asked(monkeypatch):
    calls = []
    monkeypatch.setattr(space_access, "ensure_space_access",
                        lambda space_id, level: calls.append((space_id, level)))
    return calls


def _refusing(monkeypatch, status, code):
    def refuse(space_id, level):
        raise HTTPException(status, detail={"code": code, "required": level.value,
                                            "message": "no", "platform_message": "genie said no"})
    monkeypatch.setattr(space_access, "ensure_space_access", refuse)


def test_a_binding_asks_for_its_own_space_at_the_given_level(asked):
    space_authz.authorize(_ACTOR, _BINDING, L.EDIT, workspace_id="target")
    assert asked == [("space-1", L.EDIT)]


def test_another_workspace_is_refused_before_genie_is_asked(asked):
    outsider = vc.ActorContext("user@x", "other", "human")
    with pytest.raises(PermissionError):
        space_authz.authorize(outsider, _BINDING, L.VIEW, workspace_id="target")
    with pytest.raises(PermissionError):
        space_authz.authorize_space(outsider, "space-1", L.VIEW, workspace_id="target")
    assert asked == []


def test_an_unbound_binding_is_refused(asked):
    provisional = vc.BindingRef(str(UUID(int=2)), 1, "space-1", "target", None, "prod")
    with pytest.raises(PermissionError):
        space_authz.authorize(_ACTOR, provisional, L.VIEW, workspace_id="target")
    assert asked == []


def test_a_refusal_leaves_in_the_version_control_error_shape(monkeypatch):
    _refusing(monkeypatch, 403, "space_access_denied")
    with pytest.raises(HTTPException) as caught:
        space_authz.authorize_space(_ACTOR, "space-1", L.EDIT, workspace_id="target")
    assert caught.value.status_code == 403
    assert caught.value.detail == {
        "code": "space_access_denied", "message": "no", "retryable": False, "stale": False,
        "details": {"required": "edit", "platform_message": "genie said no"},
    }


def test_an_unanswered_check_is_retryable(monkeypatch):
    _refusing(monkeypatch, 503, "space_access_unavailable")
    with pytest.raises(HTTPException) as caught:
        space_authz.authorize_space(_ACTOR, "space-1", L.VIEW, workspace_id="target")
    assert caught.value.status_code == 503
    assert caught.value.detail["retryable"] is True
