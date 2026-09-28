"""MV-D109: the package's space access checks ask Genie, under the caller's client."""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors.platform import NotFound, PermissionDenied, Unauthenticated

from genie_space_optimizer.common import genie_client as gc
from genie_space_optimizer.common.genie_client import SpaceAccessLevel

_SPACE = "01f19f413ccc1ea3a42055a66e886302"
_ADMINS_ACL = {
    "access_control_list": [
        {"group_name": "admins", "all_permissions": [{"permission_level": "CAN_MANAGE", "inherited": True}]},
    ],
}


class _Genie:
    """Answers the three reads the way Genie did on a live workspace (M0 check V3)."""

    def __init__(self, *, view=True, edit=False, manage=False, export=True, error=None):
        self.view, self.edit, self.manage, self.export, self.error = view, edit, manage, export, error
        self.calls: list[tuple] = []

    def do(self, method, path, query=None, **_):
        self.calls.append((method, path, query))
        if self.error is not None:
            raise self.error
        if path == f"/api/2.0/permissions/genie/{_SPACE}":
            if not self.manage:
                raise PermissionDenied("does not have CAN_MANAGE permissions")
            return _ADMINS_ACL
        wants_export = (query or {}).get("include_serialized_space") == "true"
        if wants_export and not self.edit:
            raise PermissionDenied('You need "Can Edit" permission to perform this action')
        if not self.view:
            raise PermissionDenied('You need "Can View" permission to perform this action')
        body = {"space_id": _SPACE, "title": "Flights"}
        if wants_export and self.export:
            body["serialized_space"] = '{"version": 2}'
        return body


def _client(**answers) -> MagicMock:
    w = MagicMock(name="caller_ws")
    w.api_client = _Genie(**answers)
    return w


def test_view_is_a_plain_read():
    w = _client()
    assert gc.check_space_access(w, _SPACE, SpaceAccessLevel.VIEW).allowed is True
    assert w.api_client.calls == [("GET", f"/api/2.0/genie/spaces/{_SPACE}", None)]


def test_view_denial_keeps_the_platform_message():
    result = gc.check_space_access(_client(view=False), _SPACE, SpaceAccessLevel.VIEW)
    assert (result.allowed, result.status) == (False, 403)
    assert "Can View" in result.message


def test_edit_is_allowed_when_genie_exports_the_configuration():
    w = _client(edit=True)
    assert gc.check_space_access(w, _SPACE, SpaceAccessLevel.EDIT).allowed is True
    assert w.api_client.calls == [
        ("GET", f"/api/2.0/genie/spaces/{_SPACE}", {"include_serialized_space": "true"}),
    ]


def test_edit_is_denied_for_a_viewer():
    result = gc.check_space_access(_client(), _SPACE, SpaceAccessLevel.EDIT)
    assert (result.allowed, result.status) == (False, 403)
    assert "Can Edit" in result.message


def test_edit_needs_the_export_not_just_a_200():
    result = gc.check_space_access(_client(edit=True, export=False), _SPACE, SpaceAccessLevel.EDIT)
    assert (result.allowed, result.status) == (False, 200)


def test_manage_is_allowed_when_the_acl_is_readable():
    w = _client(manage=True)
    assert gc.check_space_access(w, _SPACE, SpaceAccessLevel.MANAGE).allowed is True
    assert w.api_client.calls == [("GET", f"/api/2.0/permissions/genie/{_SPACE}", None)]


def test_manage_is_denied_when_the_acl_read_is_forbidden():
    result = gc.check_space_access(_client(edit=True), _SPACE, SpaceAccessLevel.MANAGE)
    assert (result.allowed, result.status) == (False, 403)


def test_a_missing_space_reports_404():
    result = gc.check_space_access(_client(error=NotFound("no such space")), _SPACE, SpaceAccessLevel.VIEW)
    assert (result.allowed, result.status) == (False, 404)


def test_an_unauthenticated_read_is_a_401():
    result = gc.check_space_access(
        _client(error=Unauthenticated("no token")), _SPACE, SpaceAccessLevel.VIEW,
    )
    assert (result.allowed, result.status) == (False, 401)


def test_a_string_level_is_coerced_and_garbage_is_refused():
    w = _client(edit=True)
    result = gc.check_space_access(w, _SPACE, "manage")
    assert (result.allowed, result.status) == (False, 403)
    assert w.api_client.calls == [("GET", f"/api/2.0/permissions/genie/{_SPACE}", None)]
    with pytest.raises(ValueError):
        gc.check_space_access(w, _SPACE, "owner")


def test_an_unanswered_check_raises_unavailable():
    with pytest.raises(gc.SpaceAccessUnavailable):
        gc.check_space_access(_client(error=RuntimeError("connection reset")), _SPACE, SpaceAccessLevel.EDIT)


def test_an_admins_acl_entry_grants_a_viewer_nothing():
    # The ACL this client could read lists `admins` CAN_MANAGE, which matched every
    # caller before MV-D109. The EDIT answer comes from Genie's export read alone.
    assert gc.user_can_edit_space(_client(manage=True, edit=False), _SPACE) is False


def test_the_boolean_helpers_deny_when_genie_does_not_answer():
    w = _client(error=RuntimeError("connection reset"))
    assert gc.user_can_edit_space(w, _SPACE) is False
    assert gc.user_can_manage_space(w, _SPACE) is False


def test_the_boolean_helpers_take_only_the_callers_client():
    for helper in (gc.user_can_edit_space, gc.user_can_manage_space):
        assert list(inspect.signature(helper).parameters) == ["w", "space_id"]
