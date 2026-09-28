"""MV-D109: one resolver, asked under the caller's token, never the service principal."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from backend.services import auth, space_access
from backend.services.space_access import SpaceAccessLevel as L
from backend.tests._event_loop import off_event_loop
from genie_space_optimizer.common.genie_client import SpaceAccessCheck, SpaceAccessUnavailable

_SPACE = "01f19f413ccc1ea3a42055a66e886302"
_ALLOW = SpaceAccessCheck(True, 200)
_DENY = SpaceAccessCheck(False, 403, 'You need "Can Edit" permission to perform this action')


@pytest.fixture(autouse=True)
def _fresh_cache():
    space_access.clear_cache()
    yield
    space_access.clear_cache()


@pytest.fixture
def as_user():
    tokens = []

    def _set(token: str = "tok-a"):
        tokens.append(auth._obo_client.set(SimpleNamespace(config=SimpleNamespace(token=token))))

    yield _set
    for token in reversed(tokens):
        auth._obo_client.reset(token)


def _genie(monkeypatch, *answers):
    calls: list[tuple] = []
    queue = list(answers)

    def fake(client, space_id, level):
        calls.append((client.config.token, space_id, level, off_event_loop()))
        answer = queue.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(space_access, "check_space_access", fake)
    return calls


def _require(level=L.EDIT, space_id=_SPACE):
    asyncio.run(space_access.require_space_access(space_id, level))


def _refusal(level=L.EDIT) -> HTTPException:
    with pytest.raises(HTTPException) as caught:
        _require(level)
    return caught.value


def test_no_user_token_is_401_and_genie_is_not_asked(monkeypatch):
    calls = _genie(monkeypatch)
    refusal = _refusal()
    assert refusal.status_code == 401
    assert refusal.detail["code"] == "authentication_required"
    assert calls == []


def test_a_denial_is_403_with_a_structured_detail(monkeypatch, as_user):
    as_user()
    _genie(monkeypatch, _DENY)
    refusal = _refusal()
    assert refusal.status_code == 403
    assert refusal.detail == {
        "code": "space_access_denied",
        "required": "edit",
        "message": "You need Can Edit permission on this Genie Agent.",
        "platform_message": 'You need "Can Edit" permission to perform this action',
    }


def test_a_missing_space_is_404(monkeypatch, as_user):
    as_user()
    _genie(monkeypatch, SpaceAccessCheck(False, 404, "no such space"))
    assert _refusal(L.VIEW).status_code == 404


def test_an_unanswered_check_is_503(monkeypatch, as_user):
    as_user()
    _genie(monkeypatch, SpaceAccessUnavailable("connection reset"))
    refusal = _refusal()
    assert refusal.status_code == 503
    assert refusal.detail["code"] == "space_access_unavailable"


def test_the_check_runs_off_the_event_loop_with_the_callers_token(monkeypatch, as_user):
    as_user("tok-a")
    calls = _genie(monkeypatch, _ALLOW)
    _require()
    assert calls == [("tok-a", _SPACE, L.EDIT, True)]


def test_an_allow_is_cached_for_the_same_token_and_level(monkeypatch, as_user):
    as_user()
    calls = _genie(monkeypatch, _ALLOW)
    _require()
    _require()
    assert len(calls) == 1


def test_a_cached_higher_level_satisfies_a_lower_one(monkeypatch, as_user):
    as_user()
    calls = _genie(monkeypatch, _ALLOW)
    _require(L.MANAGE)
    _require(L.VIEW)
    assert [c[2] for c in calls] == [L.MANAGE]


def test_a_cached_view_allow_does_not_satisfy_edit(monkeypatch, as_user):
    as_user()
    calls = _genie(monkeypatch, _ALLOW, _DENY)
    _require(L.VIEW)
    assert _refusal(L.EDIT).status_code == 403
    assert [c[2] for c in calls] == [L.VIEW, L.EDIT]


def test_a_denial_is_never_cached(monkeypatch, as_user):
    as_user()
    calls = _genie(monkeypatch, _DENY, _ALLOW)
    _refusal()
    _require()
    assert len(calls) == 2


def test_the_cache_never_crosses_tokens(monkeypatch, as_user):
    calls = _genie(monkeypatch, _ALLOW, _DENY)
    as_user("tok-a")
    _require()
    as_user("tok-b")
    assert _refusal().status_code == 403
    assert [c[0] for c in calls] == ["tok-a", "tok-b"]


def test_an_allow_expires(monkeypatch, as_user):
    as_user()
    calls = _genie(monkeypatch, _ALLOW, _ALLOW)
    clock = iter([100.0, 100.0, 100.0 + space_access._ALLOW_TTL_S + 1, 200.0])
    monkeypatch.setattr(space_access, "_now", lambda: next(clock))
    _require()
    _require()
    assert len(calls) == 2
