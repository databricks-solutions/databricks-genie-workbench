"""Unit tests for ``capture_initial_version`` — the fail-soft first-capture hook.

Offline, no network. The helper must NEVER raise: space creation succeeds even
when VC is off/unintegrated (no runtime, writes disabled, no auth) or the capture
itself throws. The positive path records a CREATE-origin capture through the same
observe seam the space router uses.
"""

from types import SimpleNamespace
from unittest.mock import Mock, patch

from backend.services.version_control import contracts as vc
from backend.services.version_control.platform.app_observe import capture_initial_version

SPACE_ID = "space-1"


def _runtime(*, writes=True):
    """Fake ObserveRuntime with the surface the helper touches: flags/identity/observer.

    Mirrors the shapes in ``test_vc_spaces_router._runtime`` (flags.enabled dispatched by
    switch name; identity.actor returns an ActorContext; observer is a Mock).
    """
    observer = Mock()
    identity = Mock()
    identity.actor.return_value = vc.ActorContext("user@x", "target", "human")
    flags = Mock()
    flags.enabled.side_effect = lambda switch: {
        "vc_history_enabled": True, "vc_writes_enabled": writes,
        "vc_restore_enabled": True}.get(switch, False)
    return SimpleNamespace(observer=observer, identity=identity, flags=flags)


def _request(runtime, auth):
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(vc_observe=runtime)),
        state=SimpleNamespace(vc_auth=auth))


def test_noop_when_no_runtime():
    request = _request(None, vc.AuthenticatedRequest("user@x", "target"))
    with patch("backend.services.version_control.observe_optimizer.resolve_or_enroll_bound") as enroll, \
            patch("backend.services.genie_client.get_genie_space") as reader:
        assert capture_initial_version(request, SPACE_ID) is None
    enroll.assert_not_called()
    reader.assert_not_called()


def test_noop_when_writes_disabled():
    runtime = _runtime(writes=False)
    request = _request(runtime, vc.AuthenticatedRequest("user@x", "target"))
    with patch("backend.services.version_control.observe_optimizer.resolve_or_enroll_bound") as enroll:
        assert capture_initial_version(request, SPACE_ID) is None
    enroll.assert_not_called()
    runtime.observer.capture_on_open.assert_not_called()


def test_noop_when_no_auth():
    runtime = _runtime()
    request = _request(runtime, None)
    with patch("backend.services.version_control.observe_optimizer.resolve_or_enroll_bound") as enroll:
        assert capture_initial_version(request, SPACE_ID) is None
    enroll.assert_not_called()
    runtime.observer.capture_on_open.assert_not_called()


def test_positive_path_captures_create_origin():
    runtime = _runtime()
    auth = vc.AuthenticatedRequest("user@x", "target")
    request = _request(runtime, auth)
    binding = object()
    with patch("backend.services.version_control.observe_optimizer.resolve_or_enroll_bound",
               return_value=binding) as enroll, \
            patch("backend.services.genie_client.get_genie_space") as reader:
        assert capture_initial_version(request, SPACE_ID) is None

    enroll.assert_called_once_with(runtime, space_id=SPACE_ID)
    runtime.identity.actor.assert_called_once_with(auth)
    actor = runtime.identity.actor.return_value

    runtime.observer.capture_on_open.assert_called_once()
    call = runtime.observer.capture_on_open.call_args
    assert call.args == (binding, actor)
    assert call.kwargs["origin"] == vc.Origin.CREATE
    assert call.kwargs["actor_override"] == actor
    assert callable(call.kwargs["live_reader"])
    # The injected reader resolves get_genie_space for this space (OBO live read).
    assert call.kwargs["live_reader"]() is reader.return_value
    reader.assert_called_once_with(SPACE_ID)


def test_swallows_capture_exception():
    runtime = _runtime()
    runtime.observer.capture_on_open.side_effect = RuntimeError("boom")
    request = _request(runtime, vc.AuthenticatedRequest("user@x", "target"))
    with patch("backend.services.version_control.observe_optimizer.resolve_or_enroll_bound",
               return_value=object()), \
            patch("backend.services.genie_client.get_genie_space"):
        assert capture_initial_version(request, SPACE_ID) is None
