"""Observe-around-optimizer: auto-enroll + before/after capture, best-effort & gated."""

from types import SimpleNamespace
from unittest.mock import Mock
from uuid import UUID

from backend.services.version_control import contracts as vc
from backend.services.version_control.observe_optimizer import (
    capture_after,
    capture_after_when_complete,
    capture_before,
    resolve_or_enroll_bound,
)

SPACE_ID = "space-1"
_BOUND = vc.BindingRef(str(UUID(int=5)), 1, SPACE_ID, "target", SPACE_ID, "prod")
_PROVISIONAL = vc.BindingRef(str(UUID(int=5)), 1, SPACE_ID, "target", None, "prod")


def _runtime(*, writes=True, existing=None, capture_raises=False):
    registry = Mock()
    registry.find_active_by_space_key.return_value = existing
    registry.enroll.return_value = _PROVISIONAL
    registry.bind_created.return_value = _BOUND
    identity = Mock()
    identity.executor.return_value = "executor"
    observer = Mock()
    if capture_raises:
        observer.capture.side_effect = RuntimeError("delta down")
    else:
        observer.capture.return_value = "obs-result"
    flags = Mock()
    flags.enabled.return_value = writes
    return SimpleNamespace(
        registry=registry, actor=vc.ActorContext("sp", "target", "service"),
        workspace_id="target", environment="prod", identity=identity, observer=observer,
        flags=flags, reader_selection=object())


# -- resolve_or_enroll_bound ------------------------------------------------

def test_fresh_space_is_enrolled_then_bound():
    rt = _runtime(existing=None)
    binding = resolve_or_enroll_bound(rt, space_id=SPACE_ID)
    assert binding == _BOUND
    request = rt.registry.enroll.call_args.args[0]
    assert isinstance(request, vc.EnrollmentRequest)
    assert request.space_key == SPACE_ID and request.workspace_id == "target"
    rt.registry.bind_created.assert_called_once()
    # bind_created attaches the physical space id.
    assert rt.registry.bind_created.call_args.args[1] == SPACE_ID


def test_already_bound_space_resolves_without_writing():
    rt = _runtime(existing=_BOUND)
    assert resolve_or_enroll_bound(rt, space_id=SPACE_ID) == _BOUND
    rt.registry.enroll.assert_not_called()
    rt.registry.bind_created.assert_not_called()


def test_provisional_head_is_bound_without_re_enrolling():
    rt = _runtime(existing=_PROVISIONAL)
    assert resolve_or_enroll_bound(rt, space_id=SPACE_ID) == _BOUND
    rt.registry.enroll.assert_not_called()
    rt.registry.bind_created.assert_called_once()


# -- capture_before / capture_after ----------------------------------------

def test_capture_before_is_noop_when_writes_disabled():
    rt = _runtime(writes=False)
    assert capture_before(rt, space_id=SPACE_ID) is None
    rt.observer.capture.assert_not_called()
    rt.registry.find_active_by_space_key.assert_not_called()


def test_capture_before_records_optimizer_before_without_run_id():
    rt = _runtime(existing=_BOUND)
    assert capture_before(rt, space_id=SPACE_ID) == "obs-result"
    rt.observer.capture.assert_called_once_with(_BOUND, "optimizer_before", "executor",
                                                origin=vc.Origin.OPTIMIZER, optimizer_run_id=None)


def test_capture_after_stamps_optimizer_run_id():
    rt = _runtime(existing=_BOUND)
    assert capture_after(rt, space_id=SPACE_ID, run_id="run-1") == "obs-result"
    rt.observer.capture.assert_called_once_with(_BOUND, "optimizer_after", "executor",
                                                origin=vc.Origin.OPTIMIZER, optimizer_run_id="run-1")


def test_capture_is_fail_safe_and_never_raises():
    rt = _runtime(existing=_BOUND, capture_raises=True)
    assert capture_before(rt, space_id=SPACE_ID) is None
    assert capture_after(rt, space_id=SPACE_ID, run_id="run-1") is None


def test_capture_none_runtime_is_noop():
    assert capture_before(None, space_id=SPACE_ID) is None
    assert capture_after(None, space_id=SPACE_ID, run_id="r") is None


# -- capture_after_when_complete -------------------------------------------

def _run(state):
    return SimpleNamespace(state=SimpleNamespace(life_cycle_state=SimpleNamespace(value=state)))


def test_after_poll_waits_for_terminal_then_captures():
    rt = _runtime(existing=_BOUND)
    runs = iter([_run("RUNNING"), _run("RUNNING"), _run("TERMINATED")])
    sleeps: list = []
    result = capture_after_when_complete(
        rt, space_id=SPACE_ID, run_id="run-1", job_run_id=42,
        get_run=lambda rid: next(runs), sleep=sleeps.append, poll_interval_s=1)
    assert result == "obs-result"
    assert len(sleeps) == 2  # slept through the two RUNNING polls
    rt.observer.capture.assert_called_once_with(_BOUND, "optimizer_after", "executor",
                                                origin=vc.Origin.OPTIMIZER, optimizer_run_id="run-1")


def test_after_poll_disabled_is_noop():
    rt = _runtime(writes=False)
    assert capture_after_when_complete(rt, space_id=SPACE_ID, run_id="r", job_run_id=1,
                                       get_run=lambda rid: _run("TERMINATED")) is None
    rt.observer.capture.assert_not_called()


def test_after_poll_captures_even_when_polling_fails():
    rt = _runtime(existing=_BOUND)
    def boom(_rid):
        raise RuntimeError("jobs api down")
    result = capture_after_when_complete(rt, space_id=SPACE_ID, run_id="run-1", job_run_id=42,
                                         get_run=boom, sleep=lambda _s: None)
    assert result == "obs-result"  # fell back to a best-effort capture
    rt.observer.capture.assert_called_once()


def test_after_poll_missing_job_run_id_captures_immediately():
    rt = _runtime(existing=_BOUND)
    called: list = []
    result = capture_after_when_complete(rt, space_id=SPACE_ID, run_id="run-1", job_run_id=None,
                                         get_run=lambda rid: called.append(rid) or _run("TERMINATED"))
    assert result == "obs-result" and called == []  # no poll without a job_run_id
