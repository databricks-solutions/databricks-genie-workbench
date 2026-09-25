"""Observe a Genie space around an optimizer run (the observe-only pivot core).

The optimizer is self-safe (it rolls back its own non-improving iterations); Version
Control does NOT govern it. It only *records* what the space looked like before the run
and after it settles, so an operator can see the change history and (later) restore.

The ``/trigger`` endpoint calls these best-effort and flag-gated:

* ``capture_before`` — resolve-or-**auto-enroll** the space's binding, then append a
  ``optimizer_before`` observation of the current serialized space.
* ``capture_after_when_complete`` — after the optimizer Job run reaches a terminal
  state, append an ``optimizer_after`` observation stamped with the optimizer ``run_id``
  (the passive path — a UI ``capture_on_open`` — is handled by the observe endpoint).

Everything here is additive and fail-safe: enrollment/capture are writes gated behind
``vc_writes_enabled`` and every call site swallows failures, so VC can never alter or
break optimizer behavior. All Delta writes are validated on deploy, not offline.
"""

import logging
import time
from datetime import UTC, datetime
from typing import Any, Callable
from uuid import NAMESPACE_URL, uuid5

from backend.services.version_control import contracts as vc

logger = logging.getLogger(__name__)

_BINDING_REASON = "vc observe-only auto-enroll (optimizer run)"
# Terminal Databricks Jobs run life-cycle states (RunLifeCycleState).
_TERMINAL_STATES = frozenset({"TERMINATED", "SKIPPED", "INTERNAL_ERROR"})


def _deterministic_uuid(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, "/".join(("vc-observe", *parts))))


def _digest(domain: str, payload: dict) -> vc.Digest:
    return vc.canonical_json_hash(domain, payload)


def _create_intent(binding: vc.BindingRef, space_id: str, operation_id: str) -> vc.CreateIntentRef:
    event_id = _deterministic_uuid("create-intent", binding.binding_id)
    request_digest = _digest("vc-create-intent/1", {
        "binding_id": binding.binding_id, "space_id": space_id, "operation_id": operation_id})
    return vc.CreateIntentRef(event_id, operation_id, binding.binding_id,
                              binding.binding_revision, request_digest)


def _identity_evidence(workspace_id: str, space_id: str, actor: vc.ActorContext,
                       now: datetime) -> vc.IdentityEvidence:
    evidence_digest = _digest("vc-identity-evidence/1", {
        "space_id": space_id, "workspace_id": workspace_id, "subject_id": actor.subject_id})
    return vc.IdentityEvidence("VC/1.0", workspace_id, space_id, evidence_digest, now, actor)


def resolve_or_enroll_bound(runtime, *, space_id: str, environment: str | None = None,
                            now: datetime | None = None,
                            operation_id: str | None = None) -> vc.BindingRef:
    """Return the BOUND binding for ``space_id``, auto-enrolling+binding if absent.

    The logical ``space_key`` is the physical ``space_id`` (a 1:1 mapping for the
    observe surface). Idempotent: an already-bound space resolves without a write; a
    provisional head is bound; a fresh space is enrolled then bound.
    """
    registry = runtime.registry
    actor = runtime.actor
    workspace_id = runtime.workspace_id
    environment = environment or runtime.environment
    now = now or datetime.now(UTC)
    space_key = space_id

    existing = registry.find_active_by_space_key(space_key)
    if existing is not None and existing.space_id == space_id:
        return existing
    operation_id = operation_id or _deterministic_uuid("enroll", space_key)
    if existing is None:
        provisional = registry.enroll(
            vc.EnrollmentRequest(space_key, workspace_id, None, environment, _BINDING_REASON),
            actor)
    else:
        provisional = existing  # provisional (unbound) head from a partial prior run
    return registry.bind_created(
        _create_intent(provisional, space_id, operation_id), space_id,
        _identity_evidence(workspace_id, space_id, actor, now))


def _capture(runtime, *, space_id: str, reason: str, optimizer_run_id: str | None,
             environment: str | None = None):
    binding = resolve_or_enroll_bound(runtime, space_id=space_id, environment=environment)
    executor = runtime.identity.executor(runtime.reader_selection)
    # before/after snapshots bracket an optimizer run; badge them `optimizer` (CUJ-1 §4.1).
    return runtime.observer.capture(binding, reason, executor, origin=vc.Origin.OPTIMIZER,
                                    optimizer_run_id=optimizer_run_id)


def capture_before(runtime, *, space_id: str, environment: str | None = None):
    """Best-effort pre-run capture; no-op unless writes are enabled. Never raises."""
    if runtime is None or runtime.flags.enabled("vc_writes_enabled") is not True:
        return None
    try:
        return _capture(runtime, space_id=space_id, reason="optimizer_before",
                        optimizer_run_id=None, environment=environment)
    except Exception:  # noqa: BLE001 - observe must never break the optimizer trigger
        logger.warning("VC observe capture-before failed for space %s", space_id, exc_info=True)
        return None


def capture_after(runtime, *, space_id: str, run_id: str, environment: str | None = None):
    """Best-effort post-run capture stamped with the optimizer run id. Never raises."""
    if runtime is None or runtime.flags.enabled("vc_writes_enabled") is not True:
        return None
    try:
        return _capture(runtime, space_id=space_id, reason="optimizer_after",
                        optimizer_run_id=run_id, environment=environment)
    except Exception:  # noqa: BLE001
        logger.warning("VC observe capture-after failed for space %s (run %s)",
                       space_id, run_id, exc_info=True)
        return None


def capture_after_when_complete(runtime, *, space_id: str, run_id: str, job_run_id: str | int | None,
                                get_run: Callable[[int], Any], sleep: Callable[[float], None] = time.sleep,
                                poll_interval_s: float = 30.0, timeout_s: float = 3 * 60 * 60,
                                environment: str | None = None):
    """Poll the optimizer Job run until terminal, then capture the after-state.

    ``get_run(run_id)`` returns the SDK run whose ``.state.life_cycle_state`` is checked.
    Fail-safe: any polling error, a missing ``job_run_id``, or a timeout falls back to a
    best-effort immediate capture so the after-version is still recorded (the passive
    ``capture_on_open`` path is the other backstop).
    """
    if runtime is None or runtime.flags.enabled("vc_writes_enabled") is not True:
        return None
    deadline = time.monotonic() + timeout_s
    if job_run_id is not None:
        try:
            while time.monotonic() < deadline:
                run = get_run(int(job_run_id))
                state = getattr(getattr(run, "state", None), "life_cycle_state", None)
                state_name = getattr(state, "value", state)
                if state_name in _TERMINAL_STATES:
                    break
                sleep(poll_interval_s)
        except Exception:  # noqa: BLE001 - fall through to a best-effort capture
            logger.warning("VC observe after-run poll failed for space %s (run %s)",
                           space_id, run_id, exc_info=True)
    return capture_after(runtime, space_id=space_id, run_id=run_id, environment=environment)
