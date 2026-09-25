"""Authenticated M04 observation and restore adapters; M08 mounts this router."""

from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from backend.services.version_control import contracts as vc


class ObserveBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: Literal["open", "history", "refresh", "return"] = "open"


class RestoreBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version_id: UUID
    binding_revision: int = Field(gt=0)
    expected_base: str = Field(pattern=r"^[0-9a-f]{64}$")
    approval_id: UUID


CommandKey = Annotated[str, Header(alias="Idempotency-Key", min_length=1)]


def _error(status, code, message, *, stale=False, operation_id=None):
    return HTTPException(status, detail=vc.to_wire(vc.ApiError(
        code=code, message=message, retryable=False, stale=stale, operation_id=operation_id)))


def _invoke(call):
    try:
        return vc.to_wire(call())
    except HTTPException:
        raise
    except PermissionError as error:
        raise _error(403, "scope_denied", str(error)) from error
    except LookupError as error:
        raise _error(404, "resource_not_found", "Scoped resource not found") from error
    except (ValueError, TypeError) as error:
        raise _error(409, "request_conflict", str(error)) from error
    except Exception as error:
        raise _error(503, "evidence_unavailable", "Evidence unavailable; do not replay mutation", stale=True) from error


def build_router(*, observer, restore, identity, registry, facts, authorize_history,
                 reader_selection, flags=None):
    # M08 mounts this router and supplies authenticated context and scoped ports.
    router = APIRouter(prefix="/api/version-control")

    def scope(request, binding_id):
        authentication = getattr(request.state, "vc_auth", None)
        if authentication is None:
            raise _error(401, "authentication_required", "Authentication required")
        actor = identity.actor(authentication)
        binding = registry.resolve(str(binding_id))
        if (binding.binding_id != str(binding_id) or actor.workspace_id != binding.workspace_id
                or authorize_history(actor, binding) is not True):
            raise PermissionError("Binding history scope denied")
        return actor, binding

    @router.post("/bindings/{binding_id}/observe")
    def observe(binding_id: UUID, body: ObserveBody, request: Request, idempotency_key: CommandKey):
        def capture():
            actor, binding = scope(request, binding_id)
            if body.reason == "open":
                return observer.capture_on_open(binding, actor)
            executor = identity.executor(reader_selection)
            if executor.workspace_id != binding.workspace_id:
                raise PermissionError("Snapshot reader workspace mismatch")
            return observer.capture(binding, body.reason, executor)
        return _invoke(capture)

    @router.post("/bindings/{binding_id}/restore", status_code=202)
    def submit_restore(binding_id: UUID, body: RestoreBody, request: Request, idempotency_key: CommandKey):
        def submit():
            actor, binding = scope(request, binding_id)
            if (flags is None or flags.enabled("vc_writes_enabled") is not True
                    or flags.enabled("vc_restore_enabled") is not True):
                raise _error(503, "vc_writes_disabled", "VC restore writes disabled", stale=True)
            history = facts.lookup_request(binding, idempotency_key)
            identities = {fact.request for fact in history.facts}
            if (history.ambiguous or len(identities) != 1
                    or any(fact.binding != binding or fact.request.idempotency_key != idempotency_key
                           or fact.operation_id != fact.request.operation_id for fact in history.facts)):
                raise ValueError("A unique durable restore request is required")
            operation = facts.get_request(next(iter(identities)).operation_id).request
            if (not isinstance(operation, vc.MutationRequest) or operation.operation_type != "restore"
                    or operation.identity not in identities or operation.binding != binding
                    or operation.binding.binding_revision != body.binding_revision
                    or operation.source_version_id != str(body.version_id)
                    or operation.expected_base != body.expected_base
                    or operation.approval_id != str(body.approval_id)):
                raise ValueError("Restore body must match the immutable approved request")
            try:
                return restore.submit(operation.identity.operation_id, actor)
            except (PermissionError, LookupError, ValueError, TypeError):
                raise
            except Exception as error:
                raise _error(503, "dispatch_unavailable", "Poll operation; do not replay mutation",
                             stale=True, operation_id=operation.identity.operation_id) from error
        return _invoke(submit)

    return router
