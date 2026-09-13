"""Space-keyed VC bridge: the workbench is ``space_id``-keyed, the ledger is
binding-keyed. These endpoints resolve (or auto-enroll) the binding for a Genie space so
the SpaceDetail Version Control tab can list captured history and capture the current
state without knowing the internal binding id.

* ``GET /spaces/{space_id}/versions`` — history for the space (empty when not yet
  enrolled); read, gated on ``vc_history_enabled``.
* ``POST /spaces/{space_id}/observe`` — auto-enroll + capture the current state; write,
  gated on ``vc_writes_enabled``.
"""

import json
import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Path, Query, Request
from pydantic import BaseModel, ConfigDict, StringConstraints

from backend.services.version_control import contracts as vc
from backend.services.version_control.observe_optimizer import resolve_or_enroll_bound
from backend.services.version_control.restore_local import restore_space_version

logger = logging.getLogger(__name__)

SpaceId = Annotated[str, Path(pattern=r"^[0-9a-zA-Z_-]{1,128}$")]


class RestoreSpaceBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version_id: UUID
    expected_current_version_id: UUID


class TagBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    label: Annotated[str, StringConstraints(min_length=1, max_length=60, strip_whitespace=True)]
    note: str | None = None


def _obo_patch_live(client, space_id, serialized_space, description):
    """PATCH the live serialized space (+ description) as the OBO user. No SP fallback:
    the user's own edit rights are the authorization, so a non-editor is rejected here."""
    path = f"/api/2.0/genie/spaces/{space_id}"
    client.api_client.do("PATCH", path, body={"serialized_space": json.dumps(serialized_space)})
    if description is not None:
        client.api_client.do("PATCH", path, body={"description": description})


def _error(status, code, message, *, stale=False):
    return HTTPException(status, detail=vc.to_wire(vc.ApiError(
        code=code, message=message, retryable=False, stale=stale)))


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
        # Fail closed to the client, but never silently: the server-side cause is the
        # only signal for an opaque evidence_unavailable 503.
        logger.exception("VC space request failed")
        raise _error(503, "evidence_unavailable", "Evidence unavailable", stale=True) from error


async def _ainvoke(op):
    """Async twin of ``_invoke`` for the tag routes (which await lakebase). Maps the same
    exceptions but returns the awaited result verbatim — tag responses are plain dicts, not
    wire values, so there is no ``to_wire`` wrap here."""
    try:
        return await op()
    except HTTPException:
        raise
    except PermissionError as error:
        raise _error(403, "scope_denied", str(error)) from error
    except LookupError as error:
        raise _error(404, "resource_not_found", "Scoped resource not found") from error
    except (ValueError, TypeError) as error:
        raise _error(409, "request_conflict", str(error)) from error
    except Exception as error:
        logger.exception("VC space request failed")
        raise _error(503, "evidence_unavailable", "Evidence unavailable", stale=True) from error


def build_router(*, runtime):
    router = APIRouter(prefix="/api/version-control")
    ledger, registry, identity = runtime.ledger, runtime.registry, runtime.identity
    flags, authorize_history = runtime.flags, runtime.authorize_history

    def actor_for(request):
        authentication = getattr(request.state, "vc_auth", None)
        if authentication is None:
            raise _error(401, "authentication_required", "Authentication required")
        return identity.actor(authentication)

    @router.get("/config")
    def config():
        # Unauthenticated flag surface so the UI can render disabled-with-explainer states
        # (CUJ-1 §5) before attempting a gated write.
        return {
            "history_enabled": flags.enabled("vc_history_enabled") is True,
            "writes_enabled": flags.enabled("vc_writes_enabled") is True,
            "restore_enabled": flags.enabled("vc_restore_enabled") is True,
        }

    @router.get("/spaces/{space_id}/versions")
    def space_versions(space_id: SpaceId, request: Request,
                       limit: Annotated[int, Query(ge=1, le=100)] = 25,
                       cursor: str | None = None):
        def read():
            if flags.enabled("vc_history_enabled") is not True:
                raise _error(503, "vc_history_disabled", "VC history reads disabled", stale=True)
            actor = actor_for(request)
            binding = registry.find_active_by_space_key(space_id)
            if binding is None:
                return vc.VersionPage((), None)  # not enrolled yet -> empty history
            if (actor.workspace_id != binding.workspace_id
                    or authorize_history(actor, binding) is not True):
                raise PermissionError("Binding history scope denied")
            return ledger.history(binding, cursor, limit)
        return _invoke(read)

    @router.post("/spaces/{space_id}/observe")
    def space_observe(space_id: SpaceId, request: Request):
        def capture():
            if flags.enabled("vc_writes_enabled") is not True:
                raise _error(503, "vc_writes_disabled", "VC writes disabled", stale=True)
            actor = actor_for(request)
            binding = resolve_or_enroll_bound(runtime, space_id=space_id)
            if authorize_history(actor, binding) is not True:
                raise PermissionError("Binding history scope denied")
            # Record the authenticated human as the ledger actor (authorship), and read the
            # live serialized space under the SAME user (OBO) via `live_reader`: the app SP
            # usually has no grant on a user-owned Genie space, so an SP-pinned read 403s
            # (mirrors restore's OBO live GET). The lease, status read, and ledger append
            # still run as the SP executor inside `capture_on_open`.
            from backend.services.genie_client import get_genie_space
            return runtime.observer.capture_on_open(
                binding, actor, actor_override=actor,
                live_reader=lambda: get_genie_space(space_id))
        return _invoke(capture)

    @router.post("/spaces/{space_id}/restore")
    def space_restore(space_id: SpaceId, body: RestoreSpaceBody, request: Request):
        def restore():
            if (flags.enabled("vc_writes_enabled") is not True
                    or flags.enabled("vc_restore_enabled") is not True):
                raise _error(503, "vc_restore_disabled", "VC restore is not enabled", stale=True)
            actor = actor_for(request)
            # Live GET/PATCH run as the OBO user (their edit rights are the authorization);
            # the ledger record runs as the SP via the runtime, like every other capture.
            from backend.services.auth import get_workspace_client
            from backend.services.genie_client import get_genie_space
            client = get_workspace_client()
            return restore_space_version(
                runtime, space_id=space_id, version_id=str(body.version_id),
                expected_current_version_id=str(body.expected_current_version_id), actor=actor,
                live_reader=lambda sid: get_genie_space(sid),
                live_writer=lambda sid, ss, desc: _obo_patch_live(client, sid, ss, desc))
        return _invoke(restore)

    def resolve_readable(space_id, request):
        """Mirror the space_versions read gate: authenticate the actor, resolve the
        space's active binding, and enforce the workspace + history scope. Returns
        ``(binding, actor)``; binding is None when the space is not yet enrolled."""
        actor = actor_for(request)
        binding = registry.find_active_by_space_key(space_id)
        if binding is not None and (actor.workspace_id != binding.workspace_id
                                    or authorize_history(actor, binding) is not True):
            raise PermissionError("Binding history scope denied")
        return binding, actor

    @router.get("/spaces/{space_id}/tags")
    async def space_tags(space_id: SpaceId, request: Request):
        from backend.services import lakebase

        async def op():
            if flags.enabled("vc_history_enabled") is not True:
                raise _error(503, "vc_history_disabled", "VC history reads disabled", stale=True)
            binding, _ = resolve_readable(space_id, request)
            if binding is None:
                return {}  # not enrolled yet -> no tags
            return await lakebase.get_version_tags(space_id)
        return await _ainvoke(op)

    @router.put("/spaces/{space_id}/versions/{version_id}/tag")
    async def set_tag(space_id: SpaceId, version_id: UUID, body: TagBody, request: Request):
        from backend.services import lakebase

        async def op():
            if flags.enabled("vc_writes_enabled") is not True:
                raise _error(503, "vc_writes_disabled", "VC writes disabled", stale=True)
            binding, actor = resolve_readable(space_id, request)
            if binding is None:
                raise _error(404, "resource_not_found", "Space is not enrolled in version control")
            await lakebase.set_version_tag(space_id, str(version_id), body.label, body.note,
                                           actor.subject_id)
            return {"version_id": str(version_id), "label": body.label,
                    "note": body.note, "author": actor.subject_id}
        return await _ainvoke(op)

    @router.delete("/spaces/{space_id}/versions/{version_id}/tag")
    async def delete_tag(space_id: SpaceId, version_id: UUID, request: Request):
        from backend.services import lakebase

        async def op():
            if flags.enabled("vc_writes_enabled") is not True:
                raise _error(503, "vc_writes_disabled", "VC writes disabled", stale=True)
            binding, _ = resolve_readable(space_id, request)
            if binding is None:
                raise _error(404, "resource_not_found", "Space is not enrolled in version control")
            await lakebase.delete_version_tag(space_id, str(version_id))
            return {"version_id": str(version_id), "deleted": True}
        return await _ainvoke(op)

    return router
