"""Version control asks the one space-access resolver (MV-D109, VC-D-authz1).

Workspace equality is checked first and needs no Genie call. The space's own permission
decides the rest, answered by Genie under the signed-in user's token. A refusal leaves
in the version-control error shape the tab reads.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from backend.services import space_access
from backend.services.space_access import SpaceAccessLevel
from backend.services.version_control import contracts as vc

_SCOPE_DENIED = "Binding history scope denied"


def authorize_space(actor: Any, space_id: str, level: SpaceAccessLevel, *, workspace_id: str) -> None:
    """Refuse unless *actor* is in this workspace and holds *level* on *space_id*."""
    if actor.workspace_id != workspace_id:
        raise PermissionError(_SCOPE_DENIED)
    try:
        space_access.ensure_space_access(space_id, level)
    except HTTPException as refusal:
        raise _vc_refusal(refusal) from refusal


def authorize(actor: Any, binding: Any, level: SpaceAccessLevel, *, workspace_id: str) -> None:
    """Refuse unless *actor* holds *level* on the Genie Agent *binding* is bound to."""
    if not (actor.workspace_id == binding.workspace_id == workspace_id):
        raise PermissionError(_SCOPE_DENIED)
    if not binding.space_id:
        raise PermissionError("Binding is not bound to a Genie Agent")
    authorize_space(actor, binding.space_id, level, workspace_id=workspace_id)


def _vc_refusal(refusal: HTTPException) -> HTTPException:
    detail = refusal.detail if isinstance(refusal.detail, dict) else {}
    return HTTPException(refusal.status_code, detail=vc.to_wire(vc.ApiError(
        code=str(detail.get("code") or "space_access_denied"),
        message=str(detail.get("message") or "Access to this Genie Agent was refused."),
        retryable=refusal.status_code == 503,
        stale=False,
        details={"required": detail.get("required"),
                 "platform_message": detail.get("platform_message", "")},
    )))
