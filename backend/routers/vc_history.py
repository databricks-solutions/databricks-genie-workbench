"""Authenticated VC version-history reads (M02 ledger); main mounts this router.

Reads only — gated on ``vc_history_enabled`` (a read flag, independent of the write
switches). Restore/observe writes live in ``vc_mutations``. History surfacing must
never require ``vc_writes_enabled``: an operator can inspect what the optimizer
changed even when governed writes stay off.
"""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request

from backend.services.version_control import contracts as vc
from backend.services.version_control.canonical_diff import semantic_diff


def _summary_of(version) -> vc.VersionSummary:
    """Project a full ``Version`` down to the wire ``VersionSummary`` shape the
    detail route returns (summary fields the history list already speaks, plus the
    restorable snapshot attached alongside)."""
    context = version.context
    return vc.VersionSummary(
        version_id=version.version_id,
        binding_id=context.binding.binding_id,
        observed_at=context.observed_at,
        origin=context.origin,
        observed_by=context.actor.subject_id,
        parent_version_id=context.parent_version_id,
        restored_from_version_id=context.restored_from_version_id,
        fingerprints=version.snapshot.fingerprints,
        optimizer_run_id=context.optimizer_run_id,
        champion_id=context.champion_id,
    )


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
        raise _error(503, "evidence_unavailable", "Evidence unavailable", stale=True) from error


def build_router(*, ledger, registry, identity, authorize_history, flags=None):
    # main mounts this router and supplies authenticated context and scoped ports.
    router = APIRouter(prefix="/api/version-control")

    def scope(request, binding_id):
        if flags is None or flags.enabled("vc_history_enabled") is not True:
            raise _error(503, "vc_history_disabled", "VC history reads disabled", stale=True)
        authentication = getattr(request.state, "vc_auth", None)
        if authentication is None:
            raise _error(401, "authentication_required", "Authentication required")
        actor = identity.actor(authentication)
        binding = registry.resolve(str(binding_id))
        if (binding.binding_id != str(binding_id) or actor.workspace_id != binding.workspace_id
                or authorize_history(actor, binding) is not True):
            raise PermissionError("Binding history scope denied")
        return actor, binding

    @router.get("/bindings/{binding_id}/versions")
    def versions(binding_id: UUID, request: Request,
                 limit: Annotated[int, Query(ge=1, le=100)] = 25,
                 cursor: str | None = None):
        def read():
            _actor, binding = scope(request, binding_id)
            return ledger.history(binding, cursor, limit)
        return _invoke(read)

    @router.get("/bindings/{binding_id}/versions/{version_id}")
    def version_detail(binding_id: UUID, version_id: UUID, request: Request):
        def read():
            _actor, binding = scope(request, binding_id)
            version = ledger.get_version(binding, str(version_id))  # KeyError -> 404
            detail = dict(vc.to_wire(_summary_of(version)))
            detail["snapshot"] = vc.to_wire(version.snapshot.serialized_space)
            return detail
        return _invoke(read)

    @router.get("/bindings/{binding_id}/diff")
    def diff(binding_id: UUID, request: Request,
             left: Annotated[str, Query(min_length=1)],
             right: Annotated[str, Query(min_length=1)]):
        def read():
            _actor, binding = scope(request, binding_id)
            left_version = ledger.get_version(binding, left)  # KeyError -> 404
            right_version = ledger.get_version(binding, right)
            try:
                items = tuple(semantic_diff(left_version.snapshot, right_version.snapshot))
            except ValueError:
                # Incompatible canonicalizer versions: report UNKNOWN, never a
                # misleading empty (equal) diff.
                return vc.SemanticDiff(vc.Comparison.UNKNOWN, ())
            comparison = vc.Comparison.EQUAL if not items else vc.Comparison.DIFFERENT
            return vc.SemanticDiff(comparison, items)
        return _invoke(read)

    return router
