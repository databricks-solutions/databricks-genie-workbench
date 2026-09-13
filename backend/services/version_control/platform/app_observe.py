"""Fail-closed app wiring for the observe-only VC surface.

``main`` calls these to (1) derive the observe runtime config from the app
environment (returning ``None`` — no surface — unless the trusted-workspace
control-plane vars are all set), (2) mount the reads-plus-capture routers, and
(3) derive the per-request ``vc_auth`` from the Databricks Apps forwarded-identity
headers. Everything is fail-closed: without config, ``main`` mounts nothing and
behaves exactly as before (deploy-only validated per the install-path contract).

CUJ-1 (history, capture, in-workspace restore) is native: ``observe_config_from_env``
hardwires it on whenever the control-plane vars resolve, and the space-keyed restore
(``restore_local``, mounted by ``vc_spaces``) is a live OBO write. The GOVERNED
binding-keyed restore (``vc_mutations``) stays fail-closed via ``FailClosedRestore`` —
it can never execute until a real governed-write restore service is wired and
platform-checked. Promotion / optimizer-apply / reconcile are not built here.
"""

from typing import Any

from backend.services.version_control import contracts as vc
from backend.services.version_control.platform.observe_seams import ObserveRuntime

_REQUIRED_ENV = (
    "VC_OBSERVE_WORKSPACE_ID",
    "VC_OBSERVE_CATALOG",
    "VC_OBSERVE_CONTROL_SCHEMA",
    "VC_OBSERVE_WAREHOUSE_ID",
    "VC_OBSERVE_HOST",
    "VC_OBSERVE_SP_PRINCIPAL_ID",
)


def observe_config_from_env(env: Any) -> dict | None:
    """Assemble the observe runtime config from env, or ``None`` if incomplete.

    On Databricks Apps the app SP is the ambient identity, so ``roles.executor`` carries
    no profile/auth block and ``PlatformAdapters`` builds an ambient ``WorkspaceClient``.
    """
    if any(not env.get(key) for key in _REQUIRED_ENV):
        return None
    warehouse_id = env["VC_OBSERVE_WAREHOUSE_ID"]
    host = env["VC_OBSERVE_HOST"]
    workspace_id = env["VC_OBSERVE_WORKSPACE_ID"]
    profile = env.get("VC_OBSERVE_PROFILE") or None
    # The VC identity model admits an executor only via an explicitly-bound OAuth M2M
    # profile or OBO/pat — never a bare ambient client. On Databricks Apps the app runs
    # as its service principal via ambient OAuth M2M credentials injected as
    # DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET (see services/auth.py). So when no
    # operator profile is supplied (the deployed-app path), bind the app SP as an m2m
    # executor: a stable routing key into the identity provider's ``_profiles`` plus an
    # ``auth`` block that constructs the client from those env vars. This is the in-Job
    # m2m pattern validated by test_client_prefers_m2m_auth_over_profile_routing_key_in_job;
    # ``profile`` there is a routing key only, never a local ~/.databrickscfg profile.
    executor_role: dict[str, Any] = {"warehouse_id": warehouse_id, "host": host}
    if profile:
        executor_role["profile"] = profile
        selection_profile = profile
    else:
        selection_profile = "vc-observe-app-sp"
        executor_role["profile"] = selection_profile
        executor_role["auth"] = {
            "mode": "m2m",
            "host": host,
            "client_id_env": env.get("VC_OBSERVE_CLIENT_ID_ENV") or "DATABRICKS_CLIENT_ID",
            "client_secret_env": env.get("VC_OBSERVE_CLIENT_SECRET_ENV") or "DATABRICKS_CLIENT_SECRET",
        }
    selection = {
        "workspace_id": workspace_id,
        "host": host,
        "principal_id": env["VC_OBSERVE_SP_PRINCIPAL_ID"],
        "execution_ref": env.get("VC_OBSERVE_EXECUTION_REF") or "app/genie-workbench",
        "profile": selection_profile,
    }
    return {
        "workspace_id": workspace_id,
        "catalog": env["VC_OBSERVE_CATALOG"],
        "control_schema": env["VC_OBSERVE_CONTROL_SCHEMA"],
        "warehouse_id": warehouse_id,
        "target_warehouse_id": warehouse_id,
        "target_selection": selection,
        "environment": env.get("VC_OBSERVE_ENVIRONMENT") or "prod",
        "roles": {"executor": executor_role},
        # CUJ-1 (version history, capture, in-workspace restore) is a native, baked-in
        # feature: it is ON whenever the observe surface is wired (all VC_OBSERVE_* above
        # resolved). There is deliberately no env switch to disable it. The governed CUJ-2+
        # writes (promotion / optimizer-apply / reconcile) are NOT part of this surface —
        # they default off in the governed composition and the app never performs them
        # (vc_mutations mounts FailClosedRestore).
        "flags": {
            "vc_history_enabled": True,
            "vc_writes_enabled": True,
            "vc_restore_enabled": True,
        },
    }


def vc_auth_from_request(headers: Any, workspace_id: str, *, dev_email: str | None = None):
    """Derive ``AuthenticatedRequest`` from Databricks Apps forwarded-identity headers.

    Returns ``None`` when no authenticated subject is present (the routers then fail
    closed with 401). Reads are proxied through the trusted target workspace, so the
    workspace_id is the observe runtime's, not the caller's.

    Prefer the human email: Databricks Apps forwards the address in ``X-Forwarded-Email``
    (and the username in ``X-Forwarded-Preferred-Username``), while ``X-Forwarded-User`` is
    the *numeric user id* — a cryptic value that would surface as the version author. Only
    fall back to the id when no readable identity is forwarded.
    """
    subject = (headers.get("X-Forwarded-Email")
               or headers.get("X-Forwarded-Preferred-Username")
               or headers.get("X-Forwarded-User")
               or dev_email)
    if not subject:
        return None
    return vc.AuthenticatedRequest(subject, workspace_id)


class FailClosedRestore:
    """Satisfies the ``vc_mutations`` restore port while never executing a mutation."""

    def submit(self, _operation_id: str, _actor: Any) -> Any:
        raise PermissionError("Restore is not wired in the observe-only surface")


def mount_observe_routers(app: Any, runtime: ObserveRuntime) -> None:
    """Mount the reads-plus-capture routers (history + observe/restore) from the runtime.

    History reads, observe capture, and the space-keyed in-workspace restore
    (``vc_spaces``) are the native live surface. The governed binding-keyed restore on
    ``vc_mutations`` stays fail-closed via ``FailClosedRestore``.
    """
    from backend.routers.vc_history import build_router as build_history_router
    from backend.routers.vc_mutations import build_router as build_mutations_router
    from backend.routers.vc_spaces import build_router as build_spaces_router

    app.include_router(build_history_router(
        ledger=runtime.ledger, registry=runtime.registry, identity=runtime.identity,
        authorize_history=runtime.authorize_history, flags=runtime.flags))
    app.include_router(build_mutations_router(
        observer=runtime.observer, restore=FailClosedRestore(), identity=runtime.identity,
        registry=runtime.registry, facts=runtime.facts,
        authorize_history=runtime.authorize_history,
        reader_selection=runtime.reader_selection, flags=runtime.flags))
    # Space-keyed bridge for the SpaceDetail Version Control tab.
    app.include_router(build_spaces_router(runtime=runtime))
