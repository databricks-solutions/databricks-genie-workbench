"""
Genie Workbench - Main entry point.

Unified Databricks Genie Space management platform combining:
- GenieRx: Deep LLM analysis, optimization suggestions, fix agent
- GenieIQ: Org-wide IQ scoring, Lakebase persistence, admin dashboard
"""

import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(dotenv_path=".env.local", override=True)
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _validate_mlflow_experiment() -> bool:
    """Validate that MLFLOW_EXPERIMENT_ID exists in the tracking server."""
    experiment_id = os.environ.get("MLFLOW_EXPERIMENT_ID", "").strip()

    if not experiment_id:
        logger.warning(
            "MLFLOW_EXPERIMENT_ID is not set. MLflow tracing will be disabled."
        )
        return False

    try:
        import mlflow

        tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)

        experiment = mlflow.get_experiment(experiment_id)
        if experiment is None:
            raise ValueError(f"Experiment {experiment_id} not found")

        logger.info(f"MLflow experiment validated: {experiment.name} (ID: {experiment_id})")
        return True

    except Exception as e:
        logger.warning(
            f"MLflow experiment ID '{experiment_id}' is not valid: {e}. "
            "MLflow tracing will be disabled."
        )
        os.environ.pop("MLFLOW_EXPERIMENT_ID", None)
        return False


_mlflow_configured = _validate_mlflow_experiment()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from backend.services.auth import is_running_on_databricks_apps, set_obo_user_token, clear_obo_user_token
from backend.routers.analysis import router as analysis_router
from backend.routers.spaces import router as spaces_router
from backend.routers.admin import router as admin_router
from backend.routers.auth import router as auth_router
from backend.routers.create import router as create_router
from backend.routers.auto_optimize import router as auto_optimize_router


class OBOAuthMiddleware(BaseHTTPMiddleware):
    """Extract the user's access token and set a per-request OBO client.

    On Databricks Apps the platform forwards the user's OAuth token in the
    ``x-forwarded-access-token`` header (NOT the standard Authorization
    header).  We store it in a ContextVar so that every
    ``get_workspace_client()`` call in the request path returns a client
    authenticated as the user — not the service principal.

    Ref: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth#user-authorization

    For streaming endpoints (SSE), the ContextVar is NOT cleared after
    ``call_next`` because the response body streams lazily. Instead,
    streaming handlers must call ``set_obo_user_token`` themselves from
    within the generator (the token is stashed on ``request.state``).
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path.startswith("/api/"):
            token = request.headers.get("x-forwarded-access-token", "")
            if token:
                set_obo_user_token(token)
                logger.info("OBO: using user token for %s", request.url.path)
            else:
                logger.info("OBO: no x-forwarded-access-token, using SP for %s", request.url.path)
            request.state.user_token = token
        else:
            request.state.user_token = ""

        response = await call_next(request)

        is_streaming = getattr(response, "media_type", "") == "text/event-stream"
        if not is_streaming:
            clear_obo_user_token()
        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to all responses."""

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        return response


app = FastAPI(
    title="Genie Workbench",
    description="Unified Databricks Genie Space management platform",
    version="1.0.0",
)

if _mlflow_configured:
    try:
        from mlflow.genai.agent_server import setup_mlflow_git_based_version_tracking
        setup_mlflow_git_based_version_tracking()
    except Exception as e:
        logger.warning(f"MLflow git-based version tracking not configured: {e}")

app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(OBOAuthMiddleware)

if not is_running_on_databricks_apps():
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization"],
    )

# Lakebase connection pool lifecycle
def _ensure_gso_job_run_as() -> None:
    """Ensure the GSO optimization job's run_as matches this app's SP.

    Ported from the standalone GSO app's _JobRunAsBootstrap (app.py).
    The app runs as the SP, so it can update its own job's run_as
    without needing the "Service Principal User" role on the deployer.
    """
    job_id_str = os.environ.get("GSO_JOB_ID", "")
    if not job_id_str.isdigit():
        return
    try:
        from backend.services.auth import get_service_principal_client
        from genie_space_optimizer.backend.job_launcher import ensure_job_run_as

        ws = get_service_principal_client()
        sp_client_id = ws.config.client_id or os.environ.get("DATABRICKS_CLIENT_ID", "")
        if sp_client_id:
            ensure_job_run_as(ws, int(job_id_str), sp_client_id)
    except Exception:
        logger.warning("Could not verify GSO job run_as", exc_info=True)


@app.on_event("startup")
async def startup():
    from backend.services.lakebase import init_pool
    await init_pool()
    from backend.services.create_agent_session import _ensure_table
    await _ensure_table()
    _ensure_gso_job_run_as()
    try:
        from backend.routers.auto_optimize import probe_iterations_schema
        probe_iterations_schema()
    except Exception:
        logger.warning("iterations schema probe failed", exc_info=True)


@app.on_event("shutdown")
async def shutdown():
    from backend.services.lakebase import close_pool
    await close_pool()


# Mount all routers
app.include_router(analysis_router)
app.include_router(spaces_router)
app.include_router(admin_router)
app.include_router(auth_router)
app.include_router(create_router)
app.include_router(auto_optimize_router)

# Serve static files from React build
FRONTEND_DIST = Path(__file__).parent.parent / "frontend" / "dist"
FRONTEND_DIST_RESOLVED = FRONTEND_DIST.resolve()

if FRONTEND_DIST.exists():
    if (FRONTEND_DIST / "assets").exists():
        app.mount(
            "/assets", StaticFiles(directory=FRONTEND_DIST / "assets"), name="assets"
        )

    _NO_CACHE = {"Cache-Control": "no-store, must-revalidate"}

    @app.get("/")
    async def serve_root():
        return FileResponse(FRONTEND_DIST / "index.html", headers=_NO_CACHE)

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        # Serve static files from dist/ (e.g. favicon.svg) if they exist
        # .resolve() + .is_relative_to() prevents path traversal via encoded ..
        static_file = (FRONTEND_DIST / full_path).resolve()
        if static_file.is_file() and static_file.is_relative_to(FRONTEND_DIST_RESOLVED):
            return FileResponse(static_file)
        return FileResponse(FRONTEND_DIST / "index.html", headers=_NO_CACHE)

else:
    @app.get("/")
    async def serve_root_debug():
        return {
            "error": "Frontend not built or not deployed",
            "expected_path": str(FRONTEND_DIST),
            "hint": "Run: cd frontend && npm run build",
        }


def main():
    """Start the Genie Workbench server."""
    import uvicorn
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        reload=not is_running_on_databricks_apps(),
    )


if __name__ == "__main__":
    main()
