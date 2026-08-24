"""GenieWatch routers (observability surface)."""

from backend.watch.routers.admin import router as watch_admin_router
from backend.watch.routers.cost import router as watch_cost_router
from backend.watch.routers.feedback import router as watch_feedback_router
from backend.watch.routers.resources import router as watch_resources_router
from backend.watch.routers.settings import router as watch_settings_router
from backend.watch.routers.spaces import router as watch_spaces_router
from backend.watch.routers.traffic_gaps import router as watch_traffic_gaps_router
from backend.watch.routers.usage import router as watch_usage_router

__all__ = [
    "watch_admin_router",
    "watch_cost_router",
    "watch_feedback_router",
    "watch_resources_router",
    "watch_settings_router",
    "watch_spaces_router",
    "watch_traffic_gaps_router",
    "watch_usage_router",
]
