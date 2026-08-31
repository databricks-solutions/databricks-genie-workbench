"""Ontology routers (read-only surface; the only writes are settings PUT, the
refresh POST (job trigger), and the Phase-3d decision POST (app-state ledger, OBO)).
No governed-tag / UC write anywhere."""

from backend.ontology.routers.drafts import router as ontology_drafts_router
from backend.ontology.routers.inventory import router as ontology_inventory_router
from backend.ontology.routers.preflight import router as ontology_preflight_router
from backend.ontology.routers.refresh import router as ontology_refresh_router
from backend.ontology.routers.settings import router as ontology_settings_router
from backend.ontology.routers.tags import router as ontology_tags_router
from backend.ontology.routers.taxonomy import router as ontology_taxonomy_router

__all__ = [
    "ontology_drafts_router",
    "ontology_inventory_router",
    "ontology_preflight_router",
    "ontology_refresh_router",
    "ontology_settings_router",
    "ontology_tags_router",
    "ontology_taxonomy_router",
]
