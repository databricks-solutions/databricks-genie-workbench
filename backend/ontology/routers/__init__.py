"""Ontology routers (read-only surface; the only writes are settings PUT, the
refresh POST (job trigger), the Phase-3d decision POST (app-state ledger, OBO),
and — once Phase 5 lands — the human-gated apply POST (the Phase-5 governed-tag
write-back). No governed-tag / UC write anywhere except the Phase-5 apply lane."""

from backend.ontology.routers.apply import router as ontology_apply_router
from backend.ontology.routers.drafts import router as ontology_drafts_router
from backend.ontology.routers.graph import router as ontology_graph_router
from backend.ontology.routers.inventory import router as ontology_inventory_router
from backend.ontology.routers.preflight import router as ontology_preflight_router
from backend.ontology.routers.refresh import router as ontology_refresh_router
from backend.ontology.routers.settings import router as ontology_settings_router
from backend.ontology.routers.tags import router as ontology_tags_router
from backend.ontology.routers.taxonomy import router as ontology_taxonomy_router

__all__ = [
    "ontology_apply_router",
    "ontology_drafts_router",
    "ontology_graph_router",
    "ontology_inventory_router",
    "ontology_preflight_router",
    "ontology_refresh_router",
    "ontology_settings_router",
    "ontology_tags_router",
    "ontology_taxonomy_router",
]
