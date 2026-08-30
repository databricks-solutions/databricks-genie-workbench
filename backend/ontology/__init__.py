"""Ontology — the read-only estate-ontology surface (Phase 1).

A self-contained subsystem, mounted under ``/api/ontology/*`` and registered
separately in ``backend/main.py`` (the exact GenieWatch shape). Phase 1 is a
thin, read-only page that renders the estate's existing ontology and the
governed-tag substrate — no proposal engine, no embeddings, no clustering, no
external context, and the only write is ``PUT /api/ontology/settings`` (our own
config, never Unity Catalog).

See ``docs/design/ontology-phase1-build.md`` (build spec) and
``docs/design/ontology-engine-architecture.md`` (design source of truth).
"""
