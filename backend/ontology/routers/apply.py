"""Ontology apply routes (Phase 5 / 17i) — the consented governed-tag apply.

Pre-seeded seam (empty on purpose). The ``router`` object exists and is already
registered in ``backend/main.py`` + ``routers/__init__.py`` so the Phase-5 lane
adds its ``POST /api/ontology/apply/preview`` (dry-run ``ApplyPlan``) and
``POST /api/ontology/apply/execute`` handlers to *this* file only, without
touching the shared registration block. Until Phase 5 lands this router exposes
no routes and carries **no write path** — the governed-tag write tokens and the
single-writer carve arrive with the Phase-5 build, which also extends the
ontology firewall test (``backend/tests/test_ontology_firewall.py``) to sanction
this one module.

Phase-5 apply is the ONLY governed-tag writer in the product and stays strictly
human-gated: it applies only consented decisions from the ledger, under OBO, and
records each action in an audit table. The batch materializer never writes tags.
"""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/api/ontology")
