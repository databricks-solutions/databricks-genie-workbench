"""Context Sources registry (Phase 4, MV-D47) — PURE, wheel-native.

The curated set of AI-Gateway MCP context sources, each classified by
``{class, provenance_tier, influence}`` so the MV-D38 firewall
(:mod:`~genie_space_optimizer.ontology.context_firewall`) holds. This is the SINGLE
home of the registry: the backend preflight tier
(``backend/ontology/services/context_sources.py``) and the wheel resolver
(``context_pack.py``) both import it, so the source list can never drift between
the banner and the batch.

Naming a source here is **config, not a call**: nothing in this module resolves a
Context Pack, calls the AI Gateway, or performs a web search — those live in the
resolver. ``system.ai.web_search`` appears as a registry entry (its securable
name), not an egress path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# Mirror of the backend Literals (kept as strings here so the wheel never imports
# ``backend.*``; the values are identical, so a re-exported entry validates against
# the backend Pydantic model unchanged).
ContextClass = Literal["internal", "external"]
ProvenanceTier = Literal["T0", "T1", "T2", "T3"]

# Plain-language influence summaries surfaced on each source row (MV-D23 — no jargon).
_EXTERNAL_INFLUENCE = "naming, descriptions, synonyms, gap hypotheses, recent context"
_INTERNAL_INFLUENCE = "structural signals, validation (+ naming, synonyms, descriptions)"

# Excluded per MV-D47 — personal-inbox / chat / calendar surfaces are never context
# sources. Named here so the acceptance test can assert they never enter the registry.
EXCLUDED_SOURCE_IDS: frozenset[str] = frozenset({"gmail", "slack", "calendar"})


@dataclass(frozen=True)
class ContextSource:
    """One curated Context Source (MV-D47). ``mcp_fqn`` is the source's securable /
    endpoint identifier (a dotted UC name like ``system.ai.web_search`` for the
    AI-Gateway MCP services, or a workspace MCP path like ``/api/2.0/mcp/genie`` for
    the internal ones). ``default_enabled`` is a per-source default; the whole tier is
    still gated by ``ExternalContext.enabled`` (DEFAULT OFF, MV-D44)."""
    id: str
    label: str
    mcp_fqn: str
    klass: ContextClass
    provenance_tier: ProvenanceTier
    influence: str
    default_enabled: bool


# ── The curated registry (MV-D47 lead) ───────────────────────────────────────
# External web [T3]: AI-Gateway web search + You.com. External company docs [T1]:
# Confluence / Google Drive / Microsoft 365. Internal verified [T0]: Genie One +
# Databricks SQL. Gmail / Slack / Calendar are EXCLUDED. Every external source is
# opt-in (default_enabled=False); the internal verified MCPs default on (they add no
# egress) but still only run when the tier is enabled.
CONTEXT_SOURCES: tuple[ContextSource, ...] = (
    ContextSource(
        id="web_search",
        label="Web search (AI Gateway)",
        mcp_fqn="system.ai.web_search",
        klass="external",
        provenance_tier="T3",
        influence=_EXTERNAL_INFLUENCE,
        default_enabled=False,
    ),
    ContextSource(
        id="youcom",
        label="You.com",
        mcp_fqn="myyoumcp",
        klass="external",
        provenance_tier="T3",
        influence=_EXTERNAL_INFLUENCE,
        default_enabled=False,
    ),
    ContextSource(
        id="confluence",
        label="Confluence",
        mcp_fqn="confluence",
        klass="external",
        provenance_tier="T1",
        influence=_EXTERNAL_INFLUENCE,
        default_enabled=False,
    ),
    ContextSource(
        id="google_drive",
        label="Google Drive",
        mcp_fqn="google_drive",
        klass="external",
        provenance_tier="T1",
        influence=_EXTERNAL_INFLUENCE,
        default_enabled=False,
    ),
    ContextSource(
        id="microsoft_365",
        label="Microsoft 365",
        mcp_fqn="microsoft_365",
        klass="external",
        provenance_tier="T1",
        influence=_EXTERNAL_INFLUENCE,
        default_enabled=False,
    ),
    ContextSource(
        id="genie_one",
        label="Genie One",
        mcp_fqn="/api/2.0/mcp/genie",
        klass="internal",
        provenance_tier="T0",
        influence=_INTERNAL_INFLUENCE,
        default_enabled=True,
    ),
    ContextSource(
        id="databricks_sql",
        label="Databricks SQL",
        mcp_fqn="/api/2.0/mcp/sql",
        klass="internal",
        provenance_tier="T0",
        influence=_INTERNAL_INFLUENCE,
        default_enabled=True,
    ),
)


def get_source(source_id: str) -> ContextSource | None:
    """Look up a registry entry by id, or ``None``."""
    for entry in CONTEXT_SOURCES:
        if entry.id == source_id:
            return entry
    return None


def grant_execute_line(entry: ContextSource, sp: str | None = None) -> str:
    """A single copy-paste-ready ``GRANT EXECUTE`` statement for the banner/tier."""
    target = f"`{sp}`" if sp else "`<app-service-principal>`"
    return f"GRANT EXECUTE ON `{entry.mcp_fqn}` TO {target}"


__all__ = [
    "CONTEXT_SOURCES",
    "ContextSource",
    "EXCLUDED_SOURCE_IDS",
    "ContextClass",
    "ProvenanceTier",
    "get_source",
    "grant_execute_line",
]
