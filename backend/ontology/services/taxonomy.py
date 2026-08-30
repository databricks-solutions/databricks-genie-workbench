"""Deterministic taxonomy (Phase-1 route wrapper over the shared transforms).

The pure tree-building + domain/sub-domain classification now lives in the GSO
wheel (``genie_space_optimizer.ontology.transforms``) so the Phase-2 batch job and
the Phase-1 route call the *same* code (the parity guarantee). This module keeps
the Phase-1 public surface — ``build_taxonomy`` returning an ``OntologyTaxonomy``
model, plus the ``acts_as_domain`` / ``acts_as_subdomain`` helpers the tags router
uses — so the route contract and the Phase-1 tests are unchanged.

Domain identification is the ``{Domain}`` / ``{Domain}/{Sub}`` governed-tag
convention (MV-D37): a key with ``/`` is a sub-domain; a top-level key that parents
≥1 sub-domain is a domain; other top-level tags (e.g. ``sensitivity``) are not
domains and their assets fall into ``ungrouped`` (a coverage signal, never an
invented domain). No clustering, no LLM.
"""

from __future__ import annotations

from typing import Any

from genie_space_optimizer.ontology import transforms

from backend.ontology.models import OntologyTaxonomy

# Re-export the pure classification helpers (single source of truth in the wheel)
# so callers like the tags router keep importing them from here.
domain_part = transforms.domain_part
subdomain_part = transforms.subdomain_part
is_subdomain_key = transforms.is_subdomain_key
domain_keys = transforms.domain_keys
acts_as_domain = transforms.acts_as_domain
acts_as_subdomain = transforms.acts_as_subdomain


def build_taxonomy(
    graph: dict[str, Any],
    metric_views: list[str],
    genie_agents: list[str],
) -> OntologyTaxonomy:
    """Assemble the Domain → Sub-Domain tree + ungrouped coverage bucket.

    ``graph`` is the tag-graph structure (mirror- or live-sourced). Delegates the
    pure assembly to the shared transform and wraps it in the Phase-1 model.
    """
    tree = transforms.build_taxonomy_dict(graph, metric_views, genie_agents)
    return OntologyTaxonomy(**tree)
