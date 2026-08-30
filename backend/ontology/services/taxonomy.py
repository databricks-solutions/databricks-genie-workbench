"""Deterministic taxonomy (pure function of the tag graph).

Groups governed-tag assignments into a Domain → Sub-Domain → member-asset tree
using the ``{Domain}`` / ``{Domain}/{Sub}`` governed-tag convention (MV-D37) —
no clustering, no LLM, no lineage-invented domains (that is L4, deferred).

Domain identification (Phase 1, deterministic): the ``/`` convention is the only
signal. A tag key with ``/`` is a sub-domain; a top-level key that is the parent
of at least one sub-domain is a domain. Top-level tags with no sub-domain
children (e.g. classification tags like ``sensitivity``) are NOT treated as
domains — their assets fall into ``ungrouped`` as a coverage signal rather than
inventing a domain from a classification tag. Lineage adjacency (SP) may later
order/annotate the ungrouped bucket; it never creates a domain.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from backend.ontology.models import (
    DomainNode,
    MemberAsset,
    OntologyTaxonomy,
    SubDomainNode,
    UngroupedBucket,
)


def domain_part(tag_key: str) -> str:
    """The top-level segment of a tag key (before the first ``/``)."""
    return tag_key.split("/", 1)[0]


def subdomain_part(tag_key: str) -> str | None:
    """The sub-domain segment (after the first ``/``), or ``None`` if top-level."""
    return tag_key.split("/", 1)[1] if "/" in tag_key else None


def is_subdomain_key(tag_key: str) -> bool:
    return "/" in tag_key


def domain_keys(all_keys: list[str]) -> set[str]:
    """The top-level keys that act as domains: those with ≥1 sub-domain child."""
    parents: set[str] = set()
    for k in all_keys:
        if "/" in k:
            parents.add(domain_part(k))
    return parents


def acts_as_domain(tag_key: str, all_keys: list[str]) -> bool:
    return "/" not in tag_key and tag_key in domain_keys(all_keys)


def acts_as_subdomain(tag_key: str) -> bool:
    return is_subdomain_key(tag_key)


def _members(raw: list[dict[str, Any]]) -> list[MemberAsset]:
    return [MemberAsset(fqn=m["fqn"], asset_type=m.get("asset_type", "table")) for m in raw]


def build_taxonomy(
    graph: dict[str, Any],
    metric_views: list[str],
    genie_agents: list[str],
) -> OntologyTaxonomy:
    """Assemble the Domain → Sub-Domain tree + ungrouped coverage bucket.

    ``graph`` is :func:`backend.ontology.services.tag_graph.build_graph` output.
    ``metric_views`` / ``genie_agents`` are the in-scope estate inventories; any
    that carry no domain tag land in ``ungrouped`` (the coverage signal).
    """
    tags = {t["tag_key"]: t for t in graph.get("tags", [])}
    all_keys = list(tags.keys())
    doms = domain_keys(all_keys)

    # Collect every asset fqn that appears under a domain tag (any grain), so the
    # ungrouped bucket is "estate minus tagged".
    tagged_fqns: set[str] = set()

    domains: list[DomainNode] = []
    for dkey in sorted(doms):
        # Direct (un-sub-domained) members: assignments on the top-level tag itself.
        direct_raw = tags.get(dkey, {}).get("members", [])
        for m in direct_raw:
            tagged_fqns.add(m["fqn"])

        subdomains: list[SubDomainNode] = []
        for skey in sorted(k for k in all_keys if "/" in k and domain_part(k) == dkey):
            sraw = tags[skey].get("members", [])
            for m in sraw:
                tagged_fqns.add(m["fqn"])
            sval = subdomain_part(skey) or skey
            subdomains.append(SubDomainNode(
                tag_value=sval,
                name=sval,
                member_count=len(sraw),
                members=_members(sraw),
            ))

        member_count = len(direct_raw) + sum(s.member_count for s in subdomains)
        domains.append(DomainNode(
            tag_key=dkey,
            name=dkey,
            member_count=member_count,
            subdomains=subdomains,
            members=_members(direct_raw),
        ))

    ungrouped = UngroupedBucket(
        metric_views=[
            MemberAsset(fqn=fqn, asset_type="metric_view")
            for fqn in metric_views
            if fqn not in tagged_fqns
        ],
        genie_agents=[
            MemberAsset(fqn=a, asset_type="genie_agent")
            for a in genie_agents
            if a not in tagged_fqns
        ],
    )

    as_of = graph.get("as_of") or datetime.now(timezone.utc).isoformat()
    return OntologyTaxonomy(domains=domains, ungrouped=ungrouped, as_of=as_of)
