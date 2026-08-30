"""Deterministic taxonomy (spec §11): a fixture tag-graph with Finance,
Finance/Tax, and an untagged metric view produces one DomainNode with one
SubDomainNode and one entry in ungrouped.metric_views."""

from __future__ import annotations

from backend.ontology.services import taxonomy


def _graph():
    return {
        "tags": [
            {
                "tag_key": "Finance",
                "allowed_values": [],
                "assignment_count": 1,
                "members": [{"fqn": "finance.core.ledger", "asset_type": "table"}],
            },
            {
                "tag_key": "Finance/Tax",
                "allowed_values": [],
                "assignment_count": 1,
                "members": [{"fqn": "finance.tax.filings", "asset_type": "table"}],
            },
        ],
        "as_of": "2026-08-29T00:00:00Z",
    }


def test_finance_domain_with_one_subdomain_and_ungrouped_mv():
    metric_views = ["finance.reporting.untagged_mv"]
    result = taxonomy.build_taxonomy(_graph(), metric_views, genie_agents=[])

    assert len(result.domains) == 1
    dom = result.domains[0]
    assert dom.tag_key == "Finance"
    assert len(dom.subdomains) == 1
    assert dom.subdomains[0].name == "Tax"
    assert dom.subdomains[0].tag_value == "Tax"
    # Direct member on Finance + one under Finance/Tax = member_count 2.
    assert dom.member_count == 2

    assert len(result.ungrouped.metric_views) == 1
    assert result.ungrouped.metric_views[0].fqn == "finance.reporting.untagged_mv"
    assert result.ungrouped.metric_views[0].asset_type == "metric_view"


def test_tagged_metric_view_not_ungrouped():
    # A metric view that IS tagged under a domain must not appear in ungrouped.
    metric_views = ["finance.core.ledger", "finance.reporting.untagged_mv"]
    result = taxonomy.build_taxonomy(_graph(), metric_views, genie_agents=[])
    ungrouped_fqns = {m.fqn for m in result.ungrouped.metric_views}
    assert "finance.core.ledger" not in ungrouped_fqns
    assert "finance.reporting.untagged_mv" in ungrouped_fqns


def test_classification_tag_is_not_a_domain():
    # A top-level tag with no '/' children (e.g. sensitivity) is not a domain;
    # its assets fall into ungrouped, never invent a domain.
    graph = {
        "tags": [
            {"tag_key": "sensitivity", "allowed_values": ["public"], "assignment_count": 1,
             "members": [{"fqn": "c.s.t", "asset_type": "table"}]},
        ],
        "as_of": "2026-08-29T00:00:00Z",
    }
    result = taxonomy.build_taxonomy(graph, ["c.s.mv"], genie_agents=[])
    assert result.domains == []
    assert {m.fqn for m in result.ungrouped.metric_views} == {"c.s.mv"}


def test_domain_classification_helpers():
    keys = ["Finance", "Finance/Tax", "sensitivity"]
    assert taxonomy.acts_as_domain("Finance", keys) is True
    assert taxonomy.acts_as_domain("sensitivity", keys) is False
    assert taxonomy.acts_as_subdomain("Finance/Tax") is True
    assert taxonomy.acts_as_subdomain("Finance") is False
