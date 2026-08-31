"""Facet vs aboutness classifier — offline unit tests (Stage 1, MV-D51).

The seed denylist routes to FACET; airline business areas stay ABOUTNESS; the enum
backstop and the optional degrading LLM tiebreaker behave.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.ontology import transforms as t

# The seed denylist from §5.2 / the acceptance criteria.
_FACET_SEED = [
    "Contains Synthetic", "Data Tier", "Certification", "Controlled Placeholder",
    "Governance", "Reference", "Open Reference", "Demo Domain", "Demos", "Demo",
    "Techsummit-fy27", "Domain", "Flight School Team", "Sensitivity",
]

_ABOUTNESS = ["Revenue", "Maintenance", "Loyalty", "Reservation", "Route", "Fleet",
              "Passenger", "Commercial", "Finance"]


@pytest.mark.parametrize("tag", _FACET_SEED)
def test_seed_denylist_routes_to_facet(tag):
    klass, reason = t.classify_tag(tag)
    assert klass == "facet", f"{tag} should be a facet ({reason})"
    assert t.is_facet_tag(tag)


@pytest.mark.parametrize("tag", _ABOUTNESS)
def test_business_areas_stay_aboutness(tag):
    klass, reason = t.classify_tag(tag)
    assert klass == "aboutness", f"{tag} should be aboutness ({reason})"
    assert not t.is_facet_tag(tag)


def test_subtag_inherits_top_level_classification():
    # Facet on the domain part → facet regardless of the leaf.
    assert t.is_facet_tag("Data Tier/Gold")
    # Aboutness domain with a sub is still aboutness.
    assert not t.is_facet_tag("Revenue/Bookings")


def test_enum_backstop_flags_flag_like_value_sets():
    klass, reason = t.classify_tag("Access Level", allowed_values=["public", "internal", "confidential"])
    assert klass == "facet" and "enumerated" in reason
    # A business tag whose values are not flag-like stays aboutness.
    assert not t.is_facet_tag("Region", allowed_values=["EMEA", "APAC", "North America"])


def test_llm_tiebreaker_only_for_ambiguous_and_degrades():
    calls = []

    def tb(key):
        calls.append(key)
        return True  # says facet

    # A clear facet never reaches the tiebreaker.
    t.classify_tag("Data Tier", tiebreaker=tb)
    assert calls == []
    # An ambiguous name does reach it.
    klass, reason = t.classify_tag("Widget", tiebreaker=tb)
    assert calls == ["Widget"] and klass == "facet" and "tiebreaker" in reason

    # A raising tiebreaker degrades to the aboutness default (never blocks).
    def boom(key):
        raise RuntimeError("endpoint down")

    klass, _ = t.classify_tag("Widget", tiebreaker=boom)
    assert klass == "aboutness"
