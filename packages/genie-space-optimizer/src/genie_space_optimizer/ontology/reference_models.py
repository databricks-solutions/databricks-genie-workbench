"""Bundled Vibe industry reference models (MV-D58, §9) — the T2 industry-canonical
vocabulary + structure that :mod:`~genie_space_optimizer.ontology.alignment` matches
discovered domains against.

**Why bundled Python data, not fetched JSON.** The real models live in the external
``lakehouse-industry-data-models`` repo, but this build must be deterministic, offline,
and add **no new dependency** (MV-D45) and perform **no network egress** here (§9 is
read-only). So a small curated seed set ships as plain Python dicts and the loader
takes an **injectable** ``loader`` (see :func:`alignment.load_reference_model`) so the job
can point at the real repo/volume later without touching this module. The bundled models
are enough for the offline slice and the airline deploy-verify estate.

Each model is a **T2 industry-canonical** reference: vocabulary (``name`` + ``synonyms`` +
``seed_anchors``) and structure (``neighbors`` — the reference FK/join adjacency the
structural-propagation pass walks). It is NEVER a structural writer — membership always
comes from the graph (MV-D58 "align, don't conform").

A model dict is: ``{model_id, label, source_url, as_of, tier, domains:[{name, synonyms,
description, seed_anchors, neighbors}]}``. The loader normalizes an id / alias / NAICS code
to a bundled model; an unknown id degrades to ``None`` (alignment then no-ops, MV-D43).
"""

from __future__ import annotations

from typing import Any

# The provenance source for every bundled leaf — the canonical industry-model repo.
_SOURCE_URL = "https://github.com/databricks-industry-solutions/industry-data-models"
_AS_OF = "2026-09-15"


# ── Airline / passenger aviation (the deploy-verify estate) ──────────────────
AIRLINE: dict[str, Any] = {
    "model_id": "airline",
    "label": "Airline / Passenger Aviation",
    "source_url": _SOURCE_URL,
    "as_of": _AS_OF,
    "tier": "T2",
    "domains": [
        {
            "name": "Revenue",
            "synonyms": ["ticket revenue", "fare revenue", "sales", "yield", "revenue management"],
            "description": "Fares, ancillary sales, yield and revenue-accounting facts.",
            "seed_anchors": ["revenue", "fare", "ticket price", "yield", "booking value", "ancillary sales"],
            "neighbors": ["Reservation", "Passenger", "Route"],
        },
        {
            "name": "Reservation",
            "synonyms": ["booking", "reservations", "pnr", "itinerary"],
            "description": "Bookings, itineraries and passenger name records (PNR).",
            "seed_anchors": ["reservation", "booking", "pnr", "itinerary", "seat assignment"],
            "neighbors": ["Passenger", "Revenue", "Route"],
        },
        {
            "name": "Passenger",
            "synonyms": ["customer", "traveller", "guest", "flyer"],
            "description": "Passenger profiles, contact and travel-document data.",
            "seed_anchors": ["passenger", "customer profile", "traveller", "frequent flyer"],
            "neighbors": ["Reservation", "Loyalty", "Revenue"],
        },
        {
            "name": "Loyalty",
            "synonyms": ["mileage plan", "frequent flyer program", "rewards", "miles"],
            "description": "Frequent-flyer program, miles balances, tiers and redemptions.",
            "seed_anchors": ["loyalty", "mileage", "frequent flyer", "rewards points", "elite tier"],
            "neighbors": ["Passenger", "Revenue"],
        },
        {
            "name": "Route",
            "synonyms": ["network", "schedule", "flight schedule", "routes", "origin destination"],
            "description": "Flight schedules, routes, and origin-destination network.",
            "seed_anchors": ["route", "flight schedule", "network", "origin destination", "leg"],
            "neighbors": ["Fleet", "Revenue", "Reservation"],
        },
        {
            "name": "Fleet",
            "synonyms": ["aircraft", "airplane", "tail", "equipment"],
            "description": "Aircraft, tail numbers, seating configuration and equipment.",
            "seed_anchors": ["fleet", "aircraft", "tail number", "seat configuration", "equipment type"],
            "neighbors": ["Maintenance", "Route"],
        },
        {
            "name": "Maintenance",
            "synonyms": ["mro", "engineering", "aircraft maintenance", "work orders"],
            "description": "Maintenance, repair and overhaul (MRO): work orders, parts, checks.",
            "seed_anchors": ["maintenance", "mro", "work order", "aircraft check", "spare parts"],
            "neighbors": ["Fleet"],
        },
        # ── Reference domains the airline estate typically LACKS → gap hypotheses ──
        {
            "name": "Cargo",
            "synonyms": ["freight", "air cargo", "belly cargo"],
            "description": "Air-cargo shipments, air waybills and freight capacity.",
            "seed_anchors": ["cargo", "freight", "air waybill", "shipment"],
            "neighbors": ["Route", "Revenue"],
        },
        {
            "name": "Crew Scheduling",
            "synonyms": ["crew", "pilots", "flight attendants", "rostering"],
            "description": "Crew rosters, duty limits, pairings and qualifications.",
            "seed_anchors": ["crew scheduling", "pilot roster", "cabin crew", "duty time", "pairing"],
            "neighbors": ["Route", "Fleet"],
        },
        {
            "name": "In-flight Catering",
            "synonyms": ["catering", "onboard meals", "galley"],
            "description": "Onboard meal planning, galley loading and catering suppliers.",
            "seed_anchors": ["catering", "onboard meal", "galley", "menu"],
            "neighbors": ["Route", "Passenger"],
        },
        {
            "name": "Ground Operations",
            "synonyms": ["ground handling", "airport operations", "turnaround", "baggage"],
            "description": "Airport turnaround, ground handling, gates and baggage handling.",
            "seed_anchors": ["ground operations", "turnaround", "baggage handling", "gate", "ramp"],
            "neighbors": ["Route", "Fleet"],
        },
    ],
}


# ── Retail / consumer (a second bundled model, proves the loader generalizes) ─
RETAIL: dict[str, Any] = {
    "model_id": "retail",
    "label": "Retail / Consumer Goods",
    "source_url": _SOURCE_URL,
    "as_of": _AS_OF,
    "tier": "T2",
    "domains": [
        {
            "name": "Sales",
            "synonyms": ["orders", "transactions", "point of sale", "revenue"],
            "description": "Orders, transactions and point-of-sale revenue.",
            "seed_anchors": ["sales", "order", "transaction", "point of sale", "revenue"],
            "neighbors": ["Customer", "Product", "Inventory"],
        },
        {
            "name": "Customer",
            "synonyms": ["shopper", "member", "consumer", "account"],
            "description": "Customer profiles, segments and contact data.",
            "seed_anchors": ["customer", "shopper", "member", "segment"],
            "neighbors": ["Sales", "Loyalty", "Marketing"],
        },
        {
            "name": "Product",
            "synonyms": ["catalog", "sku", "merchandise", "item"],
            "description": "Product catalog, SKUs, categories and pricing.",
            "seed_anchors": ["product", "sku", "catalog", "merchandise", "category"],
            "neighbors": ["Inventory", "Sales", "Supply Chain"],
        },
        {
            "name": "Inventory",
            "synonyms": ["stock", "warehouse", "fulfillment"],
            "description": "On-hand stock, warehouses and fulfillment.",
            "seed_anchors": ["inventory", "stock", "warehouse", "on hand", "fulfillment"],
            "neighbors": ["Product", "Supply Chain", "Sales"],
        },
        {
            "name": "Loyalty",
            "synonyms": ["rewards", "points", "membership program"],
            "description": "Loyalty program, points balances and redemptions.",
            "seed_anchors": ["loyalty", "rewards", "points", "membership"],
            "neighbors": ["Customer", "Marketing"],
        },
        {
            "name": "Marketing",
            "synonyms": ["campaigns", "promotions", "advertising"],
            "description": "Campaigns, promotions and channel attribution.",
            "seed_anchors": ["marketing", "campaign", "promotion", "advertising"],
            "neighbors": ["Customer", "Sales"],
        },
        {
            "name": "Supply Chain",
            "synonyms": ["procurement", "suppliers", "logistics", "vendors"],
            "description": "Procurement, suppliers and inbound logistics.",
            "seed_anchors": ["supply chain", "procurement", "supplier", "logistics", "vendor"],
            "neighbors": ["Product", "Inventory"],
        },
    ],
}


# The bundled registry, keyed by ``model_id``.
_BUNDLED: dict[str, dict[str, Any]] = {AIRLINE["model_id"]: AIRLINE, RETAIL["model_id"]: RETAIL}

# Aliases / NAICS-GICS hints → a bundled ``model_id``. Lets the resolved industry (a code
# or a label) pick a model without an exact id. Deterministic; extend as models are added.
_ALIASES: dict[str, str] = {
    "airline": "airline",
    "airlines": "airline",
    "aviation": "airline",
    "air transportation": "airline",
    "passenger aviation": "airline",
    "481": "airline",       # NAICS Air Transportation
    "4811": "airline",      # NAICS Scheduled Air Transportation
    "retail": "retail",
    "retail trade": "retail",
    "consumer": "retail",
    "consumer goods": "retail",
    "ecommerce": "retail",
    "e-commerce": "retail",
    "44": "retail",         # NAICS Retail Trade
    "45": "retail",
}


def _norm(reference_model: str | None) -> str:
    """Normalize a reference-model id / alias to a lowercase lookup key."""
    return " ".join(str(reference_model or "").strip().lower().split())


def resolve_model_id(reference_model: str | None) -> str | None:
    """Map a ``reference_model`` string (id, alias, or NAICS code) to a bundled model id,
    or ``None`` when nothing matches (alignment then no-ops, MV-D43)."""
    key = _norm(reference_model)
    if not key:
        return None
    if key in _BUNDLED:
        return key
    return _ALIASES.get(key)


def bundled_model(reference_model: str | None) -> dict[str, Any] | None:
    """The bundled model dict for a ``reference_model`` (id/alias/NAICS), else ``None``.

    This is the DEFAULT loader used by :func:`alignment.load_reference_model` when the job
    injects none. A returned dict is a fresh shallow copy of the module constant so a caller
    cannot mutate the registry."""
    mid = resolve_model_id(reference_model)
    if mid is None:
        return None
    model = _BUNDLED.get(mid)
    return dict(model) if model else None


__all__ = ["AIRLINE", "RETAIL", "bundled_model", "resolve_model_id"]
