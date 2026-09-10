"""Ontology Phase 4 Stage B (17h) — Context Pack resolver + web search + self-validation
+ the two read-only plug-points (spec §11). All offline; the resolver's egress + LLM are
injected as deterministic fakes. Guardrails proven here: DEFAULT OFF byte-identical, the
provenance ladder (T0/curated wins), degrade-not-hang, the firewall carve, and the
self-validation rails (unsourced-number drop, PII quarantine, confidence-gate)."""

from __future__ import annotations

import json

from genie_space_optimizer.ontology import (
    context_firewall,
    context_pack as cp,
    ddl,
    rank,
    web_search,
)
from genie_space_optimizer.ontology.context_pack import ContextPack

_AS_OF = "2026-09-10"


# ── Deterministic injected egress + synthesis ────────────────────────────────


def _search_fn(_query):
    return [
        web_search.WebResult(title="Northwind Foods — About", url="https://ex.com/about", snippet="grocery retail"),
        web_search.WebResult(title="Sector", url="https://ex.com/sector", snippet="consumer staples"),
    ]


_LLM_JSON = json.dumps({
    "industry": {"label": "Food Retail", "naics": "445110", "gics_sector": "Consumer Staples", "gate_confidence": 0.9},
    "canonical_domains": [
        {"name": "Revenue", "description": "net sales and orders", "source_url": "https://ex.com/rev"},
        {"name": "Loyalty", "description": "member programs", "source_url": "https://ex.com/loy"},
    ],
    "lexicon": [
        {"term": "GMV", "synonyms": ["gross merchandise value"], "synonym_class": "acronym", "source_url": "https://ex.com/gmv"},
    ],
    "competitors": [{"name": "Acme Grocers", "source_url": "https://ex.com/acme"}],
    "financial_context": [
        {"concept": "Revenue", "period": "FY24", "value": "$4.2B net sales", "source_url": "https://ex.com/10k"},
    ],
    "regulatory_notes": [
        {"regime": "FDA", "scope": "food safety labeling", "applies_to_hint": ["products"], "source_url": "https://ex.com/fda"},
    ],
})


def _llm_fn(_prompt):
    return _LLM_JSON


def _resolve(**over):
    kw = dict(company="Northwind Foods", enabled_providers=["web_search"],
              search_fn=_search_fn, llm_fn=_llm_fn, as_of=_AS_OF)
    kw.update(over)
    return cp.resolve_context_pack(**kw)


# ── Happy path: a full, validated pack ───────────────────────────────────────


def test_resolver_builds_a_validated_pack():
    pack = _resolve()
    assert pack is not None
    assert pack.company_key == "northwind foods"
    assert pack.status == "active" and pack.content_hash
    assert pack.industry["label"]["value"] == "Food Retail"
    assert pack.industry["gate_confidence"] == 0.9
    # Two canonical domains, sorted; each name is a Provenanced leaf with a source_url.
    names = [n for n, _leaf in pack.canonical_domain_names()]
    assert names == ["Loyalty", "Revenue"]
    for _name, leaf in pack.canonical_domain_names():
        assert leaf["tier"] == "T2" and leaf["source_url"]
    assert pack.lexicon and pack.financial_context and pack.regulatory_notes and pack.competitors
    assert pack.gap_check_suppressed is False  # high confidence ⇒ gap-check runs


def test_determinism_same_inputs_same_content_hash():
    a = _resolve()
    b = _resolve()
    assert a is not None and b is not None
    assert a.content_hash == b.content_hash  # generated_at differs; content_hash does not


# ── DEFAULT OFF / degrade-not-hang (MV-D43/D44) ──────────────────────────────


def test_no_providers_resolves_to_none():
    assert cp.resolve_context_pack(company="X", enabled_providers=[]) is None


def test_hipaa_baa_is_hard_off():
    assert _resolve(hipaa_baa=True) is None


def test_no_company_resolves_to_none():
    assert _resolve(company=None) is None


def test_all_providers_absent_degrades_to_none():
    # Every search yields nothing (the ladder degraded to estate-only) ⇒ no pack.
    assert _resolve(search_fn=lambda q: []) is None


def test_broken_search_never_raises():
    def _boom(_q):
        raise RuntimeError("gateway down")

    # A raising search degrades to estate-only, never propagates (MV-D43).
    assert _resolve(search_fn=_boom) is None


def test_no_llm_still_resolves_without_synthesis():
    # search hits but no synthesis ⇒ no sourced leaves survive ⇒ estate-only (None).
    assert _resolve(llm_fn=None) is None


# ── Self-validation rails (MV-D38) ───────────────────────────────────────────


def test_unsourced_number_is_dropped():
    bad = dict(json.loads(_LLM_JSON))
    bad["financial_context"] = [{"concept": "Revenue", "period": "FY24", "value": "$9.9B", "source_url": ""}]
    pack = _resolve(llm_fn=lambda p: json.dumps(bad))
    assert pack is not None
    # The unsourced figure was dropped; a sourced leaf would have survived.
    assert pack.financial_context == []


def test_pii_in_a_canonical_domain_name_is_quarantined():
    bad = dict(json.loads(_LLM_JSON))
    bad["canonical_domains"] = [
        {"name": "customer_ssn", "description": "d", "source_url": "https://ex.com/x"},
        {"name": "Revenue", "description": "d", "source_url": "https://ex.com/rev"},
    ]
    pack = _resolve(llm_fn=lambda p: json.dumps(bad))
    assert pack is not None
    names = [n for n, _l in pack.canonical_domain_names()]
    assert "customer_ssn" not in names and "Revenue" in names
    assert any(f["action"] == "blocked" for f in pack.pii_findings)


def test_low_confidence_suppresses_the_gap_check():
    bad = dict(json.loads(_LLM_JSON))
    bad["industry"] = {"label": "Food Retail", "gate_confidence": 0.2}
    pack = _resolve(llm_fn=lambda p: json.dumps(bad), gate_tau=0.5)
    assert pack is not None and pack.gap_check_suppressed is True
    # …and the gap-hypothesis plug-point emits nothing when suppressed.
    assert rank.context_gap_hypotheses(pack, surfaced_domain_names=[]) == []


# ── Firewall carve (positive guard, §7/§11) ──────────────────────────────────


def test_every_pack_section_targets_a_naming_family_target_never_forbidden():
    for section, target in cp.SECTION_TARGET.items():
        assert target not in context_firewall.FORBIDDEN_TARGETS, section
        # An external source may reach every pack-section target.
        assert context_firewall.influence_allows(cp._EXTERNAL_SHIM, target) is True, section


def test_external_source_can_never_reach_a_structural_writer():
    for forbidden in ("membership", "measure", "certification"):
        assert context_firewall.influence_allows(cp._EXTERNAL_SHIM, forbidden) is False


# ── Plug-point 1: provenance ladder in rank (T0/curated wins) ────────────────


def _pack_with_domains(*names):
    return ContextPack(
        pack_id="p", company_key="c", version=1, content_hash="h",
        generated_at="2026-09-10T00:00:00+00:00",
        canonical_domains=[
            {"name": {"value": n, "tier": "T2", "source_url": "https://ex.com/x",
                      "source_kind": "industry_model", "as_of": _AS_OF, "confidence": 0.6,
                      "decay_weight": 1.0}, "is_template": True}
            for n in names
        ],
    )


def _domain_row(domain_id, name, tag_decision):
    return {
        "domain_id": domain_id, "name": name, "tag_decision": tag_decision,
        "evidence": json.dumps({"rank": {"provenance_tier": "T0"}}),
    }


def test_context_prior_renames_a_create_cluster_but_never_a_curated_one():
    create = _domain_row("d1", "cat.revenue.orders group", "create")
    reuse = _domain_row("d2", "Finance", "reuse")
    members = {"d1": ["cat.revenue.orders"], "d2": ["cat.finance.gl"]}
    pack = _pack_with_domains("Revenue", "Finance")

    renamed = rank.apply_context_prior([create, reuse], pack, members_by_domain=members)

    # The create cluster took the business-language name (a weak default → improved).
    assert renamed == 1
    assert create["name"] == "Revenue"
    # The curated (reuse) governed-tag Domain kept its T0/curated name — the prior is
    # recorded as corroborating provenance but did NOT apply (T0/curated wins).
    assert reuse["name"] == "Finance"
    reuse_prior = json.loads(reuse["evidence"])["rank"]["naming_prior"]
    assert reuse_prior["applied"] is False and reuse_prior["outranked_by"] == "curated"
    create_prior = json.loads(create["evidence"])["rank"]["naming_prior"]
    assert create_prior["applied"] is True and create_prior["tier"] == "T2"


def test_context_prior_none_pack_is_a_noop():
    row = _domain_row("d1", "orig", "create")
    assert rank.apply_context_prior([row], None, members_by_domain={"d1": ["c.s.t"]}) == 0
    assert row["name"] == "orig" and json.loads(row["evidence"]) == {"rank": {"provenance_tier": "T0"}}


def test_gap_hypotheses_rank_below_graph_and_skip_covered():
    pack = _pack_with_domains("Revenue", "Loyalty")
    gaps = rank.context_gap_hypotheses(pack, surfaced_domain_names=["Revenue Domain"])
    # Revenue is covered by a surfaced domain; only Loyalty is a gap.
    assert [g["name"] for g in gaps] == ["Loyalty"]
    assert all(g["ranks_below_graph_proposals"] and g["surfaced"] is False for g in gaps)


# ── Plug-point 2: Page Recent-context overlay (certify-no) ───────────────────


def test_recent_context_overlay_is_labeled_dated_certify_no():
    pack = _resolve()
    pages = [
        {"page_id": "pg1", "title": "Revenue Guardrail", "synonyms": [], "certify": False, "evidence": "{}"},
        {"page_id": "pg2", "title": "Unrelated Taxonomy", "synonyms": [], "certify": True, "evidence": "{}"},
    ]
    n = cp.apply_recent_context(pages, pack)
    assert n == 1
    ev = json.loads(pages[0]["evidence"])
    rc = ev["recent_context"]
    assert rc["certify"] is False  # certify-no (MV-D28)
    assert rc["as_of"] and rc["items"] and "public sources" in rc["disclaimer"].lower()
    # The overlay never flips page.certify and never touches a non-matching page.
    assert pages[0]["certify"] is False
    assert "recent_context" not in json.loads(pages[1]["evidence"])


def test_recent_context_none_pack_is_a_noop():
    pages = [{"page_id": "pg1", "title": "Revenue", "synonyms": [], "certify": False, "evidence": "{}"}]
    assert cp.apply_recent_context(pages, None) == 0
    assert pages[0]["evidence"] == "{}"


# ── Persistence rows (additive DDL, MV-D49) ──────────────────────────────────


def test_pack_rows_shape_and_citation_index():
    pack = _resolve()
    built = cp.pack_rows(pack, metastore_id="ms1", workspace_id="ws1", run_id="r1", as_of=_AS_OF)
    assert len(built["context_pack_rows"]) == 1
    row = built["context_pack_rows"][0]
    assert row["metastore_id"] == "ms1" and row["company_key"] == "northwind foods"
    assert row["content_hash"] == pack.content_hash and row["status"] == "active"
    assert row["industry_code"] == "445110"
    assert json.loads(row["pack_json"])["pack_id"] == pack.pack_id
    # Every citation row carries a real source_url + tier (the egress audit index).
    src = built["context_source_rows"]
    assert src, "a validated pack has sourced leaves"
    for s in src:
        assert s["source_url"] and s["tier"] in ("T0", "T1", "T2", "T3")
        assert s["pack_id"] == pack.pack_id and s["metastore_id"] == "ms1"
    # Deterministic ordering.
    assert src == sorted(src, key=lambda r: (r["field_path"], r["source_url"]))


# ── web_search ladder + degrade (MV-D46) ─────────────────────────────────────


def _payload(url):
    return {"result": {"content": [{"type": "text", "text": json.dumps([{"title": "T", "url": url, "snippet": "s"}])}]}}


def test_web_search_hits_primary_provider():
    calls = []

    def _transport(path, body):
        calls.append(path)
        return _payload("https://ex.com/hit")

    out = web_search.web_search("q", enabled_providers=["web_search"], transport=_transport)
    assert [r.url for r in out] == ["https://ex.com/hit"]
    assert "system.ai.web_search" in calls[0]


def test_web_search_falls_back_past_a_broken_rung():
    def _transport(path, body):
        if "system.ai.web_search" in path:
            raise RuntimeError("primary down")
        return _payload("https://ex.com/youcom")

    out = web_search.web_search("q", enabled_providers=["web_search", "youcom"], transport=_transport)
    assert [r.url for r in out] == ["https://ex.com/youcom"]


def test_web_search_all_absent_returns_empty_never_raises():
    def _transport(path, body):
        raise RuntimeError("all down")

    assert web_search.web_search("q", enabled_providers=["web_search", "youcom"], transport=_transport) == []
    # No provider enabled / empty query / hipaa ⇒ empty, no transport call.
    assert web_search.web_search("q", enabled_providers=[], transport=_transport) == []
    assert web_search.web_search("", enabled_providers=["web_search"], transport=_transport) == []
    assert web_search.web_search("q", enabled_providers=["web_search"], transport=_transport, hipaa_baa=True) == []


def test_web_search_drops_a_result_without_a_url():
    def _transport(path, body):
        return {"result": {"content": [{"type": "text", "text": json.dumps([{"title": "T", "url": "", "snippet": "s"}])}]}}

    assert web_search.web_search("q", enabled_providers=["web_search"], transport=_transport) == []


# ── DDL: additive context tables (MV-D49) ────────────────────────────────────


def test_context_tables_are_additive_and_metastore_keyed():
    assert ddl.CONTEXT_TABLES == ("genie_ont_context_pack", "genie_ont_context_sources")
    rendered = ddl.all_ddl("cat", "sch")
    for t in ddl.CONTEXT_TABLES:
        assert t in rendered
        assert rendered[t].startswith(f"CREATE TABLE IF NOT EXISTS cat.sch.{t}")
        assert "delta.enableChangeDataFeed" in rendered[t]
    assert ddl.CONTEXT_PACK_KEYS[0] == "metastore_id" and ddl.CONTEXT_SOURCE_KEYS[0] == "metastore_id"
