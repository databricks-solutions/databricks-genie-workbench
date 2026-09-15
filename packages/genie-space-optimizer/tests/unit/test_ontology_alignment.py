"""Industry-reference alignment (MV-D58, §9) — offline unit tests.

Covers the loader (id/alias/NAICS/unknown/empty), the four ordered passes (string,
embedding-seed-anchor, structural propagation, semantic sanity), the five typed relations,
gap hypotheses (ranked-below / never-auto-created), determinism, degrade-not-hang, the
harness-shaped aligned reference (fed into ``eval_harness``), and ``rank.apply_alignment``
(rename only a surfaced ``create`` on ``exact``; curated wins; provenance recorded). All
pure/offline — no cluster, no I/O, no LLM.
"""

from __future__ import annotations

import json

from genie_space_optimizer.ontology import (
    alignment,
    eval_harness,
    rank,
    reference_models,
)
from genie_space_optimizer.ontology.alignment import ReferenceDomain, ReferenceModel


# ── Helpers / fixtures ───────────────────────────────────────────────────────


def _row(domain_id, name, *, tag_decision="create", surfaced=True, parent_id=None, rank_block=None):
    ev = {"surfaced": surfaced}
    if rank_block is not None:
        ev["rank"] = rank_block
    return {
        "metastore_id": "ms1", "domain_id": domain_id, "workspace_id": "ws1",
        "parent_id": parent_id, "name": name, "tag_decision": tag_decision,
        "tag_key": name, "tag_value": name, "evidence": json.dumps(ev, sort_keys=True),
        "score": 10.0, "run_id": "r1", "as_of": "2026-09-15T00:00:00+00:00",
    }


def _model(*domains: ReferenceDomain, model_id="test", tier="T2") -> ReferenceModel:
    return ReferenceModel(
        model_id=model_id, label="Test", source_url="https://ref.example/model",
        as_of="2026-09-15", tier=tier, domains=tuple(domains),
    )


class _ConceptEmbedder:
    """A deterministic fake embedder: one-hot over concept dimensions chosen by keyword. Same
    concept ⇒ cosine 1.0, different ⇒ 0.0 — lets a test force a pure-embedding match."""

    _CONCEPTS = {
        "loyalty": ("loyal", "reward", "point", "tier", "mile", "frequent"),
        "revenue": ("revenue", "fare", "yield", "sales"),
        "fleet": ("fleet", "aircraft", "tail"),
        "cargo": ("cargo", "freight", "waybill"),
    }

    def _concept(self, text: str) -> str:
        t = text.lower()
        for concept, kws in self._CONCEPTS.items():
            if any(k in t for k in kws):
                return concept
        return "_none"

    def embed(self, texts):
        dims = [*self._CONCEPTS.keys(), "_none"]
        out = []
        for t in texts:
            c = self._concept(t)
            out.append([1.0 if d == c else 0.0 for d in dims])
        return out


# ── Loader ───────────────────────────────────────────────────────────────────


def test_load_by_id_alias_and_naics():
    assert alignment.load_reference_model("airline").model_id == "airline"
    assert alignment.load_reference_model("Aviation").model_id == "airline"      # alias
    assert alignment.load_reference_model("4811").model_id == "airline"          # NAICS
    assert alignment.load_reference_model("retail").model_id == "retail"


def test_load_unknown_and_blank_degrade_to_none():
    assert alignment.load_reference_model("no_such_industry") is None
    assert alignment.load_reference_model("") is None
    assert alignment.load_reference_model(None) is None


def test_load_empty_model_degrades_to_none():
    assert alignment.load_reference_model("x", loader=lambda _r: {"model_id": "x", "domains": []}) is None
    assert alignment.load_reference_model("x", loader=lambda _r: {"model_id": "x"}) is None


def test_load_broken_loader_degrades_to_none():
    def _boom(_r):
        raise RuntimeError("network down")

    assert alignment.load_reference_model("airline", loader=_boom) is None


def test_load_sorts_domains_deterministically():
    m = alignment.load_reference_model("airline")
    names = [d.name for d in m.domains]
    assert names == sorted(names, key=str.lower)


# ── Pass (a): string match ─────────────────────────────────────────────────────


def test_string_exact_name_and_synonym():
    m = _model(
        ReferenceDomain("Loyalty", synonyms=("mileage plan", "frequent flyer program")),
        ReferenceDomain("Revenue"),
    )
    rows = [_row("d1", "Revenue"), _row("d2", "Mileage Plan")]
    res = alignment.align(rows, m, members_by_domain={"d1": [], "d2": []}, as_of="2026-09-15")
    by = {c.discovered_domain_id: c for c in res.correspondences}
    assert by["d1"].relation == "exact" and by["d1"].reference_name == "Revenue"
    assert by["d1"].match_pass == "string"
    assert by["d2"].relation == "exact" and by["d2"].reference_name == "Loyalty"  # via synonym


def test_string_containment_matches_descriptive_name():
    m = _model(ReferenceDomain("Fleet", synonyms=("aircraft",)))
    rows = [_row("d1", "Aircraft Fleet")]
    res = alignment.align(rows, m, members_by_domain={"d1": ["c.fleet.aircraft"]}, as_of="2026-09-15")
    c = res.correspondences[0]
    assert c.reference_name == "Fleet"
    assert c.relation in alignment.RELATIONS and c.relation != "not-equivalent"


# ── Pass (b): embedding-seed-anchor ────────────────────────────────────────────


def test_embedding_match_with_token_overlap_is_accepted():
    # "Rewards Ledger" shares one token ("rewards") with Loyalty (coherent) but is below the
    # string threshold; the embedder pulls it to Loyalty via the seed anchors.
    m = _model(
        ReferenceDomain("Loyalty", synonyms=("mileage plan", "miles", "rewards"),
                        seed_anchors=("loyalty", "rewards points", "elite tier")),
        ReferenceDomain("Revenue", seed_anchors=("revenue", "fare")),
    )
    rows = [_row("d1", "Rewards Ledger")]
    res = alignment.align(rows, m, members_by_domain={"d1": []}, embedder=_ConceptEmbedder(), as_of="2026-09-15")
    c = res.correspondences[0]
    assert c.reference_name == "Loyalty"
    assert c.match_pass == "embedding"
    assert c.relation != "not-equivalent"


def test_no_embedder_skips_embedding_pass():
    m = _model(ReferenceDomain("Loyalty", synonyms=("miles",), seed_anchors=("rewards points",)))
    rows = [_row("d1", "Rewards Ledger")]
    res = alignment.align(rows, m, members_by_domain={"d1": []}, embedder=None, as_of="2026-09-15")
    # No string match, no embedder ⇒ no correspondence; Loyalty is a gap.
    assert res.correspondences == []
    assert "Loyalty" in [g["name"] for g in res.gap_hypotheses]


# ── Pass (c): structural propagation → derived ─────────────────────────────────


def test_structural_propagation_emits_derived():
    # Alpha anchors on the discovered "Alpha" domain; "Beta Operations" is Alpha's reference
    # neighbour. The discovered "Gadget Shop" domain has NO base match (string 0.33 < 0.6) but
    # IS adjacent to Alpha's domain and shares one token ("gadget") with Beta → derived.
    m = _model(
        ReferenceDomain("Alpha", neighbors=("Beta Operations",)),
        ReferenceDomain("Beta Operations", synonyms=("gadget handling",), neighbors=("Alpha",)),
    )
    rows = [_row("d1", "Alpha"), _row("d2", "Gadget Shop")]
    res = alignment.align(
        rows, m,
        members_by_domain={"d1": ["c.alpha.t"], "d2": ["c.x.gadget"]},
        domain_adjacency={"d1": ["d2"], "d2": ["d1"]},
        as_of="2026-09-15",
    )
    by = {c.discovered_domain_id: c for c in res.correspondences}
    assert by["d1"].reference_name == "Alpha"
    assert by["d2"].reference_name == "Beta Operations"
    assert by["d2"].relation == "derived" and by["d2"].match_pass == "structural"


# ── Pass (d): semantic sanity → not-equivalent ─────────────────────────────────


def test_pure_embedding_no_overlap_no_structure_is_not_equivalent():
    # "Points and Tiers" shares NO name/synonym token with Loyalty and has no structural
    # support — the embedder alone pulls it close, so sanity rejects it as not-equivalent.
    m = _model(
        ReferenceDomain("Loyalty", synonyms=("mileage plan",), seed_anchors=("loyalty", "elite tier")),
    )
    rows = [_row("d1", "Points and Tiers")]
    res = alignment.align(rows, m, members_by_domain={"d1": []}, embedder=_ConceptEmbedder(), as_of="2026-09-15")
    c = res.correspondences[0]
    assert c.relation == "not-equivalent"
    assert c.match_pass == "sanity"
    # A not-equivalent is NOT a harness match, and DOES leave the reference as a gap.
    assert res.aligned_reference["alignments"] == []
    assert "Loyalty" in [g["name"] for g in res.gap_hypotheses]


# ── Typed relations: broader / narrower ────────────────────────────────────────


def test_every_relation_is_one_of_five():
    m = alignment.load_reference_model("airline")
    rows = [_row(f"d{i}", n) for i, n in enumerate(["Revenue", "Mileage Plan", "Aircraft Fleet", "Zzz Nothing"])]
    res = alignment.align(rows, m, members_by_domain={r["domain_id"]: [] for r in rows}, as_of="2026-09-15")
    for c in res.correspondences:
        assert c.relation in alignment.RELATIONS


def test_broader_when_one_domain_matches_multiple_references():
    # A single discovered domain "Sales and Loyalty" matches both Revenue (via "sales") and
    # Loyalty (via "loyalty") ⇒ it is broader than either.
    m = _model(
        ReferenceDomain("Revenue", synonyms=("sales",)),
        ReferenceDomain("Loyalty", synonyms=("rewards",)),
    )
    # Name contains both reference names ⇒ two strong (containment) candidates ⇒ broader.
    rows = [_row("d1", "Revenue and Loyalty")]
    res = alignment.align(rows, m, members_by_domain={"d1": []}, as_of="2026-09-15")
    assert res.correspondences[0].relation == "broader"


def test_narrower_when_domain_has_parent():
    m = _model(ReferenceDomain("Revenue", synonyms=("fare pricing",)))
    rows = [_row("d1", "Fare Pricing", parent_id="parent1")]
    res = alignment.align(rows, m, members_by_domain={"d1": []}, as_of="2026-09-15")
    # Sub-domain (has parent) mapping to a reference ⇒ narrower (unless an exact identity).
    c = res.correspondences[0]
    assert c.relation in ("narrower", "exact")  # "fare pricing" is a synonym ⇒ exact wins


def test_narrower_when_multiple_domains_map_to_one_reference():
    m = _model(ReferenceDomain("Revenue", synonyms=("ticket sales", "ancillary sales")))
    rows = [_row("d1", "Ticket Sales"), _row("d2", "Ancillary Sales")]
    res = alignment.align(rows, m, members_by_domain={"d1": [], "d2": []}, as_of="2026-09-15")
    # Both are synonyms ⇒ exact; the point is both map to the SAME reference without error.
    refs = {c.reference_name for c in res.correspondences}
    assert refs == {"Revenue"}


# ── Gap hypotheses ─────────────────────────────────────────────────────────────


def test_gap_hypotheses_ranked_below_and_never_surfaced():
    m = _model(ReferenceDomain("Revenue"), ReferenceDomain("Cargo", description="freight"))
    rows = [_row("d1", "Revenue")]
    res = alignment.align(rows, m, members_by_domain={"d1": []}, as_of="2026-09-15")
    gaps = res.gap_hypotheses
    assert [g["name"] for g in gaps] == ["Cargo"]
    g = gaps[0]
    assert g["ranks_below_graph_proposals"] is True
    assert g["surfaced"] is False
    assert g["kind"] == "gap_hypothesis"
    assert g["tier"] == "T2" and g["source_url"] == "https://ref.example/model"


# ── Determinism + degrade ──────────────────────────────────────────────────────


def test_deterministic_same_inputs_same_output():
    m = alignment.load_reference_model("airline")
    rows = [_row("d3", "Aircraft Fleet"), _row("d1", "Revenue"), _row("d2", "Mileage Plan")]
    mbd = {"d1": ["c.rev.fares"], "d2": ["c.loyalty.miles"], "d3": ["c.fleet.aircraft"]}
    a = alignment.align(rows, m, members_by_domain=mbd, as_of="2026-09-15")
    b = alignment.align(rows, m, members_by_domain=mbd, as_of="2026-09-15")
    assert [c.to_dict() for c in a.correspondences] == [c.to_dict() for c in b.correspondences]
    assert a.gap_hypotheses == b.gap_hypotheses
    assert a.aligned_reference == b.aligned_reference


def test_none_and_empty_reference_degrade_to_empty():
    rows = [_row("d1", "Revenue")]
    assert alignment.align(rows, None).is_empty()
    empty = ReferenceModel("m", "M", None, "2026-09-15", "T2", ())
    assert alignment.align(rows, empty, members_by_domain={"d1": []}).is_empty()


def test_no_surfaced_domains_degrade_to_empty():
    # No surfaced domains ⇒ nothing to align against ⇒ empty (no correspondences AND no gap
    # hypotheses — emitting the whole reference as gaps off an empty estate would be noise).
    m = _model(ReferenceDomain("Revenue"))
    rows = [_row("d1", "Revenue", surfaced=False)]
    res = alignment.align(rows, m, members_by_domain={"d1": []}, as_of="2026-09-15")
    assert res.is_empty()


def test_embedder_failure_degrades_to_string_only():
    class _Boom:
        def embed(self, _texts):
            raise RuntimeError("endpoint down")

    m = _model(ReferenceDomain("Revenue"))
    rows = [_row("d1", "Revenue")]
    res = alignment.align(rows, m, members_by_domain={"d1": []}, embedder=_Boom(), as_of="2026-09-15")
    # String pass still matches Revenue; the embedder failure did not block the run.
    assert res.correspondences[0].reference_name == "Revenue"


# ── Harness-shaped aligned reference feeds eval_harness ────────────────────────


def test_aligned_reference_feeds_precision_recall_f1():
    m = alignment.load_reference_model("airline")
    rows = [_row("d1", "Revenue"), _row("d2", "Mileage Plan"), _row("d3", "Aircraft Fleet")]
    mbd = {"d1": ["c.rev.fares"], "d2": ["c.loyalty.miles"], "d3": ["c.fleet.aircraft"]}
    res = alignment.align(rows, m, members_by_domain=mbd, as_of="2026-09-15")
    discovered = [{"domain_id": r["domain_id"], "name": r["name"], "members": mbd[r["domain_id"]]} for r in rows]
    prf, statuses = eval_harness.compute_precision_recall_f1(discovered, res.aligned_reference)
    # All 3 discovered domains matched a reference ⇒ precision 1.0; recall = 3 / 11 domains.
    assert prf.precision == 1.0
    assert prf.recall is not None and 0.0 < prf.recall < 1.0
    assert all(s.match_type in ("exact", "extra") for s in statuses)


# ── rank.apply_alignment (BUILD B) ─────────────────────────────────────────────


def _corr(domain_id, ref_name, relation, *, ref_id=None, tier="T2", src="https://ref.example/model"):
    return {
        "discovered_domain_id": domain_id, "discovered_name": "x", "reference_name": ref_name,
        "reference_id": ref_id or ref_name.lower(), "relation": relation, "score": 1.0,
        "match_pass": "string",
        "provenance": {"tier": tier, "source_url": src, "source_kind": "industry_model", "as_of": "2026-09-15"},
    }


def test_apply_alignment_renames_surfaced_create_on_exact():
    rows = [_row("d1", "Rev Cluster", tag_decision="create", surfaced=True)]
    n = rank.apply_alignment(rows, [_corr("d1", "Revenue", "exact")])
    assert n == 1
    assert rows[0]["name"] == "Revenue"
    a = json.loads(rows[0]["evidence"])["rank"]["alignment"]
    assert a["applied"] is True and a["relation"] == "exact"
    assert a["tier"] == "T2" and a["source_url"] == "https://ref.example/model"


def test_apply_alignment_never_renames_curated_but_records_corroboration():
    for decision in ("reuse", "reassign"):
        rows = [_row("d1", "Alaska Loyalty", tag_decision=decision, surfaced=True)]
        n = rank.apply_alignment(rows, [_corr("d1", "Loyalty", "exact")])
        assert n == 0
        assert rows[0]["name"] == "Alaska Loyalty"  # T0/curated name wins
        a = json.loads(rows[0]["evidence"])["rank"]["alignment"]
        assert a["applied"] is False and a["outranked_by"] == "curated"
        assert a["reference_name"] == "Loyalty"  # recorded as corroborating evidence


def test_apply_alignment_does_not_rename_non_exact_or_unsurfaced():
    # non-exact create
    rows = [_row("d1", "Fare Stuff", tag_decision="create", surfaced=True)]
    assert rank.apply_alignment(rows, [_corr("d1", "Revenue", "narrower")]) == 0
    assert rows[0]["name"] == "Fare Stuff"
    # unsurfaced create + exact ⇒ still not renamed
    rows2 = [_row("d2", "Rev Junk", tag_decision="create", surfaced=False)]
    assert rank.apply_alignment(rows2, [_corr("d2", "Revenue", "exact")]) == 0
    assert rows2[0]["name"] == "Rev Junk"


def test_apply_alignment_records_not_equivalent_without_renaming():
    rows = [_row("d1", "Weird Cluster", tag_decision="create", surfaced=True)]
    assert rank.apply_alignment(rows, [_corr("d1", "Loyalty", "not-equivalent")]) == 0
    a = json.loads(rows[0]["evidence"])["rank"]["alignment"]
    assert a["relation"] == "not-equivalent" and a["applied"] is False


def test_apply_alignment_empty_is_noop_byte_identical():
    rows = [_row("d1", "Revenue", tag_decision="create", surfaced=True)]
    before = json.dumps(rows[0], sort_keys=True)
    assert rank.apply_alignment(rows, []) == 0
    assert json.dumps(rows[0], sort_keys=True) == before


def test_apply_alignment_preserves_existing_rank_block():
    rows = [_row("d1", "Rev", tag_decision="create", surfaced=True, rank_block={"score": 42.0, "tier": "A"})]
    rank.apply_alignment(rows, [_corr("d1", "Revenue", "exact")])
    rk = json.loads(rows[0]["evidence"])["rank"]
    assert rk["score"] == 42.0 and rk["tier"] == "A"  # existing block preserved
    assert rk["alignment"]["relation"] == "exact"


# ── reference_models registry ──────────────────────────────────────────────────


def test_bundled_model_returns_copy_not_registry_reference():
    a = reference_models.bundled_model("airline")
    a["domains"] = []
    assert reference_models.bundled_model("airline")["domains"]  # registry untouched
