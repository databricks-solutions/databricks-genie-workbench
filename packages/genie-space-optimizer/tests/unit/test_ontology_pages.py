"""L5 Page-miner engine — offline unit tests (Phase 3c §11).

Detectors are deterministic and run in-process; the drafting LLM and the ask_genie
routing validator are injected fakes; no cluster, no Lakebase Search, no live Genie.

Covers: per-archetype detectors on the architecture worked example, canonical-concept
keying (cross-sub-domain collapse; page_id from canonical_id), the estate
corroboration gate, the identifier gate, the four retrieval gates, contradiction →
CONFLICT (read-only), the certify rule, drafting/routing degrade, and
determinism/idempotency.
"""

from __future__ import annotations

from genie_space_optimizer.ontology import pages
from genie_space_optimizer.ontology.pages import ColumnSignal, MeasureSignal


# ── Fakes ────────────────────────────────────────────────────────────────────


def _good_drafter(facts: dict) -> str:
    """A drafter that writes a gate-passing body from the spec facts (real backticked
    identifiers), standing in for a healthy LLM."""
    lines = [f"Description: {facts['description']}", "", "Definition:", f"  {facts['definition']}"]
    if facts["rules"]:
        lines.append("")
        lines.append("Rules:")
        lines.extend(f"  - {r}" for r in facts["rules"])
    return "\n".join(lines)


def _boom_drafter(facts: dict) -> str:
    raise RuntimeError("serving endpoint unreachable")


def _invented_drafter(facts: dict) -> str:
    return "Description: x\n\nDefinition:\n  Use `finance.sales.ghost_column` to answer."


# ── Fixtures (the architecture worked example) ───────────────────────────────

_ORDER_MV = "finance.sales.order_revenue"
_SRC = ("finance.sales.orders", "finance.sales.order_items")
_SALES_AGENT = "Sales · 01ef9a2b3c4d5e6f"


def _total_revenue() -> MeasureSignal:
    return MeasureSignal(
        mv_fqn=_ORDER_MV, name="total_revenue",
        expression="SUM(order_items.quantity * order_items.unit_price)",
        comment="TR; net sales; revenue booked", source_fqns=_SRC,
        agent_fqns=(_SALES_AGENT,), domain_id="sug_sales",
    )


def _discount_rate() -> MeasureSignal:
    return MeasureSignal(
        mv_fqn=_ORDER_MV, name="discount_rate",
        expression="SUM(order_items.discount_amt) / SUM(order_items.gross_amt)",
        fmt="0.00%", comment="disc rate; DR; how much we discounted", source_fqns=_SRC,
        agent_fqns=(_SALES_AGENT,), domain_id="sug_sales",
    )


def _revenue_conflict() -> list[MeasureSignal]:
    return [
        MeasureSignal(mv_fqn="finance.sales.agent_a_rev", name="revenue",
                      expression="SUM(net_amount)", comment="ARR; net rev; how much we made",
                      source_fqns=("finance.sales.orders",), agent_fqns=("AgentA · 01aa",),
                      domain_id="sug_sales"),
        MeasureSignal(mv_fqn="marketing.rev.agent_b_rev", name="revenue",
                      expression="SUM(gross_amount)", comment="gross rev; GR; top line",
                      source_fqns=("marketing.rev.bookings",), agent_fqns=("AgentB · 01bb",),
                      domain_id="sug_marketing"),
    ]


def _status_column() -> ColumnSignal:
    return ColumnSignal(table_fqn="finance.sales.orders", column="status",
                        comment="order status code; state; how it stands",
                        distinct_values=("O", "F", "P"), governed=False, domain_id="sug_sales")


def _members() -> tuple[str, ...]:
    return (
        _ORDER_MV, "finance.sales.agent_a_rev", "marketing.rev.agent_b_rev",
        *_SRC, "finance.sales.orders", "marketing.rev.bookings",
        _SALES_AGENT, "AgentA · 01aa", "AgentB · 01bb",
    )


def _mine(**over):
    kw = dict(
        measures=[_total_revenue(), _discount_rate(), *_revenue_conflict()],
        columns=[_status_column()], members=_members(), drafter=_good_drafter,
    )
    kw.update(over)
    return pages.mine_pages(**kw)


def _by_archetype(cands):
    out: dict[str, list] = {}
    for c in cands:
        out.setdefault(c.archetype, []).append(c)
    return out


# ── Per-archetype detectors on the worked example ────────────────────────────


def test_worked_example_yields_the_four_archetypes():
    got = _by_archetype(_mine())
    assert set(got) == {"Routing", "Guardrail", "Disambiguation", "Taxonomy"}
    assert len(got["Routing"]) == 1 and len(got["Guardrail"]) == 1
    assert len(got["Disambiguation"]) == 1 and len(got["Taxonomy"]) == 1


def test_routing_sources_are_mv_and_related_is_agent():
    routing = _by_archetype(_mine())["Routing"][0]
    assert _ORDER_MV in routing.source_fqns
    assert all(s in _members() for s in routing.source_fqns)
    assert _SALES_AGENT in routing.related_fqns
    assert f"{_ORDER_MV}.total_revenue" in routing.body or "`total_revenue`" in routing.body


def test_guardrail_fires_on_percentage_rate_and_names_the_ratio():
    guard = _by_archetype(_mine())["Guardrail"][0]
    assert "discount_rate" in guard.title
    assert "average" in guard.body.lower()  # never-average-a-rate rule
    # Sources are the numerator/denominator origin tables + the MV.
    assert _ORDER_MV in guard.source_fqns


def test_disambiguation_from_conflicting_revenue_definitions():
    disamb = _by_archetype(_mine())["Disambiguation"][0]
    # Both conflicting measures aggregate into ONE Page, both as Sources.
    assert "finance.sales.agent_a_rev" in disamb.source_fqns
    assert "marketing.rev.agent_b_rev" in disamb.source_fqns
    # Inherently >=2 independent artifacts by construction.
    assert disamb.corroboration >= 2


def test_taxonomy_from_low_cardinality_coded_column_not_governed_no_certify():
    tax = _by_archetype(_mine())["Taxonomy"][0]
    assert "status" in tax.title
    assert tax.certify is False  # not a governed code list


# ── Canonical-concept keying (MV-D49) ───────────────────────────────────────


def test_cross_subdomain_same_concept_collapses_to_one_page_stable_id():
    # Same measure name + expression in two DIFFERENT sub-domains → ONE Page.
    m1 = MeasureSignal(mv_fqn="finance.a.rev", name="total_revenue",
                       expression="SUM(x)", comment="TR; net sales; booked",
                       source_fqns=("finance.a.t",), agent_fqns=("A · 1",), domain_id="sug_finance")
    m2 = MeasureSignal(mv_fqn="ops.b.rev", name="total_revenue",
                       expression="SUM(x)", comment="TR; net sales; booked",
                       source_fqns=("ops.b.t",), agent_fqns=("B · 2",), domain_id="sug_ops")
    members = ["finance.a.rev", "ops.b.rev", "finance.a.t", "ops.b.t", "A · 1", "B · 2"]
    cands = pages.mine_pages(measures=[m1, m2], members=members, drafter=_good_drafter)
    routing = [c for c in cands if c.archetype == "Routing"]
    assert len(routing) == 1
    # Sources aggregate BOTH sub-domains' MVs.
    assert {"finance.a.rev", "ops.b.rev"} <= set(routing[0].source_fqns)

    # page_id is derived from canonical_id, NOT domain_id: moving the home sub-domain
    # leaves it unchanged.
    moved = pages.mine_pages(
        measures=[m1, MeasureSignal(mv_fqn="ops.b.rev", name="total_revenue", expression="SUM(x)",
                                    comment="TR; net sales; booked", source_fqns=("ops.b.t",),
                                    agent_fqns=("B · 2",), domain_id="sug_DIFFERENT")],
        members=members, drafter=_good_drafter,
    )
    moved_routing = [c for c in moved if c.archetype == "Routing"][0]
    assert moved_routing.page_id == routing[0].page_id


def test_page_id_ignores_body_text_variation():
    a = _mine(drafter=_good_drafter)
    b = _mine(drafter=lambda f: _good_drafter(f) + "\n\n(extra prose)")
    assert {c.page_id for c in a} == {c.page_id for c in b}


# ── Estate corroboration gate (MV-D35) ──────────────────────────────────────


def test_single_artifact_concept_is_low_confidence_and_not_certified():
    solo = MeasureSignal(mv_fqn="finance.x.mv", name="total_revenue", expression="SUM(a)",
                         comment="TR; net sales; booked", source_fqns=("finance.x.t",),
                         domain_id="sug_x")  # no agent → one artifact
    [c] = [c for c in pages.mine_pages(measures=[solo], members=["finance.x.mv", "finance.x.t"],
                                       drafter=_good_drafter) if c.archetype == "Routing"]
    assert c.corroboration == 1
    assert c.certify is False
    assert c.evidence["low_confidence"] is True


def test_two_artifact_concept_is_certify_eligible():
    routing = _by_archetype(_mine())["Routing"][0]
    assert routing.corroboration >= 2
    assert routing.certify is True  # corroborated + synonyms + healthy draft


# ── Identifier gate ─────────────────────────────────────────────────────────


def test_invented_identifier_degrades_to_stub():
    routing = _by_archetype(_mine(drafter=_invented_drafter))["Routing"][0]
    assert routing.evidence["body_source"] == "stub"
    assert "ghost_column" not in routing.body
    assert routing.certify is False  # LLM output rejected → not certified


def test_source_fqns_all_exist_in_members():
    for c in _mine():
        for s in c.source_fqns:
            assert s in _members(), f"invented Source {s}"


# ── Retrieval gates ─────────────────────────────────────────────────────────


def test_synonym_shortfall_flags_low_confidence_not_certified():
    thin = MeasureSignal(mv_fqn="finance.x.mv", name="amt", expression="SUM(a)",
                         source_fqns=("finance.x.t",), agent_fqns=("A · 1",), domain_id="sug_x")
    [c] = [c for c in pages.mine_pages(measures=[thin], members=["finance.x.mv", "finance.x.t", "A · 1"],
                                       drafter=_good_drafter) if c.archetype == "Routing"]
    assert c.evidence["gate_results"]["synonyms"] is False
    assert c.certify is False


def test_chunk_safe_gate_rejects_bare_pronoun_rule():
    assert pages.chunk_safe_gate("Rules:\n  - Route `x` to `y`.") is True
    assert pages.chunk_safe_gate("Rules:\n  - It should never be averaged.") is False


def test_specificity_gate_requires_backticked_identifier():
    assert pages.specificity_gate("Definition:\n  Use `a.b.c` here.") is True
    assert pages.specificity_gate("Definition:\n  Be careful with rates.") is False


# ── Contradiction gate (read-only) ──────────────────────────────────────────


def test_contradiction_downgrades_to_conflict_and_never_writes_back():
    instr = ["revenue is defined as SUM(list_price) for the quarter"]
    disamb = _by_archetype(_mine(instructions=instr))["Disambiguation"][0]
    assert disamb.evidence["status"] == "CONFLICT"
    assert disamb.certify is False
    # The engine reuses the mv_fingerprint comparator, not a new one, and writes nothing.
    src = pages.__file__
    text = open(src).read().lower()
    assert "mv_fingerprint" in text
    for banned in ("text_instructions =", ".put(", "update instruction"):
        assert banned not in text


# ── Certify rule ─────────────────────────────────────────────────────────────


def test_certify_true_only_for_corroborated_formula_archetypes():
    got = _by_archetype(_mine())
    assert got["Routing"][0].certify is True
    assert got["Guardrail"][0].certify is True
    assert got["Disambiguation"][0].certify is True
    assert got["Taxonomy"][0].certify is False  # non-governed code list


# ── Degrade (MV-D43) ─────────────────────────────────────────────────────────


def test_llm_down_yields_stub_bodies_and_certify_false():
    for c in _mine(drafter=_boom_drafter):
        assert c.evidence["body_source"] == "stub"
        assert c.certify is False
        assert c.body  # a real deterministic body, run succeeds


def test_routing_validation_degrades_to_unvalidated_when_genie_unreachable():
    def _unreachable(_q, _m):
        raise RuntimeError("genie down")

    routing = _by_archetype(_mine(routing_validator=_unreachable))["Routing"][0]
    assert routing.evidence["routing_validated"] is None  # unvalidated, run continues


def test_routing_validation_records_confirmation():
    routing = _by_archetype(_mine(routing_validator=lambda q, m: True))["Routing"][0]
    assert routing.evidence["routing_validated"] is True


# ── Determinism / idempotency ────────────────────────────────────────────────


def test_mining_is_deterministic_same_page_ids_and_order():
    a = _mine()
    b = _mine()
    assert [c.page_id for c in a] == [c.page_id for c in b]
    assert [c.archetype for c in a] == [c.archetype for c in b]


def test_anchor_identity_map_collapses_two_refs_to_one_concept():
    # Two differently-NAMED measures the 17d map merged into one canonical concept.
    class _V:
        canonical_id = "dedupe_anchored"
        members = ("finance.p.mv.rev_a", "finance.q.mv.rev_b")
        verdict = "merge"

    m1 = MeasureSignal(mv_fqn="finance.p.mv", name="rev_a", expression="SUM(x)",
                       comment="TR; net sales; booked", source_fqns=("finance.p.t",),
                       agent_fqns=("A · 1",), domain_id="sug_p")
    m2 = MeasureSignal(mv_fqn="finance.q.mv", name="rev_b", expression="SUM(x)",
                       comment="TR; net sales; booked", source_fqns=("finance.q.t",),
                       agent_fqns=("B · 2",), domain_id="sug_q")
    cands = pages.mine_pages(measures=[m1, m2], identity_verdicts=[_V()],
                             members=["finance.p.mv", "finance.q.mv", "finance.p.mv.rev_a",
                                      "finance.q.mv.rev_b", "finance.p.t", "finance.q.t",
                                      "A · 1", "B · 2"],
                             drafter=_good_drafter)
    routing = [c for c in cands if c.archetype == "Routing"]
    assert len(routing) == 1
    assert routing[0].canonical_id == "dedupe_anchored"


def test_default_page_drafter_degrades_to_empty_when_backend_absent():
    # On a job cluster without `backend` importable, the drafter returns "" so the
    # engine falls back to the deterministic stub (MV-D43) — never raises.
    drafter = pages.default_page_drafter()
    assert drafter({"archetype": "Routing", "concept": "x", "description": "",
                    "definition": "", "rules": [], "sources": []}) == ""
