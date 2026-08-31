"""L6 rank & trust gate — offline unit tests (Phase 3d §11, items 1-4, 6).

Covers the ranking order + coverage cap (a single signal never outranks a
corroborated finding), the PII / policy firewalls, the dormant provenance seam
("T3 hint never outranks a T0 fact"), suppression idempotency (a dismissed
proposal + a rejected reassign stay suppressed on re-run, MV-D26), and the
metastore-grain ledger match. All pure/offline — no cluster, no I/O.
"""

from __future__ import annotations

import json

from genie_space_optimizer.ontology import rank, transforms


def _domain_row(domain_id, *, tag_decision="create", tag_key="Finance", tag_value="Finance",
                parent_id=None, evidence=None):
    return {
        "metastore_id": "ms1",
        "domain_id": domain_id,
        "workspace_id": "ws1",
        "parent_id": parent_id,
        "name": tag_value,
        "description": "d",
        "tag_decision": tag_decision,
        "tag_key": tag_key,
        "tag_value": tag_value,
        "evidence": json.dumps(evidence or {}, sort_keys=True),
        "score": 0.0,
        "run_id": "r1",
        "as_of": "2026-08-31T00:00:00+00:00",
    }


def _ev(row):
    return json.loads(row["evidence"])


# ── 1. Ranking order + coverage cap ─────────────────────────────────────────


def test_corroborated_outranks_single_signal_via_coverage_cap():
    corroborated = _domain_row("sug_corr")
    single = _domain_row("sug_single", tag_key="Sales", tag_value="Sales")
    members = {"sug_corr": ["c.s.a"], "sug_single": ["c.s.b"]}
    # corroborated: all three factors present (high coverage). single: usage only.
    signals = rank.RankSignals(
        usage={"c.s.a": 0.9, "c.s.b": 0.95},
        centrality={"c.s.a": 0.9},
        governance={"c.s.a": "governed"},
    )
    rank.score_proposals([corroborated, single], [], members_by_domain=members, signals=signals)

    corr_rank = _ev(corroborated)["rank"]
    single_rank = _ev(single)["rank"]
    # Corroborated clears full coverage → HIGH; single-signal is capped LOW even though
    # its raw score is high (coverage 0.40 < 0.50 ceiling).
    assert corr_rank["tier"] == "high"
    assert single_rank["uncapped_tier"] == "high" and single_rank["tier"] == "low"
    assert single_rank["tier_capped_by_coverage"] is True
    # The corroborated finding scores strictly above the single-signal one on tier rank.
    order = ("low", "medium", "high")
    assert order.index(corr_rank["tier"]) > order.index(single_rank["tier"])


def test_tier_of_thresholds_stable():
    assert transforms.tier_of(90) == "high"
    assert transforms.tier_of(60) == "medium"
    assert transforms.tier_of(30) == "low"
    assert transforms.tier_of(10) is None  # sub-threshold → never served


def test_sub_threshold_is_not_surfaced():
    row = _domain_row("sug_weak")
    # governance ungoverned only → value 0.2, coverage 0.25 → score 20 → sub-threshold.
    signals = rank.RankSignals(governance={"c.s.a": "ungoverned"})
    rank.score_proposals([row], [], members_by_domain={"sug_weak": ["c.s.a"]}, signals=signals)
    r = _ev(row)
    assert r["rank"]["tier"] is None
    assert r["surfaced"] is False


# ── 2. PII / policy firewalls ───────────────────────────────────────────────


def test_pii_tag_name_is_blocked_and_dropped_from_surfaced():
    row = _domain_row("sug_pii", tag_key="customer_ssn", tag_value="customer_ssn")
    signals = rank.RankSignals(governance={"c.s.a": "governed"}, centrality={"c.s.a": 1.0},
                               usage={"c.s.a": 1.0})
    rank.score_proposals([row], [], members_by_domain={"sug_pii": ["c.s.a"]}, signals=signals)
    r = _ev(row)
    assert r["rank"]["blocked"] is True
    assert r["rank"]["block_reason"] == "tag_name_pii"
    assert r["surfaced"] is False  # blocked → never surfaced, regardless of a high score
    report = rank.mark_surfaced([row], [], [])
    assert report["blocked"] == 1 and report["surfaced"] == 0


def test_pii_tag_value_email_is_blocked():
    row = _domain_row("sug_email", tag_key="Team", tag_value="alice@example.com")
    rank.score_proposals([row], [], members_by_domain={"sug_email": ["c.s.a"]},
                         signals=rank.RankSignals(governance={"c.s.a": "governed"}))
    assert _ev(row)["rank"]["blocked"] is True


def test_policy_write_intent_is_rejected():
    row = _domain_row("sug_write", evidence={"write_intent": True})
    rank.score_proposals([row], [], members_by_domain={"sug_write": ["c.s.a"]},
                         signals=rank.RankSignals(governance={"c.s.a": "governed"}))
    r = _ev(row)["rank"]
    assert r["blocked"] is True and r["block_reason"] == "policy_write_intent"


def test_propose_only_tag_decisions_pass_policy():
    for decision in ("reuse", "create", "reassign"):
        ok, _ = rank.policy_conform("domain", decision, {})
        assert ok is True
    assert rank.policy_conform("domain", "APPLY", {})[0] is False


# ── 3. Provenance ladder (dormant seam, MV-D38) ─────────────────────────────


def test_provenance_ladder_is_a_dormant_no_op_pass():
    ok, reason = rank.provenance_ladder("T0")
    assert ok is True and reason == "dormant"


def test_t3_hint_never_outranks_a_t0_fact():
    # The invariant 17h enforces; pinned dormant here so the seam cannot regress.
    assert rank.outranks("T0", "T3") is True
    assert rank.outranks("T3", "T0") is False
    assert rank.outranks("T0", "T1") and rank.outranks("T1", "T2") and rank.outranks("T2", "T3")


# ── 4. Suppression idempotency (MV-D26) ─────────────────────────────────────


def _governed_signals(fqn="c.s.a"):
    return rank.RankSignals(usage={fqn: 0.9}, centrality={fqn: 0.9}, governance={fqn: "governed"})


def test_dismissed_proposal_is_suppressed_and_stays_suppressed_on_rerun():
    # The legitimacy bar (MV-D57) is orthogonal to suppression; disable it here so a
    # minimal single-asset fixture still surfaces and the suppression path is exercised.
    def fresh():
        row = _domain_row("sug_dom")
        rank.score_proposals([row], [], members_by_domain={"sug_dom": ["c.s.a"]},
                             signals=_governed_signals(),
                             min_tables=1, min_schemas=1, require_connection=False)
        return row

    suppressions = [{"metastore_id": "ms1", "proposal_kind": "domain", "proposal_id": "sug_dom"}]

    row1 = fresh()
    assert _ev(row1)["surfaced"] is True  # would surface absent a suppression
    rank.mark_surfaced([row1], [], suppressions)
    assert _ev(row1)["surfaced"] is False and _ev(row1)["rank"]["dismissed"] is True

    # Re-run: deterministic re-score + the same suppression → still not surfaced.
    row2 = fresh()
    report = rank.mark_surfaced([row2], [], suppressions)
    assert _ev(row2)["surfaced"] is False
    assert report["suppressed"] == 1 and report["surfaced"] == 0


def test_rejected_reassign_stays_suppressed():
    row = _domain_row("sug_re", tag_decision="reassign", tag_key="Finance", tag_value="Finance")
    assert transforms.proposal_kind_of(row) == "reassign"
    rank.score_proposals([row], [], members_by_domain={"sug_re": ["c.s.a"]}, signals=_governed_signals())
    # A rejected reassign is stored with proposal_kind="reassign".
    supp = [{"metastore_id": "ms1", "proposal_kind": "reassign", "proposal_id": "sug_re"}]
    rank.mark_surfaced([row], [], supp)
    assert _ev(row)["surfaced"] is False


def test_suppression_of_other_kind_does_not_hide_a_domain():
    row = _domain_row("sug_dom")
    # Legitimacy bar off (orthogonal) so the single-asset fixture surfaces.
    rank.score_proposals([row], [], members_by_domain={"sug_dom": ["c.s.a"]}, signals=_governed_signals(),
                         min_tables=1, min_schemas=1, require_connection=False)
    # A page suppression with the same id must NOT hide the domain (kind-scoped).
    rank.mark_surfaced([row], [], [{"proposal_kind": "page", "proposal_id": "sug_dom"}])
    assert _ev(row)["surfaced"] is True


# ── 6. Metastore grain ──────────────────────────────────────────────────────


# ── Stage 3: legitimacy bar (MV-D57) + honest confidence band (MV-D56) ──────


def test_legitimacy_ok_thresholds():
    assert transforms.legitimacy_ok(3, 2, True)[0] is True
    ok, reason = transforms.legitimacy_ok(1, 1, False)
    assert ok is False and "legitimacy bar" in reason
    # require_connection off lets a big edgeless group pass; config can lower the bar.
    assert transforms.legitimacy_ok(5, 3, False, require_connection=False)[0] is True
    assert transforms.legitimacy_ok(1, 1, True, min_tables=1, min_schemas=1)[0] is True


def test_below_bar_group_kept_not_surfaced_with_hint():
    # A 1-table / 1-schema shared-schema fragment (the §Appendix A junk) is KEPT but
    # not surfaced, with an "add to existing domain" hint — not a standalone Domain.
    row = _domain_row("sug_small", evidence={"reason": "grouped by shared schema: c.bakehouse"})
    rank.score_proposals([row], [], members_by_domain={"sug_small": ["c.bakehouse.sales"]},
                         signals=_governed_signals("c.bakehouse.sales"))
    r = _ev(row)
    assert r["rank"]["legitimate"] is False
    assert r["surfaced"] is False              # gated even though the score is high
    assert r["gate_hint"].startswith("add to existing domain:")
    assert "c.bakehouse" in r["gate_hint"]


def test_legit_group_surfaces_over_the_bar():
    ev = {"shared_spine": ["a.rev.fact", "a.rev.dim"],
          "reason": "grouped by foreign key / shared join column"}
    row = _domain_row("sug_big", evidence=ev)
    members = ["a.rev.fact", "a.rev.dim", "a.ops.log"]  # 3 tables, 2 schemas, connected
    rank.score_proposals([row], [], members_by_domain={"sug_big": members},
                         signals=_governed_signals("a.rev.fact"))
    r = _ev(row)
    assert r["rank"]["legitimate"] is True
    assert r["surfaced"] is True


def test_shared_schema_only_group_gated_on_connection():
    # 3 tables but ONE schema and no structural connection → require_connection prunes.
    row = _domain_row("sug_sch", evidence={"reason": "grouped by shared schema: c.bakehouse"})
    members = ["c.bakehouse.a", "c.bakehouse.b", "c.bakehouse.c"]
    rank.score_proposals([row], [], members_by_domain={"sug_sch": members},
                         signals=_governed_signals("c.bakehouse.a"))
    r = _ev(row)
    assert r["rank"]["legitimate"] is False
    assert r["surfaced"] is False


def test_subdomain_is_exempt_from_legitimacy_bar():
    row = _domain_row("sug_sub", parent_id="sug_parent")
    rank.score_proposals([row], [], members_by_domain={"sug_sub": ["c.s.a"]}, signals=_governed_signals())
    r = _ev(row)
    assert transforms.proposal_kind_of(row) == "subdomain"
    assert r["rank"].get("legitimate") is None  # gate never runs for a sub-domain
    assert r["surfaced"] is True


def test_confidence_band_full_coverage_is_high_no_gap_no_percent():
    b = rank.blend(["c.s.a"], rank.RankSignals(
        usage={"c.s.a": 0.9}, centrality={"c.s.a": 0.8}, governance={"c.s.a": "governed"}))
    band = transforms.confidence_band(b)
    assert band["band"] == "High"
    assert band["signals_present"] == [
        "actively queried", "central to how the data connects", "built on governed data"]
    assert band["gap"] == ""
    # NEVER a percent (MV-D35).
    assert "%" not in band["gap"] and not any("%" in s for s in band["signals_present"])


def test_confidence_band_partial_coverage_names_the_gap():
    # Centrality only → coverage 0.35 caps the tier to LOW; the gap names the first
    # missing factor (usage) as a next step, never a number.
    b = rank.blend(["c.s.a"], rank.RankSignals(centrality={"c.s.a": 0.9}))
    band = transforms.confidence_band(b)
    assert band["band"] == "Low"
    assert band["signals_present"] == ["central to how the data connects"]
    assert band["gap"] == "connect query history to rank by usage"


def test_confidence_band_written_into_rank_block():
    row = _domain_row("sug_big", evidence={
        "shared_spine": ["a.rev.fact", "a.rev.dim"], "reason": "grouped by foreign key"})
    rank.score_proposals([row], [], members_by_domain={"sug_big": ["a.rev.fact", "a.rev.dim", "a.ops.log"]},
                         signals=_governed_signals("a.rev.fact"))
    conf = _ev(row)["rank"]["confidence"]
    assert set(conf) == {"band", "signals_present", "gap"}
    assert conf["band"] in ("High", "Medium", "Low")


def test_sub_threshold_confidence_band_is_none():
    b = rank.blend(["c.s.a"], rank.RankSignals(governance={"c.s.a": "ungoverned"}))  # score 20 → None
    assert transforms.confidence_band(b)["band"] is None


def test_ledger_match_is_by_kind_and_id_not_workspace():
    """The suppression match keys on (proposal_kind, proposal_id) at metastore grain;
    workspace_id is provenance and never part of the match (MV-D49)."""
    row = _domain_row("sug_dom")
    rank.score_proposals([row], [], members_by_domain={"sug_dom": ["c.s.a"]}, signals=_governed_signals())
    # A suppression row carrying a different workspace_id still matches.
    supp = [{"metastore_id": "ms1", "workspace_id": "ws-other", "proposal_kind": "domain", "proposal_id": "sug_dom"}]
    rank.mark_surfaced([row], [], supp)
    assert _ev(row)["surfaced"] is False
