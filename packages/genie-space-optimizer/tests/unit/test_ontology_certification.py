"""Unit tests for Signal Authority Stage 2 (authority: certification, MV-D94).

Four concerns:
  1. ``certification.certification_map`` — the pure status reducer (native + boolean
     surfaces, honest-gap omission, deprecated-wins conflict, deterministic).
  2. Governance rung — a certified-only FQN lifts to ``curated`` (0.6); an already-governed
     FQN stays ``governed`` (1.0, max wins); an untagged FQN is absent. ``_GOVERNANCE_VALUE``
     / ``blend`` / ``FACTOR_WEIGHTS`` are UNCHANGED — this only populates a rung the ladder
     already reserved.
  3. Deprecation firewall — a proposal (Domain or Page) anchored on a deprecated asset is
     ``blocked`` with ``block_reason == "deprecated_asset"`` and ``surfaced == false`` even at
     a high blend (mirrors ``test_pii_tag_name_is_blocked_and_dropped_from_surfaced``).
  4. Naming invariant + degrade-not-hang — ``certified`` / ``system.certification_status``
     never name a domain (``is_domain_entity_tag`` still drops them); the ``_gather_certification``
     wheel boundary degrades to {} (byte-identical default).
"""

from __future__ import annotations

import json

from genie_space_optimizer.ontology import certification, materialize, rank, transforms


def _domain_row(domain_id, *, tag_decision="create", tag_key="Finance", tag_value="Finance",
                evidence=None):
    return {
        "metastore_id": "ms1",
        "domain_id": domain_id,
        "workspace_id": "ws1",
        "parent_id": None,
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


# ── 1. The pure status reducer ──────────────────────────────────────────────

def test_certification_map_reads_both_surfaces():
    """Native ``system.certification_status`` carries its status as the value; the boolean
    ``certified`` tag asserts ``certified`` only when true."""
    out = certification.certification_map([
        {"fqn": "c.s.native_cert", "tag_name": "system.certification_status", "tag_value": "certified"},
        {"fqn": "c.s.native_dep", "tag_name": "system.certification_status", "tag_value": "deprecated"},
        {"fqn": "c.s.bool_cert", "tag_name": "certified", "tag_value": "true"},
    ])
    assert out == {
        "c.s.native_cert": "certified",
        "c.s.native_dep": "deprecated",
        "c.s.bool_cert": "certified",
    }


def test_certification_map_omits_uncertified_and_absent_honest_gap():
    """``certified=false``, an unrecognized value, or a blank fqn ⇒ OMITTED (never inferred
    ``ungoverned`` — the honest-gap discipline)."""
    out = certification.certification_map([
        {"fqn": "c.s.notcert", "tag_name": "certified", "tag_value": "false"},
        {"fqn": "c.s.weird", "tag_name": "system.certification_status", "tag_value": "pending"},
        {"fqn": "", "tag_name": "certified", "tag_value": "true"},
        {"tag_name": "certified", "tag_value": "true"},  # no fqn
    ])
    assert out == {}


def test_certification_map_deprecated_wins_over_certified_and_is_order_independent():
    """A contradictory asset (both deprecated and certified) is steered away from: deprecated
    wins, regardless of the order the reader hands the rows in (deterministic)."""
    rows = [
        {"fqn": "c.s.x", "tag_name": "certified", "tag_value": "true"},
        {"fqn": "c.s.x", "tag_name": "system.certification_status", "tag_value": "deprecated"},
    ]
    out1 = certification.certification_map(rows)
    out2 = certification.certification_map(list(reversed(rows)))
    assert out1 == out2 == {"c.s.x": "deprecated"}


def test_certification_map_is_sorted_and_case_insensitive():
    out = certification.certification_map([
        {"fqn": "c.s.b", "tag_name": "CERTIFIED", "tag_value": "TRUE"},
        {"fqn": "c.s.a", "tag_name": "System.Certification_Status", "tag_value": "Certified"},
    ])
    assert list(out) == ["c.s.a", "c.s.b"]           # sorted output
    assert out == {"c.s.a": "certified", "c.s.b": "certified"}


def test_certification_map_empty_input_degrades_to_empty():
    assert certification.certification_map([]) == {}
    assert certification.certification_map(()) == {}


# ── 2. The governance rung (blend / FACTOR_WEIGHTS UNCHANGED) ────────────────

def test_certified_only_fqn_lifts_governance_factor_to_curated():
    """A certified FQN with no governed aboutness tag populates the ``curated`` (0.6) rung the
    ladder reserved — present, value 0.6 — where before it was absent."""
    gov = materialize._governance_map({"tags": []}, {"c.s.cert": "certified"})
    assert gov == {"c.s.cert": "curated"}
    factor = rank.blend(["c.s.cert"], rank.RankSignals(governance=gov))["factors"]["governance"]
    assert factor["present"] is True
    assert factor["value"] == rank._GOVERNANCE_VALUE["curated"] == 0.6


def test_already_governed_fqn_stays_governed_when_also_certified():
    """Certification never demotes an aboutness-governed asset: the max rung wins (governed
    1.0 > curated 0.6)."""
    graph_struct = {"tags": [{"members": [{"fqn": "c.s.gov"}]}]}
    gov = materialize._governance_map(graph_struct, {"c.s.gov": "certified"})
    assert gov["c.s.gov"] == "governed"
    factor = rank.blend(["c.s.gov"], rank.RankSignals(governance=gov))["factors"]["governance"]
    assert factor["value"] == rank._GOVERNANCE_VALUE["governed"] == 1.0


def test_untagged_fqn_is_absent_from_governance_map():
    """An asset with neither an aboutness tag nor a certification tag is absent (honest-gap)."""
    gov = materialize._governance_map({"tags": []}, {"c.s.cert": "certified"})
    assert "c.s.other" not in gov
    # …and a deprecated FQN is NOT a governance rung (it is a firewall, wired separately).
    gov_dep = materialize._governance_map({"tags": []}, {"c.s.dep": "deprecated"})
    assert gov_dep == {}


def test_governance_map_byte_identical_without_certification():
    """The default (no certification) path is byte-identical to the governed-only map."""
    graph_struct = {"tags": [{"members": [{"fqn": "c.s.gov"}]}]}
    assert materialize._governance_map(graph_struct) == {"c.s.gov": "governed"}
    assert materialize._governance_map(graph_struct, {}) == {"c.s.gov": "governed"}


# ── 3. The deprecation firewall (mirror the PII block) ──────────────────────

def test_deprecated_asset_is_blocked_and_dropped_from_surfaced():
    """A Domain anchored on a deprecated asset is blocked with ``deprecated_asset`` and never
    surfaces, even at a maxed blend (mirror ``test_pii_tag_name_is_blocked_and_dropped_from_surfaced``)."""
    row = _domain_row("sug_dep")
    signals = rank.RankSignals(
        governance={"c.s.a": "governed"}, centrality={"c.s.a": 1.0}, usage={"c.s.a": 1.0},
        deprecated=frozenset({"c.s.a"}),
    )
    rank.score_proposals([row], [], members_by_domain={"sug_dep": ["c.s.a"]}, signals=signals)
    r = _ev(row)
    assert r["rank"]["blocked"] is True
    assert r["rank"]["block_reason"] == "deprecated_asset"
    assert r["surfaced"] is False  # blocked → never surfaced, regardless of a high score
    report = rank.mark_surfaced([row], [], [])
    assert report["blocked"] == 1 and report["surfaced"] == 0


def test_deprecated_source_blocks_a_page_proposal():
    """A deprecated asset poisons a Page too — the firewall is not Domain-only."""
    page = {
        "metastore_id": "ms1", "page_id": "pg_dep", "workspace_id": "ws1",
        "title": "Fares", "body": "b", "domain_id": "",
        "source_fqns": ["c.s.dep_src"], "related_fqns": [],
        "evidence": json.dumps({}, sort_keys=True), "score": 0.0,
        "run_id": "r1", "as_of": "2026-08-31T00:00:00+00:00",
    }
    signals = rank.RankSignals(usage={"c.s.dep_src": 1.0}, deprecated=frozenset({"c.s.dep_src"}))
    rank.score_proposals([], [page], signals=signals, page_require_domain=False)
    r = _ev(page)
    assert r["rank"]["blocked"] is True and r["rank"]["block_reason"] == "deprecated_asset"
    assert r["surfaced"] is False


def test_empty_deprecated_set_blocks_nothing():
    """The default empty ``deprecated`` set is a no-op firewall (today's bytes)."""
    row = _domain_row("sug_ok")
    signals = rank.RankSignals(governance={"c.s.a": "governed"}, centrality={"c.s.a": 1.0},
                               usage={"c.s.a": 1.0})
    rank.score_proposals([row], [], members_by_domain={"sug_ok": ["c.s.a"]}, signals=signals)
    r = _ev(row)
    # No deprecation block: the firewall is a pure no-op with an empty set (other gates,
    # e.g. the legitimacy bar, are unaffected and out of scope here).
    assert r["rank"].get("blocked") is False
    assert "block_reason" not in r["rank"]


# ── 4. Naming invariant + degrade-not-hang ──────────────────────────────────

def test_certification_never_names_a_domain():
    """Stage 2 redirects certification into AUTHORITY, not naming: ``certified`` /
    ``system.certification_status`` are still dropped by ``is_domain_entity_tag`` (MV-D51)."""
    assert transforms.is_domain_entity_tag("certified") is False
    assert transforms.is_domain_entity_tag("system.certification_status") is False


def test_gather_certification_degrades_to_empty_when_reader_raises():
    """A reader whose ``certification_status`` raises degrades to {} (MV-D43) — never a faked map."""
    class _Boom:
        def certification_status(self, allowlist):
            raise RuntimeError("no certification grant")

    assert materialize._gather_certification(_Boom(), ["c"]) == {}


def test_gather_certification_empty_when_reader_missing_or_empty():
    """No certification grant / empty allowlist / an older reader without the method ⇒ {}."""
    class _Empty:
        def certification_status(self, allowlist):
            return {}

    class _Older:
        pass

    assert materialize._gather_certification(_Empty(), []) == {}
    assert materialize._gather_certification(_Empty(), ["c"]) == {}
    assert materialize._gather_certification(_Older(), ["c"]) == {}
