"""Ontology L3 ER/dedupe — offline unit tests (Phase-3a §11).

Runs on the in-process cosine backend over fixture vectors; the LLM adjudicator is
mocked; no Lakebase Search, no cluster.
"""

from __future__ import annotations

from genie_space_optimizer.ontology import er, similarity

C = er.DedupeCandidate


# ── Blocking recall + sub-quadratic ─────────────────────────────────────────


def test_blocking_puts_true_dupes_in_shared_bucket_and_is_subquadratic():
    cands = [
        C("m.order_revenue", "measure", "order_revenue", "order revenue"),
        C("m.orders_revenue", "measure", "orders_revenue", "orders revenue"),
        C("m.net_revenue", "measure", "net revenue", "net revenue after discount"),
        C("m.rev_after_disc", "measure", "revenue after discount", "revenue after discount"),
        C("m.headcount", "measure", "headcount", "employee headcount"),
    ]
    buckets = er.block(cands)
    pairs = er.candidate_pairs(buckets)
    # True-dup pairs that share a token co-occur (recall 1.0 on these).
    assert ("m.net_revenue", "m.rev_after_disc") in pairs or ("m.rev_after_disc", "m.net_revenue") in pairs
    assert ("m.order_revenue", "m.orders_revenue") in pairs or ("m.orders_revenue", "m.order_revenue") in pairs
    # Sub-quadratic: fewer than all C(n,2) pairs (headcount shares no token).
    n = len(cands)
    assert len(pairs) < n * (n - 1) // 2
    # headcount is not compared to revenue-family (no shared token).
    assert all("m.headcount" not in p for p in pairs)


# ── String vs embedding catch ───────────────────────────────────────────────


def _canonical_of(verdicts, ref):
    for v in verdicts:
        if ref in v.members:
            return v.canonical_id
    return None


def test_string_signal_collapses_typo_plural():
    cands = [
        C("m.order_revenue", "measure", "order_revenue", "order_revenue"),
        C("m.orders_revenue", "measure", "orders_revenue", "orders_revenue"),
    ]
    # No vectors → embedding signal is silent; the string signal must merge.
    v = er.run_er(cands, backend=similarity.InProcessCosineBackend(), vectors={}, adjudicator=None)
    assert len(v) == 1 and v[0].verdict == "merge" and v[0].method == "string"


def test_embedding_signal_collapses_paraphrase():
    cands = [
        C("m.net_revenue", "measure", "net revenue", "net revenue"),
        C("m.rev_after_disc", "measure", "revenue after discount", "revenue after discount"),
    ]
    vecs = {"m.net_revenue": [1.0, 0.0, 0.0], "m.rev_after_disc": [0.97, 0.24, 0.0]}  # cosine ~0.97
    v = er.run_er(cands, backend=similarity.InProcessCosineBackend(), vectors=vecs, adjudicator=None)
    assert len(v) == 1 and v[0].verdict == "merge" and v[0].method == "embedding"


def test_true_distinct_pair_never_merges():
    cands = [
        C("m.headcount", "measure", "headcount metric", "employee headcount"),
        C("m.revenue", "measure", "revenue metric", "total revenue"),
    ]
    vecs = {"m.headcount": [0.0, 1.0], "m.revenue": [1.0, 0.0]}  # orthogonal
    v = er.run_er(cands, backend=similarity.InProcessCosineBackend(), vectors=vecs, adjudicator=None)
    assert _canonical_of(v, "m.headcount") != _canonical_of(v, "m.revenue")


# ── Adjudication band: LLM reached ONLY for near-ties ───────────────────────


def _band_candidates():
    return [
        C("m.cust_segment", "measure", "customer segment", "customer segment"),
        C("m.cust_segmentation", "measure", "customer segmentation", "customer segmentation"),  # ~0.76 -> band
        C("m.cust_age", "measure", "customer age", "customer age"),
        C("m.cust_ages", "measure", "customer ages", "customer ages"),                           # ~0.92 -> merge
    ]


def test_adjudicator_called_only_on_band_and_no_keeps_distinct():
    cands = _band_candidates()
    calls = []

    def adj(a, b):
        calls.append(tuple(sorted((a.ref, b.ref))))
        return (False, "different concepts")

    v = er.run_er(cands, backend=similarity.InProcessCosineBackend(), vectors={}, adjudicator=adj)
    # Only the near-tie pair reached the adjudicator; the ≥merge and <escalate pairs did not.
    assert calls == [("m.cust_segment", "m.cust_segmentation")]
    # "no" keeps them distinct.
    assert _canonical_of(v, "m.cust_segment") != _canonical_of(v, "m.cust_segmentation")
    # The clear-merge pair (age/ages) still merged.
    assert _canonical_of(v, "m.cust_age") == _canonical_of(v, "m.cust_ages")


def test_adjudicator_yes_merges_the_near_tie():
    cands = _band_candidates()
    v = er.run_er(cands, backend=similarity.InProcessCosineBackend(), vectors={}, adjudicator=lambda a, b: (True, "same"))
    seg = _canonical_of(v, "m.cust_segment")
    assert seg == _canonical_of(v, "m.cust_segmentation")
    merged = next(x for x in v if x.canonical_id == seg)
    assert merged.verdict == "merge" and merged.method == "llm"


def test_default_adjudicator_reached_only_on_band_via_mocked_endpoint(monkeypatch):
    from genie_space_optimizer.common import llm as common_llm

    calls = {"n": 0}

    def fake_call(w, *, messages, model=None, max_tokens=None, **kwargs):
        calls["n"] += 1
        return "NO: distinct", None

    monkeypatch.setattr(common_llm, "call_llm_core", fake_call)
    v = er.run_er(
        _band_candidates(), backend=similarity.InProcessCosineBackend(),
        vectors={}, adjudicator=er.default_adjudicator(),
    )
    assert calls["n"] == 1  # exactly the one near-tie pair
    assert _canonical_of(v, "m.cust_segment") != _canonical_of(v, "m.cust_segmentation")


def test_adjudicator_down_degrades_to_escalate_unmerged():
    def boom(a, b):
        raise RuntimeError("LLM endpoint down")

    v = er.run_er(_band_candidates(), backend=similarity.InProcessCosineBackend(), vectors={}, adjudicator=boom)
    # Degrade (MV-D43): the near-tie is left unmerged; the run still produces verdicts.
    assert _canonical_of(v, "m.cust_segment") != _canonical_of(v, "m.cust_segmentation")
    seg = next(x for x in v if "m.cust_segment" in x.members)
    assert seg.verdict == "escalate"


# ── Bounded batch adjudication (MV-D68) ─────────────────────────────────────
# Two independent near-tie pairs, blocked by a unique per-pair token, with cosine driven
# by fixture vectors: P ~0.88, Q ~0.78 (both in the [0.72, 0.90) band). Text tokens are
# disjoint so the keyword signal never overtakes cosine.
def _two_band_pairs():
    return [
        C("m.p1a", "measure", "grpx aaa", "north wind rst"),
        C("m.p1b", "measure", "grpx bbb", "south rain zzz"),
        C("m.p2a", "measure", "grpy ccc", "cat dog fish"),
        C("m.p2b", "measure", "grpy ddd", "tree lamp desk"),
    ]


_TWO_BAND_VECS = {
    "m.p1a": [1.0, 0.0], "m.p1b": [0.88, 0.475],    # cosine ~0.88 (stronger near-tie)
    "m.p2a": [1.0, 0.0], "m.p2b": [0.78, 0.6258],   # cosine ~0.78 (weaker near-tie)
}


def test_adjudicate_cap_limits_llm_calls_and_defers_rest_to_escalate():
    cands = _two_band_pairs()
    calls = []

    def adj(a, b):
        calls.append(tuple(sorted((a.ref, b.ref))))
        return (True, "same")

    v = er.run_er(
        cands, backend=similarity.InProcessCosineBackend(), vectors=_TWO_BAND_VECS,
        adjudicator=adj, adjudicate_max_pairs=1, adjudicate_min_score=er.ESCALATE_LOW,
    )
    # Only the STRONGEST near-tie (P, ~0.88) is LLM-adjudicated; the cap defers the rest.
    assert calls == [("m.p1a", "m.p1b")]
    assert _canonical_of(v, "m.p1a") == _canonical_of(v, "m.p1b")  # P merged (yes)
    # Q is beyond the cap → escalate: left DISTINCT (a curator dedupe proposal), never a
    # silent LLM merge.
    assert _canonical_of(v, "m.p2a") != _canonical_of(v, "m.p2b")
    q = next(x for x in v if "m.p2a" in x.members)
    assert q.verdict == "escalate"


def test_adjudicate_min_score_floor_defers_low_band():
    cands = _two_band_pairs()
    calls = []

    def adj(a, b):
        calls.append(tuple(sorted((a.ref, b.ref))))
        return (True, "same")

    # Floor at 0.85: only P (~0.88) clears it; Q (~0.78) is below the floor → deferred.
    v = er.run_er(
        cands, backend=similarity.InProcessCosineBackend(), vectors=_TWO_BAND_VECS,
        adjudicator=adj, adjudicate_max_pairs=10, adjudicate_min_score=0.85,
    )
    assert calls == [("m.p1a", "m.p1b")]
    assert _canonical_of(v, "m.p1a") == _canonical_of(v, "m.p1b")
    q = next(x for x in v if "m.p2a" in x.members)
    assert q.verdict == "escalate"


def test_adjudicate_concurrency_matches_sequential():
    cands = _two_band_pairs()

    def adj(a, b):
        return (True, "same")

    def _summary(vs):
        return sorted((tuple(x.members), x.verdict, x.method) for x in vs)

    seq = er.run_er(
        cands, backend=similarity.InProcessCosineBackend(), vectors=_TWO_BAND_VECS,
        adjudicator=adj, adjudicate_max_pairs=10, adjudicate_min_score=er.ESCALATE_LOW,
        adjudicate_max_workers=1,
    )
    conc = er.run_er(
        cands, backend=similarity.InProcessCosineBackend(), vectors=_TWO_BAND_VECS,
        adjudicator=adj, adjudicate_max_pairs=10, adjudicate_min_score=er.ESCALATE_LOW,
        adjudicate_max_workers=4,
    )
    # Same written verdicts regardless of worker count (run_bounded preserves order).
    assert _summary(seq) == _summary(conc)


# ── Backend selection / degrade parity ──────────────────────────────────────


def test_default_backend_is_in_process():
    assert isinstance(similarity.get_similarity_backend(None), similarity.InProcessCosineBackend)

    class _S:
        lakebase_search_enabled = False

    assert isinstance(similarity.get_similarity_backend(_S()), similarity.InProcessCosineBackend)


def test_fake_lakebase_backend_parity_with_in_process():
    cands = [
        C("m.net_revenue", "measure", "net revenue", "net revenue"),
        C("m.rev_after_disc", "measure", "revenue after discount", "revenue after discount"),
        C("m.order_revenue", "measure", "order_revenue", "order_revenue"),
        C("m.orders_revenue", "measure", "orders_revenue", "orders_revenue"),
    ]
    vecs = {"m.net_revenue": [1.0, 0.0], "m.rev_after_disc": [0.97, 0.24]}
    in_proc = er.run_er(cands, backend=similarity.InProcessCosineBackend(), vectors=vecs, adjudicator=None)
    fake_lb = er.run_er(
        cands,
        backend=similarity.LakebaseSearchBackend(similarity.InProcessLakebaseExecutor()),
        vectors=vecs,
        adjudicator=None,
    )
    shape = lambda vs: sorted((v.members, v.verdict, v.method, round(v.score, 4)) for v in vs)
    assert shape(in_proc) == shape(fake_lb)


# ── Map-not-merge across bounded contexts (MV-D60) ──────────────────────────


def test_same_name_different_context_stays_distinct_and_maps():
    # "customer" in two contexts is the same real-world noun in different roles — it
    # must NOT collapse to one canonical entity; instead a same-as correspondence maps it.
    cands = [
        C("sales.customer", "tag", "customer", "customer", context="Sales"),
        C("support.customer", "tag", "customer", "customer", context="Support"),
    ]
    corr: list = []
    v = er.run_er(cands, backend=similarity.InProcessCosineBackend(), vectors={},
                  adjudicator=None, correspondences=corr)
    # Two distinct canonical entities (kept distinct), not one merged group.
    assert _canonical_of(v, "sales.customer") != _canonical_of(v, "support.customer")
    # A typed same-as correspondence was recorded instead of a merge.
    assert len(corr) == 1
    c = corr[0]
    assert c.relation == "same-as" and {c.context_a, c.context_b} == {"Sales", "Support"}


def test_same_context_still_merges_exact_duplicates():
    # Within ONE context, exact duplicates still merge (map-not-merge is cross-context).
    cands = [
        C("sales.customer", "tag", "customer", "customer", context="Sales"),
        C("sales.customers", "tag", "customer", "customer", context="Sales"),
    ]
    corr: list = []
    v = er.run_er(cands, backend=similarity.InProcessCosineBackend(), vectors={},
                  adjudicator=None, correspondences=corr)
    assert _canonical_of(v, "sales.customer") == _canonical_of(v, "sales.customers")
    assert corr == []  # no cross-context map — same context


def test_contextless_candidates_are_byte_identical_to_before():
    # The default (no context) path is unchanged: exact-name dupes merge as always.
    cands = [
        C("m.order_revenue", "measure", "order_revenue", "order_revenue"),
        C("m.orders_revenue", "measure", "orders_revenue", "orders_revenue"),
    ]
    v = er.run_er(cands, backend=similarity.InProcessCosineBackend(), vectors={}, adjudicator=None)
    assert len(v) == 1 and v[0].verdict == "merge"


# ── PII firewall ─────────────────────────────────────────────────────────────


def test_pii_reject_matches_pii_tokens_and_shapes():
    assert er.pii_reject("customer_ssn")
    assert er.pii_reject("email_address")
    assert er.pii_reject("user@example.com")
    assert er.pii_reject("ssn_123_45_6789") or er.pii_reject("123-45-6789")
    assert not er.pii_reject("revenue")
    assert not er.pii_reject("Finance/Tax")


def test_pii_echoing_tag_never_enters_identity_map():
    cands = [
        C("customer_ssn", "tag", "customer_ssn", "customer_ssn"),
        C("Finance", "tag", "Finance", "Finance"),
    ]
    v = er.run_er(cands, backend=similarity.InProcessCosineBackend(), vectors={}, adjudicator=None)
    all_members = {m for x in v for m in x.members}
    assert "customer_ssn" not in all_members
    assert "Finance" in all_members
