"""Unit tests for Signal Authority Stage 1 (popularity, MV-D93).

Three concerns:
  1. ``usage.normalize_usage`` — the pure percentile-rank normalizer (honest-gap,
     deterministic, values in [0, 1], zero-read tables omitted).
  2. The blend lift — a proposal whose anchor now carries a usage entry has
     ``factors.usage.present == true`` and a HIGHER ``evidence_coverage`` than the same
     proposal without usage, so its tier lifts where earned. ``rank.blend`` /
     ``FACTOR_WEIGHTS`` / ``coverage_cap`` are UNCHANGED — this only feeds a better input.
  3. Degrade-not-hang — a raising/parse-failing reader path yields {} at the
     ``materialize._gather_usage`` wheel boundary (the job reader itself is notebook
     source and not importable; it degrades through ``_rows_safe`` → [] → {}).
"""

from __future__ import annotations

from genie_space_optimizer.ontology import materialize, rank, usage


# ── 1. The pure normalizer ──────────────────────────────────────────────────

def test_normalize_usage_percentile_sorted_in_unit_range():
    rows = [
        {"fqn": "c.s.busy", "reads": 1000, "users": 40},
        {"fqn": "c.s.mid", "reads": 50, "users": 8},
        {"fqn": "c.s.quiet", "reads": 2, "users": 1},
    ]
    out = usage.normalize_usage(rows)

    # A value for every read table, all in [0, 1].
    assert set(out) == {"c.s.busy", "c.s.mid", "c.s.quiet"}
    assert all(0.0 <= v <= 1.0 for v in out.values())
    # Busiest at the top percentile, least-busy at the bottom (percentile rank).
    assert out["c.s.busy"] == 1.0
    assert out["c.s.quiet"] == 0.0
    assert out["c.s.quiet"] < out["c.s.mid"] < out["c.s.busy"]
    # Sorted by fqn (deterministic output).
    assert list(out) == sorted(out)


def test_normalize_usage_omits_zero_read_tables_honest_gap():
    """A table with zero reads is ABSENT (never scored 0.0) — the honest-gap discipline."""
    rows = [
        {"fqn": "c.s.busy", "reads": 500, "users": 20},
        {"fqn": "c.s.dormant", "reads": 0, "users": 0},
    ]
    out = usage.normalize_usage(rows)
    assert "c.s.dormant" not in out           # omitted, not 0.0
    assert out == {"c.s.busy": 1.0}           # a lone read table is the busiest ⇒ 1.0


def test_normalize_usage_breadth_lifts_a_widely_touched_table():
    """User breadth is a real term in the demand blend: with equal reads, the table more
    people touch ranks higher."""
    out = usage.normalize_usage([
        {"fqn": "c.s.wide", "reads": 100, "users": 50},
        {"fqn": "c.s.narrow", "reads": 100, "users": 1},
    ])
    assert out["c.s.wide"] > out["c.s.narrow"]


def test_normalize_usage_ties_share_percentile_and_is_deterministic():
    rows = [
        {"fqn": "c.s.a", "reads": 10, "users": 3},
        {"fqn": "c.s.b", "reads": 10, "users": 3},  # identical demand → tie
        {"fqn": "c.s.c", "reads": 1000, "users": 30},
    ]
    out1 = usage.normalize_usage(rows)
    out2 = usage.normalize_usage(list(reversed(rows)))  # order of input must not matter
    assert out1 == out2
    assert out1["c.s.a"] == out1["c.s.b"]       # ties share the same percentile
    assert out1["c.s.c"] == 1.0


def test_normalize_usage_empty_and_malformed_input_degrades_to_empty():
    assert usage.normalize_usage([]) == {}
    assert usage.normalize_usage(()) == {}
    # All zero-read ⇒ nothing survives ⇒ {}.
    assert usage.normalize_usage([{"fqn": "c.s.x", "reads": 0, "users": 0}]) == {}
    # Unparseable reads / missing fqn are dropped, never faked to 0.0.
    assert usage.normalize_usage([{"fqn": "", "reads": 5}]) == {}
    assert usage.normalize_usage([{"fqn": "c.s.x", "reads": "nan-ish"}]) == {}


# ── 2. The blend lift (rank.blend UNCHANGED — usage only feeds a better input) ──

def test_usage_entry_lifts_coverage_and_tier_where_earned():
    """A proposal anchored on a table that now carries a usage entry gains
    ``factors.usage.present`` and a higher ``evidence_coverage`` — governance(1.0) +
    centrality(0.9) alone caps at MEDIUM (coverage 0.60); adding usage(1.0) reaches full
    coverage (1.0) and HIGH. Proves the tier lifts purely from the new input."""
    assets = ["c.s.anchor"]
    gov_and_centrality = rank.RankSignals(
        centrality={"c.s.anchor": 0.9},
        governance={"c.s.anchor": "governed"},
    )
    # The usage value comes from the REAL normalizer (the anchor is the busiest table).
    usage_map = usage.normalize_usage([
        {"fqn": "c.s.anchor", "reads": 1000, "users": 40},
        {"fqn": "c.s.other", "reads": 1, "users": 1},
    ])
    with_usage = rank.RankSignals(
        usage=usage_map,
        centrality={"c.s.anchor": 0.9},
        governance={"c.s.anchor": "governed"},
    )

    before = rank.blend(assets, gov_and_centrality)
    after = rank.blend(assets, with_usage)

    # The usage factor flips from absent → present.
    assert before["factors"]["usage"]["present"] is False
    assert after["factors"]["usage"]["present"] is True
    assert usage_map["c.s.anchor"] == 1.0

    # Coverage rises (0.60 → 1.0) — usage joins governance + centrality.
    assert before["evidence_coverage"] == 0.6
    assert after["evidence_coverage"] == 1.0

    # …and the tier lifts where earned (coverage cap no longer holds it down).
    order = ("low", "medium", "high")
    assert before["tier"] == "medium"          # capped by 0.60 coverage
    assert after["tier"] == "high"
    assert order.index(after["tier"]) > order.index(before["tier"])


# ── 3. Degrade-not-hang at the wheel boundary ──────────────────────────────

def test_gather_usage_degrades_to_empty_when_reader_raises():
    """A reader whose ``usage_signals`` raises degrades to {} — never propagates, never a
    zero map (MV-D43). This is the same channel the job reader uses (``_rows_safe`` → [] →
    normalize_usage([]) → {}), asserted at the importable wheel boundary."""
    class _Boom:
        def usage_signals(self, allowlist):
            raise RuntimeError("no lineage grant")

    assert materialize._gather_usage(_Boom(), ["c"]) == {}


def test_gather_usage_empty_when_reader_returns_empty():
    """The default (no lineage grants / empty allowlist) path: {} = today's bytes."""
    class _Empty:
        def usage_signals(self, allowlist):
            return {}

    assert materialize._gather_usage(_Empty(), []) == {}
    assert materialize._gather_usage(_Empty(), ["c"]) == {}
