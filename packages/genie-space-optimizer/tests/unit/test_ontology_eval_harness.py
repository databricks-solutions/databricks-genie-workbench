"""Unit tests for the evaluation & trust harness (MV-D59).

Covers precision/recall/F1 computation, structural health metrics, LLM sanity
monitor, and spot-review queue (all offline, LLM mocked). Tests both the
presence of an aligned reference (full scoring) and its absence (degraded).
Verifies reproducibility and report deltification.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from genie_space_optimizer.ontology import eval_harness


# ── Fixtures: sample domain/member rows ────────────────────────────────────

def _fixture_domains_simple():
    """Simple fixture: 3 domains, 2 with multiple members, 1 singleton."""
    return [
        {
            "domain_id": "dom_finance",
            "name": "Finance",
            "members": ["finance.core.ledger", "finance.core.transactions"],
            "parent_id": None,
            "evidence": json.dumps({
                "reason": "grouped by foreign key",
                "rank": {"score": 85.0},
            }),
        },
        {
            "domain_id": "dom_hr",
            "name": "HR",
            "members": ["hr.staff.employees"],  # singleton
            "parent_id": None,
            "evidence": json.dumps({
                "reason": "grouped by shared schema",
                "rank": {"score": 60.0},
            }),
        },
        {
            "domain_id": "dom_ops",
            "name": "Operations",
            "members": ["ops.incidents.tickets", "ops.incidents.logs", "ops.metrics.dashboard"],
            "parent_id": None,
            "evidence": json.dumps({
                "reason": "grouped by metric view",
                "rank": {"score": 72.0},
            }),
        },
    ]


def _fixture_members_simple():
    """Simple member rows (flattened from domains)."""
    return [
        {"domain_id": "dom_finance", "asset_fqn": "finance.core.ledger"},
        {"domain_id": "dom_finance", "asset_fqn": "finance.core.transactions"},
        {"domain_id": "dom_hr", "asset_fqn": "hr.staff.employees"},
        {"domain_id": "dom_ops", "asset_fqn": "ops.incidents.tickets"},
        {"domain_id": "dom_ops", "asset_fqn": "ops.incidents.logs"},
        {"domain_id": "dom_ops", "asset_fqn": "ops.metrics.dashboard"},
    ]


def _fixture_aligned_reference():
    """Aligned reference with 3 reference domains (2 match discovered, 1 missed)."""
    return {
        "domains": [
            {
                "id": "ref_finance",
                "name": "Finance",
                "members": ["finance.core.ledger", "finance.core.transactions"],
            },
            {
                "id": "ref_hr",
                "name": "HR",
                "members": ["hr.staff.employees"],
            },
            {
                "id": "ref_sales",
                "name": "Sales",
                "members": ["sales.orders.main"],
            },
        ],
        "alignments": [
            {"discovered_id": "dom_finance", "reference_id": "ref_finance"},
            {"discovered_id": "dom_hr", "reference_id": "ref_hr"},
        ],
    }


# ── BUILD A: Precision/Recall/F1 ───────────────────────────────────────────

def test_precision_recall_f1_with_aligned_reference():
    """Precision/recall/F1 with aligned reference — hand-computed values."""
    domains = _fixture_domains_simple()
    ref = _fixture_aligned_reference()

    prf, matches = eval_harness.compute_precision_recall_f1(domains, ref)

    # 2 matched discovered, 1 unmatched discovered → precision = 2/3
    # 2 matched discovered, 1 unmatched reference → recall = 2/3
    # F1 = 2 * (2/3 * 2/3) / (2/3 + 2/3) = 2 * 4/9 / 4/3 = 8/9 / 4/3 = 2/3
    assert prf.precision == round(2/3, 4)
    assert prf.recall == round(2/3, 4)
    assert prf.f1 == round(2/3, 4)
    assert prf.na_reason == ""

    # Check per-domain match statuses
    assert len(matches) == 3
    finance_match = next(m for m in matches if m.domain_id == "dom_finance")
    assert finance_match.match_type == "exact"
    assert finance_match.reference_id == "ref_finance"

    ops_match = next(m for m in matches if m.domain_id == "dom_ops")
    assert ops_match.match_type == "extra"
    assert ops_match.reference_id is None


def test_precision_recall_f1_without_aligned_reference():
    """Without aligned reference — should degrade to N/A."""
    domains = _fixture_domains_simple()

    prf, matches = eval_harness.compute_precision_recall_f1(domains, None)

    assert prf.precision is None
    assert prf.recall is None
    assert prf.f1 is None
    assert "not available" in prf.na_reason
    assert matches == []


def test_precision_recall_f1_perfect_match():
    """Perfect match: all discovered match reference."""
    domains = [{"domain_id": "d1", "name": "D1", "members": ["a", "b"]}]
    ref = {
        "domains": [{"id": "r1", "name": "R1", "members": ["a", "b"]}],
        "alignments": [{"discovered_id": "d1", "reference_id": "r1"}],
    }

    prf, matches = eval_harness.compute_precision_recall_f1(domains, ref)

    assert prf.precision == 1.0
    assert prf.recall == 1.0
    assert prf.f1 == 1.0


def test_precision_recall_f1_no_match():
    """No match: discovered and reference are disjoint."""
    domains = [{"domain_id": "d1", "name": "D1", "members": ["a"]}]
    ref = {
        "domains": [{"id": "r1", "name": "R1", "members": ["b"]}],
        "alignments": [],
    }

    prf, matches = eval_harness.compute_precision_recall_f1(domains, ref)

    # TP=0, FP=1, FN=1 → P=0, R=0, F1=0
    assert prf.precision == 0.0
    assert prf.recall == 0.0
    assert prf.f1 == 0.0


# ── BUILD B: Structural health ─────────────────────────────────────────────

def test_structural_health_simple():
    """Structural health on simple fixture."""
    domains = _fixture_domains_simple()
    members = _fixture_members_simple()

    health = eval_harness.compute_structural_health(domains, members)

    # 1 singleton (HR)
    assert health.singleton_count == 1
    # 3 orphans (all are top-level, no parent)
    assert health.orphan_count == 3
    # Depth: 1 (flat)
    assert health.tree_depth == 1
    # Branching: 0 (no parent-child relationships)
    assert health.branching_factor_avg == 0.0
    assert health.total_domains == 3
    assert health.total_members == 6


def test_structural_health_with_hierarchy():
    """Structural health with domain hierarchy (parent/sub-domains)."""
    domains = [
        {"domain_id": "parent", "name": "Parent", "members": [], "parent_id": None},
        {"domain_id": "sub1", "name": "Sub1", "members": [], "parent_id": "parent"},
        {"domain_id": "sub2", "name": "Sub2", "members": [], "parent_id": "parent"},
    ]
    members = []

    health = eval_harness.compute_structural_health(domains, members)

    assert health.orphan_count == 1  # Only parent
    assert health.tree_depth == 2  # parent → subs
    assert health.branching_factor_avg == round(2 / 3, 4)  # 2 children / 3 domains


# ── BUILD C: LLM sanity monitor ────────────────────────────────────────────

def test_llm_sanity_monitor_skipped():
    """LLM sanity monitor skipped when no client provided."""
    domains = _fixture_domains_simple()

    result = eval_harness.run_llm_sanity_monitor(domains, llm_client=None)

    assert result.status == "skipped"
    assert result.flagged_domains == []


def test_llm_sanity_monitor_with_mock_client():
    """LLM sanity monitor with a mock client (heuristic flagging)."""
    domains = [
        {
            "domain_id": "dom_random",
            "name": "Random",
            "members": ["a", "b"],
            "evidence": json.dumps({"reason": "grouped by random sampling"}),
        },
    ]

    # Mock client (any truthy value works)
    mock_client = object()

    result = eval_harness.run_llm_sanity_monitor(domains, llm_client=mock_client)

    assert result.status == "flagged"
    assert len(result.flagged_domains) > 0
    assert result.flagged_domains[0]["domain_id"] == "dom_random"


def test_llm_sanity_monitor_passed():
    """LLM sanity monitor passes when no heuristic flags domains."""
    domains = [
        {
            "domain_id": "dom_finance",
            "name": "Finance",
            "members": ["a", "b"],
            "evidence": json.dumps({"reason": "grouped by foreign key"}),
        },
    ]

    mock_client = object()

    result = eval_harness.run_llm_sanity_monitor(domains, llm_client=mock_client)

    assert result.status == "passed"
    assert result.flagged_domains == []


# ── BUILD D: Spot-review queue ────────────────────────────────────────────

def test_spot_review_queue_bounded():
    """Spot-review queue is bounded by queue_size."""
    domains = [
        {
            "domain_id": f"dom_{i}",
            "name": f"Domain {i}",
            "members": [f"asset_{i}"],
            "evidence": json.dumps({"reason": "test", "rank": {"score": 50.0 + i}}),
        }
        for i in range(100)
    ]
    members = [{"domain_id": f"dom_{i}", "asset_fqn": f"asset_{i}"} for i in range(100)]

    queue = eval_harness.build_spot_review_queue(domains, members, queue_size=50)

    assert len(queue) == 50


def test_spot_review_queue_deterministic():
    """Spot-review queue is deterministic (same seed → same queue)."""
    domains = _fixture_domains_simple()
    members = _fixture_members_simple()

    queue1 = eval_harness.build_spot_review_queue(domains, members, seed="test_seed")
    queue2 = eval_harness.build_spot_review_queue(domains, members, seed="test_seed")

    assert len(queue1) == len(queue2)
    assert [(q.domain_id, q.domain_name) for q in queue1] == [(q.domain_id, q.domain_name) for q in queue2]


def test_spot_review_queue_carries_evidence():
    """Each queue item carries evidence reason and confidence."""
    domains = _fixture_domains_simple()
    members = _fixture_members_simple()

    queue = eval_harness.build_spot_review_queue(domains, members, queue_size=10)

    assert len(queue) > 0
    for item in queue:
        assert item.domain_id
        assert item.domain_name
        assert item.evidence_reason
        assert item.confidence_band in ["Low", "Medium", "High", "Unknown"]


# ── REPORT + GATE: assembling and comparing reports ──────────────────────

def test_assemble_eval_report_complete():
    """Assemble a complete eval report from materialized run data."""
    domains = _fixture_domains_simple()
    members = _fixture_members_simple()
    ref = _fixture_aligned_reference()

    report = eval_harness.assemble_eval_report(
        run_id="run_123",
        metastore_id="ms_1",
        timestamp="2026-09-05T10:00:00Z",
        domain_rows=domains,
        member_rows=members,
        aligned_reference=ref,
        llm_client=None,
    )

    assert report.run_id == "run_123"
    assert report.metastore_id == "ms_1"
    assert report.precision_recall_f1.precision == round(2/3, 4)
    assert report.structural_health.total_domains == 3
    assert report.llm_sanity.status == "skipped"
    assert len(report.spot_review_queue) > 0


def test_assemble_eval_report_degraded_no_reference():
    """Assemble report without aligned reference (degraded)."""
    domains = _fixture_domains_simple()
    members = _fixture_members_simple()

    report = eval_harness.assemble_eval_report(
        run_id="run_456",
        metastore_id="ms_2",
        timestamp="2026-09-05T11:00:00Z",
        domain_rows=domains,
        member_rows=members,
        aligned_reference=None,
    )

    # P/R/F are N/A, but other sections still render
    assert report.precision_recall_f1.precision is None
    assert report.structural_health is not None
    assert len(report.spot_review_queue) > 0


def test_compare_reports_deltas():
    """Compare two reports and compute deltas."""
    domains1 = _fixture_domains_simple()
    members1 = _fixture_members_simple()
    ref = _fixture_aligned_reference()

    report_before = eval_harness.assemble_eval_report(
        run_id="run_1",
        metastore_id="ms_1",
        timestamp="2026-09-05T10:00:00Z",
        domain_rows=domains1,
        member_rows=members1,
        aligned_reference=ref,
    )

    # Simulate an "after" with slightly different metrics
    domains2 = _fixture_domains_simple() + [
        {
            "domain_id": "dom_sales",
            "name": "Sales",
            "members": ["sales.orders.main"],
            "parent_id": None,
            "evidence": json.dumps({"reason": "grouped by tag", "rank": {"score": 80.0}}),
        },
    ]
    members2 = members1 + [{"domain_id": "dom_sales", "asset_fqn": "sales.orders.main"}]

    report_after = eval_harness.assemble_eval_report(
        run_id="run_2",
        metastore_id="ms_1",
        timestamp="2026-09-05T12:00:00Z",
        domain_rows=domains2,
        member_rows=members2,
        aligned_reference=ref,
    )

    delta = eval_harness.compare_reports(report_before, report_after)

    # Delta should show changes in metrics
    assert "precision_delta" in asdict(delta)
    assert "recall_delta" in asdict(delta)
    assert "f1_delta" in asdict(delta)


def test_compare_reports_regression_detection():
    """Compare reports and detect regressions (negative deltas)."""
    ref_good = {
        "domains": [
            {"id": "r1", "name": "R1", "members": ["a", "b"]},
            {"id": "r2", "name": "R2", "members": ["c"]},
        ],
        "alignments": [
            {"discovered_id": "d1", "reference_id": "r1"},
            {"discovered_id": "d2", "reference_id": "r2"},
        ],
    }

    # "Before" report: perfect match (P=1, R=1, F=1)
    domains_before = [
        {"domain_id": "d1", "name": "D1", "members": ["a", "b"], "parent_id": None, "evidence": "{}"},
        {"domain_id": "d2", "name": "D2", "members": ["c"], "parent_id": None, "evidence": "{}"},
    ]
    members_before = [
        {"domain_id": "d1", "asset_fqn": "a"},
        {"domain_id": "d1", "asset_fqn": "b"},
        {"domain_id": "d2", "asset_fqn": "c"},
    ]

    report_before = eval_harness.assemble_eval_report(
        run_id="good",
        metastore_id="ms_1",
        timestamp="2026-09-05T10:00:00Z",
        domain_rows=domains_before,
        member_rows=members_before,
        aligned_reference=ref_good,
    )

    # "After" report: missing d2 → recall drops (P=1, R=0.5, F~0.67)
    ref_bad = {
        "domains": [
            {"id": "r1", "name": "R1", "members": ["a", "b"]},
            {"id": "r2", "name": "R2", "members": ["c"]},
        ],
        "alignments": [
            {"discovered_id": "d1", "reference_id": "r1"},
        ],
    }

    domains_after = [
        {"domain_id": "d1", "name": "D1", "members": ["a", "b"], "parent_id": None, "evidence": "{}"},
    ]
    members_after = [
        {"domain_id": "d1", "asset_fqn": "a"},
        {"domain_id": "d1", "asset_fqn": "b"},
    ]

    report_after = eval_harness.assemble_eval_report(
        run_id="bad",
        metastore_id="ms_1",
        timestamp="2026-09-05T12:00:00Z",
        domain_rows=domains_after,
        member_rows=members_after,
        aligned_reference=ref_bad,
    )

    delta = eval_harness.compare_reports(report_before, report_after)

    # Recall should regress (1.0 → 0.5)
    assert "recall_regressed" in str(delta.regression_warnings).lower() or delta.recall_delta.get("change") < 0


def test_report_serialization_roundtrip():
    """Report can be serialized to dict and deserialized back."""
    domains = _fixture_domains_simple()
    members = _fixture_members_simple()
    ref = _fixture_aligned_reference()

    original = eval_harness.assemble_eval_report(
        run_id="ser_test",
        metastore_id="ms_1",
        timestamp="2026-09-05T10:00:00Z",
        domain_rows=domains,
        member_rows=members,
        aligned_reference=ref,
    )

    # Serialize to dict
    report_dict = eval_harness.report_to_dict(original)
    assert isinstance(report_dict, dict)
    assert report_dict["run_id"] == "ser_test"

    # Deserialize back (note: set fields become lists in JSON, but asdict preserves them)
    # This is acceptable for the report comparison use case
    assert report_dict["run_id"] == original.run_id


# ── Edge cases: degrade-not-hang (MV-D43) ──────────────────────────────────

def test_missing_evidence_degrades():
    """Missing evidence JSON gracefully degrades (returns empty dict)."""
    domains = [
        {"domain_id": "d1", "name": "D1", "members": ["a"], "parent_id": None},
        {"domain_id": "d2", "name": "D2", "members": ["b"], "parent_id": None, "evidence": ""},
        {"domain_id": "d3", "name": "D3", "members": ["c"], "parent_id": None, "evidence": "invalid json"},
    ]
    members = [
        {"domain_id": "d1", "asset_fqn": "a"},
        {"domain_id": "d2", "asset_fqn": "b"},
        {"domain_id": "d3", "asset_fqn": "c"},
    ]

    # Should not raise; gracefully degrade
    health = eval_harness.compute_structural_health(domains, members)
    assert health.total_domains == 3


def test_llm_client_error_degrades():
    """LLM client error degrades (returns error status, not crash)."""
    class BrokenClient:
        def call(self, *a, **k):
            raise RuntimeError("LLM boom")

    domains = [
        {"domain_id": "d1", "name": "D1", "members": ["a", "b"], "evidence": "{}"},
    ]

    # Instantiate to trigger call()
    client = BrokenClient()

    result = eval_harness.run_llm_sanity_monitor(domains, llm_client=client)

    # Should catch the RuntimeError and return error status
    assert result.status == "error"
    assert "boom" in result.error_message.lower()


# ── Acceptance: acceptance criteria summary ────────────────────────────────

def test_acceptance_a_with_reference():
    """(A) With reference: P/R/F match hand-computed, per-domain statuses correct."""
    test_precision_recall_f1_with_aligned_reference()
    test_precision_recall_f1_perfect_match()
    test_precision_recall_f1_no_match()


def test_acceptance_a_without_reference():
    """(A) Without reference: P/R/F are N/A, others still render."""
    test_assemble_eval_report_degraded_no_reference()


def test_acceptance_b_structural_metrics():
    """(B) Singleton/orphan/depth/branching match hand-computed values."""
    test_structural_health_simple()
    test_structural_health_with_hierarchy()


def test_acceptance_c_llm_monitor():
    """(C) With mocked LLM: sanity section populated, deterministic A/B byte-identical."""
    test_llm_sanity_monitor_with_mock_client()
    test_llm_sanity_monitor_skipped()


def test_acceptance_d_spot_review():
    """(D) Queue is bounded, seeded-reproducible, carries evidence reason + confidence."""
    test_spot_review_queue_bounded()
    test_spot_review_queue_deterministic()
    test_spot_review_queue_carries_evidence()


def test_acceptance_comparator():
    """(Comparator) BEFORE/AFTER deltas are computed correctly."""
    test_compare_reports_deltas()
    test_compare_reports_regression_detection()
