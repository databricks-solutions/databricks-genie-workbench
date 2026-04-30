from __future__ import annotations

from unittest.mock import patch


def test_preflight_synthesis_returns_accepted_examples(monkeypatch):
    from genie_space_optimizer.optimization import preflight_synthesis as ps

    proposal = {
        "patch_type": "add_example_sql",
        "example_question": "Show stores by region",
        "example_sql": "SELECT region, COUNT(*) FROM cat.sch.stores GROUP BY region",
        "usage_guidance": "Use for regional store counts.",
    }

    class _Gate:
        def __init__(self, gate: str, passed: bool, reason: str = ""):
            self.gate = gate
            self.passed = passed
            self.reason = reason

    monkeypatch.setattr(
        ps,
        "plan_asset_coverage",
        lambda metadata_snapshot, need, rng=None: [
            (
                ps.ARCHETYPES[0],
                ps.AssetSlice(tables=[{"identifier": "cat.sch.stores"}]),
            )
        ],
    )
    monkeypatch.setattr(
        ps,
        "synthesize_preflight_candidate",
        lambda *args, **kwargs: dict(proposal),
    )
    monkeypatch.setattr(
        ps,
        "validate_synthesis_proposal",
        lambda *args, **kwargs: (True, [_Gate("parse", True), _Gate("execute", True), _Gate("structural", True), _Gate("arbiter", True), _Gate("firewall", True)]),
    )
    monkeypatch.setattr(
        ps,
        "_apply_preflight_proposals",
        lambda proposals, **kwargs: {"applied_count": len(proposals), "applied_examples": proposals},
    )

    result = ps.run_preflight_example_synthesis(
        w=None,
        spark=None,
        run_id="r1",
        space_id="s1",
        config={"_parsed_space": {"instructions": {"example_question_sqls": []}}},
        metadata_snapshot={
            "instructions": {"example_question_sqls": []},
            "data_sources": {"tables": [{"identifier": "cat.sch.stores", "column_configs": []}]},
        },
        benchmarks=[],
        catalog="cat",
        schema="sch",
        target=1,
    )

    assert result["applied"] == 1
    assert result["accepted_examples"] == [proposal]
