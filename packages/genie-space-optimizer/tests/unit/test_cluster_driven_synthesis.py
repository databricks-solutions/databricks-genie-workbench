"""Tests for cluster-driven example SQL synthesis (Bug #4 Phase 3).

Covers the 6 sequencing steps from the plan:

- ClusterContext + planner (6 tests)
- Orchestrator (``run_cluster_driven_synthesis_for_single_cluster``) (5 tests)
- Lever 5 intercept inside ``generate_proposals_from_strategy`` (4 tests)
- Leak safety (3 tests)
- Observability + provenance (2 tests)

All tests are LLM/SDK-free — ``llm_caller``, ``validate_synthesis_proposal``,
``_gate_genie_agreement`` are injected or patched.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from genie_space_optimizer.common.config import (
    CLUSTER_SYNTHESIS_PER_ITERATION,
    EXAMPLE_QUESTION_SQLS_SAFETY_CAP,
)
from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    ClusterContext,
    _derive_asset_slice_from_afs,
    _find_matching_join_spec,
    _resolve_asset_by_identifier,
    render_afs_block,
    render_cluster_driven_prompt,
    run_cluster_driven_synthesis_for_single_cluster,
)
from genie_space_optimizer.optimization.preflight_synthesis import (
    AssetSlice,
    SynthesisContext,
)
from genie_space_optimizer.optimization.synthesis import GateResult


# ═══════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════


def _mk_table(identifier: str, columns: list[dict] | None = None) -> dict:
    return {
        "identifier": identifier,
        "name": identifier.split(".")[-1],
        "column_configs": columns or [
            {"column_name": "id", "type_text": "BIGINT", "description": "PK"},
            {"column_name": "amount", "type_text": "DECIMAL"},
            {"column_name": "region", "type_text": "STRING"},
            {"column_name": "sale_date", "type_text": "DATE"},
        ],
    }


def _mk_join(left_id: str, right_id: str) -> dict:
    return {
        "left": {"identifier": left_id, "alias": left_id.split(".")[-1]},
        "right": {"identifier": right_id, "alias": right_id.split(".")[-1]},
        "sql": [
            f"`{left_id.split('.')[-1]}`.`id` = `{right_id.split('.')[-1]}`.`id`",
        ],
    }


def _mk_snapshot(
    existing_examples: int = 0,
    *,
    with_join: bool = True,
    space_id: str = "SP123",
) -> dict:
    tables = [
        _mk_table("cat.sch.fact_sales"),
        _mk_table(
            "cat.sch.dim_region",
            columns=[
                {"column_name": "id", "type_text": "STRING"},
                {"column_name": "region_name", "type_text": "STRING"},
            ],
        ),
    ]
    joins = [_mk_join("cat.sch.fact_sales", "cat.sch.dim_region")] if with_join else []
    examples = [
        {"question": f"Q{i}", "sql": f"SELECT {i} FROM cat.sch.fact_sales"}
        for i in range(existing_examples)
    ]
    return {
        "data_sources": {"tables": tables, "metric_views": []},
        "instructions": {
            "join_specs": joins,
            "example_question_sqls": examples,
        },
        "tables": tables,
        "metric_views": [],
        "_failure_clusters": [],
        "_space_id": space_id,
        "_cluster_synthesis_count": 0,
    }


def _mk_cluster(
    cluster_id: str = "C1",
    root_cause: str = "missing_aggregation",
    blame: list[str] | None = None,
) -> dict:
    """Cluster in the raw shape ``format_afs`` accepts."""
    return {
        "cluster_id": cluster_id,
        "root_cause": root_cause,
        "affected_judge": "answer_correctness",
        "asi_blame_set": blame or ["cat.sch.fact_sales"],
        "question_ids": ["q1", "q2"],
    }


def _fake_llm(prompt: str) -> str:
    """Returns a JSON proposal that passes the structural/parse gates."""
    return (
        '{"example_question": "What are total sales by region?", '
        '"example_sql": "SELECT cat.sch.fact_sales.region, '
        'SUM(cat.sch.fact_sales.amount) FROM cat.sch.fact_sales '
        'GROUP BY 1 ORDER BY 2 DESC LIMIT 10", '
        '"rationale": "Top-N aggregation demo", '
        '"usage_guidance": "Use when grouping by region"}'
    )


def _all_gates_pass(*_args, **_kwargs):
    return True, [
        GateResult(True, "parse"),
        GateResult(True, "execute"),
        GateResult(True, "structural"),
        GateResult(True, "arbiter", "skipped_no_arbiter"),
        GateResult(True, "firewall"),
    ]


def _gate_fails_at(gate: str):
    def _stub(*_args, **_kwargs):
        results = []
        order = ["parse", "execute", "structural", "arbiter", "firewall"]
        for name in order:
            if name == gate:
                results.append(GateResult(False, name, f"{gate} failed"))
                return False, results
            results.append(GateResult(True, name))
        return True, results
    return _stub


def _genie_agreement_passes(*_args, **_kwargs):
    return GateResult(True, "genie_agreement", "both_correct")


def _genie_agreement_fails(*_args, **_kwargs):
    return GateResult(False, "genie_agreement", "disagreement")


# ═══════════════════════════════════════════════════════════════════════
# 1. ClusterContext + planner (6 tests)
# ═══════════════════════════════════════════════════════════════════════


class TestClusterContextAndPlanner:
    def test_cluster_context_conforms_to_synthesis_context_protocol(self):
        """ClusterContext must satisfy the structural SynthesisContext protocol
        so the synthesis engine can accept it anywhere AssetSlice works."""
        ctx = ClusterContext(
            afs={"cluster_id": "C1"},
            asset_slice=AssetSlice(tables=[_mk_table("cat.sch.fact_sales")]),
        )
        assert isinstance(ctx, SynthesisContext)
        assert "cat.sch.fact_sales" in ctx.to_identifier_allowlist()
        assert "cat.sch.fact_sales" in ctx.asset_ids()

    def test_resolve_asset_by_fq_and_short_identifier(self):
        snap = _mk_snapshot()
        fq = _resolve_asset_by_identifier(snap, "cat.sch.fact_sales")
        short = _resolve_asset_by_identifier(snap, "fact_sales")
        assert fq is not None and short is not None
        assert fq["identifier"] == short["identifier"] == "cat.sch.fact_sales"
        assert _resolve_asset_by_identifier(snap, "nonexistent") is None

    def test_find_matching_join_spec_order_insensitive(self):
        snap = _mk_snapshot(with_join=True)
        js = _find_matching_join_spec(
            snap, "cat.sch.fact_sales", "cat.sch.dim_region",
        )
        assert js is not None
        js_rev = _find_matching_join_spec(
            snap, "cat.sch.dim_region", "cat.sch.fact_sales",
        )
        assert js_rev is not None
        assert _find_matching_join_spec(snap, "cat.sch.fact_sales", "cat.sch.nonexistent") is None

    def test_derive_slice_single_table(self):
        from genie_space_optimizer.optimization.afs import format_afs
        cluster = _mk_cluster(
            root_cause="missing_aggregation",
            blame=["cat.sch.fact_sales"],
        )
        afs = format_afs(cluster)
        result = _derive_asset_slice_from_afs(afs, _mk_snapshot())
        assert result is not None
        slice_, archetype = result
        assert len(slice_.tables) == 1
        assert slice_.tables[0]["identifier"] == "cat.sch.fact_sales"
        assert slice_.join_spec is None
        assert archetype is not None

    def test_derive_slice_two_tables_with_matching_join_spec(self):
        from genie_space_optimizer.optimization.afs import format_afs
        cluster = _mk_cluster(
            root_cause="wrong_join",
            blame=["cat.sch.fact_sales", "cat.sch.dim_region"],
        )
        afs = format_afs(cluster)
        result = _derive_asset_slice_from_afs(afs, _mk_snapshot(with_join=True))
        assert result is not None
        slice_, _ = result
        assert len(slice_.tables) == 2
        assert slice_.join_spec is not None

    def test_derive_slice_missing_join_spec_falls_back_to_single_table(self):
        """Invariant D — the critical planner branch. Two-table blame_set
        + no join_spec should *not* yield None: the planner retries with a
        single-table view if a non-JOIN archetype matches the reduced AFS.
        """
        from genie_space_optimizer.optimization.afs import format_afs
        cluster = _mk_cluster(
            # missing_filter matches FILTER archetype (no has_joinable
            # requirement), so the fallback should succeed.
            root_cause="missing_filter",
            blame=["cat.sch.fact_sales", "cat.sch.dim_region"],
        )
        afs = format_afs(cluster)
        result = _derive_asset_slice_from_afs(
            afs, _mk_snapshot(with_join=False),
        )
        assert result is not None, (
            "missing-join-spec fallback must find a single-table archetype"
        )
        slice_, _ = result
        assert len(slice_.tables) == 1, "fallback should reduce to a single table"


# ═══════════════════════════════════════════════════════════════════════
# 2. Orchestrator (5 tests)
# ═══════════════════════════════════════════════════════════════════════


class TestOrchestrator:
    def test_safety_cap_short_circuits_before_llm(self):
        """Invariant: safety cap is checked first — no budget bump, no LLM."""
        snap = _mk_snapshot(existing_examples=EXAMPLE_QUESTION_SQLS_SAFETY_CAP)
        calls: list[str] = []

        def tracking_llm(prompt: str) -> str:
            calls.append(prompt)
            return _fake_llm(prompt)

        synthesis = run_cluster_driven_synthesis_for_single_cluster(
            _mk_cluster(), snap, benchmarks=[], llm_caller=tracking_llm,
        )
        result = synthesis.proposal
        assert result is None
        assert calls == []
        assert snap["_cluster_synthesis_count"] == 0

    def test_budget_cap_short_circuits_before_llm(self):
        """Invariant C — per-iteration budget is enforced before any LLM call."""
        snap = _mk_snapshot()
        snap["_cluster_synthesis_count"] = CLUSTER_SYNTHESIS_PER_ITERATION
        calls: list[str] = []

        def tracking_llm(prompt: str) -> str:
            calls.append(prompt)
            return _fake_llm(prompt)

        synthesis = run_cluster_driven_synthesis_for_single_cluster(
            _mk_cluster(), snap, benchmarks=[], llm_caller=tracking_llm,
        )
        result = synthesis.proposal
        assert result is None
        assert calls == []
        assert snap["_cluster_synthesis_count"] == CLUSTER_SYNTHESIS_PER_ITERATION

    def test_successful_path_returns_proposal_with_provenance_fields(self):
        snap = _mk_snapshot()
        with patch(
            "genie_space_optimizer.optimization.cluster_driven_synthesis.validate_synthesis_proposal",
            side_effect=_all_gates_pass,
        ), patch(
            "genie_space_optimizer.optimization.preflight_synthesis._gate_genie_agreement",
            side_effect=_genie_agreement_passes,
        ):
            synthesis = run_cluster_driven_synthesis_for_single_cluster(
                _mk_cluster(), snap, benchmarks=[], llm_caller=_fake_llm,
            )
        result = synthesis.proposal
        assert result is not None
        assert result["patch_type"] == "add_example_sql"
        assert result["example_question"]
        assert result["example_sql"]
        assert result["_archetype_name"]
        assert result["_cluster_id"] == "C1"
        assert snap["_cluster_synthesis_count"] == 1

    def test_gate_failure_returns_none_and_bumps_budget(self):
        """Budget is bumped even on gate failure — we consumed an LLM call."""
        snap = _mk_snapshot()
        with patch(
            "genie_space_optimizer.optimization.cluster_driven_synthesis.validate_synthesis_proposal",
            side_effect=_gate_fails_at("execute"),
        ):
            synthesis = run_cluster_driven_synthesis_for_single_cluster(
                _mk_cluster(), snap, benchmarks=[], llm_caller=_fake_llm,
            )
        result = synthesis.proposal
        assert result is None
        assert snap["_cluster_synthesis_count"] == 1

    def test_missing_space_id_fails_closed(self):
        """Invariant B — no space_id means no Genie check; fail-closed."""
        snap = _mk_snapshot(space_id="")
        snap["_space_id"] = ""
        with patch(
            "genie_space_optimizer.optimization.cluster_driven_synthesis.validate_synthesis_proposal",
            side_effect=_all_gates_pass,
        ):
            synthesis = run_cluster_driven_synthesis_for_single_cluster(
                _mk_cluster(), snap, benchmarks=[], llm_caller=_fake_llm,
            )
        result = synthesis.proposal
        assert result is None

    def test_successful_path_returns_teaching_kit_supporting_proposals(self):
        snap = _mk_snapshot()

        def fake_kit_llm(prompt: str) -> str:
            return (
                '{"kit_summary": "Teach grain and term mapping", '
                '"example_sql": {'
                '"example_question": "Which regions have the highest sales?", '
                '"example_sql": "SELECT region, SUM(amount) FROM cat.sch.fact_sales GROUP BY region", '
                '"usage_guidance": "Use for regional aggregation."'
                '}, '
                '"supporting_changes": ['
                '{"patch_type": "add_column_synonym", '
                '"table": "cat.sch.fact_sales", "column": "region", '
                '"synonyms": ["sales region"]}, '
                '{"patch_type": "add_sql_snippet_measure", '
                '"display_name": "Total Sales", "sql": "SUM(amount)", '
                '"instruction": "Use for total sales.", '
                '"target_table": "cat.sch.fact_sales"}'
                ']}'
            )

        with patch(
            "genie_space_optimizer.optimization.cluster_driven_synthesis.validate_synthesis_proposal",
            side_effect=_all_gates_pass,
        ), patch(
            "genie_space_optimizer.optimization.preflight_synthesis._gate_genie_agreement",
            side_effect=_genie_agreement_passes,
        ), patch(
            "genie_space_optimizer.optimization.cluster_driven_synthesis._validate_supporting_sql_snippet",
            side_effect=lambda proposal, **kwargs: {
                **proposal,
                "validation_passed": True,
                "sql_snippet": {
                    "id": "snip1",
                    "name": proposal["display_name"],
                    "sql": proposal["sql"],
                },
            },
        ):
            synthesis = run_cluster_driven_synthesis_for_single_cluster(
                _mk_cluster(),
                snap,
                benchmarks=[],
                llm_caller=fake_kit_llm,
            )

        result = synthesis.proposal
        assert result is not None
        assert result["patch_type"] == "add_example_sql"
        assert result["kit_id"].startswith("kit_C1_")
        supporting = result["_supporting_proposals"]
        assert [p["patch_type"] for p in supporting] == [
            "add_column_synonym",
            "add_sql_snippet_measure",
        ]
        assert all(p["kit_id"] == result["kit_id"] for p in supporting)
        assert supporting[1]["validation_passed"] is True


# ═══════════════════════════════════════════════════════════════════════
# 3. Lever 5 intercept (4 tests)
# ═══════════════════════════════════════════════════════════════════════


class TestLever5Intercept:
    def test_resolve_source_cluster_picks_first_listed_cluster(self):
        """Helper returns the first source cluster that resolves to an
        archetype. Post Phase 1.R4 the ``simple_enumerate`` safety-net
        archetype makes every root cause archetype-eligible, so the
        resolver simply honours ``source_cluster_ids`` ordering."""
        from genie_space_optimizer.optimization.optimizer import (
            _resolve_source_cluster_for_ag,
        )
        snap = _mk_snapshot()
        cluster_bad = {
            "cluster_id": "CB",
            "root_cause": "terminology_mismatch",
            "asi_blame_set": [],
            "question_ids": [],
        }
        cluster_good = _mk_cluster("CG", "missing_aggregation")
        snap["_failure_clusters"] = [cluster_bad, cluster_good]
        ag = {"source_cluster_ids": ["CB", "CG"]}
        picked = _resolve_source_cluster_for_ag(ag, snap)
        assert picked is not None
        # CB is listed first and is eligible via simple_enumerate.
        assert picked["cluster_id"] == "CB"

    def test_resolve_source_cluster_returns_safety_net_match(self):
        """Post Phase 1.R4: ``simple_enumerate`` makes any root cause
        archetype-eligible, so even a previously-unhandled cluster
        (``terminology_mismatch``) resolves. This is the intended
        trait-detector regression safety net."""
        from genie_space_optimizer.optimization.optimizer import (
            _resolve_source_cluster_for_ag,
        )
        snap = _mk_snapshot()
        cluster = {
            "cluster_id": "CB",
            "root_cause": "terminology_mismatch",
            "asi_blame_set": [],
            "question_ids": [],
        }
        snap["_failure_clusters"] = [cluster]
        picked = _resolve_source_cluster_for_ag(
            {"source_cluster_ids": ["CB", "CX"]}, snap,
        )
        assert picked is not None
        assert picked["cluster_id"] == "CB"

    def test_feature_flag_is_read_lazily_at_call_site(self):
        """The Lever 5 intercept reads ``ENABLE_CLUSTER_DRIVEN_SYNTHESIS``
        via a local ``from config import`` inside the function body (not
        at module import). This guarantees that flipping the env var at
        runtime (or patching the config constant in a test) takes effect
        on the very next proposal iteration — no reimport required.

        Structural test: the config module exposes the flag, and the
        optimizer module does NOT bind it at import time.
        """
        from genie_space_optimizer.common import config as cfg
        from genie_space_optimizer.optimization import optimizer as opt
        assert hasattr(cfg, "ENABLE_CLUSTER_DRIVEN_SYNTHESIS")
        # Optimizer must NOT have cached the flag at import — call-site
        # reads only. If this assertion changes, the lazy-read
        # invariant has been broken and the env-var kill-switch no
        # longer works without a full module reimport.
        assert not hasattr(opt, "ENABLE_CLUSTER_DRIVEN_SYNTHESIS"), (
            "optimizer.py must read ENABLE_CLUSTER_DRIVEN_SYNTHESIS lazily "
            "at call site, not cache it at import time"
        )

    def test_lever5_fallback_shape_when_synthesis_returns_none(self):
        """Tier 2.9: fallback emits ``add_instruction`` only when the AFS
        carries actionable counterfactual fixes (not bare metadata like
        ``failure_type: missing_aggregation``).

        - Without counterfactuals → returns None (no diagnostic-prose
          patches leak into Genie Space text_instructions).
        - With counterfactuals → emits ``add_instruction`` whose body
          leads with the actionable guidance.
        """
        from genie_space_optimizer.optimization.synthesis import (
            instruction_only_fallback,
        )
        from genie_space_optimizer.optimization.afs import format_afs

        # Case 1: no counterfactuals — fallback must decline (Tier 2.9).
        cluster = _mk_cluster("CF", "missing_aggregation")
        fb = instruction_only_fallback(format_afs(cluster))
        assert fb is None

        # Case 2: SQL-shape counterfactuals present — under the new
        # instruction publishability contract, the fallback declines for
        # SQL-shape failures (``missing_aggregation`` here). Counterfactual
        # text is optimizer-facing and must not enter ``text_instructions``.
        afs_with_fix = format_afs(cluster)
        afs_with_fix["counterfactual_fixes"] = [
            "Aggregate sales by region using SUM(amount) and GROUP BY region",
        ]
        fb2 = instruction_only_fallback(afs_with_fix)
        assert fb2 is None

    def test_teaching_kit_primary_proposal_carries_target_qids_and_kit_provenance(self, monkeypatch):
        from genie_space_optimizer.optimization import optimizer
        from genie_space_optimizer.optimization.cluster_driven_synthesis import (
            ClusterSynthesisResult,
        )

        snap = _mk_snapshot()
        snap["_failure_clusters"] = [_mk_cluster("C1", "missing_aggregation")]

        def fake_synthesis(*_args, **_kwargs):
            return ClusterSynthesisResult(
                proposal={
                    "patch_type": "add_example_sql",
                    "example_question": "Which regions have the highest sales?",
                    "example_sql": "SELECT region, SUM(amount) FROM cat.sch.fact_sales GROUP BY region",
                    "usage_guidance": "Use for regional aggregation.",
                    "_archetype_name": "simple_group_by",
                    "_cluster_id": "C1",
                    "kit_id": "kit_C1_1",
                    "target_qids": ["q1"],
                    "rca_id": "rca_agg",
                    "_supporting_proposals": [],
                },
                attempted_archetypes=("simple_group_by",),
            )

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.cluster_driven_synthesis.run_cluster_driven_synthesis_for_single_cluster",
            fake_synthesis,
        )

        proposals = optimizer.generate_proposals_from_strategy(
            strategy={},
            action_group={
                "id": "AG1",
                "root_cause_summary": "missing aggregation",
                "affected_questions": ["q1"],
                "source_cluster_ids": ["C1"],
                "lever_directives": {"5": {"example_sqls": [{"intent": "teach aggregation"}]}},
            },
            metadata_snapshot=snap,
            target_lever=5,
            apply_mode="genie_config",
            benchmarks=[],
        )

        primary = next(p for p in proposals if p["patch_type"] == "add_example_sql")
        assert primary["kit_id"] == "kit_C1_1"
        assert primary["target_qids"] == ["q1"]
        assert primary["rca_id"] == "rca_agg"
        assert primary["provenance"]["kit_id"] == "kit_C1_1"
        assert primary["provenance"]["target_qids"] == ["q1"]

    def test_lever5_intercept_appends_teaching_kit_supporting_proposals(self, monkeypatch):
        from genie_space_optimizer.optimization import optimizer
        from genie_space_optimizer.optimization.cluster_driven_synthesis import (
            ClusterSynthesisResult,
        )

        snap = _mk_snapshot()
        snap["_failure_clusters"] = [_mk_cluster("C1", "missing_aggregation")]

        def fake_synthesis(*_args, **_kwargs):
            return ClusterSynthesisResult(
                proposal={
                    "patch_type": "add_example_sql",
                    "example_question": "Which regions have the highest sales?",
                    "example_sql": "SELECT region, SUM(amount) FROM cat.sch.fact_sales GROUP BY region",
                    "usage_guidance": "Use for regional aggregation.",
                    "_archetype_name": "simple_group_by",
                    "_cluster_id": "C1",
                    "kit_id": "kit_C1_1",
                    "target_qids": ["q1", "q2"],
                    "_supporting_proposals": [
                        {
                            "patch_type": "add_column_synonym",
                            "table": "cat.sch.fact_sales",
                            "table_id": "cat.sch.fact_sales",
                            "column": "region",
                            "column_name": "region",
                            "synonyms": ["sales region"],
                            "kit_id": "kit_C1_1",
                            "target_qids": ["q1", "q2"],
                        }
                    ],
                },
                attempted_archetypes=("simple_group_by",),
            )

        monkeypatch.setattr(
            "genie_space_optimizer.optimization.cluster_driven_synthesis.run_cluster_driven_synthesis_for_single_cluster",
            fake_synthesis,
        )

        proposals = optimizer.generate_proposals_from_strategy(
            strategy={},
            action_group={
                "id": "AG1",
                "root_cause_summary": "missing aggregation",
                "affected_questions": ["q1", "q2"],
                "source_cluster_ids": ["C1"],
                "lever_directives": {
                    "5": {"example_sqls": [{"intent": "teach aggregation"}]},
                },
            },
            metadata_snapshot=snap,
            target_lever=5,
            apply_mode="genie_config",
            benchmarks=[],
        )

        assert any(p["patch_type"] == "add_example_sql" for p in proposals)
        support = [p for p in proposals if p["patch_type"] == "add_column_synonym"]
        assert len(support) == 1
        assert support[0]["kit_id"] == "kit_C1_1"
        assert support[0]["target_qids"] == ["q1", "q2"]
        assert support[0]["provenance"]["synthesis_source"] == "cluster_driven_teaching_kit"


# ═══════════════════════════════════════════════════════════════════════
# 4. Leak safety (3 tests)
# ═══════════════════════════════════════════════════════════════════════


class TestLeakSafety:
    def test_preflight_prompt_bytes_equivalent_without_afs(self):
        """Byte-equivalence contract: the pre-flight render path must
        produce identical bytes when the AFS block is absent. This
        guards against future PRs that would inadvertently inject AFS
        scaffolding into the pre-flight template."""
        from genie_space_optimizer.optimization.preflight_synthesis import (
            render_preflight_prompt,
        )
        from genie_space_optimizer.optimization.archetypes import ARCHETYPES
        slice_ = AssetSlice(tables=[_mk_table("cat.sch.fact_sales")])
        pre = render_preflight_prompt(ARCHETYPES[0], slice_, ["existing Q?"])
        # Render again via the cluster wrapper with an EMPTY AFS — must
        # delegate straight through to the pre-flight render.
        ctx = ClusterContext(afs={}, asset_slice=slice_)
        via_cluster = render_cluster_driven_prompt(
            ARCHETYPES[0], ctx, ["existing Q?"],
        )
        assert via_cluster == pre

    def test_regression_mining_hints_prepended_when_provided(self):
        """When the harness threads non-empty mining hints through
        ``ClusterContext.regression_mining_hints``, the rendered prompt
        must include them above the pre-flight prompt body. Empty
        hints preserve byte-equivalence (covered by the test above)."""
        from genie_space_optimizer.optimization.archetypes import ARCHETYPES
        slice_ = AssetSlice(tables=[_mk_table("cat.sch.fact_sales")])
        hints = (
            "## Lessons from rolled-back iterations (regression mining)\n"
            "- column_confusion in WHERE: prefer `is_month_to_date` over "
            "`use_mtdate_flag`; contrastive fix → update_column_description."
        )
        ctx = ClusterContext(
            afs={}, asset_slice=slice_,
            regression_mining_hints=hints,
        )
        rendered = render_cluster_driven_prompt(
            ARCHETYPES[0], ctx, ["existing Q?"],
        )
        assert "Lessons from rolled-back iterations" in rendered
        assert "is_month_to_date" in rendered
        assert "use_mtdate_flag" in rendered

    def test_regression_mining_hints_default_empty_preserves_legacy_path(self):
        """The default ``regression_mining_hints=""`` MUST yield the same
        bytes as a freshly-constructed context without the field. Guards
        against accidental regressions when the strategist input flag is
        off (the default)."""
        from genie_space_optimizer.optimization.preflight_synthesis import (
            render_preflight_prompt,
        )
        from genie_space_optimizer.optimization.archetypes import ARCHETYPES
        slice_ = AssetSlice(tables=[_mk_table("cat.sch.fact_sales")])
        pre = render_preflight_prompt(ARCHETYPES[0], slice_, ["existing Q?"])

        ctx_default = ClusterContext(afs={}, asset_slice=slice_)
        ctx_empty = ClusterContext(
            afs={}, asset_slice=slice_, regression_mining_hints="",
        )
        ctx_whitespace = ClusterContext(
            afs={}, asset_slice=slice_, regression_mining_hints="   \n   ",
        )
        for ctx in (ctx_default, ctx_empty, ctx_whitespace):
            assert render_cluster_driven_prompt(
                ARCHETYPES[0], ctx, ["existing Q?"],
            ) == pre

    def test_afs_leak_validation_rejects_pass_through(self):
        """validate_afs must reject AFS fields that echo benchmark text."""
        from genie_space_optimizer.optimization.afs import (
            validate_afs, AFSLeakError,
        )
        from genie_space_optimizer.optimization.leakage import BenchmarkCorpus
        corpus = BenchmarkCorpus.from_benchmarks([
            {"question": "What are total sales by region in Q1 2024 please",
             "expected_sql": "SELECT 1"},
        ])
        bad_afs = {
            "cluster_id": "C1",
            "failure_type": "unknown",
            "suggested_fix_summary": (
                "What are total sales by region in Q1 2024 please"
            ),
        }
        with pytest.raises(AFSLeakError):
            validate_afs(bad_afs, corpus)

    def test_afs_block_contains_no_raw_benchmark_fields(self):
        """AFS rendering touches only whitelisted fields — never raw
        question/expected_sql/generated_sql. Structural guard so a
        regressing contributor can't accidentally widen the surface."""
        afs_view = {
            "cluster_id": "C1",
            "failure_type": "wrong_join",
            "blame_set": ["cat.sch.fact_sales"],
            "suggested_fix_summary": "Prefer inner join over cross join",
            "counterfactual_fixes": ["Use join on region_id"],
            "structural_diff": {"ops": [{"op": "swap_join_kind"}]},
            # These keys would be rejected by _strip_unknown_fields in
            # format_afs — added here to guard render_afs_block from
            # accidentally picking them up.
            "question": "SHOULD NOT APPEAR",
            "expected_sql": "SHOULD NOT APPEAR",
            "generated_sql": "SHOULD NOT APPEAR",
        }
        block = render_afs_block(afs_view)
        assert "SHOULD NOT APPEAR" not in block

    def test_failure_context_block_includes_genie_sql_not_expected_sql(self):
        from genie_space_optimizer.optimization.archetypes import ARCHETYPES

        slice_ = AssetSlice(tables=[_mk_table("cat.sch.fact_sales")])
        ctx = ClusterContext(
            afs={"cluster_id": "C1", "failure_type": "wrong_grouping"},
            asset_slice=slice_,
            failure_contexts=[
                {
                    "question_id": "q_001",
                    "generated_sql": "SELECT store_id, SUM(amount) FROM cat.sch.fact_sales GROUP BY store_id",
                    "root_cause": "wrong_grouping",
                    "failed_judges": ["schema_accuracy"],
                    "blame_set": ["zone_vp_name"],
                    "counterfactual_fixes": ["Group by zone_vp_name instead of store_id."],
                    "rationales": ["The generated SQL used the wrong grain."],
                    "arbiter_verdict": "ground_truth_correct",
                }
            ],
        )

        from genie_space_optimizer.optimization.cluster_driven_synthesis import (
            render_failure_context_block,
        )

        rendered = render_cluster_driven_prompt(
            ARCHETYPES[0],
            ctx,
            ["existing Q?"],
        )
        block = render_failure_context_block(ctx.failure_contexts)

        assert "## RCA failure evidence" in rendered
        assert "Genie generated SQL" in rendered
        assert "SELECT store_id" in rendered
        assert "Group by zone_vp_name" in rendered
        assert "expected_sql" not in rendered
        # Failure block itself must not echo benchmark question phrasing.
        assert "benchmark question" not in block.lower()


# ═══════════════════════════════════════════════════════════════════════
# 5. Observability + provenance (2 tests)
# ═══════════════════════════════════════════════════════════════════════


class TestObservability:
    def test_success_logs_synthesis_summary(self, caplog):
        """Structured log line must fire on applied outcome."""
        snap = _mk_snapshot()
        with caplog.at_level(
            logging.INFO,
            logger="genie_space_optimizer.optimization.cluster_driven_synthesis",
        ), patch(
            "genie_space_optimizer.optimization.cluster_driven_synthesis.validate_synthesis_proposal",
            side_effect=_all_gates_pass,
        ), patch(
            "genie_space_optimizer.optimization.preflight_synthesis._gate_genie_agreement",
            side_effect=_genie_agreement_passes,
        ):
            synthesis = run_cluster_driven_synthesis_for_single_cluster(
                _mk_cluster(), snap, benchmarks=[], llm_caller=_fake_llm,
            )
        result = synthesis.proposal
        assert result is not None
        summary_lines = [
            r.message for r in caplog.records
            if r.message.startswith("synthesis.summary")
        ]
        assert summary_lines, "orchestrator must emit synthesis.summary"
        assert any("trigger=cluster" in m for m in summary_lines)
        assert any("outcome=applied" in m for m in summary_lines)

    def test_gate_failure_logs_skipped_reason(self, caplog):
        snap = _mk_snapshot()
        with caplog.at_level(
            logging.INFO,
            logger="genie_space_optimizer.optimization.cluster_driven_synthesis",
        ), patch(
            "genie_space_optimizer.optimization.cluster_driven_synthesis.validate_synthesis_proposal",
            side_effect=_gate_fails_at("execute"),
        ):
            synthesis = run_cluster_driven_synthesis_for_single_cluster(
                _mk_cluster(), snap, benchmarks=[], llm_caller=_fake_llm,
            )
        result = synthesis.proposal
        assert result is None
        summary_lines = [
            r.message for r in caplog.records
            if r.message.startswith("synthesis.summary")
        ]
        assert any("outcome=gate_fail" in m for m in summary_lines)
        assert any("skipped_reason=gate:execute" in m for m in summary_lines)


class TestHardening:
    """Plan 2026-05-17-cluster-driven-example-synthesis-hardening — only
    the production-correctness fixes from this plan are tested here.
    The heavyweight typed-IO + content + observability tasks are
    deferred (the prompt is dormant in production)."""

    def test_lever_5b_cluster_driven_max_tokens_constant_exists_and_is_sized(self):
        """Task 4: max_tokens=1000 cap for cluster-driven synthesis."""
        from genie_space_optimizer.common import config

        assert hasattr(config, "LEVER_5B_CLUSTER_DRIVEN_MAX_TOKENS")
        assert isinstance(config.LEVER_5B_CLUSTER_DRIVEN_MAX_TOKENS, int)
        assert 600 <= config.LEVER_5B_CLUSTER_DRIVEN_MAX_TOKENS <= 1500

    def test_cluster_driven_link_prompt_to_trace_uses_correct_key(self):
        """Task 7: the cluster-driven call site MUST link to the
        'cluster_driven_example_synthesis' registry key — NOT the
        legacy 'lever_5b_example_sql' mis-key.
        """
        import pathlib

        src = (
            pathlib.Path(__file__).resolve().parents[2]
            / "src" / "genie_space_optimizer" / "optimization"
            / "cluster_driven_synthesis.py"
        )
        body = src.read_text()
        # The mis-key must be gone
        assert '_link_prompt_to_trace("lever_5b_example_sql")' not in body, (
            "cluster_driven_synthesis.py still has the legacy mis-key — "
            "Task 7 of the hardening plan should have replaced it."
        )
        # And the correct key must be present, at least once
        assert '_link_prompt_to_trace("cluster_driven_example_synthesis")' in body, (
            "cluster_driven_synthesis.py must link to "
            "'cluster_driven_example_synthesis' (its actual registry name)."
        )

    def test_cluster_driven_is_in_typed_output_deferred_allowlist(self):
        """The prompt's typed-output contract is deferred (the kit
        shape requires a TeachingKitOutput Pydantic model that hasn't
        been built yet — Task 1 of the hardening plan)."""
        from tests.unit.optimization.test_prompt_registry_inventory import (
            TYPED_OUTPUT_DEFERRED_ALLOWLIST,
        )

        assert "cluster_driven_example_synthesis" in TYPED_OUTPUT_DEFERRED_ALLOWLIST

    # ── Tasks 1 + 5: TeachingKitOutput Pydantic model + call-site wiring ──

    def test_teaching_kit_output_model_exists(self):
        """Task 1: TeachingKitOutput must exist with nested _KitExampleSql
        and _SupportingChange and be exported from prompt_io."""
        from genie_space_optimizer.optimization import prompt_io as pio

        assert hasattr(pio, "TeachingKitOutput"), (
            "Task 1: TeachingKitOutput must be defined in prompt_io.py"
        )
        cls = pio.TeachingKitOutput
        fields = cls.model_fields
        assert "kit_summary" in fields
        assert "example_sql" in fields
        assert "supporting_changes" in fields

    def test_teaching_kit_output_parses_valid_payload(self):
        """The model must accept the four-field kit shape the prompt
        emits — including discriminated supporting_changes variants."""
        from genie_space_optimizer.optimization.prompt_io import TeachingKitOutput

        payload = {
            "kit_summary": "Teach segmentation by tier when filters fail",
            "example_sql": {
                "example_question": "How many orders per tier last month?",
                "example_sql": (
                    "SELECT c.tier, COUNT(*) FROM cat.sch.orders o "
                    "INNER JOIN cat.sch.customers c "
                    "ON o.customer_id = c.customer_id GROUP BY c.tier"
                ),
                "usage_guidance": "When user asks for per-tier order counts.",
            },
            "supporting_changes": [
                {
                    "patch_type": "add_instruction",
                    "section_name": "QUERY CONSTRUCTION",
                    "new_text": "Join orders to customers on customer_id.",
                },
                {
                    "patch_type": "add_column_synonym",
                    "table": "cat.sch.customers",
                    "column": "tier",
                    "synonyms": ["membership level", "loyalty tier"],
                },
                {
                    "patch_type": "add_sql_snippet_measure",
                    "display_name": "order_count",
                    "sql": "COUNT(*)",
                    "instruction": "Use for total order volume.",
                    "target_table": "cat.sch.orders",
                    "synonyms": ["orders", "purchases"],
                },
            ],
        }
        kit = TeachingKitOutput.model_validate(payload)
        assert kit.kit_summary.startswith("Teach segmentation")
        assert kit.example_sql.example_question.startswith("How many")
        assert len(kit.supporting_changes) == 3
        assert kit.supporting_changes[0].patch_type == "add_instruction"

    def test_teaching_kit_output_response_format_is_databricks_safe(self):
        """``build_response_format(TeachingKitOutput)`` must produce a
        payload that Databricks FM APIs accept — no anyOf/oneOf/pattern
        in the schema. Supporting-change variants are modeled via
        nullable-variant-fields so the kit_summary + example_sql
        nesting stays type-safe."""
        from genie_space_optimizer.optimization.prompt_io import (
            TeachingKitOutput,
            build_response_format,
        )

        rf = build_response_format(TeachingKitOutput)
        import json as _json

        body = _json.dumps(rf)
        # Match JSON keywords (as quoted keys), not free text inside descriptions.
        for forbidden in ('"anyOf":', '"oneOf":', '"allOf":', '"pattern":', '"$ref":'):
            assert forbidden not in body, (
                f"response_format contains FM-API-forbidden keyword {forbidden!r}"
            )

    def test_cluster_driven_call_sites_wire_teaching_kit_output(self):
        """Task 5: both primary and retry cluster-driven LLM call sites
        must wire ``response_model=TeachingKitOutput`` — not the legacy
        ``Lever5bExampleSqlOutput`` stopgap that ships today."""
        import pathlib

        src = (
            pathlib.Path(__file__).resolve().parents[2]
            / "src" / "genie_space_optimizer" / "optimization"
            / "cluster_driven_synthesis.py"
        )
        body = src.read_text()
        # The legacy stopgap must be gone from cluster-driven.
        assert "response_model=Lever5bExampleSqlOutput" not in body, (
            "Task 5: cluster_driven_synthesis.py still wires the legacy "
            "Lever5bExampleSqlOutput — replace with TeachingKitOutput."
        )
        # The correct contract must appear at the call sites — once for
        # the primary call, once for the retry call.
        assert body.count("response_model=TeachingKitOutput") >= 2, (
            "Task 5: TeachingKitOutput must be wired at both the primary "
            "and retry cluster-driven LLM call sites."
        )
