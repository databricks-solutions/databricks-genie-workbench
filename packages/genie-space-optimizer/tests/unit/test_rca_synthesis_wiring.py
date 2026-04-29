from __future__ import annotations

from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme


def test_rca_example_synthesis_request_omits_benchmark_question_and_expected_sql():
    from genie_space_optimizer.optimization.synthesis import afs_from_rca_theme

    theme = RcaPatchTheme(
        rca_id="rca_shape",
        rca_kind=RcaKind.EXAMPLE_SQL_SHAPE_NEEDED,
        patch_family="example_sql_shape_guidance",
        patches=(
            {
                "type": "request_example_sql_synthesis",
                "lever": 5,
                "root_cause": "wide_vs_long_shape",
                "blame_set": ["calendar_month", "gross_sales"],
                "intent": "synthesize original non-benchmark example SQL",
            },
        ),
        target_qids=("eval_q_123",),
        touched_objects=("calendar_month", "gross_sales"),
        confidence=0.84,
        evidence_summary=(
            "counterfactual_fix=Use calendar_month and gross_sales; "
            "expected_sql=SELECT calendar_month, SUM(gross_sales) FROM secret_eval"
        ),
    )

    afs = afs_from_rca_theme(theme)

    assert afs["cluster_id"] == "rca_shape"
    assert afs["failure_type"] == "wide_vs_long_shape"
    assert afs["blame_set"] == ["calendar_month", "gross_sales"]
    assert "eval_q_123" not in str(afs)
    assert "secret_eval" not in str(afs)
    assert "expected_sql" not in str(afs)


def test_rca_example_synthesis_uses_cluster_driven_engine(monkeypatch):
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    calls = {"cluster_driven": 0}

    def fake_cluster_driven(cluster, metadata_snapshot, **kwargs):
        calls["cluster_driven"] += 1
        assert cluster["cluster_id"] == "rca_shape"
        assert cluster["root_cause"] == "wrong_grouping"
        assert cluster["question_ids"] == ["q1"]
        assert cluster["rca_id"] == "rca_shape"
        return {
            "patch_type": "add_example_sql",
            "example_question": "Show sales by category",
            "example_sql": "SELECT category, SUM(sales) FROM cat.sch.orders GROUP BY category",
            "usage_guidance": "Use for category aggregations.",
            "_archetype_name": "simple_group_by",
            "_cluster_id": "rca_shape",
            "kit_id": "kit_rca_shape_1",
            "target_qids": ["q1"],
            "_supporting_proposals": [],
        }

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.cluster_driven_synthesis.run_cluster_driven_synthesis_for_single_cluster",
        fake_cluster_driven,
    )
    monkeypatch.setattr(optimizer, "ENABLE_RCA_EXAMPLE_SQL_SYNTHESIS", True)
    # Stub validator: the fake cluster-driven proposal references
    # ``cat.sch.orders`` which is not in the empty snapshot, so the
    # downstream Lever-5 identifier firewall would otherwise reject it.
    monkeypatch.setattr(
        optimizer,
        "_validate_lever5_proposals",
        lambda proposals, *_a, **_kw: proposals,
    )

    theme = RcaPatchTheme(
        rca_id="rca_shape",
        rca_kind=RcaKind.EXAMPLE_SQL_SHAPE_NEEDED,
        patch_family="example_sql_shape_guidance",
        patches=(
            {
                "type": "request_example_sql_synthesis",
                "lever": 5,
                "root_cause": "wrong_grouping",
                "blame_set": ["cat.sch.orders", "category", "sales"],
            },
        ),
        target_qids=("q1",),
        touched_objects=("category", "sales"),
    )

    proposals = optimizer.generate_proposals_from_strategy(
        strategy={},
        action_group={
            "id": "AG_RCA",
            "root_cause_summary": "wrong grouping",
            "affected_questions": ["q1"],
            "source_cluster_ids": [],
            "lever_directives": {},
        },
        metadata_snapshot={
            "_rca_themes": [theme],
            "instructions": {},
            "data_sources": {"tables": [], "metric_views": []},
            "_space_id": "SP123",
        },
        target_lever=5,
        apply_mode="genie_config",
        benchmarks=[],
    )

    assert calls["cluster_driven"] == 1
    assert any(p.get("source") == "rca_teaching_kit" for p in proposals)


def test_rca_example_synthesis_flag_is_imported_by_optimizer() -> None:
    import inspect

    from genie_space_optimizer.optimization import optimizer

    src = inspect.getsource(optimizer)

    assert "ENABLE_RCA_EXAMPLE_SQL_SYNTHESIS" in src
    assert "run_cluster_driven_synthesis_for_single_cluster" in src
    assert "_cluster_from_rca_example_theme" in src


def test_rca_synthesis_requests_are_collected_from_selected_themes() -> None:
    from genie_space_optimizer.optimization.optimizer import _rca_themes_requesting_synthesis
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    themes = [
        RcaPatchTheme(
            rca_id="rca_no_synth",
            rca_kind=RcaKind.EXTRA_DEFENSIVE_FILTER,
            patch_family="avoid_unrequested_defensive_filters",
            patches=({"type": "add_instruction", "lever": 5},),
            target_qids=("q1",),
            touched_objects=("QUERY CONSTRUCTION",),
        ),
        RcaPatchTheme(
            rca_id="rca_synth",
            rca_kind=RcaKind.EXAMPLE_SQL_SHAPE_NEEDED,
            patch_family="example_sql_shape_guidance",
            patches=({"type": "request_example_sql_synthesis", "lever": 5},),
            target_qids=("q2",),
            touched_objects=("gross_sales",),
        ),
    ]

    selected = _rca_themes_requesting_synthesis(themes)

    assert [t.rca_id for t in selected] == ["rca_synth"]


def test_rca_themes_requesting_sql_snippets_filters_correctly() -> None:
    from genie_space_optimizer.optimization.optimizer import (
        _rca_themes_requesting_sql_snippets,
    )
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    themes = [
        RcaPatchTheme(
            rca_id="rca_no_snippet",
            rca_kind=RcaKind.EXTRA_DEFENSIVE_FILTER,
            patch_family="avoid_unrequested_defensive_filters",
            patches=({"type": "add_instruction", "lever": 5},),
            target_qids=("q1",),
            touched_objects=("QUERY CONSTRUCTION",),
        ),
        RcaPatchTheme(
            rca_id="rca_measure",
            rca_kind=RcaKind.MEASURE_SWAP,
            patch_family="contrastive_measure_disambiguation",
            patches=(
                {"type": "update_column_description", "lever": 1},
                {"type": "add_sql_snippet_measure", "lever": 6,
                 "snippet_type": "measure", "target_object": "gross_sales"},
            ),
            target_qids=("q2",),
            touched_objects=("gross_sales",),
        ),
        RcaPatchTheme(
            rca_id="rca_filter",
            rca_kind=RcaKind.FILTER_LOGIC_MISMATCH,
            patch_family="filter_logic_guidance",
            patches=({"type": "add_sql_snippet_filter", "lever": 6,
                      "snippet_type": "filter"},),
            target_qids=("q3",),
            touched_objects=("date_window",),
        ),
    ]

    selected = _rca_themes_requesting_sql_snippets(themes)

    assert [t.rca_id for t in selected] == ["rca_measure", "rca_filter"]


def test_rca_sql_snippet_bridge_flag_is_imported_by_optimizer() -> None:
    import inspect

    from genie_space_optimizer.optimization import optimizer

    src = inspect.getsource(optimizer)

    assert "ENABLE_RCA_SQL_SNIPPET_BRIDGE" in src
    assert "_rca_themes_requesting_sql_snippets" in src


def test_rca_sql_snippet_bridge_produces_lever6_proposal_when_flag_on(
    monkeypatch,
) -> None:
    """End-to-end bridge test with mocked LLM and validation gates."""
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    monkeypatch.setattr(optimizer, "ENABLE_RCA_SQL_SNIPPET_BRIDGE", True)

    def fake_generate(cluster, metadata_snapshot, **kwargs):
        # Distinguish strategist-driven vs RCA-bridge path so dedup
        # doesn't silently merge them. In production, two distinct
        # cluster contexts would naturally produce different SQL.
        is_rca = cluster.get("cluster_id", "").startswith("rca_")
        return {
            "patch_type": "add_sql_snippet_measure",
            "lever": 6,
            "snippet_type": "measure",
            "display_name": "Gross Sales (RCA)" if is_rca else "Gross Sales",
            "alias": "gross_sales_rca" if is_rca else "gross_sales_total",
            "sql": "SUM(gross_sales) -- rca" if is_rca else "SUM(gross_sales)",
            "synonyms": ["sales before returns"],
            "instruction": "Use for revenue before returns.",
            "target_table": "orders",
            "rationale": "RCA-derived" if is_rca else "strategist",
            "affected_questions": cluster.get("question_ids", []),
            "confidence": 0.7,
            "questions_fixed": len(cluster.get("question_traces", [])),
            "validation_passed": True,
        }

    monkeypatch.setattr(optimizer, "_generate_lever6_proposal", fake_generate)

    theme = RcaPatchTheme(
        rca_id="rca_measure",
        rca_kind=RcaKind.MEASURE_SWAP,
        patch_family="contrastive_measure_disambiguation",
        patches=(
            {
                "type": "add_sql_snippet_measure",
                "lever": 6,
                "snippet_type": "measure",
                "target_object": "orders.gross_sales",
                "intent": "define reusable measure",
            },
        ),
        target_qids=("q_measure",),
        touched_objects=("gross_sales",),
    )

    metadata_snapshot = {"_rca_themes": [theme]}
    proposals = optimizer.generate_proposals_from_strategy(
        strategy={"action_groups": []},
        action_group={
            "id": "AG1",
            "lever_directives": {"6": {}},
            "source_cluster_ids": [],
            "affected_questions": [],
            "root_cause_summary": "test",
        },
        metadata_snapshot=metadata_snapshot,
        target_lever=6,
        apply_mode="apply",
        benchmarks=[],
    )

    p = next((x for x in proposals if x.get("source") == "rca_theme_lever6"), None)
    assert p is not None, f"bridge produced no rca_theme_lever6 proposal; got {proposals}"
    assert p["lever"] == 6
    assert p["rca_id"] == "rca_measure"
    assert p["patch_family"] == "contrastive_measure_disambiguation"
    assert p["target_qids"] == ["q_measure"]
    assert p["provenance"]["synthesis_source"] == "rca_theme_lever6"
    assert p["patch_type"] == "add_sql_snippet_measure"


def test_rca_sql_snippet_bridge_no_op_when_flag_off(monkeypatch) -> None:
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    monkeypatch.setattr(optimizer, "ENABLE_RCA_SQL_SNIPPET_BRIDGE", False)

    theme = RcaPatchTheme(
        rca_id="rca_measure",
        rca_kind=RcaKind.MEASURE_SWAP,
        patch_family="contrastive_measure_disambiguation",
        patches=({"type": "add_sql_snippet_measure", "lever": 6,
                  "snippet_type": "measure"},),
        target_qids=("q_measure",),
        touched_objects=("gross_sales",),
    )

    proposals = optimizer.generate_proposals_from_strategy(
        strategy={"action_groups": []},
        action_group={
            "id": "AG1",
            "lever_directives": {"6": {}},
            "source_cluster_ids": [],
            "affected_questions": [],
            "root_cause_summary": "test",
        },
        metadata_snapshot={"_rca_themes": [theme]},
        target_lever=6,
        apply_mode="apply",
        benchmarks=[],
    )

    assert not [p for p in proposals if p.get("source") == "rca_theme_lever6"]


def test_rca_themes_requesting_join_specs_filters_correctly() -> None:
    from genie_space_optimizer.optimization.optimizer import (
        _rca_themes_requesting_join_specs,
    )
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    themes = [
        RcaPatchTheme(
            rca_id="rca_no_join",
            rca_kind=RcaKind.MEASURE_SWAP,
            patch_family="contrastive_measure_disambiguation",
            patches=({"type": "update_column_description", "lever": 1},),
            target_qids=("q1",),
            touched_objects=("gross_sales",),
        ),
        RcaPatchTheme(
            rca_id="rca_join",
            rca_kind=RcaKind.JOIN_SPEC_MISSING_OR_WRONG,
            patch_family="join_spec_guidance",
            patches=(
                {
                    "type": "add_join_spec",
                    "lever": 4,
                    "expected_objects": [
                        "orders.customer_id",
                        "customers.customer_id",
                    ],
                },
                {"type": "request_example_sql_synthesis", "lever": 5},
            ),
            target_qids=("q2",),
            touched_objects=("orders", "customers"),
        ),
    ]

    selected = _rca_themes_requesting_join_specs(themes)

    assert [t.rca_id for t in selected] == ["rca_join"]


def test_rca_join_spec_bridge_flag_is_imported_by_optimizer() -> None:
    import inspect

    from genie_space_optimizer.optimization import optimizer

    src = inspect.getsource(optimizer)

    assert "ENABLE_RCA_JOIN_SPEC_BRIDGE" in src
    assert "_rca_themes_requesting_join_specs" in src


def test_rca_join_spec_bridge_produces_lever4_proposal_when_flag_on(
    monkeypatch,
) -> None:
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    monkeypatch.setattr(optimizer, "ENABLE_RCA_JOIN_SPEC_BRIDGE", True)
    # validate_join_spec_types reads metadata; stub with a permissive validator
    # so we don't need a full schema fixture.
    monkeypatch.setattr(
        optimizer, "validate_join_spec_types",
        lambda spec, snapshot: (True, ""),
    )

    theme = RcaPatchTheme(
        rca_id="rca_join",
        rca_kind=RcaKind.JOIN_SPEC_MISSING_OR_WRONG,
        patch_family="join_spec_guidance",
        patches=(
            {
                "type": "add_join_spec",
                "lever": 4,
                "expected_objects": [
                    "orders.customer_id",
                    "customers.customer_id",
                ],
                "intent": "missing customer linkage",
            },
        ),
        target_qids=("q_join",),
        touched_objects=("orders", "customers"),
    )

    proposals = optimizer.generate_proposals_from_strategy(
        strategy={"action_groups": [], "_source_clusters": []},
        action_group={
            "id": "AG1",
            "lever_directives": {"4": {}},
            "source_cluster_ids": [],
            "affected_questions": [],
            "root_cause_summary": "test",
        },
        metadata_snapshot={"_rca_themes": [theme], "instructions": {}},
        target_lever=4,
        apply_mode="apply",
        benchmarks=[],
    )

    p = next(
        (x for x in proposals if x.get("source") == "rca_theme_lever4"),
        None,
    )
    assert p is not None, f"bridge produced no rca_theme_lever4 proposal; got {proposals}"
    assert p["lever"] == 4
    assert p["patch_type"] == "add_join_spec"
    assert p["rca_id"] == "rca_join"
    assert p["patch_family"] == "join_spec_guidance"
    assert p["target_qids"] == ["q_join"]
    assert p["join_spec"]["left"]["identifier"] == "orders"
    assert p["join_spec"]["right"]["identifier"] == "customers"


def test_rca_join_spec_bridge_no_op_when_flag_off(monkeypatch) -> None:
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    monkeypatch.setattr(optimizer, "ENABLE_RCA_JOIN_SPEC_BRIDGE", False)

    theme = RcaPatchTheme(
        rca_id="rca_join",
        rca_kind=RcaKind.JOIN_SPEC_MISSING_OR_WRONG,
        patch_family="join_spec_guidance",
        patches=({"type": "add_join_spec", "lever": 4,
                  "expected_objects": ["a.x", "b.x"]},),
        target_qids=("q",),
        touched_objects=("a", "b"),
    )

    proposals = optimizer.generate_proposals_from_strategy(
        strategy={"action_groups": [], "_source_clusters": []},
        action_group={
            "id": "AG1",
            "lever_directives": {"4": {}},
            "source_cluster_ids": [],
            "affected_questions": [],
            "root_cause_summary": "test",
        },
        metadata_snapshot={"_rca_themes": [theme], "instructions": {}},
        target_lever=4,
        apply_mode="apply",
        benchmarks=[],
    )

    assert not [p for p in proposals if p.get("source") == "rca_theme_lever4"]


def test_rca_join_spec_bridge_skips_when_fewer_than_two_qualified_objects(
    monkeypatch,
) -> None:
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    monkeypatch.setattr(optimizer, "ENABLE_RCA_JOIN_SPEC_BRIDGE", True)

    theme = RcaPatchTheme(
        rca_id="rca_join",
        rca_kind=RcaKind.JOIN_SPEC_MISSING_OR_WRONG,
        patch_family="join_spec_guidance",
        patches=({"type": "add_join_spec", "lever": 4,
                  "expected_objects": ["orders.customer_id"]},),
        target_qids=("q",),
        touched_objects=("orders",),
    )

    proposals = optimizer.generate_proposals_from_strategy(
        strategy={"action_groups": [], "_source_clusters": []},
        action_group={
            "id": "AG1",
            "lever_directives": {"4": {}},
            "source_cluster_ids": [],
            "affected_questions": [],
            "root_cause_summary": "test",
        },
        metadata_snapshot={"_rca_themes": [theme], "instructions": {}},
        target_lever=4,
        apply_mode="apply",
        benchmarks=[],
    )

    assert not [p for p in proposals if p.get("source") == "rca_theme_lever4"]


def test_rca_themes_requesting_lever1_filters_correctly() -> None:
    from genie_space_optimizer.optimization.optimizer import (
        _rca_themes_requesting_lever1,
    )
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    themes = [
        RcaPatchTheme(
            rca_id="rca_join_only",
            rca_kind=RcaKind.JOIN_SPEC_MISSING_OR_WRONG,
            patch_family="join_spec_guidance",
            patches=({"type": "add_join_spec", "lever": 4},),
            target_qids=("q1",),
            touched_objects=("orders",),
        ),
        RcaPatchTheme(
            rca_id="rca_synonym",
            rca_kind=RcaKind.SYNONYM_OR_ENTITY_MATCH_MISSING,
            patch_family="synonym_entity_matching_guidance",
            patches=({"type": "add_column_synonym", "lever": 1,
                      "table": "orders", "column": "gross_sales"},),
            target_qids=("q2",),
            touched_objects=("gross_sales",),
        ),
        RcaPatchTheme(
            rca_id="rca_mv_routing",
            rca_kind=RcaKind.METRIC_VIEW_ROUTING_CONFUSION,
            patch_family="contrastive_metric_routing",
            patches=({"type": "update_description", "lever": 1,
                      "target": "mv_gross_sales"},),
            target_qids=("q3",),
            touched_objects=("mv_gross_sales",),
        ),
    ]

    selected = _rca_themes_requesting_lever1(themes)

    assert sorted(t.rca_id for t in selected) == ["rca_mv_routing", "rca_synonym"]


def test_rca_lever1_bridge_flag_is_imported_by_optimizer() -> None:
    import inspect

    from genie_space_optimizer.optimization import optimizer

    src = inspect.getsource(optimizer)

    assert "ENABLE_RCA_LEVER1_BRIDGE" in src
    assert "_rca_themes_requesting_lever1" in src
    assert "_generate_lever1_rca_proposal" in src


def test_rca_lever1_bridge_column_path_produces_synonyms(monkeypatch) -> None:
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    monkeypatch.setattr(optimizer, "ENABLE_RCA_LEVER1_BRIDGE", True)
    monkeypatch.setattr(
        optimizer, "_traced_llm_call",
        lambda *a, **k: (
            '{"description": "Gross sales pre-returns.", '
            '"synonyms": ["net sales", "sales after returns"]}',
            {},
        ),
    )

    theme = RcaPatchTheme(
        rca_id="rca_measure",
        rca_kind=RcaKind.MEASURE_SWAP,
        patch_family="contrastive_measure_disambiguation",
        patches=(
            {"type": "update_column_description", "lever": 1,
             "table": "orders", "column": "gross_sales",
             "intent": "strengthen description",
             "actual_objects": ["net_sales"]},
        ),
        target_qids=("q1",),
        touched_objects=("gross_sales",),
    )

    proposals = optimizer.generate_proposals_from_strategy(
        strategy={"action_groups": [], "_source_clusters": []},
        action_group={
            "id": "AG1",
            "lever_directives": {"1": {"tables": [], "columns": []}},
            "source_cluster_ids": [],
            "affected_questions": [],
            "root_cause_summary": "test",
        },
        metadata_snapshot={"_rca_themes": [theme], "instructions": {}},
        target_lever=1,
        apply_mode="apply",
        benchmarks=[],
    )

    p = next((x for x in proposals if x.get("source") == "rca_theme_lever1"), None)
    assert p is not None, f"bridge produced nothing; got {proposals}"
    assert p["patch_type"] == "update_column_description"
    assert p["table"] == "orders"
    assert p["column"] == "gross_sales"
    assert p["rca_id"] == "rca_measure"
    assert p["column_sections"]["synonyms"] == ["net sales", "sales after returns"]
    assert p["column_sections"]["description"] == "Gross sales pre-returns."


def test_rca_lever1_bridge_table_path_produces_description(monkeypatch) -> None:
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    monkeypatch.setattr(optimizer, "ENABLE_RCA_LEVER1_BRIDGE", True)
    monkeypatch.setattr(
        optimizer, "_traced_llm_call",
        lambda *a, **k: ('{"description": "default routing target"}', {}),
    )

    theme = RcaPatchTheme(
        rca_id="rca_mv",
        rca_kind=RcaKind.METRIC_VIEW_ROUTING_CONFUSION,
        patch_family="contrastive_metric_routing",
        patches=(
            {"type": "update_description", "lever": 1,
             "target": "mv_gross_sales",
             "intent": "default routing"},
        ),
        target_qids=("q1",),
        touched_objects=("mv_gross_sales",),
    )

    proposals = optimizer.generate_proposals_from_strategy(
        strategy={"action_groups": [], "_source_clusters": []},
        action_group={
            "id": "AG1",
            "lever_directives": {"1": {"tables": [], "columns": []}},
            "source_cluster_ids": [],
            "affected_questions": [],
            "root_cause_summary": "test",
        },
        metadata_snapshot={"_rca_themes": [theme], "instructions": {}},
        target_lever=1,
        apply_mode="apply",
        benchmarks=[],
    )

    p = next((x for x in proposals if x.get("source") == "rca_theme_lever1"), None)
    assert p is not None
    assert p["patch_type"] == "update_description"
    assert p["table"] == "mv_gross_sales"
    assert p["table_sections"]["description"] == "default routing target"


def test_rca_lever1_bridge_no_op_when_flag_off(monkeypatch) -> None:
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    monkeypatch.setattr(optimizer, "ENABLE_RCA_LEVER1_BRIDGE", False)

    theme = RcaPatchTheme(
        rca_id="rca_measure",
        rca_kind=RcaKind.MEASURE_SWAP,
        patch_family="contrastive_measure_disambiguation",
        patches=({"type": "update_column_description", "lever": 1,
                  "table": "orders", "column": "gross_sales",
                  "intent": "x"},),
        target_qids=("q1",),
        touched_objects=("gross_sales",),
    )

    proposals = optimizer.generate_proposals_from_strategy(
        strategy={"action_groups": [], "_source_clusters": []},
        action_group={
            "id": "AG1",
            "lever_directives": {"1": {"tables": [], "columns": []}},
            "source_cluster_ids": [],
            "affected_questions": [],
            "root_cause_summary": "test",
        },
        metadata_snapshot={"_rca_themes": [theme], "instructions": {}},
        target_lever=1,
        apply_mode="apply",
        benchmarks=[],
    )

    assert not [p for p in proposals if p.get("source") == "rca_theme_lever1"]


def test_rca_lever1_bridge_merges_synonyms_into_strategist_proposal(monkeypatch) -> None:
    from genie_space_optimizer.optimization import optimizer
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    monkeypatch.setattr(optimizer, "ENABLE_RCA_LEVER1_BRIDGE", True)
    monkeypatch.setattr(
        optimizer, "_traced_llm_call",
        lambda *a, **k: (
            '{"description": "X", "synonyms": ["topline", "ttl revenue"]}',
            {},
        ),
    )

    theme = RcaPatchTheme(
        rca_id="rca_synonym",
        rca_kind=RcaKind.MEASURE_SWAP,
        patch_family="contrastive_measure_disambiguation",
        patches=({"type": "add_column_synonym", "lever": 1,
                  "table": "orders", "column": "gross_sales",
                  "intent": "merge"},),
        target_qids=("q1",),
        touched_objects=("gross_sales",),
    )

    proposals = optimizer.generate_proposals_from_strategy(
        strategy={"action_groups": [], "_source_clusters": []},
        action_group={
            "id": "AG1",
            "lever_directives": {"1": {
                "columns": [
                    {"table": "orders", "column": "gross_sales",
                     "entity_type": "column_measure",
                     "sections": {"definition": "Strategist text",
                                  "synonyms": "existing alias"}},
                ],
            }},
            "source_cluster_ids": [],
            "affected_questions": [],
            "root_cause_summary": "test",
        },
        metadata_snapshot={"_rca_themes": [theme], "instructions": {}},
        target_lever=1,
        apply_mode="apply",
        benchmarks=[],
    )

    rca_only = [p for p in proposals if p.get("source") == "rca_theme_lever1"]
    assert not rca_only, f"bridge should have merged, not appended; got {rca_only}"
    strategist = next(
        (p for p in proposals
         if p.get("table") == "orders" and p.get("column") == "gross_sales"
         and p.get("source") != "rca_theme_lever1"),
        None,
    )
    assert strategist is not None
    syns = strategist["column_sections"].get("synonyms")
    assert "topline" in syns
    assert "ttl revenue" in syns
    assert "rca_synonym" in strategist["provenance"].get("rca_synonym_themes", [])


def test_rca_lever1_synonym_filter_drops_low_quality_candidates() -> None:
    from genie_space_optimizer.optimization.optimizer import _filter_rca_synonyms

    out = _filter_rca_synonyms(
        candidates=[
            "GROSS_SALES",          # snake_case → drop
            "ALL_CAPS_PHRASE",      # ALL_CAPS → drop
            "x",                    # too short → drop
            "net sales",            # ok
            "net sales",            # dup → drop
            "topline revenue",      # ok
            "existing alias",       # already in existing → drop
            "another phrase",       # ok (3rd valid)
            "fourth phrase",        # ok (4th)
            "fifth phrase",         # ok (5th)
            "sixth phrase",         # cap at 5 → drop
        ],
        existing=["existing alias"],
    )

    assert out == [
        "net sales", "topline revenue", "another phrase",
        "fourth phrase", "fifth phrase",
    ]
