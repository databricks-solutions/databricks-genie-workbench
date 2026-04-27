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


def test_synthesize_example_sqls_for_rca_reuses_existing_validator(monkeypatch):
    from genie_space_optimizer.optimization import synthesis
    from genie_space_optimizer.optimization.rca import RcaKind, RcaPatchTheme

    called = {"validate": False}

    def fake_validate(proposal, **kwargs):
        called["validate"] = True
        return True, []

    monkeypatch.setattr(synthesis, "validate_synthesis_proposal", fake_validate)
    monkeypatch.setattr(
        synthesis,
        "_extract_json_proposal",
        lambda raw: {
            "patch_type": "add_example_sql",
            "example_question": "Show monthly sales by category",
            "example_sql": "SELECT category, SUM(sales) FROM orders GROUP BY category",
        },
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
                "blame_set": ["category", "sales"],
            },
        ),
        target_qids=("q1",),
        touched_objects=("category", "sales"),
    )

    proposal = synthesis.synthesize_example_sqls_for_rca(
        theme,
        metadata_snapshot={},
        benchmark_corpus=None,
        llm_caller=lambda prompt: "{}",
    )

    assert called["validate"] is True
    assert proposal["patch_type"] == "add_example_sql"
    assert proposal["rca_id"] == "rca_shape"
    assert proposal["source"] == "rca_theme"


def test_rca_example_synthesis_flag_is_imported_by_optimizer() -> None:
    import inspect

    from genie_space_optimizer.optimization import optimizer

    src = inspect.getsource(optimizer)

    assert "ENABLE_RCA_EXAMPLE_SQL_SYNTHESIS" in src
    assert "synthesize_example_sqls_for_rca" in src


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
