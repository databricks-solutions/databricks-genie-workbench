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
