from __future__ import annotations


def test_strip_trailing_statement_semicolon_before_sample_wrap() -> None:
    from genie_space_optimizer.optimization.evaluation import (
        _strip_trailing_statement_semicolon,
    )

    sql = "SELECT * FROM cat.sch.table ORDER BY id;\n"

    assert _strip_trailing_statement_semicolon(sql) == (
        "SELECT * FROM cat.sch.table ORDER BY id"
    )


def test_alignment_rules_default_to_empty_no_issues() -> None:
    """With no rules registered the function must always return []."""
    from genie_space_optimizer.optimization.benchmarks import (
        deterministic_question_sql_alignment_issues,
    )

    issues = deterministic_question_sql_alignment_issues(
        {
            "question": "Show country-level performance: total sales and store count.",
            "expected_sql": (
                "SELECT country_code, SUM(total_revenue_usd) "
                "FROM cat.sch.fact_orders "
                "WHERE is_active_flag = 'Y' "
                "GROUP BY country_code"
            ),
        }
    )

    assert issues == []


def test_alignment_rules_registered_rule_fires_when_filter_unmentioned(
    monkeypatch,
) -> None:
    """A registered rule fires when the SQL has the filter but the question
    does not mention any of the rule's question terms.
    """
    from genie_space_optimizer.optimization import alignment_rules
    from genie_space_optimizer.optimization.benchmarks import (
        deterministic_question_sql_alignment_issues,
    )

    rule = alignment_rules.ExtraFilterRule(
        name="active_flag_implicit",
        column_substring="is_active_flag",
        question_terms=("active", "currently active"),
        issue_template=(
            "EXTRA_FILTER: SQL filters on {column} but the question does not "
            "ask for active-only results."
        ),
    )
    monkeypatch.setattr(
        alignment_rules, "DETERMINISTIC_EXTRA_FILTER_RULES", (rule,)
    )

    issues = deterministic_question_sql_alignment_issues(
        {
            "question": "Show country-level performance: total revenue.",
            "expected_sql": (
                "SELECT country_code, SUM(total_revenue_usd) "
                "FROM cat.sch.fact_orders "
                "WHERE is_active_flag = 'Y' "
                "GROUP BY country_code"
            ),
        }
    )

    assert issues == [
        "EXTRA_FILTER: SQL filters on is_active_flag but the question does not "
        "ask for active-only results."
    ]


def test_alignment_rules_registered_rule_does_not_fire_when_question_mentions_term(
    monkeypatch,
) -> None:
    """A registered rule must NOT fire when the question mentions one of
    its question terms (even synonyms).
    """
    from genie_space_optimizer.optimization import alignment_rules
    from genie_space_optimizer.optimization.benchmarks import (
        deterministic_question_sql_alignment_issues,
    )

    rule = alignment_rules.ExtraFilterRule(
        name="active_flag_implicit",
        column_substring="is_active_flag",
        question_terms=("active", "currently active"),
        issue_template="EXTRA_FILTER: SQL filters on {column} ...",
    )
    monkeypatch.setattr(
        alignment_rules, "DETERMINISTIC_EXTRA_FILTER_RULES", (rule,)
    )

    issues = deterministic_question_sql_alignment_issues(
        {
            "question": "Show currently active customers by country.",
            "expected_sql": (
                "SELECT country_code, COUNT(*) "
                "FROM cat.sch.dim_customer "
                "WHERE is_active_flag = 'Y' "
                "GROUP BY country_code"
            ),
        }
    )

    assert issues == []


def test_alignment_rules_load_from_json_path(tmp_path, monkeypatch) -> None:
    """A JSON file at GSO_EXTRA_FILTER_RULES_PATH is loaded into the
    rule registry on module import.
    """
    import importlib

    rules_path = tmp_path / "rules.json"
    rules_path.write_text(
        '[{"name": "region_filter", '
        '"column_substring": "region_code", '
        '"question_terms": ["region", "country"], '
        '"issue_template": "EXTRA_FILTER: filters on {column} but question does not mention region."}]'
    )

    monkeypatch.setenv("GSO_EXTRA_FILTER_RULES_PATH", str(rules_path))

    from genie_space_optimizer.optimization import alignment_rules

    reloaded = importlib.reload(alignment_rules)
    try:
        assert len(reloaded.DETERMINISTIC_EXTRA_FILTER_RULES) == 1
        rule = reloaded.DETERMINISTIC_EXTRA_FILTER_RULES[0]
        assert rule.name == "region_filter"
        assert rule.column_substring == "region_code"
        assert rule.question_terms == ("region", "country")
    finally:
        monkeypatch.delenv("GSO_EXTRA_FILTER_RULES_PATH", raising=False)
        importlib.reload(alignment_rules)


def test_alignment_rules_malformed_json_falls_back_to_empty(
    tmp_path, monkeypatch
) -> None:
    """Malformed JSON must not crash; the registry stays empty."""
    import importlib

    rules_path = tmp_path / "rules.json"
    rules_path.write_text("{not valid json")

    monkeypatch.setenv("GSO_EXTRA_FILTER_RULES_PATH", str(rules_path))

    from genie_space_optimizer.optimization import alignment_rules

    reloaded = importlib.reload(alignment_rules)
    try:
        assert reloaded.DETERMINISTIC_EXTRA_FILTER_RULES == ()
    finally:
        monkeypatch.delenv("GSO_EXTRA_FILTER_RULES_PATH", raising=False)
        importlib.reload(alignment_rules)
