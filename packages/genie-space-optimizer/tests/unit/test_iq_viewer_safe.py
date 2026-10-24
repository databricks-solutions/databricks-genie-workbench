"""Every string calculate_score interpolates has a viewer-safe form, and no form
outlives the scorer (MV-D110, MV-D119)."""

import ast
import inspect

from genie_space_optimizer.iq_scan import scoring
from genie_space_optimizer.iq_scan.scoring import (
    VIEWER_SAFE_FORMS,
    calculate_score,
    viewer_safe_text,
)

_SECRET = "zq_secret"

_DESCRIPTION = (
    "Sales analytics for regional managers: orders, revenue, returns and margin "
    "across every channel, region and fiscal quarter."
)
_TEXT = "Revenue means net sales after returns. Fiscal quarters start in February."
_GUIDANCE = {"sql_snippets": {"measures": [{"id": "m"}], "filters": [{"id": "f"}]}}


def _col(name: str, *, described: bool = True, **flags) -> dict:
    col = {"name": name, **flags}
    if described:
        col["description"] = "Useful business column for analysis"
    return col


def _table(name: str, n_cols: int = 4, *, described: bool = True, cols: list | None = None, **extra) -> dict:
    if cols is None:
        cols = [_col(f"{name}_metric_{i}") for i in range(n_cols)]
        cols[0].update(enable_entity_matching=True, synonyms=["alias"])
    table = {"name": name, "columns": cols, **extra}
    if described:
        table["description"] = "Fact table of business events"
    return table


def _tables(n: int, *, described: int | None = None) -> list[dict]:
    described = n if described is None else described
    return [_table(f"table_{i}", described=i < described) for i in range(n)]


def _space(
    tables: list | None = None,
    *,
    metric_views: list | None = None,
    joins: int | None = None,
    text: str | None = _TEXT,
    guidance: dict | None = None,
    benchmarks: int = 10,
    description: str = _DESCRIPTION,
) -> dict:
    tables = [_table("orders")] if tables is None and metric_views is None else (tables or [])
    joins = max(len(tables) - 1, 0) if joins is None else joins
    instructions = {
        "text_instructions": [{"content": text}] if text is not None else [],
        "join_specs": [{"id": f"join_{i}"} for i in range(joins)],
        **(_GUIDANCE if guidance is None else guidance),
    }
    return {
        "description": description,
        "data_sources": {"tables": tables, "metric_views": metric_views or []},
        "instructions": instructions,
        "benchmarks": {"questions": [{"id": f"q{i}"} for i in range(benchmarks)]},
    }


def _redaction_fixture() -> dict:
    """The shape of backend/tests/test_scan_redaction.py, with the sentinel in every quoted slot."""
    noisy = [f"etl_{_SECRET}", "raw_payload_json", "debug_flag", "audit_user", "col_1", "load_timestamp"]
    cols = [{"name": f"business_col_{i}", "description": "Useful business column"} for i in range(14)]
    cols[0]["enable_entity_matching"] = True
    cols += [{"name": name} for name in noisy]
    instruction = f"Use SELECT * FROM {_SECRET}_orders WHERE region = 'AMER' for American orders."
    return {
        "data_sources": {"tables": [{"name": f"{_SECRET}_fact_table", "row_filter": "true", "columns": cols}]},
        "instructions": {"text_instructions": [{"content": [instruction]}]},
        "benchmarks": {},
    }


def _wide_table(n_cols: int, n_noisy: int) -> dict:
    cols = [_col(f"orders_metric_{i}") for i in range(n_cols - n_noisy)]
    cols += [_col(f"etl_batch_{i}") for i in range(n_noisy)]
    cols[0].update(enable_entity_matching=True, synonyms=["alias"])
    return _table("orders", cols=cols)


def _entity_table(n_entity: int) -> dict:
    cols = [_col(f"orders_dim_{i}", enable_entity_matching=True, synonyms=["alias"]) for i in range(n_entity)]
    return _table("orders", cols=cols)


SCENARIOS: dict[str, tuple[dict, dict | None]] = {
    "empty": ({}, None),
    "baseline": (_space(), None),
    "metric_view_only": (
        _space(metric_views=[{
            "name": "mv_sales",
            "column_configs": [{"column_name": "region", "enable_entity_matching": True}],
        }]),
        None,
    ),
    "redaction_fixture": (_redaction_fixture(), None),
    "thirteen_tables": (_space(_tables(13)), None),
    "nine_tables_two_joins": (_space(_tables(9), joins=2), None),
    "multi_table_no_join": (_space(_tables(2), joins=0), None),
    "no_entity_matching": (_space([_table("orders", cols=[_col("orders_metric_0", synonyms=["a"])])]), None),
    "entity_matching_101": (_space([_entity_table(101)]), None),
    "entity_matching_121": (_space([_entity_table(121)]), None),
    "no_text_instructions": (_space(text=None), None),
    "long_instructions": (_space(text="Explain business terms in plain words. " * 60), None),
    "brief_instructions": (_space(text="Be concise."), None),
    "no_sql_guidance": (_space(guidance={}), None),
    "measures_only": (_space(guidance={"sql_snippets": {"measures": [{"id": "m"}]}}), None),
    "functions_only": (_space(guidance={"sql_functions": [{"id": "fn"}]}), None),
    "filters_only": (_space(guidance={"sql_snippets": {"filters": [{"id": "f"}]}}), None),
    "examples_without_usage_guidance": (
        _space(guidance={**_GUIDANCE, "example_question_sqls": [{"question": "q1"}, {"question": "q2"}]}),
        None,
    ),
    "five_benchmarks": (_space(benchmarks=5), None),
    "fifteen_benchmarks": (_space(benchmarks=15), None),
    "run_at_half_accuracy": (_space(), {"accuracy": 0.5}),
    "run_above_target_accuracy": (_space(benchmarks=25), {"accuracy": 0.9}),
    "column_descriptions_below": (
        _space([_table("orders", cols=[_col("orders_metric_0", enable_entity_matching=True)]
                       + [_col(f"orders_metric_{i}", described=False) for i in range(1, 4)])]),
        None,
    ),
    "column_descriptions_between": (
        _space([_table("orders", cols=[_col(f"orders_metric_{i}", described=i < 3, enable_entity_matching=True,
                                            synonyms=["a"]) for i in range(5)])]),
        None,
    ),
    "table_descriptions_below": (_space(_tables(2, described=1)), None),
    "table_descriptions_between": (_space(_tables(5, described=4)), None),
    "visibility_noisy_20pct": (_space([_wide_table(20, 4)]), None),
    "visibility_wide_table": (_space([_wide_table(80, 0)]), None),
    "visibility_noisy_and_wide": (_space([_wide_table(80, 16)]), None),
    "short_description": (_space(description="Orders and revenue for regional managers"), None),
}


def _emitted(space: dict, run: dict | None) -> list[str]:
    result = calculate_score(space, run)
    details = [c["detail"] for c in result["checks"] if c["detail"] is not None]
    return [*result["findings"], *result["warnings"], *details]


def _all_emitted() -> list[str]:
    return [text for space, run in SCENARIOS.values() for text in _emitted(space, run)]


def test_every_emitted_string_has_a_viewer_safe_form():
    assert sorted({t for t in _all_emitted() if not viewer_safe_text(t)}) == []


def test_every_form_is_emitted_by_some_scenario():
    safe = [viewer_safe_text(t) for t in _all_emitted()]
    dead = [f.pattern for f in VIEWER_SAFE_FORMS if not any(s and f.fullmatch(s) for s in safe)]
    assert dead == []


def test_quoted_space_content_never_survives():
    quoting = [t for t in _all_emitted() if _SECRET in t]
    assert len(quoting) >= 3  # positive control: excerpt, column sample, RLS names
    assert all(_SECRET not in viewer_safe_text(t) for t in quoting)


def test_the_quoting_strings_keep_their_counts():
    assert viewer_safe_text(
        "SQL patterns found in text instructions — move to Example SQLs or SQL Expressions. "
        "First offender: 'SELECT * FROM zq_secret'"
    ) == "SQL patterns found in text instructions — move to Example SQLs or SQL Expressions."
    assert viewer_safe_text("6/20 visible columns look internal/noisy (zq_secret, etl_x)") == (
        "6/20 visible columns look internal/noisy"
    )
    assert viewer_safe_text("6/20 visible columns look internal/noisy (30%)") == (
        "6/20 visible columns look internal/noisy (30%)"
    )
    assert viewer_safe_text(
        "Tables with row-level security (zq_secret) — entity matching is silently disabled for these"
    ) == "Tables with row-level security — entity matching is silently disabled for these"


def test_text_outside_the_forms_has_none():
    for text in [
        "No column synonyms defined (zq_secret)",
        "Missing or placeholder space description",  # pre-rename wording (Ruling 5)
        "Custom note about zq_secret",
        "١٢٣ chars",  # Arabic-Indic digits: the scorer emits only ASCII counts
        "Accuracy: ٨٥%",
        "٦/٢٠ visible columns look internal/noisy (etl_zq_secret)",
        "",
        None,
        42,
    ]:
        assert viewer_safe_text(text) is None, text
    noisy = "6/20 visible columns look internal/noisy"
    assert viewer_safe_text(f"{noisy} (٣٠%)") == noisy


def test_next_steps_and_check_labels_are_literals_in_the_scorer():
    tree = ast.parse(inspect.getsource(scoring.calculate_score))
    appends = labels = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if (isinstance(func, ast.Attribute) and func.attr == "append"
                and isinstance(func.value, ast.Name)
                and func.value.id in {"next_steps", "warning_next_steps"}):
            appends += 1
            assert isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)
        if isinstance(func, ast.Name) and func.id == "_check":
            labels += 1
            assert isinstance(node.args[1], ast.Constant) and isinstance(node.args[1].value, str)
    assert appends >= 15 and labels >= 12  # positive control
