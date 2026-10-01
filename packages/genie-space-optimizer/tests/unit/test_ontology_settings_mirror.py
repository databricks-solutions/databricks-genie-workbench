"""Nightly settings mirror, wheel side (MV-D109 P5): the pure encoder + resolver.

A param-less nightly run (trigger=nightly, empty allowlist widget) reads the
app-written Settings row and uses it for every Settings-derived knob; any other run,
or a missing / bad row, uses the widgets (an empty allowlist ⇒ today's ``skipped``).
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import genie_space_optimizer
from genie_space_optimizer.ontology import ddl
from genie_space_optimizer.ontology import settings_mirror as sm

_NOTEBOOK = (
    Path(genie_space_optimizer.__file__).resolve().parent / "jobs" / "run_ontology_materialize.py"
)

_NIGHTLY = {"trigger": "nightly", "catalog_allowlist": "[]"}

_SAVED = {
    "company_name": "Acme Air",
    "catalog_allowlist": ["finance", "ops"],
    "read_identity": "obo",
    "domain_facet_denylist": ["pii"],
    "domain_min_tables": 5,
    "domain_min_schemas": 3,
    "domain_require_connection": False,
    "domain_schema_denylist": ["information_schema", "scratch"],
    "domain_join_col_suffixes": ["_id"],
    "domain_join_col_max_schemas": 4,
    "domain_join_col_denylist": ["id"],
    "domain_max_diffuse_schemas": 9,
    "domain_min_home_concentration": 0.75,
    "page_autodraft_min_corroboration": 2,
    "page_autodraft_max_pages": 7,
    "industry_alignment": {"enabled": True, "reference_model": "airline"},
    "external_context": {"enabled": True, "sources": {"web": True}},
}


def _row(settings) -> dict:
    return {"settings_json": json.dumps(settings), "updated_at": "2026-09-30T07:00:00Z"}


def test_on_demand_never_consults_the_mirror():
    widgets = {"trigger": "on_demand", "catalog_allowlist": "[]"}
    assert sm.resolve_nightly_settings(widgets, _row(_SAVED)) is None
    assert sm.resolve_nightly_settings_with_reason(widgets, _row(_SAVED))[1] == "not_nightly"


def test_nightly_with_an_allowlist_keeps_the_widgets():
    widgets = {"trigger": "nightly", "catalog_allowlist": '["other"]'}
    assert sm.resolve_nightly_settings_with_reason(widgets, _row(_SAVED)) == (None, "allowlist_given")


def test_nightly_without_a_row_falls_through():
    assert sm.resolve_nightly_settings_with_reason(_NIGHTLY, None) == (None, "no_row")
    assert sm.resolve_nightly_settings_with_reason(_NIGHTLY, {}) == (None, "no_row")


def test_bad_json_or_non_object_row_falls_through():
    assert sm.resolve_nightly_settings_with_reason(_NIGHTLY, {"settings_json": "{not json"}) == (None, "bad_row")
    assert sm.resolve_nightly_settings_with_reason(_NIGHTLY, {"settings_json": "[1, 2]"}) == (None, "bad_row")
    assert sm.resolve_nightly_settings_with_reason(_NIGHTLY, {"settings_json": None}) == (None, "bad_row")


def test_valid_row_supplies_all_sixteen_keys_with_launch_encodings():
    out, reason = sm.resolve_nightly_settings_with_reason(_NIGHTLY, _row(_SAVED))
    assert reason == "mirror"
    assert out == {
        "catalog_allowlist": '["finance", "ops"]',
        "domain_facet_denylist": '["pii"]',
        "domain_min_tables": "5",
        "domain_min_schemas": "3",
        "domain_require_connection": "false",
        "domain_schema_denylist": '["information_schema", "scratch"]',
        "domain_join_col_suffixes": '["_id"]',
        "domain_join_col_max_schemas": "4",
        "domain_join_col_denylist": '["id"]',
        "domain_max_diffuse_schemas": "9",
        "domain_min_home_concentration": "0.75",
        "page_autodraft_min_corroboration": "2",
        "page_autodraft_max_pages": "7",
        "industry_alignment_enabled": "true",
        "industry_alignment_reference_model": "airline",
        "company_name": "Acme Air",
    }
    # External context stays off nightly (on-demand does not forward it either; P4).
    assert not any(k.startswith("external_context") for k in out)


def test_row_with_an_empty_allowlist_still_overrides_and_scans_nothing():
    out = sm.resolve_nightly_settings(_NIGHTLY, _row({**_SAVED, "catalog_allowlist": []}))
    assert out is not None and out["catalog_allowlist"] == "[]"


def test_old_row_missing_newer_keys_degrades_to_defaults():
    out = sm.resolve_nightly_settings(_NIGHTLY, _row({"catalog_allowlist": ["finance"]}))
    assert out == {**sm.WIDGET_DEFAULTS, "catalog_allowlist": '["finance"]'}


def test_malformed_values_degrade_per_key():
    bad = {
        "catalog_allowlist": "finance",  # not a list
        "domain_min_tables": "lots",
        "domain_min_home_concentration": None,
        "domain_require_connection": "false",  # not a bool
        "industry_alignment": "on",  # not an object
    }
    out = sm.settings_to_widgets(bad)
    for key in ("catalog_allowlist", "domain_min_tables", "domain_min_home_concentration",
                "domain_require_connection", "industry_alignment_enabled",
                "industry_alignment_reference_model"):
        assert out[key] == sm.WIDGET_DEFAULTS[key], key


def test_gate_matches_the_notebook_parse():
    # Blank trigger parses as nightly; an unparsable / blank-only allowlist parses empty.
    assert sm.gate_reason({"trigger": "", "catalog_allowlist": "[]"}) is None
    assert sm.gate_reason({"trigger": "nightly", "catalog_allowlist": "not json"}) is None
    assert sm.gate_reason({"trigger": "nightly", "catalog_allowlist": '["  "]'}) is None
    assert sm.gate_reason({"trigger": " nightly ", "catalog_allowlist": ""}) is None


def test_mirror_read_sql_escapes_and_selects_only():
    sql = sm.mirror_read_sql("cat", "sch", "ws'1")
    assert sql.startswith("SELECT settings_json, updated_at FROM cat.sch.genie_ont_settings_mirror ")
    assert "workspace_id = 'ws''1'" in sql
    assert "LIMIT 1" in sql


def _notebook_widget_defaults() -> dict[str, str]:
    tree = ast.parse(_NOTEBOOK.read_text())
    out: dict[str, str] = {}
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "text"
            and ast.unparse(node.func.value) == "dbutils.widgets"
        ):
            out[node.args[0].value] = node.args[1].value
    return out


def test_widget_defaults_match_the_notebook():
    nb = _notebook_widget_defaults()
    for key, default in sm.WIDGET_DEFAULTS.items():
        assert nb[key] == default, key


def test_notebook_reads_every_settings_key_through_the_override():
    src = _NOTEBOOK.read_text()
    helper_keys = {
        "domain_schema_denylist", "domain_join_col_suffixes", "domain_join_col_max_schemas",
        "domain_join_col_denylist", "domain_max_diffuse_schemas", "domain_min_home_concentration",
        "page_autodraft_min_corroboration", "page_autodraft_max_pages",
    }
    for key in sm.SETTINGS_WIDGET_KEYS:
        if key in helper_keys:
            assert re.search(rf'_parse_(str_list|int|float)\("{key}"', src), key
            continue
        assert f'_w("{key}")' in src, key
    # The parse helpers read through the override; external context never does.
    assert "json.loads(_w(name)" in src and "int(_w(name)" in src and "float(_w(name)" in src
    assert 'dbutils.widgets.get("external_context_enabled")' in src
    for key in sm.SETTINGS_WIDGET_KEYS:
        if key != "catalog_allowlist":  # the gate reads the raw widget
            assert f'dbutils.widgets.get("{key}")' not in src, key


def test_settings_mirror_table_is_registered():
    assert ddl.TABLE_ONT_SETTINGS_MIRROR == "genie_ont_settings_mirror"
    assert ddl.SETTINGS_TABLES == ("genie_ont_settings_mirror",)
    rendered = ddl.all_ddl("c", "s")[ddl.TABLE_ONT_SETTINGS_MIRROR]
    assert rendered.startswith("CREATE TABLE IF NOT EXISTS c.s.genie_ont_settings_mirror (")
    for col in ("workspace_id", "metastore_id", "settings_json", "updated_by", "updated_at"):
        assert col in rendered
