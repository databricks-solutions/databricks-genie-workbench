"""Nightly settings mirror: pure encoder + resolver (MV-D109 P5).

The app's Ontology Settings live in Lakebase, which only the app can read. An
on-demand scan works because the app forwards Settings as ``run_now`` job
parameters (``backend/ontology/services/refresh.py::_launch``). The 07:00 UTC
schedule passes only the job's declared defaults, so its ``catalog_allowlist`` is
``"[]"`` and the run records ``skipped``. The app therefore also writes Settings to
one Delta row per workspace (``genie_ont_settings_mirror``); a param-less nightly
run reads that row and uses it in place of the widgets.

This module is the pure seam the notebook calls — no I/O, no Spark, no SDK:

* :func:`settings_to_widgets` maps a stored Settings dict to the exact widget
  strings ``_launch`` forwards on demand (the 16 Settings-derived keys, the same
  encodings), so nightly == on-demand. External context is NOT mapped: on-demand
  does not forward it either (MV-D109 P4 extends both paths together).
* :func:`resolve_nightly_settings` opens the gate only for a nightly run with an
  empty allowlist widget (all-or-nothing: the notebook cannot tell an explicit
  widget value from its in-code default) and returns the overrides, else ``None``.
* :func:`mirror_read_sql` is the job's SELECT. The job only reads this table.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from genie_space_optimizer.ontology.ddl import TABLE_ONT_SETTINGS_MIRROR

# The notebook's in-code widget defaults for the 16 Settings-derived keys, i.e. what
# a param-less run uses today. A mirror row missing a key (an older app wrote it)
# degrades to these, never raises. Pinned against the notebook by a test.
WIDGET_DEFAULTS: dict[str, str] = {
    "catalog_allowlist": "[]",
    "domain_facet_denylist": "[]",
    "domain_min_tables": "3",
    "domain_min_schemas": "2",
    "domain_require_connection": "true",
    "domain_schema_denylist": '["information_schema"]',
    "domain_join_col_suffixes": '["_id", "_key"]',
    "domain_join_col_max_schemas": "2",
    "domain_join_col_denylist": (
        '["id", "user_id", "workspace_id", "category_id", "tenant_id", "account_id"]'
    ),
    "domain_max_diffuse_schemas": "6",
    "domain_min_home_concentration": "0.5",
    "page_autodraft_min_corroboration": "3",
    "page_autodraft_max_pages": "50",
    "industry_alignment_enabled": "false",
    "industry_alignment_reference_model": "",
    "company_name": "",
}

SETTINGS_WIDGET_KEYS: tuple[str, ...] = tuple(WIDGET_DEFAULTS)

_LIST_KEYS = frozenset({
    "catalog_allowlist", "domain_facet_denylist", "domain_schema_denylist",
    "domain_join_col_suffixes", "domain_join_col_denylist",
})
_INT_KEYS = frozenset({
    "domain_min_tables", "domain_min_schemas", "domain_join_col_max_schemas",
    "domain_max_diffuse_schemas", "page_autodraft_min_corroboration",
    "page_autodraft_max_pages",
})
_FLOAT_KEYS = frozenset({"domain_min_home_concentration"})
_BOOL_KEYS = frozenset({"domain_require_connection", "industry_alignment_enabled"})
_STR_KEYS = frozenset({"industry_alignment_reference_model", "company_name"})

# Gate-closed / miss reasons, logged by the notebook (plus its own ``read_failed``).
REASON_MIRROR = "mirror"
REASON_NOT_NIGHTLY = "not_nightly"
REASON_ALLOWLIST_GIVEN = "allowlist_given"
REASON_NO_ROW = "no_row"
REASON_BAD_ROW = "bad_row"


def _raw_value(settings: Mapping[str, Any], key: str) -> Any:
    """The stored value for a widget key (industry alignment is nested)."""
    if key.startswith("industry_alignment_"):
        nested = settings.get("industry_alignment")
        if not isinstance(nested, Mapping):
            return None
        return nested.get("enabled" if key == "industry_alignment_enabled" else "reference_model")
    return settings.get(key)


def _encode(key: str, value: Any) -> str:
    """``_launch``'s encoding for one key; a missing/malformed value → the widget default."""
    if value is None:
        return WIDGET_DEFAULTS[key]
    try:
        if key in _LIST_KEYS:
            if not isinstance(value, (list, tuple)):
                return WIDGET_DEFAULTS[key]
            return json.dumps(list(value))
        if key in _INT_KEYS:
            if isinstance(value, bool):
                return WIDGET_DEFAULTS[key]
            return str(int(value))
        if key in _FLOAT_KEYS:
            if isinstance(value, bool):
                return WIDGET_DEFAULTS[key]
            return str(float(value))
        if key in _BOOL_KEYS:
            if not isinstance(value, bool):
                return WIDGET_DEFAULTS[key]
            return "true" if value else "false"
        return str(value or "")
    except (TypeError, ValueError):
        return WIDGET_DEFAULTS[key]


def settings_to_widgets(settings: Mapping[str, Any]) -> dict[str, str]:
    """Stored Settings (``OntologySettings.model_dump(mode="json")``) → the 16 widget
    strings ``_launch`` forwards on demand. Never raises on an old or partial row."""
    return {key: _encode(key, _raw_value(settings, key)) for key in SETTINGS_WIDGET_KEYS}


def _allowlist_is_empty(raw: str | None) -> bool:
    """Mirrors the notebook's allowlist parse: bad JSON or blank entries → empty."""
    try:
        parsed = json.loads(raw or "[]")
        return not [c for c in parsed if str(c).strip()]
    except (TypeError, ValueError):
        return True


def gate_reason(widget_values: Mapping[str, str]) -> str | None:
    """``None`` when the mirror should be consulted, else why not.

    Open only when ``trigger`` (blank ⇒ ``nightly``, as the notebook parses it) is
    ``nightly`` AND the ``catalog_allowlist`` widget parses to empty. A manual run
    with an allowlist keeps full control of every knob.
    """
    trigger = (widget_values.get("trigger") or "").strip() or "nightly"
    if trigger != "nightly":
        return REASON_NOT_NIGHTLY
    if not _allowlist_is_empty(widget_values.get("catalog_allowlist")):
        return REASON_ALLOWLIST_GIVEN
    return None


def resolve_nightly_settings_with_reason(
    widget_values: Mapping[str, str], mirror_row: Mapping[str, Any] | None
) -> tuple[dict[str, str] | None, str]:
    """``(overrides, reason)``: overrides for the 16 keys when the gate is open and
    the row parses (reason ``mirror``), else ``(None, <why>)``."""
    closed = gate_reason(widget_values)
    if closed is not None:
        return None, closed
    if not mirror_row:
        return None, REASON_NO_ROW
    try:
        settings = json.loads(mirror_row.get("settings_json") or "")
    except (TypeError, ValueError):
        return None, REASON_BAD_ROW
    if not isinstance(settings, Mapping):
        return None, REASON_BAD_ROW
    return settings_to_widgets(settings), REASON_MIRROR


def resolve_nightly_settings(
    widget_values: Mapping[str, str], mirror_row: Mapping[str, Any] | None
) -> dict[str, str] | None:
    """The widget overrides for a param-less nightly run, or ``None`` (use the widgets)."""
    return resolve_nightly_settings_with_reason(widget_values, mirror_row)[0]


def mirror_read_sql(catalog: str, schema: str, workspace_id: str) -> str:
    """The job's read of this workspace's mirror row (quote-escaped like the
    suppression read). The newest row wins should a duplicate ever exist."""
    ws = str(workspace_id).replace("'", "''")
    return (
        f"SELECT settings_json, updated_at FROM {catalog}.{schema}.{TABLE_ONT_SETTINGS_MIRROR} "
        f"WHERE workspace_id = '{ws}' ORDER BY updated_at DESC LIMIT 1"
    )
