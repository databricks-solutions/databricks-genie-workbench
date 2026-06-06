"""Trial 26 W26.3 — ``add_sql_snippet_filter`` applier ``name``-field fix.

Pins:

* When ``GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX`` is ON (default), every
  producer path emits ``display_name`` on the nested ``sql_snippet``
  body (canonical Genie schema), NOT ``name`` (which the API rejects
  with ``Invalid serialized_space: Unknown field 'name'``).
* When the flag is OFF, the legacy ``name`` field is preserved so the
  rejection payload can be regressed end-to-end (byte-stable rollback).
* A single ``GSO_TRIAL26_APPLIER_SNIPPET_FIELDS_V1`` marker is emitted
  on every Trial-26-fixed snippet stamp, recording the ``kept`` and
  ``dropped`` field sets so postmortems can verify the fix is reaching
  production.

Covers the three producer paths identified in the Trial 26 plan:

* ``producer_snippet_validator.stamp_snippet_validation_on_body``
* ``sql_snippet_finalizer._finalize_lever_6_sql_snippet`` (the
  W26.3-relevant return dict's nested ``sql_snippet``)
* ``stages/validate_patch._materialize_sql_snippet_body`` (F1 path)

Plus an end-to-end "applier accepts our payload" regression that mimics
the airline iter-4 / 7now iter-2 add_sql_snippet_filter failure mode
(``Unknown field 'name'``).
"""
from __future__ import annotations

import io
import json
import re
import sys

import pytest


# ---------------------------------------------------------------------------
# Producer path A — producer_snippet_validator.stamp_snippet_validation_on_body
# ---------------------------------------------------------------------------


def _stamp_kwargs():
    return {
        "patch_body": {},
        "intent_id": "intent-trial26",
        "snippet_name": "positive_amount",
        "normalized_sql": "amount > 0",
        "snippet_type": "filter",
        "description": "exclude refunds",
    }


def test_producer_validator_emits_display_name_when_flag_on(monkeypatch, capsys):
    """The fix: canonical Genie schema uses ``display_name`` only."""
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX", raising=False)

    from genie_space_optimizer.optimization import producer_snippet_validator

    kwargs = _stamp_kwargs()
    producer_snippet_validator.stamp_snippet_validation_on_body(**kwargs)

    snippet = kwargs["patch_body"]["sql_snippet"]
    assert "display_name" in snippet, "Trial 26 W26.3: nested sql_snippet must expose display_name"
    assert snippet["display_name"] == "positive_amount"
    assert "name" not in snippet, (
        "Trial 26 W26.3: nested sql_snippet must NOT expose `name` — the "
        "Genie API rejects unknown fields with `Invalid serialized_space: "
        "Unknown field 'name'`"
    )


def test_producer_validator_emits_legacy_name_when_flag_off(monkeypatch):
    """Rollback path: byte-stable restore of the pre-Trial-26 ``name`` field."""
    monkeypatch.setenv("GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX", "0")

    from genie_space_optimizer.optimization import producer_snippet_validator

    kwargs = _stamp_kwargs()
    producer_snippet_validator.stamp_snippet_validation_on_body(**kwargs)

    snippet = kwargs["patch_body"]["sql_snippet"]
    assert snippet.get("name") == "positive_amount"
    assert "display_name" not in snippet


def test_producer_validator_emits_legacy_name_when_master_off(monkeypatch):
    """Master rollback also forces W26.3 off."""
    monkeypatch.setenv("GSO_TRIAL26_KIT_GATE_REACHABLE", "0")
    monkeypatch.setenv("GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX", "1")

    from genie_space_optimizer.optimization import producer_snippet_validator

    kwargs = _stamp_kwargs()
    producer_snippet_validator.stamp_snippet_validation_on_body(**kwargs)

    snippet = kwargs["patch_body"]["sql_snippet"]
    assert snippet.get("name") == "positive_amount"
    assert "display_name" not in snippet


def test_producer_validator_emits_w26_marker(monkeypatch, capsys):
    """``GSO_TRIAL26_APPLIER_SNIPPET_FIELDS_V1`` marker carries kept+dropped."""
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX", raising=False)

    from genie_space_optimizer.optimization import producer_snippet_validator

    kwargs = _stamp_kwargs()
    producer_snippet_validator.stamp_snippet_validation_on_body(**kwargs)
    out = capsys.readouterr().out

    assert "GSO_TRIAL26_APPLIER_SNIPPET_FIELDS_V1" in out, (
        f"missing marker in:\n{out}"
    )

    match = re.search(
        r"GSO_TRIAL26_APPLIER_SNIPPET_FIELDS_V1\s+(\{[^\n]*\})", out
    )
    assert match, f"could not parse marker payload from:\n{out}"
    payload = json.loads(match.group(1))

    assert payload["site"] == "producer_snippet_validator"
    assert "display_name" in payload["kept"]
    assert "name" in payload["dropped"]


def test_producer_validator_no_w26_marker_when_flag_off(monkeypatch, capsys):
    """No marker on the legacy path — distinguish fix-applied from fix-disabled."""
    monkeypatch.setenv("GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX", "0")

    from genie_space_optimizer.optimization import producer_snippet_validator

    kwargs = _stamp_kwargs()
    producer_snippet_validator.stamp_snippet_validation_on_body(**kwargs)
    out = capsys.readouterr().out

    assert "GSO_TRIAL26_APPLIER_SNIPPET_FIELDS_V1" not in out


# ---------------------------------------------------------------------------
# Producer path B — sql_snippet_finalizer (nested sql_snippet on lever 6 dict)
# ---------------------------------------------------------------------------


def test_finalizer_nested_sql_snippet_uses_display_name_when_flag_on(monkeypatch):
    """The W26.3 fix flips the nested ``sql_snippet`` field name only."""
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX", raising=False)

    from genie_space_optimizer.optimization.sql_snippet_finalizer import (
        _build_nested_sql_snippet,
    )

    nested = _build_nested_sql_snippet(
        snippet_id="abc123",
        name="total_revenue",
        sql_expression="SUM(amount)",
        snippet_type="measure",
        description="aggregate amount",
    )

    assert nested == {
        "id": "abc123",
        "display_name": "total_revenue",
        "sql": "SUM(amount)",
        "type": "measure",
        "description": "aggregate amount",
    }
    assert "name" not in nested


def test_finalizer_nested_sql_snippet_legacy_name_when_flag_off(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX", "0")

    from genie_space_optimizer.optimization.sql_snippet_finalizer import (
        _build_nested_sql_snippet,
    )

    nested = _build_nested_sql_snippet(
        snippet_id="abc123",
        name="total_revenue",
        sql_expression="SUM(amount)",
        snippet_type="measure",
        description="aggregate amount",
    )

    assert nested["name"] == "total_revenue"
    assert "display_name" not in nested


# ---------------------------------------------------------------------------
# Producer path C — stages/validate_patch F1 materialization
# ---------------------------------------------------------------------------


def test_validate_patch_f1_emits_display_name_only_when_flag_on(monkeypatch):
    """F1 materialization previously emitted BOTH ``name`` and
    ``display_name`` on the nested snippet. The fix keeps
    ``display_name`` and drops ``name``.
    """
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX", raising=False)

    from genie_space_optimizer.optimization.stages.validate_patch import (
        _materialize_sql_snippet_body,
    )

    body = {
        "name": "positive_amount",
        "display_name": "Positive Amount",
        "sql_expression": "amount > 0",
        "target_table": "main.shop.orders",
        "description": "exclude refunds",
        "synonyms": [],
    }

    snippet = _materialize_sql_snippet_body(
        body=body,
        intent_id="intent-trial26",
        normalized_sql="amount > 0",
    )

    assert snippet["display_name"] == "Positive Amount"
    assert "name" not in snippet


def test_validate_patch_f1_keeps_legacy_fields_when_flag_off(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX", "0")

    from genie_space_optimizer.optimization.stages.validate_patch import (
        _materialize_sql_snippet_body,
    )

    body = {
        "name": "positive_amount",
        "display_name": "Positive Amount",
        "sql_expression": "amount > 0",
        "target_table": "main.shop.orders",
        "description": "exclude refunds",
        "synonyms": [],
    }

    snippet = _materialize_sql_snippet_body(
        body=body,
        intent_id="intent-trial26",
        normalized_sql="amount > 0",
    )

    assert snippet.get("name") == "positive_amount"
    assert snippet.get("display_name") == "Positive Amount"


# ---------------------------------------------------------------------------
# End-to-end regression — Genie schema accepts the fixed payload
# ---------------------------------------------------------------------------


def test_fixed_payload_passes_local_genie_schema_validation(monkeypatch):
    """End-to-end: the Trial 26 W26.3 fixed payload no longer trips the
    ``Unknown field 'name'`` rejection that took out the airline iter-4
    and 7now iter-2 add_sql_snippet_filter patches.
    """
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX", raising=False)

    from genie_space_optimizer.optimization import producer_snippet_validator
    from genie_space_optimizer.common.genie_schema import SqlSnippetFilter

    body: dict = {}
    producer_snippet_validator.stamp_snippet_validation_on_body(
        patch_body=body,
        intent_id="intent-airline-iter4",
        snippet_name="exclude_refunds",
        normalized_sql="amount > 0",
        snippet_type="filter",
        description="airline iter-4 reproduction",
    )

    snippet = body["sql_snippet"]

    parsed = SqlSnippetFilter.model_validate(snippet)
    assert parsed.display_name == "exclude_refunds"
    assert getattr(parsed, "name", None) is None, (
        "Trial 26 W26.3: post-fix snippet must not surface a `name` field "
        "in the parsed Pydantic shape — the Genie API rejects it"
    )


def test_pre_fix_payload_carries_rejected_name_field(monkeypatch):
    """Pin the legacy rejection path: with the flag OFF, the nested
    ``sql_snippet`` carries the offending ``name`` field (proving our
    fix is what removes it, not some other ambient change).
    """
    monkeypatch.setenv("GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX", "0")

    from genie_space_optimizer.optimization import producer_snippet_validator

    body: dict = {}
    producer_snippet_validator.stamp_snippet_validation_on_body(
        patch_body=body,
        intent_id="intent-airline-iter4-legacy",
        snippet_name="exclude_refunds",
        normalized_sql="amount > 0",
        snippet_type="filter",
        description="airline iter-4 legacy reproduction",
    )

    snippet = body["sql_snippet"]
    assert snippet.get("name") == "exclude_refunds"
    assert "display_name" not in snippet
