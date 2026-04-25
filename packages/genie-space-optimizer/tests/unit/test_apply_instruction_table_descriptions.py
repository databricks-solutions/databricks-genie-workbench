"""Unit tests for ``_apply_instruction_table_descriptions``.

Covers Fix 1 from the *Lever Loop Iteration 3 Fixes* plan: the function
must preserve the original shape of the ``description`` field on a table
or metric view. Genie's serialized space stores ``description`` as
``list[str]`` for tables / metric views; older code wrote a Python
``repr`` of the list back as a string, which caused the API to reject
the PATCH with ``Expected an array for description but found
"['PURPOSE:…']"``.

The tests cover:

  * Empty description (``""``) → string concatenation, returns string.
  * String description → string concatenation, returns string.
  * List description → list append, returns list (preserves API
    contract).
  * Idempotency → re-running with the same span is a no-op.
  * Result shape matches input shape across both write paths.
"""

from __future__ import annotations

from genie_space_optimizer.optimization import harness as harness_mod


def _candidate(table: str, append: str) -> dict:
    return {
        "table_identifier": table,
        "description_append": append,
    }


def _snapshot_with_table(
    *, identifier: str = "cat.sch.t1",
    description=None,
    is_metric_view: bool = False,
) -> dict:
    """Build a minimal metadata snapshot with one table or metric view."""
    table = {
        "identifier": identifier,
        "name": identifier.split(".")[-1],
        "column_configs": [],
    }
    if description is not None:
        table["description"] = description
    snapshot = {
        "data_sources": {
            "tables": [] if is_metric_view else [table],
            "metric_views": [table] if is_metric_view else [],
        },
        "instructions": {"join_specs": []},
    }
    return snapshot


def _stub_side_effects(monkeypatch) -> None:
    """No-op SDK side effects so the unit test stays hermetic."""
    monkeypatch.setattr(harness_mod, "write_stage", lambda *a, **k: None)
    # patch_space_config is imported locally inside the function under
    # test, so monkeypatching the genie_client module path catches it.
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.patch_space_config",
        lambda *a, **k: None,
    )


# ═══════════════════════════════════════════════════════════════════════
# Empty description
# ═══════════════════════════════════════════════════════════════════════


def test_empty_description_appends_string(monkeypatch):
    _stub_side_effects(monkeypatch)
    snapshot = _snapshot_with_table(description="")
    candidates = [_candidate("cat.sch.t1", "PURPOSE: track sales")]

    applied = harness_mod._apply_instruction_table_descriptions(
        w=None, spark=None, run_id="r1", space_id="s1",
        candidates=candidates, metadata_snapshot=snapshot,
        catalog="c", schema="s",
    )

    assert applied == 1
    desc = snapshot["data_sources"]["tables"][0]["description"]
    assert isinstance(desc, str)
    assert desc == "PURPOSE: track sales"


# ═══════════════════════════════════════════════════════════════════════
# String description
# ═══════════════════════════════════════════════════════════════════════


def test_string_description_concatenates_and_stays_string(monkeypatch):
    _stub_side_effects(monkeypatch)
    snapshot = _snapshot_with_table(description="Existing prose.")
    candidates = [_candidate("cat.sch.t1", "PURPOSE: track sales")]

    applied = harness_mod._apply_instruction_table_descriptions(
        w=None, spark=None, run_id="r1", space_id="s1",
        candidates=candidates, metadata_snapshot=snapshot,
        catalog="c", schema="s",
    )

    assert applied == 1
    desc = snapshot["data_sources"]["tables"][0]["description"]
    assert isinstance(desc, str)
    assert "Existing prose." in desc
    assert "PURPOSE: track sales" in desc


# ═══════════════════════════════════════════════════════════════════════
# List description (the headline bug case)
# ═══════════════════════════════════════════════════════════════════════


def test_list_description_appends_as_list_element(monkeypatch):
    """Headline regression: list-shaped description MUST stay a list.

    The prior bug stringified the list (``str(['PURPOSE:…'])``) and
    concatenated with the new text, producing ``"['PURPOSE:…']\n…"`` —
    rejected by the Genie API with ``Expected an array for description
    but found "['PURPOSE:…']"``.
    """
    _stub_side_effects(monkeypatch)
    snapshot = _snapshot_with_table(
        description=["PURPOSE: existing line one"],
    )
    candidates = [_candidate("cat.sch.t1", "DOMAIN: retail sales")]

    applied = harness_mod._apply_instruction_table_descriptions(
        w=None, spark=None, run_id="r1", space_id="s1",
        candidates=candidates, metadata_snapshot=snapshot,
        catalog="c", schema="s",
    )

    assert applied == 1
    desc = snapshot["data_sources"]["tables"][0]["description"]
    assert isinstance(desc, list), (
        f"expected list, got {type(desc).__name__}: {desc!r}"
    )
    assert "PURPOSE: existing line one" in desc
    assert "DOMAIN: retail sales" in desc
    # No element is a Python ``repr`` of a list — the headline bug.
    for entry in desc:
        assert not entry.startswith("["), (
            f"description element looks like a list repr: {entry!r}"
        )


def test_list_description_preserves_existing_entries(monkeypatch):
    _stub_side_effects(monkeypatch)
    snapshot = _snapshot_with_table(
        description=["LINE 1", "LINE 2", "LINE 3"],
    )
    candidates = [_candidate("cat.sch.t1", "LINE 4")]

    harness_mod._apply_instruction_table_descriptions(
        w=None, spark=None, run_id="r1", space_id="s1",
        candidates=candidates, metadata_snapshot=snapshot,
        catalog="c", schema="s",
    )

    desc = snapshot["data_sources"]["tables"][0]["description"]
    assert desc == ["LINE 1", "LINE 2", "LINE 3", "LINE 4"]


def test_list_description_works_for_metric_views(monkeypatch):
    _stub_side_effects(monkeypatch)
    snapshot = _snapshot_with_table(
        description=["MV PURPOSE: aggregate sales"],
        is_metric_view=True,
    )
    candidates = [_candidate("cat.sch.t1", "GRAIN: store-day")]

    applied = harness_mod._apply_instruction_table_descriptions(
        w=None, spark=None, run_id="r1", space_id="s1",
        candidates=candidates, metadata_snapshot=snapshot,
        catalog="c", schema="s",
    )

    assert applied == 1
    desc = snapshot["data_sources"]["metric_views"][0]["description"]
    assert isinstance(desc, list)
    assert "GRAIN: store-day" in desc


# ═══════════════════════════════════════════════════════════════════════
# Idempotency — re-running with the same span is a no-op
# ═══════════════════════════════════════════════════════════════════════


def test_idempotent_when_string_already_contains_span(monkeypatch):
    _stub_side_effects(monkeypatch)
    snapshot = _snapshot_with_table(
        description="PURPOSE: track sales\nDOMAIN: retail",
    )
    candidates = [_candidate("cat.sch.t1", "DOMAIN: retail")]

    applied = harness_mod._apply_instruction_table_descriptions(
        w=None, spark=None, run_id="r1", space_id="s1",
        candidates=candidates, metadata_snapshot=snapshot,
        catalog="c", schema="s",
    )

    assert applied == 0
    desc = snapshot["data_sources"]["tables"][0]["description"]
    assert isinstance(desc, str)
    assert desc == "PURPOSE: track sales\nDOMAIN: retail"


def test_idempotent_when_list_already_contains_span(monkeypatch):
    _stub_side_effects(monkeypatch)
    snapshot = _snapshot_with_table(
        description=["PURPOSE: track sales", "DOMAIN: retail"],
    )
    candidates = [_candidate("cat.sch.t1", "DOMAIN: retail")]

    applied = harness_mod._apply_instruction_table_descriptions(
        w=None, spark=None, run_id="r1", space_id="s1",
        candidates=candidates, metadata_snapshot=snapshot,
        catalog="c", schema="s",
    )

    assert applied == 0
    desc = snapshot["data_sources"]["tables"][0]["description"]
    assert isinstance(desc, list)
    assert desc == ["PURPOSE: track sales", "DOMAIN: retail"]


# ═══════════════════════════════════════════════════════════════════════
# Lookup by short identifier (table not registered with full path)
# ═══════════════════════════════════════════════════════════════════════


def test_short_name_lookup_still_resolves_table(monkeypatch):
    _stub_side_effects(monkeypatch)
    snapshot = _snapshot_with_table(
        identifier="cat.sch.t1",
        description=["EXISTING"],
    )
    # Candidate refers to the leaf-only stem; ``by_short`` lookup
    # should still resolve it.
    candidates = [_candidate("t1", "NEW LINE")]

    applied = harness_mod._apply_instruction_table_descriptions(
        w=None, spark=None, run_id="r1", space_id="s1",
        candidates=candidates, metadata_snapshot=snapshot,
        catalog="c", schema="s",
    )

    assert applied == 1
    desc = snapshot["data_sources"]["tables"][0]["description"]
    assert isinstance(desc, list)
    assert "NEW LINE" in desc
