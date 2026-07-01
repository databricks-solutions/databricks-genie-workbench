"""Unit tests for ``_apply_instruction_table_descriptions``.

Genie's serialized space stores a table / metric-view ``description`` as
``list[str]`` and the API rejects a bare string with ``Expected an array
for description``. The function must therefore ALWAYS write back
``list[str]``, regardless of the shape of the existing value:

  * Empty description (``""`` / absent) → the append text wrapped in a
    one-element ``list[str]``. This empty-base branch was the origin of
    the #260 follow-up bug: the old ``else`` path assigned the bare
    ``new_desc`` string, poisoning the shared ``metadata_snapshot`` that
    later PATCHes re-serialize (see ``TestPoisonedSharedSnapshot``).
  * String description → merged into one string, wrapped in ``list[str]``.
  * List description → the append text added as a new list element.
  * Idempotency → re-running with the same span is a no-op (the existing
    value, whatever its shape, is left untouched).

An earlier variant of the bug wrote a Python ``repr`` of the list back as
a string (``"['PURPOSE:…']"``); the list-append path guards against that
too.
"""

from __future__ import annotations

import copy
import json
from unittest.mock import MagicMock

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
# Empty description — the empty-base branch (origin of the #260 follow-up)
# ═══════════════════════════════════════════════════════════════════════


def test_empty_description_writes_single_element_list(monkeypatch):
    """Empty-base regression: a table with NO existing description must get
    a ``list[str]`` — not the bare ``new_desc`` string the old ``else``
    branch assigned (which the Genie API rejects with ``Expected an array
    for description``)."""
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
    assert isinstance(desc, list), (
        f"empty-base description must be list[str], got {type(desc).__name__}: {desc!r}"
    )
    assert desc == ["PURPOSE: track sales"]


def test_absent_description_writes_single_element_list(monkeypatch):
    """Same empty-base contract when the ``description`` key is entirely
    absent (the exact live shape for a freshly-added metric view)."""
    _stub_side_effects(monkeypatch)
    snapshot = _snapshot_with_table(description=None)  # key omitted
    candidates = [_candidate("cat.sch.t1", "PURPOSE: track sales")]

    applied = harness_mod._apply_instruction_table_descriptions(
        w=None, spark=None, run_id="r1", space_id="s1",
        candidates=candidates, metadata_snapshot=snapshot,
        catalog="c", schema="s",
    )

    assert applied == 1
    desc = snapshot["data_sources"]["tables"][0]["description"]
    assert desc == ["PURPOSE: track sales"]


# ═══════════════════════════════════════════════════════════════════════
# String description — legacy string base is normalized to list[str]
# ═══════════════════════════════════════════════════════════════════════


def test_string_description_merges_into_list(monkeypatch):
    """A legacy bare-string description is merged with the append text and
    written back as ``list[str]`` (the API contract) — never a bare string."""
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
    assert isinstance(desc, list)
    merged = "\n".join(desc)
    assert "Existing prose." in merged
    assert "PURPOSE: track sales" in merged


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


def test_empty_description_metric_view_writes_list(monkeypatch):
    """The exact failing shape from the #260 follow-up: a metric view with
    NO prior description (``mv_banking_transactions``). It must receive a
    ``list[str]`` — the old ``else`` branch wrote the bare append string,
    which the API rejected with ``Expected an array for description``."""
    _stub_side_effects(monkeypatch)
    snapshot = _snapshot_with_table(
        identifier="cat.sch.mv_banking_transactions",
        description=None,
        is_metric_view=True,
    )
    append = (
        "Prefer this materialized view for aggregate questions over dates "
        "or periods when it covers the requested dimensions."
    )
    candidates = [_candidate("cat.sch.mv_banking_transactions", append)]

    applied = harness_mod._apply_instruction_table_descriptions(
        w=None, spark=None, run_id="r1", space_id="s1",
        candidates=candidates, metadata_snapshot=snapshot,
        catalog="c", schema="s",
    )

    assert applied == 1
    desc = snapshot["data_sources"]["metric_views"][0]["description"]
    assert isinstance(desc, list), (
        f"empty-base MV description must be list[str], got {type(desc).__name__}"
    )
    assert desc == [append]


# ═══════════════════════════════════════════════════════════════════════
# Poisoned-shared-dict regression — the whole #260 follow-up failure mode
# ═══════════════════════════════════════════════════════════════════════


def _shared_snapshot() -> dict:
    """A strict-valid snapshot with a metric view that has NO description
    and one table with one column — the shape the three-in-sequence
    enrichment PATCHes all mutate in place."""
    return {
        "version": 2,
        "data_sources": {
            "tables": [
                {
                    "identifier": "cat.sch.transactions",
                    "name": "transactions",
                    "column_configs": [{"column_name": "amount"}],
                },
            ],
            "metric_views": [
                {
                    "identifier": "cat.sch.mv_banking_transactions",
                    "name": "mv_banking_transactions",
                    "column_configs": [],
                    # NB: NO ``description`` — the empty-base case.
                },
            ],
        },
        "instructions": {"join_specs": []},
    }


def _capturing_w():
    """A WorkspaceClient double whose ``api_client.do`` records the parsed
    ``serialized_space`` of every PATCH body it is handed."""
    w = MagicMock(name="w")
    sent: list[dict] = []

    def _do(method, path, body=None):
        assert method == "PATCH"
        sent.append(json.loads(body["serialized_space"]))
        return {}

    w.api_client.do.side_effect = _do
    return w, sent


class TestPoisonedSharedSnapshot:
    """Root cause: three enrichment steps share ONE ``metadata_snapshot``
    and none re-fetches. A bare-string description written by step 1
    poisoned it for steps 2 and 3, whose PATCHes (which only write arrays
    themselves) were rejected too. Here the REAL ``patch_space_config``
    choke point is exercised (only ``w.api_client.do`` is mocked), so the
    test proves both the per-site fix (step 1 writes a list) and the
    systemic coercion (choke point) keep every one of the three PATCH
    payloads array-shaped."""

    def test_all_three_patches_carry_array_descriptions(self, monkeypatch):
        monkeypatch.setattr(harness_mod, "write_stage", lambda *a, **k: None)
        w, sent = _capturing_w()
        shared = _shared_snapshot()
        space_id = "01f1756be5bf1db8895263c014de28c4"

        # Step 1 (ORIGIN): append a description to the description-less MV.
        mv_append = (
            "Prefer this materialized view for aggregate questions over "
            "dates or periods when it covers the requested dimensions; fall "
            "back to the raw transactions table only when needed dimensions "
            "are absent."
        )
        desc_applied = harness_mod._apply_instruction_table_descriptions(
            w=w, spark=MagicMock(), run_id="r1", space_id=space_id,
            candidates=[_candidate("cat.sch.mv_banking_transactions", mv_append)],
            metadata_snapshot=shared, catalog="c", schema="s",
        )
        assert desc_applied == 1

        # Step 2 (VICTIM): add a column synonym on the table. This PATCH
        # only writes arrays itself, yet re-serializes the whole shared
        # snapshot — including the MV description from step 1.
        syn_applied = harness_mod._apply_instruction_column_synonyms(
            w=w, spark=MagicMock(), run_id="r1", space_id=space_id,
            candidates=[
                {
                    "table_identifier": "cat.sch.transactions",
                    "column_name": "amount",
                    "synonyms": ["value", "txn amount"],
                }
            ],
            metadata_snapshot=shared, catalog="c", schema="s",
        )
        assert syn_applied == 1

        # Step 3 (VICTIM): the prose-rewrite step re-sends the same shared
        # snapshot verbatim via the same choke point.
        from genie_space_optimizer.common.genie_client import patch_space_config
        patch_space_config(w, space_id, shared)

        # All three PATCHes reached the API layer...
        assert len(sent) == 3, f"expected 3 PATCHes, got {len(sent)}"

        # ...and NONE would be rejected: every table/MV description in every
        # payload is an array (never a bare string).
        for i, payload in enumerate(sent):
            ds = payload["data_sources"]
            for source_key in ("tables", "metric_views"):
                for tbl in ds.get(source_key, []):
                    desc = tbl.get("description")
                    assert desc is None or isinstance(desc, list), (
                        f"PATCH #{i} {source_key} {tbl.get('identifier')!r} "
                        f"description is a bare string: {desc!r}"
                    )
            # The exact field the live API named in the rejection.
            mv = ds["metric_views"][0]
            assert mv["description"] == [mv_append]

    def test_strict_validator_accepts_every_sent_payload(self, monkeypatch):
        """The bare-string class is gone from the sent payloads: strict
        validation raises no ``must be an array`` error for any of the
        three PATCHes."""
        from genie_space_optimizer.common.genie_schema import (
            validate_serialized_space,
        )

        monkeypatch.setattr(harness_mod, "write_stage", lambda *a, **k: None)
        w, sent = _capturing_w()
        shared = _shared_snapshot()
        space_id = "01f1756be5bf1db8895263c014de28c4"

        harness_mod._apply_instruction_table_descriptions(
            w=w, spark=MagicMock(), run_id="r1", space_id=space_id,
            candidates=[_candidate("cat.sch.mv_banking_transactions", "AGG view")],
            metadata_snapshot=shared, catalog="c", schema="s",
        )
        for payload in sent:
            ok, errors = validate_serialized_space(copy.deepcopy(payload), strict=True)
            array_errors = [e for e in errors if "must be an array" in e]
            assert not array_errors, f"bare-string array field survived: {array_errors}"
            assert ok, f"sent payload must be strict-valid; got: {errors}"
