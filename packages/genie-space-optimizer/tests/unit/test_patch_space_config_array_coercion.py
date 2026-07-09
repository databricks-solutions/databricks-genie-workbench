"""Systemic array-field coercion at the ``patch_space_config`` choke point.

Root cause (#260 follow-up): the Genie API requires a fixed set of leaf
fields (``description`` / ``synonyms`` / ``content`` / …) to be
``list[str]`` and rejects a bare string with ``Invalid serialized_space:
Expected an array for <field>``. ``validate_serialized_space`` runs the
Pydantic ``_coerce_str_to_list`` validators but DISCARDS the coerced model
(it returns only ``(ok, errors)``), so a bare string survived into the raw
dict that ``patch_space_config`` serialized with ``json.dumps`` — local
validation passed while the server rejected the PATCH.

The fix normalizes the payload IN PLACE (``normalize_array_fields``) at the
single choke point every PATCH flows through, right before validation and
serialization. It must NOT wholesale-replace the payload with
``SerializedSpace(...).model_dump()`` — ``serialized_space`` is a
FULL-REPLACEMENT PATCH, so dropping an unmodeled field (or injecting unset
model defaults) would corrupt the space.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from genie_space_optimizer.common.genie_client import patch_space_config
from genie_space_optimizer.common.genie_schema import (
    normalize_array_fields,
    validate_serialized_space,
)

_SPACE_ID = "01f1756be5bf1db8895263c014de28c4"
_ID = "a" * 32


def _send(config: dict) -> dict:
    """Run ``patch_space_config`` with only the HTTP layer mocked; return
    the parsed ``serialized_space`` that was actually put on the wire."""
    w = MagicMock(name="w")
    captured: dict = {}

    def _do(method, path, body=None):
        assert method == "PATCH"
        assert path == f"/api/2.0/genie/spaces/{_SPACE_ID}"
        captured["sent"] = json.loads(body["serialized_space"])
        return {}

    w.api_client.do.side_effect = _do
    patch_space_config(w, _SPACE_ID, config)
    return captured["sent"]


# ═══════════════════════════════════════════════════════════════════════
# Systemic coercion — bare strings across the field class become arrays
# ═══════════════════════════════════════════════════════════════════════


class TestSystemicCoercion:
    def test_bare_description_synonyms_content_become_arrays_in_wire_payload(self):
        config = {
            "version": 2,
            "config": {
                "sample_questions": [{"id": _ID, "question": "What is revenue?"}],
            },
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.transactions",
                        "description": "Raw transactions.",  # bare string
                        "column_configs": [
                            {
                                "column_name": "amount",
                                "description": "Txn amount.",  # bare string
                                "synonyms": "value",           # bare string
                            }
                        ],
                    }
                ],
                "metric_views": [
                    {
                        "identifier": "cat.sch.mv_agg",
                        "description": "Aggregate view.",  # bare string
                    }
                ],
            },
            "instructions": {
                "text_instructions": [
                    {"id": _ID, "content": "Always filter by region."}  # bare string
                ],
            },
        }

        sent = _send(config)

        tbl = sent["data_sources"]["tables"][0]
        assert tbl["description"] == ["Raw transactions."]
        assert tbl["column_configs"][0]["description"] == ["Txn amount."]
        assert tbl["column_configs"][0]["synonyms"] == ["value"]
        assert sent["data_sources"]["metric_views"][0]["description"] == [
            "Aggregate view."
        ]
        assert sent["instructions"]["text_instructions"][0]["content"] == [
            "Always filter by region."
        ]
        assert sent["config"]["sample_questions"][0]["question"] == [
            "What is revenue?"
        ]

    def test_existing_arrays_are_left_unchanged(self):
        """Coercion is idempotent: correctly-shaped arrays pass through
        byte-for-byte (no double-wrapping)."""
        config = {
            "version": 2,
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.t",
                        "description": ["line one", "line two"],
                    }
                ],
                "metric_views": [],
            },
        }
        sent = _send(config)
        assert sent["data_sources"]["tables"][0]["description"] == [
            "line one",
            "line two",
        ]


# ═══════════════════════════════════════════════════════════════════════
# No-field-loss guard — full-replacement PATCH keeps unmodeled fields
# ═══════════════════════════════════════════════════════════════════════


class TestNoFieldLoss:
    def test_raw_space_response_is_unwrapped_before_wire_payload(self):
        """History/revert paths may pass a raw Genie Space response. The wire
        payload must still be the parsed serialized_space object, not the
        response wrapper with a nested ``serialized_space`` key."""
        raw_response = {
            "title": "Revenue Space",
            "serialized_space": {
                "version": 2,
                "data_sources": {
                    "tables": [{"identifier": "cat.sch.t"}],
                    "metric_views": [],
                },
            },
        }
        sent = _send(raw_response)
        assert sent["version"] == 2
        assert sent["data_sources"]["tables"][0]["identifier"] == "cat.sch.t"
        assert "serialized_space" not in sent
        assert "title" not in sent

    def test_unmodeled_nested_field_survives_serialization(self):
        """A field the Pydantic model does not declare must reach the wire
        unchanged — the coercion is an in-place walk, not a model round-trip
        that would silently drop it."""
        config = {
            "version": 2,
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.t",
                        "description": "bare",  # forces the coercion path
                        # Not in TableDataSource — must be preserved verbatim.
                        "future_api_field": {"nested": [1, 2, 3], "flag": True},
                    }
                ],
                "metric_views": [],
            },
        }
        sent = _send(config)
        tbl = sent["data_sources"]["tables"][0]
        assert tbl["description"] == ["bare"]
        assert tbl["future_api_field"] == {"nested": [1, 2, 3], "flag": True}

    def test_no_unset_model_defaults_are_injected(self):
        """A ``model_dump`` round-trip would inject ``description: null`` /
        ``column_configs: null`` on a table that declared neither, and
        top-level ``config``/``instructions``/``benchmarks`` nulls. The
        in-place walker must inject nothing — the sent table carries ONLY
        the keys it started with."""
        config = {
            "version": 2,
            "data_sources": {
                "tables": [{"identifier": "cat.sch.t", "only_field": "keep-me"}],
                "metric_views": [],
            },
        }
        sent = _send(config)
        tbl = sent["data_sources"]["tables"][0]
        assert tbl == {"identifier": "cat.sch.t", "only_field": "keep-me"}
        assert "description" not in tbl
        assert "column_configs" not in tbl
        # No null-valued top-level keys the model would have added.
        assert set(sent.keys()) == {"version", "data_sources"}


# ═══════════════════════════════════════════════════════════════════════
# Strict-mode tripwire — a bare string in an array field is flagged
# ═══════════════════════════════════════════════════════════════════════


class TestStrictShapeGuard:
    def test_strict_flags_bare_string_in_array_field(self):
        """The divergence that shipped the bug (local validate passed, server
        rejected) can no longer recur silently: strict validation of a raw,
        un-normalized config names the offending field."""
        config = {
            "version": 2,
            "data_sources": {
                "tables": [{"identifier": "cat.sch.t", "description": "bare"}],
                "metric_views": [],
            },
        }
        ok, errors = validate_serialized_space(config, strict=True)
        assert not ok
        joined = "\n".join(errors)
        assert "data_sources.tables[0].description" in joined
        assert "must be an array of strings" in joined

    def test_strict_passes_after_normalization(self):
        config = {
            "version": 2,
            "data_sources": {
                "tables": [{"identifier": "cat.sch.t", "description": "bare"}],
                "metric_views": [],
            },
        }
        normalize_array_fields(config)
        ok, errors = validate_serialized_space(config, strict=True)
        assert ok, errors

    def test_lenient_mode_stays_permissive_about_bare_strings(self):
        """Reads of API configs use lenient mode and must not error on a
        legacy bare string (the model layer coerces it there)."""
        config = {
            "version": 2,
            "data_sources": {
                "tables": [{"identifier": "cat.sch.t", "description": "bare"}],
                "metric_views": [],
            },
        }
        ok, _ = validate_serialized_space(config, strict=False)
        assert ok
