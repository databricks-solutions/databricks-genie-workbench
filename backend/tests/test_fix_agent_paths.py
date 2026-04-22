"""Tests for config path navigation, ID sanitization, and apply-with-retry (backend/services/fix_agent.py).

Tests _get_value_at_path(), _set_value_at_path(), _sanitize_ids (pure functions),
and _apply_config_sync (retry logic, mocked).
"""

import asyncio
import json
import re
from unittest.mock import MagicMock, patch, call

import pytest

from backend.services.fix_agent import (
    FixAgent,
    _get_value_at_path,
    _parse_patches,
    _set_value_at_path,
    _sanitize_ids,
)


# ---------------------------------------------------------------------------
# Sample config fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return {
        "name": "My Space",
        "data_sources": {
            "tables": [
                {"name": "orders", "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "amount", "type": "decimal"},
                ]},
                {"name": "customers", "columns": [
                    {"name": "id", "type": "bigint"},
                ]},
            ]
        },
        "instructions": {
            "sql_snippets": {
                "filters": [
                    {"id": "f0", "sql": ["WHERE x=1"]},
                    {"id": "f1", "sql": ["WHERE y=2"]},
                    {"id": "f2", "sql": ["WHERE z=3", "AND w=4"]},
                ]
            }
        }
    }


# ---------------------------------------------------------------------------
# _get_value_at_path
# ---------------------------------------------------------------------------

class TestGetValueAtPath:
    def test_top_level_key(self, config):
        assert _get_value_at_path(config, "name") == "My Space"

    def test_nested_key(self, config):
        tables = _get_value_at_path(config, "data_sources.tables")
        assert isinstance(tables, list)
        assert len(tables) == 2

    def test_array_index(self, config):
        name = _get_value_at_path(config, "data_sources.tables[0].name")
        assert name == "orders"

    def test_second_array_index(self, config):
        name = _get_value_at_path(config, "data_sources.tables[1].name")
        assert name == "customers"

    def test_deep_nesting(self, config):
        val = _get_value_at_path(config, "instructions.sql_snippets.filters[2].sql[0]")
        assert val == "WHERE z=3"

    def test_missing_key_returns_none(self, config):
        assert _get_value_at_path(config, "nonexistent") is None

    def test_missing_nested_key_returns_none(self, config):
        assert _get_value_at_path(config, "data_sources.missing.deep") is None

    def test_out_of_bounds_index_returns_none(self, config):
        assert _get_value_at_path(config, "data_sources.tables[99]") is None


# ---------------------------------------------------------------------------
# _set_value_at_path
# ---------------------------------------------------------------------------

class TestSetValueAtPath:
    def test_set_existing_key(self, config):
        _set_value_at_path(config, "name", "New Name")
        assert config["name"] == "New Name"

    def test_set_nested_existing_key(self, config):
        _set_value_at_path(config, "data_sources.tables[0].name", "renamed")
        assert config["data_sources"]["tables"][0]["name"] == "renamed"

    def test_auto_creates_intermediate_dicts(self):
        config = {}
        _set_value_at_path(config, "a.b.c", "deep")
        assert config["a"]["b"]["c"] == "deep"

    def test_auto_creates_intermediate_lists(self):
        config = {}
        _set_value_at_path(config, "items[0]", "first")
        assert config["items"][0] == "first"

    def test_auto_extends_list(self):
        config = {"items": ["a"]}
        _set_value_at_path(config, "items[2]", "c")
        assert config["items"] == ["a", None, "c"]

    def test_mixed_dict_and_array_creation(self):
        config = {}
        _set_value_at_path(config, "data.items[0].name", "first")
        assert config["data"]["items"][0]["name"] == "first"

    def test_set_join_spec_left_as_object(self):
        config = {"instructions": {}}
        _set_value_at_path(config, "instructions.join_specs[0].left",
                           {"identifier": "c.s.orders", "alias": "orders"})
        left = config["instructions"]["join_specs"][0]["left"]
        assert left["identifier"] == "c.s.orders"
        assert left["alias"] == "orders"

    def test_set_column_config_creates_entry_without_column_name(self):
        """When the LLM patches a column_config at a new index,
        _set_value_at_path creates the entry with only the patched field.
        column_name will be missing — the apply path must handle this."""
        config = {"data_sources": {"tables": [
            {"identifier": "c.s.t1", "column_configs": [
                {"column_name": "col_a", "description": ["existing"]},
            ]}
        ]}}
        _set_value_at_path(config, "data_sources.tables[0].column_configs[1].description",
                           ["new desc"])
        cc = config["data_sources"]["tables"][0]["column_configs"][1]
        assert cc["description"] == ["new desc"]
        assert "column_name" not in cc  # no column_name — apply path must clean this

    def test_set_join_spec_right_after_left(self):
        config = {"instructions": {"join_specs": [
            {"left": {"identifier": "c.s.orders", "alias": "orders"}}
        ]}}
        _set_value_at_path(config, "instructions.join_specs[0].right",
                           {"identifier": "c.s.customers", "alias": "customers"})
        js = config["instructions"]["join_specs"][0]
        assert js["left"]["identifier"] == "c.s.orders"
        assert js["right"]["identifier"] == "c.s.customers"

    def test_mutates_in_place(self, config):
        original_id = id(config)
        _set_value_at_path(config, "name", "changed")
        assert id(config) == original_id

    def test_set_deep_sql_value(self, config):
        _set_value_at_path(config, "instructions.sql_snippets.filters[1].sql[0]", "WHERE new=1")
        assert config["instructions"]["sql_snippets"]["filters"][1]["sql"][0] == "WHERE new=1"


# ---------------------------------------------------------------------------
# _sanitize_ids
# ---------------------------------------------------------------------------

HEX32 = re.compile(r"^[0-9a-f]{32}$")


class TestSanitizeIds:
    def test_valid_id_unchanged(self):
        config = {"id": "a1b2c3d4e5f60000000000000000000a", "name": "test"}
        _sanitize_ids(config)
        assert config["id"] == "a1b2c3d4e5f60000000000000000000a"

    def test_invalid_hex_chars_replaced(self):
        bad_id = "2e41b27gb76746g438d3d00ff5e27fa03"  # contains 'g'
        config = {"id": bad_id, "name": "test"}
        _sanitize_ids(config)
        assert config["id"] != bad_id
        assert HEX32.match(config["id"])

    def test_hyphenated_uuid_replaced(self):
        config = {"id": "2e41b27b-b767-46e4-38d3-d00ff5e27fa0"}
        _sanitize_ids(config)
        assert HEX32.match(config["id"])

    def test_too_short_id_replaced(self):
        config = {"id": "abc123"}
        _sanitize_ids(config)
        assert HEX32.match(config["id"])

    def test_nested_ids_fixed(self):
        config = {
            "benchmarks": {
                "questions": [
                    {"id": "INVALID_NOT_HEX_AT_ALL_32CHARS!!", "question": ["Q1"]},
                    {"id": "a1b2c3d4e5f60000000000000000000b", "question": ["Q2"]},
                ]
            }
        }
        _sanitize_ids(config)
        assert HEX32.match(config["benchmarks"]["questions"][0]["id"])
        # Valid ID should be unchanged
        assert config["benchmarks"]["questions"][1]["id"] == "a1b2c3d4e5f60000000000000000000b"

    def test_non_id_fields_untouched(self):
        config = {"name": "NOT_HEX", "identifier": "catalog.schema.table"}
        _sanitize_ids(config)
        assert config["name"] == "NOT_HEX"
        assert config["identifier"] == "catalog.schema.table"

    def test_none_id_replaced(self):
        config = {"id": None, "name": "test"}
        _sanitize_ids(config)
        assert HEX32.match(config["id"])

    def test_empty_string_id_replaced(self):
        config = {"id": "", "name": "test"}
        _sanitize_ids(config)
        assert HEX32.match(config["id"])

    def test_missing_id_injected_in_known_array(self):
        config = {
            "instructions": {
                "example_question_sqls": [
                    {"question": ["What is X?"], "sql": ["SELECT 1"]},
                ]
            }
        }
        _sanitize_ids(config)
        entry = config["instructions"]["example_question_sqls"][0]
        assert "id" in entry
        assert HEX32.match(entry["id"])

    def test_missing_id_not_injected_in_unknown_array(self):
        config = {
            "some_custom_field": [
                {"name": "test"},
            ]
        }
        _sanitize_ids(config)
        assert "id" not in config["some_custom_field"][0]


# ---------------------------------------------------------------------------
# _apply_config_sync  (retry logic)
# ---------------------------------------------------------------------------

def _make_fresh_config():
    """Minimal valid config returned by get_serialized_space."""
    return {"version": 2, "instructions": {"text_instructions": [
        {"id": "a" * 32, "content": ["old instruction\n"]}
    ]}}


_PATCHES = [{"field_path": "instructions.text_instructions[0].content", "new_value": ["new instruction\n"]}]


def _retry_sleep_calls(mock_sleep):
    """Extract only the retry-delay sleep calls (2s, 4s), ignoring MLflow internals."""
    return [c for c in mock_sleep.call_args_list if c[0][0] in (2, 4)]


@patch("backend.services.fix_agent.time.sleep")
@patch("backend.services.fix_agent.get_workspace_client")
@patch("backend.services.genie_client.get_genie_space")
class TestApplyConfigSyncRetry:
    """Tests for the retry loop in _apply_config_sync."""

    def _call(self, space_id, patches):
        from backend.services.fix_agent import _apply_config_sync
        _apply_config_sync(space_id, patches)

    def test_success_first_attempt_no_retry(self, mock_get_space, mock_ws, mock_sleep):
        import json
        mock_get_space.return_value = {"serialized_space": json.dumps(_make_fresh_config())}
        mock_ws.return_value.api_client.do = MagicMock()

        self._call("space123", _PATCHES)

        assert mock_get_space.call_count == 1
        assert _retry_sleep_calls(mock_sleep) == []
        mock_ws.return_value.api_client.do.assert_called_once()

    def test_retries_on_failure_then_succeeds(self, mock_get_space, mock_ws, mock_sleep):
        import json
        mock_get_space.return_value = {"serialized_space": json.dumps(_make_fresh_config())}
        mock_do = mock_ws.return_value.api_client.do
        mock_do.side_effect = [
            RuntimeError("Space configuration has been modified"),
            None,  # second attempt succeeds
        ]

        self._call("space123", _PATCHES)

        assert mock_get_space.call_count == 2  # re-fetched on retry
        assert mock_do.call_count == 2
        assert _retry_sleep_calls(mock_sleep) == [call(2)]

    def test_all_attempts_exhausted_raises(self, mock_get_space, mock_ws, mock_sleep):
        import json
        mock_get_space.return_value = {"serialized_space": json.dumps(_make_fresh_config())}
        err = RuntimeError("Space configuration has been modified")
        mock_ws.return_value.api_client.do.side_effect = err

        with pytest.raises(RuntimeError, match="modified"):
            self._call("space123", _PATCHES)

        assert mock_get_space.call_count == 3
        assert mock_ws.return_value.api_client.do.call_count == 3
        assert _retry_sleep_calls(mock_sleep) == [call(2), call(4)]

    def test_refetches_fresh_config_each_attempt(self, mock_get_space, mock_ws, mock_sleep):
        """Each retry calls get_serialized_space again — not reusing stale data."""
        import json
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            cfg = _make_fresh_config()
            cfg["_call"] = call_count
            return {"serialized_space": json.dumps(cfg)}

        mock_get_space.side_effect = side_effect
        mock_ws.return_value.api_client.do.side_effect = [
            RuntimeError("modified"),
            RuntimeError("modified"),
            None,
        ]

        self._call("space123", _PATCHES)

        assert mock_get_space.call_count == 3

    def test_deduplicates_instruction_ids(self, mock_get_space, mock_ws, mock_sleep):
        """Duplicate instruction IDs in example_question_sqls are stripped before PATCH."""
        import json
        dup_id = "d" * 32  # must differ from _make_fresh_config's text_instruction id
        cfg = _make_fresh_config()
        cfg["instructions"]["example_question_sqls"] = [
            {"id": "b" * 32, "question": ["Q1"], "sql": ["SELECT 1"]},
            {"id": dup_id, "question": ["Q2"], "sql": ["SELECT 2"]},
            {"id": dup_id, "question": ["Q3"], "sql": ["SELECT 3"]},  # duplicate
        ]
        mock_get_space.return_value = {"serialized_space": json.dumps(cfg)}
        mock_do = mock_ws.return_value.api_client.do
        mock_do.return_value = None

        self._call("space123", [])

        # Inspect the serialized_space sent to the API
        patch_body = mock_do.call_args[1]["body"]
        patched_config = json.loads(patch_body["serialized_space"])
        sqls = patched_config["instructions"]["example_question_sqls"]
        ids = [e["id"] for e in sqls]
        assert ids.count(dup_id) == 1  # duplicate removed
        assert len(sqls) == 2

    def test_deduplicates_question_ids_across_sections(self, mock_get_space, mock_ws, mock_sleep):
        """Same question ID in sample_questions and benchmarks.questions is deduped cross-section."""
        import json
        shared_id = "e" * 32  # valid 32-char hex
        other_id = "f" * 32
        cfg = _make_fresh_config()
        cfg["config"] = {"sample_questions": [
            {"id": shared_id, "question": "What is revenue?"},
        ]}
        cfg["benchmarks"] = {"questions": [
            {"id": shared_id, "question": "What is revenue?"},  # cross-section dup
            {"id": other_id, "question": "What is cost?"},
        ]}
        mock_get_space.return_value = {"serialized_space": json.dumps(cfg)}
        mock_do = mock_ws.return_value.api_client.do
        mock_do.return_value = None

        self._call("space123", [])

        patch_body = mock_do.call_args[1]["body"]
        patched_config = json.loads(patch_body["serialized_space"])
        sq_ids = [e["id"] for e in patched_config["config"]["sample_questions"]]
        bq_ids = [e["id"] for e in patched_config["benchmarks"]["questions"]]
        all_ids = sq_ids + bq_ids
        assert all_ids.count(shared_id) == 1  # kept in sample_questions, removed from benchmarks
        assert len(patched_config["benchmarks"]["questions"]) == 1  # only other_id remains


# ---------------------------------------------------------------------------
# GSL section-header preservation: _parse_patches and run() (near-term, epic #87)
# ---------------------------------------------------------------------------

class TestParsePatchesDecline:
    """_parse_patches must handle the decline shape emitted when a fix would
    erase a canonical GSL section header in text_instructions.content."""

    def test_decline_true_returns_marker_patch(self):
        content = json.dumps({
            "decline": True,
            "rationale": "Applying this fix would erase the ## PURPOSE header.",
        })
        patches = _parse_patches(content)
        assert len(patches) == 1
        assert patches[0]["declined"] is True
        assert patches[0]["field_path"] == ""
        assert patches[0]["new_value"] is None
        assert "## PURPOSE" in patches[0]["rationale"]

    def test_decline_true_without_rationale_uses_fallback(self):
        content = json.dumps({"decline": True})
        patches = _parse_patches(content)
        assert len(patches) == 1
        assert patches[0]["declined"] is True
        assert patches[0]["rationale"]  # non-empty fallback

    def test_decline_false_is_not_a_decline(self):
        """Only `decline: true` triggers the decline path — `decline: false` is treated
        as missing; falls through to normal parsing."""
        content = json.dumps({"decline": False, "field_path": "x.y", "new_value": "v", "rationale": "r"})
        patches = _parse_patches(content)
        assert len(patches) == 1
        assert patches[0].get("declined") is not True
        assert patches[0]["field_path"] == "x.y"

    def test_normal_patch_unaffected(self):
        content = json.dumps({"field_path": "x.y", "new_value": "v", "rationale": "r"})
        patches = _parse_patches(content)
        assert len(patches) == 1
        assert patches[0].get("declined") is not True
        assert patches[0]["field_path"] == "x.y"

    def test_patches_array_unaffected(self):
        content = json.dumps({"patches": [
            {"field_path": "a.b", "new_value": "1", "rationale": "r1"},
            {"field_path": "c.d", "new_value": "2", "rationale": "r2"},
        ]})
        patches = _parse_patches(content)
        assert len(patches) == 2
        assert all(p.get("declined") is not True for p in patches)


class TestFixAgentRunSkippedEvent:
    """FixAgent.run must emit {status: "skipped", ...} carrying the LLM's rationale
    when a patch is declined — not try to apply, not report success as a silent no-op."""

    def _make_agent(self) -> FixAgent:
        agent = FixAgent.__new__(FixAgent)
        agent.model = "test-model"
        return agent

    def _collect_events(self, agent: FixAgent, space_id: str, findings: list[str], space_config: dict) -> list[dict]:
        async def _drain():
            events = []
            async for ev in agent.run(space_id=space_id, findings=findings, space_config=space_config):
                events.append(ev)
            return events
        return asyncio.run(_drain())

    @patch("backend.services.fix_agent._apply_config_to_databricks")
    @patch("backend.services.fix_agent._generate_patches_for_finding")
    def test_declined_patch_emits_skipped_event(self, mock_gen, mock_apply):
        mock_gen.return_value = [{
            "field_path": "",
            "new_value": None,
            "rationale": "Applying this fix would erase the ## PURPOSE header.",
            "declined": True,
        }]
        events = self._collect_events(
            self._make_agent(),
            space_id="s1",
            findings=["text instructions too brief"],
            space_config={"instructions": {}, "data_sources": {"tables": []}},
        )

        skipped = [e for e in events if e.get("status") == "skipped"]
        assert len(skipped) == 1, f"Expected one skipped event, got {[e.get('status') for e in events]}"
        assert "## PURPOSE" in skipped[0]["rationale"]
        assert skipped[0]["field_path"] == ""
        # No config was applied
        mock_apply.assert_not_called()

    @patch("backend.services.fix_agent._apply_config_to_databricks")
    @patch("backend.services.fix_agent._generate_patches_for_finding")
    def test_normal_patch_still_emits_patch_event(self, mock_gen, mock_apply):
        """Smoke: a normal (non-declined) patch is applied and emits status=patch, not skipped."""
        mock_gen.return_value = [{
            "field_path": "data_sources.tables[0].description",
            "new_value": ["New description"],
            "rationale": "Add description for the orders table.",
        }]
        async def noop(space_id, patches):
            return None
        mock_apply.side_effect = noop

        events = self._collect_events(
            self._make_agent(),
            space_id="s1",
            findings=["orders table has no description"],
            space_config={
                "instructions": {},
                "data_sources": {"tables": [{"identifier": "c.s.orders"}]},
            },
        )

        statuses = [e.get("status") for e in events]
        assert "patch" in statuses
        assert "skipped" not in statuses

    @patch("backend.services.fix_agent._apply_config_to_databricks")
    @patch("backend.services.fix_agent._generate_patches_for_finding")
    def test_legacy_empty_field_path_emits_skipped_not_patch(self, mock_gen, mock_apply):
        """If the LLM picks the legacy no-patch shape (empty field_path, no
        `declined` flag) — e.g. for a GSL section-erasure case where it did not
        read the decline instruction — run() must still normalize to skipped
        so the UI surfaces the rationale and phase resolves to 'complete'.
        Without this guard, the loop yields a synthetic status=patch event
        with empty field_path, dropping the rationale and flipping the panel
        to error."""
        mock_gen.return_value = [{
            "field_path": "",
            "new_value": None,
            "rationale": "Would erase the ## PURPOSE header; no other field covers this finding.",
        }]

        events = self._collect_events(
            self._make_agent(),
            space_id="s1",
            findings=["text instructions too brief"],
            space_config={"instructions": {}, "data_sources": {"tables": []}},
        )

        statuses = [e.get("status") for e in events]
        assert "skipped" in statuses, f"Expected 'skipped', got {statuses}"
        # No synthetic patch event with empty field_path
        patch_events = [e for e in events if e.get("status") == "patch"]
        assert not patch_events, (
            f"Legacy empty-field_path shape must not surface as a 'patch' event, "
            f"got: {patch_events}"
        )
        # Rationale is carried through to the skipped event
        skipped = [e for e in events if e.get("status") == "skipped"]
        assert "## PURPOSE" in skipped[0]["rationale"]
        # Nothing was applied to Databricks
        mock_apply.assert_not_called()
