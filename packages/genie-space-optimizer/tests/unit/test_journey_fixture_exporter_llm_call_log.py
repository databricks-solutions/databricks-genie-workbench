"""Phase 3.5 Task 4 — exporter serializes llm_call_log."""
from __future__ import annotations

import json

from genie_space_optimizer.optimization.journey_fixture_exporter import (
    _ALLOWED_ITERATION_KEYS,
    _ALLOWED_LLM_CALL_KEYS,
    serialize_replay_fixture,
)


def test_llm_call_log_is_whitelisted():
    assert "llm_call_log" in _ALLOWED_ITERATION_KEYS


def test_allowed_llm_call_keys_define_shape():
    required = {
        "span_name",
        "iteration",
        "ag_id",
        "cluster_id",
        "prompt_sha256",
        "system_msg",
        "prompt",
        "response_text",
        "response_metadata",
    }
    assert set(_ALLOWED_LLM_CALL_KEYS) == required


def test_serialize_preserves_llm_call_log():
    iterations_data = [
        {
            "iteration": 0,
            "eval_rows": [],
            "clusters": [],
            "soft_clusters": [],
            "strategist_response": {"action_groups": []},
            "ag_outcomes": {},
            "post_eval_passing_qids": [],
            "journey_validation": None,
            "decision_records": [],
            "llm_call_log": [
                {
                    "span_name": "stage_1_discovery",
                    "iteration": 0,
                    "ag_id": "",
                    "cluster_id": "",
                    "prompt_sha256": "a" * 64,
                    "system_msg": "sys",
                    "prompt": "stage1-prompt",
                    "response_text": '{"picks":[]}',
                    "response_metadata": {"model": "stub", "prompt_tokens": 1},
                    "ignored_extra_field": "stripped",
                },
            ],
        },
    ]
    raw = serialize_replay_fixture(
        fixture_id="t1", iterations_data=iterations_data,
    )
    fix = json.loads(raw)
    assert len(fix["iterations"]) == 1
    log = fix["iterations"][0]["llm_call_log"]
    assert len(log) == 1
    entry = log[0]
    assert entry["span_name"] == "stage_1_discovery"
    assert entry["prompt_sha256"] == "a" * 64
    assert "ignored_extra_field" not in entry


def test_malformed_call_entry_does_not_crash_export():
    iterations_data = [
        {
            "iteration": 0,
            "eval_rows": [],
            "clusters": [],
            "soft_clusters": [],
            "strategist_response": {"action_groups": []},
            "ag_outcomes": {},
            "post_eval_passing_qids": [],
            "journey_validation": None,
            "decision_records": [],
            "llm_call_log": [
                "not a dict",
                {"span_name": "stage_1_discovery"},
            ],
        },
    ]
    raw = serialize_replay_fixture(
        fixture_id="t2", iterations_data=iterations_data,
    )
    fix = json.loads(raw)
    log = fix["iterations"][0]["llm_call_log"]
    assert isinstance(log, list)
    assert any(
        e.get("span_name") == "stage_1_discovery"
        for e in log if isinstance(e, dict)
    )
