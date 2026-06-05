"""Trial 23 W7 — snippet repair loop unit tests.

On ``snippet_invalid`` the producer validator used to DROP the
almost-right SQL and tell the caller to "pivot to a different
mechanism". W7 instead re-prompts ONCE with the exact canonical
validator error + resolved schema, re-validates, and drops only if the
repair attempt also fails.
"""
from __future__ import annotations

from genie_space_optimizer.optimization import snippet_repair as sr


def test_repair_payload_carries_error_and_schema():
    payload = sr.build_snippet_repair_payload(
        patch_type_wire="add_sql_snippet_filter",
        patch_body={"name": "f", "sql_expression": "WHERE 1="},
        validator_error="syntax error near '='",
        schema_slice={"data_sources": {"tables": []}},
    )
    assert payload["task"] == "repair_invalid_sql_snippet"
    assert payload["patch_type"] == "add_sql_snippet_filter"
    assert payload["validator_error"] == "syntax error near '='"
    assert payload["invalid_patch_body"]["name"] == "f"
    assert "schema_slice" in payload


def test_extract_repaired_sql_from_proposal_output():
    parsed = {
        "proposals": [
            {
                "patch_type": "add_sql_snippet_filter",
                "patch_body": {
                    "name": "f",
                    "sql_expression": "amount > 0",
                },
            }
        ]
    }
    assert sr.extract_repaired_sql(parsed) == "amount > 0"


def test_extract_repaired_sql_from_example_sql_body():
    parsed = {
        "proposals": [
            {
                "patch_type": "add_example_sql",
                "patch_body": {"example_sql": "SELECT 1"},
            }
        ]
    }
    assert sr.extract_repaired_sql(parsed) == "SELECT 1"


def test_extract_repaired_sql_empty_on_garbage():
    assert sr.extract_repaired_sql(None) == ""
    assert sr.extract_repaired_sql({"proposals": []}) == ""
    assert sr.extract_repaired_sql({"proposals": [{}]}) == ""


def test_apply_repaired_sql_to_snippet_body():
    body = {"name": "f", "sql_expression": "bad"}
    out = sr.apply_repaired_sql(
        body, "amount > 0", patch_type_wire="add_sql_snippet_filter"
    )
    assert out["sql_expression"] == "amount > 0"
    # original untouched (no in-place mutation)
    assert body["sql_expression"] == "bad"


def test_apply_repaired_sql_to_example_body():
    body = {"example_question": "q", "example_sql": "bad"}
    out = sr.apply_repaired_sql(
        body, "SELECT 1", patch_type_wire="add_example_sql"
    )
    assert out["example_sql"] == "SELECT 1"


def test_marker_payload_shape():
    line = sr.snippet_repair_marker(
        optimization_run_id="run_x",
        iteration=2,
        cluster_id="H001",
        intent_id="H001_000",
        patch_type="add_sql_snippet_filter",
        outcome="repaired",
        validator_error="syntax error",
    )
    assert line.startswith("GSO_TRIAL23_SNIPPET_REPAIR_V1 ")
    import json

    payload = json.loads(line.split(" ", 1)[1])
    assert payload["outcome"] == "repaired"
    assert payload["intent_id"] == "H001_000"
    assert payload["patch_type"] == "add_sql_snippet_filter"


# ---- synthesis wiring -----------------------------------------------

# Each initial synthesis response carries the snippet proposal under
# repair PLUS a benign ``add_example_sql`` survivor so the slate never
# empties — an empty slate trips an unrelated Trial 13e contract marker
# invariant that is not what these W7 tests exercise.

def _initial_response(snippet_sql: str):
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningResponse,
    )
    return LlmReasoningResponse(
        call_id="plan11_stage3_synthesize.H001.iter_2",
        skill_id="plan11_synthesize",
        succeeded=True,
        parsed_output={
            "proposals": [
                {
                    "intent_name": "Filter",
                    "intent_description": "filter rows",
                    "repair_hypothesis": "scope the filter",
                    "patch_type": "add_sql_snippet_filter",
                    "rationale": "narrow the result",
                    "confidence": "high",
                    "patch_body": {
                        "name": "f_scope",
                        "sql_expression": snippet_sql,
                    },
                    "blame_set": [],
                    "target_qids": ["gs_009"],
                },
                {
                    "intent_name": "Example",
                    "intent_description": "exemplar",
                    "repair_hypothesis": "show pattern",
                    "patch_type": "add_example_sql",
                    "rationale": "demonstrate",
                    "confidence": "high",
                    "patch_body": {
                        "example_question": "q?",
                        "example_sql": "SELECT 1",
                    },
                    "blame_set": [],
                    "target_qids": ["gs_009"],
                },
            ],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=10,
        tokens_output=10,
        duration_ms=1,
        error=None,
    )


def _repair_response(sql_expression: str):
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningResponse,
    )
    return LlmReasoningResponse(
        call_id="plan11_snippet_repair.H001.H001_000.iter_2",
        skill_id="plan11_synthesize",
        succeeded=True,
        parsed_output={
            "proposals": [
                {
                    "patch_type": "add_sql_snippet_filter",
                    "patch_body": {
                        "name": "f_scope",
                        "sql_expression": sql_expression,
                    },
                }
            ],
        },
        declined=None,
        raw_text="{...}",
        tokens_input=10,
        tokens_output=10,
        duration_ms=1,
        error=None,
    )


def _verdict(outcome, error=""):
    from genie_space_optimizer.optimization.llm_abstain import AbstainReason
    from genie_space_optimizer.optimization.producer_snippet_validator import (
        SnippetValidatorVerdict,
    )
    return SnippetValidatorVerdict(
        outcome=outcome,
        abstain_reason=(
            None if outcome == "stamped" else AbstainReason.SNIPPET_INVALID
        ),
        error_message=error,
    )


def _content_aware_validator():
    """Decline the known-bad snippet SQL; stamp everything else.

    Mirrors the real validator's read of ``sql_expression or
    example_sql`` so the survivor example_sql and a successful repair
    both stamp, while the bad filter SQL declines.
    """
    _BAD = {"WHERE 1=", "STILL BAD"}

    def _validate(body, **kw):
        ptw = str(kw.get("patch_type_wire") or "")
        sql = str(
            (body or {}).get("sql_expression")
            or (body or {}).get("example_sql")
            or ""
        )
        if ptw == "add_sql_snippet_filter" and sql in _BAD:
            return _verdict("declined", "syntax error near '='")
        return _verdict("stamped")

    return _validate


def _cluster():
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )
    return FailureCluster(
        cluster_id="H001",
        semantic_theme="theme",
        member_qids=("gs_009",),
        unifying_evidence="evidence",
        repair_hypothesis="hyp",
        primary_blame_set=("catalog.schema.orders.order_id",),
        confidence="high",
        root_cause="extra_defensive_filter",
    )


def test_synthesis_repairs_invalid_snippet_before_dropping(capsys):
    from unittest.mock import MagicMock, patch

    invoke = MagicMock(
        side_effect=[
            _initial_response("WHERE 1="),     # initial (invalid)
            _repair_response("order_id > 0"),  # repair (valid)
        ]
    )
    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall, patch(
        "genie_space_optimizer.optimization.producer_snippet_validator."
        "validate_and_stamp_snippet_patch_body",
        side_effect=_content_aware_validator(),
    ):
        MockCall.return_value.invoke = invoke
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        run_plan11_synthesis_for_single_cluster(
            cluster=_cluster(),
            schema_slice={},
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
        )
    assert invoke.call_count == 2, (
        "W7 must issue one repair LLM call after snippet_invalid"
    )
    out = capsys.readouterr().out
    assert "GSO_TRIAL23_SNIPPET_REPAIR_V1" in out
    assert '"outcome": "repaired"' in out


def test_synthesis_drops_when_repair_still_invalid(capsys):
    from unittest.mock import MagicMock, patch

    invoke = MagicMock(
        side_effect=[
            _initial_response("WHERE 1="),   # initial (invalid)
            _repair_response("STILL BAD"),   # repair (still invalid)
        ]
    )
    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall, patch(
        "genie_space_optimizer.optimization.producer_snippet_validator."
        "validate_and_stamp_snippet_patch_body",
        side_effect=_content_aware_validator(),
    ):
        MockCall.return_value.invoke = invoke
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        run_plan11_synthesis_for_single_cluster(
            cluster=_cluster(),
            schema_slice={},
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
        )
    assert invoke.call_count == 2, "W7 attempts exactly one repair re-prompt"
    out = capsys.readouterr().out
    assert '"outcome": "repair_failed"' in out


def test_synthesis_no_repair_when_flag_off(capsys, monkeypatch):
    from unittest.mock import MagicMock, patch

    monkeypatch.setenv("GSO_TRIAL23_SNIPPET_REPAIR", "0")
    invoke = MagicMock(return_value=_initial_response("WHERE 1="))
    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall, patch(
        "genie_space_optimizer.optimization.producer_snippet_validator."
        "validate_and_stamp_snippet_patch_body",
        side_effect=_content_aware_validator(),
    ):
        MockCall.return_value.invoke = invoke
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        run_plan11_synthesis_for_single_cluster(
            cluster=_cluster(),
            schema_slice={},
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
        )
    assert invoke.call_count == 1, "rollback: no repair re-prompt when flag off"
    out = capsys.readouterr().out
    assert "GSO_TRIAL23_SNIPPET_REPAIR_V1" not in out
