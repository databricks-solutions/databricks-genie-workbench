"""Plan 10 Phase B3 — pin ``Lever5bExampleSqlOutput`` and surface the
contract-failure path as a typed observable artefact.

Closes the 2026-05-19 ``ab65fefe`` (7now) silent-recovery anchor where
the LLM emitted ``example_sql: null``, ``_gate_parse`` coerced
``None`` -> ``""`` and rejected the proposal as a generic
``"empty example_question or example_sql"`` Gate-1 failure, the retry
slot fired with the same broken payload, and the postmortem reader had
no way to attribute the failure to the actual contract bug.

Three guards land here together because they make up one indivisible
fix:

* **The schema** rejects ``null`` for the required ``example_sql``
  field (Pydantic ``str`` is non-nullable). No ``Optional`` wrapper,
  no default.

* **The emitter helpers** (``decision_emitters.llm_contract_failure_record``
  + ``run_analysis_contract.llm_contract_failure_marker``) carry the
  raw payload + failing fields + schema name so the failure is
  unambiguous in both the in-process trace and the stdout grep path.

* **The synthesizer** (``synthesize_example_sqls``) runs the schema
  check BEFORE the legacy 5-gate path. On rejection it emits the
  stdout marker, refuses the retry, and returns ``None``.

Today: silent recovery. After Phase B3: structured decline record.
"""

from __future__ import annotations

import json
import re

import pytest

from genie_space_optimizer.optimization.leakage import BenchmarkCorpus
from genie_space_optimizer.optimization.synthesis import (
    SynthesisBudget,
    synthesize_example_sqls,
)


_BENCHMARKS = [
    {
        "id": f"q{i}",
        "question": f"Benchmark question number {i}",
        "expected_sql": f"SELECT col_{i} FROM t WHERE k = {i}",
    }
    for i in range(3)
]


_SCHEMA_SNAPSHOT = {
    "tables": [
        {
            "name": "sales",
            "column_configs": [
                {"name": "category", "type_text": "string"},
                {"name": "revenue", "type_text": "double"},
                {"name": "order_date", "type_text": "date"},
            ],
        },
    ],
}


@pytest.fixture
def corpus() -> BenchmarkCorpus:
    return BenchmarkCorpus.from_benchmarks(_BENCHMARKS)


def _parse_marker(line: str) -> dict:
    """Pull the JSON payload out of a single ``GSO_<NAME>_V1 {...}`` line."""
    match = re.match(r"GSO_LLM_CONTRACT_FAILURE_V1\s+(\{.*\})\s*$", line)
    assert match is not None, f"line does not match marker shape: {line!r}"
    return json.loads(match.group(1))


# ── Schema pin (Pydantic contract layer) ──────────────────────────────────


def test_lever5b_example_sql_schema_rejects_null_example_sql() -> None:
    """``Lever5bExampleSqlOutput.example_sql`` is ``str`` (required, not
    ``Optional``). Pydantic rejects ``None`` at validation time so the
    synthesizer's schema-check helper sees a ``ValidationError`` instead
    of a silent coerced empty string.
    """
    from genie_space_optimizer.optimization.prompt_io import (
        Lever5bExampleSqlOutput,
    )
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as excinfo:
        Lever5bExampleSqlOutput.model_validate(
            {
                "example_question": "What are top categories by revenue?",
                "example_sql": None,
            }
        )
    failing = {".".join(str(p) for p in (err.get("loc") or ())) for err in excinfo.value.errors()}
    assert "example_sql" in failing, (
        f"expected example_sql to fail validation, got loc set {failing!r}"
    )


def test_lever5b_example_sql_schema_rejects_missing_example_sql() -> None:
    """``example_sql`` has no default — omitting it is a contract bug
    too, not just emitting ``null``. Both shapes are caught at the
    same layer so the postmortem reader sees one closed failure mode.
    """
    from genie_space_optimizer.optimization.prompt_io import (
        Lever5bExampleSqlOutput,
    )
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as excinfo:
        Lever5bExampleSqlOutput.model_validate(
            {"example_question": "What are top categories by revenue?"}
        )
    failing = {".".join(str(p) for p in (err.get("loc") or ())) for err in excinfo.value.errors()}
    assert "example_sql" in failing


# ── Decision-record emitter (in-process trace layer) ──────────────────────


def test_llm_contract_failure_record_carries_schema_and_failing_fields() -> None:
    """The typed ``DecisionRecord`` carries the schema name, failing
    field paths, and the raw payload so postmortem readers can pivot
    on contract failures without parsing free-form text.
    """
    from genie_space_optimizer.optimization.decision_emitters import (
        llm_contract_failure_record,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionOutcome,
        DecisionType,
        ReasonCode,
    )

    record = llm_contract_failure_record(
        run_id="run-7now-ab65fefe",
        iteration=3,
        schema_name="Lever5bExampleSqlOutput",
        failing_fields=("example_sql",),
        raw_payload={
            "example_question": "What are top categories by revenue?",
            "example_sql": None,
        },
        skill_name="lever_5b_example_sql",
        cluster_id="C_top_n_collapse",
        ag_id="AG_DECOMPOSED_H001",
        error_repr="ValidationError(1 errors)",
    )

    assert record.decision_type == DecisionType.LLM_CONTRACT_FAILURE
    assert record.outcome == DecisionOutcome.FAILED
    assert record.reason_code == ReasonCode.LLM_CONTRACT_FAILURE
    assert record.run_id == "run-7now-ab65fefe"
    assert record.iteration == 3
    assert record.cluster_id == "C_top_n_collapse"
    assert record.ag_id == "AG_DECOMPOSED_H001"
    assert "Lever5bExampleSqlOutput" in record.reason_detail
    assert "example_sql" in record.reason_detail
    assert record.evidence_refs == ("schema:Lever5bExampleSqlOutput",)

    metrics = dict(record.metrics or {})
    assert metrics.get("schema_name") == "Lever5bExampleSqlOutput"
    assert metrics.get("skill_name") == "lever_5b_example_sql"
    assert metrics.get("failing_fields") == ["example_sql"]
    raw = dict(metrics.get("raw_payload") or {})
    assert raw.get("example_sql") is None
    assert raw.get("example_question") == "What are top categories by revenue?"


# ── Stdout marker (postmortem grep layer) ─────────────────────────────────


def test_llm_contract_failure_marker_payload_shape() -> None:
    """The stdout marker is JSON-serialisable and round-trips the same
    closed-vocabulary fields the in-process record carries. Mirrors the
    pairing pattern used by every other marker/record helper in this
    package (e.g. ``no_structural_candidate_marker`` /
    ``no_structural_candidate_record``).
    """
    from genie_space_optimizer.optimization.run_analysis_contract import (
        llm_contract_failure_marker,
    )

    line = llm_contract_failure_marker(
        schema_name="Lever5bExampleSqlOutput",
        failing_fields=("example_sql",),
        raw_payload={"example_question": "Q", "example_sql": None},
        optimization_run_id="run-X",
        iteration=2,
        cluster_id="C1",
        ag_id="AG_1",
        skill_name="lever_5b_example_sql",
        error_repr="ValidationError",
    )
    payload = _parse_marker(line)
    assert payload["schema_name"] == "Lever5bExampleSqlOutput"
    assert payload["failing_fields"] == ["example_sql"]
    assert payload["optimization_run_id"] == "run-X"
    assert payload["iteration"] == 2
    assert payload["cluster_id"] == "C1"
    assert payload["ag_id"] == "AG_1"
    assert payload["skill_name"] == "lever_5b_example_sql"
    # The shared ``marker_line`` helper coerces ``None`` -> ``""`` so the
    # rendered JSON is stable across marker emitters (run_analysis_contract
    # ``_clean``). The closed-vocabulary observation is "example_sql was
    # not a real SQL string" — both encodings (null in the proposal dict,
    # empty string in stdout) preserve that signal.
    assert payload["raw_payload"]["example_sql"] == ""
    assert payload["raw_payload"]["example_question"] == "Q"


def test_llm_contract_failure_marker_accepts_non_dict_raw_payload() -> None:
    """A non-JSON LLM response (raw string) is still observable — the
    marker wraps the free-form payload under a ``raw`` key instead of
    crashing on serialisation. Defensive for the case where the LLM
    returned plain prose (no JSON envelope) and ``_extract_json_proposal``
    returned ``None``.
    """
    from genie_space_optimizer.optimization.run_analysis_contract import (
        llm_contract_failure_marker,
    )

    line = llm_contract_failure_marker(
        schema_name="Lever5bExampleSqlOutput",
        failing_fields=("__root__",),
        raw_payload="not even close to JSON",
    )
    payload = _parse_marker(line)
    assert payload["raw_payload"] == {"raw": "not even close to JSON"}


# ── Synthesizer wire-in (end-to-end contract layer) ──────────────────────


def test_synthesize_example_sqls_returns_none_and_emits_marker_on_null_example_sql(
    corpus: BenchmarkCorpus,
    capsys: pytest.CaptureFixture,
) -> None:
    """End-to-end: when the LLM returns ``example_sql: null`` the
    synthesizer no longer silently recovers via the 5-gate path. It
    emits a ``GSO_LLM_CONTRACT_FAILURE_V1`` stdout marker, refuses to
    burn the retry slot on the same broken payload, and returns
    ``None`` so the caller falls through to its contract-failure
    handling path.
    """
    cluster = {
        "cluster_id": "C_contract_failure",
        "root_cause": "wrong_aggregation",
        "question_ids": ["q1", "q2"],
        "asi_blame_set": ["sales.revenue"],
    }

    attempts = {"n": 0}

    def fake_llm(prompt: str) -> str:
        attempts["n"] += 1
        return json.dumps(
            {
                "example_question": "What are top categories by revenue?",
                "example_sql": None,
            }
        )

    budget = SynthesisBudget.new()
    result = synthesize_example_sqls(
        cluster,
        _SCHEMA_SNAPSHOT,
        corpus,
        budget=budget,
        llm_caller=fake_llm,
    )

    assert result is None, (
        "synthesize_example_sqls must return None on Pydantic rejection — "
        "the legacy 5-gate retry path is bypassed for contract failures"
    )
    assert attempts["n"] == 1, (
        "the retry slot must NOT fire on contract failure; previously "
        "the broad except Exception in _call_llm swallowed the "
        "ValidationError and burned the retry on the same broken payload"
    )
    assert budget.consecutive_failures >= 1, (
        "consecutive_failures must advance so the fallback policy can "
        "downgrade synthesis after repeated contract bugs"
    )

    captured = capsys.readouterr().out
    marker_lines = [
        ln for ln in captured.splitlines()
        if ln.startswith("GSO_LLM_CONTRACT_FAILURE_V1 ")
    ]
    assert len(marker_lines) == 1, (
        "exactly one GSO_LLM_CONTRACT_FAILURE_V1 marker must surface in "
        f"stdout; captured: {captured!r}"
    )
    payload = _parse_marker(marker_lines[0])
    assert payload["schema_name"] == "Lever5bExampleSqlOutput"
    assert payload["cluster_id"] == "C_contract_failure"
    assert payload["skill_name"] == "lever_5b_example_sql"
    assert "example_sql" in payload["failing_fields"]
    # ``marker_line`` coerces ``None`` -> ``""`` for stable stdout JSON;
    # the closed-vocabulary signal "example_sql was not a real SQL string"
    # is preserved either way.
    assert payload["raw_payload"]["example_sql"] == ""
