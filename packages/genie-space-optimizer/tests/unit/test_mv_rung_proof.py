"""The join-rung bodies proven live in Unity Catalog still render (MV-D124).

Each body in ``data/mv_rung_proof_7eeb5f5b.json`` was created on a SQL warehouse
and passed every check in its ``proven_at`` block. ``MV_PROVEN_JOIN_STRATEGIES``
lets the create paths accept those strategies only because of that proof, so the
proof holds only while ``generate`` renders the same bytes from the same inputs.

The rule: a renderer change that alters a proven body means re-proving that rung
live, or removing it from ``MV_PROVEN_JOIN_STRATEGIES`` and deleting its golden.
Never re-capture a golden without a live proof.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from genie_space_optimizer.common.config import (
    MV_CAPABILITY_NESTED_JOINS,
    MV_JOIN_STRATEGY_DIRECT,
    MV_JOIN_STRATEGY_NESTED,
    MV_JOIN_STRATEGY_SUBQUERY,
    MV_PROVEN_JOIN_STRATEGIES,
)
from genie_space_optimizer.optimization.mv_scoring import MetricViewCandidate
from genie_space_optimizer.optimization.mv_yaml import (
    ColumnFacts,
    JoinHop,
    KeyUniqueness,
    MeasureRequest,
    MvProfiling,
    RequestedAttribute,
    generate,
    validate,
)

GOLDEN_PATH = Path(__file__).parent / "data" / "mv_rung_proof_7eeb5f5b.json"
_LIVE_CHECKS = (
    "body_identical",
    "create_ddl",
    "confirm_metric_view",
    "describe_json_metric_view",
    "measures_match_plain_sql",
    "fan_out_smoke",
    "deepest_attribute_groups",
    "trap_defeated",
    "existing_view_matches",
)


def _golden() -> dict:
    return json.loads(GOLDEN_PATH.read_text())


def _rungs() -> list[dict]:
    return _golden()["rungs"]


def _rung(strategy: str) -> dict:
    return next(r for r in _rungs() if r["strategy"] == strategy)


def _profiling(d: dict) -> MvProfiling:
    return MvProfiling(
        source_table=d["source_table"],
        table_columns={
            t: tuple(ColumnFacts(**c) for c in cols) for t, cols in d["table_columns"].items()
        },
        uniqueness={(u["table"], u["column"]): KeyUniqueness(**u) for u in d["uniqueness"]},
        hops=tuple(JoinHop(**h) for h in d["hops"]),
        attributes=tuple(
            RequestedAttribute(**{**a, "synonyms": tuple(a["synonyms"])}) for a in d["attributes"]
        ),
        measures=tuple(
            MeasureRequest(**{**m, "synonyms": tuple(m["synonyms"])}) for m in d["measures"]
        ),
        capabilities=dict(d["capabilities"]),
        domain=d["domain"],
        row_counts=dict(d["row_counts"]),
    )


def _candidate(d: dict) -> MetricViewCandidate:
    return MetricViewCandidate(
        space_id=d["space_id"],
        concept=d["concept"],
        measure_expr=d["measure_expr"],
        source_tables=tuple(d["source_tables"]),
        benchmark_question_ids=tuple(d["benchmark_question_ids"]),
    )


@pytest.mark.parametrize("strategy", [r["strategy"] for r in _rungs()])
def test_each_proven_body_regenerates_byte_for_byte(strategy):
    rung = _rung(strategy)
    result = generate(_candidate(rung["candidate"]), _profiling(rung["profiling"]))
    assert result.ok, result.rejections
    assert result.join_strategy == rung["strategy"]
    assert result.yaml_text == rung["yaml_text"]


def test_every_proven_strategy_has_a_golden_and_every_golden_is_proven():
    assert sorted(r["strategy"] for r in _rungs()) == sorted(MV_PROVEN_JOIN_STRATEGIES)
    assert MV_JOIN_STRATEGY_DIRECT in MV_PROVEN_JOIN_STRATEGIES


def test_the_direct_golden_has_first_level_joins():
    joins = yaml.safe_load(_rung(MV_JOIN_STRATEGY_DIRECT)["yaml_text"]).get("joins") or []
    assert joins
    assert not any(j.get("joins") for j in joins)


@pytest.mark.parametrize("strategy", [r["strategy"] for r in _rungs()])
def test_each_golden_records_the_live_run_that_proved_it(strategy):
    proven_at = _rung(strategy)["proven_at"]
    assert proven_at["date"] == "2026-10-02"
    assert proven_at["base"] == "7eeb5f5b"
    assert "renderer_commit" not in proven_at
    assert proven_at["warehouse"] == "fda7c3ad00bfac47"
    assert proven_at["dbsql_version"]
    assert tuple(proven_at["checks"]) == _LIVE_CHECKS


def test_the_nested_golden_validates_only_with_the_capability_granted():
    body = _rung(MV_JOIN_STRATEGY_NESTED)["yaml_text"]

    granted = validate(body, capabilities={MV_CAPABILITY_NESTED_JOINS: "GRANTED"})
    assert granted.ok
    assert granted.downgrade_to is None

    on_a_warehouse = validate(body, capabilities={MV_CAPABILITY_NESTED_JOINS: "UNKNOWN"})
    assert on_a_warehouse.ok
    assert on_a_warehouse.downgrade_to == MV_JOIN_STRATEGY_SUBQUERY


def test_the_golden_records_no_host_token_or_email():
    text = GOLDEN_PATH.read_text()
    for needle in ("@", "https://", "token", "dapi", "cloud.databricks"):
        assert needle not in text
