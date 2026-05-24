"""Trial 13l — ``GSO_PLAN11_SCHEMA_COLUMNS_INJECTION_V1`` marker unit tests.

Covers:

* Marker name string (exact match — postmortem scrapers grep on it).
* Payload shape (every required field present, types coerced).
* Source-vocabulary invariant (every reachable source value emits a
  valid marker; nothing in the closed vocabulary is rejected).
* JSON round-trip stability under :func:`json.dumps` with sorted keys.
"""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.run_analysis_contract import (
    plan11_schema_columns_injection_marker,
)
from genie_space_optimizer.optimization.schema_columns import (
    SCHEMA_COLUMNS_INJECTION_SOURCES,
)


def _parse_marker(line: str) -> tuple[str, dict]:
    """Split ``"MARKER_NAME {json_payload}"`` into ``(name, dict)``."""
    name, _, payload = line.partition(" ")
    return name, json.loads(payload)


def test_marker_name_is_exact_grep_string() -> None:
    """Postmortem scrapers grep on the literal marker name string."""
    line = plan11_schema_columns_injection_marker(
        optimization_run_id="r123",
        iteration=0,
        space_id="01f143dfbeec15a3a0e87ced8662f4ed",
        injected=True,
        source="genie_api",
        column_count=42,
        latency_ms=17,
    )
    name, _ = _parse_marker(line)
    assert name == "GSO_PLAN11_SCHEMA_COLUMNS_INJECTION_V1"


def test_payload_carries_all_required_fields() -> None:
    line = plan11_schema_columns_injection_marker(
        optimization_run_id="r123",
        iteration=2,
        space_id="01f143dfbeec15a3a0e87ced8662f4ed",
        injected=True,
        source="genie_api",
        column_count=42,
        latency_ms=17,
    )
    _, payload = _parse_marker(line)
    assert payload == {
        "optimization_run_id": "r123",
        "iteration": 2,
        "space_id": "01f143dfbeec15a3a0e87ced8662f4ed",
        "injected": True,
        "source": "genie_api",
        "column_count": 42,
        "latency_ms": 17,
    }


def test_payload_keys_are_sorted_for_stable_diffs() -> None:
    """``marker_line`` uses ``sort_keys=True``; postmortem diffs depend on it."""
    line = plan11_schema_columns_injection_marker(
        optimization_run_id="r",
        iteration=0,
        space_id="s",
        injected=False,
        source="api_error",
        column_count=0,
        latency_ms=0,
    )
    _, _, payload_raw = line.partition(" ")
    # Re-encode and compare: the marker payload is canonical when
    # round-tripping through json.loads + json.dumps(sort_keys=True)
    # is byte-identical.
    payload_dict = json.loads(payload_raw)
    canonical = json.dumps(payload_dict, sort_keys=True, separators=(",", ":"))
    assert payload_raw == canonical


@pytest.mark.parametrize("source", sorted(SCHEMA_COLUMNS_INJECTION_SOURCES))
def test_every_vocabulary_member_yields_a_valid_marker(source: str) -> None:
    """Every closed-vocabulary source emits a parseable marker."""
    injected = source == "genie_api"
    count = 5 if injected else 0
    line = plan11_schema_columns_injection_marker(
        optimization_run_id="r123",
        iteration=1,
        space_id="01f143dfbeec15a3a0e87ced8662f4ed",
        injected=injected,
        source=source,
        column_count=count,
        latency_ms=3,
    )
    name, payload = _parse_marker(line)
    assert name == "GSO_PLAN11_SCHEMA_COLUMNS_INJECTION_V1"
    assert payload["source"] == source
    assert payload["injected"] is injected
    assert payload["column_count"] == count


def test_int_coercion_on_numeric_fields() -> None:
    """The marker coerces numeric fields so callers don't have to."""
    line = plan11_schema_columns_injection_marker(
        optimization_run_id="r",
        iteration=True,  # type: ignore[arg-type]  # bool happens to subtype int
        space_id="s",
        injected=True,
        source="genie_api",
        column_count="9",  # type: ignore[arg-type]  # production passes int; defensive
        latency_ms="42",  # type: ignore[arg-type]
    )
    _, payload = _parse_marker(line)
    assert payload["iteration"] == 1
    assert payload["column_count"] == 9
    assert payload["latency_ms"] == 42


def test_string_coercion_on_string_fields() -> None:
    line = plan11_schema_columns_injection_marker(
        optimization_run_id=123,  # type: ignore[arg-type]
        iteration=0,
        space_id=None,  # type: ignore[arg-type]
        injected=False,
        source="no_space_id",
        column_count=0,
        latency_ms=0,
    )
    _, payload = _parse_marker(line)
    assert payload["optimization_run_id"] == "123"
    assert payload["space_id"] == "None"
