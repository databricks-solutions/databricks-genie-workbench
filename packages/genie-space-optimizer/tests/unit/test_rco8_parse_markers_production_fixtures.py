"""RCO-8 — production-shape fixtures for ``parse_markers``.

Pin the helper's current MarkerLog output for two shapes:

  * C14-V / C14-W multi-marker stdout stream featuring the new
    marker types introduced in those cycles.
  * Empty stdout edge case (every list-typed field is empty, every
    optional Mapping field is None).

The expected output is the canonical JSON of MarkerLog (tuples
serialized as lists, optional Mappings as nullable objects).

A future refactor of MarkerLog's shape or parse_markers's
classification logic that changes any field forces a deliberate
fixture update.
"""

from __future__ import annotations

import dataclasses

import pytest

from genie_space_optimizer.tools.marker_parser import (
    MarkerLog,
    parse_markers,
)
from tests.unit.fixtures.rco8._loader import load_text_in_json_out_pairs


_CASES = load_text_in_json_out_pairs("parse_markers")


def _case_id(case: tuple[str, str, dict]) -> str:
    return case[0]


def _marker_log_to_canonical_dict(log: MarkerLog) -> dict:
    """Convert MarkerLog to a JSON-shaped dict.

    - tuples become lists (lossy on type, lossless on order)
    - Mapping fields stay as dicts
    - frozen dataclass passthroughs handled via dataclasses.asdict

    Note: ``dataclasses.asdict`` preserves tuples as tuples (it does
    not coerce them to lists). We round-trip through JSON to get the
    canonical form where tuples → lists and Mappings → dicts.
    """
    import json

    raw = dataclasses.asdict(log)
    # ``unknown`` is dict[str, tuple[Mapping, ...]] — convert before
    # JSON round-trip so the inner Mappings are plain dicts.
    raw["unknown"] = {
        k: [dict(item) for item in v]
        for k, v in (raw.get("unknown") or {}).items()
    }
    # JSON round-trip coerces all tuples to lists uniformly.
    return json.loads(json.dumps(raw))


@pytest.mark.parametrize("case", _CASES, ids=_case_id)
def test_parse_markers_matches_production_shape(case) -> None:
    name, input_text, expected = case
    log = parse_markers(input_text)
    actual = _marker_log_to_canonical_dict(log)
    assert actual == expected, (
        f"RCO-8 fixture '{name}' drifted. "
        f"Expected={expected!r} Actual={actual!r}. "
        f"If this drift is intentional, update "
        f"tests/unit/fixtures/rco8/parse_markers/{name}/"
        f"expected_output.json deliberately."
    )


def test_parse_markers_fixtures_exist() -> None:
    """RCO-8 floor — both anchored shapes must be present."""
    case_names = {c[0] for c in _CASES}
    required = {"c14v_c14w_marker_stream", "empty_stdout"}
    missing = required - case_names
    assert not missing, (
        f"RCO-8 floor not met: missing fixture cases {sorted(missing)}"
    )
