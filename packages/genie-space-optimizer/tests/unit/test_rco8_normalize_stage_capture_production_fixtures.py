"""RCO-8 — production-shape fixtures for ``_normalize_stage_capture``.

Pin the helper's current behavior on the four production shapes that
mattered in the C14-V D-4 / C14-W regression chain:

  * list-of-dict (collapse to first dict)
  * list-of-non-dict (empty dict)
  * empty list (empty dict)
  * dict (passthrough)

If a future refactor of ``_normalize_stage_capture`` changes any of
these outputs, the offending fixture pair fails. Updating the
expected_output.json is then a deliberate, reviewed act — not an
accident.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.run_output_bundle import (
    _normalize_stage_capture,
)
from tests.unit.fixtures.rco8._loader import load_json_pairs


_CASES = load_json_pairs("normalize_stage_capture")


def _case_id(case: tuple[str, dict, dict]) -> str:
    return case[0]


@pytest.mark.parametrize("case", _CASES, ids=_case_id)
def test_normalize_stage_capture_matches_production_shape(case) -> None:
    name, payload, expected = case
    raw_value = payload["value"]
    actual = _normalize_stage_capture(
        raw_value,
        stage_key=str(payload.get("stage_key", "")),
        iteration=int(payload.get("iteration", 0)),
    )
    assert actual == expected, (
        f"RCO-8 fixture '{name}' drifted. "
        f"Expected={expected!r} Actual={actual!r}. "
        f"If this drift is intentional, update "
        f"tests/unit/fixtures/rco8/normalize_stage_capture/{name}/"
        f"expected_output.json deliberately."
    )


def test_normalize_stage_capture_fixtures_exist() -> None:
    """RCO-8 floor — at least the four named C14-V / C14-W anchor
    cases must be present so the regression pattern is covered."""
    case_names = {c[0] for c in _CASES}
    required = {
        "airline_f7_list_of_dict",
        "airline_f7_list_of_non_dict",
        "airline_f7_empty_list",
        "airline_f7_dict_passthrough",
    }
    missing = required - case_names
    assert not missing, (
        f"RCO-8 floor not met: missing fixture cases {sorted(missing)}"
    )
