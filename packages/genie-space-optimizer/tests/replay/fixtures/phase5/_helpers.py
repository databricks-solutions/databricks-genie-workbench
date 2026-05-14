"""Shared loaders for Phase 5 replay fixtures.

Distinct from tests/integration/_replay_helpers.py because Phase 5
fixtures are pre-shaped iteration summaries (one JSON per iteration)
rather than per-stage input/output pairs. They are consumed by
Phase-5-specific tests that exercise terminal-policy, acceptance-tier,
and recovery-priority logic directly against in-memory payloads.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

FIXTURE_ROOT = Path(__file__).resolve().parent


def load(name: str) -> Any:
    path = FIXTURE_ROOT / name
    if not path.exists():
        raise FileNotFoundError(f"Phase 5 fixture missing: {path}")
    return json.loads(path.read_text())


def load_iteration(anchor: str, iteration: int) -> dict:
    """Convenience: load ccf1d60d_iter1.json or 31ecd96f_iter2_collateral.json
    by canonical name.
    """
    if anchor == "ccf1d60d":
        return load(f"ccf1d60d_iter{iteration}.json")
    if anchor == "31ecd96f":
        canonical = {
            1: "31ecd96f_iter1_h001.json",
            2: "31ecd96f_iter2_collateral.json",
            "alt": "31ecd96f_iter2_iter4_alternation.json",
        }
        key = canonical.get(iteration)
        if key is None:
            raise KeyError(f"31ecd96f iteration {iteration} not in canonical map")
        return load(key)
    raise KeyError(f"unknown anchor: {anchor}")


def assert_marker_in_stream(
    markers: list[dict], marker_name: str, **payload_filters: Any
) -> dict:
    """Assert that the marker stream contains at least one marker of the
    given name with payload fields matching the filters. Returns the
    first matching marker payload for further inspection.
    """
    matches = [
        m for m in markers if m.get("marker") == marker_name
    ]
    assert matches, (
        f"marker {marker_name} not emitted; "
        f"stream contained: {sorted({m.get('marker') for m in markers})}"
    )
    for m in matches:
        payload = m.get("payload", {})
        if all(payload.get(k) == v for k, v in payload_filters.items()):
            return payload
    raise AssertionError(
        f"marker {marker_name} present but no payload matched filters "
        f"{payload_filters!r}; payloads seen: "
        f"{[m.get('payload') for m in matches]!r}"
    )
