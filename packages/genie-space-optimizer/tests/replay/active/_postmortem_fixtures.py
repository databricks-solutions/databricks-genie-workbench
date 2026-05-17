"""Phase 4 (2026-05-16) — shared loader for the Trial-5 postmortem
fixtures.

Resolves the directory↔run-id inversion documented in Phase 4
Audit A.1: the user-spec labels ("Run A ab65fefe", "Run B 59a173d3")
address fixtures by run-id substring, while the on-disk layout uses
directory names from a prior convention.

All Phase 4 active-callsite tests MUST address fixtures through
this module so a single change here propagates to every test.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# User-spec run-id substrings — these are the source of truth.
RUN_A_AB65FEFE = "ab65fefe"
RUN_B_59A173D3 = "59a173d3"

# Physical directory names — INVERTED relative to the user-spec
# labels. Phase 4 Audit A.1 documents this inversion.
_RUN_A_DIRECTORY = "run_b_7now"  # contains run-id ab65fefe
_RUN_B_DIRECTORY = "run_a_airline"  # contains run-id 59a173d3

_FIXTURE_ROOT = (
    Path(__file__).parent.parent.parent
    / "fixtures"
    / "trial5_postmortem"
)


def _load_fixture(directory: str) -> dict[str, Any]:
    path = _FIXTURE_ROOT / directory / "replay_fixture.json"
    if not path.exists():
        raise FileNotFoundError(
            f"Postmortem fixture not found at {path}. "
            f"See tests/fixtures/trial5_postmortem/README.md."
        )
    return json.loads(path.read_text())


def load_run_a_ab65fefe() -> dict[str, Any]:
    """Run A (user-spec label) — fixture whose run-id contains
    'ab65fefe'. Physically located in ``run_b_7now/`` per the
    directory↔run-id inversion."""
    fixture = _load_fixture(_RUN_A_DIRECTORY)
    assert RUN_A_AB65FEFE in fixture.get("fixture_id", ""), (
        f"Run A fixture's run-id drifted. Expected substring "
        f"{RUN_A_AB65FEFE!r}, got "
        f"fixture_id={fixture.get('fixture_id')!r}"
    )
    return fixture


def load_run_b_59a173d3() -> dict[str, Any]:
    """Run B (user-spec label) — fixture whose run-id contains
    '59a173d3'. Physically located in ``run_a_airline/`` per the
    directory↔run-id inversion."""
    fixture = _load_fixture(_RUN_B_DIRECTORY)
    assert RUN_B_59A173D3 in fixture.get("fixture_id", ""), (
        f"Run B fixture's run-id drifted. Expected substring "
        f"{RUN_B_59A173D3!r}, got "
        f"fixture_id={fixture.get('fixture_id')!r}"
    )
    return fixture


def get_iteration(fixture: dict[str, Any], n: int) -> dict[str, Any]:
    """Return the iteration record whose ``iteration`` field equals
    ``n``. 1-indexed per the fixture convention. Raises
    ``LookupError`` if no such iteration exists."""
    for it in fixture.get("iterations") or []:
        if int(it.get("iteration", -1)) == n:
            return it
    available = [
        it.get("iteration") for it in fixture.get("iterations") or []
    ]
    raise LookupError(
        f"Iteration {n} not found in fixture "
        f"(fixture_id={fixture.get('fixture_id')!r}). "
        f"Available iterations: {available}"
    )


def get_clusters(iteration: dict[str, Any]) -> list[dict[str, Any]]:
    """Convenience: return the ``clusters`` list from an iteration."""
    return list(iteration.get("clusters") or [])


def get_cluster_by_id(
    iteration: dict[str, Any], cluster_id: str,
) -> dict[str, Any]:
    """Convenience: return the cluster record with the given
    ``cluster_id``. Raises ``LookupError`` if missing."""
    for c in get_clusters(iteration):
        if str(c.get("cluster_id") or "") == cluster_id:
            return c
    available = [c.get("cluster_id") for c in get_clusters(iteration)]
    raise LookupError(
        f"Cluster {cluster_id!r} not found in iteration "
        f"{iteration.get('iteration')}. Available: {available}"
    )


def get_decision_records(
    iteration: dict[str, Any],
    *,
    decision_type: str | None = None,
) -> list[dict[str, Any]]:
    """Convenience: return decision records, optionally filtered
    by ``decision_type``."""
    drs = list(iteration.get("decision_records") or [])
    if decision_type is None:
        return drs
    return [dr for dr in drs if dr.get("decision_type") == decision_type]
