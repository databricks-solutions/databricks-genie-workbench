"""Regression: every named site in ``docs/harness/control_flow_sites.md``
is reachable under at least one of the two committed anchor tapes.

Catches refactors of ``_run_lever_loop`` that silently remove or
relocate a named site's underlying block, which would invalidate any
plan citing that site by name.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pytest

from genie_space_optimizer.optimization.tape import LeverLoopTape
from genie_space_optimizer.optimization.harness_control_flow_tracer import (
    trace_run_lever_loop,
)


_PKG_ROOT = Path(__file__).resolve().parents[3]
_SITES_MD = _PKG_ROOT / "docs" / "harness" / "control_flow_sites.md"
_TAPES_DIR = (
    _PKG_ROOT / "tests" / "replay" / "active" / "fixtures" / "production_tapes"
)


@dataclass(frozen=True)
class NamedSite:
    name: str
    start_lineno: int
    end_lineno: int
    reach_airline_expected: bool
    reach_7now_expected: bool


def _parse_sites_table() -> list[NamedSite]:
    text = _SITES_MD.read_text()
    m = re.search(
        r"<!-- BEGIN CANONICAL SITES TABLE -->(.*?)<!-- END CANONICAL SITES TABLE -->",
        text,
        flags=re.S,
    )
    if not m:
        raise RuntimeError("Canonical sites table block not found in doc")
    rows: list[NamedSite] = []
    for line in m.group(1).strip().splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = [c.strip() for c in stripped.strip("|").split("|")]
        if len(cells) != 5:
            continue
        if cells[0] == "name" or cells[0].startswith("---"):
            continue
        name, start, end, ra, rn = cells
        rows.append(
            NamedSite(
                name=name,
                start_lineno=int(start),
                end_lineno=int(end),
                reach_airline_expected=(ra == "YES"),
                reach_7now_expected=(rn == "YES"),
            )
        )
    return rows


@pytest.fixture(scope="module")
def airline_executed_lines() -> frozenset[int]:
    tape_path = _TAPES_DIR / "airline_run_59a173d3.json"
    if not tape_path.exists():
        pytest.skip("airline production tape missing")
    tape = LeverLoopTape.from_json_file(tape_path)
    return trace_run_lever_loop(
        tape=tape,
        run_id="audit-airline-regression",
        space_id="space-airline",
        domain="airline",
        prev_accuracy=0.9167,
    )


@pytest.fixture(scope="module")
def seven_now_executed_lines() -> frozenset[int]:
    tape_path = _TAPES_DIR / "seven_now_run_ab65fefe.json"
    if not tape_path.exists():
        pytest.skip("7now production tape missing")
    tape = LeverLoopTape.from_json_file(tape_path)
    return trace_run_lever_loop(
        tape=tape,
        run_id="audit-7now-regression",
        space_id="space-7now",
        domain="7now",
        prev_accuracy=0.9130,
    )


@pytest.mark.parametrize("site", _parse_sites_table(), ids=lambda s: s.name)
def test_named_site_reachable_under_at_least_one_anchor(
    site: NamedSite,
    airline_executed_lines: frozenset[int],
    seven_now_executed_lines: frozenset[int],
) -> None:
    """Each named site must be reachable under at least one anchor tape.

    The site is "reachable" if any line in its [start_lineno,
    end_lineno] range was executed under the relevant tape. This
    tolerates the tracer reporting line events for sub-statements
    within multi-line blocks.
    """
    site_range = range(site.start_lineno, site.end_lineno + 1)
    airline_hit = any(ln in airline_executed_lines for ln in site_range)
    seven_now_hit = any(ln in seven_now_executed_lines for ln in site_range)

    assert airline_hit or seven_now_hit, (
        f"named site '{site.name}' (lines {site.start_lineno}–"
        f"{site.end_lineno}) not reachable under any anchor tape. "
        f"airline executed={airline_hit}, 7now executed={seven_now_hit}. "
        f"Either the site has been refactored away, or its line range "
        f"in docs/harness/control_flow_sites.md is stale."
    )


@pytest.mark.parametrize("site", _parse_sites_table(), ids=lambda s: s.name)
def test_named_site_expected_reach_matches_observed(
    site: NamedSite,
    airline_executed_lines: frozenset[int],
    seven_now_executed_lines: frozenset[int],
) -> None:
    """The expected reachability in the doc matches what the tracer
    actually observes. Drift in either direction (doc says YES but
    site is unreachable, or doc says no but site IS reached) means
    the doc is out of sync with the harness — refresh by re-running
    ``scripts/audit_harness_control_flow.py``."""
    site_range = range(site.start_lineno, site.end_lineno + 1)
    airline_hit = any(ln in airline_executed_lines for ln in site_range)
    seven_now_hit = any(ln in seven_now_executed_lines for ln in site_range)

    assert airline_hit == site.reach_airline_expected, (
        f"'{site.name}' airline reachability drift: doc says "
        f"{site.reach_airline_expected}, observed {airline_hit}. "
        f"Refresh docs/harness/control_flow_sites.md."
    )
    assert seven_now_hit == site.reach_7now_expected, (
        f"'{site.name}' 7now reachability drift: doc says "
        f"{site.reach_7now_expected}, observed {seven_now_hit}. "
        f"Refresh docs/harness/control_flow_sites.md."
    )
