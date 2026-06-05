"""e943 Phase-2 mechanism-binding — end-to-end drop proof via tape replay.

The pure ``mechanism_binding`` selectors are unit-tested in
``tests/unit/test_mechanism_binding.py`` and the OFF-leg byte-stability is
covered by the offline workbench comparison. What neither proves is that the
*wiring* inside ``run_plan11_synthesis_for_single_cluster`` actually mutates
the surviving slate when the flag flips — the binding only fires on a *mixed*
slate (an ``example_sql`` proposal alongside a non-``example_sql``,
non-fixing proposal) under an example-SQL-insufficient RCA, which no captured
production tape happens to contain.

This test drives the **real production state machine** (via the local
lever-loop workbench in ``sm-tape`` mode) against a hand-crafted triggering
fixture:

* Stage 1 diagnose labels the QID's RCA ``extra_defensive_filter`` — an RCA
  the W4 router knows ``add_example_sql`` cannot fix (fixing mechanisms are
  ``instruction_text`` / ``sql_snippet``).
* Stage 3 synthesis emits a mixed slate: ``add_example_sql`` (mechanism
  ``example_sql`` → *defaulted*) **and** ``add_column_description``
  (mechanism ``metadata_description`` → *not defaulted*, and resolvable
  against the bundle's ``main.public.orders`` table).

With ``GSO_RCA_MECHANISM_ROUTE_BINDING=0`` (explicit opt-out) the binding is
observe-only: both proposals survive and the example-SQL patch is applied.
With the flag ON (the default since promotion) the binding DROPS the
behaviorally-inert ``add_example_sql`` proposal, emits a ``CONTRACT_FAILED``
outcome for it, and the ``add_column_description`` proposal is applied
instead — while slate-safety keeps the slate non-empty.

Both legs set the flag *explicitly* via ``monkeypatch`` so the test pins the
binding behaviour regardless of the module default.

The fixture lives at
``tests/integration/postmortem_replay/fixtures/mechanism_binding_trigger/``.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from local_lever_workbench.input_bundle import from_bundle_json
from local_lever_workbench.local_runner import (
    LLM_MODE_TAPE,
    run_workbench_iteration,
)
from local_lever_workbench.models import WorkbenchRunConfig

_FIXTURE_DIR = (
    Path(__file__).resolve().parents[1]
    / "integration"
    / "postmortem_replay"
    / "fixtures"
    / "mechanism_binding_trigger"
)
_BUNDLE = _FIXTURE_DIR / "bundle.json"
_TAPE = _FIXTURE_DIR / "tape.jsonl"

_PATCH_OUTCOME_RE = re.compile(r"GSO_PATCH_OUTCOME_V1 (\{.*\})")
_W4_MARKER_RE = re.compile(
    r"GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_EXAMPLE_SQL_V1 (\{.*\})"
)


def _run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    route_binding: bool,
) -> tuple[tuple, str]:
    """Replay the crafted fixture once. Returns (recorded_patches, stdout).

    The RCA-route flag is set explicitly on every leg so the test does not
    depend on the module default (which is ON since promotion).
    """
    monkeypatch.setenv(
        "GSO_RCA_MECHANISM_ROUTE_BINDING", "1" if route_binding else "0"
    )
    bundle = from_bundle_json(_BUNDLE)
    config = WorkbenchRunConfig(
        bundle_path=_BUNDLE,
        output_dir=tmp_path / ("on" if route_binding else "off"),
        llm_mode=LLM_MODE_TAPE,
        tape_path=_TAPE,
        iteration=1,
    )
    artifacts = run_workbench_iteration(bundle, config)
    return artifacts.recorder.as_tuple(), artifacts.stdout_text


def _w4_observed_mechanisms(stdout: str) -> list[str]:
    m = _W4_MARKER_RE.search(stdout)
    assert m, (
        "W4 anti-pattern marker did not fire — the crafted slate is no "
        "longer mixed (example_sql + a non-fixing mechanism) under an "
        "example-SQL-insufficient RCA. Re-check the fixture."
    )
    return json.loads(m.group(1)).get("observed_mechanisms", [])


def _rca_default_contract_failures(stdout: str) -> list[dict]:
    out = []
    for raw in _PATCH_OUTCOME_RE.findall(stdout):
        payload = json.loads(raw)
        reason = str(
            payload.get("terminal_reason")
            or payload.get("outcome_reason")
            or ""
        )
        if "rca_mechanism_defaulted_to_example_sql" in reason:
            out.append(payload)
    return out


@pytest.fixture(autouse=True)
def _isolate_binding_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    # This test isolates the *RCA-route* binding. Pin the sibling coverage
    # binding OFF so a coverage drop can never confound the route-binding
    # assertions (coverage defaults ON since promotion). Each test sets the
    # route flag itself via ``_run``.
    monkeypatch.setenv("GSO_MECHANISM_COVERAGE_BINDING", "0")


@pytest.mark.workbench
@pytest.mark.integration
def test_fixture_presents_a_mixed_slate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Sanity: both proposals survive to the W4 block (mixed slate).

    If the ``add_column_description`` proposal stops resolving (e.g. the
    target table is dropped from the snapshot) the slate collapses to all-
    ``example_sql`` and the drop can never be exercised — this guard makes
    that failure mode loud instead of silently turning the drop test green.
    """
    _patches, stdout = _run(tmp_path, monkeypatch, route_binding=False)
    observed = _w4_observed_mechanisms(stdout)
    assert "example_sql" in observed and "metadata_description" in observed, (
        f"expected a mixed slate; W4 observed_mechanisms={observed!r}"
    )


@pytest.mark.workbench
@pytest.mark.integration
def test_route_binding_off_is_observe_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Flag explicitly OFF: no drop — the example-SQL patch is applied."""
    patches, stdout = _run(tmp_path, monkeypatch, route_binding=False)
    assert _rca_default_contract_failures(stdout) == [], (
        "binding fired with the flag OFF — observe-only contract broken"
    )
    applied = [p.patch_type for p in patches]
    assert "add_example_sql" in applied, (
        f"flag OFF should keep + apply the example_sql proposal; "
        f"applied={applied!r}"
    )


@pytest.mark.workbench
@pytest.mark.integration
def test_route_binding_on_drops_the_defaulted_example_sql(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Flag ON: the behaviorally-inert example_sql proposal is DROPPED.

    The slate is not emptied (slate-safety) — the surviving
    ``add_column_description`` proposal is applied in its place, and the
    dropped proposal carries a typed ``rca_mechanism_defaulted_to_example_sql``
    contract-failure outcome.
    """
    patches, stdout = _run(tmp_path, monkeypatch, route_binding=True)

    failures = _rca_default_contract_failures(stdout)
    assert len(failures) == 1, (
        f"expected exactly one rca-default CONTRACT_FAILED outcome for the "
        f"dropped example_sql proposal; got {len(failures)}"
    )

    applied = [p.patch_type for p in patches]
    assert patches, "slate-safety violated: binding emptied the slate"
    assert "add_example_sql" not in applied, (
        f"binding ON must drop the defaulted example_sql proposal; "
        f"applied={applied!r}"
    )
    assert "add_column_description" in applied, (
        f"the surviving non-defaulted proposal should be applied instead; "
        f"applied={applied!r}"
    )
