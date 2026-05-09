"""Cycle 14-V Task 1 — shadow-mode observability for the C13
forbidden-AG admission predicate.

Anchored on 7Now run 338386531912450 iter 2-5: AG1 emits
``Proposals (0 total)`` so the reflection entry classifies as
RollbackClass.NO_ACTION. With GSO_FORBIDDEN_AG_ADMITS_NO_ACTION=0
the admission set is empty and AG1 reappears on iters 3-5; with
the new GSO_FORBIDDEN_AG_ADMISSION_OBSERVE=1 flag a typed marker
is emitted for every WOULD-admit reflection, even though
behavior is unchanged.
"""

from __future__ import annotations

import json
import re

from genie_space_optimizer.optimization.harness import (
    _compute_forbidden_ag_set,
)


def _no_action_reflection(
    iteration: int = 2,
    root_cause: str = "plural_top_n_collapse",
    lever_set: list[int] | None = None,
) -> dict:
    return {
        "iteration": iteration,
        "rollback_class": "no_action",
        "rollback_reason": "no_proposals",
        "accepted": False,
        "escalation_handled": False,
        "root_cause": root_cause,
        "blame_set": ("mv_esr_dim_location.zone_vp_name",),
        "lever_set": lever_set or [1, 5],
    }


def test_observe_emits_marker_on_no_action_reflection_when_observe_on(
    monkeypatch, capsys,
) -> None:
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMISSION_OBSERVE", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "0")

    forbidden = _compute_forbidden_ag_set([_no_action_reflection()])
    out = capsys.readouterr().out
    assert "GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1" in out

    payload_match = re.search(
        r"GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1\s+(\{.*\})", out
    )
    assert payload_match is not None, out
    payload = json.loads(payload_match.group(1))
    assert payload["rollback_class"] == "no_action"
    assert payload["rollback_reason"] == "no_proposals"
    assert payload["would_admit_with_admit_no_action_on"] is True
    assert payload["behavior_flag_on"] is False
    assert payload["suppressed_by_admit_no_action_off"] is True

    # Behavior unchanged: the actual admission set is empty.
    assert forbidden == set()


def test_observe_does_not_emit_when_admission_observe_off(
    monkeypatch, capsys,
) -> None:
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMISSION_OBSERVE", "0")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "0")

    _compute_forbidden_ag_set([_no_action_reflection()])
    out = capsys.readouterr().out
    assert "GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1" not in out


def test_observe_marks_suppressed_false_when_behavior_flag_on(
    monkeypatch, capsys,
) -> None:
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMISSION_OBSERVE", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "1")

    forbidden = _compute_forbidden_ag_set([_no_action_reflection()])
    out = capsys.readouterr().out
    assert "GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1" in out

    payload = json.loads(re.search(
        r"GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1\s+(\{.*\})", out
    ).group(1))
    assert payload["behavior_flag_on"] is True
    assert payload["suppressed_by_admit_no_action_off"] is False

    # Admission predicate accepts: forbidden set is non-empty.
    assert len(forbidden) == 1


def test_observe_does_not_emit_for_content_regression_reflections(
    monkeypatch, capsys,
) -> None:
    """Only NO_ACTION reflections benefit from the shadow mode;
    CONTENT_REGRESSION admissions are already gated by the legacy
    flag-independent path."""
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMISSION_OBSERVE", "1")
    monkeypatch.setenv("GSO_FORBIDDEN_AG_ADMITS_NO_ACTION", "0")

    cr = _no_action_reflection()
    cr["rollback_class"] = "content_regression"
    cr["rollback_reason"] = "full_eval:hard_regression"
    forbidden = _compute_forbidden_ag_set([cr])
    out = capsys.readouterr().out

    assert "GSO_FORBIDDEN_AG_ADMISSION_OBSERVE_V1" not in out
    assert len(forbidden) == 1
