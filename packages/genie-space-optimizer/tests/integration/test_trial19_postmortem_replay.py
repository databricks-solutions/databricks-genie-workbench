"""Trial 19 — postmortem-tape replay integration test.

Replays the two postmortem fixtures (airline 634185464201993 and 7now
953593238005228) through the Trial 19 enforcement primitives and asserts
the success criteria from the trial plan:

* No insufficient-repair signature repeats across iterations for any QID.
* No ``ag_collision_with_forbidden_set`` on the final iteration once the
  admission gate is wired in.
* Structural gate emits ``retry_with_typed_feedback`` (not ``rejected`` /
  not admit-absent) on at least one cluster where ``intended_patch_shape``
  is named and ``emitted_patch_shape == "absent"``.
* At least one QID flagged ``already_correct_under_arbiter`` in the
  baseline pre-eval; that QID never enters the hard list.
* ``dominant_root_cause_label`` returns the typed label (not collapsed to
  ``unknown``) for the postmortem cluster.

The test exercises the Trial 19 primitives directly (no full lever loop)
so it stays fast and deterministic; the full sweep is the operator
hand-off step.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

FIXTURE_DIR = (
    Path(__file__).parent.parent
    / "fixtures"
    / "trial19_postmortem"
)
FIXTURE_PATHS = (
    FIXTURE_DIR / "airline_634185464201993.json",
    FIXTURE_DIR / "7now_953593238005228.json",
)


def _load_fixture(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


@pytest.fixture(autouse=True)
def _trial19_flags_on(monkeypatch):
    """Force Trial 19 master + all sub-flags ON for the replay."""
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE", "1")
    monkeypatch.setenv("GSO_TRIAL19_ENFORCE_INSUFFICIENT", "1")
    monkeypatch.setenv("GSO_TRIAL19_LLM_FIRST_RCA", "1")
    monkeypatch.setenv("GSO_TRIAL19_ALREADY_CORRECT_FILTER", "1")
    monkeypatch.setenv("GSO_TRIAL19_GT_PENDING_REVIEW", "1")
    yield


@pytest.mark.parametrize("fixture_path", FIXTURE_PATHS, ids=lambda p: p.stem)
def test_c1_already_correct_under_arbiter_filter_fires(
    fixture_path: Path,
) -> None:
    """C1 success criterion: at least one QID flagged
    ``already_correct_under_arbiter`` in the baseline; the filter
    excludes it from the hard list.
    """
    from genie_space_optimizer.optimization.ground_truth_corrections import (
        is_trial19_arbiter_correct_gt_disagrees,
    )
    fixture = _load_fixture(fixture_path)
    excluded = [
        row for row in fixture["baseline_eval_rows"]
        if is_trial19_arbiter_correct_gt_disagrees(row)
    ]
    assert len(excluded) >= 1, (
        f"Expected at least one C1 hit in {fixture_path.name}: "
        f"baseline must contain ≥1 arbiter=both_correct + byte=no row"
    )


@pytest.mark.parametrize("fixture_path", FIXTURE_PATHS, ids=lambda p: p.stem)
def test_a1_admission_gate_rejects_insufficient_repeat(
    fixture_path: Path,
) -> None:
    """A1 success criterion: a Stage 3 proposal that re-emits the
    iteration-1 ``insufficient_repair_signature`` as a sole primary
    must be rejected by the admission gate.
    """
    from genie_space_optimizer.optimization.admission_gate import (
        REJECTED_INSUFFICIENT_REPEAT,
        evaluate_admission,
    )
    fixture = _load_fixture(fixture_path)
    iter1 = fixture["iterations"][0]
    insufficient_sig = iter1["kept_insufficient_signature"]
    qid = iter1["qid"]

    # Extract (lever, patch_type, rca_kind) from the signature so we can
    # build a sole-primary proposal that matches.
    # Signature format: lever-N:patch_type:insufficient:rca=KIND:behavior=...
    parts = insufficient_sig.split(":")
    lever = parts[0]
    patch_type = parts[1]
    rca_kind = ""
    for p in parts[2:]:
        if p.startswith("rca="):
            rca_kind = p[len("rca="):]
            break

    class _StubTarget:
        identifier = "test-target"

    class _StubPatchType:
        def __init__(self, v):
            self.value = v

    class _StubProposal:
        def __init__(self):
            self.intent_id = "test-intent"
            self.patch_type = _StubPatchType(patch_type)
            self.target_objects = (_StubTarget(),)
            self.target_qids = (qid,)
            self.patch_body = {}
            self.selected_lever = lever
            self.bundle_id = ""

    admission = evaluate_admission(
        [_StubProposal()],
        insufficient_signatures=(insufficient_sig,),
        forbidden_signatures=(),
        rca_kind_label_by_qid={qid: rca_kind},
    )

    rejected_decisions = [
        v.decision for v in admission.verdicts
        if v.decision == REJECTED_INSUFFICIENT_REPEAT
    ]
    assert len(rejected_decisions) == 1, (
        f"Expected sole-primary repeat to be rejected for "
        f"{fixture_path.name}: got decisions "
        f"{[v.decision for v in admission.verdicts]}"
    )


@pytest.mark.parametrize("fixture_path", FIXTURE_PATHS, ids=lambda p: p.stem)
def test_b4_structural_gate_retries_on_named_intent(
    fixture_path: Path,
) -> None:
    """B4 success criterion: structural-repair gate emits
    ``retry_with_typed_feedback`` (not ``rejected``) when a named
    ``intended_patch_shape`` is supplied with ``emitted_patch_shape ==
    "absent"``.
    """
    from genie_space_optimizer.optimization.structural_repair_gate import (
        EmittedPatchShape,
        RETRY_WITH_TYPED_FEEDBACK,
        enforce_structural_repair_shape,
    )
    fixture = _load_fixture(fixture_path)
    iter1 = fixture["iterations"][0]
    emissions = iter1.get("structural_emissions", [])
    assert emissions, (
        f"Fixture {fixture_path.name} must include at least one "
        f"absent-shape emission to exercise B4"
    )
    retry_count = 0
    for emission in emissions:
        verdict = enforce_structural_repair_shape(
            intended_patch_shape=emission["intended_patch_shape"],
            emitted_patch_shape=EmittedPatchShape.ABSENT,
        )
        if verdict.outcome == RETRY_WITH_TYPED_FEEDBACK:
            retry_count += 1
            assert verdict.retry_feedback, (
                f"retry verdict must carry a non-empty feedback string "
                f"for intent '{emission['intended_patch_shape']}'"
            )
            assert (
                emission["intended_patch_shape"] in verdict.retry_feedback
            ), (
                f"feedback must name the intent "
                f"'{emission['intended_patch_shape']}' for replay diagnosis"
            )
    assert retry_count >= 1, (
        f"Expected at least one retry_with_typed_feedback verdict for "
        f"{fixture_path.name}"
    )


@pytest.mark.parametrize("fixture_path", FIXTURE_PATHS, ids=lambda p: p.stem)
def test_b1_dominant_root_cause_label_preserves_typed_label(
    fixture_path: Path,
) -> None:
    """B1 success criterion: ``dominant_root_cause_label`` returns the
    typed label string (not collapsed to ``unknown``) when at least one
    QID in the cluster carries a typed ``rca_kind_label``.
    """
    from genie_space_optimizer.optimization.rca_card_builder import (
        dominant_root_cause_label,
    )
    fixture = _load_fixture(fixture_path)
    asi_by_qid = fixture["rca_cluster"]["qid_to_asi"]
    label = dominant_root_cause_label(asi_by_qid)
    assert label and label != "unknown", (
        f"Expected typed dominant_root_cause_label for "
        f"{fixture_path.name} cluster; got {label!r}"
    )
    typed_labels = {
        v.get("rca_kind_label", "") for v in asi_by_qid.values()
    }
    typed_labels.discard("")
    assert label in typed_labels, (
        f"Returned label {label!r} not in cluster's typed labels "
        f"{typed_labels}"
    )


@pytest.mark.parametrize("fixture_path", FIXTURE_PATHS, ids=lambda p: p.stem)
def test_g5_final_iteration_does_not_terminate_on_collision(
    fixture_path: Path,
) -> None:
    """G5 invariant: the fixture's recorded final-iteration terminal
    reason is the pre-Trial-19 ``ag_collision_with_forbidden_set``.
    Trial 19 success requires the regenerator wrapper to convert that
    into either a different AG (test environment can't exercise the
    live LLM) OR ``fallback_no_new_strategy``. We assert the wrapper
    emits the marker when invoked with the fixture's signature set.
    """
    from genie_space_optimizer.optimization.stages.action_groups import (
        regenerate_action_groups_with_signatures,
    )
    fixture = _load_fixture(fixture_path)
    iter1 = fixture["iterations"][0]
    insufficient_sig = iter1["kept_insufficient_signature"]

    # Stage a regenerator that returns empty — the wrapper's A6 hook
    # should detect this and emit GSO_FALLBACK_NO_NEW_STRATEGY_V1.
    captured: dict[str, Any] = {}

    def _inner_regen(*, prior_clusters, forbidden_set, **kwargs):
        captured["forbidden_set"] = set(forbidden_set)
        captured["insufficient"] = tuple(
            kwargs.get("insufficient_repair_signatures") or ()
        )
        # Simulate the legacy collision path: no new AG.
        return []

    import io
    import contextlib

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        result = regenerate_action_groups_with_signatures(
            prior_clusters=[],
            prior_terminal_signatures=[],
            existing_forbidden_set=set(),
            inner_regenerate=_inner_regen,
            insufficient_repair_signatures=(insufficient_sig,),
        )

    assert result == [], "Stub regenerator should return empty"
    assert insufficient_sig in captured["forbidden_set"], (
        f"Wrapper must union insufficient signature into forbidden_set "
        f"for {fixture_path.name}"
    )
    assert captured["insufficient"] == (insufficient_sig,)
    output = buf.getvalue()
    assert "GSO_FALLBACK_NO_NEW_STRATEGY_V1" in output, (
        f"A6 marker missing for {fixture_path.name}; instead saw:\n"
        f"{output}"
    )
