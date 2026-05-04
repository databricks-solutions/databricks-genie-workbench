"""The harness must surface the rendered Phase H transcript in its
return value so the notebook (and any other caller) can print it on
stdout without re-rendering.

After the Phase H reliability fix, stdout rendering is decoupled from
MLflow artifact upload: whenever the transcript renders, it is attached
to ``loop_out`` regardless of whether an MLflow anchor was resolved. A
diagnostic ``phase_h_pretty_print_status`` / ``phase_h_pretty_print_reason``
pair is always stamped on the returned dict so the notebook fallback
log can explain what happened even when stdout is unavailable.
"""

from genie_space_optimizer.optimization.harness import (
    _build_loop_out_with_pretty_print,
)


def test_pretty_print_set_when_phase_h_assembly_succeeded():
    loop_out = _build_loop_out_with_pretty_print(
        loop_out_base={"accuracy": 0.95, "scores": {}, "model_id": "m"},
        phase_h_full_transcript="GSO LEVER LOOP RUN\nbody",
        phase_h_anchor_run_id="abc",
    )
    assert loop_out["pretty_print_transcript"] == "GSO LEVER LOOP RUN\nbody"
    assert loop_out["phase_h_pretty_print_status"] == "rendered_and_uploaded"
    assert loop_out["phase_h_pretty_print_reason"] == "ok"


def test_pretty_print_set_when_rendered_without_anchor():
    """Stdout rendering no longer depends on an MLflow anchor. Even when
    bundle upload is impossible (replay path, no active MLflow run, etc.),
    the rendered transcript must still round-trip through ``loop_out`` so
    the notebook prints it."""
    loop_out = _build_loop_out_with_pretty_print(
        loop_out_base={"accuracy": 0.95, "scores": {}, "model_id": "m"},
        phase_h_full_transcript="GSO LEVER LOOP RUN\nbody",
        phase_h_anchor_run_id=None,
    )
    assert loop_out["pretty_print_transcript"] == "GSO LEVER LOOP RUN\nbody"
    assert loop_out["phase_h_pretty_print_status"] == "rendered_stdout_only"
    assert loop_out["phase_h_pretty_print_reason"] == "no_mlflow_anchor"


def test_pretty_print_unset_when_phase_h_skipped():
    loop_out = _build_loop_out_with_pretty_print(
        loop_out_base={"accuracy": 0.95, "scores": {}, "model_id": "m"},
        phase_h_full_transcript=None,
        phase_h_anchor_run_id=None,
    )
    assert "pretty_print_transcript" not in loop_out
    assert loop_out["phase_h_pretty_print_status"] == "skipped"
    assert loop_out["phase_h_pretty_print_reason"] == "no_transcript_rendered"


def test_pretty_print_unset_when_render_failed_but_anchor_present():
    """If rendering produced no transcript (legacy harness or render error)
    but an anchor exists, stdout cannot show anything. The reason code
    makes that distinguishable from the no-anchor case."""
    loop_out = _build_loop_out_with_pretty_print(
        loop_out_base={"accuracy": 0.95, "scores": {}, "model_id": "m"},
        phase_h_full_transcript=None,
        phase_h_anchor_run_id="abc",
    )
    assert "pretty_print_transcript" not in loop_out
    assert loop_out["phase_h_pretty_print_status"] == "skipped"
    assert loop_out["phase_h_pretty_print_reason"] == "no_transcript_rendered"
