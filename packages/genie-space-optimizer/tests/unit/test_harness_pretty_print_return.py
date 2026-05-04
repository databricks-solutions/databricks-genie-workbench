"""The harness must surface the rendered Phase H transcript in its
return value so the notebook (and any other caller) can print it on
stdout without re-rendering."""

from genie_space_optimizer.optimization.harness import _build_loop_out_with_pretty_print


def test_pretty_print_set_when_phase_h_assembly_succeeded():
    loop_out = _build_loop_out_with_pretty_print(
        loop_out_base={"accuracy": 0.95, "scores": {}, "model_id": "m"},
        phase_h_full_transcript="GSO LEVER LOOP RUN\nbody",
        phase_h_anchor_run_id="abc",
    )
    assert loop_out["pretty_print_transcript"] == "GSO LEVER LOOP RUN\nbody"


def test_pretty_print_unset_when_phase_h_skipped():
    loop_out = _build_loop_out_with_pretty_print(
        loop_out_base={"accuracy": 0.95, "scores": {}, "model_id": "m"},
        phase_h_full_transcript=None,
        phase_h_anchor_run_id=None,
    )
    assert "pretty_print_transcript" not in loop_out


def test_pretty_print_unset_when_render_failed_but_anchor_present():
    loop_out = _build_loop_out_with_pretty_print(
        loop_out_base={"accuracy": 0.95, "scores": {}, "model_id": "m"},
        phase_h_full_transcript=None,
        phase_h_anchor_run_id="abc",
    )
    assert "pretty_print_transcript" not in loop_out
