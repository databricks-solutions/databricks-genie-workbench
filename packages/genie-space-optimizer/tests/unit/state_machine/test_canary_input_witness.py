def test_canary_logs_input_counts_before_run(capsys, monkeypatch):
    monkeypatch.setenv("GSO_PLAN_V3_STATE_MACHINE_ITERATION", "true")
    from genie_space_optimizer.optimization.optimizer import (
        maybe_run_state_machine_canary_iteration,
    )
    rows = [
        {"question_id": "gs_009", "feedback/result_correctness/value": "no"},
        {"question_id": "gs_010", "feedback/result_correctness/value": "yes"},
    ]
    maybe_run_state_machine_canary_iteration(
        eval_rows=rows, iteration=1, run_id="r1",
    )
    out = capsys.readouterr().out
    assert "GSO_PLAN_V3_CANARY_INPUT_V1" in out
    assert "eval_rows=2" in out
    assert "hard_rows=1" in out
