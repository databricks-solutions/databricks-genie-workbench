from pathlib import Path


def test_harness_has_authoritative_diagnostic_action_queue():
    source = Path("src/genie_space_optimizer/optimization/harness.py").read_text()

    assert "diagnostic_action_queue" in source
    assert "USING DIAGNOSTIC AG FROM COVERAGE GAP" in source
    assert "SKIPPING DIAGNOSTIC AG BECAUSE CLUSTER RESOLVED" in source
    assert source.index("diagnostic_action_queue") < source.index("_call_llm_for_adaptive_strategy")


def test_harness_prints_control_plane_baseline_source_and_iteration_ids():
    from pathlib import Path

    source = Path("src/genie_space_optimizer/optimization/harness.py").read_text()

    assert "Baseline source for control plane" in source
    assert "Pre row iteration id" in source
    assert "Post row iteration id" in source


def test_harness_uses_qid_values_for_gt_correction_candidate_count():
    from pathlib import Path

    source = Path("src/genie_space_optimizer/optimization/harness.py").read_text()

    assert "_gt_correction_candidate_qids" in source
    assert "GT correction candidates" in source
    assert "len(_gt_correction_candidate_qids)" in source


def test_gate_baseline_contract_documents_accepted_rows_after_rejection():
    from pathlib import Path

    source = Path("src/genie_space_optimizer/optimization/harness.py").read_text()

    assert "accepted_baseline_rows_for_control_plane" in source
    assert "candidate row we just persisted cannot serve as its own baseline" in source
    assert "Baseline source for control plane" in source
