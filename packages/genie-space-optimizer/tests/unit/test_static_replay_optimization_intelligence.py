from pathlib import Path


def test_harness_has_authoritative_diagnostic_action_queue():
    source = Path("src/genie_space_optimizer/optimization/harness.py").read_text()

    assert "diagnostic_action_queue" in source
    assert "USING DIAGNOSTIC AG FROM COVERAGE GAP" in source
    assert "SKIPPING DIAGNOSTIC AG BECAUSE CLUSTER RESOLVED" in source
    assert source.index("diagnostic_action_queue") < source.index("_call_llm_for_adaptive_strategy")
