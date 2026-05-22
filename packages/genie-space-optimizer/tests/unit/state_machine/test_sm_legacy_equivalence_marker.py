def test_equivalence_marker_records_agreement():
    from genie_space_optimizer.optimization.state_machine.markers import (
        sm_legacy_equivalence_marker,
    )
    line = sm_legacy_equivalence_marker(
        iteration=2, qid="gs_009",
        sm_terminal="structural_gate_dropped_instruction_only",
        legacy_terminal="structural_gate_dropped_instruction_only",
    )
    assert "GSO_PLAN_V3_EQUIVALENCE_V1" in line
    assert "agreement=yes" in line
    assert "qid=gs_009" in line


def test_equivalence_marker_records_divergence():
    from genie_space_optimizer.optimization.state_machine.markers import (
        sm_legacy_equivalence_marker,
    )
    line = sm_legacy_equivalence_marker(
        iteration=2, qid="gs_009",
        sm_terminal="applied",
        legacy_terminal="structural_gate_dropped_instruction_only",
    )
    assert "agreement=no" in line
    assert "divergence_reason=sm_advanced_legacy_stalled" in line
