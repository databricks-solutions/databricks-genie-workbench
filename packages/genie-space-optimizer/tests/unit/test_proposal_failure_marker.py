"""Plan P-F — stdout marker emit + parse tests."""

from __future__ import annotations


def test_proposal_failure_decided_marker_round_trips() -> None:
    """Emit a GSO_PROPOSAL_FAILURE_DECIDED_V1 line and parse it back."""
    from genie_space_optimizer.optimization.run_analysis_contract import (
        proposal_failure_decided_marker,
    )
    from genie_space_optimizer.tools.marker_parser import (
        parse_proposal_failure_decided_marker,
    )

    line = proposal_failure_decided_marker(
        ag_id="AG_TEST",
        iteration=2,
        failure_mode="proposal_generation_empty",
        next_action="rotate_lever_family",
        cluster_signature="sig:abc",
        prior_failure_count=0,
    )
    assert line.startswith("GSO_PROPOSAL_FAILURE_DECIDED_V1 ")

    parsed = parse_proposal_failure_decided_marker(line)
    assert parsed["ag_id"] == "AG_TEST"
    assert parsed["iteration"] == 2
    assert parsed["failure_mode"] == "proposal_generation_empty"
    assert parsed["next_action"] == "rotate_lever_family"
    assert parsed["cluster_signature"] == "sig:abc"
    assert parsed["prior_failure_count"] == 0


def test_parser_raises_on_wrong_marker_name() -> None:
    """Parser refuses lines that do not start with the expected name."""
    from genie_space_optimizer.tools.marker_parser import (
        parse_proposal_failure_decided_marker,
    )

    import pytest

    with pytest.raises(ValueError):
        parse_proposal_failure_decided_marker(
            'GSO_PROPOSAL_GENERATION_EMPTY_V1 {"ag_id": "AG"}'
        )
