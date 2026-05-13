"""Phase 3 Task 5 — directive_outcome_marker formatter."""

from __future__ import annotations

import json
import re


def test_directive_outcome_marker_has_canonical_shape() -> None:
    from genie_space_optimizer.optimization.directive_outcome import (
        AgDirectiveLedger,
        DirectiveOutcomeCode,
    )
    from genie_space_optimizer.optimization.run_analysis_contract import (
        directive_outcome_marker,
    )

    ledger = AgDirectiveLedger(
        ag_id="AG2",
        iteration=2,
        directives_present=(5, 6),
        outcomes_by_lever={
            5: DirectiveOutcomeCode.NO_STRUCTURAL_CANDIDATE,
            6: DirectiveOutcomeCode.FORCE_LLM_DECLINED,
        },
    )
    line = directive_outcome_marker(
        optimization_run_id="2314bb2c-95a1-4d60-8226-09e5155aee2a",
        ledger=ledger,
    )
    assert line.startswith("GSO_DIRECTIVE_OUTCOME_V1 ")
    body = re.search(r"\s+(\{.*\})", line).group(1)
    payload = json.loads(body)
    assert payload["optimization_run_id"] == (
        "2314bb2c-95a1-4d60-8226-09e5155aee2a"
    )
    assert payload["ag_id"] == "AG2"
    assert payload["iteration"] == 2
    assert payload["directives_present"] == [5, 6]
    assert payload["outcomes_by_lever"] == {
        "5": "no_structural_candidate",
        "6": "force_llm_declined",
    }


def test_directive_outcome_marker_is_single_line_json() -> None:
    """No internal newlines — the marker is consumed by stdout parsers that
    split on '\\n\\n' or per-line patterns."""
    from genie_space_optimizer.optimization.directive_outcome import (
        AgDirectiveLedger,
        DirectiveOutcomeCode,
    )
    from genie_space_optimizer.optimization.run_analysis_contract import (
        directive_outcome_marker,
    )

    ledger = AgDirectiveLedger(
        ag_id="AG1",
        iteration=1,
        directives_present=(5,),
        outcomes_by_lever={
            5: DirectiveOutcomeCode.PROPOSAL_EMITTED,
        },
    )
    line = directive_outcome_marker(
        optimization_run_id="r",
        ledger=ledger,
    )
    assert "\n" not in line.rstrip("\n")
