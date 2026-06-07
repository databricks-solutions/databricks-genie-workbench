"""Trial 29 W29.1 — Stage 3 synthesis prompt renders the inert-mechanism
history so the LLM picks from ``_structural_fix_mechanisms(rca) - rejected``.

Does NOT exercise the LLM — only the prompt-section assembly path.
The full Stage 3 prompt assembly is wired in a follow-up; this test
pins the unit-level renderer behaviour: empty input → empty string
(byte-stable), populated input → explicit avoidance instruction +
per-(qid, rca_kind) section.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.inert_mechanism_history import (
    InertMechanismHistory,
)
from genie_space_optimizer.optimization.stages.synthesize import (
    render_inert_mechanism_history_section,
)


def test_renders_rejected_mechanism_in_prompt():
    history = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=(
                "add_sql_snippet_filter:filter:insufficient:"
                "rca=wrong_aggregation:behavior=unchanged",
            ),
        ),
    )
    section = render_inert_mechanism_history_section(history)
    assert "gs_009" in section
    assert "wrong_aggregation" in section
    assert "add_sql_snippet_filter" in section
    # The renderer must instruct the LLM to AVOID the rejected
    # mechanism — explicit verb, not implied by data layout.
    lower = section.lower()
    assert (
        "avoid" in lower
        or "do not" in lower
        or "must not" in lower
    )


def test_empty_history_renders_empty_string():
    section = render_inert_mechanism_history_section(())
    assert section == ""


def test_multiple_qids_render_distinct_sections():
    history = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=("sig1",),
        ),
        InertMechanismHistory(
            qid="gs_026",
            rca_kind="plural_top_n_collapse",
            rejected_mechanisms=("add_example_sql", "replace_join"),
            signatures=("sig2", "sig3"),
        ),
    )
    section = render_inert_mechanism_history_section(history)
    assert "gs_009" in section
    assert "gs_026" in section
    assert "add_sql_snippet_filter" in section
    assert "add_example_sql" in section
    assert "replace_join" in section
