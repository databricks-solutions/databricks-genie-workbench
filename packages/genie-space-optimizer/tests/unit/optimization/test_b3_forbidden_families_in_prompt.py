"""Trial 19 B3 — assert Stage 3 prompt encodes explicit patch-family-fit rules.

The closed ``_FORBIDDEN_FAMILIES`` dict in ``rca_card_builder`` is the
back-compat reader. The authoritative copy of those constraints now
lives in the Stage 3 ``plan11_synthesize`` skill prompt. This test
pins the rules so silent regressions on the prompt text are caught.
"""
from importlib import resources

import pytest


_SKILL_PATH = (
    "genie_space_optimizer.skills.plan11_synthesize"
)


@pytest.fixture(scope="module")
def synthesize_prompt() -> str:
    return resources.files(_SKILL_PATH).joinpath("SKILL.md").read_text()


_EXPECTED_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "top_n_cardinality_collapse",
        ("avoid_unrequested_defensive_filters", "filter_logic_guidance"),
    ),
    ("measure_swap", ("avoid_unrequested_defensive_filters",)),
    ("extra_defensive_filter", ("filter_logic_guidance",)),
    ("missing_required_dimension", ("avoid_unrequested_defensive_filters",)),
    ("grain_or_grouping_mismatch", ("avoid_unrequested_defensive_filters",)),
    ("time_window_logic_mismatch", ("avoid_unrequested_defensive_filters",)),
)


@pytest.mark.parametrize(
    "rca_label,forbidden_families",
    [(label, fams) for label, fams in _EXPECTED_RULES],
)
def test_synthesize_prompt_lists_forbidden_family(
    synthesize_prompt: str,
    rca_label: str,
    forbidden_families: tuple[str, ...],
) -> None:
    assert rca_label in synthesize_prompt, (
        f"rca_kind_label {rca_label!r} missing from Stage 3 prompt"
    )
    for family in forbidden_families:
        assert family in synthesize_prompt, (
            f"forbidden patch family {family!r} for rca_kind_label "
            f"{rca_label!r} missing from Stage 3 prompt"
        )


def test_synthesize_prompt_marks_blocklist_non_exhaustive(
    synthesize_prompt: str,
) -> None:
    """Prompt must explicitly invite the LLM to invent new labels."""
    assert "NOT exhaustive" in synthesize_prompt
    assert "rca_kind_label" in synthesize_prompt
