from __future__ import annotations

import pytest
from genie_space_optimizer.optimization.unified_loop import (
    PAIRED_SIGN_EVIDENCE_THRESHOLD,
    paired_sign_evidence,
)


def _paired_rows(
    *,
    wins: int,
    losses: int,
    both_good: int = 0,
    both_bad: int = 0,
) -> tuple[dict, dict]:
    control: list[dict[str, str]] = []
    candidate: list[dict[str, str]] = []

    def append_pair(control_assessment: str, candidate_assessment: str) -> None:
        question_id = f"q{len(control) + 1}"
        control.append(
            {"question_id": question_id, "assessment": control_assessment}
        )
        candidate.append(
            {"question_id": question_id, "assessment": candidate_assessment}
        )

    for _ in range(wins):
        append_pair("BAD", "GOOD")
    for _ in range(losses):
        append_pair("GOOD", "BAD")
    for _ in range(both_good):
        append_pair("GOOD", "GOOD")
    for _ in range(both_bad):
        append_pair("BAD", "BAD")
    return {"rows": control}, {"rows": candidate}


def test_formula_five_wins_two_losses_is_rejected() -> None:
    control, candidate = _paired_rows(wins=5, losses=2, both_good=33, both_bad=47)

    evidence = paired_sign_evidence(control, candidate)

    assert evidence == {
        "valid": True,
        "passes": False,
        "wins": 5,
        "losses": 2,
        "ties": 80,
        "discordant": 7,
        "p_value": pytest.approx(29 / 128),
        "threshold": PAIRED_SIGN_EVIDENCE_THRESHOLD,
        "reason": "insufficient_paired_evidence",
    }


def test_toxicology_thirty_seven_wins_zero_losses_is_accepted() -> None:
    control, candidate = _paired_rows(wins=37, losses=0, both_good=15, both_bad=26)

    evidence = paired_sign_evidence(control, candidate)

    assert evidence["valid"] is True
    assert evidence["passes"] is True
    assert evidence["wins"] == 37
    assert evidence["losses"] == 0
    assert evidence["p_value"] == pytest.approx(1 / (2**37))


def test_financial_nine_wins_three_losses_is_accepted() -> None:
    control, candidate = _paired_rows(wins=9, losses=3, both_good=13, both_bad=31)

    evidence = paired_sign_evidence(control, candidate)

    assert evidence["valid"] is True
    assert evidence["passes"] is True
    assert evidence["wins"] == 9
    assert evidence["losses"] == 3
    assert evidence["p_value"] == pytest.approx(299 / 4096)


@pytest.mark.parametrize(
    ("control_rows", "candidate_rows", "expected_reason"),
    [
        ([], [], "missing_rows"),
        (
            [{"question_id": "q1", "assessment": "BAD"}],
            [{"question_id": "q2", "assessment": "GOOD"}],
            "question_id_mismatch",
        ),
        (
            [
                {"question_id": "q1", "assessment": "BAD"},
                {"question_id": "q1", "assessment": "BAD"},
            ],
            [{"question_id": "q1", "assessment": "GOOD"}],
            "duplicate_question_id",
        ),
        (
            [{"assessment": "BAD"}],
            [{"question_id": "q1", "assessment": "GOOD"}],
            "missing_question_id",
        ),
        (
            [{"question_id": "q1", "assessment": ""}],
            [{"question_id": "q1", "assessment": "GOOD"}],
            "unscored_row",
        ),
    ],
)
def test_invalid_pairings_fail_closed(
    control_rows: list[dict[str, str]],
    candidate_rows: list[dict[str, str]],
    expected_reason: str,
) -> None:
    evidence = paired_sign_evidence(
        {"rows": control_rows},
        {"rows": candidate_rows},
    )

    assert evidence["valid"] is False
    assert evidence["passes"] is False
    assert evidence["reason"] == expected_reason


def test_zero_discordant_pairs_are_rejected() -> None:
    control, candidate = _paired_rows(wins=0, losses=0, both_good=3, both_bad=2)

    evidence = paired_sign_evidence(control, candidate)

    assert evidence["valid"] is True
    assert evidence["passes"] is False
    assert evidence["discordant"] == 0
    assert evidence["p_value"] == 1.0
    assert evidence["reason"] == "no_discordant_pairs"


def test_needs_review_is_counted_as_an_official_non_good_outcome() -> None:
    evidence = paired_sign_evidence(
        {"rows": [{"question_id": "q1", "assessment": "NEEDS_REVIEW"}]},
        {"rows": [{"question_id": "q1", "assessment": "GOOD"}]},
    )

    assert evidence["valid"] is True
    assert evidence["wins"] == 1
    assert evidence["losses"] == 0
