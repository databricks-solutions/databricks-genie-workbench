from __future__ import annotations


def test_extract_json_returns_none_for_empty_string() -> None:
    from genie_space_optimizer.optimization.evaluation import _extract_json

    assert _extract_json("") is None


def test_extract_json_returns_none_for_whitespace_only() -> None:
    from genie_space_optimizer.optimization.evaluation import _extract_json

    assert _extract_json("   \n\t  ") is None


def test_extract_json_returns_none_for_fenced_block_with_no_body() -> None:
    from genie_space_optimizer.optimization.evaluation import _extract_json

    assert _extract_json("```json\n```") is None


def test_extract_json_returns_none_for_non_json_prose() -> None:
    from genie_space_optimizer.optimization.evaluation import _extract_json

    assert _extract_json("I cannot answer that.") is None


def test_extract_json_still_parses_valid_json() -> None:
    from genie_space_optimizer.optimization.evaluation import _extract_json

    assert _extract_json('{"verdict": "ground_truth_correct"}') == {
        "verdict": "ground_truth_correct"
    }


def test_extract_json_can_still_raise_when_strict_is_set() -> None:
    import pytest

    from genie_space_optimizer.optimization.evaluation import _extract_json

    with pytest.raises(Exception):
        _extract_json("", strict=True)
