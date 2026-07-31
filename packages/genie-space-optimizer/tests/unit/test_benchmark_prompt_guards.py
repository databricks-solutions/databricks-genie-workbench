from __future__ import annotations

from genie_space_optimizer.common.config import (
    BENCHMARK_CORRECTION_PROMPT,
    BENCHMARK_COVERAGE_GAP_PROMPT,
    BENCHMARK_GENERATION_PROMPT,
    BENCHMARK_QUALITY_REVIEW_PROMPT,
    CURATED_SQL_GENERATION_PROMPT,
)


def test_generation_prompts_require_black_box_business_questions() -> None:
    for prompt in (BENCHMARK_GENERATION_PROMPT, BENCHMARK_COVERAGE_GAP_PROMPT):
        assert "Black-Box Question Contract" in prompt
        assert "must never reveal how to produce the answer" in prompt
        assert "join X to Y" in prompt
        assert "Business terminology" in prompt
        assert "expected_sql may use Join Specifications internally" in prompt


def test_sql_generation_and_repair_prompts_make_questions_immutable() -> None:
    for prompt in (CURATED_SQL_GENERATION_PROMPT, BENCHMARK_CORRECTION_PROMPT):
        assert "question_id" in prompt
        assert "question text is immutable" in prompt
        assert "QUESTION_CHANGE_REQUIRED" in prompt
        assert "Do not return ``question``" in prompt

    assert "either remove the filter or update the question text" not in BENCHMARK_CORRECTION_PROMPT


def test_semantic_review_owns_implementation_hint_detection() -> None:
    assert "IMPLEMENTATION_HINT" in BENCHMARK_QUALITY_REVIEW_PROMPT
    assert 'single word such as "where" or "from"' in BENCHMARK_QUALITY_REVIEW_PROMPT
    assert "advisory review metadata only" in BENCHMARK_QUALITY_REVIEW_PROMPT
