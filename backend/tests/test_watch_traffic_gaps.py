from __future__ import annotations

from datetime import datetime, timezone

from backend.watch.services.traffic_gaps import (
    TrafficMessage,
    analyze_traffic_gaps,
    normalize_question,
)


def _message(
    content: str,
    *,
    conversation_id: str,
    user_key: str = "user-1",
    status: str = "COMPLETED",
    feedback: str | None = None,
) -> TrafficMessage:
    return TrafficMessage(
        content=content,
        conversation_id=conversation_id,
        user_key=user_key,
        status=status,
        feedback=feedback,
        created_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
    )


def test_normalization_groups_literal_variants_without_fuzzy_matching() -> None:
    assert normalize_question("Revenue for 'EMEA' in 2025?") == normalize_question(
        'revenue for "APAC" in 2024'
    )
    assert normalize_question("revenue by country") != normalize_question(
        "revenue for country"
    )
    assert "revenue" in normalize_question("What's revenue for John's region?")


def test_covered_family_is_not_returned() -> None:
    result = analyze_traffic_gaps(
        messages=[
            _message(
                "Revenue for 'EMEA' in 2025",
                conversation_id="conv-1",
                status="FAILED",
            )
        ],
        benchmark_questions=["Revenue for 'APAC' in 2024"],
        conversation_url=lambda conversation_id: f"https://example/{conversation_id}",
    )

    assert result.covered_family_count == 1
    assert result.candidates == []


def test_cross_user_repeat_is_actionable_but_single_user_repeat_is_not() -> None:
    repeated = [
        _message("Show churn for 2025", conversation_id="conv-1", user_key="u1"),
        _message("Show churn for 2024", conversation_id="conv-2", user_key="u2"),
    ]
    result = analyze_traffic_gaps(
        messages=repeated,
        benchmark_questions=[],
        conversation_url=lambda conversation_id: f"https://example/{conversation_id}",
    )
    assert len(result.candidates) == 1
    assert result.candidates[0].signals == ["cross_user_repeat"]
    assert result.candidates[0].distinct_user_count == 2

    same_user = [
        _message("Show churn for 2025", conversation_id="conv-1"),
        _message("Show churn for 2024", conversation_id="conv-2"),
    ]
    result = analyze_traffic_gaps(
        messages=same_user,
        benchmark_questions=[],
        conversation_url=lambda conversation_id: f"https://example/{conversation_id}",
    )
    assert result.candidates == []


def test_failures_and_negative_feedback_are_actionable() -> None:
    result = analyze_traffic_gaps(
        messages=[
            _message("Question one", conversation_id="conv-1", status="FAILED"),
            _message(
                "Question two",
                conversation_id="conv-2",
                feedback="NEGATIVE",
            ),
        ],
        benchmark_questions=[],
        conversation_url=lambda conversation_id: f"https://example/{conversation_id}",
    )

    assert [candidate.signals for candidate in result.candidates] == [
        ["negative_feedback"],
        ["failed"],
    ]


def test_in_flight_and_cancelled_messages_are_not_evidence() -> None:
    result = analyze_traffic_gaps(
        messages=[
            _message(
                "Show churn for 2025",
                conversation_id="conv-1",
                user_key="u1",
                status="SUBMITTED",
            ),
            _message(
                "Show churn for 2024",
                conversation_id="conv-2",
                user_key="u2",
                status="CANCELLED",
            ),
        ],
        benchmark_questions=[],
        conversation_url=lambda conversation_id: f"https://example/{conversation_id}",
    )

    assert result.scanned_message_count == 0
    assert result.candidates == []


def test_response_has_opaque_ids_no_raw_content_and_at_most_three_links() -> None:
    messages = [
        _message(
            f"Show orders for {2020 + index}",
            conversation_id=f"conv-{index}",
            user_key=f"user-{index}",
        )
        for index in range(5)
    ]
    result = analyze_traffic_gaps(
        messages=messages,
        benchmark_questions=[],
        conversation_url=lambda conversation_id: f"https://example/{conversation_id}",
    )

    payload = result.model_dump(mode="json")
    assert payload["candidates"][0]["candidate_id"] == "candidate-1"
    assert len(payload["candidates"][0]["conversation_urls"]) == 3
    serialized = result.model_dump_json()
    assert "Show orders" not in serialized
    assert "user-" not in serialized
    assert "normalized" not in serialized
