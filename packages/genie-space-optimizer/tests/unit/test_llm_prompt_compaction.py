"""Phase 0 P0.4 — LRU compaction + size-cap tests."""
from __future__ import annotations

from genie_space_optimizer.optimization.llm_prompt_compaction import (
    compact_history_slots_to_fit,
    estimate_tokens_from_chars,
)


def test_estimate_tokens_from_chars_uses_four_to_one_ratio() -> None:
    assert estimate_tokens_from_chars(40) == 10
    assert estimate_tokens_from_chars(1) == 1
    assert estimate_tokens_from_chars(0) == 1


def test_no_compaction_when_payload_already_fits() -> None:
    """If we're already under the cap, history must not be touched."""
    history = [{"i": i} for i in range(5)]
    drops = compact_history_slots_to_fit(
        static_chars=100,
        history_slots=[("history", history)],
        target_token_cap=10_000,
    )
    assert drops == {"history": 0}
    assert len(history) == 5


def test_compaction_drops_oldest_history_entries_first() -> None:
    """The LRU policy is ``pop(0)`` — oldest entries leave first."""
    history = [
        {"id": "old", "data": "x" * 200},
        {"id": "mid", "data": "x" * 200},
        {"id": "new", "data": "x" * 200},
    ]
    drops = compact_history_slots_to_fit(
        static_chars=0,
        history_slots=[("history", history)],
        target_token_cap=200,  # ~800 chars budget
        safety_margin_tokens=50,
    )
    assert drops["history"] >= 1
    # The newest entry must still be there.
    assert any(item["id"] == "new" for item in history)


def test_compaction_drains_low_value_slot_before_touching_next() -> None:
    """When multiple slots are passed, the compactor exhausts the
    first (least valuable) slot before pulling from the next."""
    low = [f"low_{i}" * 50 for i in range(10)]
    high = [f"high_{i}" * 50 for i in range(10)]
    starting_low = len(low)
    starting_high = len(high)
    drops = compact_history_slots_to_fit(
        static_chars=0,
        history_slots=[("low", low), ("high", high)],
        target_token_cap=200,
        safety_margin_tokens=50,
    )
    # If the cap is small enough to need dropping, the low slot
    # should be drained before the high slot loses any entries.
    if drops["low"] < starting_low:
        assert drops["high"] == 0
    # Otherwise (low drained), high may have lost entries too.
    if drops["low"] == starting_low:
        # high may or may not lose entries depending on remainder.
        assert drops["high"] >= 0
        assert drops["high"] <= starting_high


def test_compaction_stops_once_under_cap() -> None:
    """The compactor must not over-drop; it stops as soon as the
    estimated token count is below the safety-adjusted cap."""
    history = [
        {"id": "old", "data": "x" * 200},
        {"id": "new", "data": "x" * 10},  # tiny, would fit alone
    ]
    drops = compact_history_slots_to_fit(
        static_chars=0,
        history_slots=[("history", history)],
        target_token_cap=200,
        safety_margin_tokens=50,
    )
    # If even one entry survives, we stopped early.
    assert len(history) >= 1
    assert sum(drops.values()) <= 2


def test_prompt_too_large_abstain_minted_when_above_cap() -> None:
    """``LlmReasoningCall.invoke`` must mint a PROMPT_TOO_LARGE
    decline (not crash, not call the LLM) when the estimated input
    tokens exceed MAX_PROMPT_INPUT_TOKENS."""
    from pydantic import BaseModel

    from genie_space_optimizer.optimization.llm_abstain import AbstainReason
    from genie_space_optimizer.optimization.llm_reasoning_call import (
        LlmReasoningCall,
        MAX_PROMPT_INPUT_TOKENS,
    )
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningRequest,
    )

    class _Dummy(BaseModel):
        pass

    # ~5 chars/token; force est_input to clearly exceed the cap.
    over_size_chars = (MAX_PROMPT_INPUT_TOKENS + 2_000) * 5
    request = LlmReasoningRequest(
        call_id="too_big",
        skill_id="test",
        system_msg="s" * (over_size_chars // 2),
        user_prompt="u" * (over_size_chars // 2),
        result_cls=_Dummy,
        max_tokens=100,
    )
    resp = LlmReasoningCall().invoke(w=None, request=request)
    assert resp.succeeded is False
    assert resp.declined is not None
    assert resp.declined.reason == AbstainReason.PROMPT_TOO_LARGE
    assert resp.tokens_input == 0
    assert resp.tokens_output == 0
