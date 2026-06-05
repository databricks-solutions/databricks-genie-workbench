"""Phase 0 P0.5 — Anthropic prompt-cache helpers."""
from __future__ import annotations

import os
from unittest import mock

from genie_space_optimizer.optimization.llm_prompt_cache import (
    build_cached_messages,
    cache_control_enabled,
)


def test_cache_control_enabled_default_is_true() -> None:
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_PROMPT_CACHE", None)
        assert cache_control_enabled() is True


def test_cache_control_disabled_when_flag_off() -> None:
    with mock.patch.dict(os.environ, {"GSO_PROMPT_CACHE": "0"}, clear=False):
        assert cache_control_enabled() is False
    with mock.patch.dict(os.environ, {"GSO_PROMPT_CACHE": "false"}, clear=False):
        assert cache_control_enabled() is False


def test_system_block_carries_cache_control_when_enabled() -> None:
    messages = build_cached_messages(
        system_text="skill body",
        cacheable_user_blocks=(),
        dynamic_user_text="dynamic payload",
        cache_control=True,
    )
    assert messages[0]["role"] == "system"
    content = messages[0]["content"]
    assert isinstance(content, list)
    assert content[0]["cache_control"] == {"type": "ephemeral"}
    assert content[0]["text"] == "skill body"


def test_user_block_carries_cache_control_when_enabled() -> None:
    messages = build_cached_messages(
        system_text="skill",
        cacheable_user_blocks=("menu_block",),
        dynamic_user_text="dyn",
        cache_control=True,
    )
    user_msg = next(m for m in messages if m["role"] == "user")
    assert isinstance(user_msg["content"], list)
    # First block is the cacheable one.
    assert user_msg["content"][0]["text"] == "menu_block"
    assert user_msg["content"][0]["cache_control"] == {"type": "ephemeral"}
    # Last block is the dynamic one, NO cache_control.
    assert user_msg["content"][-1]["text"] == "dyn"
    assert "cache_control" not in user_msg["content"][-1]


def test_three_cacheable_blocks_each_marked() -> None:
    """The Anthropic budget is up to 4 breakpoints: 1 system + 3 user.
    All three cacheable user blocks must carry cache_control."""
    messages = build_cached_messages(
        system_text="skill",
        cacheable_user_blocks=("a", "b", "c"),
        dynamic_user_text="d",
        cache_control=True,
    )
    user_msg = next(m for m in messages if m["role"] == "user")
    content = user_msg["content"]
    # 3 cached + 1 dynamic = 4 blocks.
    assert len(content) == 4
    for block in content[:3]:
        assert block.get("cache_control") == {"type": "ephemeral"}
    assert "cache_control" not in content[3]


def test_overflow_blocks_merged_into_dynamic() -> None:
    """More than 3 cacheable blocks: the extras must NOT carry
    cache_control and must be folded into the dynamic block so the
    prefix-cache lookup terminates at the 3rd cached block."""
    messages = build_cached_messages(
        system_text="skill",
        cacheable_user_blocks=("a", "b", "c", "d_overflow"),
        dynamic_user_text="real_dyn",
        cache_control=True,
    )
    user_msg = next(m for m in messages if m["role"] == "user")
    content = user_msg["content"]
    # 3 cached + 1 dynamic = 4 blocks; overflow merged into final.
    assert len(content) == 4
    cached_texts = [b["text"] for b in content[:3]]
    assert cached_texts == ["a", "b", "c"]
    assert "d_overflow" in content[3]["text"]
    assert "real_dyn" in content[3]["text"]


def test_cache_control_off_emits_legacy_string_shape() -> None:
    """When caching is disabled and there is exactly one user block,
    fall back to plain string content for byte-stability."""
    messages = build_cached_messages(
        system_text="skill",
        cacheable_user_blocks=(),
        dynamic_user_text="just text",
        cache_control=False,
    )
    user_msg = next(m for m in messages if m["role"] == "user")
    assert user_msg["content"] == "just text"
    sys_msg = next(m for m in messages if m["role"] == "system")
    assert sys_msg["content"] == "skill"


def test_empty_system_omits_system_message() -> None:
    messages = build_cached_messages(
        system_text="",
        cacheable_user_blocks=("a",),
        dynamic_user_text="b",
        cache_control=True,
    )
    assert all(m["role"] == "user" for m in messages)


def test_llm_reasoning_request_carries_cacheable_blocks() -> None:
    """The dataclass exposes ``cacheable_user_blocks`` with a tuple
    default and accepts an explicit value."""
    from pydantic import BaseModel

    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningRequest,
    )

    class _Dummy(BaseModel):
        pass

    req = LlmReasoningRequest(
        call_id="c", skill_id="s",
        system_msg="sys", user_prompt="usr",
        result_cls=_Dummy, max_tokens=100,
    )
    assert req.cacheable_user_blocks == ()

    req2 = LlmReasoningRequest(
        call_id="c", skill_id="s",
        system_msg="sys", user_prompt="usr",
        result_cls=_Dummy, max_tokens=100,
        cacheable_user_blocks=("block1", "block2"),
    )
    assert req2.cacheable_user_blocks == ("block1", "block2")
