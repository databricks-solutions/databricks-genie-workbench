"""Plan 11 — feature flag truthy parsing.

PR 3 flipped the default ON (matches the canonical Plan 3 / 4 / 5 / 7
pattern in this codebase). Override per-deploy by setting the env var
to any falsy value to fall back to the legacy synthesis path.
"""
import os
from unittest.mock import patch


def test_flag_on_by_default():
    """PR 3: with no env var set, plan11_llm_first_enabled() returns True."""
    from genie_space_optimizer.common.config import plan11_llm_first_enabled
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_PLAN11_LLM_FIRST", None)
        assert plan11_llm_first_enabled() is True


def test_flag_on_with_truthy_values():
    from genie_space_optimizer.common.config import plan11_llm_first_enabled
    for val in ("true", "True", "TRUE", "1", "yes", "on"):
        with patch.dict(os.environ, {"GSO_PLAN11_LLM_FIRST": val}):
            assert plan11_llm_first_enabled() is True, (
                f"Expected True for {val!r}"
            )


def test_flag_off_with_falsy_values():
    """Explicit override to disable Plan 11 and fall back to the
    legacy archetype path."""
    from genie_space_optimizer.common.config import plan11_llm_first_enabled
    for val in ("false", "False", "0", "no", "off"):
        with patch.dict(os.environ, {"GSO_PLAN11_LLM_FIRST": val}):
            assert plan11_llm_first_enabled() is False, (
                f"Expected False for {val!r}"
            )


def test_flag_on_with_empty_value():
    """Empty string is treated as 'not explicitly disabled' under the
    _flag_default_on contract — same shape as Plan 3/4/5/7."""
    from genie_space_optimizer.common.config import plan11_llm_first_enabled
    with patch.dict(os.environ, {"GSO_PLAN11_LLM_FIRST": ""}):
        assert plan11_llm_first_enabled() is True
