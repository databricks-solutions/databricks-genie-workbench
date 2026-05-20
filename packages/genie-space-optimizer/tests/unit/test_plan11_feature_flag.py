"""Plan 11 — feature flag truthy parsing."""
import os
from unittest.mock import patch


def test_flag_off_by_default():
    from genie_space_optimizer.common.config import plan11_llm_first_enabled
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_PLAN11_LLM_FIRST", None)
        assert plan11_llm_first_enabled() is False


def test_flag_on_with_true():
    from genie_space_optimizer.common.config import plan11_llm_first_enabled
    for val in ("true", "True", "TRUE", "1", "yes", "on"):
        with patch.dict(os.environ, {"GSO_PLAN11_LLM_FIRST": val}):
            assert plan11_llm_first_enabled() is True, f"Expected True for {val!r}"


def test_flag_off_with_false():
    from genie_space_optimizer.common.config import plan11_llm_first_enabled
    for val in ("false", "False", "0", "no", "off", ""):
        with patch.dict(os.environ, {"GSO_PLAN11_LLM_FIRST": val}):
            assert plan11_llm_first_enabled() is False, f"Expected False for {val!r}"
