"""Plan 12 PR 4 deferred — plan12_live_narrow_replacement_enabled()
feature flag. Default OFF — the legacy
narrow_skipped_no_original_patch_type typed-decline emission path
is preserved byte-stable. Operators flip the flag ON per-deploy to
swap in the actual LLM-based narrow-replacement call."""
import os
from unittest.mock import patch


def test_flag_off_by_default():
    from genie_space_optimizer.common.config import (
        plan12_live_narrow_replacement_enabled,
    )
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("GSO_PLAN12_LIVE_NARROW_REPLACEMENT", None)
        assert plan12_live_narrow_replacement_enabled() is False


def test_flag_on_with_truthy_values():
    from genie_space_optimizer.common.config import (
        plan12_live_narrow_replacement_enabled,
    )
    for val in ("true", "True", "TRUE", "1", "yes", "on"):
        with patch.dict(
            os.environ, {"GSO_PLAN12_LIVE_NARROW_REPLACEMENT": val},
        ):
            assert plan12_live_narrow_replacement_enabled() is True, (
                f"Expected True for {val!r}"
            )


def test_flag_off_with_falsy_values():
    from genie_space_optimizer.common.config import (
        plan12_live_narrow_replacement_enabled,
    )
    for val in ("false", "False", "0", "no", "off", ""):
        with patch.dict(
            os.environ, {"GSO_PLAN12_LIVE_NARROW_REPLACEMENT": val},
        ):
            assert plan12_live_narrow_replacement_enabled() is False, (
                f"Expected False for {val!r}"
            )
