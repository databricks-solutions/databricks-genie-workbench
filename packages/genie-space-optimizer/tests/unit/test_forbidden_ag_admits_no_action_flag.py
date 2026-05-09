"""Cycle 13 — GSO_FORBIDDEN_AG_ADMITS_NO_ACTION is default-off."""

from __future__ import annotations

import os
from unittest import mock


def test_default_off() -> None:
    with mock.patch.dict(os.environ, {}, clear=True):
        from genie_space_optimizer.common.config import (
            forbidden_ag_admits_no_action_enabled,
        )
        assert forbidden_ag_admits_no_action_enabled() is False


def test_env_var_on() -> None:
    with mock.patch.dict(
        os.environ, {"GSO_FORBIDDEN_AG_ADMITS_NO_ACTION": "1"}, clear=True
    ):
        from genie_space_optimizer.common.config import (
            forbidden_ag_admits_no_action_enabled,
        )
        assert forbidden_ag_admits_no_action_enabled() is True


def test_env_var_zero_is_off() -> None:
    with mock.patch.dict(
        os.environ, {"GSO_FORBIDDEN_AG_ADMITS_NO_ACTION": "0"}, clear=True
    ):
        from genie_space_optimizer.common.config import (
            forbidden_ag_admits_no_action_enabled,
        )
        assert forbidden_ag_admits_no_action_enabled() is False
