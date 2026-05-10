"""Cycle 13 / Cycle 14-W T4 — ``GSO_FORBIDDEN_AG_ADMITS_NO_ACTION``
default-state regression rail.

Cycle 14-W T4 flipped the default to ON after corpus evidence
from 7Now run 960148942255012 F5 confirmed the C14-V T1 shadow
marker fired on 5/5 NO_ACTION reflections with the regression rail
silent.
"""

from __future__ import annotations

import os
from unittest import mock


def test_default_on() -> None:
    """Cycle 14-W T4: default flipped to ON."""
    with mock.patch.dict(os.environ, {}, clear=True):
        from genie_space_optimizer.common.config import (
            forbidden_ag_admits_no_action_enabled,
        )
        assert forbidden_ag_admits_no_action_enabled() is True


def test_env_var_on() -> None:
    with mock.patch.dict(
        os.environ, {"GSO_FORBIDDEN_AG_ADMITS_NO_ACTION": "1"}, clear=True
    ):
        from genie_space_optimizer.common.config import (
            forbidden_ag_admits_no_action_enabled,
        )
        assert forbidden_ag_admits_no_action_enabled() is True


def test_env_var_zero_is_off() -> None:
    """Operator override `GSO_FORBIDDEN_AG_ADMITS_NO_ACTION=0` is
    respected (replay byte-stability against pre-14-W fixtures)."""
    with mock.patch.dict(
        os.environ, {"GSO_FORBIDDEN_AG_ADMITS_NO_ACTION": "0"}, clear=True
    ):
        from genie_space_optimizer.common.config import (
            forbidden_ag_admits_no_action_enabled,
        )
        assert forbidden_ag_admits_no_action_enabled() is False
