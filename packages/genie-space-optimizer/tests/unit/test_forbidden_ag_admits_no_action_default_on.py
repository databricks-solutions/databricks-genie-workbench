"""Cycle 14-W T4 — ``GSO_FORBIDDEN_AG_ADMITS_NO_ACTION`` flips
default-on (corpus-validated by C14-V T1 on 7Now run
960148942255012 F5: shadow marker fired 5/5 with the regression
rail silent).

Regression rail ``GSO_FORBIDDEN_AG_ADMISSION_BYPASSED_V1`` must
stay silent post-flip on a clean corpus.
"""

from __future__ import annotations

import os
from unittest import mock


def test_default_on_in_clean_environment() -> None:
    """No env override → flag returns True (Cycle 14-W T4 flip)."""
    with mock.patch.dict(os.environ, {}, clear=True):
        from genie_space_optimizer.common.config import (
            forbidden_ag_admits_no_action_enabled,
        )
        assert forbidden_ag_admits_no_action_enabled() is True


def test_explicit_off_disables() -> None:
    """Operator override ``GSO_FORBIDDEN_AG_ADMITS_NO_ACTION=0`` is
    respected (replay byte-stability against pre-14-W fixtures)."""
    with mock.patch.dict(
        os.environ,
        {"GSO_FORBIDDEN_AG_ADMITS_NO_ACTION": "0"},
        clear=True,
    ):
        from genie_space_optimizer.common.config import (
            forbidden_ag_admits_no_action_enabled,
        )
        assert forbidden_ag_admits_no_action_enabled() is False


def test_explicit_on_still_on() -> None:
    with mock.patch.dict(
        os.environ,
        {"GSO_FORBIDDEN_AG_ADMITS_NO_ACTION": "1"},
        clear=True,
    ):
        from genie_space_optimizer.common.config import (
            forbidden_ag_admits_no_action_enabled,
        )
        assert forbidden_ag_admits_no_action_enabled() is True


def test_no_action_reflection_is_admitted_post_default_flip() -> None:
    """Corpus validation: under the new default-on, a NO_ACTION
    reflection contributes its (root_cause, blame, levers) tuple
    to the forbidden set without an explicit env override."""
    with mock.patch.dict(os.environ, {}, clear=True):
        from genie_space_optimizer.optimization.harness import (
            _compute_forbidden_ag_set,
        )

        reflection = {
            "iteration": 2,
            "rollback_class": "no_action",
            "rollback_reason": "no_proposals",
            "accepted": False,
            "escalation_handled": False,
            "root_cause": "plural_top_n_collapse",
            "blame_set": ("mv_esr_dim_location.zone_vp_name",),
            "lever_set": [1, 5],
        }
        forbidden = _compute_forbidden_ag_set([reflection])
    assert (
        "plural_top_n_collapse",
        ("mv_esr_dim_location.zone_vp_name",),
        frozenset({1, 5}),
    ) in forbidden
