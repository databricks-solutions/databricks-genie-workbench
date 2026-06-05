"""Trial 23 W3 — reliable pivot from-state inference.

The d139 postmortem showed ``pivot_recommended=false`` for a
kept_insufficient cluster because the pivot from-state
(``prior_patch_family``) was empty. W3 recovers the from-state from the
prior terminal signatures so the Plan 12 pivot decider has a real
"from".
"""
from __future__ import annotations

from genie_space_optimizer.optimization.harness import (
    _infer_prior_family_from_signatures,
)
from genie_space_optimizer.optimization.terminal_signature import (
    build_terminal_signature,
)


def _sig(*, prior_patch_family="", prior_lever_set=None, lever_set=None):
    return build_terminal_signature(
        root_cause="rc",
        blame_set=(),
        lever_set=lever_set or set(),
        target_qids={"gs_009"},
        terminal_reason="kept_insufficient",
        prior_lever_set=prior_lever_set,
        prior_patch_family=prior_patch_family,
    )


def test_infers_from_prior_patch_family_field():
    sigs = [_sig(prior_patch_family="add_example_sql")]
    assert (
        _infer_prior_family_from_signatures(sigs) == "add_example_sql"
    )


def test_latest_signature_wins():
    sigs = [
        _sig(prior_patch_family="add_column_description"),
        _sig(prior_patch_family="add_example_sql"),
    ]
    assert (
        _infer_prior_family_from_signatures(sigs) == "add_example_sql"
    )


def test_falls_back_to_prior_lever_set():
    sigs = [_sig(prior_lever_set=("lever-5",))]
    assert (
        _infer_prior_family_from_signatures(sigs) == "add_example_sql"
    )


def test_falls_back_to_int_lever_set():
    sigs = [_sig(lever_set={6})]
    assert (
        _infer_prior_family_from_signatures(sigs)
        == "add_sql_snippet_expression"
    )


def test_empty_when_nothing_inferable():
    assert _infer_prior_family_from_signatures([]) == ""
    assert _infer_prior_family_from_signatures([_sig()]) == ""
