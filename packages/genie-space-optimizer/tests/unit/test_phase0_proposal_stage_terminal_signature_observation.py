"""Phase 0 — proposal-stage forbidden-AG observation must report
matches on the terminal-signature axis.

``_check_proposal_stage_forbidden_ag_leakage`` today only consults
the root_cause and signature axes. The terminal-signature axis
(Phase 6.1) is bypassed at this call site, which is why both live
runs' postmortems show
``proposal_stage_forbidden_ag_observed_count_by_call_site``=
``[("cluster_driven_synthesis", 0), ("force_lever6", 0)]`` even
though retired signatures matched the same axis Task 1 just fixed.
"""

from genie_space_optimizer.optimization.harness import (
    _check_proposal_stage_forbidden_ag_leakage,
    _CollisionKeyPair,
    _ForbiddenSetPair,
)


def test_terminal_signature_match_is_reported_at_proposal_stage(monkeypatch):
    monkeypatch.setenv(
        "GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "1",
    )
    candidate = _CollisionKeyPair(
        root_cause_key=None,
        signature_keys=(),
        terminal_signature_keys=(
            (
                frozenset({"7now_delivery_analytics_space_gs_013"}),
                frozenset({6}),
            ),
        ),
    )
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset(),
        by_signature=frozenset(),
        by_terminal_signature=frozenset({
            (
                frozenset({"7now_delivery_analytics_space_gs_013"}),
                frozenset({6}),
            ),
        }),
    )
    iter_inputs: dict = {}
    result = _check_proposal_stage_forbidden_ag_leakage(
        run_id="test-run",
        iteration=2,
        ag_id="AG_DECOMPOSED_H001",
        cluster_id="H001",
        root_cause="wrong_filter_condition",
        collision_pair=candidate,
        forbidden_pair=forbidden,
        cluster_signature="",
        lever_set=(6,),
        call_site="force_lever6",
        iter_inputs=iter_inputs,
    )
    assert result is not None and "terminal_signature" in (result or ""), (
        f"Expected a non-None reason mentioning terminal_signature; "
        f"got {result!r}"
    )


def test_no_match_when_only_signatures_differ(monkeypatch):
    """Negative control: same lever, different target_qids — must NOT fire."""
    monkeypatch.setenv(
        "GSO_PROPOSAL_STAGE_FORBIDDEN_AG_OBSERVED", "1",
    )
    candidate = _CollisionKeyPair(
        root_cause_key=None,
        signature_keys=(),
        terminal_signature_keys=(
            (
                frozenset({"7now_delivery_analytics_space_gs_099"}),
                frozenset({6}),
            ),
        ),
    )
    forbidden = _ForbiddenSetPair(
        by_root_cause=frozenset(),
        by_signature=frozenset(),
        by_terminal_signature=frozenset({
            (
                frozenset({"7now_delivery_analytics_space_gs_013"}),
                frozenset({6}),
            ),
        }),
    )
    iter_inputs: dict = {}
    result = _check_proposal_stage_forbidden_ag_leakage(
        run_id="test-run",
        iteration=2,
        ag_id="AG_DECOMPOSED_H099",
        cluster_id="H099",
        root_cause="wrong_filter_condition",
        collision_pair=candidate,
        forbidden_pair=forbidden,
        cluster_signature="",
        lever_set=(6,),
        call_site="force_lever6",
        iter_inputs=iter_inputs,
    )
    assert result is None, (
        f"Unrelated AG must not be observed; got {result!r}"
    )
