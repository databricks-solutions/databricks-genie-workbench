"""Plan 9 Task 9 — forbidden-set filtering runs before
generate_proposals_from_strategy, not after.

Verifies that AGs whose source_cluster_signatures match the
forbidden_pair are skipped before any LLM call fires. Saves
iterations on retired signatures.
"""
from genie_space_optimizer.optimization.harness import (
    filter_ags_by_forbidden_set_pre_generation,
)


def _make_ag(ag_id, signatures):
    return {
        "id": ag_id,
        "source_cluster_signatures": list(signatures),
        "target_qids": [f"q_{ag_id}"],
    }


def _make_forbidden_pair(*signatures, root_causes=()):
    from types import SimpleNamespace
    return SimpleNamespace(
        by_signature=frozenset(signatures),
        by_root_cause=frozenset(root_causes),
        by_terminal_signature=frozenset(),
    )


def test_filter_drops_ag_with_forbidden_signature():
    ags = [
        _make_ag("AG_001", ["sig_alpha"]),
        _make_ag("AG_002", ["sig_beta"]),
    ]
    forbidden = _make_forbidden_pair("sig_alpha")
    surviving, dropped = filter_ags_by_forbidden_set_pre_generation(
        ags, forbidden,
    )
    assert [ag["id"] for ag in surviving] == ["AG_002"]
    assert [ag["id"] for ag in dropped] == ["AG_001"]


def test_filter_keeps_all_when_no_forbidden_signatures():
    ags = [
        _make_ag("AG_001", ["sig_alpha"]),
        _make_ag("AG_002", ["sig_beta"]),
    ]
    forbidden = _make_forbidden_pair()
    surviving, dropped = filter_ags_by_forbidden_set_pre_generation(
        ags, forbidden,
    )
    assert len(surviving) == 2
    assert len(dropped) == 0


def test_filter_handles_ags_with_no_signatures():
    ags = [_make_ag("AG_001", [])]
    forbidden = _make_forbidden_pair("sig_alpha")
    surviving, dropped = filter_ags_by_forbidden_set_pre_generation(
        ags, forbidden,
    )
    assert [ag["id"] for ag in surviving] == ["AG_001"]
