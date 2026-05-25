"""Shrinker unit tests — v1.7 chunk 4.

The shrinker is exercised with synthetic predicates so the tests stay
fast (no workbench cycles) and the contract is asserted directly.
"""
from __future__ import annotations

from local_lever_workbench.fuzzer.shrinker import shrink_bundle
from local_lever_workbench.input_bundle import from_production_replay


def test_shrinker_returns_unchanged_when_predicate_does_not_fire() -> None:
    """If the caller's predicate is wrong, the shrinker returns the input."""
    bundle = from_production_replay(qids=("gs_009",))
    result = shrink_bundle(
        bundle, triggers_violation=lambda _b: False,
    )
    assert result.rounds == 0
    assert result.drops == ()
    assert result.minimal.to_dict() == bundle.to_dict()


def test_shrinker_drops_hard_cases_until_minimal() -> None:
    """Predicate fires only if a specific QID is present; shrinker
    drops every other QID and the post-apply tape, leaving exactly
    the triggering QID.
    """
    base = from_production_replay()
    target_qid = "domain_a_gs_009"
    assert target_qid in {c.qid for c in base.hard_cases}, (
        "production-replay corpus missing the gs_009 fixture"
    )

    # Predicate: triggers iff the target QID is still in the bundle.
    def predicate(bundle):
        return any(c.qid == target_qid for c in bundle.hard_cases)

    result = shrink_bundle(base, triggers_violation=predicate)
    assert any("drop_hard_case" in d for d in result.drops), (
        f"shrinker should have dropped non-triggering cases; "
        f"drops={result.drops!r}"
    )
    assert {c.qid for c in result.minimal.hard_cases} == {target_qid}


def test_shrinker_drops_tape_entries() -> None:
    """Predicate fires only if a specific tape entry's eval_row_id is
    present; shrinker drops every other tape entry.
    """
    import dataclasses

    base = from_production_replay(qids=("gs_009",))
    target_row_id = "target-row-must-stay"
    bundle = dataclasses.replace(
        base,
        post_apply_eval_tape=(
            {"question_id": "q1", "eval_row_id": "garbage-1"},
            {"question_id": "q2", "eval_row_id": target_row_id},
            {"question_id": "q3", "eval_row_id": "garbage-2"},
        ),
    )

    def predicate(b):
        return any(
            (e.get("eval_row_id") if isinstance(e, dict) else None)
            == target_row_id
            for e in b.post_apply_eval_tape
        )

    result = shrink_bundle(bundle, triggers_violation=predicate)
    assert len(result.minimal.post_apply_eval_tape) == 1
    only_entry = dict(result.minimal.post_apply_eval_tape[0])
    assert only_entry.get("eval_row_id") == target_row_id


def test_shrinker_clears_blame_set_when_predicate_tolerates() -> None:
    """If the predicate fires regardless of blame_set contents, the
    shrinker clears blame_set entries to minimise noise in repro.
    """
    base = from_production_replay(qids=("gs_009",))
    target_qid = "domain_a_gs_009"

    def predicate(b):
        return any(c.qid == target_qid for c in b.hard_cases)

    result = shrink_bundle(base, triggers_violation=predicate)
    [case] = result.minimal.hard_cases
    if isinstance(case.typed_evidence, dict):
        assert case.typed_evidence.get("blame_set") in ([], None), (
            f"shrinker did not clear blame_set; got "
            f"{case.typed_evidence.get('blame_set')!r}"
        )
    assert any("clear_blame_set" in d for d in result.drops)


def test_shrinker_is_deterministic() -> None:
    """Same input + same predicate ⇒ same minimal result. Pins the
    contract the CLI replay path depends on.
    """
    base = from_production_replay()
    target_qid = "domain_a_gs_009"

    def predicate(b):
        return any(c.qid == target_qid for c in b.hard_cases)

    a = shrink_bundle(base, triggers_violation=predicate)
    b = shrink_bundle(base, triggers_violation=predicate)
    assert a.minimal.to_dict() == b.minimal.to_dict()
    assert a.drops == b.drops
