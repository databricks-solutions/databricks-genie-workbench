"""Greedy drop-one shrinker for failing workbench bundles — v1.7 chunk 4.

When the fuzzer finds an input that violates an invariant, the
shrinker reduces it to the smallest input that still triggers the
violation, so the operator can paste a single tiny example into a
bug report instead of a 7-case 5-permutation chain.

The strategy is greedy drop-one:

1. Try dropping each hard case one at a time. If the violation still
   fires, keep the smaller bundle.
2. Try dropping each post-apply-tape entry one at a time.
3. Try simplifying ``typed_evidence`` per case (clear blame_set, clear
   expected_card_violations).

Repeat the cycle until one full pass produces no further shrink. The
result is reported as a :class:`ShrinkResult` with the minimal bundle
plus a summary of what was removed.

The shrinker is deterministic: same input + same trigger predicate ⇒
same minimal bundle.
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Callable

from local_lever_workbench.models import (
    WorkbenchHardCase,
    WorkbenchInputBundle,
)


TriggerPredicate = Callable[[WorkbenchInputBundle], bool]


@dataclass(frozen=True)
class ShrinkResult:
    """Outcome of a shrinking run."""

    minimal: WorkbenchInputBundle
    rounds: int
    drops: tuple[str, ...] = field(default_factory=tuple)

    def summary(self) -> str:
        n_qids = len(self.minimal.hard_cases)
        n_tape = len(self.minimal.post_apply_eval_tape)
        return (
            f"shrunk in {self.rounds} round(s) ({len(self.drops)} drops); "
            f"minimal bundle: hard_cases={n_qids}, "
            f"post_apply_eval_tape={n_tape}"
        )


def _drop_hard_case(
    bundle: WorkbenchInputBundle, qid: str,
) -> WorkbenchInputBundle:
    return dataclasses.replace(
        bundle,
        hard_cases=tuple(c for c in bundle.hard_cases if c.qid != qid),
    )


def _drop_tape_entry(
    bundle: WorkbenchInputBundle, index: int,
) -> WorkbenchInputBundle:
    return dataclasses.replace(
        bundle,
        post_apply_eval_tape=tuple(
            e for i, e in enumerate(bundle.post_apply_eval_tape)
            if i != index
        ),
    )


def _clear_blame_set(
    bundle: WorkbenchInputBundle, qid: str,
) -> WorkbenchInputBundle:
    new_cases: list[WorkbenchHardCase] = []
    for case in bundle.hard_cases:
        if case.qid == qid and isinstance(case.typed_evidence, dict):
            new_te = dict(case.typed_evidence)
            new_te["blame_set"] = []
            new_cases.append(
                dataclasses.replace(case, typed_evidence=new_te)
            )
        else:
            new_cases.append(case)
    return dataclasses.replace(bundle, hard_cases=tuple(new_cases))


def shrink_bundle(
    bundle: WorkbenchInputBundle,
    *,
    triggers_violation: TriggerPredicate,
    max_rounds: int = 50,
) -> ShrinkResult:
    """Greedy drop-one shrinker. Returns the minimal triggering bundle.

    ``triggers_violation`` should return ``True`` if the supplied
    bundle still triggers the invariant violation under investigation.
    The shrinker assumes a single pass through ``triggers_violation``
    is cheap (sub-second) — the workbench in sm-tape mode meets this.

    The shrinker preserves at least one ``hard_case`` (a zero-case
    bundle is not a meaningful triggering input).
    """
    if not triggers_violation(bundle):
        # The caller passed an input that does not actually trigger the
        # violation. Return it unchanged so the caller can spot the
        # bug in their predicate.
        return ShrinkResult(minimal=bundle, rounds=0, drops=())

    current = bundle
    drops: list[str] = []

    for round_idx in range(1, max_rounds + 1):
        shrunk_this_round = False

        # Strategy 1: drop one hard case at a time.
        for case in list(current.hard_cases):
            if len(current.hard_cases) <= 1:
                break
            candidate = _drop_hard_case(current, case.qid)
            if triggers_violation(candidate):
                current = candidate
                drops.append(f"drop_hard_case:{case.qid}")
                shrunk_this_round = True
                break  # restart the loop so indices stay valid

        if shrunk_this_round:
            continue

        # Strategy 2: drop one tape entry at a time.
        for idx, _entry in enumerate(list(current.post_apply_eval_tape)):
            candidate = _drop_tape_entry(current, idx)
            if triggers_violation(candidate):
                current = candidate
                drops.append(f"drop_tape_entry:{idx}")
                shrunk_this_round = True
                break

        if shrunk_this_round:
            continue

        # Strategy 3: simplify typed_evidence (clear blame_set per case).
        for case in list(current.hard_cases):
            if not isinstance(case.typed_evidence, dict):
                continue
            blame = case.typed_evidence.get("blame_set") or []
            if not blame:
                continue
            candidate = _clear_blame_set(current, case.qid)
            if triggers_violation(candidate):
                current = candidate
                drops.append(f"clear_blame_set:{case.qid}")
                shrunk_this_round = True
                break

        if not shrunk_this_round:
            return ShrinkResult(
                minimal=current,
                rounds=round_idx,
                drops=tuple(drops),
            )

    return ShrinkResult(
        minimal=current,
        rounds=max_rounds,
        drops=tuple(drops),
    )


__all__ = ["ShrinkResult", "shrink_bundle"]
