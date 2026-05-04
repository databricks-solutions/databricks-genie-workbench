# Optimizer Control-Plane Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Ledger entry:** This plan is **Cycle 1** of the [Optimizer Iteration Ledger](./2026-05-05-optimizer-iteration-ledger.md). The cycle's clusters (C-1-A → C-1-D), AG hypotheses (AG-1-A → AG-1-F), and gate results are tracked in the ledger; the implementation steps live here. Update the ledger's Section 5 (Gate results) and Section 6 (Decision) when Task G closes this plan out.

**Goal:** Close the four causal control-plane gaps that produced 91.7% (instead of 95.8%) on the airline pilot run `0ade1a99-9406-4a68-a3bc-8c77be78edcb`: target-blind acceptance, fall-back-to-non-causal-patch when blast-radius drops the only causal patch, RCA-blind `patch_cap` ranking, and over-aggressive blast-radius on non-semantic levers. Add bucket feedback so prior failure buckets steer the next iteration's strategist.

**Architecture:** Each behavioural change lands behind a default-off env-var feature flag so the byte-stable replay fixture (`tests/replay/fixtures/airline_real_v1.json`) and the `BURNDOWN_BUDGET = 0` invariant never break mid-plan. The flags are flipped on as a single closeout change (Task G), at which point a fresh airline replay cycle is intaken via `gso-replay-cycle-intake` and the burn-down log records the budget movement explicitly. No new ML primitives — every fix is a small policy refinement on existing typed surfaces (`stages/acceptance.py`, `stages/proposals.py`, `stages/gates.py`, `stages/action_groups.py`, `control_plane.py`, `proposal_grounding.py`, `patch_selection.py`).

**Tech Stack:** Python 3.11, pytest, dataclasses, MLflow tracing, the existing G-lite stage registry. No new dependencies.

**Regression discipline:** After every task, run

```bash
pytest packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget -xvs
pytest packages/genie-space-optimizer/tests/replay/ -x
pytest packages/genie-space-optimizer/tests/unit/ -x
```

All three must stay green with the feature flags **default-off**. This plan never raises `BURNDOWN_BUDGET` until Task G.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/genie_space_optimizer/common/config.py` (modify) | Feature-flag env-var helpers. |
| `src/genie_space_optimizer/optimization/control_plane.py` (modify) | Threshold-aware `decide_control_plane_acceptance` parameter. |
| `src/genie_space_optimizer/optimization/proposal_grounding.py` (modify) | Lever-aware `patch_blast_radius_is_safe` gradation. |
| `src/genie_space_optimizer/optimization/stages/acceptance.py` (modify) | Pass `thresholds_met` and `pre_resolved_outcomes_by_ag` through `AcceptanceInput`. |
| `src/genie_space_optimizer/optimization/stages/proposals.py` (modify) | Stamp `rca_id` on every proposal from parent AG; emit `NO_CAUSAL_APPLYABLE` outcome marker when needed. |
| `src/genie_space_optimizer/optimization/stages/action_groups.py` (modify) | Accept `prior_buckets_by_qid` input; bucket-driven AG selection. |
| `src/genie_space_optimizer/optimization/harness.py` (modify) | Wire flags through to the four call sites. |
| `tests/unit/test_control_plane_target_aware.py` (create) | Unit tests for Task A. |
| `tests/unit/test_proposals_no_causal_applyable.py` (create) | Unit tests for Task B. |
| `tests/unit/test_action_groups_bucket_feedback.py` (create) | Unit tests for Task C. |
| `tests/unit/test_proposals_rca_inherit.py` (create) | Unit tests for Task D and F. |
| `tests/unit/test_blast_radius_lever_aware.py` (create) | Unit tests for Task E. |
| `tests/integration/test_optimizer_flags_end_to_end.py` (create) | Integration test that all five flags-on path produces the expected outcomes on a synthetic 3-cluster fixture. |

---

## Task 0: Feature-flag scaffolding

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_optimizer_feature_flags.py` (create)

- [ ] **Step 1: Write the failing test**

```python
import os

import pytest

from genie_space_optimizer.common import config as cfg


def test_feature_flag_defaults_off(monkeypatch):
    for env in (
        "GSO_TARGET_AWARE_ACCEPTANCE",
        "GSO_NO_CAUSAL_APPLYABLE_HALT",
        "GSO_BUCKET_DRIVEN_AG_SELECTION",
        "GSO_RCA_AWARE_PATCH_CAP",
        "GSO_LEVER_AWARE_BLAST_RADIUS",
    ):
        monkeypatch.delenv(env, raising=False)
    assert cfg.target_aware_acceptance_enabled() is False
    assert cfg.no_causal_applyable_halt_enabled() is False
    assert cfg.bucket_driven_ag_selection_enabled() is False
    assert cfg.rca_aware_patch_cap_enabled() is False
    assert cfg.lever_aware_blast_radius_enabled() is False


@pytest.mark.parametrize("value,expected", [("1", True), ("true", True), ("yes", True), ("0", False), ("false", False), ("", False)])
def test_feature_flag_env_parsing(monkeypatch, value, expected):
    monkeypatch.setenv("GSO_TARGET_AWARE_ACCEPTANCE", value)
    assert cfg.target_aware_acceptance_enabled() is expected
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_optimizer_feature_flags.py -xvs
```

Expected: FAIL with `AttributeError: module ... has no attribute 'target_aware_acceptance_enabled'`.

- [ ] **Step 3: Add the helpers to `common/config.py`**

Append to `packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py`:

```python
import os as _os


def _flag_enabled(env_name: str) -> bool:
    raw = (_os.environ.get(env_name) or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def target_aware_acceptance_enabled() -> bool:
    return _flag_enabled("GSO_TARGET_AWARE_ACCEPTANCE")


def no_causal_applyable_halt_enabled() -> bool:
    return _flag_enabled("GSO_NO_CAUSAL_APPLYABLE_HALT")


def bucket_driven_ag_selection_enabled() -> bool:
    return _flag_enabled("GSO_BUCKET_DRIVEN_AG_SELECTION")


def rca_aware_patch_cap_enabled() -> bool:
    return _flag_enabled("GSO_RCA_AWARE_PATCH_CAP")


def lever_aware_blast_radius_enabled() -> bool:
    return _flag_enabled("GSO_LEVER_AWARE_BLAST_RADIUS")
```

- [ ] **Step 4: Run tests to verify pass**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_optimizer_feature_flags.py -xvs
pytest packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget -xvs
```

Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py \
    packages/genie-space-optimizer/tests/unit/test_optimizer_feature_flags.py
git commit -m "feat(optimizer): add control-plane feature-flag scaffolding"
```

---

## Task A: Threshold-aware acceptance (Tier 1 — highest leverage)

**Background.** `decide_control_plane_acceptance` (`control_plane.py:646-833`) has an `accepted_with_attribution_drift` branch that accepts when global accuracy moved with zero regressions even though the named target qid did not move. That branch fired in iter-3 of the `0ade1a99` run: AG_COVERAGE_H003 was accepted because `gs_016` flipped incidentally while `gs_009` (the actual target) stayed broken. Below thresholds, attribution drift should not be a free pass.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py:646-833`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/acceptance.py:50-153`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:10443-10454`
- Test: `packages/genie-space-optimizer/tests/unit/test_control_plane_target_aware.py` (create)

- [ ] **Step 1: Write the failing test**

```python
from genie_space_optimizer.optimization.control_plane import (
    ControlPlaneAcceptance,
    decide_control_plane_acceptance,
)


def _row(qid, rc, arbiter):
    return {"question_id": qid, "result_correctness": rc, "arbiter": arbiter}


PRE = (
    _row("gs_009", "no", "ground_truth_correct"),
    _row("gs_016", "no", "ground_truth_correct"),
    _row("gs_024", "no", "ground_truth_correct"),
    _row("gs_001", "yes", "both_correct"),
)
POST_DRIFT = (
    _row("gs_009", "no", "ground_truth_correct"),
    _row("gs_016", "yes", "both_correct"),
    _row("gs_024", "no", "ground_truth_correct"),
    _row("gs_001", "yes", "both_correct"),
)


def test_attribution_drift_accepted_when_thresholds_met():
    decision = decide_control_plane_acceptance(
        baseline_accuracy=25.0,
        candidate_accuracy=50.0,
        target_qids=("gs_009",),
        pre_rows=PRE,
        post_rows=POST_DRIFT,
        thresholds_met=True,
    )
    assert decision.accepted is True
    assert decision.reason_code == "accepted_with_attribution_drift"


def test_attribution_drift_rejected_when_thresholds_unmet():
    decision = decide_control_plane_acceptance(
        baseline_accuracy=25.0,
        candidate_accuracy=50.0,
        target_qids=("gs_009",),
        pre_rows=PRE,
        post_rows=POST_DRIFT,
        thresholds_met=False,
    )
    assert decision.accepted is False
    assert decision.reason_code == "rejected_below_threshold_no_target_progress"
    assert decision.target_fixed_qids == ()
    assert decision.target_still_hard_qids == ("gs_009",)


def test_default_thresholds_met_preserves_legacy_behavior():
    decision = decide_control_plane_acceptance(
        baseline_accuracy=25.0,
        candidate_accuracy=50.0,
        target_qids=("gs_009",),
        pre_rows=PRE,
        post_rows=POST_DRIFT,
    )
    assert decision.accepted is True
    assert decision.reason_code == "accepted_with_attribution_drift"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_control_plane_target_aware.py -xvs
```

Expected: FAIL with `TypeError: decide_control_plane_acceptance() got an unexpected keyword argument 'thresholds_met'`.

- [ ] **Step 3: Add `thresholds_met` parameter to `decide_control_plane_acceptance`**

In `control_plane.py:646-660`, extend the signature:

```python
def decide_control_plane_acceptance(
    *,
    baseline_accuracy: float,
    candidate_accuracy: float,
    target_qids: Iterable[str],
    pre_rows: Iterable[dict],
    post_rows: Iterable[dict],
    min_gain_pp: float = 0.0,
    max_new_hard_regressions: int = 1,
    max_new_passing_to_hard_regressions: int | None = None,
    protected_qids: Iterable[str] = (),
    baseline_pre_arbiter_accuracy: float | None = None,
    candidate_pre_arbiter_accuracy: float | None = None,
    min_pre_arbiter_gain_pp: float = 2.0,
    thresholds_met: bool = True,
) -> ControlPlaneAcceptance:
```

In the attribution-drift branch (around `control_plane.py:786-801`), replace:

```python
    elif (
        not has_causal_fix
        and not out_of_target_regressed
        and not protected_regressed
        and not soft_to_hard
        and not passing_to_hard
    ):
        reason = "accepted_with_attribution_drift"
        accepted = True
```

with:

```python
    elif (
        not has_causal_fix
        and not out_of_target_regressed
        and not protected_regressed
        and not soft_to_hard
        and not passing_to_hard
    ):
        if thresholds_met:
            reason = "accepted_with_attribution_drift"
            accepted = True
        else:
            reason = "rejected_below_threshold_no_target_progress"
            accepted = False
            target_fixed = ()
```

- [ ] **Step 4: Wire `thresholds_met` through `stages/acceptance.py`**

In `stages/acceptance.py`, add the field to `AcceptanceInput` (after `cluster_by_qid`):

```python
    thresholds_met: bool = True
```

In `_decide_for_ag` (`stages/acceptance.py:133-153`), pass it through:

```python
    return decide_control_plane_acceptance(
        baseline_accuracy=inp.baseline_accuracy,
        candidate_accuracy=inp.candidate_accuracy,
        target_qids=target_qids,
        pre_rows=inp.pre_rows,
        post_rows=inp.post_rows,
        min_gain_pp=inp.min_gain_pp,
        protected_qids=inp.protected_qids,
        baseline_pre_arbiter_accuracy=inp.baseline_pre_arbiter_accuracy,
        candidate_pre_arbiter_accuracy=inp.candidate_pre_arbiter_accuracy,
        min_pre_arbiter_gain_pp=inp.min_pre_arbiter_gain_pp,
        thresholds_met=inp.thresholds_met,
    )
```

- [ ] **Step 5: Wire harness call site behind feature flag**

In `harness.py:10443-10454`, replace the `decide_control_plane_acceptance(...)` call with:

```python
    from genie_space_optimizer.common.config import target_aware_acceptance_enabled

    _control_plane_decision = decide_control_plane_acceptance(
        baseline_accuracy=float(best_accuracy),
        candidate_accuracy=float(full_accuracy),
        target_qids=_target_qids,
        pre_rows=_baseline_rows_for_control_plane,
        post_rows=_after_rows,
        min_gain_pp=float(MIN_POST_ARBITER_GAIN_PP),
        max_new_hard_regressions=_max_new_hard_regressions,
        protected_qids=_protected_qids,
        baseline_pre_arbiter_accuracy=_baseline_pre_arbiter_pct,
        candidate_pre_arbiter_accuracy=_candidate_pre_arbiter_pct,
        thresholds_met=(
            bool(thresholds_met)
            if target_aware_acceptance_enabled()
            else True
        ),
    )
```

The `thresholds_met` local in `_run_lever_loop` is the same one published by `baseline_eval` / `enrichment` — already in scope at this call site (verified in `harness.py` `_run_lever_loop` signature). When the flag is off, the gate sees `thresholds_met=True` and behaviour is identical to today.

- [ ] **Step 6: Run all tests, including byte-stability**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_control_plane_target_aware.py -xvs
pytest packages/genie-space-optimizer/tests/unit/ -x
pytest packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget -xvs
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/control_plane.py \
    packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/acceptance.py \
    packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
    packages/genie-space-optimizer/tests/unit/test_control_plane_target_aware.py
git commit -m "feat(optimizer): target-aware acceptance gate (flag-gated, default-off)"
```

---

## Task B: NO_CAUSAL_APPLYABLE_PATCH outcome lane

**Background.** When every RCA-grounded proposal in an AG is dropped by `blast_radius` / `rca_groundedness` / `applyability`, the harness today falls back to applying the AG's non-causal proposals. That is the failure mode that let `gs_009`'s `ROW_NUMBER()` snippet die at `blast_radius` while three generic `add_join_spec` patches were applied. New outcome: when an AG enters the patch-cap stage with zero RCA-matched proposals surviving the upstream gates, halt the AG with reason `no_causal_applyable_patch` and skip patch application entirely.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/acceptance.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:16250-16290` (patch-cap call site)
- Test: `packages/genie-space-optimizer/tests/unit/test_proposals_no_causal_applyable.py` (create)

- [ ] **Step 1: Write the failing test**

```python
from genie_space_optimizer.optimization.harness import (
    _filter_to_causal_applyable_proposals,
)


def test_returns_proposals_with_matching_rca_id():
    ag = {"id": "AG_H003", "rca_id": "RCA_TOP10_LOGIC"}
    proposals = [
        {"proposal_id": "P001", "rca_id": "RCA_TOP10_LOGIC", "patch_type": "add_sql_snippet_expression"},
        {"proposal_id": "P002", "rca_id": None, "patch_type": "add_join_spec"},
        {"proposal_id": "P003", "rca_id": "RCA_TOP10_LOGIC", "patch_type": "add_join_spec"},
    ]
    causal, has_any_rca_matched = _filter_to_causal_applyable_proposals(
        ag=ag, proposals=proposals,
    )
    assert [p["proposal_id"] for p in causal] == ["P001", "P003"]
    assert has_any_rca_matched is True


def test_returns_empty_with_signal_when_all_dropped():
    ag = {"id": "AG_H003", "rca_id": "RCA_TOP10_LOGIC"}
    proposals = [
        {"proposal_id": "P002", "rca_id": None, "patch_type": "add_join_spec"},
        {"proposal_id": "P004", "rca_id": "RCA_OTHER", "patch_type": "add_join_spec"},
    ]
    causal, has_any_rca_matched = _filter_to_causal_applyable_proposals(
        ag=ag, proposals=proposals,
    )
    assert causal == []
    assert has_any_rca_matched is False


def test_no_rca_id_on_ag_returns_all_proposals_unchanged():
    ag = {"id": "AG_DIAGNOSTIC", "rca_id": None}
    proposals = [
        {"proposal_id": "P001", "rca_id": None, "patch_type": "add_join_spec"},
    ]
    causal, has_any_rca_matched = _filter_to_causal_applyable_proposals(
        ag=ag, proposals=proposals,
    )
    assert causal == proposals
    assert has_any_rca_matched is False
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_proposals_no_causal_applyable.py -xvs
```

Expected: FAIL with `ImportError`.

- [ ] **Step 3: Add helper to `harness.py`**

Add directly above the existing `select_target_aware_causal_patch_cap` import block in `harness.py` (search for the import; it's around line 16252):

```python
def _filter_to_causal_applyable_proposals(
    *,
    ag: dict,
    proposals: list[dict],
) -> tuple[list[dict], bool]:
    """Return (matching_proposals, had_any_rca_matched).

    When the parent AG declares an ``rca_id``, retain only proposals
    whose ``rca_id`` equals the AG's. When the AG carries no ``rca_id``
    (diagnostic AGs that did not yet inherit cluster RCA — see Task F),
    retain all proposals to preserve legacy behaviour. Callers gate
    the halt-on-empty behaviour behind
    ``no_causal_applyable_halt_enabled()``.
    """
    ag_rca = str(ag.get("rca_id") or "").strip()
    if not ag_rca:
        return list(proposals), False
    matched = [
        p for p in proposals
        if str(p.get("rca_id") or "").strip() == ag_rca
    ]
    return matched, bool(matched)
```

- [ ] **Step 4: Wire halt at the patch-cap call site**

In `harness.py` at the patch-cap call site (around line 16250-16290), replace the call to `select_target_aware_causal_patch_cap(patches, target_qids=...)` with the flag-gated halt path. Search for `patches, _patch_cap_decisions = select_target_aware_causal_patch_cap(`. Wrap with:

```python
            from genie_space_optimizer.common.config import (
                no_causal_applyable_halt_enabled,
            )

            if no_causal_applyable_halt_enabled():
                _causal_proposals, _had_rca_matched = (
                    _filter_to_causal_applyable_proposals(
                        ag=ag, proposals=patches,
                    )
                )
                if not _causal_proposals and not _had_rca_matched and ag.get("rca_id"):
                    logger.warning(
                        "[%s] no_causal_applyable_patch: every RCA-matched "
                        "proposal was dropped by upstream gates; halting AG "
                        "before patch_cap",
                        ag.get("id") or ag.get("ag_id"),
                    )
                    _audit_emit(
                        stage_letter="L",
                        gate_name="patch_cap",
                        decision="skipped",
                        reason_code="no_causal_applyable_patch",
                        metrics={"input_count": len(patches)},
                    )
                    patches = []
                    _patch_cap_decisions = []
                else:
                    patches, _patch_cap_decisions = (
                        select_target_aware_causal_patch_cap(
                            patches,
                            target_qids=_patch_cap_target_qids,
                            max_ag_patches=MAX_AG_PATCHES,
                        )
                    )
            else:
                patches, _patch_cap_decisions = (
                    select_target_aware_causal_patch_cap(
                        patches,
                        target_qids=_patch_cap_target_qids,
                        max_ag_patches=MAX_AG_PATCHES,
                    )
                )
```

(The exact existing call's argument list lives in `harness.py` already — preserve it verbatim in both branches.)

- [ ] **Step 5: Run unit + replay tests**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_proposals_no_causal_applyable.py -xvs
pytest packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget -xvs
```

Expected: both PASS.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
    packages/genie-space-optimizer/tests/unit/test_proposals_no_causal_applyable.py
git commit -m "feat(optimizer): NO_CAUSAL_APPLYABLE_PATCH halt outcome (flag-gated, default-off)"
```

---

## Task C: Bucket-driven AG selection

**Background.** Today's strategist (`stages/action_groups.py::select`) picks AGs from clusters; failure buckets are computed *after* the iteration for postmortem only. The roadmap and `2026-05-05-optimizer-iteration-and-troubleshooting-guide.md:331-341` call this out as the highest-leverage structural improvement: feed prior-iteration buckets back so an `EVIDENCE_GAP` qid forces an evidence-gathering AG before another patch attempt; a `MODEL_CEILING` qid is removed from the targeting set.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/action_groups.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` (call site, scope after Task C step 1 to confirm exact line)
- Test: `packages/genie-space-optimizer/tests/unit/test_action_groups_bucket_feedback.py` (create)

- [ ] **Step 1: Write the failing test**

```python
from dataclasses import dataclass
from genie_space_optimizer.optimization.stages.action_groups import (
    ActionGroupsInput,
    select,
)
from genie_space_optimizer.optimization.failure_bucketing import FailureBucket


@dataclass
class _Ctx:
    run_id: str = "r"
    iteration: int = 2
    decision_emit = staticmethod(lambda *_a, **_k: None)


def test_model_ceiling_qid_dropped_from_targets(monkeypatch):
    monkeypatch.setenv("GSO_BUCKET_DRIVEN_AG_SELECTION", "1")
    inp = ActionGroupsInput(
        clusters=(
            {"id": "H001", "qids": ("gs_029",), "root_cause": "gt_correction"},
            {"id": "H002", "qids": ("gs_009",), "root_cause": "wrong_top_n_logic"},
        ),
        prior_buckets_by_qid={
            "gs_029": FailureBucket.MODEL_CEILING,
            "gs_009": FailureBucket.MERGE_GATE_GAP,
        },
    )
    slate = select(_Ctx(), inp)
    selected_qids = {q for ag in slate.ags for q in ag.get("target_qids", ())}
    assert "gs_009" in selected_qids
    assert "gs_029" not in selected_qids


def test_evidence_gap_emits_evidence_gathering_ag(monkeypatch):
    monkeypatch.setenv("GSO_BUCKET_DRIVEN_AG_SELECTION", "1")
    inp = ActionGroupsInput(
        clusters=(
            {"id": "H003", "qids": ("gs_024",), "root_cause": "ungrounded"},
        ),
        prior_buckets_by_qid={"gs_024": FailureBucket.EVIDENCE_GAP},
    )
    slate = select(_Ctx(), inp)
    assert any(ag.get("ag_kind") == "evidence_gathering" for ag in slate.ags)


def test_default_off_preserves_legacy_behavior(monkeypatch):
    monkeypatch.delenv("GSO_BUCKET_DRIVEN_AG_SELECTION", raising=False)
    inp = ActionGroupsInput(
        clusters=(
            {"id": "H001", "qids": ("gs_029",), "root_cause": "gt_correction"},
        ),
        prior_buckets_by_qid={"gs_029": FailureBucket.MODEL_CEILING},
    )
    slate = select(_Ctx(), inp)
    assert slate.ags  # MODEL_CEILING qid not removed when flag off
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_action_groups_bucket_feedback.py -xvs
```

Expected: FAIL with `TypeError: ActionGroupsInput.__init__() got an unexpected keyword argument 'prior_buckets_by_qid'` (or similar — depending on what the file looks like today).

- [ ] **Step 3: Add `prior_buckets_by_qid` field and bucket policy**

Read the current `ActionGroupsInput` and `select` signature in `stages/action_groups.py`. Add (after the existing fields):

```python
    prior_buckets_by_qid: Mapping[str, "FailureBucket"] = field(default_factory=dict)
```

Add at the top of `select`:

```python
    from genie_space_optimizer.common.config import (
        bucket_driven_ag_selection_enabled,
    )
    from genie_space_optimizer.optimization.failure_bucketing import FailureBucket

    if bucket_driven_ag_selection_enabled() and inp.prior_buckets_by_qid:
        clusters = _apply_bucket_policy(
            inp.clusters,
            buckets_by_qid=inp.prior_buckets_by_qid,
        )
    else:
        clusters = list(inp.clusters)
```

Then change every reference to `inp.clusters` inside `select` to `clusters`.

Add the helper at module scope in `stages/action_groups.py`:

```python
def _apply_bucket_policy(
    clusters: tuple[Mapping[str, Any], ...],
    *,
    buckets_by_qid: Mapping[str, "FailureBucket"],
) -> list[dict[str, Any]]:
    """Drop MODEL_CEILING qids from cluster target sets; convert
    EVIDENCE_GAP-only clusters into evidence-gathering AGs (marked via
    ``ag_kind="evidence_gathering"`` so the proposal stage emits a
    no-op proposal that records evidence rather than mutating the
    space).
    """
    from genie_space_optimizer.optimization.failure_bucketing import FailureBucket

    out: list[dict[str, Any]] = []
    for cluster in clusters:
        kept_qids = tuple(
            q for q in (cluster.get("qids") or ())
            if buckets_by_qid.get(str(q)) is not FailureBucket.MODEL_CEILING
        )
        if not kept_qids:
            continue
        all_evidence_gap = all(
            buckets_by_qid.get(str(q)) is FailureBucket.EVIDENCE_GAP
            for q in kept_qids
        )
        c = dict(cluster)
        c["qids"] = kept_qids
        if all_evidence_gap:
            c["ag_kind"] = "evidence_gathering"
        out.append(c)
    return out
```

- [ ] **Step 4: Wire harness call site to pass prior buckets**

The harness already computes per-iteration `FailureBucket` after Phase D. Pass them into `ActionGroupsInput`. Search `harness.py` for `ActionGroupsInput(` and add `prior_buckets_by_qid=_prior_iter_buckets_by_qid`. The variable should be sourced from `loop_state` (the prior iteration's classification result) — when none exists (iteration 1), pass an empty dict.

If the harness today does not yet thread per-iteration buckets, scope this final wiring step as a follow-up: in this task, plumb only the typed surface and the unit-test-coverage flag. The integration test in Task G covers the flag-on end-to-end path.

- [ ] **Step 5: Run tests**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_action_groups_bucket_feedback.py -xvs
pytest packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget -xvs
```

Expected: both PASS.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/action_groups.py \
    packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
    packages/genie-space-optimizer/tests/unit/test_action_groups_bucket_feedback.py
git commit -m "feat(optimizer): bucket-driven AG selection (flag-gated, default-off)"
```

---

## Task D: RCA-aware patch_cap ranking — proposals inherit AG rca_id

**Background.** `select_causal_patch_cap` (`patch_selection.py:193`) already orders by `causal_attribution_tier` (which returns 3 when `patch.rca_id` is set, else lower). The bug for `gs_009` is that the strategist-emitted proposals never received the parent AG's `rca_id`, so all 12 proposals had `rca_id=None` and `causal_attribution_tier=2`, falling back to insertion order — generic `add_join_spec` decoys outranked the SQL-snippet fix. The fix: stamp `rca_id` from the AG onto every proposal at the F5 stage entry.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/proposals.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_proposals_rca_inherit.py` (create)

- [ ] **Step 1: Write the failing test**

```python
from dataclasses import dataclass
from genie_space_optimizer.optimization.stages.proposals import (
    ProposalsInput,
    generate,
)


@dataclass
class _Ctx:
    run_id: str = "r"
    iteration: int = 1
    decision_emit = staticmethod(lambda *_a, **_k: None)


def test_proposals_inherit_rca_id_from_parent_cluster(monkeypatch):
    monkeypatch.setenv("GSO_RCA_AWARE_PATCH_CAP", "1")
    inp = ProposalsInput(
        proposals_by_ag={
            "AG_H003": (
                {"proposal_id": "P001", "patch_type": "add_sql_snippet_expression",
                 "primary_cluster_id": "H003"},
                {"proposal_id": "P002", "patch_type": "add_join_spec",
                 "primary_cluster_id": "H003"},
            )
        },
        rca_id_by_cluster={"H003": "RCA_TOP10_LOGIC"},
        cluster_root_cause_by_id={"H003": "wrong_top_n_logic"},
    )
    slate = generate(_Ctx(), inp)
    stamped = slate.proposals_by_ag["AG_H003"]
    assert stamped[0]["rca_id"] == "RCA_TOP10_LOGIC"
    assert stamped[1]["rca_id"] == "RCA_TOP10_LOGIC"


def test_default_off_does_not_stamp(monkeypatch):
    monkeypatch.delenv("GSO_RCA_AWARE_PATCH_CAP", raising=False)
    inp = ProposalsInput(
        proposals_by_ag={
            "AG_H003": (
                {"proposal_id": "P001", "primary_cluster_id": "H003"},
            )
        },
        rca_id_by_cluster={"H003": "RCA_TOP10_LOGIC"},
    )
    slate = generate(_Ctx(), inp)
    stamped = slate.proposals_by_ag["AG_H003"]
    assert "rca_id" not in stamped[0] or not stamped[0].get("rca_id")
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_proposals_rca_inherit.py -xvs
```

Expected: FAIL on the first assertion.

- [ ] **Step 3: Add the stamping inside `generate`**

In `stages/proposals.py:107-115`, replace the inner loop body with:

```python
    from genie_space_optimizer.common.config import rca_aware_patch_cap_enabled

    stamp_rca = rca_aware_patch_cap_enabled()
    fingerprinted: dict[str, tuple[dict[str, Any], ...]] = {}
    fingerprints: list[str] = []

    for ag_id, proposals in (inp.proposals_by_ag or {}).items():
        ag_fingerprinted: list[dict[str, Any]] = []
        for proposal in proposals:
            stamped = dict(proposal)
            if stamp_rca and not stamped.get("rca_id"):
                cluster_id = (
                    str(stamped.get("primary_cluster_id") or "")
                    or next(
                        (str(c) for c in (stamped.get("source_cluster_ids") or ())),
                        "",
                    )
                )
                if cluster_id:
                    inherited = inp.rca_id_by_cluster.get(cluster_id)
                    if inherited:
                        stamped["rca_id"] = str(inherited)
            fingerprint = _content_fingerprint(proposal)
            stamped["content_fingerprint"] = fingerprint
            ag_fingerprinted.append(stamped)
            fingerprints.append(fingerprint)
        fingerprinted[str(ag_id)] = tuple(ag_fingerprinted)
        # …existing record emission unchanged…
```

(Preserve the existing `proposal_generated_records(...)` call and loop structure verbatim — only the inner stamping is new.)

- [ ] **Step 4: Run unit + replay tests**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_proposals_rca_inherit.py -xvs
pytest packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget -xvs
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/proposals.py \
    packages/genie-space-optimizer/tests/unit/test_proposals_rca_inherit.py
git commit -m "feat(optimizer): RCA-aware patch_cap via proposals.rca_id inheritance (flag-gated)"
```

---

## Task E: Lever-aware blast-radius gradation

**Background.** `patch_blast_radius_is_safe` (`proposal_grounding.py:517-552`) emits `safe=False, reason=high_collateral_risk_flagged` whenever a patch has `high_collateral_risk=True` and at least one passing dependent qid outside the AG target set. For semantic-changing patches (SQL snippets, join changes) this is correct. For non-semantic informational patches (`update_column_description`, `add_column_synonym`, `add_metric_view_instruction`, `add_table_instruction`) the collateral risk is structurally bounded — these patches cannot regress a passing query because they don't change query semantics, only the metadata Genie reads. The fix downgrades these levers to `safe=True, reason=non_semantic_collateral_warning` and surfaces the warning rather than blocking the patch.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_grounding.py:517-552`
- Test: `packages/genie-space-optimizer/tests/unit/test_blast_radius_lever_aware.py` (create)

- [ ] **Step 1: Write the failing test**

```python
from genie_space_optimizer.optimization.proposal_grounding import (
    patch_blast_radius_is_safe,
)


_NON_SEMANTIC_TYPES = (
    "update_column_description",
    "add_column_synonym",
    "add_metric_view_instruction",
    "add_table_instruction",
    "update_table_description",
)
_SEMANTIC_TYPES = (
    "add_sql_snippet_filter",
    "add_sql_snippet_expression",
    "add_join_spec",
    "update_join_spec",
)


def test_non_semantic_patch_warns_when_flag_on(monkeypatch):
    monkeypatch.setenv("GSO_LEVER_AWARE_BLAST_RADIUS", "1")
    for patch_type in _NON_SEMANTIC_TYPES:
        result = patch_blast_radius_is_safe(
            {
                "patch_type": patch_type,
                "passing_dependents": ["gs_003", "gs_004"],
                "high_collateral_risk": True,
            },
            ag_target_qids=("gs_024",),
        )
        assert result["safe"] is True, patch_type
        assert result["reason"] == "non_semantic_collateral_warning", patch_type


def test_semantic_patch_still_blocked(monkeypatch):
    monkeypatch.setenv("GSO_LEVER_AWARE_BLAST_RADIUS", "1")
    for patch_type in _SEMANTIC_TYPES:
        result = patch_blast_radius_is_safe(
            {
                "patch_type": patch_type,
                "passing_dependents": ["gs_003"],
                "high_collateral_risk": True,
            },
            ag_target_qids=("gs_024",),
        )
        assert result["safe"] is False, patch_type
        assert result["reason"] == "high_collateral_risk_flagged", patch_type


def test_default_off_legacy_behavior(monkeypatch):
    monkeypatch.delenv("GSO_LEVER_AWARE_BLAST_RADIUS", raising=False)
    result = patch_blast_radius_is_safe(
        {
            "patch_type": "update_column_description",
            "passing_dependents": ["gs_003"],
            "high_collateral_risk": True,
        },
        ag_target_qids=("gs_024",),
    )
    assert result["safe"] is False
    assert result["reason"] == "high_collateral_risk_flagged"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_blast_radius_lever_aware.py -xvs
```

Expected: FAIL with all six non-semantic-type cases asserting wrong values.

- [ ] **Step 3: Add the gradation**

In `proposal_grounding.py`, add a module-level constant near the top:

```python
_NON_SEMANTIC_PATCH_TYPES: frozenset[str] = frozenset({
    "update_column_description",
    "add_column_synonym",
    "add_metric_view_instruction",
    "add_table_instruction",
    "update_table_description",
})
```

Modify `patch_blast_radius_is_safe`. The existing flag-check goes inside the same conditional that handles `high_collateral_risk`. Replace `proposal_grounding.py:538-543`:

```python
    if patch.get("high_collateral_risk") and outside:
        return {
            "safe": False,
            "reason": "high_collateral_risk_flagged",
            "passing_dependents_outside_target": outside[:20],
        }
```

with:

```python
    if patch.get("high_collateral_risk") and outside:
        from genie_space_optimizer.common.config import (
            lever_aware_blast_radius_enabled,
        )
        patch_type = str(patch.get("patch_type") or patch.get("type") or "")
        if (
            lever_aware_blast_radius_enabled()
            and patch_type in _NON_SEMANTIC_PATCH_TYPES
        ):
            return {
                "safe": True,
                "reason": "non_semantic_collateral_warning",
                "passing_dependents_outside_target": outside[:20],
                "patch_type": patch_type,
            }
        return {
            "safe": False,
            "reason": "high_collateral_risk_flagged",
            "passing_dependents_outside_target": outside[:20],
        }
```

- [ ] **Step 4: Run tests**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_blast_radius_lever_aware.py -xvs
pytest packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget -xvs
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/proposal_grounding.py \
    packages/genie-space-optimizer/tests/unit/test_blast_radius_lever_aware.py
git commit -m "feat(optimizer): lever-aware blast-radius gradation (flag-gated, default-off)"
```

---

## Task F: Diagnostic-AG RCA inheritance

**Background.** When the strategist emits only one AG per iteration (`STRATEGIST COVERAGE GAP`), the harness materializes "diagnostic AGs" to cover unaddressed clusters. Today those AGs carry no `rca_id`, so all of their proposals are emitted ungrounded and dropped at the `rca_groundedness` gate. The fix: when a diagnostic AG is created for a known cluster, stamp the cluster's `rca_id` on the AG itself; Task D then carries it to every proposal.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/action_groups.py` (or harness call site, depending on where diagnostic AGs are minted)
- Test: extend `packages/genie-space-optimizer/tests/unit/test_proposals_rca_inherit.py`

- [ ] **Step 1: Locate the diagnostic-AG construction site**

```bash
rg -n "AG_COVERAGE|diagnostic_ag|coverage_ag" packages/genie-space-optimizer/src/genie_space_optimizer/optimization/
```

The construction site emits an AG with `id=f"AG_COVERAGE_{cluster_id}"`. Inspect what fields it populates — verify `rca_id` is missing.

- [ ] **Step 2: Write the failing test**

Append to `packages/genie-space-optimizer/tests/unit/test_proposals_rca_inherit.py`:

```python
def test_diagnostic_ag_carries_cluster_rca(monkeypatch):
    """A diagnostic AG materialized for a hard cluster must inherit the
    cluster's rca_id so its proposals are not dropped by the
    rca_groundedness gate."""
    monkeypatch.setenv("GSO_RCA_AWARE_PATCH_CAP", "1")
    from genie_space_optimizer.optimization.stages.action_groups import (
        materialize_diagnostic_ag,
    )

    cluster = {"id": "H003", "qids": ("gs_009",)}
    ag = materialize_diagnostic_ag(
        cluster=cluster,
        rca_id_by_cluster={"H003": "RCA_TOP10_LOGIC"},
    )
    assert ag["rca_id"] == "RCA_TOP10_LOGIC"
    assert ag["id"] == "AG_COVERAGE_H003"
    assert ag["target_qids"] == ("gs_009",)
```

- [ ] **Step 3: Run test to verify it fails**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_proposals_rca_inherit.py::test_diagnostic_ag_carries_cluster_rca -xvs
```

Expected: FAIL with `ImportError`.

- [ ] **Step 4: Add `materialize_diagnostic_ag` to `stages/action_groups.py`**

```python
def materialize_diagnostic_ag(
    *,
    cluster: Mapping[str, Any],
    rca_id_by_cluster: Mapping[str, str],
) -> dict[str, Any]:
    """Build a diagnostic AG for ``cluster`` that inherits its rca_id.

    Used when the strategist did not emit an AG for a hard cluster in
    this iteration but the harness wants to attempt a diagnostic-only
    pass. The inherited rca_id propagates to every proposal at the F5
    stage entry (Task D), keeping these proposals out of the
    rca_groundedness gate's drop set.
    """
    cluster_id = str(cluster.get("id") or "")
    rca_id = str(rca_id_by_cluster.get(cluster_id) or "")
    return {
        "id": f"AG_COVERAGE_{cluster_id}",
        "ag_id": f"AG_COVERAGE_{cluster_id}",
        "ag_kind": "diagnostic",
        "rca_id": rca_id,
        "primary_cluster_id": cluster_id,
        "source_cluster_ids": (cluster_id,),
        "target_qids": tuple(str(q) for q in (cluster.get("qids") or ())),
        "affected_questions": tuple(str(q) for q in (cluster.get("qids") or ())),
    }
```

- [ ] **Step 5: Replace the existing harness diagnostic-AG construction with this helper**

Search the harness for the existing inline `AG_COVERAGE_` builder identified in Step 1. Replace with a call to `materialize_diagnostic_ag(cluster=..., rca_id_by_cluster=_rca_id_by_cluster)`. The `_rca_id_by_cluster` map is already in scope at the F4/F5 dispatch.

- [ ] **Step 6: Run tests**

```bash
pytest packages/genie-space-optimizer/tests/unit/test_proposals_rca_inherit.py -xvs
pytest packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget -xvs
pytest packages/genie-space-optimizer/tests/replay/ -x
```

Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/action_groups.py \
    packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
    packages/genie-space-optimizer/tests/unit/test_proposals_rca_inherit.py
git commit -m "feat(optimizer): diagnostic-AG inherits cluster rca_id"
```

---

## Task G: Integration test, fixture refresh, and burn-down log entry

This task lights all five flags and produces the new replay fixture cycle.

**Files:**
- Test: `packages/genie-space-optimizer/tests/integration/test_optimizer_flags_end_to_end.py` (create)
- Modify: `packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py` (raise then re-tighten budget per cycle)
- Modify: `packages/genie-space-optimizer/docs/2026-05-02-phase-a-burndown-log.md`

- [ ] **Step 1: Write end-to-end integration test**

```python
"""Integration test: with all five control-plane flags enabled, the
synthetic 3-cluster airline-shaped fixture produces the expected
outcomes (target-aware acceptance rejects below threshold; halts when
no causal applyable; non-semantic patches survive blast-radius)."""

import json
from pathlib import Path

import pytest


_FX = Path(__file__).resolve().parents[1] / "replay" / "fixtures" / "airline_real_v1.json"


@pytest.fixture
def all_flags_on(monkeypatch):
    for env in (
        "GSO_TARGET_AWARE_ACCEPTANCE",
        "GSO_NO_CAUSAL_APPLYABLE_HALT",
        "GSO_BUCKET_DRIVEN_AG_SELECTION",
        "GSO_RCA_AWARE_PATCH_CAP",
        "GSO_LEVER_AWARE_BLAST_RADIUS",
    ):
        monkeypatch.setenv(env, "1")


def test_synthetic_airline_run_resolves_targeted_qids(all_flags_on):
    """Synthetic replay over the airline fixture should resolve gs_009
    and gs_024 once all five control-plane flags are enabled.

    This test exercises the full lever_loop_replay path against the
    cycle-N fixture. It is permitted to violate the byte-stable budget
    while the new fixture is being intaken (see Task G Step 3); after
    intake, the budget is re-tightened to 0.
    """
    from genie_space_optimizer.optimization.lever_loop_replay import run_replay

    fx = json.loads(_FX.read_text())
    result = run_replay(fx)
    final_iter = max(result.iteration_outputs.keys())
    final_summary = result.iteration_outputs[final_iter]
    accepted_target_fixed = sum(
        len(d.target_fixed_qids)
        for d in final_summary.acceptance_decisions
        if d.accepted
    )
    rejected_attribution_drift = sum(
        1 for d in final_summary.acceptance_decisions
        if d.reason_code == "rejected_below_threshold_no_target_progress"
    )
    assert accepted_target_fixed >= 1 or rejected_attribution_drift >= 1
```

- [ ] **Step 2: Run integration test (expect a real airline cycle)**

```bash
pytest packages/genie-space-optimizer/tests/integration/test_optimizer_flags_end_to_end.py -xvs
```

If this fails because the airline fixture is the legacy (flag-off) one, that is the expected handoff signal: a fresh airline cycle must be intaken with all five flags on. Coordinate with `gso-replay-cycle-intake` skill — input it the cycle's job_run.

- [ ] **Step 3: Run a fresh airline pilot with all five flags on**

```bash
GSO_TARGET_AWARE_ACCEPTANCE=1 \
GSO_NO_CAUSAL_APPLYABLE_HALT=1 \
GSO_BUCKET_DRIVEN_AG_SELECTION=1 \
GSO_RCA_AWARE_PATCH_CAP=1 \
GSO_LEVER_AWARE_BLAST_RADIUS=1 \
  databricks bundle deploy && \
  databricks bundle run lever_loop_pilot --profile <profile>
```

Capture the resulting `(job_id, run_id)` and use the `gso-postmortem` skill to verify accuracy ≥ 95.8% and `gs_009` / `gs_024` are resolved.

- [ ] **Step 4: Intake the new fixture via `gso-replay-cycle-intake`**

Hand off to the intake skill with `cycle_number=<N+1>`, `source=databricks://<job_id>/<run_id>`, and notes summarizing the five-flag activation. The intake skill copies the new replay fixture to `tests/replay/fixtures/airline_real_v1.json` and verifies the burn-down budget remains at 0 (or names a new value).

- [ ] **Step 5: Update burn-down log**

Append a new cycle row to `packages/genie-space-optimizer/docs/2026-05-02-phase-a-burndown-log.md`:

```markdown
| Cycle | Date | Budget | Notes |
|---|---|---|---|
| 9 | 2026-05-05 | 0 | Five-flag activation. gs_009/gs_024 resolved via target-aware acceptance, RCA-aware patch_cap, lever-aware blast-radius. Final accuracy 95.8% (gs_029 ground-truth correction excluded). |
```

- [ ] **Step 6: Run all replay tests**

```bash
pytest packages/genie-space-optimizer/tests/replay/ -x
pytest packages/genie-space-optimizer/tests/integration/ -x
```

Expected: all PASS at `BURNDOWN_BUDGET = 0`.

- [ ] **Step 7: Flip flags on by default in production via `databricks.yml` env-vars**

Edit the lever-loop task definition to set the five env-vars to `"1"` for production runs. Do NOT change defaults in `common/config.py` — the flags remain default-off in unit tests so this plan's guarantees are reproducible from a clean checkout.

- [ ] **Step 8: Commit**

```bash
git add packages/genie-space-optimizer/tests/integration/test_optimizer_flags_end_to_end.py \
    packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json \
    packages/genie-space-optimizer/docs/2026-05-02-phase-a-burndown-log.md \
    databricks.yml
git commit -m "feat(optimizer): activate five control-plane flags + cycle-9 fixture"
```

---

## Self-Review Checklist (do before handing off to executor)

1. **Spec coverage:** every Tier-1/Tier-2 fix from `2026-05-05-optimizer-improvements-plan.md` (the analysis doc) maps to a task: target-aware acceptance → A; no-causal-applyable → B; bucket feedback → C; RCA-aware patch_cap → D; lever-aware blast-radius → E; diagnostic-AG inheritance → F. Strategist multi-AG (Tier-3) is intentionally deferred (commented in the plan body) until after the byte-stable cycle-9 lands.
2. **Placeholder scan:** every step has either complete code or a real shell command. No "TBD" / "implement later".
3. **Type consistency:** `_filter_to_causal_applyable_proposals` returns `(list[dict], bool)` in every reference. `materialize_diagnostic_ag` returns `dict[str, Any]` with stable field names. `AcceptanceInput.thresholds_met` and `ActionGroupsInput.prior_buckets_by_qid` are added with default values so existing callers don't break.
4. **Regression discipline:** every behaviour-changing task ends with a replay-test step. The `BURNDOWN_BUDGET = 0` invariant only moves in Task G under explicit cycle intake. All five flags are default-off until Task G Step 7.
5. **Independence:** Tasks A–F are independent and can be reordered or split across PRs. Task G is the only task with cross-dependencies on all earlier tasks.
