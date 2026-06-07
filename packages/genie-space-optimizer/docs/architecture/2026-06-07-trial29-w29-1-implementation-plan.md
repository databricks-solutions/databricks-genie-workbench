# Trial 29 W29.1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the W28.4 inert-patch blocker by adding a typed post-apply behaviour gate that re-routes kit-forced RCAs with `behavioral_diff = "unchanged"` to a different structural mechanism, persisting typed diagnostics, and decomposing the monolithic `architecture_invariants_held` into per-domain sub-invariants.

**Architecture:** Mirrors the existing `insufficient_repair_signature` threading pattern (acceptance_gate → harvest → `TransformerContext` field → cluster_batch → Stage 3 prompt). Adds a new SM decision literal (`kit_forced_inert_reroute`), a new typed `InertMechanismHistory` channel, a typed `Trial29InertPatchDiagnostic` Pydantic record for postmortem persistence, and a decomposed `ArchitectureInvariants` typed model with a `all_held`-property backwards-compat shim. All paths gated by `GSO_TRIAL29_BEHAVIOR_DELTA` master flag + sub-flags so `=0` restores pre-Trial-29 behaviour byte-for-byte.

**Tech Stack:** Python 3.11, Pydantic v2, pytest, `monkeypatch`, project-local `JsonRoundTrip` dataclass base, `Literal` typing, `TransformerContext` SM pattern.

**Spec:** `docs/architecture/2026-06-07-trial29-w29-1-inert-patch-reroute-design.md` (commit `c529a841`).

---

## File Structure

### New files

| Path | Responsibility |
|---|---|
| `src/genie_space_optimizer/optimization/trial29_flags.py` | Master `trial29_behavior_delta_enabled()` + sub-flag `trial29_inert_reroute_enabled()` accessors (mirror of `trial28_flags.py`). |
| `src/genie_space_optimizer/optimization/inert_patch_diagnostic.py` | `Trial29InertPatchDiagnostic` typed model + `Trial29InertPatchDiagnosticPersister` (writes JSONL into the postmortem bundle). |
| `src/genie_space_optimizer/optimization/inert_mechanism_history.py` | `InertMechanismHistory` typed model + `harvest_sm_inert_mechanism_history()` + `extend_sm_inert_mechanism_history()` (mirror of `forbidden_signatures` module). |
| `src/genie_space_optimizer/optimization/architecture_invariants.py` | `ArchitectureInvariants` typed model with `rca_invariants_held` / `lever_lattice_invariants_held` / `bundle_completeness_invariants_held` + `all_held` property + per-domain check functions. |
| `tests/unit/optimization/test_trial29_flags.py` | Flag matrix tests (mirror of `test_trial27_flags.py`). |
| `tests/unit/optimization/test_trial29_inert_patch_reroute.py` | Acceptance gate emits new lane on (kit_forced ∧ unchanged); byte-stable when sub-flag `=0`. |
| `tests/unit/optimization/test_trial29_inert_mechanism_history.py` | Model round-trip, harvest function, threading through `cluster_batch`. |
| `tests/unit/optimization/test_trial29_inert_patch_diagnostic.py` | Pydantic round-trip, JSONL persistence into postmortem bundle. |
| `tests/unit/optimization/test_trial29_architecture_invariants.py` | Model serialization, `all_held` property, backwards-compat shim. |
| `tests/integration/postmortem_replay/test_trial29_w29_1_kit_forced_inert_reroute_replay.py` | End-to-end with a 7now-shaped kit-forced inert patch payload. |

### Modified files

| Path | Change |
|---|---|
| `src/genie_space_optimizer/optimization/state_machine/records.py` | Extend `AcceptanceDecisionRecord.decision` literal with `"kit_forced_inert_reroute"`; add field `rejected_mechanism: str = ""`. |
| `src/genie_space_optimizer/optimization/state_machine/transformers/acceptance_gate.py` | Add new lane parallel to `KEPT_INSUFFICIENT`; emit marker; populate `rejected_mechanism`. |
| `src/genie_space_optimizer/optimization/state_machine/verdict.py` | Add `inert_mechanism_history: tuple[InertMechanismHistory, ...] = ()` to `TransformerContext`. |
| `src/genie_space_optimizer/optimization/state_machine/transformers/cluster_batch.py` | Plumb `inert_mechanism_history` through `build_stage2_batch_input` so Stage 3 sees it. |
| `src/genie_space_optimizer/optimization/stages/synthesize.py` | Render the per-QID `inert_mechanism_history` in the Stage 3 prompt; instruct the LLM to pick from `_structural_fix_mechanisms(rca) - rejected`. |
| `docs/architecture/lever-loop-iteration-tracker.md` | Mark W29.1 `[x]` with module owners + acceptance evidence. |

---

## Task ordering rationale

TDD-first per phase. Each phase's TEST task lands FAILING first, then minimal implementation lands to pass it, then commit. Phases are ordered so each phase's surface depends only on prior phases. The integration replay (Phase 7) ties everything together and runs LAST.

| Phase | What | Why this order |
|---|---|---|
| 1 | Flags | Trivial scaffolding; every later phase imports from here. |
| 2 | New SM decision literal + `rejected_mechanism` field on `AcceptanceDecisionRecord` | Types are the contract; every later phase consumes them. |
| 3 | `InertMechanismHistory` model + harvest module | Pure data type, no SM dependency yet. |
| 4 | Acceptance-gate new lane | Wires phases 1–3 together at the FIRST mutation site. |
| 5 | `TransformerContext` threading + `cluster_batch` plumbing | Connects acceptance-gate output to next-iteration input. |
| 6 | Stage 3 prompt rendering | The downstream consumer of the threaded history. |
| 7 | `Trial29InertPatchDiagnostic` + postmortem persistence | Observability layer (works once phases 4–6 are in). |
| 8 | `ArchitectureInvariants` typed model + decomposition | Backwards-compat refactor, independent of phases 4–7 surface but consumes them for the new sub-invariant. |
| 9 | Integration replay test + tracker mark + final verification | End-to-end gate. |

---

## Phase 1: Flags

### Task 1.1: trial29_flags.py — failing test for default-ON master

**Files:**
- Create: `tests/unit/optimization/test_trial29_flags.py`

- [ ] **Step 1: Write the failing test**

```python
"""Trial 29 — master + sub-flag default-ON semantics.

Mirrors :mod:`tests.unit.optimization.test_trial27_flags`. Pins:

* Default ON (env unset) for master and every sub-flag.
* Off-values ``0`` / ``false`` / ``no`` / ``off`` (case-insensitive)
  disable.
* Master OFF forces every sub-flag OFF regardless of its own env var
  (single emergency rollback knob).
* Sub-flag OFF leaves master and siblings ON.
* Unknown / typo values treated as ON.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.trial29_flags import (
    trial29_behavior_delta_enabled,
    trial29_inert_reroute_enabled,
)


_SUB_FLAG_HELPERS = [
    (trial29_inert_reroute_enabled, "GSO_TRIAL29_INERT_REROUTE"),
]


def test_master_default_on(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL29_BEHAVIOR_DELTA", raising=False)
    assert trial29_behavior_delta_enabled() is True


@pytest.mark.parametrize(
    "off_value", ["0", "false", "no", "off", "FALSE", "OFF", "No"]
)
def test_master_off_values_disable(monkeypatch, off_value):
    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", off_value)
    assert trial29_behavior_delta_enabled() is False


def test_master_off_forces_all_sub_flags_off(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", "0")
    for helper, env in _SUB_FLAG_HELPERS:
        monkeypatch.setenv(env, "1")
        assert helper() is False, (
            f"{helper.__name__} must be OFF when master is OFF, even "
            f"with {env}=1"
        )


@pytest.mark.parametrize("helper,env_name", _SUB_FLAG_HELPERS)
def test_sub_flag_default_on(monkeypatch, helper, env_name):
    monkeypatch.delenv("GSO_TRIAL29_BEHAVIOR_DELTA", raising=False)
    monkeypatch.delenv(env_name, raising=False)
    assert helper() is True


@pytest.mark.parametrize("helper,env_name", _SUB_FLAG_HELPERS)
def test_sub_flag_off_leaves_master_on(monkeypatch, helper, env_name):
    monkeypatch.delenv("GSO_TRIAL29_BEHAVIOR_DELTA", raising=False)
    monkeypatch.setenv(env_name, "0")
    assert trial29_behavior_delta_enabled() is True
    assert helper() is False


def test_unknown_value_treated_as_on(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", "enabled")
    assert trial29_behavior_delta_enabled() is True

    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", "")
    assert trial29_behavior_delta_enabled() is True

    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", "true")
    assert trial29_behavior_delta_enabled() is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/test_trial29_flags.py -q`
Expected: `ModuleNotFoundError: No module named 'genie_space_optimizer.optimization.trial29_flags'`

### Task 1.2: trial29_flags.py — minimal implementation

**Files:**
- Create: `src/genie_space_optimizer/optimization/trial29_flags.py`

- [ ] **Step 1: Write the implementation**

```python
"""Trial 29 — behaviour-changing structural lever for kit-forced RCAs.

Mirror of :mod:`trial28_flags`. Same default-ON / OFF-vocabulary
semantics. Single emergency rollback knob is ``GSO_TRIAL29_BEHAVIOR_DELTA``.

Trial 29 closes the W28.4 inert-patch blocker:

1. **Post-apply behaviour gate + structural-lever routing (W29.1).**
   When the kit gate fires + the patch applies + the post-eval
   ``behavioral_diff == "unchanged"``, route to a new
   ``kit_forced_inert_reroute`` acceptance lane and feed the rejected
   mechanism back into the next iteration's Stage 3 synthesis so the
   LLM picks a different mechanism from
   ``_structural_fix_mechanisms(rca) - rejected``.
"""
from __future__ import annotations

import os


_TRIAL29_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL29_FLAG_OFF_VALUES


def trial29_behavior_delta_enabled() -> bool:
    """Trial 29 master flag. Default ON.

    Opt out with ``export GSO_TRIAL29_BEHAVIOR_DELTA=0`` for emergency
    rollback. When OFF, every Trial 29 sub-flag is forced OFF
    regardless of its own env var, so the new acceptance lane is
    skipped and inert kit-forced patches drop into the existing
    ``kept_insufficient`` lane (byte-stable rollback).
    """
    return _flag_enabled("GSO_TRIAL29_BEHAVIOR_DELTA")


def _subflag_opt_out(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    off = raw in _TRIAL29_FLAG_OFF_VALUES
    return trial29_behavior_delta_enabled() and not off


def trial29_inert_reroute_enabled() -> bool:
    """W29.1 — enable the ``kit_forced_inert_reroute`` acceptance lane
    + ``inert_mechanism_history`` lever-loop feedback channel.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL29_INERT_REROUTE=0``.

    When OFF, inert kit-forced patches drop into ``kept_insufficient``
    exactly as before Trial 29 (byte-stable). The acceptance-gate test
    suite covers both branches.
    """
    return _subflag_opt_out("GSO_TRIAL29_INERT_REROUTE")


__all__ = [
    "trial29_behavior_delta_enabled",
    "trial29_inert_reroute_enabled",
]
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/test_trial29_flags.py -q`
Expected: `19 passed`

- [ ] **Step 3: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/trial29_flags.py \
        packages/genie-space-optimizer/tests/unit/optimization/test_trial29_flags.py
git commit -m "feat(gso): Trial 29 flags — master + W29.1 sub-flag (default ON)"
```

---

## Phase 2: SM types — new decision literal + rejected_mechanism field

### Task 2.1: failing test — `AcceptanceDecisionRecord` accepts new literal + field

**Files:**
- Create: `tests/unit/optimization/test_trial29_inert_patch_reroute.py`

- [ ] **Step 1: Write the FIRST batch of failing tests (types only — gate-logic tests come in Phase 4)**

```python
"""Trial 29 W29.1 — kit-forced inert-patch re-route acceptance lane.

Phase 2 (this file's first cohort): the AcceptanceDecisionRecord
literal accepts ``"kit_forced_inert_reroute"`` and carries a
``rejected_mechanism`` field. Pure type test; no acceptance-gate
behaviour yet.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
)


def test_record_accepts_kit_forced_inert_reroute_literal():
    record = AcceptanceDecisionRecord(
        decision="kit_forced_inert_reroute",
        arbiter_reason="kit_forced_inert_reroute:behavior=unchanged",
        target_fixed=False,
        collateral_regressions=(),
        insufficient_repair_signature="",
        behavioral_diff="unchanged",
        rejected_mechanism="add_sql_snippet_filter",
    )
    assert record.decision == "kit_forced_inert_reroute"
    assert record.rejected_mechanism == "add_sql_snippet_filter"


def test_record_rejected_mechanism_defaults_to_empty():
    record = AcceptanceDecisionRecord(
        decision="accepted",
        arbiter_reason="ok",
        target_fixed=True,
        collateral_regressions=(),
    )
    assert record.rejected_mechanism == ""


def test_record_serialises_rejected_mechanism():
    record = AcceptanceDecisionRecord(
        decision="kit_forced_inert_reroute",
        arbiter_reason="kit_forced_inert_reroute:behavior=unchanged",
        target_fixed=False,
        collateral_regressions=(),
        insufficient_repair_signature="add_sql_snippet_filter:filter:insufficient:rca=wrong_aggregation:behavior=unchanged",
        behavioral_diff="unchanged",
        rejected_mechanism="add_sql_snippet_filter",
    )
    blob = record.to_dict() if hasattr(record, "to_dict") else record.__dict__
    payload = dict(blob)
    assert payload["decision"] == "kit_forced_inert_reroute"
    assert payload["rejected_mechanism"] == "add_sql_snippet_filter"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/test_trial29_inert_patch_reroute.py -q`
Expected: All FAIL — `Literal['accepted', 'rolled_back', ...]` does not include `"kit_forced_inert_reroute"`, and `rejected_mechanism` field doesn't exist.

### Task 2.2: extend `AcceptanceDecisionRecord`

**Files:**
- Modify: `src/genie_space_optimizer/optimization/state_machine/records.py`

- [ ] **Step 1: Patch the decision literal + add the field**

Find the `AcceptanceDecisionRecord` block (around line 117) and change:

```python
    decision: Literal[
        "accepted",
        "rolled_back",
        "kept_insufficient",
        "already_correct_under_arbiter",
    ]
```

to:

```python
    decision: Literal[
        "accepted",
        "rolled_back",
        "kept_insufficient",
        "already_correct_under_arbiter",
        # Trial 29 W29.1 — kit-forced patch with applied but
        # ``behavioral_diff == "unchanged"``. Sibling of
        # ``kept_insufficient`` but distinct because the lever loop
        # MUST pick a different mechanism next iteration (the
        # rejected one is recorded in ``rejected_mechanism``).
        # Routed via ``inert_mechanism_history`` accumulator into
        # ``TransformerContext.inert_mechanism_history`` and rendered
        # in the Stage 3 prompt so the LLM picks from
        # ``_structural_fix_mechanisms(rca) - rejected``.
        "kit_forced_inert_reroute",
    ]
```

Add after the `behavioral_diff` field:

```python
    # Trial 29 W29.1 — the mechanism that produced the inert patch.
    # Populated only when ``decision == "kit_forced_inert_reroute"``.
    # Empty string for every other decision. Harvested by
    # ``inert_mechanism_history.harvest_sm_inert_mechanism_history``
    # at iteration end and surfaced into
    # ``ctx.inert_mechanism_history`` so the next iteration's Stage 3
    # synthesis picks from
    # ``_structural_fix_mechanisms(rca) - rejected_mechanism``.
    rejected_mechanism: str = ""
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/test_trial29_inert_patch_reroute.py -q`
Expected: `3 passed`

- [ ] **Step 3: Run the full records-related test suite to confirm no regressions**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/ -q -k 'record or acceptance' --tb=no`
Expected: same pass count as baseline (or higher with the 3 new ones); no NEW failures.

- [ ] **Step 4: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state_machine/records.py \
        packages/genie-space-optimizer/tests/unit/optimization/test_trial29_inert_patch_reroute.py
git commit -m "feat(gso): W29.1 — extend AcceptanceDecisionRecord with kit_forced_inert_reroute lane"
```

---

## Phase 3: InertMechanismHistory model + harvest module

### Task 3.1: failing test — `InertMechanismHistory` model + harvest

**Files:**
- Create: `tests/unit/optimization/test_trial29_inert_mechanism_history.py`

- [ ] **Step 1: Write the failing test**

```python
"""Trial 29 W29.1 — InertMechanismHistory typed model + harvest.

Mirrors the threading pattern of ``forbidden_signatures.harvest_sm_*``
but for kit-forced inert-patch rejections. The harvest function reads
all AcceptanceDecisionRecord entries with
``decision == "kit_forced_inert_reroute"`` and aggregates them per
(qid, rca_kind) so the next iteration's ``TransformerContext`` has the
history.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.inert_mechanism_history import (
    InertMechanismHistory,
    extend_sm_inert_mechanism_history,
    harvest_sm_inert_mechanism_history,
)
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
)


def test_history_model_round_trip():
    history = InertMechanismHistory(
        qid="gs_009",
        rca_kind="wrong_aggregation",
        rejected_mechanisms=("add_sql_snippet_filter",),
        signatures=(
            "add_sql_snippet_filter:filter:insufficient:rca=wrong_aggregation:behavior=unchanged",
        ),
    )
    blob = history.model_dump()
    rebuilt = InertMechanismHistory.model_validate(blob)
    assert rebuilt == history


def test_harvest_extracts_only_kit_forced_inert_reroute():
    records = [
        # other decisions are ignored
        _stub_record(decision="accepted"),
        _stub_record(decision="rolled_back"),
        _stub_record(decision="kept_insufficient"),
        # target decision
        AcceptanceDecisionRecord(
            decision="kit_forced_inert_reroute",
            arbiter_reason="kit_forced_inert_reroute:behavior=unchanged",
            target_fixed=False,
            collateral_regressions=(),
            insufficient_repair_signature="add_sql_snippet_filter:filter:insufficient:rca=wrong_aggregation:behavior=unchanged",
            behavioral_diff="unchanged",
            rejected_mechanism="add_sql_snippet_filter",
        ),
    ]
    qid_rca_pairs = [
        ("gs_009", "wrong_aggregation"),
    ]
    out = harvest_sm_inert_mechanism_history(records, qid_rca_pairs=qid_rca_pairs)
    assert len(out) == 1
    assert out[0].qid == "gs_009"
    assert out[0].rca_kind == "wrong_aggregation"
    assert out[0].rejected_mechanisms == ("add_sql_snippet_filter",)


def test_extend_accumulates_across_iterations():
    prior = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=("sig1",),
        ),
    )
    fresh = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("replace_join",),
            signatures=("sig2",),
        ),
    )
    merged = extend_sm_inert_mechanism_history(prior, fresh)
    assert len(merged) == 1
    assert merged[0].qid == "gs_009"
    assert merged[0].rejected_mechanisms == ("add_sql_snippet_filter", "replace_join")
    assert merged[0].signatures == ("sig1", "sig2")


def test_extend_keeps_distinct_qid_rca_pairs_separate():
    prior = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=("sig1",),
        ),
    )
    fresh = (
        InertMechanismHistory(
            qid="gs_026",
            rca_kind="plural_top_n_collapse",
            rejected_mechanisms=("add_example_sql",),
            signatures=("sig2",),
        ),
    )
    merged = extend_sm_inert_mechanism_history(prior, fresh)
    assert len(merged) == 2
    qids = {h.qid for h in merged}
    assert qids == {"gs_009", "gs_026"}


def test_extend_dedupes_same_mechanism_within_qid():
    prior = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=("sig1",),
        ),
    )
    fresh = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),  # duplicate
            signatures=("sig1",),  # duplicate
        ),
    )
    merged = extend_sm_inert_mechanism_history(prior, fresh)
    assert merged[0].rejected_mechanisms == ("add_sql_snippet_filter",)
    assert merged[0].signatures == ("sig1",)


def _stub_record(*, decision: str) -> AcceptanceDecisionRecord:
    return AcceptanceDecisionRecord(
        decision=decision,  # type: ignore[arg-type]
        arbiter_reason="",
        target_fixed=False,
        collateral_regressions=(),
    )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/test_trial29_inert_mechanism_history.py -q`
Expected: `ModuleNotFoundError: No module named 'genie_space_optimizer.optimization.inert_mechanism_history'`

### Task 3.2: implement `inert_mechanism_history.py`

**Files:**
- Create: `src/genie_space_optimizer/optimization/inert_mechanism_history.py`

- [ ] **Step 1: Write the implementation**

```python
"""Trial 29 W29.1 — InertMechanismHistory typed accumulator.

The lever-loop feedback channel for kit-forced inert patches. Mirrors
:mod:`forbidden_signatures` (same harvest/extend shape) but the
per-iteration record is a typed Pydantic model carrying the
``(qid, rca_kind)`` key and the ordered list of rejected mechanisms
the system already tried for that pair.

Stage 3 synthesis reads
``TransformerContext.inert_mechanism_history`` and instructs the LLM
to pick from
``_structural_fix_mechanisms(rca_kind) - rejected_mechanisms`` so the
next iteration cannot re-emit a mechanism we already proved inert.
"""
from __future__ import annotations

from typing import Iterable, Sequence

from pydantic import BaseModel, ConfigDict, Field

from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
)


class InertMechanismHistory(BaseModel):
    """Per ``(qid, rca_kind)`` record of mechanisms proven inert."""

    model_config = ConfigDict(frozen=True)

    qid: str
    rca_kind: str  # canonical key (must be in RCA_CANONICAL_KEY_SET)
    rejected_mechanisms: tuple[str, ...] = Field(default_factory=tuple)
    signatures: tuple[str, ...] = Field(default_factory=tuple)


def harvest_sm_inert_mechanism_history(
    records: Sequence[AcceptanceDecisionRecord],
    *,
    qid_rca_pairs: Sequence[tuple[str, str]],
) -> tuple[InertMechanismHistory, ...]:
    """Extract InertMechanismHistory entries from this iteration's
    AcceptanceDecisionRecord stream.

    ``records`` and ``qid_rca_pairs`` are positionally paired (the
    caller threads them so we don't have to re-extract the qid/rca
    from inside the SM transformer that already had them in scope).
    Only records with ``decision == "kit_forced_inert_reroute"``
    contribute; everything else is ignored.
    """
    if len(records) != len(qid_rca_pairs):
        # Defensive: silently truncate to the shorter sequence so a
        # threading bug does not crash the harvest pass.
        n = min(len(records), len(qid_rca_pairs))
        records = records[:n]
        qid_rca_pairs = qid_rca_pairs[:n]

    harvested: list[InertMechanismHistory] = []
    for record, (qid, rca_kind) in zip(records, qid_rca_pairs):
        if record.decision != "kit_forced_inert_reroute":
            continue
        if not record.rejected_mechanism:
            continue
        harvested.append(
            InertMechanismHistory(
                qid=str(qid),
                rca_kind=str(rca_kind),
                rejected_mechanisms=(str(record.rejected_mechanism),),
                signatures=(str(record.insufficient_repair_signature or ""),),
            )
        )
    return tuple(harvested)


def extend_sm_inert_mechanism_history(
    prior: Iterable[InertMechanismHistory],
    fresh: Iterable[InertMechanismHistory],
) -> tuple[InertMechanismHistory, ...]:
    """Merge a fresh iteration's harvest into the cumulative history,
    keyed by ``(qid, rca_kind)``.

    Dedupes mechanisms + signatures within each pair. Order is
    preserved (FIFO: earliest insertion wins).
    """
    by_key: dict[tuple[str, str], InertMechanismHistory] = {}
    for entry in list(prior) + list(fresh):
        key = (entry.qid, entry.rca_kind)
        if key not in by_key:
            by_key[key] = entry
            continue
        existing = by_key[key]
        new_mechanisms = tuple(
            m for m in entry.rejected_mechanisms if m not in existing.rejected_mechanisms
        )
        new_signatures = tuple(
            s for s in entry.signatures if s not in existing.signatures
        )
        by_key[key] = InertMechanismHistory(
            qid=existing.qid,
            rca_kind=existing.rca_kind,
            rejected_mechanisms=existing.rejected_mechanisms + new_mechanisms,
            signatures=existing.signatures + new_signatures,
        )
    return tuple(by_key.values())


__all__ = [
    "InertMechanismHistory",
    "harvest_sm_inert_mechanism_history",
    "extend_sm_inert_mechanism_history",
]
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/test_trial29_inert_mechanism_history.py -q`
Expected: `5 passed`

- [ ] **Step 3: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/inert_mechanism_history.py \
        packages/genie-space-optimizer/tests/unit/optimization/test_trial29_inert_mechanism_history.py
git commit -m "feat(gso): W29.1 — InertMechanismHistory typed accumulator + harvest"
```

---

## Phase 4: Acceptance gate — new lane

### Task 4.1: failing test — gate emits new lane on (kit_forced ∧ unchanged)

**Files:**
- Modify: `tests/unit/optimization/test_trial29_inert_patch_reroute.py` (append the SECOND cohort)

- [ ] **Step 1: Append the gate-behaviour tests**

```python
# --- Phase 4: acceptance-gate behaviour ---
# These tests require the new lane to be wired into acceptance_gate.py
# and the sub-flag honoured. The fixtures simulate a minimal SM state
# with: kit_forced marker present, behavioral_diff == "unchanged",
# target unfixed.

from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization.state_machine.transformers import (
    acceptance_gate,
)


def _make_state(*, kit_forced: bool, behavioral_diff: str, target_fixed: bool):
    state = MagicMock()
    state.evaluated.behavioral_diff = behavioral_diff
    state.evaluated.target_fixed = target_fixed
    state.evaluated.pre_apply_score = 0.0
    state.evaluated.post_apply_score = 0.0
    state.diagnosed.rca_kind_label = "wrong_aggregation"
    state.applied.kit_forced = kit_forced
    state.applied.selected_lever = "add_sql_snippet_filter"
    state.applied.patch_type = "filter"
    return state


def test_kit_forced_inert_routes_to_new_lane(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL29_BEHAVIOR_DELTA", raising=False)
    monkeypatch.delenv("GSO_TRIAL29_INERT_REROUTE", raising=False)
    state = _make_state(kit_forced=True, behavioral_diff="unchanged", target_fixed=False)
    record = acceptance_gate.decide_for_state(state)
    assert record.decision == "kit_forced_inert_reroute"
    assert record.rejected_mechanism == "add_sql_snippet_filter"
    assert record.behavioral_diff == "unchanged"


def test_kit_forced_changed_does_not_route_to_new_lane(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL29_BEHAVIOR_DELTA", raising=False)
    monkeypatch.delenv("GSO_TRIAL29_INERT_REROUTE", raising=False)
    state = _make_state(kit_forced=True, behavioral_diff="matches_expected", target_fixed=True)
    record = acceptance_gate.decide_for_state(state)
    assert record.decision != "kit_forced_inert_reroute"


def test_non_kit_forced_inert_falls_through_to_kept_insufficient(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL29_BEHAVIOR_DELTA", raising=False)
    monkeypatch.delenv("GSO_TRIAL29_INERT_REROUTE", raising=False)
    state = _make_state(kit_forced=False, behavioral_diff="unchanged", target_fixed=False)
    record = acceptance_gate.decide_for_state(state)
    assert record.decision == "kept_insufficient"


def test_flag_off_restores_kept_insufficient(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL29_INERT_REROUTE", "0")
    monkeypatch.delenv("GSO_TRIAL29_BEHAVIOR_DELTA", raising=False)
    state = _make_state(kit_forced=True, behavioral_diff="unchanged", target_fixed=False)
    record = acceptance_gate.decide_for_state(state)
    assert record.decision == "kept_insufficient"  # byte-stable rollback
    assert record.rejected_mechanism == ""


def test_master_off_forces_byte_stable_rollback(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", "0")
    state = _make_state(kit_forced=True, behavioral_diff="unchanged", target_fixed=False)
    record = acceptance_gate.decide_for_state(state)
    assert record.decision == "kept_insufficient"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/test_trial29_inert_patch_reroute.py -q`
Expected: 5 of 8 fail (the 3 Phase 2 type tests pass; the 5 new gate tests fail because `acceptance_gate.decide_for_state` doesn't exist OR the new lane logic isn't wired).

### Task 4.2: implement the new lane in acceptance_gate.py

**Files:**
- Modify: `src/genie_space_optimizer/optimization/state_machine/transformers/acceptance_gate.py`

- [ ] **Step 1: Read the existing kept_insufficient lane to find the insertion point**

Run: `cd packages/genie-space-optimizer && rg -n "kept_insufficient" src/genie_space_optimizer/optimization/state_machine/transformers/acceptance_gate.py | head -10`

Look for the block (around line 205+) that emits the `kept_insufficient` decision. The new lane goes BEFORE that block (kit_forced ∧ unchanged check fires first).

- [ ] **Step 2: Add the new lane logic**

Insert this block immediately BEFORE the existing `kept_insufficient` branch (the one that builds the `signature = f"{selected_lever}:{patch_type}:insufficient:rca={rca_kind}:behavior={behavioral_diff}"`):

```python
# Trial 29 W29.1 — kit-forced inert patch re-route lane.
#
# When the kit gate fired (GSO_TRIAL24_KIT_FORCED_V1 emitted), the
# patch applied, and the post-eval shows ``behavioral_diff == "unchanged"``,
# route to a distinct ``kit_forced_inert_reroute`` lane instead of
# ``kept_insufficient``. The rejected mechanism is recorded on the
# AcceptanceDecisionRecord so the lever-loop harvest reads it and
# threads it into the next iteration's ``ctx.inert_mechanism_history``.
# The new lane is gated by the Trial 29 sub-flag for byte-stable
# rollback.
from genie_space_optimizer.optimization.trial29_flags import (
    trial29_inert_reroute_enabled,
)

_kit_forced = bool(getattr(state.applied, "kit_forced", False))
_behavior_unchanged_for_kit = (
    getattr(state.evaluated, "behavioral_diff", "unchanged") == "unchanged"
)
if (
    trial29_inert_reroute_enabled()
    and _kit_forced
    and _behavior_unchanged_for_kit
    and not target_fixed
):
    rejected_mechanism = str(
        getattr(state.applied, "selected_lever", "") or ""
    )
    rca_kind_str = str(
        getattr(state.diagnosed, "rca_kind_label", "") or ""
    )
    patch_type_str = str(
        getattr(state.applied, "patch_type", "") or ""
    )
    signature = (
        f"{rejected_mechanism or '?'}:{patch_type_str or '?'}"
        f":insufficient:rca={rca_kind_str or '?'}"
        f":behavior=unchanged"
    )
    # Plain print mirrors the existing ``GSO_GATE_REASONING_V1``
    # marker style — dashboards parse one marker per decision lane.
    print(
        "GSO_TRIAL29_INERT_PATCH_REROUTE_V1"
        + json.dumps(
            {
                "qid": getattr(state, "qid", "") or "",
                "rca_kind": rca_kind_str,
                "rejected_mechanism": rejected_mechanism,
                "patch_type": patch_type_str,
                "behavioral_diff": "unchanged",
                "signature": signature,
            },
            sort_keys=True,
            default=str,
        ),
    )
    return GateVerdict.success(record=AcceptanceDecisionRecord(
        decision="kit_forced_inert_reroute",
        arbiter_reason=f"kit_forced_inert_reroute:behavior=unchanged",
        target_fixed=False,
        collateral_regressions=(),
        insufficient_repair_signature=signature,
        behavioral_diff="unchanged",
        rejected_mechanism=rejected_mechanism,
    ))
```

NOTE: if the existing acceptance_gate module already imports `json`, omit the import. If `decide_for_state` is not the public entry point, the test file imports must match the actual function name — adjust the test in Phase 4.1 to use `acceptance_gate.<actual_name>` (read the module to confirm).

- [ ] **Step 3: Run tests to verify they pass**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/test_trial29_inert_patch_reroute.py -q`
Expected: `8 passed`

- [ ] **Step 4: Run the broader acceptance-gate suite to confirm no regressions**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/state_machine/ tests/unit/state_machine/ -q --tb=no -k acceptance`
Expected: same pass count as baseline; no NEW failures.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state_machine/transformers/acceptance_gate.py \
        packages/genie-space-optimizer/tests/unit/optimization/test_trial29_inert_patch_reroute.py
git commit -m "feat(gso): W29.1 — acceptance_gate kit_forced_inert_reroute lane"
```

---

## Phase 5: TransformerContext threading + cluster_batch plumbing

### Task 5.1: extend `TransformerContext` with `inert_mechanism_history`

**Files:**
- Modify: `src/genie_space_optimizer/optimization/state_machine/verdict.py`
- Modify: `src/genie_space_optimizer/optimization/state_machine/transformers/cluster_batch.py`

- [ ] **Step 1: Write the failing test (extend the existing history test file)**

Append to `tests/unit/optimization/test_trial29_inert_mechanism_history.py`:

```python
# --- Phase 5: TransformerContext + cluster_batch threading ---
from genie_space_optimizer.optimization.state_machine.verdict import (
    TransformerContext,
)
from genie_space_optimizer.optimization.state_machine.transformers.cluster_batch import (
    build_stage2_batch_input,
)


def test_transformer_context_carries_inert_mechanism_history():
    ctx = TransformerContext(
        forbidden_signatures=(),
        insufficient_repair_signatures=(),
    )
    assert ctx.inert_mechanism_history == ()


def test_cluster_batch_propagates_inert_mechanism_history():
    history = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=("sig1",),
        ),
    )
    batch = build_stage2_batch_input(
        diagnosed_states=(),
        forbidden_signatures=(),
        insufficient_repair_signatures=(),
        inert_mechanism_history=history,
    )
    assert batch.inert_mechanism_history == history
```

- [ ] **Step 2: Run failing tests**

Expected: AttributeError on `TransformerContext.inert_mechanism_history`, TypeError on `build_stage2_batch_input(inert_mechanism_history=...)`.

- [ ] **Step 3: Add the field to `TransformerContext`**

In `state_machine/verdict.py` around line 73-74 (after `insufficient_repair_signatures`):

```python
    # Trial 29 W29.1 — typed lever-loop feedback channel for
    # kit-forced inert patches. Populated by
    # ``inert_mechanism_history.harvest_sm_inert_mechanism_history``
    # at iteration end and consumed by Stage 3 synthesis so the LLM
    # picks from ``_structural_fix_mechanisms(rca) - rejected``.
    inert_mechanism_history: tuple["InertMechanismHistory", ...] = ()
```

Add the import at the top (use TYPE_CHECKING guard to avoid circular imports):

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from genie_space_optimizer.optimization.inert_mechanism_history import (
        InertMechanismHistory,
    )
```

- [ ] **Step 4: Add the field to `Stage2BatchInput` and `build_stage2_batch_input`**

In `state_machine/transformers/cluster_batch.py`:

- Add `inert_mechanism_history: tuple["InertMechanismHistory", ...] = ()` to the `Stage2BatchInput` dataclass.
- Add the parameter to `build_stage2_batch_input(*, inert_mechanism_history: tuple["InertMechanismHistory", ...] = (), ...)`.
- Pass it through to the returned `Stage2BatchInput`.
- Add the TYPE_CHECKING import for `InertMechanismHistory`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/test_trial29_inert_mechanism_history.py -q`
Expected: `7 passed` (5 prior + 2 new).

- [ ] **Step 6: Run the cluster_batch test suite to confirm no regressions**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/ -q -k cluster_batch --tb=no`
Expected: same pass count as baseline.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state_machine/verdict.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state_machine/transformers/cluster_batch.py \
        packages/genie-space-optimizer/tests/unit/optimization/test_trial29_inert_mechanism_history.py
git commit -m "feat(gso): W29.1 — thread inert_mechanism_history through TransformerContext + cluster_batch"
```

---

## Phase 6: Stage 3 prompt rendering

### Task 6.1: failing test — Stage 3 prompt includes `inert_mechanism_history` excerpt

**Files:**
- Create: `tests/unit/stages/test_trial29_synthesis_inert_history_prompt.py`

- [ ] **Step 1: Write the failing test**

```python
"""Trial 29 W29.1 — Stage 3 synthesis prompt renders the inert-mechanism
history so the LLM picks from ``_structural_fix_mechanisms(rca) - rejected``.

Does NOT exercise the LLM — only the prompt assembly path. Uses a
minimal Stage2BatchInput stub with one QID + one InertMechanismHistory
entry and asserts the rendered prompt mentions the rejected mechanism
and instructs the LLM to avoid it.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.inert_mechanism_history import (
    InertMechanismHistory,
)
from genie_space_optimizer.optimization.stages.synthesize import (
    render_inert_mechanism_history_section,
)


def test_renders_rejected_mechanism_in_prompt():
    history = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=("add_sql_snippet_filter:filter:insufficient:rca=wrong_aggregation:behavior=unchanged",),
        ),
    )
    section = render_inert_mechanism_history_section(history)
    assert "gs_009" in section
    assert "wrong_aggregation" in section
    assert "add_sql_snippet_filter" in section
    # The renderer must instruct the LLM to AVOID the rejected
    # mechanism (so the synthesis prompt's intent is explicit, not
    # implied by the data layout).
    assert "avoid" in section.lower() or "do not" in section.lower() or "must not" in section.lower()


def test_empty_history_renders_empty_string():
    section = render_inert_mechanism_history_section(())
    assert section == ""


def test_multiple_qids_render_distinct_sections():
    history = (
        InertMechanismHistory(
            qid="gs_009",
            rca_kind="wrong_aggregation",
            rejected_mechanisms=("add_sql_snippet_filter",),
            signatures=("sig1",),
        ),
        InertMechanismHistory(
            qid="gs_026",
            rca_kind="plural_top_n_collapse",
            rejected_mechanisms=("add_example_sql", "replace_join"),
            signatures=("sig2", "sig3"),
        ),
    )
    section = render_inert_mechanism_history_section(history)
    assert "gs_009" in section
    assert "gs_026" in section
    assert "add_sql_snippet_filter" in section
    assert "add_example_sql" in section
    assert "replace_join" in section
```

- [ ] **Step 2: Run failing tests**

Expected: `ImportError: cannot import name 'render_inert_mechanism_history_section' from 'genie_space_optimizer.optimization.stages.synthesize'`

### Task 6.2: implement `render_inert_mechanism_history_section`

**Files:**
- Modify: `src/genie_space_optimizer/optimization/stages/synthesize.py`

- [ ] **Step 1: Add the renderer function** at the bottom of the module (just above the `__all__` or the existing renderer family):

```python
def render_inert_mechanism_history_section(
    history: tuple["InertMechanismHistory", ...],
) -> str:
    """Trial 29 W29.1 — render the per-QID rejected-mechanism history
    into the Stage 3 synthesis prompt.

    The LLM is instructed to pick a mechanism for each ``(qid, rca_kind)``
    pair from
    ``_structural_fix_mechanisms(rca_kind) - rejected_mechanisms`` so
    a previously-inert mechanism is never re-emitted for the same
    pair.

    Empty input renders the empty string (byte-stable when the
    feedback channel is empty).
    """
    if not history:
        return ""

    lines: list[str] = [
        "## Inert-Mechanism History (Trial 29 W29.1)",
        "",
        "The following ``(qid, rca_kind)`` pairs had a kit-forced patch applied",
        "in a prior iteration whose post-eval ``behavioral_diff`` was ``unchanged``.",
        "You MUST AVOID the listed mechanisms for each pair and pick a different",
        "mechanism from the structural-fix lattice for that RCA kind.",
        "",
    ]
    for entry in history:
        lines.append(
            f"- qid=`{entry.qid}` rca_kind=`{entry.rca_kind}` "
            f"rejected_mechanisms={list(entry.rejected_mechanisms)}"
        )
    return "\n".join(lines)


# Add to the TYPE_CHECKING block at the top of the module:
# if TYPE_CHECKING:
#     from genie_space_optimizer.optimization.inert_mechanism_history import (
#         InertMechanismHistory,
#     )
```

- [ ] **Step 2: Add the TYPE_CHECKING import** if not already present.

- [ ] **Step 3: Wire the renderer into the Stage 3 prompt assembly**

Find the place where the Stage 3 prompt body is assembled (look for `forbidden_signatures` or `insufficient_repair_signatures` rendering in the same module). Add a sibling line:

```python
inert_history_section = render_inert_mechanism_history_section(
    batch.inert_mechanism_history
)
# … assembled prompt …
prompt_body = "\n\n".join(
    s for s in [
        existing_section_1,
        existing_section_2,
        inert_history_section,  # NEW — empty string when no history
    ] if s
)
```

(The actual assembly site may vary; use `rg -n 'forbidden_signatures.*join\|insufficient_repair_signatures.*join' src/genie_space_optimizer/optimization/stages/synthesize.py` to locate.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/stages/test_trial29_synthesis_inert_history_prompt.py -q`
Expected: `3 passed`

- [ ] **Step 5: Run the synthesize test suite to confirm byte-stability when history is empty**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/stages/ -q --tb=no -k synthes`
Expected: same pass count as baseline (every existing test runs with empty history → empty section → byte-stable).

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/synthesize.py \
        packages/genie-space-optimizer/tests/unit/stages/test_trial29_synthesis_inert_history_prompt.py
git commit -m "feat(gso): W29.1 — render inert_mechanism_history in Stage 3 synthesis prompt"
```

---

## Phase 7: Diagnostic record + postmortem persistence

### Task 7.1: failing test — `Trial29InertPatchDiagnostic` round-trip + persistence

**Files:**
- Create: `tests/unit/optimization/test_trial29_inert_patch_diagnostic.py`

- [ ] **Step 1: Write the failing test**

```python
"""Trial 29 W29.1 — typed inert-patch diagnostic record + postmortem persistence."""
from __future__ import annotations

import json
from pathlib import Path

from genie_space_optimizer.optimization.inert_patch_diagnostic import (
    Trial29InertPatchDiagnostic,
    persist_inert_patch_diagnostic,
    load_inert_patch_diagnostics,
)


def test_diagnostic_round_trip():
    d = Trial29InertPatchDiagnostic(
        qid="gs_009",
        rca_kind="wrong_aggregation",
        rejected_mechanism="add_sql_snippet_filter",
        patch_json={"mechanism": "add_sql_snippet_filter", "filter_expr": "x IS NOT NULL"},
        pre_arbiter_score=0.0,
        post_arbiter_score=0.0,
        behavioral_diff="unchanged",
        signature="add_sql_snippet_filter:filter:insufficient:rca=wrong_aggregation:behavior=unchanged",
        iteration=2,
        trial="trial29",
    )
    blob = d.model_dump()
    rebuilt = Trial29InertPatchDiagnostic.model_validate(blob)
    assert rebuilt == d


def test_persist_and_load_jsonl(tmp_path: Path):
    bundle_dir = tmp_path / "postmortem_bundle"
    bundle_dir.mkdir()
    d1 = Trial29InertPatchDiagnostic(
        qid="gs_009",
        rca_kind="wrong_aggregation",
        rejected_mechanism="add_sql_snippet_filter",
        patch_json={"a": 1},
        pre_arbiter_score=0.0,
        post_arbiter_score=0.0,
        behavioral_diff="unchanged",
        signature="sig1",
        iteration=2,
        trial="trial29",
    )
    d2 = Trial29InertPatchDiagnostic(
        qid="gs_026",
        rca_kind="plural_top_n_collapse",
        rejected_mechanism="add_example_sql",
        patch_json={"b": 2},
        pre_arbiter_score=0.5,
        post_arbiter_score=0.5,
        behavioral_diff="unchanged",
        signature="sig2",
        iteration=3,
        trial="trial29",
    )
    persist_inert_patch_diagnostic(d1, bundle_dir=bundle_dir)
    persist_inert_patch_diagnostic(d2, bundle_dir=bundle_dir)

    out_file = bundle_dir / "trial29_inert_patch_diagnostics.jsonl"
    assert out_file.exists()
    raw_lines = out_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(raw_lines) == 2
    for line in raw_lines:
        assert json.loads(line)  # valid JSON per line

    loaded = load_inert_patch_diagnostics(bundle_dir)
    assert len(loaded) == 2
    assert loaded[0] == d1
    assert loaded[1] == d2


def test_persist_creates_bundle_dir_if_missing(tmp_path: Path):
    missing_dir = tmp_path / "does_not_exist_yet"
    d = Trial29InertPatchDiagnostic(
        qid="gs_009",
        rca_kind="wrong_aggregation",
        rejected_mechanism="add_sql_snippet_filter",
        patch_json={},
        pre_arbiter_score=0.0,
        post_arbiter_score=0.0,
        behavioral_diff="unchanged",
        signature="sig",
        iteration=1,
        trial="trial29",
    )
    persist_inert_patch_diagnostic(d, bundle_dir=missing_dir)
    assert (missing_dir / "trial29_inert_patch_diagnostics.jsonl").exists()
```

- [ ] **Step 2: Run failing tests**

Expected: `ModuleNotFoundError: No module named 'genie_space_optimizer.optimization.inert_patch_diagnostic'`

### Task 7.2: implement `inert_patch_diagnostic.py`

**Files:**
- Create: `src/genie_space_optimizer/optimization/inert_patch_diagnostic.py`

- [ ] **Step 1: Write the implementation**

```python
"""Trial 29 W29.1 — typed Trial29InertPatchDiagnostic record + JSONL
postmortem persistence.

When the acceptance gate fires the ``kit_forced_inert_reroute`` lane,
the harness builds a Trial29InertPatchDiagnostic with the full
forensic context (RCA, rejected mechanism, patch body, pre/post
arbiter scores, signature, iteration, trial). The record is appended
to the postmortem evidence bundle as JSONL so the next postmortem can
prove or refute "the re-route worked / a different mechanism is also
inert / the RCA is mislabeled".

Pure module: no env reads, no global state, no side effects beyond
the explicit file write in :func:`persist_inert_patch_diagnostic`.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


_BUNDLE_FILENAME = "trial29_inert_patch_diagnostics.jsonl"


class Trial29InertPatchDiagnostic(BaseModel):
    """One inert-patch event captured for postmortem analysis."""

    model_config = ConfigDict(frozen=True)

    qid: str
    rca_kind: str  # canonical key
    rejected_mechanism: str
    patch_json: dict[str, Any] = Field(default_factory=dict)
    pre_arbiter_score: float
    post_arbiter_score: float
    behavioral_diff: str  # always "unchanged" in this lane; recorded for completeness
    signature: str
    iteration: int
    trial: str  # e.g. "trial29"


def persist_inert_patch_diagnostic(
    diagnostic: Trial29InertPatchDiagnostic,
    *,
    bundle_dir: Path,
) -> Path:
    """Append a single diagnostic to the bundle's JSONL file.

    Creates ``bundle_dir`` if missing. Returns the path of the JSONL
    file (so callers can log it). Each call appends one line; the
    file accumulates across iterations and is read by the postmortem
    summariser via :func:`load_inert_patch_diagnostics`.
    """
    bundle_dir.mkdir(parents=True, exist_ok=True)
    target = bundle_dir / _BUNDLE_FILENAME
    payload = diagnostic.model_dump()
    with target.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, sort_keys=True, default=str) + "\n")
    return target


def load_inert_patch_diagnostics(
    bundle_dir: Path,
) -> tuple[Trial29InertPatchDiagnostic, ...]:
    """Read every diagnostic in the bundle's JSONL file in order.

    Returns an empty tuple when the file doesn't exist (no inert
    re-routes happened, byte-stable on green replays).
    """
    target = bundle_dir / _BUNDLE_FILENAME
    if not target.exists():
        return ()
    out: list[Trial29InertPatchDiagnostic] = []
    with target.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            out.append(Trial29InertPatchDiagnostic.model_validate_json(line))
    return tuple(out)


__all__ = [
    "Trial29InertPatchDiagnostic",
    "persist_inert_patch_diagnostic",
    "load_inert_patch_diagnostics",
]
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/test_trial29_inert_patch_diagnostic.py -q`
Expected: `3 passed`

- [ ] **Step 3: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/inert_patch_diagnostic.py \
        packages/genie-space-optimizer/tests/unit/optimization/test_trial29_inert_patch_diagnostic.py
git commit -m "feat(gso): W29.1 — Trial29InertPatchDiagnostic typed record + JSONL persistence"
```

---

## Phase 8: Decomposed architecture invariants

### Task 8.1: failing test — `ArchitectureInvariants` model + `all_held` property + backwards compat

**Files:**
- Create: `tests/unit/optimization/test_trial29_architecture_invariants.py`

- [ ] **Step 1: Write the failing test**

```python
"""Trial 29 W29.5 — decomposed architecture invariants (typed model).

The monolithic ``architecture_invariants_held: bool`` previously
masked progress whenever ANY orthogonal gap (e.g. bundle completeness)
forced it false. The new model splits the invariant into per-domain
sub-invariants so progress in one domain is visible even while another
is broken. ``ArchitectureInvariants.all_held`` preserves the existing
single-bool contract for backwards compatibility.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.architecture_invariants import (
    ArchitectureInvariants,
    legacy_architecture_invariants_held,
)


def test_all_held_when_every_sub_invariant_true():
    inv = ArchitectureInvariants(
        rca_invariants_held=True,
        lever_lattice_invariants_held=True,
        bundle_completeness_invariants_held=True,
    )
    assert inv.all_held is True


def test_all_held_false_when_any_sub_invariant_false():
    for falsified in (
        {"rca_invariants_held": False},
        {"lever_lattice_invariants_held": False},
        {"bundle_completeness_invariants_held": False},
    ):
        inv = ArchitectureInvariants(
            rca_invariants_held=True,
            lever_lattice_invariants_held=True,
            bundle_completeness_invariants_held=True,
            **falsified,
        )
        assert inv.all_held is False


def test_legacy_helper_matches_all_held():
    inv = ArchitectureInvariants(
        rca_invariants_held=True,
        lever_lattice_invariants_held=False,  # the W28.4 state
        bundle_completeness_invariants_held=False,
    )
    assert legacy_architecture_invariants_held(inv) == inv.all_held
    assert legacy_architecture_invariants_held(inv) is False


def test_model_round_trip():
    inv = ArchitectureInvariants(
        rca_invariants_held=True,
        lever_lattice_invariants_held=False,
        bundle_completeness_invariants_held=True,
    )
    blob = inv.model_dump()
    rebuilt = ArchitectureInvariants.model_validate(blob)
    assert rebuilt == inv


def test_postmortem_section_renders_each_sub_invariant():
    from genie_space_optimizer.optimization.architecture_invariants import (
        render_postmortem_section,
    )
    inv = ArchitectureInvariants(
        rca_invariants_held=True,
        lever_lattice_invariants_held=False,
        bundle_completeness_invariants_held=False,
    )
    section = render_postmortem_section(inv)
    assert "rca_invariants_held = true" in section
    assert "lever_lattice_invariants_held = false" in section
    assert "bundle_completeness_invariants_held = false" in section
    assert "architecture_invariants_held = false" in section  # backwards-compat shim
```

- [ ] **Step 2: Run failing tests**

Expected: `ModuleNotFoundError: No module named 'genie_space_optimizer.optimization.architecture_invariants'`

### Task 8.2: implement `architecture_invariants.py`

**Files:**
- Create: `src/genie_space_optimizer/optimization/architecture_invariants.py`

- [ ] **Step 1: Write the implementation**

```python
"""Trial 29 W29.5 — decomposed ArchitectureInvariants typed model.

Splits the monolithic ``architecture_invariants_held: bool`` into
per-domain sub-invariants so progress is visible per domain.

Sub-invariants:

* ``rca_invariants_held`` — RCA canonicaliser (Trial 26 W26.1, Trial 28
  W28.1), kit-for-RCA validator (Trial 24), kit-map coverage (Trial 26
  W26.2). True today after W28.1 deploy.
* ``lever_lattice_invariants_held`` — Stage 3 prompt fits cap (Trial 27
  W27.1), lever loop runs when needed (Trial 27 W27.3 force override),
  inert kit-forced patches re-route to a different structural mechanism
  (Trial 29 W29.1). Will be true after W29.1 deploys.
* ``bundle_completeness_invariants_held`` — postmortem evidence bundle
  is complete (every kit-forced acceptance has a behavior_delta record,
  every inert re-route has a Trial29InertPatchDiagnostic, persistence /
  handoff hops succeed end-to-end). Currently false due to an
  orthogonal infra gap; will be true after Trial 29 W29.2.

``all_held`` is the conjunction (preserving the legacy single-bool
contract for harness reads). :func:`legacy_architecture_invariants_held`
is a free-function alias for the same conjunction so the postmortem
serialiser does not need to reach into the typed model.
"""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class ArchitectureInvariants(BaseModel):
    """Per-domain typed invariant view."""

    model_config = ConfigDict(frozen=True)

    rca_invariants_held: bool
    lever_lattice_invariants_held: bool
    bundle_completeness_invariants_held: bool

    @property
    def all_held(self) -> bool:
        return (
            self.rca_invariants_held
            and self.lever_lattice_invariants_held
            and self.bundle_completeness_invariants_held
        )


def legacy_architecture_invariants_held(inv: ArchitectureInvariants) -> bool:
    """Backwards-compat helper for postmortem serialisers that still
    write the single ``architecture_invariants_held: bool`` field.
    """
    return inv.all_held


def render_postmortem_section(inv: ArchitectureInvariants) -> str:
    """Render the per-domain sub-invariants + the backwards-compat
    aggregate into the postmortem markdown.

    Output format matches the existing
    ``---architectural self-assessment---`` section vocabulary so the
    /goal harness parser keeps working.
    """
    return (
        f"rca_invariants_held = {str(inv.rca_invariants_held).lower()}\n"
        f"lever_lattice_invariants_held = {str(inv.lever_lattice_invariants_held).lower()}\n"
        f"bundle_completeness_invariants_held = {str(inv.bundle_completeness_invariants_held).lower()}\n"
        f"architecture_invariants_held = {str(inv.all_held).lower()}"
    )


__all__ = [
    "ArchitectureInvariants",
    "legacy_architecture_invariants_held",
    "render_postmortem_section",
]
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/test_trial29_architecture_invariants.py -q`
Expected: `5 passed`

- [ ] **Step 3: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/architecture_invariants.py \
        packages/genie-space-optimizer/tests/unit/optimization/test_trial29_architecture_invariants.py
git commit -m "feat(gso): W29.5 — decomposed ArchitectureInvariants typed model + backwards-compat"
```

---

## Phase 9: Integration test + tracker mark + final verification

### Task 9.1: integration replay test — end-to-end with a 7now-shaped payload

**Files:**
- Create: `tests/integration/postmortem_replay/test_trial29_w29_1_kit_forced_inert_reroute_replay.py`

- [ ] **Step 1: Write the failing test**

```python
"""Trial 29 W29.1 — end-to-end replay: kit-forced inert patch routes
through the new lane, history accumulates, diagnostic persists,
Stage 3 prompt of next iteration includes the rejected mechanism.

Uses the existing postmortem-replay test scaffolding to feed a 7now-
shaped (qid=gs_026, rca=plural_top_n_collapse, kit_forced=True,
behavioral_diff=unchanged) payload through the SM and assert each
hop.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from genie_space_optimizer.optimization.inert_mechanism_history import (
    InertMechanismHistory,
    harvest_sm_inert_mechanism_history,
    extend_sm_inert_mechanism_history,
)
from genie_space_optimizer.optimization.inert_patch_diagnostic import (
    Trial29InertPatchDiagnostic,
    persist_inert_patch_diagnostic,
    load_inert_patch_diagnostics,
)
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
)
from genie_space_optimizer.optimization.state_machine.transformers import (
    acceptance_gate,
)
from genie_space_optimizer.optimization.stages.synthesize import (
    render_inert_mechanism_history_section,
)


def _make_kit_forced_inert_state(*, qid: str, rca_kind: str, mechanism: str):
    state = MagicMock()
    state.qid = qid
    state.evaluated.behavioral_diff = "unchanged"
    state.evaluated.target_fixed = False
    state.evaluated.pre_apply_score = 0.0
    state.evaluated.post_apply_score = 0.0
    state.diagnosed.rca_kind_label = rca_kind
    state.applied.kit_forced = True
    state.applied.selected_lever = mechanism
    state.applied.patch_type = "filter"
    return state


def test_kit_forced_inert_reroute_end_to_end(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("GSO_TRIAL29_BEHAVIOR_DELTA", raising=False)
    monkeypatch.delenv("GSO_TRIAL29_INERT_REROUTE", raising=False)

    # Iteration 1: kit-forced patch with add_sql_snippet_filter, inert.
    state_iter1 = _make_kit_forced_inert_state(
        qid="gs_026",
        rca_kind="plural_top_n_collapse",
        mechanism="add_sql_snippet_filter",
    )
    record_iter1 = acceptance_gate.decide_for_state(state_iter1)
    assert record_iter1.decision == "kit_forced_inert_reroute"
    assert record_iter1.rejected_mechanism == "add_sql_snippet_filter"

    # Harvest iter1 history.
    history_iter1 = harvest_sm_inert_mechanism_history(
        [record_iter1],
        qid_rca_pairs=[("gs_026", "plural_top_n_collapse")],
    )
    assert len(history_iter1) == 1
    assert history_iter1[0].rejected_mechanisms == ("add_sql_snippet_filter",)

    # Persist diagnostic.
    bundle_dir = tmp_path / "postmortem_bundle"
    diagnostic_iter1 = Trial29InertPatchDiagnostic(
        qid="gs_026",
        rca_kind="plural_top_n_collapse",
        rejected_mechanism="add_sql_snippet_filter",
        patch_json={"mechanism": "add_sql_snippet_filter"},
        pre_arbiter_score=0.0,
        post_arbiter_score=0.0,
        behavioral_diff="unchanged",
        signature=record_iter1.insufficient_repair_signature,
        iteration=1,
        trial="trial29",
    )
    persist_inert_patch_diagnostic(diagnostic_iter1, bundle_dir=bundle_dir)

    # Render the Stage 3 prompt section for iter 2 — must include
    # the rejected mechanism so the LLM avoids it.
    prompt_section = render_inert_mechanism_history_section(history_iter1)
    assert "gs_026" in prompt_section
    assert "plural_top_n_collapse" in prompt_section
    assert "add_sql_snippet_filter" in prompt_section
    assert (
        "avoid" in prompt_section.lower()
        or "do not" in prompt_section.lower()
        or "must not" in prompt_section.lower()
    )

    # Iteration 2: LLM (mocked) picks a different mechanism, also inert.
    state_iter2 = _make_kit_forced_inert_state(
        qid="gs_026",
        rca_kind="plural_top_n_collapse",
        mechanism="replace_join",
    )
    record_iter2 = acceptance_gate.decide_for_state(state_iter2)
    assert record_iter2.rejected_mechanism == "replace_join"

    # Harvest iter2 and extend.
    history_iter2 = harvest_sm_inert_mechanism_history(
        [record_iter2],
        qid_rca_pairs=[("gs_026", "plural_top_n_collapse")],
    )
    cumulative = extend_sm_inert_mechanism_history(history_iter1, history_iter2)
    assert len(cumulative) == 1
    assert cumulative[0].rejected_mechanisms == ("add_sql_snippet_filter", "replace_join")

    # Persist iter2 diagnostic and verify file accumulates.
    diagnostic_iter2 = Trial29InertPatchDiagnostic(
        qid="gs_026",
        rca_kind="plural_top_n_collapse",
        rejected_mechanism="replace_join",
        patch_json={"mechanism": "replace_join"},
        pre_arbiter_score=0.0,
        post_arbiter_score=0.0,
        behavioral_diff="unchanged",
        signature=record_iter2.insufficient_repair_signature,
        iteration=2,
        trial="trial29",
    )
    persist_inert_patch_diagnostic(diagnostic_iter2, bundle_dir=bundle_dir)

    loaded = load_inert_patch_diagnostics(bundle_dir)
    assert len(loaded) == 2
    assert loaded[0].rejected_mechanism == "add_sql_snippet_filter"
    assert loaded[1].rejected_mechanism == "replace_join"
```

- [ ] **Step 2: Run the integration test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/integration/postmortem_replay/test_trial29_w29_1_kit_forced_inert_reroute_replay.py -q`
Expected: `1 passed` (everything from Phases 1–8 already landed).

- [ ] **Step 3: Run the FULL pretrial gate to confirm zero regressions**

Run: `bash packages/genie-space-optimizer/scripts/pretrial_gate.sh`
Expected: `PASS — 7/7 offline checks green`

- [ ] **Step 4: Run the full Trial 29 unit + integration cohort**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/optimization/ tests/unit/stages/ tests/integration/postmortem_replay/ -q -k trial29 --tb=no`
Expected: every Trial 29 test passes; total = 7 (flags) + 8 (reroute) + 7 (history) + 3 (prompt) + 3 (diagnostic) + 5 (invariants) + 1 (integration) = **34 passing**.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/tests/integration/postmortem_replay/test_trial29_w29_1_kit_forced_inert_reroute_replay.py
git commit -m "test(gso): W29.1 — end-to-end replay (kit-forced inert reroute → history → diagnostic)"
```

### Task 9.2: mark W29.1 + W29.5 done in the tracker

**Files:**
- Modify: `docs/architecture/lever-loop-iteration-tracker.md`

- [ ] **Step 1: Update the Trial 29 section**

Find the line `- [ ] W29.1 — post-apply behaviour gate + structural-lever routing for kit-forced RCAs (offline: inert patch → structural re-route; behaviour gate rejects unchanged)` and replace with:

```markdown
- [x] W29.1 — **offline-complete.** New `kit_forced_inert_reroute` acceptance lane lands (parallel to `kept_insufficient`), gated by sub-flag `GSO_TRIAL29_INERT_REROUTE` (default ON under master `GSO_TRIAL29_BEHAVIOR_DELTA`). `InertMechanismHistory` Pydantic accumulator threads through `TransformerContext` + `cluster_batch` into the Stage 3 prompt's new `render_inert_mechanism_history_section` so the LLM picks from `_structural_fix_mechanisms(rca) - rejected`. `Trial29InertPatchDiagnostic` typed record persists into the postmortem bundle as JSONL. Decomposed `ArchitectureInvariants` typed model splits the monolithic invariant into RCA / lever-lattice / bundle-completeness sub-invariants with `all_held` backwards-compat shim. 34 new tests across 6 test files; pretrial gate exit 0; full suite unchanged (22 pre-existing failures on `feat/gso-cycle13` HEAD not introduced). Live `behavioral_diff != "unchanged"` still to be confirmed by W29.4 replay.
```

- [ ] **Step 2: Commit the tracker update**

```bash
git add packages/genie-space-optimizer/docs/architecture/lever-loop-iteration-tracker.md
git commit -m "docs(gso): mark W29.1 + W29.5 offline-complete in tracker"
```

### Task 9.3: full suite regression check + final sign-off

- [ ] **Step 1: Run the full GSO unit suite (ignore the pre-existing collection error)**

Run: `cd packages/genie-space-optimizer && uv run pytest -q --tb=no --ignore=tests/replay/test_ccf1d60d_safe_subset_isolation.py 2>&1 | tail -5`
Expected: pass count INCREASES by 34 (the new Trial 29 tests); failed count unchanged at 22 (the pre-existing baseline).

- [ ] **Step 2: Verify `check_invariants` shows no NEW violations**

Run: `bash packages/genie-space-optimizer/scripts/check_invariants.sh 2>&1 | grep -cE 'BLOCK|FAIL'`
Expected: same count as the pre-Trial-29 baseline (only the existing `optimizer.py` + `diagnose_llm.py` violations).

- [ ] **Step 3: Verify `forbid_legacy_imports.sh` exit 0**

Run: `bash packages/genie-space-optimizer/scripts/forbid_legacy_imports.sh; echo "exit=$?"`
Expected: `exit=0`

- [ ] **Step 4: Final commit — log roll-up (no code)**

This is the natural checkpoint to pause and let the user review the work before deploying.

---

## Self-Review

**Spec coverage check** (against `2026-06-07-trial29-w29-1-inert-patch-reroute-design.md`):

| Spec section | Task(s) that implement it |
|---|---|
| §"New typed acceptance lane: KIT_FORCED_INERT_PATCH_REROUTE" | Tasks 2.1, 2.2, 4.1, 4.2 |
| §"Lever-loop feedback channel: inert_mechanism_history" | Tasks 3.1, 3.2, 5.1 |
| §"Typed diagnostic record: Trial29InertPatchDiagnostic" | Tasks 7.1, 7.2 |
| §"Feature-flag matrix" | Tasks 1.1, 1.2 |
| §"Coupled change: invariant decomposition (W29.5)" | Tasks 8.1, 8.2 |
| §"Test surface (TDD order)" | Tasks 1.1, 2.1, 3.1, 5.1, 6.1, 7.1, 8.1, 9.1 |
| §"Module owners" | All NEW/MODIFIED paths covered by tasks |
| §"Acceptance evidence for W29.4 live verification" | Out of scope (post-deploy verification, not offline TDD) |
| §"Risk register: LLM ignores history" | Mitigation cited in §"Stage 3 prompt" Task 6.2 (renderer's explicit AVOID instruction); production validator deferred to Trial 30 if needed. |
| §"Risk register: re-routing burns iterations" | Mitigation cited (diagnostics expose this — deferred to evidence-driven W29 follow-up). |
| §"Risk register: backwards compat" | Test 8.1 `test_legacy_helper_matches_all_held` covers it. |
| §"Risk register: other inert lanes" | Test 4.1 cohort cells (kit_forced × unchanged) cover it. |

No spec sections without tasks.

**Placeholder scan:**
- No "TBD", "TODO", or "fill in later" anywhere in the plan.
- One known site of "may need to look up the actual function name" — Task 4.1's `acceptance_gate.decide_for_state` import. The Task 4.2 step 1 includes a `rg` command to confirm the actual entry-point name.
- One known site of "(actual assembly site may vary)" — Task 6.2 step 3. Includes the `rg` command to locate.

These two instances are unavoidable contextual lookups (the harness code is 38,503 lines and `acceptance_gate.py` is 497 lines — naming details are not memorisable). Acceptable per the spirit of "complete code in every step": the plan includes the lookup command + the exact pattern to find.

**Type consistency:**
- `InertMechanismHistory.qid: str` — consistent across model definition (Task 3.2), harvest function (Task 3.2), context field (Task 5.1), prompt renderer (Task 6.2), integration test (Task 9.1).
- `InertMechanismHistory.rca_kind: str` — same.
- `InertMechanismHistory.rejected_mechanisms: tuple[str, ...]` — same.
- `Trial29InertPatchDiagnostic.behavioral_diff: str` — consistent with `BehavioralDiff = Literal["unchanged", "partial", "matches_expected"]` (we accept str for forward compatibility; the lane only fires on "unchanged" so the value is always "unchanged" in practice, but the field is loose to avoid coupling diagnostic shape to the SM literal).
- `AcceptanceDecisionRecord.rejected_mechanism: str = ""` — empty default everywhere; only populated by the new lane (Task 2.2).
- `ArchitectureInvariants.all_held: bool` — property (read-only) everywhere it's used (Task 8.2).

No type drift.

## Execution Handoff

Plan complete and saved to `packages/genie-space-optimizer/docs/architecture/2026-06-07-trial29-w29-1-implementation-plan.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per phase, review between phases, fast iteration. Uses `superpowers:subagent-driven-development`.

**2. Inline Execution** — Execute tasks in this session using `superpowers:executing-plans`, batch execution with checkpoints for review.

**Which approach?**
