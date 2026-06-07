# Trial 30 W30.1 + W30.2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the W29.1 inert-patch *detection* into an actual mechanism *switch* by (a) wiring the W29.1 feedback channel that was implemented-but-never-connected, (b) adding a deterministic post-LLM enforcement guard that hard-drops a re-emitted rejected mechanism when a structural fallback exists, and (c) carrying the rerouted QID forward so it can't be dropped or re-routed to the same patch family.

**Architecture:** Mirror the proven `kept_insufficient` cumulative-learning path one-for-one for the inert channel (harvest at `harness.py:~20712` → cross-iter accumulator → `TransformerContext` → `_build_request` prompt section). Add the enforcement guard in the post-loop binding block of `synthesize.py` (~2382), next to its architectural sibling D3, where the full per-QID proposal slate is in hand. Two-tier feature flags mirror Trial 29.

**Tech Stack:** Python 3.11, Pydantic v2 (`BaseModel`, frozen), pytest. GSO state machine + Stage 3 synthesis. Reference design: `docs/architecture/2026-06-07-trial30-enforced-mechanism-switch-design.md`.

**Working directory for all `pytest` / `git` commands:** `packages/genie-space-optimizer/`

---

## Task 1: Trial 30 feature flags

**Files:**
- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/trial30_flags.py`
- Test: `packages/genie-space-optimizer/tests/unit/optimization/test_trial30_flags.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/optimization/test_trial30_flags.py
import importlib

import genie_space_optimizer.optimization.trial30_flags as flags


def _reload(monkeypatch, env: dict[str, str]):
    for k in (
        "GSO_TRIAL30_ENFORCED_SWITCH",
        "GSO_TRIAL30_INERT_HARVEST_WIRE",
        "GSO_TRIAL30_ENFORCE_GUARD",
    ):
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    return importlib.reload(flags)


def test_master_default_on(monkeypatch):
    m = _reload(monkeypatch, {})
    assert m.trial30_enforced_switch_enabled() is True


def test_master_opt_out(monkeypatch):
    m = _reload(monkeypatch, {"GSO_TRIAL30_ENFORCED_SWITCH": "0"})
    assert m.trial30_enforced_switch_enabled() is False


def test_subflags_default_on_when_master_on(monkeypatch):
    m = _reload(monkeypatch, {})
    assert m.trial30_inert_harvest_wire_enabled() is True
    assert m.trial30_enforce_guard_enabled() is True


def test_subflags_forced_off_when_master_off(monkeypatch):
    m = _reload(
        monkeypatch,
        {
            "GSO_TRIAL30_ENFORCED_SWITCH": "off",
            "GSO_TRIAL30_INERT_HARVEST_WIRE": "1",
            "GSO_TRIAL30_ENFORCE_GUARD": "1",
        },
    )
    assert m.trial30_inert_harvest_wire_enabled() is False
    assert m.trial30_enforce_guard_enabled() is False


def test_guard_subflag_independent_opt_out(monkeypatch):
    m = _reload(monkeypatch, {"GSO_TRIAL30_ENFORCE_GUARD": "false"})
    assert m.trial30_inert_harvest_wire_enabled() is True
    assert m.trial30_enforce_guard_enabled() is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/optimization/test_trial30_flags.py -v`
Expected: FAIL with `ModuleNotFoundError` / `AttributeError` (functions not defined).

- [ ] **Step 3: Write minimal implementation** (mirror `trial29_flags.py`)

```python
# src/genie_space_optimizer/optimization/trial30_flags.py
"""Trial 30 — enforced inert-mechanism switch + rerouted-QID carry-forward.

Mirror of :mod:`trial29_flags`. Same default-ON / OFF-vocabulary
semantics. Single emergency rollback knob is
``GSO_TRIAL30_ENFORCED_SWITCH``.

Trial 30 closes the W29.4 PARTIAL: the ``kit_forced_inert_reroute``
lane fires live (detection works) but the W29.1 feedback channel was
never wired into production, so the LLM re-emitted the rejected
mechanism. W30.1a wires the channel; W30.1b adds a deterministic
post-LLM enforcement guard; W30.2 carries the rerouted QID forward.

Opt-out semantics (default ON): any of ``0`` / ``false`` / ``no`` /
``off`` (case insensitive) disables. Env unset, empty, or any other
value enables.
"""
from __future__ import annotations

import os

_TRIAL30_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL30_FLAG_OFF_VALUES


def trial30_enforced_switch_enabled() -> bool:
    """Trial 30 master flag. Default ON.

    Opt out with ``export GSO_TRIAL30_ENFORCED_SWITCH=0`` for emergency
    rollback. When OFF, every Trial 30 sub-flag is forced OFF
    regardless of its own env var (byte-stable rollback to Trial 29).
    """
    return _flag_enabled("GSO_TRIAL30_ENFORCED_SWITCH")


def _subflag_opt_out(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    off = raw in _TRIAL30_FLAG_OFF_VALUES
    return trial30_enforced_switch_enabled() and not off


def trial30_inert_harvest_wire_enabled() -> bool:
    """W30.1a + W30.2(a)/(c) — wire the InertMechanismHistory channel
    (harvest -> ctx -> prompt render), union member_qids into
    target_qids_union, and write the same-iteration live bucket.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL30_INERT_HARVEST_WIRE=0``.
    """
    return _subflag_opt_out("GSO_TRIAL30_INERT_HARVEST_WIRE")


def trial30_enforce_guard_enabled() -> bool:
    """W30.1b — deterministic post-LLM enforcement guard that hard-drops
    a re-emitted rejected mechanism when a structural fallback exists.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL30_ENFORCE_GUARD=0`` to disable the hard drop
    independently of the wiring (e.g. if it over-rejects live).
    """
    return _subflag_opt_out("GSO_TRIAL30_ENFORCE_GUARD")


__all__ = [
    "trial30_enforced_switch_enabled",
    "trial30_inert_harvest_wire_enabled",
    "trial30_enforce_guard_enabled",
]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/optimization/test_trial30_flags.py -v`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/trial30_flags.py tests/unit/optimization/test_trial30_flags.py
git commit -m "feat(gso): Trial 30 flags — two-tier enforced-switch + harvest-wire + guard sub-flags"
```

---

## Task 2: Mechanism-normalization helper (lever-id <-> PatchMechanism)

The guard compares a proposal's mechanism against `rejected_mechanism` lever-ids. They live in different vocabularies. Add a single pure helper that normalizes a lever-id string to a `PatchMechanism`, reusing the existing lever->patch_type and patch_type->mechanism maps. Placing it in `rca_mechanism_routing.py` keeps it next to `_structural_fix_mechanisms`.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/rca_mechanism_routing.py`
- Test: `packages/genie-space-optimizer/tests/unit/optimization/test_trial30_mechanism_normalization.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/optimization/test_trial30_mechanism_normalization.py
from genie_space_optimizer.optimization.patch_mechanism import PatchMechanism
from genie_space_optimizer.optimization.rca_mechanism_routing import (
    mechanisms_for_rejected_levers,
)


def test_lever5_family_maps_to_example_sql_or_instruction():
    # lever-5 spans add_instruction + add_example_sql; we expect at
    # least EXAMPLE_SQL in the normalized set (the inert behavior unit).
    out = mechanisms_for_rejected_levers(("lever-5",))
    assert PatchMechanism.EXAMPLE_SQL in out


def test_lever5_aliases_collapse_to_same_mechanism():
    # lever-5 and lever-5a are different lever ids but lever-5a is the
    # instruction variant; normalization must not silently drop them.
    out = mechanisms_for_rejected_levers(("lever-5", "lever-5a"))
    assert PatchMechanism.EXAMPLE_SQL in out
    assert PatchMechanism.INSTRUCTION_TEXT in out


def test_lever6_maps_to_sql_snippet():
    out = mechanisms_for_rejected_levers(("lever-6",))
    assert PatchMechanism.SQL_SNIPPET in out


def test_empty_and_unknown_are_safe():
    assert mechanisms_for_rejected_levers(()) == frozenset()
    assert mechanisms_for_rejected_levers(("lever-999",)) == frozenset()


def test_patch_type_signature_form_normalizes():
    # rejected_mechanism may be the lever-id, but acceptance_gate can
    # also store a patch_type token when the lever was inferred. The
    # helper accepts patch_type wire strings too.
    out = mechanisms_for_rejected_levers(("add_example_sql",))
    assert PatchMechanism.EXAMPLE_SQL in out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/optimization/test_trial30_mechanism_normalization.py -v`
Expected: FAIL with `ImportError: cannot import name 'mechanisms_for_rejected_levers'`.

- [ ] **Step 3: Write minimal implementation**

Add to `rca_mechanism_routing.py` (after `_structural_fix_mechanisms`, ~line 179). Read the existing imports at the top of the file first; add these if absent:

```python
# near the top of rca_mechanism_routing.py, with the other imports
from genie_space_optimizer.optimization.levers_contract import (
    LEVER_TO_PATCH_TYPES,
)
from genie_space_optimizer.optimization.patch_mechanism import (
    PatchMechanism,
    mechanism_for_patch_type,
)
```

```python
def mechanisms_for_rejected_levers(
    rejected: "Iterable[str]",
) -> frozenset[PatchMechanism]:
    """Normalize rejected lever-id / patch_type tokens to PatchMechanism.

    Trial 30 W30.1b. ``AcceptanceDecisionRecord.rejected_mechanism``
    stores a lever-id (``"lever-5"``) — or, when the lever was inferred
    from a patch_type, a patch_type wire token (``"add_example_sql"``).
    The enforcement guard compares on the *behavioral* unit
    (``PatchMechanism``), not the lever-id, so lever-5/5a/5b aliasing
    cannot let a re-emit slip through.

    Returns the union of mechanisms reachable from each token. Unknown
    tokens contribute nothing (empty), so the guard fails open (keeps
    the proposal) rather than mis-dropping on an unrecognised label.
    """
    out: set[PatchMechanism] = set()
    for token in rejected:
        t = str(token or "").strip()
        if not t:
            continue
        # Direct patch_type token form.
        mech = mechanism_for_patch_type(t)
        if mech is not None:
            out.add(mech)
            continue
        # Lever-id form: expand to its patch_types, then to mechanisms.
        for patch_type in LEVER_TO_PATCH_TYPES.get(t, ()):  # type: ignore[arg-type]
            pm = mechanism_for_patch_type(str(patch_type))
            if pm is not None:
                out.add(pm)
    return frozenset(out)
```

Add `"mechanisms_for_rejected_levers"` to the module `__all__` if one exists. If `Iterable` is not already imported, add `from typing import Iterable`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/optimization/test_trial30_mechanism_normalization.py -v`
Expected: PASS (5 passed). If `test_lever5_family_maps_to_example_sql_or_instruction` fails because `LEVER_TO_PATCH_TYPES["lever-5"]` does not contain `add_example_sql`, read `levers_contract.py:LEVER_TO_PATCH_TYPES` and adjust the test's expected mechanism to whatever lever-5 actually spans — do NOT change the lever map.

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/rca_mechanism_routing.py tests/unit/optimization/test_trial30_mechanism_normalization.py
git commit -m "feat(gso): Trial 30 W30.1b — mechanisms_for_rejected_levers normalization helper"
```

---

## Task 3: W30.1a — harvest the inert-mechanism history in the harness

Wire the harvest sibling next to `harvest_sm_insufficient_repair_signatures` at `harness.py:~20712`. The accumulator must be initialized once before the iteration loop. The `qid_rca_pairs` come from the final states positionally paired with their `AcceptanceDecisionRecord`s — read how `_sm_final_states` is built and how the insufficient sibling derives its inputs.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` (accumulator init + harvest block ~20712)
- Test: `packages/genie-space-optimizer/tests/unit/optimization/test_trial30_inert_harvest_wire.py`

- [ ] **Step 1: Write the failing test** (unit-level harvest contract, no full harness)

```python
# tests/unit/optimization/test_trial30_inert_harvest_wire.py
from genie_space_optimizer.optimization.inert_mechanism_history import (
    InertMechanismHistory,
    extend_sm_inert_mechanism_history,
    harvest_sm_inert_mechanism_history,
)
from genie_space_optimizer.optimization.state_machine.records import (
    AcceptanceDecisionRecord,
)


def _reroute_record(rejected: str) -> AcceptanceDecisionRecord:
    return AcceptanceDecisionRecord(
        decision="kit_forced_inert_reroute",
        target_fixed=False,
        behavioral_diff="unchanged",
        insufficient_repair_signature=(
            f"{rejected}:add_example_sql:kit_forced_inert:"
            "rca=top_n_cardinality_collapse:behavior=unchanged"
        ),
        rejected_mechanism=rejected,
    )


def test_harvest_then_extend_accumulates_across_iterations():
    rec = _reroute_record("lever-5")
    fresh = harvest_sm_inert_mechanism_history(
        [rec], qid_rca_pairs=[("gs_026", "top_n_cardinality_collapse")]
    )
    assert len(fresh) == 1
    assert fresh[0].qid == "gs_026"
    assert fresh[0].rejected_mechanisms == ("lever-5",)

    # Second iteration rejects a different mechanism for the same pair.
    rec2 = _reroute_record("lever-1")
    fresh2 = harvest_sm_inert_mechanism_history(
        [rec2], qid_rca_pairs=[("gs_026", "top_n_cardinality_collapse")]
    )
    merged = extend_sm_inert_mechanism_history(fresh, fresh2)
    assert len(merged) == 1
    assert merged[0].rejected_mechanisms == ("lever-5", "lever-1")


def test_non_reroute_records_do_not_contribute():
    rec = AcceptanceDecisionRecord(decision="accepted", target_fixed=True)
    fresh = harvest_sm_inert_mechanism_history(
        [rec], qid_rca_pairs=[("gs_001", "wrong_column")]
    )
    assert fresh == ()
```

- [ ] **Step 2: Run test to verify it passes immediately**

Run: `pytest tests/unit/optimization/test_trial30_inert_harvest_wire.py -v`
Expected: PASS — these helpers already exist (W29.1). This test PINS the harvest contract the harness wiring depends on. (If it fails, the W29.1 helpers regressed — fix those first.)

- [ ] **Step 3: Wire the harness harvest + accumulator init**

First locate the accumulator init for the insufficient sibling. Search:

Run: `grep -n "_sm_insufficient_repair_signatures" src/genie_space_optimizer/optimization/harness.py`

Find where `_sm_insufficient_repair_signatures` is initialized (a `tuple()`/list before the iteration loop). Immediately after it, add the inert accumulator init:

```python
        # Trial 30 W30.1a — cumulative inert-mechanism history. Sibling
        # of ``_sm_insufficient_repair_signatures``. The W29.1 harvest
        # helper existed but was never called; this is the wire-in.
        _sm_inert_mechanism_history: tuple = ()
```

Then in the end-of-iteration harvest block at `harness.py:~20712`, immediately after the existing `extend_sm_insufficient_repair_signatures(...)` call (after line ~20722, inside the same `try`/`except` is acceptable, but use a SEPARATE try/except so an inert-harvest failure never suppresses the insufficient harvest):

```python
            # Trial 30 W30.1a — harvest the inert-mechanism history for
            # the cross-iteration feedback channel. Mirrors the
            # insufficient-signature harvest above. Gated by
            # GSO_TRIAL30_INERT_HARVEST_WIRE; channel-skip is non-fatal.
            try:
                from genie_space_optimizer.optimization.trial30_flags import (
                    trial30_inert_harvest_wire_enabled,
                )
                if trial30_inert_harvest_wire_enabled():
                    from genie_space_optimizer.optimization.inert_mechanism_history import (
                        extend_sm_inert_mechanism_history,
                        harvest_sm_inert_mechanism_history,
                    )
                    _t30_pairs = _inert_qid_rca_pairs_from_states(
                        _sm_final_states
                    )
                    _sm_inert_mechanism_history = (
                        extend_sm_inert_mechanism_history(
                            _sm_inert_mechanism_history,
                            harvest_sm_inert_mechanism_history(
                                _t30_acceptance_records_from_states(
                                    _sm_final_states
                                ),
                                qid_rca_pairs=_t30_pairs,
                            ),
                        )
                    )
            except Exception:
                logger.debug(
                    "Trial 30 inert_mechanism_history harvest failed; "
                    "feedback channel skipped this iteration",
                    exc_info=True,
                )
```

This references two small extractors. Read how `_sm_final_states` is shaped first:

Run: `grep -n "_sm_final_states" src/genie_space_optimizer/optimization/harness.py | head -20`

`_sm_final_states` is a sequence of per-QID state objects. Each state carries the QID, the canonical `rca_kind`, and the acceptance record(s). Define these two module-level pure helpers near the top of `harness.py` (or in `inert_mechanism_history.py` if the state type is importable there — prefer harness-local to avoid an import cycle, since states are a harness concept). Read one `_sm_final_states` element's attributes via the existing insufficient harvest (`harvest_sm_insufficient_repair_signatures` in `forbidden_signatures.py:125`) to copy the exact attribute access pattern, then implement:

```python
def _t30_acceptance_records_from_states(states):
    """Trial 30 — positional AcceptanceDecisionRecord stream from final
    SM states (one record per state, matching _inert_qid_rca_pairs)."""
    out = []
    for st in states or ():
        rec = getattr(st, "acceptance_record", None)
        if rec is None:
            # match the attribute name used by the insufficient harvest
            rec = getattr(st, "decision_record", None)
        out.append(rec)
    return out


def _inert_qid_rca_pairs_from_states(states):
    """Trial 30 — (qid, rca_kind) pairs positionally paired with
    _t30_acceptance_records_from_states."""
    pairs = []
    for st in states or ():
        qid = str(getattr(st, "qid", "") or getattr(st, "question_id", ""))
        rca = str(
            getattr(st, "rca_kind", "")
            or getattr(st, "canonical_rca_kind", "")
        )
        pairs.append((qid, rca))
    return pairs
```

IMPORTANT: replace the `getattr` attribute names above with the ACTUAL attribute names on the SM state object — copy them from `harvest_sm_insufficient_repair_signatures` in `forbidden_signatures.py`, which already reads records + qid/rca off the same `states`. Do not guess; the harvest helper is the source of truth for these names. The `harvest_sm_inert_mechanism_history` contract requires `records` and `qid_rca_pairs` to be the same length and positionally aligned — both extractors iterate `states` in the same order, so this holds.

- [ ] **Step 4: Verify no harness import/syntax regression**

Run: `python -c "import genie_space_optimizer.optimization.harness"`
Expected: no output (clean import). Then run a fast harness smoke test:
Run: `pytest tests/unit/optimization/test_trial30_inert_harvest_wire.py -v`
Expected: PASS (contract still green).

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/harness.py tests/unit/optimization/test_trial30_inert_harvest_wire.py
git commit -m "feat(gso): Trial 30 W30.1a — harvest inert_mechanism_history in harness (wire the W29.1 channel)"
```

---

## Task 4: W30.1a — thread history through TransformerContext to synthesis

The field already exists on `TransformerContext` (`verdict.py:84`) and `Stage2BatchInput` (`cluster_batch.py:52`). This task SETS it in `optimizer.py` and passes it through `build_stage2_batch_input` and into `run_plan11_synthesis_for_single_cluster` -> `_build_request`, then renders it.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py` (~239-319 ctx construction)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state_machine/transformers/cluster_batch.py:308-314` (`build_stage2_batch_input` pass-through)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/synthesize_llm.py` (~531-560 call site)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/synthesize.py` (`run_plan11_synthesis_for_single_cluster` kwarg ~852; `_build_request` render ~681)
- Test: `packages/genie-space-optimizer/tests/unit/stages/test_trial30_synthesis_inert_history_threaded.py`

- [ ] **Step 1: Write the failing test** (prompt renders the section when history present)

```python
# tests/unit/stages/test_trial30_synthesis_inert_history_threaded.py
from genie_space_optimizer.optimization.inert_mechanism_history import (
    InertMechanismHistory,
)
from genie_space_optimizer.optimization.stages.synthesize import (
    render_inert_mechanism_history_section,
)


def test_render_lists_rejected_mechanisms_per_qid():
    history = (
        InertMechanismHistory(
            qid="gs_026",
            rca_kind="top_n_cardinality_collapse",
            rejected_mechanisms=("lever-5",),
        ),
    )
    section = render_inert_mechanism_history_section(history)
    assert "gs_026" in section
    assert "top_n_cardinality_collapse" in section
    assert "lever-5" in section


def test_render_empty_history_is_blank():
    assert render_inert_mechanism_history_section(()) == ""
```

Then add a threading assertion test (pins that the kwarg exists end to end):

```python
def test_build_request_accepts_inert_history_kwarg():
    import inspect

    from genie_space_optimizer.optimization.stages import synthesize

    sig = inspect.signature(synthesize._build_request)
    assert "inert_mechanism_history" in sig.parameters

    sig2 = inspect.signature(
        synthesize.run_plan11_synthesis_for_single_cluster
    )
    assert "inert_mechanism_history" in sig2.parameters
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/stages/test_trial30_synthesis_inert_history_threaded.py -v`
Expected: the two `render_*` tests PASS (helper exists from W29.1); `test_build_request_accepts_inert_history_kwarg` FAILS (kwarg not yet added).

- [ ] **Step 3: Add the kwarg + render call**

(3a) In `synthesize.py`, add `inert_mechanism_history: tuple = ()` as a kwarg to `run_plan11_synthesis_for_single_cluster` (~852) and to `_build_request` (~237). Read both signatures first to place the new kwarg after the existing `insufficient_repair_signatures` parameter for consistency.

(3b) In `_build_request`, immediately after the `insufficient_repair_signatures` payload key (~681), render the section. The existing payload is a dict `_user_payload`; add a sibling key only when non-empty so the prompt stays byte-stable for clusters with no history:

```python
            # Trial 30 W30.1a — inert-mechanism feedback. Distinct from
            # insufficient_repair_signatures: these are mechanisms the
            # acceptance gate proved behaviorally inert on a kit-forced
            # RCA (behavioral_diff=unchanged). The prompt instructs the
            # LLM to pick from _structural_fix_mechanisms(rca) MINUS
            # these. Threaded from TransformerContext.inert_mechanism_history.
            "inert_mechanism_history": (
                render_inert_mechanism_history_section(inert_mechanism_history)
                if inert_mechanism_history
                else ""
            ),
```

(3c) Pass it through `run_plan11_synthesis_for_single_cluster` -> `_build_request` (add `inert_mechanism_history=inert_mechanism_history` to the internal `_build_request(...)` call).

(3d) In `synthesize_llm.py` (~541, where `insufficient_repair_signatures=_live_insufficient_repair_signatures(ctx)` is passed to `run_plan11_synthesis_for_single_cluster`), add:

```python
            inert_mechanism_history=getattr(
                ctx, "inert_mechanism_history", ()
            ),
```

(3e) In `cluster_batch.py:build_stage2_batch_input` (~308-314), the `inert_mechanism_history` parameter exists on the function (line 60) but is omitted from the `Stage2BatchInput(...)` constructor call. Add `inert_mechanism_history=inert_mechanism_history,` to that constructor (it is already a field at line 52/77 — confirm and wire the call site).

(3f) In `optimizer.py` (~239-319 where `TransformerContext(...)` is built), add `inert_mechanism_history=_sm_inert_mechanism_history,` — but `_sm_inert_mechanism_history` lives in `harness.py`. Read how `optimizer.py` receives the cross-iteration insufficient accumulator (`insufficient_repair_signatures=`) and thread the inert accumulator through the SAME parameter path (the harness passes it into the optimizer/SM dispatch call). Mirror that exact plumbing; do not invent a new path.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/stages/test_trial30_synthesis_inert_history_threaded.py -v`
Expected: PASS (3 passed).
Then: `python -c "import genie_space_optimizer.optimization.optimizer; import genie_space_optimizer.optimization.stages.synthesize_llm"`
Expected: clean import.

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/optimizer.py src/genie_space_optimizer/optimization/state_machine/transformers/cluster_batch.py src/genie_space_optimizer/optimization/stages/synthesize_llm.py src/genie_space_optimizer/optimization/stages/synthesize.py tests/unit/stages/test_trial30_synthesis_inert_history_threaded.py
git commit -m "feat(gso): Trial 30 W30.1a — thread inert_mechanism_history ctx -> Stage 3 prompt"
```

---

## Task 5: W30.1b — deterministic enforcement guard (post-loop)

Add the guard in the post-loop binding block of `synthesize.py` (~2382), after the C5 binding filter and before/alongside the W4 routing block. It needs: the surviving `proposals` list, each proposal's `rca_kind` + mechanism, and the `inert_mechanism_history` (now threaded in Task 4). Implement the guard as a pure helper for unit-testability, then call it.

**Files:**
- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/enforced_mechanism_switch.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/synthesize.py` (~2382 post-loop, call the helper)
- Test: `packages/genie-space-optimizer/tests/unit/optimization/test_trial30_enforced_switch_guard.py`

- [ ] **Step 1: Write the failing test** (pure helper)

```python
# tests/unit/optimization/test_trial30_enforced_switch_guard.py
from dataclasses import dataclass

from genie_space_optimizer.optimization.enforced_mechanism_switch import (
    EnforcedSwitchOutcome,
    enforced_switch_survivors,
)
from genie_space_optimizer.optimization.inert_mechanism_history import (
    InertMechanismHistory,
)
from genie_space_optimizer.optimization.patch_mechanism import PatchMechanism


@dataclass
class _Prop:
    intent_id: str
    qid: str
    rca_kind: str
    mechanism: PatchMechanism


def _hist(qid, rca, rejected):
    return InertMechanismHistory(
        qid=qid, rca_kind=rca, rejected_mechanisms=tuple(rejected)
    )


def test_drops_reemit_when_fallback_exists_in_slate():
    # gs_026 / top_n: lever-5 (EXAMPLE_SQL) rejected; slate has both an
    # EXAMPLE_SQL re-emit AND a SQL_SNIPPET fallback -> drop the re-emit.
    props = [
        _Prop("p1", "gs_026", "top_n_cardinality_collapse",
              PatchMechanism.EXAMPLE_SQL),
        _Prop("p2", "gs_026", "top_n_cardinality_collapse",
              PatchMechanism.SQL_SNIPPET),
    ]
    history = (_hist("gs_026", "top_n_cardinality_collapse", ("lever-5",)),)
    outcome = enforced_switch_survivors(props, history)
    assert isinstance(outcome, EnforcedSwitchOutcome)
    survivors = {p.intent_id for p in outcome.survivors}
    assert survivors == {"p2"}
    assert outcome.dropped[0].intent_id == "p1"
    assert outcome.dropped_reasons["p1"].startswith("GSO_TRIAL30_ENFORCED_SWITCH")


def test_keeps_reemit_when_no_fallback_in_slate():
    # Only the re-emitted EXAMPLE_SQL is present; no structural fallback
    # survived -> keep it, emit NO_FALLBACK_AVAILABLE (never zero out).
    props = [
        _Prop("p1", "gs_026", "top_n_cardinality_collapse",
              PatchMechanism.EXAMPLE_SQL),
    ]
    history = (_hist("gs_026", "top_n_cardinality_collapse", ("lever-5",)),)
    outcome = enforced_switch_survivors(props, history)
    assert {p.intent_id for p in outcome.survivors} == {"p1"}
    assert outcome.dropped == []
    assert outcome.no_fallback_qids == ["gs_026"]


def test_novel_mechanism_untouched():
    props = [
        _Prop("p1", "gs_026", "top_n_cardinality_collapse",
              PatchMechanism.SQL_SNIPPET),
    ]
    history = (_hist("gs_026", "top_n_cardinality_collapse", ("lever-5",)),)
    outcome = enforced_switch_survivors(props, history)
    assert {p.intent_id for p in outcome.survivors} == {"p1"}
    assert outcome.dropped == []
    assert outcome.no_fallback_qids == []


def test_lever_alias_caught_via_enum_normalization():
    # rejected stored as lever-5 (EXAMPLE_SQL); a re-emit labelled with a
    # different lever-id that still maps to EXAMPLE_SQL must be caught.
    props = [
        _Prop("p1", "gs_026", "top_n_cardinality_collapse",
              PatchMechanism.EXAMPLE_SQL),
        _Prop("p2", "gs_026", "top_n_cardinality_collapse",
              PatchMechanism.METADATA_DESCRIPTION),
    ]
    history = (_hist("gs_026", "top_n_cardinality_collapse",
                     ("add_example_sql",)),)
    outcome = enforced_switch_survivors(props, history)
    assert {p.intent_id for p in outcome.survivors} == {"p2"}


def test_no_history_is_identity():
    props = [
        _Prop("p1", "gs_001", "wrong_column", PatchMechanism.METADATA_DESCRIPTION),
    ]
    outcome = enforced_switch_survivors(props, ())
    assert {p.intent_id for p in outcome.survivors} == {"p1"}
    assert outcome.dropped == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/optimization/test_trial30_enforced_switch_guard.py -v`
Expected: FAIL with `ModuleNotFoundError: enforced_mechanism_switch`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/genie_space_optimizer/optimization/enforced_mechanism_switch.py
"""Trial 30 W30.1b — deterministic enforced inert-mechanism switch.

Post-LLM guard: if a synthesised proposal re-emits a mechanism the
acceptance gate already proved behaviorally inert for its
``(qid, rca_kind)`` (recorded in :class:`InertMechanismHistory`), and a
structurally-distinct fallback mechanism is still present in the same
QID's surviving slate, hard-drop the re-emit. If NO fallback survives,
keep the re-emit and flag ``no_fallback`` — the guard never zeroes out
a QID.

Comparison is on :class:`PatchMechanism` (the behavioral unit), not
lever-id strings, so lever-5/5a/5b aliasing cannot let a re-emit slip
through. See :func:`mechanisms_for_rejected_levers`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence

from genie_space_optimizer.optimization.patch_mechanism import PatchMechanism
from genie_space_optimizer.optimization.rca_mechanism_routing import (
    _structural_fix_mechanisms,
    mechanisms_for_rejected_levers,
)


@dataclass
class EnforcedSwitchOutcome:
    survivors: list[Any]
    dropped: list[Any] = field(default_factory=list)
    dropped_reasons: dict[str, str] = field(default_factory=dict)
    no_fallback_qids: list[str] = field(default_factory=list)


def _proposal_mechanism(prop: Any) -> PatchMechanism | None:
    mech = getattr(prop, "mechanism", None)
    if isinstance(mech, PatchMechanism):
        return mech
    # Fall back to deriving from patch_type for real RepairProposal objects.
    from genie_space_optimizer.optimization.patch_mechanism import (
        mechanism_for_patch_type,
    )
    patch_type = getattr(prop, "patch_type", None)
    pt_value = getattr(patch_type, "value", patch_type)
    if pt_value:
        return mechanism_for_patch_type(str(pt_value))
    return None


def enforced_switch_survivors(
    proposals: Sequence[Any],
    history: Iterable[Any],
) -> EnforcedSwitchOutcome:
    """Filter ``proposals`` against the inert-mechanism ``history``.

    Pure. ``proposals`` need only expose ``intent_id``, ``qid``,
    ``rca_kind``, and either ``mechanism`` (PatchMechanism) or
    ``patch_type``. Order of survivors is preserved.
    """
    by_pair: dict[tuple[str, str], frozenset[PatchMechanism]] = {}
    for entry in history or ():
        key = (str(entry.qid), str(entry.rca_kind))
        rejected = mechanisms_for_rejected_levers(entry.rejected_mechanisms)
        by_pair[key] = by_pair.get(key, frozenset()) | rejected

    if not by_pair:
        return EnforcedSwitchOutcome(survivors=list(proposals))

    # Per (qid, rca_kind): which mechanisms survive in the slate.
    slate_mechs: dict[tuple[str, str], set[PatchMechanism]] = {}
    for p in proposals:
        key = (str(getattr(p, "qid", "")), str(getattr(p, "rca_kind", "")))
        m = _proposal_mechanism(p)
        if m is not None:
            slate_mechs.setdefault(key, set()).add(m)

    survivors: list[Any] = []
    dropped: list[Any] = []
    dropped_reasons: dict[str, str] = {}
    no_fallback_qids: list[str] = []

    for p in proposals:
        key = (str(getattr(p, "qid", "")), str(getattr(p, "rca_kind", "")))
        rejected = by_pair.get(key)
        mech = _proposal_mechanism(p)
        if not rejected or mech is None or mech not in rejected:
            survivors.append(p)
            continue
        # This proposal re-emits a rejected mechanism. Is there a
        # structurally-distinct fallback present in the surviving slate?
        structural = _structural_fix_mechanisms(key[1])
        available_fallbacks = structural - rejected
        slate_has_fallback = bool(
            (slate_mechs.get(key, set()) & available_fallbacks)
        )
        if slate_has_fallback:
            dropped.append(p)
            chosen = sorted(
                m.value for m in (slate_mechs[key] & available_fallbacks)
            )
            dropped_reasons[getattr(p, "intent_id", "")] = (
                "GSO_TRIAL30_ENFORCED_SWITCH_V1:"
                f"rca={key[1]}:rejected={mech.value}:"
                f"fallback={','.join(chosen)}"
            )
        else:
            survivors.append(p)
            if key[0] not in no_fallback_qids:
                no_fallback_qids.append(key[0])

    return EnforcedSwitchOutcome(
        survivors=survivors,
        dropped=dropped,
        dropped_reasons=dropped_reasons,
        no_fallback_qids=no_fallback_qids,
    )


__all__ = ["EnforcedSwitchOutcome", "enforced_switch_survivors"]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/optimization/test_trial30_enforced_switch_guard.py -v`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/enforced_mechanism_switch.py tests/unit/optimization/test_trial30_enforced_switch_guard.py
git commit -m "feat(gso): Trial 30 W30.1b — enforced_switch_survivors pure guard helper"
```

---

## Task 6: W30.1b — call the guard in synthesize post-loop + emit markers

Wire `enforced_switch_survivors` into `synthesize.py` post-loop (~2382), gated by `trial30_enforce_guard_enabled()`. Emit `emit_patch_outcome(...)` for each drop with `terminal_reason="enforced_inert_mechanism_switch"` and a stdout `GSO_TRIAL30_NO_FALLBACK_AVAILABLE_V1` marker for no-fallback QIDs. Real `RepairProposal` objects don't have `mechanism`/`qid`/`rca_kind` directly — adapt by deriving them (the helper's `_proposal_mechanism` already handles `patch_type`; supply `qid`/`rca_kind` via a thin per-proposal accessor).

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/synthesize.py` (~2382 post-loop)
- Test: `packages/genie-space-optimizer/tests/unit/stages/test_trial30_synthesize_guard_integration.py`

- [ ] **Step 1: Write the failing test**

Read how the existing C5 binding filter test constructs a cluster + proposals (`tests/unit/test_mechanism_binding.py` or `tests/workbench/test_mechanism_binding_trigger_replay.py`) and reuse that fixture shape. The test must: build a synthesized slate for one QID with an EXAMPLE_SQL re-emit + a SQL_SNIPPET fallback, set `ctx.inert_mechanism_history` with `lever-5` rejected for that `(qid, top_n_cardinality_collapse)`, run `run_plan11_synthesis_for_single_cluster` with `trial30_enforce_guard_enabled()` ON, and assert the EXAMPLE_SQL proposal is dropped from the returned proposals and a `GSO_TRIAL30_ENFORCED_SWITCH_V1` marker was emitted (capture stdout via `capsys`).

```python
# tests/unit/stages/test_trial30_synthesize_guard_integration.py
# Skeleton — fill cluster/proposal construction from the existing
# mechanism-binding fixture (read tests/unit/test_mechanism_binding.py).
import pytest


def test_guard_drops_reemit_when_fallback_present(monkeypatch, capsys):
    monkeypatch.setenv("GSO_TRIAL30_ENFORCED_SWITCH", "1")
    monkeypatch.setenv("GSO_TRIAL30_ENFORCE_GUARD", "1")
    # ... construct cluster + a fake LLM response yielding two proposals
    #     for the same QID: EXAMPLE_SQL (re-emit) + SQL_SNIPPET (fallback)
    # ... set inert_mechanism_history with lever-5 rejected
    # ... call run_plan11_synthesis_for_single_cluster(...)
    # result_proposals = ...
    # assert no EXAMPLE_SQL proposal remains for the QID
    # out = capsys.readouterr().out
    # assert "GSO_TRIAL30_ENFORCED_SWITCH_V1" in out
    pytest.skip("fill from mechanism-binding fixture during implementation")


def test_guard_off_is_passthrough(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL30_ENFORCE_GUARD", "0")
    # assert both proposals survive when the guard sub-flag is off
    pytest.skip("fill from mechanism-binding fixture during implementation")
```

NOTE: the `pytest.skip` placeholders MUST be replaced with the real fixture-backed assertions during implementation — copy the cluster/LLM-response construction from `tests/unit/test_mechanism_binding.py`. Do not leave skips in the committed test.

- [ ] **Step 2: Run test to verify it fails** (after filling in the fixture)

Run: `pytest tests/unit/stages/test_trial30_synthesize_guard_integration.py -v`
Expected: FAIL — guard not wired; EXAMPLE_SQL proposal still present, no marker.

- [ ] **Step 3: Wire the guard call in synthesize.py post-loop**

Insert after the C5 binding filter block (after line ~2407, before the W4 routing block at ~2409):

```python
    # Trial 30 W30.1b — enforced inert-mechanism switch. Drop a proposal
    # that re-emits a mechanism the acceptance gate proved inert for its
    # (qid, rca_kind), but ONLY when a structural fallback survives in
    # the same QID's slate (never zero out a QID). Gated by
    # GSO_TRIAL30_ENFORCE_GUARD.
    try:
        from genie_space_optimizer.optimization.trial30_flags import (
            trial30_enforce_guard_enabled,
        )
        if trial30_enforce_guard_enabled() and proposals and inert_mechanism_history:
            from genie_space_optimizer.optimization.enforced_mechanism_switch import (
                enforced_switch_survivors,
            )
            _t30_view = [
                _Trial30ProposalView(
                    intent_id=p.intent_id or "",
                    qid=(p.target_qids[0] if p.target_qids else ""),
                    rca_kind=str(getattr(cluster, "rca_kind", "") or ""),
                    patch_type=p.patch_type,
                )
                for p in proposals
            ]
            _t30_outcome = enforced_switch_survivors(
                _t30_view, inert_mechanism_history
            )
            _t30_dropped_ids = {p.intent_id for p in _t30_outcome.dropped}
            for _intent_id, _reason in _t30_outcome.dropped_reasons.items():
                _dropped_p = next(
                    (p for p in proposals if (p.intent_id or "") == _intent_id),
                    None,
                )
                if _dropped_p is not None:
                    emit_patch_outcome(
                        optimization_run_id=optimization_run_id,
                        iteration=iteration,
                        ag_id=ag_id,
                        cluster_id=cluster.cluster_id,
                        intent_id=_intent_id or "<empty>",
                        outcome_kind=PatchOutcomeKind.CONTRACT_FAILED,
                        terminal_reason="enforced_inert_mechanism_switch",
                    )
                print(_reason, flush=True)
            for _qid in _t30_outcome.no_fallback_qids:
                print(
                    "GSO_TRIAL30_NO_FALLBACK_AVAILABLE_V1:"
                    f"qid={_qid}:rca={getattr(cluster, 'rca_kind', '')}",
                    flush=True,
                )
            proposals = [
                p for p in proposals
                if (p.intent_id or "") not in _t30_dropped_ids
            ]
    except Exception:
        logger.debug(
            "Trial 30 enforced mechanism switch guard failed; "
            "passthrough this cluster",
            exc_info=True,
        )
```

Define the thin view dataclass near the top of `synthesize.py` (module level, with the other small helpers):

```python
from dataclasses import dataclass as _dataclass


@_dataclass
class _Trial30ProposalView:
    """Adapter exposing the fields enforced_switch_survivors reads off a
    RepairProposal (which has no top-level qid/rca_kind/mechanism)."""
    intent_id: str
    qid: str
    rca_kind: str
    patch_type: object
```

`enforced_switch_survivors._proposal_mechanism` already derives the mechanism from `patch_type`, so the view only needs `patch_type` (not `mechanism`). Confirm the cluster exposes `rca_kind`; if it is named differently (e.g. `dominant_rca_kind`), read `FailureCluster` and use the correct attribute.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/stages/test_trial30_synthesize_guard_integration.py -v`
Expected: PASS (2 passed) — re-emit dropped + marker emitted; guard-off passthrough holds.

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/stages/synthesize.py tests/unit/stages/test_trial30_synthesize_guard_integration.py
git commit -m "feat(gso): Trial 30 W30.1b — wire enforced switch guard into synthesize post-loop"
```

---

## Task 7: W30.2(a) — union member_qids into target_qids_union

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/synthesize.py:2269-2273`
- Test: `packages/genie-space-optimizer/tests/unit/test_plan11_stage3_target_qids_union_member_carry.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_plan11_stage3_target_qids_union_member_carry.py
# Reuse the cluster + LLM-response fixture from
# tests/unit/test_plan11_stage3_synthesize.py. Construct a cluster whose
# member_qids = {"gs_009", "gs_010"} but whose surviving proposal targets
# only {"gs_010"} (LLM dropped gs_009). With the harvest-wire flag ON,
# assert the emitted target_qids_union still contains "gs_009".
import pytest


def test_member_qid_carried_into_union(monkeypatch, capsys):
    monkeypatch.setenv("GSO_TRIAL30_ENFORCED_SWITCH", "1")
    monkeypatch.setenv("GSO_TRIAL30_INERT_HARVEST_WIRE", "1")
    # ... build cluster.member_qids = ("gs_009", "gs_010")
    # ... LLM yields one proposal with target_qids = ("gs_010",)
    # ... run synthesis; parse plan11_stage3_synthesis_marker from stdout
    # union = ... (parse marker)
    # assert "gs_009" in union
    pytest.skip("fill from test_plan11_stage3_synthesize.py fixture")


def test_flag_off_preserves_legacy_union(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL30_INERT_HARVEST_WIRE", "0")
    # assert union == only the proposal target_qids (legacy behavior)
    pytest.skip("fill from test_plan11_stage3_synthesize.py fixture")
```

Replace skips with real assertions during implementation.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_plan11_stage3_target_qids_union_member_carry.py -v`
Expected: FAIL — `gs_009` absent from the union on the synthesized branch.

- [ ] **Step 3: Modify the synthesized branch** (synthesize.py:2269-2273)

```python
    if proposals:
        outcome_label = "synthesized"
        # Trial 30 W30.2(a) — union the cluster's member QIDs into the
        # synthesized union so a rerouted QID the LLM omitted from
        # target_qids is not dropped from the marker (W29.4 gs_009 drop).
        # Gated by GSO_TRIAL30_INERT_HARVEST_WIRE for byte-stable rollback.
        from genie_space_optimizer.optimization.trial30_flags import (
            trial30_inert_harvest_wire_enabled,
        )
        _proposal_targets = {q for p in proposals for q in p.target_qids}
        if trial30_inert_harvest_wire_enabled():
            _proposal_targets |= {str(q) for q in (cluster.member_qids or ())}
        target_qids_union = sorted(_proposal_targets)
        synthesis_empty_reason = ""
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_plan11_stage3_target_qids_union_member_carry.py -v`
Expected: PASS (2 passed).
Regression: `pytest tests/unit/test_plan11_stage3_empty_synthesis_typed_reason.py tests/unit/test_plan11_stage3_synthesize.py -v`
Expected: PASS (empty-synthesis branch unchanged; synthesized branch superset under flag-on).

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/stages/synthesize.py tests/unit/test_plan11_stage3_target_qids_union_member_carry.py
git commit -m "feat(gso): Trial 30 W30.2(a) — union member_qids into synthesized target_qids_union"
```

---

## Task 8: W30.2(b) — add kit_forced_inert_reroute to the pivot set

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/action_groups.py:1007-1018`
- Test: `packages/genie-space-optimizer/tests/unit/test_trial30_pivot_membership.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_trial30_pivot_membership.py
from genie_space_optimizer.optimization.stages.action_groups import (
    _TERMINATIONS_REQUIRING_PIVOT,
)


def test_kit_forced_inert_reroute_requires_pivot():
    assert "kit_forced_inert_reroute" in _TERMINATIONS_REQUIRING_PIVOT


def test_existing_pivot_members_preserved():
    for member in ("kept_insufficient", "no_applied_patches"):
        assert member in _TERMINATIONS_REQUIRING_PIVOT
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_trial30_pivot_membership.py -v`
Expected: FAIL on `test_kit_forced_inert_reroute_requires_pivot`.

- [ ] **Step 3: Add the member** (action_groups.py:1017, inside the frozenset)

```python
    "kept_insufficient",
    # Trial 30 W30.2(b) — kit_forced_inert_reroute is a behaviour-
    # unchanged survival failure (sibling of kept_insufficient): the
    # patch applied but was inert. Plan 12 must pivot the next iteration
    # to a different patch family so it does not retry the rejected
    # mechanism. Pairs with the W30.1b enforcement guard.
    "kit_forced_inert_reroute",
})
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_trial30_pivot_membership.py -v`
Expected: PASS (2 passed).
Regression: `pytest tests/unit/test_trial20_terminal_taxonomy.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/stages/action_groups.py tests/unit/test_trial30_pivot_membership.py
git commit -m "feat(gso): Trial 30 W30.2(b) — kit_forced_inert_reroute requires pivot"
```

---

## Task 9: W30.2(c) — same-iteration live bucket in acceptance gate

Mirror the `kept_insufficient` block's `ctx.extras` writes (`_live_insufficient_repair_signatures`, `_p2_5_terminal_signature_kit_inputs`) inside the `kit_forced_inert_reroute` block (acceptance_gate.py ~299-310) so sibling clusters in the SAME iteration see the rejected signature.

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/state_machine/transformers/acceptance_gate.py` (~299-310, before the `return GateVerdict.success(...)`)
- Test: `packages/genie-space-optimizer/tests/unit/optimization/test_trial30_kit_forced_live_bucket.py`

- [ ] **Step 1: Write the failing test**

Read `tests/unit/optimization/test_trial29_inert_patch_reroute.py` for the exact `ctx` + `state` construction the kit_forced lane needs, plus `tests/unit/state_machine/transformers/test_acceptance_gate_kept_insufficient.py` for how the live bucket is asserted on `ctx.extras`. Then:

```python
# tests/unit/optimization/test_trial30_kit_forced_live_bucket.py
# Construct a ctx/state that triggers the kit_forced_inert_reroute lane
# (copy from test_trial29_inert_patch_reroute.py's positive case) with
# both Trial 29 and Trial 30 flags ON, run the acceptance gate, and
# assert the rejected signature is now visible on
# ctx.extras["_live_insufficient_repair_signatures"].
import pytest


def test_kit_forced_writes_live_bucket(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL29_BEHAVIOR_DELTA", "1")
    monkeypatch.setenv("GSO_TRIAL29_INERT_REROUTE", "1")
    monkeypatch.setenv("GSO_TRIAL30_ENFORCED_SWITCH", "1")
    monkeypatch.setenv("GSO_TRIAL30_INERT_HARVEST_WIRE", "1")
    # ... build ctx + state for the positive kit_forced_inert_reroute case
    # ... run acceptance_gate transformer
    # bucket = ctx.extras.get("_live_insufficient_repair_signatures", ())
    # assert any("kit_forced_inert" in s for s in bucket)
    pytest.skip("fill from test_trial29_inert_patch_reroute.py positive case")
```

- [ ] **Step 2: Run test to verify it fails** (after filling fixture)

Run: `pytest tests/unit/optimization/test_trial30_kit_forced_live_bucket.py -v`
Expected: FAIL — live bucket empty in the kit_forced lane.

- [ ] **Step 3: Add the live-bucket writes** (before the `return GateVerdict.success(...)` at ~299)

Read the `kept_insufficient` block (acceptance_gate.py ~411-461) and copy its `ctx.extras` mutation verbatim, substituting `_t29_signature`:

```python
                # Trial 30 W30.2(c) — same-iteration visibility. Mirror
                # the kept_insufficient block so sibling clusters in this
                # iteration see the rejected signature immediately (not
                # only after the end-of-iteration harvest). Gated by
                # GSO_TRIAL30_INERT_HARVEST_WIRE.
                try:
                    from genie_space_optimizer.optimization.trial30_flags import (
                        trial30_inert_harvest_wire_enabled,
                    )
                    if trial30_inert_harvest_wire_enabled():
                        _t30_live = list(
                            ctx.extras.get(
                                "_live_insufficient_repair_signatures", ()
                            )
                        )
                        if _t29_signature and _t29_signature not in _t30_live:
                            _t30_live.append(_t29_signature)
                            ctx.extras[
                                "_live_insufficient_repair_signatures"
                            ] = tuple(_t30_live)
                except Exception:
                    pass
```

Confirm `ctx.extras` is a mutable dict in this transformer (the `kept_insufficient` block proves it is). If the kept_insufficient block also writes `_p2_5_terminal_signature_kit_inputs`, replicate that write too with the same guard.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/optimization/test_trial30_kit_forced_live_bucket.py -v`
Expected: PASS.
Regression: `pytest tests/unit/optimization/test_trial29_inert_patch_reroute.py tests/unit/state_machine/ -v`
Expected: PASS (no SM regression; the new write is additive + flag-gated).

- [ ] **Step 5: Commit**

```bash
git add src/genie_space_optimizer/optimization/state_machine/transformers/acceptance_gate.py tests/unit/optimization/test_trial30_kit_forced_live_bucket.py
git commit -m "feat(gso): Trial 30 W30.2(c) — kit_forced_inert_reroute writes same-iteration live bucket"
```

---

## Task 10: Integration replay (7now offline fixture) + tracker + full regression

**Files:**
- Create: `packages/genie-space-optimizer/tests/integration/postmortem_replay/test_trial30_enforced_switch_replay.py`
- Modify: `packages/genie-space-optimizer/docs/architecture/lever-loop-iteration-tracker.md` (mark W30.1/W30.2 implemented)

- [ ] **Step 1: Write the integration replay test**

Model it on `tests/integration/postmortem_replay/test_trial29_w29_1_kit_forced_inert_reroute_replay.py`. The fixture drives the full harvest -> ctx -> prompt -> guard path offline using the 7now `gs_026`/`top_n_cardinality_collapse` reproduction: iteration N produces a `kit_forced_inert_reroute` (lever-5 rejected); iteration N+1's synthesis (a) renders the inert-history section in the prompt, (b) if the LLM re-emits EXAMPLE_SQL alongside a SQL_SNIPPET fallback, the guard drops the EXAMPLE_SQL re-emit, and (c) `gs_026` is present in iteration N+1's `target_qids_union`. Assert all three.

```python
# tests/integration/postmortem_replay/test_trial30_enforced_switch_replay.py
# Read test_trial29_w29_1_kit_forced_inert_reroute_replay.py first and
# reuse its harness/replay scaffolding. Assert:
#   1. render_inert_mechanism_history_section output appears in the
#      iteration N+1 synthesis prompt payload.
#   2. enforced_switch_survivors drops the EXAMPLE_SQL re-emit when a
#      SQL_SNIPPET fallback survives (GSO_TRIAL30_ENFORCED_SWITCH_V1).
#   3. gs_026 in iteration N+1 target_qids_union.
```

- [ ] **Step 2: Run the integration test**

Run: `pytest tests/integration/postmortem_replay/test_trial30_enforced_switch_replay.py -v`
Expected: PASS.

- [ ] **Step 3: Full Trial 30 suite + pretrial gate + SM regression**

Run: `pytest tests/unit/optimization/test_trial30_flags.py tests/unit/optimization/test_trial30_mechanism_normalization.py tests/unit/optimization/test_trial30_inert_harvest_wire.py tests/unit/stages/test_trial30_synthesis_inert_history_threaded.py tests/unit/optimization/test_trial30_enforced_switch_guard.py tests/unit/stages/test_trial30_synthesize_guard_integration.py tests/unit/test_plan11_stage3_target_qids_union_member_carry.py tests/unit/test_trial30_pivot_membership.py tests/unit/optimization/test_trial30_kit_forced_live_bucket.py tests/integration/postmortem_replay/test_trial30_enforced_switch_replay.py -v`
Expected: all PASS.

Run the project pretrial gate (find its exact command in the tracker / prior trial entries — typically a `scripts/` or `make` target):
Run: `grep -rn "pretrial" docs/architecture/lever-loop-iteration-tracker.md | head`
Then run the full unit+integration suite + the 7/7 pretrial gate exactly as Trial 29 did (`t29_pretrial` / `t29_w29_1_phase9` evidence in the tracker). Expected: zero regressions vs `feat/gso-cycle13` HEAD.

- [ ] **Step 4: Mark the tracker**

Update `docs/architecture/lever-loop-iteration-tracker.md::Trial 30`: mark W30.1a, W30.1b, W30.2(a)(b)(c) `[x]` with module owners (file:line), test files, and the acceptance evidence (passing test counts). Set status to "implemented — pending deploy + W30.5 live re-verification".

- [ ] **Step 5: Commit**

```bash
git add tests/integration/postmortem_replay/test_trial30_enforced_switch_replay.py docs/architecture/lever-loop-iteration-tracker.md
git commit -m "test(gso): Trial 30 integration replay (7now) + tracker mark W30.1/W30.2 implemented"
```

---

## Self-review (against the spec)

**Spec coverage:**
- W30.1a wire channel → Tasks 3 (harvest) + 4 (thread to prompt). ✓
- W30.1b enforcement guard (Approach B, post-loop, fallback-required, never-zero-out) → Tasks 5 (pure helper) + 6 (wire). ✓
- W30.1b `PatchMechanism`-enum comparison vocabulary → Task 2 normalization helper, used by Task 5. ✓
- W30.2(a) union member_qids → Task 7. ✓
- W30.2(b) `_TERMINATIONS_REQUIRING_PIVOT` → Task 8. ✓
- W30.2(c) same-iteration live bucket → Task 9. ✓
- Two-tier flags (master + harvest-wire + guard) → Task 1; consumed in Tasks 3/4/6/7/9. ✓
- Typed bail-out markers (ENFORCED_SWITCH_V1, NO_FALLBACK_AVAILABLE_V1) → Tasks 5/6. ✓
- 7now offline fixture → Task 10. ✓
- Out-of-scope (W30.3 persistence, outcome/terminal classifiers, -1 sentinel) → not in any task. ✓

**Type consistency:** `enforced_switch_survivors(proposals, history) -> EnforcedSwitchOutcome` (Task 5) is called in Task 6 with a `_Trial30ProposalView` adapter exposing `intent_id`/`qid`/`rca_kind`/`patch_type`; the helper's `_proposal_mechanism` derives mechanism from `patch_type`. `mechanisms_for_rejected_levers` (Task 2) is imported by Task 5. Flag function names (`trial30_inert_harvest_wire_enabled`, `trial30_enforce_guard_enabled`, `trial30_enforced_switch_enabled`) are defined in Task 1 and used by name in Tasks 3/4/6/7/9. Consistent.

**Placeholder scan:** Three tasks (6, 7, 9) contain `pytest.skip` skeletons that MUST be filled from named existing fixtures (`test_mechanism_binding.py`, `test_plan11_stage3_synthesize.py`, `test_trial29_inert_patch_reroute.py`) — each skip line names its source fixture and explicitly says "do not leave skips in the committed test." This is a deliberate "read the real fixture during implementation" instruction, not an unfilled placeholder, because the cluster/LLM-response construction is large and codebase-specific; reproducing it blind would be more error-prone than pointing the implementer at the canonical fixture.

**Ambiguity check:** Attribute names on the SM `_sm_final_states` object (Task 3) and the `FailureCluster.rca_kind` field (Task 6) are flagged as "read the source of truth, don't guess" with the exact source named — the only unavoidable codebase-lookup points.
