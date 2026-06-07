# Trial 29 W29.1 — Post-Apply Behaviour Gate + Inert-Patch Re-Route + Decomposed Architecture Invariants

> Design record. Authored 2026-06-07. Status: **DRAFT — pending implementation**.
> Supersedes the inert-patch criterion stub in
> `docs/architecture/lever-loop-iteration-tracker.md::Trial 29`.

## Problem statement

Trial 28 W28.1 (LLM-tier RCA canonicaliser) landed in production and
proved the kit gate fires end-to-end on 7now: `unknown_kind` rate
dropped from 66.7%/71.4% to **0.0%** on both anchors, and
`GSO_TRIAL24_KIT_FORCED_V1` registered its first live count
(`count=2` on 7now, deepest stage `accepted`).

But W28.4 acceptance came up 2-of-3:

| Criterion | Verdict |
|---|---|
| `unknown_kind` rate < 30% on both anchors | met (0.0% / 0.0%) |
| ≥1 `GSO_TRIAL24_KIT_FORCED_V1` on either anchor | met (`count=2` on 7now) |
| ≥1 `behavioral_diff != "unchanged"` patch accepted | **unmet** (count=0 on both) |

The kit gate fires, the patches apply, the patches reach `ACCEPTED` —
but every patch in the kit-forced path has `behavioral_diff = "unchanged"`.
The applied SQL transformations are **inert**: they pass the SM contracts
but don't change the planner's behaviour on the target benchmark. 7now's
`KIT_GATE_FIRED_PATCHES_APPLIED_BEHAVIORAL_DIFF_UNCHANGED` verdict is the
clean isolation of this blocker.

## Where the existing code already takes us

The acceptance gate (`state_machine/transformers/acceptance_gate.py`)
already consumes `behavioral_diff` on every evaluated proposal and
routes through two terminal lanes for inert patches:

| Existing lane | Trigger condition | What it does |
|---|---|---|
| `ALREADY_CORRECT_UNDER_ARBITER` | `pre == 1.0`, `post == 1.0`, `behavioral_diff == "unchanged"` | No-op on an already-passing QID; doesn't count against the arbiter. |
| `KEPT_INSUFFICIENT` | `post <= pre`, `behavioral_diff == "unchanged"`, target not fixed | Patch held under signature `lever:patch_type:insufficient:rca=...:behavior=unchanged`. The lever loop reads this signature next iteration. |

The lever lattice itself also exists in `optimization/rca_mechanism_routing.py`:

- `example_sql_is_insufficient_for(rca_kind)` → `bool` — "this lever is structurally inert for this RCA".
- `_structural_fix_mechanisms(rca_kind)` → `frozenset[PatchMechanism]` — the SET of mechanisms that DO fix each RCA.
- `recommended_mechanisms_for_rca(rca_kind)` → `tuple[str, ...]` — ORDERED preference for which mechanism to try first.
- `rca_mechanism_defaulted_marker(...)` — emits `GSO_TRIAL23_RCA_MECHANISM_DEFAULTED_TO_EXAMPLE_SQL_V1` when synthesis fell back to `add_example_sql` for an RCA that needs a structural fix.

## The missing piece: a feedback loop

What does NOT exist today is a feedback loop that, when an applied
patch is proven inert on a *kit-forced* RCA, tells the next iteration
of the lever loop to pick a DIFFERENT mechanism from
`_structural_fix_mechanisms(rca_kind) - already_tried`. The QID
currently lands in `KEPT_INSUFFICIENT` and the lever loop re-tries
the SAME mechanism (or a stochastically-equivalent variant), so the
inert outcome repeats.

This is the architectural gap W29.1 closes.

## W29.1 change set

### 1. New typed acceptance lane: `KIT_FORCED_INERT_PATCH_REROUTE`

Sibling of `KEPT_INSUFFICIENT` in `acceptance_gate.py`. Triggered
when ALL hold:

- The RCA kind is in `active_kit_for_rca_map()` (i.e. kit-for-RCA
  contract attaches to this RCA).
- The patch was applied through the kit-forced path
  (`GSO_TRIAL24_KIT_FORCED_V1` was emitted for this proposal).
- `behavioral_diff == "unchanged"`.

Emits typed marker `GSO_TRIAL29_INERT_PATCH_REROUTE_V1` with the
rejected mechanism + RCA + signature. The QID exits the SM with a
**re-route signal** instead of dropping to `kept_insufficient` — the
lever loop next iteration receives the rejected mechanism in the
`inert_mechanism_history` channel (below) and is required to pick a
different one.

### 2. Lever-loop feedback channel: `inert_mechanism_history`

A typed per-QID accumulator threaded through lever-loop iterations:

```python
class InertMechanismHistory(BaseModel):
    qid: str
    rca_kind: str  # canonical
    rejected_mechanisms: tuple[str, ...]  # in order tried
    signatures: tuple[str, ...]  # matching insufficient signatures
```

Threaded through:

- `harness.py` — accumulate across iterations, keyed by `(qid, rca_kind)`.
- `stages/synthesize.py` — the Stage 3 synthesis prompt receives the
  history as a typed field; the LLM is instructed to pick from
  `_structural_fix_mechanisms(rca_kind) - rejected_mechanisms`. The
  `recommended_mechanisms_for_rca()` ordering gives a deterministic
  "next mechanism" recommendation.

### 3. Typed diagnostic record: `Trial29InertPatchDiagnostic`

New module `optimization/inert_patch_diagnostic.py`. When the new
acceptance lane fires, emit:

```python
class Trial29InertPatchDiagnostic(BaseModel):
    qid: str
    rca_kind: str  # canonical
    rejected_mechanism: str
    patch_json: dict[str, Any]
    pre_arbiter_score: float
    post_arbiter_score: float
    behavioral_diff: str  # always "unchanged" in this lane; recorded for completeness
    signature: str
    iteration: int
    trial: str
```

Persists into the postmortem evidence bundle via the existing
postmortem writer so the *next* postmortem can prove or refute "the
re-route worked / a different mechanism is also inert / the RCA is
mislabeled".

### 4. Feature-flag matrix

| Flag | Default | Purpose |
|---|---|---|
| `GSO_TRIAL29_BEHAVIOR_DELTA` | ON | Master. `=0` restores pre-Trial-29 behaviour byte-for-byte. |
| `GSO_TRIAL29_W29_1_INERT_REROUTE` | ON under master | W29.1 capability (the new acceptance lane + history channel). |

Bright line: when EITHER flag is `=0`, every existing acceptance
lane, every existing Stage 3 prompt, every existing marker is
byte-stable. This is enforced by the unit tests below.

## Coupled change: invariant decomposition (W29.5)

The `/goal` halt-summary identified the monolithic
`architecture_invariants_held: bool` as masking real progress: one
orthogonal gap (e.g. bundle-completeness infra) forces it `false` so
W28.1's actual gains can't be seen in the postmortem evaluation.

### Typed sub-invariant model

New module `optimization/architecture_invariants.py`:

```python
class ArchitectureInvariants(BaseModel):
    rca_invariants_held: bool                    # canonicaliser + kit-for-RCA validator + kit-map coverage
    lever_lattice_invariants_held: bool          # Stage 3 fits cap + lever loop runs when needed + inert patches re-route
    bundle_completeness_invariants_held: bool    # postmortem evidence bundle complete + persistence/handoff works

    @property
    def all_held(self) -> bool:
        return (
            self.rca_invariants_held
            and self.lever_lattice_invariants_held
            and self.bundle_completeness_invariants_held
        )
```

Each sub-invariant has its own typed check function (in the same
module) and its own dedicated postmortem section. Postmortems
report each separately so W29.1's progress is visible as
`lever_lattice_invariants_held = true` even while
`bundle_completeness_invariants_held = false` (the bundle gap is
orthogonal and is W29's W29.2 problem).

### Backwards compatibility

`architecture_invariants_held` continues to be present in the
postmortem JSON, computed as `ArchitectureInvariants.all_held`. The
existing `/goal` harness reads keep working unchanged. New harness
reads can opt in to the per-domain channel for tighter goal
conditions.

## Test surface (TDD order)

Each test file is created with failing tests FIRST, then the
implementation lands minimal code to pass.

| Order | Test file | What it covers |
|---|---|---|
| 1 | `tests/unit/optimization/test_trial29_flags.py` | Sub-flag matrix: master ON/OFF, sub-flag ON/OFF, master `=0` short-circuits sub. |
| 2 | `tests/unit/optimization/test_trial29_inert_patch_reroute.py` | New acceptance lane fires only on (kit_forced ∧ unchanged); existing `KEPT_INSUFFICIENT` lane unchanged when sub-flag `=0`. |
| 3 | `tests/unit/optimization/test_trial29_inert_mechanism_history.py` | `InertMechanismHistory` model round-trip; threading through `harness.py`; Stage 3 prompt receives the history; deterministic next-mechanism recommendation. |
| 4 | `tests/unit/optimization/test_trial29_inert_patch_diagnostic.py` | `Trial29InertPatchDiagnostic` round-trip; persistence into postmortem bundle. |
| 5 | `tests/unit/optimization/test_trial29_architecture_invariants.py` | `ArchitectureInvariants` model; `all_held` property; backwards compat (legacy field present); postmortem renders each sub-invariant separately. |
| 6 | `tests/integration/postmortem_replay/test_trial29_w29_1_kit_forced_inert_reroute_replay.py` | End-to-end: feed a 7now-shaped kit-forced inert patch through the SM and assert the re-route lane fires + history accumulates + diagnostic record persists. |

## Module owners

| Status | Module | Purpose |
|---|---|---|
| NEW | `optimization/trial29_flags.py` | Master + sub flags (parallel to `trial27_flags.py`, `trial28_flags.py`). |
| NEW | `optimization/architecture_invariants.py` | Typed sub-invariant model + per-domain check functions. |
| NEW | `optimization/inert_patch_diagnostic.py` | Typed `Trial29InertPatchDiagnostic` + persistence helpers. |
| MODIFIED | `state_machine/transformers/acceptance_gate.py` | New `KIT_FORCED_INERT_PATCH_REROUTE` lane parallel to `KEPT_INSUFFICIENT`. |
| MODIFIED | `stages/synthesize.py` | Accept + use `inert_mechanism_history` in the Stage 3 prompt. |
| MODIFIED | `harness.py` | Thread `InertMechanismHistory` between lever-loop iterations, keyed by `(qid, rca_kind)`. |
| MODIFIED | `docs/architecture/lever-loop-iteration-tracker.md` | Mark W29.1 `[x]` with module owners + test files + acceptance evidence. |

## Acceptance evidence for W29.4 live verification

After the W29.1 deploy + a fresh replay against the 7now anchor:

1. ≥1 `GSO_TRIAL29_INERT_PATCH_REROUTE_V1` marker emitted.
2. ≥1 `behavioral_diff != "unchanged"` patch accepted on the SAME RCA in a subsequent iteration.
3. `lever_lattice_invariants_held = true` in the fresh postmortem.
4. Measurable accuracy gain on the 7now anchor (final_accuracy_pct > 91.3%).

If (1) fires but (2) does NOT — that's evidence the lever lattice
needs ANOTHER mechanism for this RCA (the SECOND mechanism is also
inert), which is a separable design problem that the diagnostic
records (criterion 3) will expose cleanly.

## Out of scope (deferred to other Trial 29 workstreams)

- **W29.2** — airline Stage-1 diagnose budget + Plan-11 dispatch projection (separate blocker on the airline anchor: `gs_009`/`gs_024` token-budget starvation in Stage 1, NOT Stage 3).
- **W29.3** — iteration-0 seed Stage-3 de-starvation (carry-over from W28.2: the FORWARD-pipeline seed pass still starves; W27.1 only covers the in-loop pass).
- **GT-review pass** — on `pending_gt_review` hard QIDs that partly bound the 100% literal ceiling. Independent, human-bound, not covered here.

## Risk register

| Risk | Likelihood | Mitigation |
|---|---|---|
| The Stage 3 LLM ignores `inert_mechanism_history` and re-emits the same mechanism. | medium | Add a typed validator in `synthesize.py` that rejects any proposal whose mechanism intersects `rejected_mechanisms`. Tested by failing-test #3. |
| Re-routing burns iterations on RCAs where ALL structural mechanisms are inert (e.g., the RCA is mislabeled and the patch is correct-by-construction but the benchmark target itself is wrong). | medium | The diagnostic records (criterion 3) expose this — after N iterations with all mechanisms exhausted, the QID lands in a typed `mechanism_lattice_exhausted` terminal lane (already a separate W29 sub-workstream if it appears in practice; deferred until evidence). |
| The decomposed invariant model breaks the existing `/goal` harness reads. | low | Backwards-compat shim keeps `architecture_invariants_held = all_held` in the postmortem JSON; tested by failing-test #5. |
| Production has more inert lanes than just kit-forced (e.g., `ALREADY_CORRECT_UNDER_ARBITER` also produces inert patches but should NOT re-route). | low | The new lane is GATED on kit-forced specifically. The unit tests cover all four (kit_forced, !kit_forced) × (unchanged, partial/changed) cells. |

---

**Spec self-review status:** placeholders ZERO; internal consistency
verified (W29.1 acceptance lane, history channel, diagnostic, and
invariant decomposition all reference each other correctly); scope
focused on a single coherent feature (post-apply behaviour gate +
re-route); ambiguity checked (`behavioral_diff` is a string with
values `"unchanged" | "partial"` per `evaluated_gate.py`, NOT a
typed enum — out of scope to refactor, documented explicitly).
