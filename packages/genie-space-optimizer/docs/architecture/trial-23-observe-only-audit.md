# Trial 23 W0 — Observe-only Audit

Purpose: before Trial 23 promotes any gate to blocking, classify every
Trial 20/21/22 gate/pivot workstream as **live** (changes optimizer
behavior), **live-but-ineffective** (emits a real verdict that produces
no usable next move), or **observe-only** (marker only, no behavior
change). W7 proved "done != live", so this audit gates Phases 2-4.

Method: direct code read of each gate and its call site. Evidence is the
file:line of the decision and whether the decision mutates the proposal
set / terminal routing or only `print(...)`s a marker.

## Findings

### Observe-only (marker emitted, behavior unchanged)

- **W7 Stage-3 subcluster slice** — `stages/synthesize.py:870-928`.
  Computes `partition_rca_subcluster_by_token_budget(...)`, prints
  `stage3_subcluster_split_marker`, then at line 928 invokes the LLM
  with the **same un-sliced `request`**. Flag `GSO_TRIAL22_SUBCLUSTER_SLICE`.
  Consequence: 16 `prompt_too_large` declines/run at 91-105k vs 40k cap.
  -> Fixed by **W6**.

- **Repair-diagnosis sufficiency gate** — `state_machine/transformers/diagnose_llm.py:527-611`.
  Builds a `RepairDiagnosis`, calls `gate_repair_diagnosis_sufficient`,
  but emits `GSO_REPAIR_DIAGNOSIS_GATE_V1` with `observe_only=True`
  hardcoded (line 603). It records `indeterminate` (missing
  `implicated_assets`) but never abstains; generation falls through to a
  generic/example shape. -> Promoted by **W5** (only after W7/W8/W9
  repair paths exist; see tension below).

- **Trial 20 D4 mechanism-repeat guard** — `stages/synthesize.py` (~1738+).
  Comment: "Observe-first: we do not block on `blocked` yet (the bundle
  still emits)." Marker only. -> Relevant to **W8**.

- **Plan 12 pivot SKIPPED marker** — `plan12_pivot_observability.py`.
  Honesty-not-execution **by design** — reports a planned pivot that did
  not run so postmortems do not mistake it for tested evidence. This one
  is correct as observe-only (it is a reporting marker, not a gate). No
  change needed; keep.

### Live verdict, but ineffective downstream

- **Structural repair gate** — `structural_repair_gate.py`. Returns
  `admitted` / `rejected` / `retry_with_typed_feedback` (a real verdict
  the dispatch loop consumes). But in both runs all four rows were
  `retry_with_typed_feedback` with `emitted_patch_shape="absent"` and
  `repairability_score=0.0` — the retry never produced a patch. Live
  decision, dead-end retry. -> Addressed by **W4** (route to a mechanism
  it can emit) + **W8** (pivot destination).

- **Trial 22 retry feedback** (`GSO_TRIAL22_RETRY_FEEDBACK_V1`) —
  `stages/synthesize.py`, `structural_repair_gate.py`. Preserves the top
  drop reason for the next attempt, but no later candidate survived in
  either run. Carries evidence; does not change the outcome.

### Live and working (do not regress)

- **Trial 20 D3 sole-lever drop** — `stages/synthesize.py:1696-1736`.
  Actually removes sole-lever proposals from rejected families. Correct
  intent, but the pivot has **no destination** -> slate empties to
  `stage3_returned_none`. -> Fixed by **W8** (redirect, do not just drop).

- **Slate compiler bundle group check + W2.1 dissolution** —
  `proposal_slate_compiler.py::_check_bundle_invariants_group`. Live:
  singletons dissolve, multi-member same-lever bundles drop. -> Extended
  by **W9** (recompose instead of drop).

- **Producer snippet validator** — `producer_snippet_validator.py:135-247`.
  Live: declines invalid SQL (`snippet_invalid`). But it is drop-only;
  the design comment routes the caller to "pivot to a different
  mechanism" rather than repairing the SQL. -> Fixed by **W7** (repair
  loop before drop).

## Re-prioritization (impact on Phases 2-4)

1. **W6 (real slice)** and **W7 (snippet repair)** are confirmed pure
   gaps (no existing live behavior to preserve). Lowest regression risk.
2. **W5 (asset grounding)** must NOT simply flip `observe_only=False`.
   The gate already exists and would block generation; flipping it
   before W7/W8/W9 reproduces the all-dropped flatline. W5 must inject
   resolved assets to **satisfy** the gate, then flip blocking last.
3. **W8 (pivot destination)** is now two problems, not one: the live
   pivot decision does not fire (W3 — empty `prior_patch_family` /
   `prior_lever_set`), AND the structural retry is a dead end (no patch
   emitted). W8 must give both a destination.
4. **Plan 12 pivot** infrastructure exists and is partly live (the
   DECIDED marker mutates the AG); the failure is input starvation, not
   missing machinery. W3 feeds it; W8 ensures the pivot has somewhere to
   go.

## Central tension (carried into sequencing)

Promoting observe-only gates to blocking (W5, W10) before the
repair/redirect paths (W7, W8, W9) exist will re-create the all-dropped
flatline Trial 22 just escaped. Order: Phase 1 (honesty/pivot) ->
Phase 2 (raise first-pass validity) -> Phase 3 (repair fallbacks) ->
then tighten boundaries.
