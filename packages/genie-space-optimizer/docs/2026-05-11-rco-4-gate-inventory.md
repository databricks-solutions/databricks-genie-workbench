# RCO-4 Stage-6 Gate Inventory

> Source-of-truth mapping from the closeout doc's six conceptual gates to their actual code homes as of 2026-05-11. Cite this from any future plan that touches Stage-6 gate orchestration.

## Six conceptual gates → code map

### 1. Blast-radius gate

| Layer | Code home | Status |
|---|---|---|
| Predicate (per-patch decision) | `optimization/proposal_grounding.py:patch_blast_radius_is_safe` | Pure ✅ |
| Secondary predicate (instruction scope) | `optimization/proposal_grounding.py:instruction_patch_scope_is_safe` | Pure ✅ |
| Pre-application observability sub-handler | `optimization/stages/gates.py:_run_blast_radius_gate` | Pure ✅ (observability-only — not the production firing site) |
| Production orchestration (build `_blast_kept` / `_blast_dropped`, emit decision records, capture DroppedCausalPatch) | `optimization/harness.py:~20860-21200` (inline) | **Inline ❌ — extraction target** |

### 2. Applyability gate

| Layer | Code home | Status |
|---|---|---|
| Per-patch applyability decision | `optimization/patch_applyability.py:PatchApplyabilityDecision` | Pure ✅ |
| Smoke-test gate (separate concern) | `optimization/harness.py:_gate_candidates_with_smoke_test` (line ~6601) | Already a named function; calls pure `run_pre_promotion_smoke_test` |
| Applyability call site in harness | TBD (located in Task 5 by grep for `PatchApplyabilityDecision` import in harness) | **Wrap-only — no logic move** |

### 3. Alignment gate

| Layer | Code home | Status |
|---|---|---|
| RCA-groundedness check (pre-application, observability) | `optimization/stages/gates.py:_run_rca_groundedness_gate` | Pure ✅ (observability-only) |
| Production alignment decision | Distributed across `_run_gate_checks` full-eval acceptance logic (line ~13565) and per-iteration RCA-id tracking | **Deferred — no single extractable site** |

See `docs/2026-05-11-rco-4-deferred-gates.md` for the deferral rationale.

### 4. Reflection gate

| Layer | Code home | Status |
|---|---|---|
| Patch-body fingerprint helper | `optimization/reflection_retry.py:patch_body_fingerprint` | Pure ✅ |
| Production reflection control flow | Interleaved with `_run_gate_checks` and the journey emitter across iteration boundaries | **Deferred — not a single gate-decision site** |

See `docs/2026-05-11-rco-4-deferred-gates.md`.

### 5. Cap gate

| Layer | Code home | Status |
|---|---|---|
| Cumulative regression debt threading | `optimization/harness.py:_run_gate_checks` param `cumulative_regression_debt` (line 12767) | Inside the 800-line `_run_gate_checks` body |
| Accuracy cap / patch cap | Mentioned in `audit_emit gate_name` strings but not a discrete function | **Deferred — extraction requires `_run_gate_checks` decomposition first** |

See `docs/2026-05-11-rco-4-deferred-gates.md`.

### 6. Narrow-replacement / Branch C gate

| Layer | Code home | Status |
|---|---|---|
| Structural-causal drop detector | `optimization/stages/gates.py:detect_structural_causal_drop` (Cycle 16 T4) | Pure ✅ |
| Branch-A vs Branch-C narrow-loop builder | `optimization/harness.py:_run_narrow_l6_replacement_loop` | Already an extracted function; called from harness orchestration |
| Production orchestration (Branch A/C decision + structural-causal halt + `no_structural_alternative` halt emission) | `optimization/harness.py:~20947-21041` (inline) | **Inline ❌ — extraction target** |

## Summary

| Gate | Plan disposition |
|---|---|
| Blast-radius | Extract orchestration → `run_blast_radius_production_gate` (Task 3) |
| Narrow-replacement / Branch C | Extract orchestration → `resolve_narrow_replacement` (Task 4) |
| Applyability | Wrap existing pure module → `run_applyability_gate` (Task 5) |
| Alignment | Deferred — see `2026-05-11-rco-4-deferred-gates.md` |
| Reflection | Deferred — see `2026-05-11-rco-4-deferred-gates.md` |
| Cap | Deferred — see `2026-05-11-rco-4-deferred-gates.md` |

Three of six conceptual gates ship in RCO-4. The remaining three are gated on RCO-4b (`_run_gate_checks` decomposition).
