# SM Cutover — Trial Protocol (Phase 7)

This document defines the trial protocol for the SM Cutover
(Deletion-First) PR. It lives in `docs/architecture/` because the
trial verifies the architectural contract that the lever loop has one
canonical iteration body, not two.

## What this PR shipped (Phases 1–6)

| Phase | Status | Surface |
|---|---|---|
| 1 — Stage-1 readiness | landed | `optimizer.py`, `eval_row_admission.py`, `diagnose.py`, `dispatch_input.py`, `run_analysis_contract.py` |
| 2 — Transformer contracts | landed | `state_machine/transformers/__init__.py` docstring; `applier_gate.py` was already in place |
| 3 — Internal-fallback deletion | landed | `state_machine/registry.py`, `iteration_terminal_policy.py`; `routing_gate.py` + `escalation_ladder.py` quarantined to `_legacy/` |
| 4 — Harness dispatcher | landed | `harness.py` — `_run_lever_loop` is now a 70-LOC dispatcher; legacy 19k-LOC body preserved verbatim as `_run_lever_loop_legacy`; new `_run_lever_loop_sm_first` stub falls through to legacy |
| 5 — Module quarantine | **deferred** | safe to land only after Phase 7 confirms the SM-first body is correct; the dispatcher defaults to legacy in this PR |
| 6 — Forward-only e2e test | landed | `tests/unit/test_sm_owns_full_iteration_end_to_end.py` |
| 7 — Trial | this doc | deploy + run lever_loop on the two anchor spaces |

The dispatcher default is **legacy = true** in this PR — no production
behaviour changes until the trial-prep PR explicitly flips the
default to SM-first.

## Trial steps

### 1. Deploy

```bash
./scripts/deploy.sh --update
```

Confirm `databricks bundle deploy -t app` succeeds and the app
restart picks up the new harness. The dispatcher marker
`GSO_LEVER_LOOP_DISPATCH_V1 legacy=true` MUST appear at iteration
start in the legacy run that follows. If the marker is absent the
dispatcher was not deployed and the trial must be aborted.

### 2. Baseline legacy run (sanity)

Run `lever_loop` for `dc89d1a9-2020-4f42-994d-1ae05b865398` and
`98ec8950-d7d4-40b3-b5c0-36dcfb3fb610` with the default
(legacy = true). Acceptance:

- `GSO_LEVER_LOOP_DISPATCH_V1 legacy=true` appears at every iteration.
- Behaviour is byte-identical to the pre-cutover legacy body.
- Phase 1 markers (`GSO_INPUT_PROJECTION_PARITY_V1` with empty
  `missing_from_*` arrays; `GSO_PLAN11_STAGE1_DIAGNOSIS_V1` carries
  `error_kind` whenever `outcome=llm_error`) are visible.

### 3. SM-first canary

Re-run on the same two anchor spaces with
`GSO_USE_LEGACY_LEVER_LOOP=false`. Acceptance criteria, in order:

| # | Criterion | Required marker / signal | If it fails |
|---|---|---|---|
| 1 | Dispatcher routes to SM-first | `GSO_LEVER_LOOP_DISPATCH_V1 legacy=false` | Trial aborted; flip flag back. |
| 2 | SM-first stub delegates to legacy in this PR | `GSO_LEVER_LOOP_SM_FIRST_STUB_V1 status=stub_delegating_to_legacy` | Trial blocked: the next PR ships the real SM-only body. |
| 3 | Stage 1 reaches DIAGNOSED for every hard QID | `GSO_QSTATE_TRANSITION_V1 to_stage=diagnosed` per QID | Stage 1 readiness fix regressed; see Phase 1.A/B/C tests. |
| 4 | At least one hard QID reaches `deepest_stage_reached=applied` | trajectory JSON for that QID | This is the actual P1 accuracy work; the postmortem must name the ONE declining transformer (e.g. `SYNTHESIZE_DECLINED for plural_top_n_collapse`). |
| 5 | Zero `routing_gate` / `escalation_ladder` invocations | absence of `transformer_name=routing_gate\|escalation_ladder` in stdout | Phase 3 deletions regressed; their `_legacy/` quarantines must be re-imported by mistake. |
| 6 | Parity markers clean | `GSO_INPUT_PROJECTION_PARITY_V1` with `missing_from_plan11 == missing_from_sm == []` | Phase 1.B admission helper regressed. |

### 4. Decision

| Outcome | Next PR |
|---|---|
| All six criteria pass | **trial-prep PR**: flip dispatcher default to `legacy = false`; replace `_run_lever_loop_sm_first` stub with the ~250 LOC SM-driven body (read baseline rows → ADMIT → SM iteration → MLflow re-eval → acceptance gate → emit `GSO_OPTIMIZER_OUTCOME_V1`); Phase 5 module quarantine into `_legacy/`. |
| Criterion 4 fails (no APPLIED) | Postmortem names ONE transformer; iterate on its prompt (e.g. synthesise) in a separate PR. Dispatcher stays defaulted to legacy. |
| Any other criterion fails | Roll back: revert this PR or set `GSO_USE_LEGACY_LEVER_LOOP=true` at app config. |

## Rollback

Single env-flag rollback. Set `GSO_USE_LEGACY_LEVER_LOOP=true` on the
app and redeploy (or hot-restart). No code change required.

## What the SM-first stub is, exactly

The Phase 4 scaffold sits at
`harness.py:_run_lever_loop_sm_first`. In this PR it is a delegating
stub:

```python
def _run_lever_loop_sm_first(...) -> dict:
    print("GSO_LEVER_LOOP_SM_FIRST_STUB_V1 status=stub_delegating_to_legacy")
    return _run_lever_loop_legacy(...)
```

The trial-prep PR replaces the body with the real SM-driven loop the
plan describes. The dispatcher contract is what this PR locks down —
the next PR cannot accidentally re-introduce a dual path because the
dispatcher tests in `test_lever_loop_dispatcher.py` enforce one body
per env-flag value.

## Why Phase 5 module quarantine is deferred

The dispatcher defaults to legacy in this PR. Physically moving
~100 modules into `_legacy/` while the legacy harness body is still
the live path would break every legacy import at import time.
Quarantine lands in the same PR that flips the dispatcher default
and replaces the SM-first stub, so the move + the flag flip ship
atomically. The CI grep that enforces "no `_legacy.` imports outside
`_legacy/`" lands with that PR.

## Acceptance test catalog (run before the trial)

```bash
cd packages/genie-space-optimizer
uv run pytest \
  tests/unit/test_lever_loop_dispatcher.py \
  tests/unit/test_sm_owns_full_iteration_end_to_end.py \
  tests/unit/test_sm_stage1_resolves_admitted_rows.py \
  tests/unit/test_eval_row_admission_helper.py \
  tests/unit/test_diagnose_llm_error_kind.py \
  tests/unit/test_state_machine_registry_coverage.py \
  tests/integration/anchors/test_anchor_qids_reach_applied.py \
  tests/unit/state_machine/transformers/test_registry_orders_transformers.py \
  -q
```

All 48 tests must pass. If any fail, the trial does not run.
