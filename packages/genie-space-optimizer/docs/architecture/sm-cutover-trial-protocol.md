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

## Pre-trial local checks (mandatory)

Full Databricks trials must be **confirmation runs**, never discovery
runs. Every contract failure that historically appeared first in a
deployed trial has a local reproduction now; running these tests
before deploying ensures the trial only exercises real endpoint /
Genie API / re-eval behaviour rather than re-discovering tape-drift,
hydration regressions, or stage-contract failures.

Run **all** of the following from
`packages/genie-space-optimizer/`. They must each finish green in
under ten seconds; a longer wall time is itself a regression signal.

| # | Command | What it pins |
|---|---|---|
| 1 | `pytest tests/unit/test_eval_row_access_handles_production_shapes.py -q` | Stage 1 evidence-card hydration covers the five production MLflow row shapes. |
| 2 | `pytest tests/integration/test_databricks_request_contract_golden.py -q` | The Databricks request envelope is wire-valid (no PR-1B/1C regression). |
| 3 | `pytest tests/integration/test_sm_tape_replay_diagnose_success.py -q` | Stage 1 tape-replay scaffold still works end-to-end. |
| 4 | `pytest tests/integration/test_sm_forward_pipeline_to_proposed.py -q` | Production-shaped rows reach at least `FunnelStage.PROPOSED` under tape replay. **The forward-pipeline smoke gate.** |
| 5 | `pytest tests/integration/test_sm_forward_pipeline_failure_modes.py -q` | Every known failure mode (empty card, non-actionable diagnosis, Stage 2 drop, Stage 3 contract failure, tape exhaustion) surfaces locally with a typed terminal. |
| 6 | `pytest tests/integration/test_sm_forward_pipeline_to_normalized.py -q` | `PROPOSED → NORMALIZED` boundary: structural patches advance past NORMALIZED; empty-body patches terminate with `OPTIMIZER_INVARIANT_VIOLATION` quoting the missing field. |
| 7 | `pytest tests/integration/test_sm_forward_pipeline_to_applyable.py -q` | `NORMALIZED → APPLYABLE` boundary: safe-by-default patches advance past NORMALIZED to APPLYABLE (no `blast_radius_batch` rejection markers); patches carrying `passing_dependents` outside the target set cycle back to PROPOSED with a typed `blast_radius_rejected` ProposalAttempt and a `GSO_GATE_REASONING_V1` line naming the collateral QIDs. |
| 8 | `pytest tests/integration/test_sm_forward_pipeline_to_applied.py -q` | `APPLYABLE → APPLIED` boundary: with a `FakeWorkspaceClient` recording every `w.api_client.do("PATCH", "/api/2.0/genie/spaces/{space_id}", body=...)`, every hard QID reaches `APPLIED`, the recorded PATCH body decodes into a `serialized_space` carrying the synthesized example SQL, and the `applier_gate` emits `GSO_PATCH_OUTCOME_V1 outcome="applied"`. The companion failure-path test wires the fake to raise on every PATCH and pins the typed `applyability_rejected` `ProposalAttempt` + `GSO_GATE_REASONING_V1` shape postmortems read. |
| 9 | `pytest tests/integration/test_stage1_card_from_production_replay_rows.py -q` | **Production-grounded** Stage 1 card-completeness contract. For every committed `ProductionCase` under `tests/integration/fixtures/production_replay/`, the canonical card builder must produce a violation set equal to the postmortem snapshot (today: `["question_text_empty"]` for all 7 cases). The companion XFAIL strict test pins the **target** contract (zero violations) and flips to a hard failure the moment the multi-source `row_question` ladder lands. Also runs the corpus sanitization audit. |
| 10 | `pytest tests/integration/test_sm_diagnosis_actionable_gate.py -q` | `DIAGNOSED → CLUSTERED` boundary: non-actionable diagnoses (zero `blame_set`, empty `evidence_summary`, insufficient-evidence sentinel) must terminate with a typed `diagnosis_not_actionable` reason — Trial 12 saw 21/24 non-actionable diagnoses silently advance into Stage 2. XFAIL strict today; flips green when the gate transformer lands. |
| 11 | `pytest tests/integration/test_sm_stage3_empty_synthesis_terminates.py -q` | Stage 3 `{"proposals": []}` must terminate with a typed `stage3_silent_decline` reason AND emit a `GSO_PATCH_OUTCOME_V1` line so postmortems can attribute zero-applied iterations to Stage 3 instead of inferring it. Trial 12 saw 6/6 actionable clusters land here silently. XFAIL strict today. |
| 12 | `pytest tests/integration/test_plan11_hard_qid_parity.py -q` | Plan 11 partial-drift gate: any non-empty `missing_from_plan11` set must raise `InputProjectionContractViolation`. dc89 ran for four iterations with partial drift while the contract only failed-closed on total starvation. XFAIL strict today. |
| 13 | `pytest tests/unit/test_plan11_diagnose_output_schema_caps.py -q` | `Plan11DiagnoseOutput.DiagnosisItem` Pydantic schema-cap audit + truncation contract. Pins minimum field caps (200 universal, 1000 narrative) AND asserts `model_validate` truncates rather than raises on overlong responses. Both target contracts XFAIL strict today — Trial 11 caught at least one production string exceeding the declared cap; recoverable shape mismatch was classified as `llm_error` downstream. |

A single command short-circuits the whole suite:

```bash
pytest \
  tests/unit/test_eval_row_access_handles_production_shapes.py \
  tests/integration/test_databricks_request_contract_golden.py \
  tests/integration/test_sm_tape_replay_diagnose_success.py \
  tests/integration/test_sm_forward_pipeline_to_proposed.py \
  tests/integration/test_sm_forward_pipeline_failure_modes.py \
  tests/integration/test_sm_forward_pipeline_to_normalized.py \
  tests/integration/test_sm_forward_pipeline_to_applyable.py \
  tests/integration/test_sm_forward_pipeline_to_applied.py \
  tests/integration/test_stage1_card_from_production_replay_rows.py \
  tests/integration/test_sm_diagnosis_actionable_gate.py \
  tests/integration/test_sm_stage3_empty_synthesis_terminates.py \
  tests/integration/test_plan11_hard_qid_parity.py \
  tests/unit/test_plan11_diagnose_output_schema_caps.py \
  -q
```

The new tests #9–#13 are deliberately XFAIL-heavy today. XFAIL `strict=True` keeps the suite green while pinning the **target** contract every follow-up implementation PR must satisfy. The moment a contract test starts passing, the suite goes red — forcing the maintainer to delete the `xfail` marker in the same diff that lands the fix. This converts the historical "trial → postmortem → unanchored fix → next trial regresses" loop into "postmortem → committed `ProductionCase` → strict xfail contract → fix removes xfail in lockstep".

If any of these fail, **do not deploy**. Fix the underlying contract
before promoting to a Databricks run.

### Failure-becomes-fixture rule

Any production failure that blocks a stage MUST become either:

- A new `TapeEntry` factory in
  `tests/integration/sm_forward_tapes.py` exercising the exact
  failure shape, OR
- A new fixture row in
  `tests/unit/fixtures/production_eval_rows.json` with the
  `_expected_hard`/`_expected_qid` markers wired so the SM forward
  tests admit and observe it (note: this corpus is **shape-only**;
  see below), OR
- A new sanitized `ProductionCase` in
  `tests/integration/fixtures/production_replay/<run_tag>__<qid>.json`
  for any failure where the production row shape itself is the
  load-bearing signal (e.g. `question_text_empty` from a bare
  `question_id`-only row).

paired with an assertion in the relevant
`test_sm_forward_pipeline_*.py` or `test_*_gate.py` file before the
next deploy. This keeps the local pre-trial suite a strictly growing
superset of every known stage-contract failure, so the loop "trial →
postmortem → local repro → fix → trial" runs against shrinking
discovery scope each cycle.

#### When to use which corpus

- `tests/unit/fixtures/production_eval_rows.json::hydration_rows` —
  shape-ladder. Each row is fully evidenced. Use for "if the row
  carries the question, the builders/SM advance" mechanics tests.
  Producing a green test against this corpus does NOT prove
  production rows hydrate.
- `tests/integration/fixtures/production_replay/` — sanitized
  snapshots of the actual upstream payload production sends Stage 1
  for hard QIDs. Use for "does production hydrate"; failures here
  are the canonical signal that the canonical row→card path is
  incomplete. See `SCHEMA.md` in that directory for the case-file
  format and the sanitization rules.

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
