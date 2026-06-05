# Trial 20 — Operator Handoff

This document is the operational runbook for the Trial 20 rollout. It
covers deploy steps, the markers to watch in MLflow / lever-loop stdout
post-deploy, the diagnostic guardrail mapping, and the single-knob
rollback procedure.

Audience: the on-call operator handling the post-merge lever-loop run.

## TL;DR

- **Default ON for merge.** No staged-rollout, no preview flag flip.
- **One env-var to rollback:** `GSO_TRIAL20_ENFORCE=0` + redeploy.
- **First post-deploy run is the canary.** Watch for the seven Trial 20
  positive markers; the absence of all of them means the master flag
  never actually engaged.
- **Anti-success markers fire on regressions.** Treat
  `GSO_TRIAL20_BLAST_RADIUS_UNSTAMPED_V1` as the most operationally
  urgent — it indicates the E1 counterfactual scanner plumbing has a
  gap.

## Pre-Deploy Local Gates (mandatory)

Run each from `packages/genie-space-optimizer/`. All must pass before
deploy.

```bash
# 1. Trial 20 unit suite (per-workstream)
PYTHONPATH=devtools:.:src GSO_SM_TEST_ALLOW_MISSING_EVAL_CONTRACT=1 \
  python -m pytest \
    tests/unit/optimization/test_trial20_blast_radius_mandatory.py \
    tests/unit/optimization/test_trial20_canonical_target_resolution.py \
    tests/unit/optimization/test_trial20_sql_snippet_validation.py \
    tests/unit/test_plan12_patch_family_pivot_after_no_applied.py \
    tests/unit/test_terminal_reason.py \
    tests/unit/test_repair_proposal_dataclass.py \
    tests/unit/test_llm_repair_proposal_output_schema.py \
    -q

# 2. Integration replay (airline accepts under A2 fix, 7now pivots after kept_insufficient)
PYTHONPATH=devtools:.:src GSO_SM_TEST_ALLOW_MISSING_EVAL_CONTRACT=1 \
  python -m pytest tests/integration/test_trial20_postmortem_replay.py -q

# 3. Workbench 500-seed ON (Trial 20 active)
PYTHONPATH=devtools:.:src GSO_SM_TEST_ALLOW_MISSING_EVAL_CONTRACT=1 GSO_TRIAL20_ENFORCE=1 \
  python -m local_lever_workbench.fuzzer --iterations 500 --seed 0 --mode mixed

# 4. Workbench 500-seed OFF (byte-stable replay)
PYTHONPATH=devtools:.:src GSO_SM_TEST_ALLOW_MISSING_EVAL_CONTRACT=1 GSO_TRIAL20_ENFORCE=0 \
  python -m local_lever_workbench.fuzzer --iterations 500 --seed 0 --mode mixed
```

Gate criteria: each of the four must report `500 pass, 0 fail` (or
green test suite). If any fails, do NOT deploy — investigate the
specific workstream by name.

## Deploy Steps

This package deploys with the GSO bundle, not the parent Genie Workbench
app. Operator runs from `packages/genie-space-optimizer/`.

```bash
# 5. Build and deploy the GSO bundle to the app target
./scripts/deploy.sh

# Or, if the app already exists and only code is changing:
./scripts/deploy.sh --update
```

No new resources are required for Trial 20. The flag module is a new
file, all other changes are in-place. No DAB resource changes.

## Post-Deploy Verification — Canary Run

After deploy, run the lever loop on the **airline** anchor space (the
A2 fix target) for one iteration. Then run the **7now** anchor space
(the C1 + B2 target).

### Markers To Confirm Trial 20 Is Engaged

| Marker | Where to find it | Means |
|---|---|---|
| `GSO_TRIAL20_FULL_EVAL_ROOT_CAUSE_V1` | MLflow run start (replay step) | Offline root-cause replay ran |
| `GSO_TRIAL20_SHADOW_DECISION_V1` | On every full-eval acceptance | Shadow decision recorded |
| `GSO_TRIAL20_KEPT_INSUFFICIENT_TERMINAL_V1` | After any iteration with `kept_insufficient` SM state | Terminal taxonomy aligned with SM |
| `GSO_TRIAL20_BUNDLE_EMITTED_V1` | When Stage 3 emits a multi-lever bundle | Bundle default directive honored |
| `GSO_TRIAL20_SINGLE_LEVER_JUSTIFIED_V1` | When Stage 3 emits a justified single-lever | Justification carried |
| `GSO_TRIAL20_SQL_SNIPPET_VALIDATED_V1` | On every `add_sql_snippet_*` patch | `validate_sql_snippet` stamped `validation_passed` |
| `GSO_TRIAL20_CANONICAL_TARGET_RESOLVED_V1` | On every metadata patch with non-canonical target | F2 target resolution fired |

### Anti-Success Markers — Stop The World If These Fire

| Marker | Severity | Action |
|---|---|---|
| `GSO_TRIAL20_BLAST_RADIUS_UNSTAMPED_V1` | P0 | E1 plumbing regression. Rollback. File against `harness.py` counterfactual scanner replication into TransformerContext. |
| `GSO_ITERATION_TERMINAL_DECIDED_V1 terminal_reason="no_applied_patches"` while any QID has `accepted.decision="kept_insufficient"` | P1 | B2 selector regression. Rollback or hotfix the iteration-terminal selector precedence rule. |
| `GSO_PLAN12_PIVOT_RECOMMENDED_V1` with `prior_patch_family == recommended_patch_family` | P1 | C1 graph not consulted, or C2 inference fallback bypassed. Check the kept-insufficient signature shape. |
| Single-lever proposal where `insufficient_repair_signatures` for the cluster is non-empty | P1 | D1 mandatory-bundle directive broke. Inspect Stage 3 prompt rendering. |
| Applier rejection `validation_passed missing` on `add_sql_snippet_*` | P2 | F1 stamping audit regression. |

## Diagnostic Guardrail Mapping

When an iteration regresses, use this table to find the responsible
Trial 20 workstream and its sub-flag:

| Symptom | Workstream | Sub-flag | Owner |
|---|---|---|---|
| Full-eval acceptance still vetoed by pre-arbiter regression with `target_fixed_qids=()` | A | `GSO_TRIAL20_PRE_ARBITER_VETO_FIX` | control plane / row projection |
| Iteration emits `no_applied_patches` over `kept_insufficient` SM state | B | `GSO_TRIAL20_KEPT_INSUFFICIENT_TERMINAL` | iteration-terminal selector |
| Plan 12 pivots back to the same family | C | `GSO_TRIAL20_FAMILY_PIVOT_GRAPH` | `action_groups._PIVOT_GRAPH` |
| Single-lever proposal when bundle is required, OR iteration-1 single-lever without justification | D | `GSO_TRIAL20_MULTI_LEVER_BUNDLE_DEFAULT` | Stage 3 prompt / strategist gate |
| Blast-radius gate is a no-op | E | `GSO_TRIAL20_BLAST_RADIUS_MANDATORY` | counterfactual scanner / SM ctx |
| `add_sql_snippet_*` rejected at applier | F1 | not gated | `validate_sql_snippet` |
| `add_column_description` against unresolved target (`tkt_payment`, `mv_*`) | F2 | not gated | Stage 3 canonical resolver |

## Rollback Procedure

### Emergency (single knob)

```bash
export GSO_TRIAL20_ENFORCE=0
./scripts/deploy.sh --update
```

This disables every Trial 20 sub-flag regardless of its own env var.
The lever loop reverts to Trial 19 behaviour byte-for-byte. The
byte-stable contract is enforced by the 500-seed workbench sweep with
the master flag OFF (run pre-merge).

### Targeted (single sub-flag)

If only one workstream regresses, opt out of just that sub-flag:

```bash
# Example: disable only the blast-radius mandatory gate
export GSO_TRIAL20_BLAST_RADIUS_MANDATORY=0
./scripts/deploy.sh --update
```

The other Trial 20 surfaces remain active. This is appropriate when
the regression is isolated and other workstreams are providing positive
signal.

### Recovery After Rollback

After a rollback, on the next deploy:

1. Re-run the four pre-deploy gates above.
2. Inspect the failure marker(s) from the regressed run — capture the
   full marker payload for postmortem.
3. File against the specific workstream owner (see Diagnostic Guardrail
   Mapping). Trial 20 architecture does NOT regress sub-flag-by-sub-flag
   beyond what the master flag does — there is no "partial Trial 20"
   intended.

## References

- Plan: `~/.cursor/plans/trial_20_outer_rails_e712fc14.plan.md`
- Postmortem guardrails: `docs/architecture/trial20-postmortem-guardrails.md`
- Iteration tracker: `docs/architecture/lever-loop-iteration-tracker.md`
- Flag module: `src/genie_space_optimizer/optimization/trial20_flags.py`
- Trial 19 precedent: `src/genie_space_optimizer/optimization/trial19_flags.py`
