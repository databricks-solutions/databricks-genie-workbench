# Roadmap Update Draft — post-Phase F+H wire-up + Phase E merge

**Purpose:** pre-staged edits to apply to `2026-05-01-burn-down-to-merge-roadmap.md` after Phase C Commit 19's smoke test passes (signaling F+H wire-up complete) and Phase E's pilot run + merge complete. This doc is **patch text only** — pasted into the roadmap by the executor as the final wire-up commit alongside C19 (or as a standalone commit immediately after).

**When to apply:**

| Section | Apply when |
|---|---|
| §1 At-a-glance row updates | Phase C Commit 19 smoke green AND Phase E Task 9 merge complete |
| §2 Phase E.0 ✅ annotation | Anytime — E.0 already shipped per the audit (see [`2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md`](./2026-05-04-mlflow-decision-artifacts-troubleshooting-plan.md) and the four landed deliverables: `tools/mlflow_audit.py`, `tools/mlflow_backfill.py`, `tools/mlflow_artifact_anchor.py`, `common/mlflow_markers.py` plus `tests/unit/test_mlflow_*` and `tests/integration/test_mlflow_smoke_one_iteration.py`) |
| §3 Phase F status flip | Phase A1-A6 + B9-B16 + C17-C19 all merged |
| §4 Phase H Option 1 → ✅ | C18 merged (closes Phase H T12 + T13) |
| §5 Phase E ✅ annotation | Phase E Task 9 merge complete |
| §6 F+H wire-up line item ✅ | C19 smoke green |
| §7 Reasons section text update | Same as §3-§6 |

---

## §1 At-a-glance row updates (`roadmap.md:124-141`)

Replace the Phase E.0, E, F, H, and totals rows in the at-a-glance table with:

```markdown
| **E.0** | **MLflow artifact integrity audit + persistence fixes (✅ complete — `mlflow_audit.py`, `mlflow_backfill.py`, `mlflow_artifact_anchor.py`, `common/mlflow_markers.py` + tests landed)** | Mostly | 0 (replay-only) + 1 backfill smoke (shipped) | shipped | pre-merge prerequisite for E ✅ |
| E | Final integration + contract-gate flip + merge **(✅ complete — pilot run validated, raise_on_violation=True flipped, replay-side decision-trace hard gate added, sanity PR proved CI fail-closed, branch merged to main)** | No | 1 (shipped) | shipped | merge point ✅ |
| F | Stage-aligned `harness.py` modularization **(✅ complete — modules half landed during F1-F9 plans; harness wire-up half landed via the F+H combined wire-up plan, A1-A6)** | Yes | 0 | shipped | post-merge follow-up ✅ |
| G | Stage Protocol + registry + RunEvaluationKwargs (G-lite) **(✅ implemented)** | Yes | 0 | shipped | post-merge architecture follow-up ✅ |
| **H** | **GSO Run Output Contract — process-first transcript + per-stage MLflow bundle (final unification). Option 1 ✅ implemented** (T1-T11, T14-T16); **T12+T13 harness wire-up + dbutils exit ✅ shipped via F+H wire-up plan C17+C18.** Detailed plan: [`2026-05-04-phase-h-gso-run-output-contract-plan.md`](./2026-05-04-phase-h-gso-run-output-contract-plan.md), with combined wire-up at [`2026-05-04-phase-f-h-harness-wireup-plan.md`](./2026-05-04-phase-f-h-harness-wireup-plan.md). | Yes | 0 | shipped | post-merge unification ✅ |
| | **Pre-merge total** | | **11 runs (~22 hrs, all spent)** | **shipped** | |
| | **Post-merge follow-up (F-modules ✅ + G-lite ✅ + H Option 1 ✅ + F+H wire-up ✅)** | | 0 | all shipped | |
```

Add a new line item below the post-merge follow-up row for outstanding follow-ups:

```markdown
| | **Outstanding post-merge follow-ups** | | 0 | F2 ([`2026-05-05-phase-f2-rca-evidence-followup-plan.md`](./2026-05-05-phase-f2-rca-evidence-followup-plan.md)) + F6 ([`2026-05-05-phase-f6-gates-order-reconciliation-followup-plan.md`](./2026-05-05-phase-f6-gates-order-reconciliation-followup-plan.md)) ~1-2 days each; F1 capture decorator wrap ~30 min | post-merge cleanup |
```

---

## §2 Phase E.0 — already shipped (`roadmap.md:478-502`)

Update the `## Phase E.0` heading line to:

```markdown
## Phase E.0 — MLflow artifact integrity audit (✅ complete — 2026-05-04)
```

Add at the **top** of the Phase E.0 section body (right after the heading):

```markdown
> **Status:** All four core deliverables landed. The CLI audit tool ships at
> `tools/mlflow_audit.py`; the backfill tool at `tools/mlflow_backfill.py`;
> the persistence anchor at `tools/mlflow_artifact_anchor.py`; the marker
> name registry at `common/mlflow_markers.py`. Unit tests in
> `tests/unit/test_mlflow_audit.py`, `tests/unit/test_mlflow_backfill.py`,
> `tests/unit/test_mlflow_markers.py`. Integration smoke at
> `tests/integration/test_mlflow_smoke_one_iteration.py`. Phase E
> consumed the audit tool to validate the pilot-run artifact integrity.
```

---

## §3 Phase F status flip (`roadmap.md:534-645`)

Update the heading on `roadmap.md:534`:

```markdown
## Phase F — Stage-aligned `harness.py` modularization (post-merge) — ✅ complete
```

Replace the entire status callout (lines 536-538) with:

```markdown
> **Status (post-F+H wire-up):** Phase F is fully landed. The "modules" half
> shipped first (9 typed stage modules under `optimization/stages/`,
> G-lite registry + Protocol + uniform `execute` aliases). The "harness
> wire-up" half landed via the **F+H combined wire-up plan** ([`2026-05-04-phase-f-h-harness-wireup-plan.md`](./2026-05-04-phase-f-h-harness-wireup-plan.md))
> — Phase A's 6 atomic byte-stable commits (A1-A6) replaced inline
> `cluster_failures(...)`, `_strategist_ag_records(...)`, `_proposal_generated_records(...)`,
> `_patch_applied_records(...)`, `_ag_outcome_decision_record(...)`,
> `_post_eval_resolution_records(...)`, and the AG_RETIRED inline emit
> with stage module calls. Two stages — F2 (rca_evidence) and F6 (gates)
> — were intentionally deferred to dedicated follow-up plans
> ([`2026-05-05-phase-f2-rca-evidence-followup-plan.md`](./2026-05-05-phase-f2-rca-evidence-followup-plan.md)
> and [`2026-05-05-phase-f6-gates-order-reconciliation-followup-plan.md`](./2026-05-05-phase-f6-gates-order-reconciliation-followup-plan.md))
> because each presents a non-trivial design choice (F2: empty bundle
> with no consumer; F6: gate-order divergence with the harness inline
> path). Those follow-ups run post-merge as cleanups; they do not block
> the merge gate or Phase E. Phase F's intended LLM-postmortem promise
> is satisfied by the 7-of-9 stages currently wired (F1+F3+F4+F5+F7+F8+F9);
> the LLM can navigate any decision_type emitted by these stages to
> exactly one source file under `optimization/stages/`.
>
> **TL;DR:** F is ✅. Two follow-ups outstanding for cleanup.
```

Update the "What landed (modules half)" + "What's pending (harness wire-up half)" subsections to reflect the new state. The "What's pending" subsection should become "What landed via the F+H wire-up" with the 6 wired stages enumerated and the 2 deferred ones referenced to their follow-up plans.

Concretely, replace `roadmap.md:550-559` (the "What's pending" bullets) with:

```markdown
### What landed via the F+H combined wire-up plan ([2026-05-04](./2026-05-04-phase-f-h-harness-wireup-plan.md))

- ✅ F3 (clustering) — `cluster_failures(...)` replaced by `_clust_stage.form(...)` at the F3 insertion point. Hard + soft branches both routed; demoted-cluster combine adapter preserves downstream consumer set.
- ✅ F4 (action_groups) — `_strategist_ag_records(...)` inline producer replaced by `_ags_stage.select(...)`; atomic dedup at `harness.py:14884`.
- ✅ F5 (proposals) — `_proposal_generated_records(...)` inline producer replaced by `_prop_stage.generate(...)`; atomic dedup at `harness.py:15030`. content_fingerprint stamping reconciled with PR-E.
- ✅ F7 (application) — `_patch_applied_records(...)` inline producer replaced by `_app_stage.apply(...)` post-stage observability; `apply_patch_set(...)` STAYS at `harness.py:16155`. Atomic dedup at `:16524`.
- ✅ F8 (acceptance) — `_ag_outcome_decision_record(...)` and `_post_eval_resolution_records(...)` inline producers replaced by `_accept_stage.decide(...)` post-stage observability; `decide_control_plane_acceptance(...)` STAYS at `harness.py:10347`. Atomic dual-site dedup at `:12235` and `:17716`.
- ✅ F9 (learning) — inline AG_RETIRED emit block replaced by `_lrn_stage.update(...)` post-stage observability; `resolve_terminal_on_plateau(...)` STAYS at `harness.py:11813`. Atomic dedup at `:11801-11828`.

### Outstanding follow-ups

- F2 (rca_evidence) — empty bundle problem; downstream consumer absent. Disposition options sketched in [`2026-05-05-phase-f2-rca-evidence-followup-plan.md`](./2026-05-05-phase-f2-rca-evidence-followup-plan.md). Recommended: Path C (self-source from eval_rows). Estimate: ~3-5 hours.
- F6 (gates) — gate-order divergence with harness inline path. Disposition options sketched in [`2026-05-05-phase-f6-gates-order-reconciliation-followup-plan.md`](./2026-05-05-phase-f6-gates-order-reconciliation-followup-plan.md). Recommended: Path C (align F6 module to harness order). Estimate: ~2-3 hours + one byte-stability replay test per gate site.
- F1 capture decorator wrap (Phase H out-of-scope follow-up per F+H plan §B prologue line 754) — ~30 min mechanical wrap.
```

---

## §4 Phase H Option 1 → ✅ (`roadmap.md:713+`)

Update the `## Phase H` heading on `roadmap.md:713`:

```markdown
## Phase H — GSO Run Output Contract: final unification on top of F + G — ✅ complete
```

Add at the top of the Phase H section body:

```markdown
> **Status:** Phase H ships in two completed batches.
>
> **Option 1 ✅ shipped first:** modules + tests + docs (Phase H Tasks
> T1-T11 and T14-T16 per [`2026-05-04-phase-h-gso-run-output-contract-plan.md`](./2026-05-04-phase-h-gso-run-output-contract-plan.md)).
> Deliverables: `optimization/run_output_contract.py` (PROCESS_STAGE_ORDER + path builders), `optimization/stage_io_capture.py` (per-stage I/O capture decorator), `optimization/run_output_bundle.py` (manifest/artifact_index/run_summary builders), `optimization/operator_process_transcript.py` (transcript renderers), `optimization/run_analysis_contract.py` (`artifact_index_marker` + `lever_loop_exit_manifest`), `tools/marker_parser.py` (consumes `GSO_ARTIFACT_INDEX_V1`), and per-module unit tests + `tests/integration/test_phase_h_bundle_smoke.py`.
>
> **Option 2 ✅ shipped via F+H combined wire-up plan:** harness wire-up (T12) and `dbutils.notebook.exit` JSON pointers (T13). The 8 Phase B commits (B9-B16) wrap each F2-F9 stage call with `wrap_with_io_capture(execute=stage.execute, stage_key=...)`. The 3 Phase C commits (C17-C19) tag the parent MLflow run, aggregate bundle inputs, assemble + upload `gso_postmortem_bundle/`, emit the `GSO_ARTIFACT_INDEX_V1` marker, extend the notebook exit JSON, and add an end-to-end bundle-populated smoke test.
>
> Phase E's pilot run validated the assembled artifact end-to-end. Bundle population confirmed by `tests/integration/test_phase_h_bundle_populated.py` and by manual inspection of the pilot's `parent_bundle_run_id` artifacts.
```

---

## §5 Phase E ✅ annotation (`roadmap.md:506-530`)

Update the `## Phase E` heading on `roadmap.md:506`:

```markdown
## Phase E — Final integration + merge — ✅ complete (merged to main on <YYYY-MM-DD>)
```

Add at the top of the Phase E section body:

```markdown
> **Status:** Pilot run validated against the 9-row matrix in
> [`2026-05-04-phase-e-pilot-run-and-merge-plan.md`](./2026-05-04-phase-e-pilot-run-and-merge-plan.md);
> all 9 rows GREEN (validation matrix at
> `docs/2026-05-04-phase-e-pilot-run-validation-matrix.md` records actual
> values). `raise_on_violation=True` flipped at `harness.py:17760` (Task 6
> commit). Replay-side decision-trace hard gate added at
> `lever_loop_replay.ContractViolationError` (Task 7 commit). Sanity PR
> opened, CI failed closed within 15 min, PR closed without merge (Task
> 8). Branch merged to main via squash + merge on <YYYY-MM-DD> (Task 9).
> Tag `gso-lossless-contract-v1` pushed.
```

Replace the prose bullets at lines 510-525 with a brief retrospective summary plus a forward pointer:

```markdown
**What happened:**

1. ✅ One real Lever Loop run on the airline benchmark (~2 hours).
2. ✅ Validated against the 9-row matrix (validator warnings, decision-trace completeness, transcript end-to-end, scoreboard sanity, bucketing labels, RCA loop state, marker discoverability, accuracy parity vs Phase A baseline). All rows GREEN.
3. ✅ `raise_on_violation=True` flipped (journey contract is now a hard gate).
4. ✅ Decision-trace replay-side fail-closed gate added.
5. ✅ Sanity PR proved CI fails closed; PR closed without merge.
6. ✅ Branch merged to main via squash; tag pushed.

**Detailed runbook:** [`2026-05-04-phase-e-pilot-run-and-merge-plan.md`](./2026-05-04-phase-e-pilot-run-and-merge-plan.md).
```

---

## §6 F+H wire-up line item ✅ (no separate row in current at-a-glance)

The current at-a-glance does not have a separate row for the F+H wire-up follow-up — it's tracked as the post-merge follow-up footer entry. After the §1 update applies, the F+H wire-up status appears as `✅ all shipped` in the post-merge follow-up row. No additional edit needed beyond §1.

---

## §7 Reasons section text update (`roadmap.md:152-155`)

Update reasons #6 (Phase F is stage-aligned) and #8 (Phase H lands GSO contract) to reflect completion:

Replace `roadmap.md:152` (reason #6):

```markdown
6. **Phase F is stage-aligned, not just byte-stable. ✅** Phases A–E and the PR-A→E batch made every stage-level decision (`EVAL_CLASSIFIED`, `CLUSTER_SELECTED`, `RCA_FORMED`, `STRATEGIST_AG_EMITTED`, `PROPOSAL_GENERATED`, `GATE_DECISION`, `PATCH_APPLIED`, `ACCEPTANCE_DECIDED`, `QID_RESOLUTION`, `AG_RETIRED`) typed and traceable. Phase F's modules half (the 9 typed stage modules under `optimization/stages/`) shipped first; the harness wire-up half landed via the F+H combined wire-up plan (Phase A: 6 atomic byte-stable commits replacing inline primitives with stage module calls). Two stages (F2 rca_evidence and F6 gates) are deferred to dedicated post-merge follow-ups due to design-choice ambiguity — neither blocks Phase E.
```

Replace `roadmap.md:154` (reason #8):

```markdown
8. **Phase H lands the GSO Run Output Contract on top of F-modules + G-lite. ✅** The process-first `operator_transcript.md`, the parent-run `gso_postmortem_bundle/` with per-stage `iter_NN/stages/<stage>/input.json + output.json + decisions.json`, and the `GSO_ARTIFACT_INDEX_V1` marker are all deterministic projections of the typed stage I/O. G-lite's registry powers the capture decorator's stage iteration. **Phase H shipped in two batches:** Option 1 (modules + tests + docs) landed first; T12+T13 (harness wire-up + dbutils exit JSON) landed via the F+H combined wire-up plan's Phase C commits.
```

Replace `roadmap.md:155` (reason #9):

```markdown
9. **The final unification reads as a tape. ✅** After the F+H wire-up landed, an iteration of `_run_lever_loop` reads as a linear sequence of `stages.evaluation.evaluate_post_patch` → `stages.clustering.form` → `stages.action_groups.select` → `stages.proposals.generate` → harness inline gates (F6 deferred) → `stages.application.apply` → `stages.evaluation.evaluate_post_patch` → `stages.acceptance.decide` → `stages.learning.update`, each wrapped with the per-stage I/O capture decorator (B9-B16). That tape is what the operator transcript renders, what the LLM postmortem reasons over, and what scoreboard/failure bucketing project into operator metrics.
```

---

## §8 New section: post-merge open work

Add at the end of the roadmap (after the existing Phase H section), a new section:

```markdown
## Post-merge open work

These plans are post-merge cleanups. None block any user-visible commitment.

| Plan | Estimate | Notes |
|---|---|---|
| [F2 rca_evidence follow-up](./2026-05-05-phase-f2-rca-evidence-followup-plan.md) | 3-5 hours (Path C) | Self-source per_qid_judge from eval_rows; wire F2 into harness; bundle becomes populated |
| [F6 gates order reconciliation](./2026-05-05-phase-f6-gates-order-reconciliation-followup-plan.md) | 2-3 hours (Path C) | Align module GATE_PIPELINE_ORDER to harness inline gate order; wire F6 sub-handlers |
| F1 capture decorator wrap | 30 min | Out-of-scope per F+H plan §B prologue. Wrap F1's `evaluate_post_patch` with `wrap_with_io_capture(...)` so `iter_NN/stages/01_evaluation_state/` populates |

When all three land, the bundle is fully populated and Phase F + Phase H are 100% complete in the strictest sense.
```

---

## Application checklist (executor)

- [ ] After Phase C Commit 19 smoke green: apply §1 row updates, §2 (E.0), §3 (Phase F flip), §4 (Phase H Option 1+2), §6 (no edit), §7 reasons, §8 post-merge section.
- [ ] After Phase E Task 9 merge complete: apply §1 row updates (Phase E line) and §5 (Phase E ✅).
- [ ] Commit message: `docs(roadmap): mark Phase F + H + F+H wire-up + E + E.0 complete`.
- [ ] Post-commit verification: `grep -nE "◐ partial|◐ in progress|◐ pending|deferred" packages/genie-space-optimizer/docs/2026-05-01-burn-down-to-merge-roadmap.md` returns ZERO matches for F/H/E phases (only acceptable matches are F2/F6 follow-up references in the post-merge open-work section).
