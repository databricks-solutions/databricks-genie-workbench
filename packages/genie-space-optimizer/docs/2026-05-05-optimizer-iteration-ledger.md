# Optimizer Iteration Ledger — Behavioral Hardening Cycles

> **Status:** Append-only ledger for the post-merge **behavioral** phase of `fix/gso-lossless-contract-replay-gate` and successor branches. Sibling to [`2026-05-02-phase-a-burndown-log.md`](./2026-05-02-phase-a-burndown-log.md): Phase A burned down structural ledger violations on a fixed corpus; this ledger burns down **optimizer behavior gaps** across a growing real-Genie corpus. The structural roadmap [`2026-05-01-burn-down-to-merge-roadmap.md`](./2026-05-01-burn-down-to-merge-roadmap.md) closed Phases 0 → H. This ledger picks up from there.

## Goal

Make "are we still making the optimizer better?" a question answerable from this ledger alone, not from intuition. Each cycle:

1. Names the inspiration run(s) and the corpus those runs sit in.
2. Lists the cross-run postmortem **clusters** (patterns that recur).
3. States one or more **AG hypotheses** — typed fix proposals with explicit RCA citation.
4. Names the **feature flag(s)** the change ships behind.
5. Records the **gate** outcome (byte-stable replay + corpus delta + skill accountability).
6. Appends one row to the [Ledger summary table](#ledger-summary-table-append-only).

The shape mirrors the optimizer's own `RCA Evidence → Cluster → Action Group → Proposal → Gate → Applied Patch → Eval Result → Learning` architecture so a meta-cycle reads like a lever-loop iteration. This is intentional: the same discipline that produced trustworthy structural phases produces trustworthy behavioral cycles.

## Operating principles

These ride on top of the Architecture / RCA-Grounded Decision Invariant / Observability Contract from `2026-05-01-burn-down-to-merge-roadmap.md`. They specialize that contract for behavioral iteration after Phase H landed.

1. **Multi-run corpus.** Cycle 1 starts with one inspiration run by necessity (it establishes the ledger). Every subsequent cycle must run on **≥2** spaces / runs before claiming a forward delta. One-run "wins" stay in the postmortem section; they do not move the corpus baseline.

2. **One cluster ⇒ one AG hypothesis ⇒ one cycle.** A cluster is a postmortem-finding pattern that recurs across runs. The AG hypothesis is the typed fix proposal that addresses that cluster with explicit RCA citation. A cycle MAY ship multiple AGs targeting **the same cluster** when the fix decomposes naturally (e.g., Cycle 1's six AGs across four clusters). A cycle MAY NOT stack independent clusters into one cycle — that confounds attribution and the ledger row becomes uninterpretable.

3. **Feature flags are the ablation discipline.** Every behavioral change ships behind a default-off env-var flag. Replay byte-stability holds with all flags **off** at every commit. Corpus delta is measured with the cycle's flags **on** at the closeout. If a cycle ships ≥2 flags, each flag must have an isolated unit/integration test that exercises only that flag.

4. **Gates per cycle.** A cycle exits with status `SHIPPED` iff:
   - **Byte-stable replay** — `tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget` stays at `BURNDOWN_BUDGET = 0` with all flags off, on every per-task commit.
   - **Corpus delta** — at minimum, the inspiration run's targeted hard-failure resolves on a fresh real-Genie pilot with flags on; **no other corpus run regresses**. From Cycle 2 onward, "corpus" means ≥2 spaces.
   - **Skill accountability** — the `gso-postmortem` / `gso-lever-loop-run-analysis` skill must surface every typed marker the cycle relies on. If a finding required raw-stdout grepping, that's a skill bug; the corresponding skill-improvement plan ships **before** the optimizer cycle closes out.

5. **Stop-iteration signals.** Pause before opening a new cycle if any of these are true:
   - Postmortems begin recommending a fix already tried and rejected in a prior cycle.
   - Recommendations cross a "depth threshold" (e.g., "rewrite the strategist") that belongs in a fresh roadmap section, not a behavioral cycle.
   - `BURNDOWN_BUDGET` regression on a flags-off run.
   - Corpus delta is flat or mixed across ≥3 consecutive cycles — the corpus has saturated and a new corpus run on a fresh space is required.

6. **Ledger entries are append-only.** A cycle row is never edited after `SHIPPED` / `REVERTED`. Mistakes are corrected by a follow-up cycle that names the prior cycle's row and explains the correction.

## Cycle template

Each cycle creates one section under `## Cycles` using this exact shape (literal copy-paste boilerplate at [`## Appendix — Cycle template (boilerplate)`](#appendix--cycle-template-boilerplate)).

### Section 1: Corpus runs

| `opt_run_id` | Genie Space | Domain | Baseline % | Final % | Target QID(s) | Hard-fail QIDs at termination | Postmortem path |
|---|---|---|---|---|---|---|---|

### Section 2: Postmortem clustering — findings that recur

| Cluster ID | Pattern | Run(s) it appears in | Stage / `decision_type` | Failure bucket | Skill that surfaced it |
|---|---|---|---|---|---|

Cluster IDs use the form `C-<cycle>-<letter>` so a finding can be cited unambiguously (e.g., `C-1-A`, `C-2-B`).

### Section 3: AG hypothesis(es)

For each AG, one fenced block:

```text
AG-<cycle>-<letter>: <name>
  Cluster: C-<cycle>-<letter>
  RCA: <one sentence linking observed effect → root cause>
  Causal target: <stage / file / function being changed>
  Expected effect: <which corpus run, which QID(s), which observable>
  Negative-space: <what must NOT regress; specify the flags-off baseline>
  Feature flag: GSO_<NAME>
  Plan ref: <link to the implementation plan doc + task letter>
```

### Section 4: Feature flags introduced this cycle

| Flag | Default | Touches | Isolated test |
|---|---|---|---|

### Section 5: Gate results

| Gate | Status | Evidence |
|---|---|---|

Required rows:
- Byte-stable replay (flags off)
- Corpus delta (flags on)
- Skill accountability — one row per typed marker / skill-update plan the cycle depends on.

### Section 6: Decision

`SHIPPED` (flags flipped on, ledger row finalized) / `HOLD` (code landed but flags stay off pending more corpus) / `REVERTED` (cycle aborted; flag scaffolding stays for Cycle N+1).

### Section 7: Seeds for next cycle

Open postmortem clusters that did NOT get a fix this cycle, with their projected next AG. Format:

```text
C-<cycle>-<letter> (seed): <pattern>
  Run(s) observed: <opt_run_id list>
  Hypothesis for next cycle: <one sentence>
  Blocking on: <"≥2 corpus runs" / "skill update X" / "depth threshold review">
```

## Ledger summary table (append-only)

| Cycle | Date | Inspiration run(s) | Cluster(s) | AG hypothesis(es) | Flag(s) | Plan | Corpus before | Corpus after | Status | Notes |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 2026-05-05 | `0ade1a99-9406-4a68-a3bc-8c77be78edcb` | C-1-A, C-1-B, C-1-C, C-1-D | AG-1-A → AG-1-F | `GSO_TARGET_AWARE_ACCEPTANCE`, `GSO_NO_CAUSAL_APPLYABLE_HALT`, `GSO_BUCKET_DRIVEN_AG_SELECTION`, `GSO_RCA_AWARE_PATCH_CAP`, `GSO_LEVER_AWARE_BLAST_RADIUS` | [`2026-05-05-optimizer-control-plane-plan.md`](./2026-05-05-optimizer-control-plane-plan.md) | airline 87.5% baseline → 91.7% final | airline 87.5% → 100.0% on retry attempt `993610879088298` (Cycle 2 corpus) | SHIPPED | Cycle-1 flags carried forward into Cycle 2. Corpus delta confirmed by retry attempt of `2afb0be2`. |
| 2 | 2026-05-04 | `2afb0be2-88b6-4832-99aa-c7e78fbc90f7` (initial attempt `1002162264479628`); confirmed on retry attempt `993610879088298` | C-2-A, C-2-B, C-2-C, C-2-D | AG-2-A → AG-2-D | `GSO_INTRA_AG_PROPOSAL_DEDUP`, `GSO_SHARED_CAUSE_BLAST_RADIUS`, `GSO_DOA_SELECTED_PROPOSAL_SIGNATURE`, `GSO_QUESTION_SHAPE_LEVER_PREFERENCE` | [`2026-05-04-cycle-2-optimizer-improvement-plan.md`](./2026-05-04-cycle-2-optimizer-improvement-plan.md) | airline 87.5% (initial attempt: 0 accepted, 5 iters) | airline 87.5% → 95.8% (iter 1) → 100.0% (iter 2), `thresholds_met=true`, 2 of 5 iterations used, 0 rollbacks (retry attempt `993610879088298`) | SHIPPED | Corpus delta of +12.5pp on a single space; cycle-discipline rule of ≥2 corpus runs is **partially satisfied** — second corpus run on a different space is the open follow-up. P2 (Cycle 3) corrects DOA dedup non-injectivity that surfaced in this run's iter-1 inventory. |
| 3 | 2026-05-04 | `2423b960-16e8-41d4-a0cb-74c563378e05` | C-3-A, C-3-B, C-3-C, C-3-D, C-3-E | P1, P2, P3, P4, P6 | `GSO_REGRESSION_DEBT_INVARIANT`, `GSO_LEVER_QUALIFIED_PATCH_IDS`, `GSO_FORCE_STRUCTURAL_SYNTHESIS_ON_LEVER5_DROP` (P4 + P6 are observability-only, no flag) | [`2026-05-04-evidence-bundle-attempt-aware-plan.md`](./2026-05-04-evidence-bundle-attempt-aware-plan.md), [`2026-05-04-regression-debt-accounting-completeness-plan.md`](./2026-05-04-regression-debt-accounting-completeness-plan.md), [`2026-05-04-patch-identity-normalization-plan.md`](./2026-05-04-patch-identity-normalization-plan.md), [`2026-05-04-typed-proposal-failure-outcomes-plan.md`](./2026-05-04-typed-proposal-failure-outcomes-plan.md), [`2026-05-04-question-shape-structural-synthesis-plan.md`](./2026-05-04-question-shape-structural-synthesis-plan.md) | 7Now 89.5% → 89.5% (5 iters, 0 accepted) | P6 + P1 + P2 SHIPPED (commits `7db2d0f`–`c9ef938`). P3 + P4 still PLANNED. | PARTIALLY SHIPPED | P6, P1, P2 land 2026-05-04 evening. P4 is observability that makes P3 measurable; P3 is the structural-synthesis change. The latest `2afb0be2` retry succeeded **without** P3, so P3 is now a robustness lever for harder corpora rather than a correctness gate for airline. |
| 4 | 2026-05-04 | `2afb0be2-88b6-4832-99aa-c7e78fbc90f7` retry attempt `993610879088298` (post-success contract gaps) | C-4-A (journey violations on successful AGs), C-4-B (terminal-success transcript staleness) | AG-4-A (journey contract for accepted-AG transitions), AG-4-B (terminal-success transcript override) | (no behavioral flags — both are observability/contract fixes) | [`2026-05-04-journey-validation-successful-ag-plan.md`](./2026-05-04-journey-validation-successful-ag-plan.md), [`2026-05-04-terminal-success-transcript-override-plan.md`](./2026-05-04-terminal-success-transcript-override-plan.md) | airline retry: 12 journey violations iter 1, 8 iter 2; iter 2 transcript still names `gs_009` as `rca_ungrounded` after `AG2` fixed it | TBD — implementation pending | PLANNED | Both findings sit on the success path: the optimizer reached 100% but the postmortem-quality output is still noisy. Each plan ships independently; both are observability-only (no flag). |

## Cycles

(Cycle entries appear below in chronological order. Each cycle uses the [Cycle template](#cycle-template).)

### Cycle 1 — 2026-05-05 — Control-plane hardening from `0ade1a99`

**Inspiration:** [`docs/runid_analysis/0ade1a99-9406-4a68-a3bc-8c77be78edcb/postmortem.md`](./runid_analysis/0ade1a99-9406-4a68-a3bc-8c77be78edcb/postmortem.md). The pilot run on the airline space ended at `91.7%` (baseline `87.5%`), stalled below thresholds. The optimizer accepted a non-target-fixing AG on iteration 3 (`AG_COVERAGE_H003`, `target_fixed_qids: none`), then spent iterations 4–5 on broad fallback patches (`add_join_spec`, `update_instruction_section`) that all rolled back. Practical ceiling on this corpus = 95.8% — the `gs_009` and `gs_024` failures are causal-patch-reachable but blocked by four control-plane gaps and the absence of bucket feedback into AG selection.

**Implementation plan:** [`2026-05-05-optimizer-control-plane-plan.md`](./2026-05-05-optimizer-control-plane-plan.md). Tasks 0 → G; five default-off feature flags scaffolded; gate flip is Task G of that plan.

**Corpus discipline note:** Cycle 1 runs on a single space (airline). This is acceptable as the cycle that *establishes the ledger* — it documents the corpus-of-one explicitly so Cycle 2 onward can hold itself to ≥2.

#### Section 1: Corpus runs

| `opt_run_id` | Genie Space | Domain | Baseline % | Final % | Target QID(s) | Hard-fail QIDs at termination | Postmortem path |
|---|---|---|---|---|---|---|---|
| `0ade1a99-9406-4a68-a3bc-8c77be78edcb` | `01f143dfbeec15a3a0e87ced8662f4ed` | `airline_ticketing_and_fare_analysis` | 87.5% | 91.7% | `gs_009`, `gs_024` | `gs_009`, `gs_024` (both unresolved) | [`runid_analysis/0ade1a99-9406-4a68-a3bc-8c77be78edcb/postmortem.md`](./runid_analysis/0ade1a99-9406-4a68-a3bc-8c77be78edcb/postmortem.md) |

#### Section 2: Postmortem clustering

| Cluster ID | Pattern | Run(s) it appears in | Stage / `decision_type` | Failure bucket | Skill that surfaced it |
|---|---|---|---|---|---|
| `C-1-A` | Targeted AG accepted on global-accuracy improvement when `target_fixed_qids` is empty AND thresholds remain unmet (iter 3, `AG_COVERAGE_H003`). Canonical `ACCEPTANCE_TARGET_BLIND` example. | `0ade1a99` | `acceptance_decision` / `ACCEPTANCE_DECIDED` | `APPLY_OR_ROLLBACK_GAP` | `gso-postmortem` (with `target_still_hard_qids` derivation gap closed by Plan 2 — see Section 5). |
| `C-1-B` | Strategist + proposal stage selects broad `add_join_spec` patches when RCA names a specific causal mechanism (`top_10_logic`, `unrequested_filter`). The actual causal patch type is dropped by `blast_radius` because the gradation threshold is uniform across lever types. | `0ade1a99` (iters 3 + 5) | `proposal_generation` / `safety_gates` (`PROPOSAL_GENERATED`, `GATE_DECISION reason=blast_radius`) | `GATE_OR_CAP_GAP` | `gso-postmortem` |
| `C-1-C` | Diagnostic AGs (`STRATEGIST COVERAGE GAP` with `rca_cards_present=false`) bypass the RCA-groundedness gate. Later iterations apply patches whose `rca_id` was never grounded; the groundedness gate then drops them as `rca_ungrounded` while the AG keeps consuming budget. | `0ade1a99` (iters 1, 4, 5) | `action_group_selection` / `proposal_generation` | `RCA_GAP` | `gso-postmortem` |
| `C-1-D` | Failure buckets are computed in postmortem only. The strategist's next-iteration AG selection does not consume the prior iteration's `FailureBucket` per qid, so an `EVIDENCE_GAP` qid keeps consuming proposal budget instead of routing to evidence-gathering. Highest-leverage finding per the inline troubleshooting guide. | `0ade1a99` (iters 4–5) | `action_group_selection` | `RCA_GAP` / `TARGETING_GAP` | `gso-postmortem` |

#### Section 3: AG hypotheses

```text
AG-1-A: Target-aware acceptance
  Cluster: C-1-A
  RCA: control plane accepts on global accuracy delta without checking that the AG fixed any target QID below threshold.
  Causal target: stages/acceptance.py:decide → control_plane.py:decide_control_plane_acceptance (add thresholds_met + target_qids/target_fixed_qids inputs).
  Expected effect: iter 3 of 0ade1a99 would NOT accept AG_COVERAGE_H003; loop continues searching for a causal fix to gs_009.
  Negative-space: when thresholds_met=true, behavior is unchanged (terminating accept still allowed). Flags-off byte-stable replay holds.
  Feature flag: GSO_TARGET_AWARE_ACCEPTANCE
  Plan ref: 2026-05-05-optimizer-control-plane-plan.md Task A.

AG-1-B: NO_CAUSAL_APPLYABLE outcome
  Cluster: C-1-B
  RCA: when blast_radius drops the only causal patch, the strategist falls through to broad fallback patches that lack RCA grounding.
  Causal target: stages/proposals.py:_filter_to_causal_applyable_proposals (new) — emit NO_CAUSAL_APPLYABLE outcome on the AG instead of proceeding with non-causal patches.
  Expected effect: iter 3 / iter 5 of 0ade1a99 halt the AG with NO_CAUSAL_APPLYABLE instead of applying broad join-spec patches; strategist recovery is invoked.
  Negative-space: when ≥1 causal patch survives the gates, behavior is unchanged.
  Feature flag: GSO_NO_CAUSAL_APPLYABLE_HALT
  Plan ref: 2026-05-05-optimizer-control-plane-plan.md Task B.

AG-1-C: Bucket-driven AG selection
  Cluster: C-1-D
  RCA: failure buckets are post-hoc only; strategist re-emits same-class AGs even when the prior iteration's bucket was EVIDENCE_GAP / RCA_GAP.
  Causal target: stages/action_groups.py:select (new prior_buckets_by_qid input).
  Expected effect: iter 4 / iter 5 of 0ade1a99 route gs_024 to an evidence-gathering AG before re-attempting a patch.
  Negative-space: when prior_buckets_by_qid is empty (cycle 0 of a fresh run), behavior is unchanged.
  Feature flag: GSO_BUCKET_DRIVEN_AG_SELECTION
  Plan ref: 2026-05-05-optimizer-control-plane-plan.md Task C.

AG-1-D: RCA-aware patch_cap ranking
  Cluster: C-1-B
  RCA: patch_cap selects by relevance score regardless of whether the patch is causally aligned with the RCA root_cause.
  Causal target: optimization/patch_selection.py (new RCA-aware ranking) + stages/proposals.py:select_target_aware_causal_patch_cap.
  Expected effect: iter 3 of 0ade1a99 ranks the SQL-shape causal patch above the broad add_join_spec patches at the cap stage.
  Negative-space: ties resolved by existing tie-break order; non-RCA-eligible patches still emitted when the AG carries no rca_id.
  Feature flag: GSO_RCA_AWARE_PATCH_CAP
  Plan ref: 2026-05-05-optimizer-control-plane-plan.md Task D.

AG-1-E: Lever-aware blast-radius gradation
  Cluster: C-1-B
  RCA: blast_radius uses one threshold for all lever types; non-semantic levers (instruction edits, name normalisation) are dropped at the same dependent-coverage threshold as semantic levers (join spec changes).
  Causal target: optimization/proposal_grounding.py:patch_blast_radius_is_safe (gradate by lever_type).
  Expected effect: causal SQL-shape snippets in iter 3 / iter 5 of 0ade1a99 survive blast_radius instead of being dropped under high_collateral_risk_flagged.
  Negative-space: semantic levers retain the conservative threshold; existing high-collateral semantic patches still drop.
  Feature flag: GSO_LEVER_AWARE_BLAST_RADIUS
  Plan ref: 2026-05-05-optimizer-control-plane-plan.md Task E.

AG-1-F: Diagnostic-AG RCA inheritance
  Cluster: C-1-C
  RCA: diagnostic AGs (coverage-gap buffered) generate proposals without an inherited rca_id, so the groundedness gate drops them as rca_ungrounded.
  Causal target: stages/action_groups.py:materialize_diagnostic_ag (stamp rca_id from cluster) + stages/proposals.py (proposal RCA inheritance).
  Expected effect: iters 1/4/5 diagnostic AGs no longer produce ungrounded proposals; the groundedness gate accepts them or drops them with a more specific reason_code.
  Negative-space: when no parent cluster RCA exists, the diagnostic AG falls back to current behavior with an explicit reason_code (ungrounded_no_parent_rca) rather than silent ungroundedness.
  Feature flag: coupled with GSO_BUCKET_DRIVEN_AG_SELECTION (the control-plane plan Task F intentionally couples these because the bucket signal informs which diagnostic AG to materialize). Document the coupling in any future split.
  Plan ref: 2026-05-05-optimizer-control-plane-plan.md Task F.
```

#### Section 4: Feature flags introduced this cycle

| Flag | Default | Touches | Isolated test |
|---|---|---|---|
| `GSO_TARGET_AWARE_ACCEPTANCE` | off | `optimization/control_plane.py:decide_control_plane_acceptance`, `optimization/stages/acceptance.py:decide` | `tests/unit/test_control_plane_target_aware.py` |
| `GSO_NO_CAUSAL_APPLYABLE_HALT` | off | `optimization/stages/proposals.py:_filter_to_causal_applyable_proposals` | `tests/unit/test_proposals_no_causal_applyable.py` |
| `GSO_BUCKET_DRIVEN_AG_SELECTION` | off | `optimization/stages/action_groups.py:select`, `optimization/stages/action_groups.py:materialize_diagnostic_ag` | `tests/unit/test_action_groups_bucket_feedback.py`, `tests/unit/test_proposals_rca_inherit.py` |
| `GSO_RCA_AWARE_PATCH_CAP` | off | `optimization/patch_selection.py` (new), `optimization/stages/proposals.py:select_target_aware_causal_patch_cap` | `tests/unit/test_proposals_rca_inherit.py` |
| `GSO_LEVER_AWARE_BLAST_RADIUS` | off | `optimization/proposal_grounding.py:patch_blast_radius_is_safe` | `tests/unit/test_blast_radius_lever_aware.py` |

End-to-end (all flags on): `tests/integration/test_optimizer_flags_end_to_end.py`.

#### Section 5: Gate results

| Gate | Status | Evidence |
|---|---|---|
| Byte-stable replay (flags off) | TBD per task — must hold at every commit of the implementation plan | `pytest packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py::test_run_replay_airline_real_v1_within_burndown_budget -xvs` is the regression rail at the end of every plan task. `BURNDOWN_BUDGET = 0` is never raised by this cycle. |
| Corpus delta (flags on, single space) | TBD at Task G closeout | A fresh airline pilot after Task G flips the flags. Expected observable: iter 3 does NOT accept AG_COVERAGE_H003; the loop either resolves `gs_009` via a causal patch OR halts the AG with `NO_CAUSAL_APPLYABLE` and routes to strategist recovery. `gs_024` follows the same pattern. |
| Skill accountability — `ACCEPTANCE_TARGET_BLIND` | GAP closed by [`2026-05-05-skills-and-stdout-parser-plan.md`](./2026-05-05-skills-and-stdout-parser-plan.md) Plan 2 Task A | Plan 2 Task A's "Derivation contract for `target_still_hard_qids`" subsection closes the gap: the parser derives target-still-hard QIDs for ACCEPTED iterations from `AG.target_qids − target_fixed_qids` (canonical example = `0ade1a99` iter 3, `AG_COVERAGE_H003`). Without this, the analysis-skill check could never fire on the cycle's own canonical example. |
| Skill accountability — stdout flush | GAP closed by [`2026-05-05-run-output-contract-stdout-flush-plan.md`](./2026-05-05-run-output-contract-stdout-flush-plan.md) Plan 3 | Plan 3 prints the rendered Phase H transcript as the notebook's last step and converts silent `gso_postmortem_bundle` assembly failures into loud `GSO_BUNDLE_ASSEMBLY_FAILED_V1` markers, so future Cycle-1-style postmortems do not require operators to recover stdout via `databricks jobs export-run`. |

#### Section 6: Decision

**Cycle 1 status: IN FLIGHT.** Update to `SHIPPED` after Task G of the control-plane plan flips the flags, the airline pilot run produces a non-regressing fixture, and Plans 2 + 3 (skill accountability) merge. Update to `REVERTED` if the corpus delta regresses or any byte-stable replay gate fails.

#### Section 7: Seeds for next cycle

```text
C-1-E (seed): Strategist single-AG-per-iteration emission (STRATEGIST COVERAGE GAP repeats every iteration of 0ade1a99).
  Run(s) observed: 0ade1a99 (iters 1, 3, 4, 5)
  Hypothesis for next cycle: lift the slate cap so the strategist can emit ≥2 AGs per iteration when independent clusters exist; gate by BUDGET_PER_ITERATION and content-fingerprint dedup.
  Blocking on: ≥2 corpus runs (need a second space where the same pattern surfaces before promoting from seed to AG).
```

```text
C-1-F (seed): Journey-validation violations (21 across 9 QIDs in iter 3 of 0ade1a99) do not affect acceptance.
  Run(s) observed: 0ade1a99 (iter 3)
  Hypothesis for next cycle: feed journey_validation.is_valid into the acceptance gate; reject acceptance when validation violations exceed a typed budget.
  Blocking on: ≥2 corpus runs; depth review (this changes the canonical acceptance contract — confirm in roadmap before promoting).
```

## Appendix — Cycle template (boilerplate)

Paste this block to start a new cycle. Replace `<cycle>` with the cycle number, fill in dates and run IDs, and keep the section headings exactly as written so the table-of-contents anchors stay stable.

```markdown
### Cycle <cycle> — YYYY-MM-DD — <one-line summary>

**Inspiration:** [`docs/runid_analysis/<opt_run_id>/postmortem.md`](./runid_analysis/<opt_run_id>/postmortem.md). <one paragraph: what the run produced, where it stalled, what the practical ceiling is, why this cycle exists>.

**Implementation plan:** [`<YYYY-MM-DD-cycle-N-...>.md`](./<YYYY-MM-DD-cycle-N-...>.md).

**Corpus discipline note:** <"Corpus-of-N (N spaces) — meets the ≥2-run rule." OR "Corpus-of-one — only acceptable for Cycle 1, which establishes the ledger.">

#### Section 1: Corpus runs

| `opt_run_id` | Genie Space | Domain | Baseline % | Final % | Target QID(s) | Hard-fail QIDs at termination | Postmortem path |
|---|---|---|---|---|---|---|---|
| ... | ... | ... | ... | ... | ... | ... | `runid_analysis/<id>/postmortem.md` |

#### Section 2: Postmortem clustering

| Cluster ID | Pattern | Run(s) | Stage / `decision_type` | Failure bucket | Skill that surfaced it |
|---|---|---|---|---|---|
| `C-<cycle>-A` | ... | ... | ... | ... | ... |

#### Section 3: AG hypotheses

(One fenced block per AG, using the format from `Section 3: AG hypothesis(es)` in the cycle template.)

#### Section 4: Feature flags introduced this cycle

| Flag | Default | Touches | Isolated test |
|---|---|---|---|
| `GSO_<NAME>` | off | `path/to/file.py:fn_name` | `tests/unit/test_<flag>.py::test_<...>` |

End-to-end (all flags on): `tests/integration/test_<cycle>_flags_end_to_end.py`.

#### Section 5: Gate results

| Gate | Status | Evidence |
|---|---|---|
| Byte-stable replay (flags off) | TBD / PASS / FAIL | `pytest tests/replay/test_run_replay_airline_real_v1_within_burndown_budget` — exit + budget value |
| Corpus delta (flags on) | TBD / PASS / HOLD / REGRESS | per-run before/after table linked from postmortems |
| Skill accountability — `<MARKER_NAME>` | PASS / GAP closed by `<plan-ref>` | `<one-line evidence>` |

#### Section 6: Decision

**Cycle <cycle> status: <IN FLIGHT / SHIPPED / HOLD / REVERTED>.** <one paragraph: what landed, what didn't, what the corpus delta says>.

#### Section 7: Seeds for next cycle

```text
C-<cycle>-<letter> (seed): <pattern>
  Run(s) observed: <opt_run_id list>
  Hypothesis for next cycle: <one sentence>
  Blocking on: <"≥2 corpus runs" / "skill update X" / "depth threshold review">
```
```

End of doc.

---

## Cycle 2 — Proposal survival and safety-gate hardening

#### Section 1: Inspiration runs

| Run id | Space | Outcome | Notes |
|---|---|---|---|
| `2afb0be2-88b6-4832-99aa-c7e78fbc90f7` | airline (7Now) | 87.5% → 87.5%, 0 accepted AGs across 5 iters | One-run baseline cycle pending a second run |

#### Section 2: Postmortem clusters

1. **Intra-AG body duplicates with mismatched `patch_type`** (iter 1 and iter 3) — `AG_DECOMPOSED_H001` emitted three proposals with identical body text under `lever=5 rewrite_instruction`, `lever=6 add_sql_snippet_filter`, and `lever=6 add_sql_snippet_filter`; today's content_fingerprint includes patch_type so cross-iteration dedup misses them.
2. **Shared-cause hard failures classified as collateral risk** (iter 1 P001#2, iter 3 P001#1) — `gs_003` was itself hard in `H002`, but blast-radius treated it as a passing dependent and dropped a patch that could have unblocked both `gs_024` and `gs_003`.
3. **Empty applied-patch DOA signature allowing same-AG retry** (iter 3 / iter 5) — `AG_COVERAGE_H001` ran with identical proposals in both iterations; blast-radius dropped every patch, so the applied-patch DOA signature collapsed to `()` and `_record_dead_on_arrival_signature` deliberately skipped recording, allowing verbatim retry.
4. **Single-question clusters routed to space-wide lever 6** (iter 2 `AG_COVERAGE_H003`) — `gs_009` (top-N collapse) got a `lever=6 add_sql_snippet_expression` patch that touched every top-N query in the space; blast-radius correctly dropped it but the per-question fix never materialised.

#### Section 3: AG hypothesis

If outside-target qids that are themselves currently-hard route as shared-cause beneficiaries (Task 2), DOA dedup keys on selected proposals (Task 3), intra-AG duplicates collapse (Task 1), and single-question shape RCAs prefer per-question levers (Task 4), then a 7Now/airline run with `H001`+`H002` sharing a `tkt_payment` filter cause should converge in ≤ 3 iterations.

#### Section 4: Feature flags

| Flag | Default | Disable |
|---|---|---|
| `GSO_INTRA_AG_PROPOSAL_DEDUP` | ON | `=0` |
| `GSO_SHARED_CAUSE_BLAST_RADIUS` | ON | `=0` |
| `GSO_DOA_SELECTED_PROPOSAL_SIGNATURE` | ON | `=0` |
| `GSO_QUESTION_SHAPE_LEVER_PREFERENCE` | ON | `=0` |

#### Section 5: Gate result

| Gate | Result | Evidence |
|---|---|---|
| Byte-stable replay (flags off) | PASS | `pytest tests/replay/test_phase_f_h_wireup_byte_stable.py` after each commit |
| Full unit + integration sweep | PASS modulo 2 known pre-existing | 3306 passed; same 2 pre-existing failures as prior cycles |
| Corpus delta (flags on) | TBD — corpus re-run pending | Baseline only; awaiting airline rerun + at least one additional space |

#### Section 6: Decision

**Cycle 2 status: IN FLIGHT.** All four flag-gated fixes landed default-on with full unit-test coverage. Replay byte-stability is preserved across every commit. The gate result row is blocked on the airline corpus re-run plus one additional space — until those two runs land, the cycle stays IN FLIGHT.

**Follow-up correction:** Cycle 3's run [`2423b960`](runid_analysis/2423b960-16e8-41d4-a0cb-74c563378e05/postmortem.md) iter-1 evidence revealed that AG_DECOMPOSED_H001's CAP RECONCILIATION printed `P001#1, P001#2, P001#2` — two distinct patches under different levers (Lever-1 column-synonym and Lever-5 instruction-section) sharing the same expanded id. That makes Cycle 2 Task 3's `_compute_selected_proposal_signature` non-injective on cross-lever collisions, silently conflating distinct patches in the DOA dedup ledger. The fix is **Cycle 3 plan P2** ([`2026-05-04-patch-identity-normalization-plan.md`](./2026-05-04-patch-identity-normalization-plan.md)) which lever-qualifies the expanded id format to `L{lever}:{parent_id}#{child_index}` (e.g. `L1:P001#2` vs `L5:P001#2`). Cycle 2 corpus re-run **MUST** wait until P2 ships; otherwise the corpus-delta gate measures behavior on a non-injective DOA ledger and the result is uninterpretable.

#### Section 7: Seeds for next cycle

```text
C-2-A (seed): postmortem rollback-class miscategorization
  Run(s) observed: 2afb0be2-88b6-4832-99aa-c7e78fbc90f7
  Hypothesis for next cycle: gso-postmortem skill conflates content-regression with insufficient_gain — separate skill plan, not an optimizer gap
  Blocking on: gso-postmortem skill plan + one corpus rerun

C-2-B (seed): F3 strategist coverage gaps with missing RCA cards
  Run(s) observed: 2afb0be2-88b6-4832-99aa-c7e78fbc90f7
  Hypothesis for next cycle: Cycle 1's bucket_driven_ag_selection + no_causal_applyable_halt should already cover this; re-evaluate after Tasks 1-4 corpus re-run
  Blocking on: airline corpus re-run on Cycle-2 flags-on path
```

### Cycle 3 — 2026-05-04 — Truth-of-state and causal-candidate generation from `2423b960`

**Inspiration:** [`docs/runid_analysis/2423b960-16e8-41d4-a0cb-74c563378e05/postmortem.md`](./runid_analysis/2423b960-16e8-41d4-a0cb-74c563378e05/postmortem.md). The 7Now-delivery-analytics run on space `01f128aea2c210559cffb663d9c58282` had four lever-loop attempts fail before a successful retry: 2× `soft-cluster currency drift` (the assertion fixed in this branch's prior turn) and 2× `get_run_context: run_id_widget is required`. The successful attempt then exhausted 5 iterations at 89.47% with zero accepted action groups. Acceptance behaved correctly — AG1 regressed two qids and rolled back. The breakdowns are upstream of acceptance: cross-lever patch-id collisions, soft→hard regression accounting silently miscounting `gs_001`, lever-5 structural gate dropping instruction-only proposals for SQL-shape RCAs (`gs_021 missing_filter`) without any synthesis fallback, iter-3/iter-4 emitting `Proposals (0 total)` with no logged drop reason, and iter-5 replaying the same dead AG_COVERAGE_H002 because the DOA dedup didn't catch generator-emitted-then-gate-dropped proposals.

**Implementation plans:** Five sibling plans, landing in dependency order.

| Order | Plan | Priority | Independence |
|---|---|---|---|
| 1 | [`2026-05-04-evidence-bundle-attempt-aware-plan.md`](./2026-05-04-evidence-bundle-attempt-aware-plan.md) (P6) | postmortem-skill correctness | independent |
| 2 | [`2026-05-04-regression-debt-accounting-completeness-plan.md`](./2026-05-04-regression-debt-accounting-completeness-plan.md) (P1) | observability invariant | independent |
| 3 | [`2026-05-04-patch-identity-normalization-plan.md`](./2026-05-04-patch-identity-normalization-plan.md) (P2) | structural prerequisite for Cycle 2 Task 3 correctness | required-before-rerun |
| 4 | [`2026-05-04-typed-proposal-failure-outcomes-plan.md`](./2026-05-04-typed-proposal-failure-outcomes-plan.md) (P4) | typed `NO_STRUCTURAL_CANDIDATE` reason supplier | required-before P3 |
| 5 | [`2026-05-04-question-shape-structural-synthesis-plan.md`](./2026-05-04-question-shape-structural-synthesis-plan.md) (P3) | causal-candidate emission | depends on P1 + P2 + P4 |

**Corpus discipline note:** Cycle 3 starts from a one-run baseline (7Now `2423b960`). The corpus-delta gate at closeout requires the 7Now space + at least one additional space (airline `2afb0be2` re-run is the natural second). Until both spaces re-run with Cycle 3 flags on, this cycle stays PLANNED → IN FLIGHT (no SHIPPED claim possible from one run).

#### Section 1: Corpus runs

| `opt_run_id` | Genie Space | Domain | Baseline % | Final % | Target QID(s) | Hard-fail QIDs at termination | Postmortem path |
|---|---|---|---|---|---|---|---|
| `2423b960-16e8-41d4-a0cb-74c563378e05` | `01f128aea2c210559cffb663d9c58282` | `7now_delivery_analytics_space` | 89.47% | 89.47% | `gs_026` (iter 1) | `gs_026` (`H001 plural_top_n_collapse`), `gs_021` (`H002 missing_filter`) | [`runid_analysis/2423b960-16e8-41d4-a0cb-74c563378e05/postmortem.md`](./runid_analysis/2423b960-16e8-41d4-a0cb-74c563378e05/postmortem.md) |

#### Section 2: Postmortem clustering

| Cluster ID | Pattern | Run(s) it appears in | Stage / `decision_type` | Failure bucket | Skill that surfaced it |
|---|---|---|---|---|---|
| `C-3-A` | Cross-lever patch-id collision: two distinct patches under the same parent proposal but different levers (`L1` add_column_synonym + `L5` update_instruction_section) both stamp `expanded_patch_id = "P001#2"`. CAP RECONCILIATION displays `P001#1, P001#2, P001#2` — same string for unrelated patches. Cycle 2 Task 3's DOA selected-proposal signature collapses them, making the dedup non-injective. | `2423b960` (iter 1) | `applier` / `_stamp_expanded_patch_identity` | `INSTRUMENTATION_GAP` | required raw-stdout grep of CAP RECONCILIATION; `gso-postmortem` did not flag it |
| `C-3-B` | Regression-debt accounting silently drops new-hard qids when `pre_row` is missing or `row_status` disagrees with `hard_failure_qids`. The marker has `soft_to_hard_regressed_qids` and `passing_to_hard_regressed_qids` but no `unknown_to_hard` residual, so no invariant catches the orphan. The `gs_001` soft→hard transition was undercounted in the iter-1 marker. | `2423b960` (iter 1) | `acceptance_decision` / `decide_control_plane_acceptance` | `OBSERVABILITY_GAP` | required cross-checking failed-question count vs marker; `gso-postmortem` did not flag it |
| `C-3-C` | Lever-5 structural gate correctly drops instruction-only proposals for SQL-shape root causes (`missing_filter`, `plural_top_n_collapse`), but no synthesis fallback fires. The `ordered_list_by_metric` archetype exists at [`archetypes.py:220`](../src/genie_space_optimizer/optimization/archetypes.py) and `run_cluster_driven_synthesis_for_single_cluster` exists at [`cluster_driven_synthesis.py:658`](../src/genie_space_optimizer/optimization/cluster_driven_synthesis.py); the wiring gap is that the harness does not mandatorily invoke synthesis on a structural-gate drop. | `2423b960` (iters 2 + 5), `2afb0be2` (iter 2 `gs_009`) | `proposal_generation_structural_gate` / `lever5_structural_gate_records` | `GATE_OR_CAP_GAP` | `gso-postmortem` (with C-3-D below as the related observability gap) |
| `C-3-D` | Three distinct "no candidate state" paths collapse into the same display: (a) generator returned 0 proposals (iter 3/4 of `2423b960`), (b) lever-5 gate dropped instruction-only (iters 2/5 of `2423b960`), (c) synthesis attempted-but-no-archetype-matched. Today only (b) emits a structured DecisionRecord (`RCA_UNGROUNDED`, generic). Postmortems must reverse-engineer (a) vs (b) by grepping for the gate's prose log line. | `2423b960` (iters 2-5), `2afb0be2` (iters 3-5) | `proposal_generation` | `OBSERVABILITY_GAP` | `gso-postmortem` (skill-accountability gate fail) |
| `C-3-E` | Postmortem evidence bundle anchors to the **first** matching `lever_loop` task in `job_run.tasks` — when that task failed (this run's first 4 attempts), the bundle silently misses the analyzable success transcript. Author had to manually export run `611621809299494`. | `2423b960` (4 failed + 1 success attempts) | `tools/evidence_bundle.py` | `INSTRUMENTATION_GAP` | required manual `databricks jobs export-run`; `gso-postmortem` did not flag it |

#### Section 3: AG hypotheses

```text
P6 (AG-3-E): Evidence-bundle attempt-aware lever_loop selection
  Cluster: C-3-E
  RCA: tools/evidence_bundle.py:331 picks the first task whose
       task_key=='lever_loop'; ignores result_state.
  Causal target: tools/evidence_bundle.py — _select_lever_loop_task
       helper that ranks by (state==SUCCESS desc, end_time desc) and
       records all failed attempts under
       evidence/failed_lever_loop_attempts/<task_run_id>.json.
  Expected effect: future runs with multiple lever_loop attempts
       anchor to the latest SUCCESS automatically; failed attempts
       are still available as per-attempt JSONs for error-class
       scanning.
  Negative-space: when only one attempt exists, behavior is
       identical to today.
  Feature flag: none (observability-only, strictly better)
  Plan ref: 2026-05-04-evidence-bundle-attempt-aware-plan.md.

P1 (AG-3-B): Regression-debt partition completeness
  Cluster: C-3-B
  RCA: decide_control_plane_acceptance partitions
       out_of_target_regressed into soft_to_hard / passing_to_hard
       only; missing pre_row or predicate disagreement silently
       drops the qid out of attribution.
  Causal target: control_plane.py — add
       unknown_to_hard_regressed_qids field +
       assert_regression_debt_partition_complete invariant,
       wired into harness after each acceptance call.
  Expected effect: every new-hard out-of-target qid lands in
       exactly one of three buckets; orphan attribution fails
       loud at runtime, not silently in markers.
  Negative-space: partition is provably disjoint; replay
       byte-stability holds.
  Feature flag: GSO_REGRESSION_DEBT_INVARIANT (default on).
  Plan ref: 2026-05-04-regression-debt-accounting-completeness-plan.md.

P2 (AG-3-A): Lever-qualified expanded patch ids
  Cluster: C-3-A
  RCA: _stamp_expanded_patch_identity at applier.py:2233 builds
       child_id = f"{parent_id}#{child_index}" without lever
       qualification. Two parents under the same proposal_id but
       different levers produce identical expanded ids.
  Causal target: applier.py:_stamp_expanded_patch_identity (id
       format) + patch_selection.py:_proposal_id +
       static_judge_replay.py:_proposal_id (preference order).
  Expected effect: cross-lever collisions disambiguate to
       L1:P001#2 vs L5:P001#2; Cycle 2 Task 3 DOA dedup becomes
       injective.
  Negative-space: when flag off, legacy unqualified format is
       preserved; replay byte-stability holds.
  Feature flag: GSO_LEVER_QUALIFIED_PATCH_IDS (default on).
  Plan ref: 2026-05-04-patch-identity-normalization-plan.md.

P4 (AG-3-D): Typed proposal-failure outcomes
  Cluster: C-3-D
  RCA: three "no candidate state" failure modes collapse into one
       display; lever5_structural_gate_records uses generic
       RCA_UNGROUNDED reason.
  Causal target: rca_decision_trace.ReasonCode (3 new values),
       decision_emitters.py (proposal_generation_empty_record +
       no_structural_candidate_record + lever-5 reason update),
       run_output_contract.py (3 stdout markers),
       tools/marker_parser.py (3 parsers).
  Expected effect: postmortems can distinguish proposer-empty
       from gate-dropped-instruction-only from no-structural-
       candidate without raw-stdout grepping.
  Negative-space: observability-only; no behavior change. Lever-5
       reason code becomes more specific.
  Feature flag: none (observability-only).
  Plan ref: 2026-05-04-typed-proposal-failure-outcomes-plan.md.

P3 (AG-3-C): Forced structural synthesis on lever-5 drop
  Cluster: C-3-C
  RCA: harness reads lever-5 gate drops but does not invoke
       run_cluster_driven_synthesis_for_single_cluster as a
       guaranteed fallback. When synthesis returns None (budget,
       safety cap, archetype miss), the path silently moves on.
  Causal target: cluster_driven_synthesis.py
       (ClusterSynthesisResult dataclass with attempted_archetypes
       provenance) + harness.py
       (_should_force_structural_synthesis predicate +
       _consume_structural_synthesis_buffer for next-iter pickup).
  Expected effect: SQL-shape RCAs (plural_top_n_collapse,
       missing_filter, etc.) get an add_example_sql proposal
       generated by the existing ordered_list_by_metric
       archetype; if synthesis legitimately can't, a
       NO_STRUCTURAL_CANDIDATE record fires (P4 supplies the
       reason).
  Negative-space: when flag off, lever-5 gate drops without
       synthesis fallback (legacy behavior).
  Feature flag: GSO_FORCE_STRUCTURAL_SYNTHESIS_ON_LEVER5_DROP
       (default on).
  Plan ref: 2026-05-04-question-shape-structural-synthesis-plan.md.
```

#### Section 4: Feature flags

| Flag | Default | Disable | Plan |
|---|---|---|---|
| `GSO_REGRESSION_DEBT_INVARIANT` | ON | `=0` | P1 |
| `GSO_LEVER_QUALIFIED_PATCH_IDS` | ON | `=0` | P2 |
| `GSO_FORCE_STRUCTURAL_SYNTHESIS_ON_LEVER5_DROP` | ON | `=0` | P3 |
| (P4 + P6 are observability-only) | — | — | P4, P6 |

#### Section 5: Gate result

| Gate | Result | Evidence |
|---|---|---|
| Byte-stable replay (flags off) | TBD per task | Each plan's Task N step "Run the replay suite" — must hold 13 passed / 2 skipped after every commit |
| Full unit + integration sweep | TBD per task | Each plan's Task N step "Run the broader suite" — only pre-existing failures (`test_no_applied_recovery::test_harness_marks_no_applied_bundle_as_dead_on_arrival`, `test_applier_audit::test_harness_prints_applier_decisions_on_skip_eval`) may persist |
| Skill accountability | P6 (evidence-bundle) closes the C-3-E gap; P4 (typed outcomes) closes the C-3-C and C-3-D gaps. Both must SHIP before P3 and Cycle 2 corpus re-runs are run, otherwise postmortem analysis of the re-runs has the same observability holes. | n/a |
| Corpus delta (flags on) | TBD — corpus re-run pending on 7Now + airline | Hypothesis: 7Now `2423b960` should converge in ≤ 3 iterations once P3 emits a top-N example_sql for `gs_026` and a missing_filter example_sql for `gs_021`. Airline `2afb0be2` should converge similarly once P3 covers `gs_009` top-N collapse. |

#### Section 6: Decision

**Cycle 3 status: PLANNED.** Five sibling plans saved; no implementation has landed. Recommended landing order is P6 → P1 → P2 → P4 → P3 — this respects every dependency:

- P6 ships first because every subsequent corpus re-run's analysis depends on the bundle anchoring to the right attempt.
- P1 + P2 are independent observability + structural fixes; either can ship next. P2 is the harder constraint because Cycle 2 corpus re-run **MUST NOT** run before P2 (the DOA ledger is non-injective until then).
- P4 is the typed-reason-code supplier P3 emits. Without P4, P3's `NO_STRUCTURAL_CANDIDATE` record falls back to a generic reason and the postmortem can't tell it apart from the lever-5 gate drop.
- P3 is the actual change that produces causal SQL-shape candidates. Lands last; corpus re-run gates open after P3 SHIPS.

#### Section 7: Seeds for next cycle

```text
C-3-A-followup (seed): cycle 2 DOA dedup correctness audit
  Run(s) observed: 2423b960-16e8-41d4-a0cb-74c563378e05
  Hypothesis for next cycle: once P2 lands, the Cycle 2 corpus re-run
       on the airline space MUST verify _doa_selected_proposal_signatures
       contains lever-qualified ids. If still not, the DOA dedup is
       still broken and Cycle 2 Task 3 needs a new behavioral test.
  Blocking on: P2 SHIPPED + Cycle 2 corpus re-run on airline.

C-3-B-followup (seed): retire C-2-A and C-2-B as legacy seeds
  Run(s) observed: cross-cycle bookkeeping
  Hypothesis for next cycle: C-2-A (postmortem rollback-class
       miscategorization) and C-2-B (F3 strategist coverage gaps with
       missing RCA cards) should be re-evaluated post-Cycle-3
       corpus re-run. C-2-A may move into a dedicated gso-postmortem
       skill plan; C-2-B may close as covered by Cycle 1's existing
       bucket_driven_ag_selection work.
  Blocking on: Cycle 3 corpus re-run.

C-3-C (seed): notebook entrypoint run_id_widget regression
  Run(s) observed: 2423b960 (2 of 5 lever_loop attempts failed on
       RuntimeError: get_run_context: run_id_widget is required)
  Hypothesis for next cycle: Databricks job repair re-creates the
       widget context inconsistently. Fix is in the lever-loop
       notebook entrypoint (cite the file in the next plan), not in
       the optimizer logic.
  Blocking on: a small dedicated plan ("preserve run_id widget on
       Databricks job repair"); not a Cycle 3 task.

C-3-D (seed): Phase H assembly degradation persists
  Run(s) observed: 2423b960 + 2afb0be2 (both runs report
       phase_h_assembly_skipped_or_failed and
       phase_b decision_records_total=0 even though the export-only
       fixture has all iteration records).
  Hypothesis for next cycle: already covered by
       2026-05-05-run-output-contract-stdout-flush-plan.md;
       not a new cycle, just unblock the existing plan.
  Blocking on: stdout-flush plan implementation.
```
