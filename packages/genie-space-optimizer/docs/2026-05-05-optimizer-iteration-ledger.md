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
| 1 | 2026-05-05 | `0ade1a99-9406-4a68-a3bc-8c77be78edcb` | C-1-A, C-1-B, C-1-C, C-1-D | AG-1-A → AG-1-F | `GSO_TARGET_AWARE_ACCEPTANCE`, `GSO_NO_CAUSAL_APPLYABLE_HALT`, `GSO_BUCKET_DRIVEN_AG_SELECTION`, `GSO_RCA_AWARE_PATCH_CAP`, `GSO_LEVER_AWARE_BLAST_RADIUS` | [`2026-05-05-optimizer-control-plane-plan.md`](./2026-05-05-optimizer-control-plane-plan.md) | airline 87.5% baseline → 91.7% final | TBD at Task G closeout | IN FLIGHT | Corpus-of-one is acceptable for Cycle 1 (establishes the ledger). Cycle 2 must hold ≥2 corpus runs. Skill-accountability gates closed by Plans 2 + 3. |

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
