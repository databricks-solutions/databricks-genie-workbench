# Phase 3.6 — Close Note

**Date:** 2026-05-18
**Branch:** `feat/gso-cycle12`
**Status:** Closed. Anchor regression bar enforced in CI.

This is the bookmark for Phase 3.6's end-state. It enumerates what the harness validates today, what's intentionally blocked, the architectural trade-off the next phase will inherit, and how to extend the regression matrix.

---

## What the harness validates (the 6 passing properties)

The Phase 3 anchor regression suite (`tests/replay/active/test_phase3_anchor_tape_replay.py`) drives the real `harness._run_lever_loop` against bit-exact historic LLM responses captured from two production anchor runs (airline `59a173d3`, 7now `ab65fefe`). Six tests pass and form the enforceable bar (CI: `phase3_6_passing_bar` job on every PR):

| Property | Test | Phase 0+1+2 change validated |
|---|---|---|
| Loop survives the historic tape end-to-end (smoke) | `test_airline_tape_replay_runs_without_crashing` | Harness wiring (binding hook, eval/PATCH/Delta stubs, LLM override) |
| Same for 7now | `test_seven_now_tape_replay_runs_without_crashing` | Same |
| Typed NSC marker emits with non-empty `skipped_reason` (airline) | `test_airline_tape_replay_emits_typed_nsc_marker` | Phase 0.3 RCA-card refusal + Phase 1.5 refuse-on-empty + Phase 2 typed skipped_reason propagation |
| Same for 7now | `test_seven_now_tape_replay_emits_typed_nsc_marker` | Same |
| No PATCH ever escapes to Genie | `test_tape_replay_never_patches_genie_space` | Patch-stub isolation contract |
| Every NSC marker's `skipped_reason` is in the closed typed vocabulary | `test_all_nsc_markers_carry_typed_skipped_reason` | Phase 1.5 closed-vocab invariant |

**Bit-for-bit reproduction of the postmortemed failure mode.** Every NSC marker fires with `skipped_reason = "missing_rca_card"` against the historic clusters — exactly what the two postmortems documented for the 4 anchor failures.

## What's intentionally blocked (the 4 skipped tests)

| Test | What it would validate | What blocks it |
|---|---|---|
| `test_airline_tape_replay_aborts_on_repeated_terminal_signature` | Phase 0.1/0.4 terminal-signature retire firing abort decision at iter 4 | Loop never reaches abort decisioning under replay (proposal generation is empty → terminal-signature collision never accumulates) |
| `test_airline_anchor_qids_are_handled_by_typed_nsc_markers` | Per-anchor-qid coverage (gs_009, gs_024) by typed NSC markers | Single NSC marker per iteration instead of per-AG (downstream of proposal generation) |
| `test_seven_now_anchor_qids_are_handled_by_typed_nsc_markers` | Same for gs_013, gs_026 | Same |
| `test_abort_marker_does_not_indicate_iteration_budget_only` | Phase 0+1+2 abort cause is `terminal_router_decision`, not budget exhaustion | Downstream of abort firing |

All four are marked `@pytest.mark.skip(reason=_PHASE_3_6_BLOCKED_REASON)` with an explicit pointer to the architectural decision (`docs/architecture/phase-3-6-classifier-divergence.md` §5).

**Phase 3.7 update (2026-05-18):** the four tests were un-skipped and re-marked `@pytest.mark.xfail(strict=True)` after Phase 3.7 landed the `historic_inject` / `historic_inject_cluster_only` replay-mode mechanism. Three STOP-and-reports during execution revealed that the historic anchor tapes' lever6 responses claim cross-cluster `affected_questions` (e.g. H001's response cites both gs_009 and gs_024); production at capture time accepted this, but the G2-2026-05-17 cross-cluster guard at `optimizer.py:14027-14045` rejects it under replay → lever6 emits no proposal → no abort fires. This is a **temporal-validity** limit of historic-tape replay (a property, not a bug — see `docs/architecture/tape-replay-protocol.md` §Temporal validity). The tests un-xfail automatically when a post-guard production stall is captured and used as the anchor tape. The `historic_inject_cluster_only` mechanism itself is exercised end-to-end during these xfail runs.

Forward pointer: Phase 3.7 plan + close are in `docs/prompt_improvements/2026-05-17-phase-3-7-historic-prompt-injection.md`.

## The architectural trade-off (deferred — G1 vs G2 vs G3)

The four blocked tests all share one root cause: the historic export's per-iteration `clusters` field carries only 4 fields (`cluster_id`, `asi_failure_type`, `question_ids`, `root_cause`). Production-shape clusters carry ~20+ fields including `question_traces` (per-qid SQL pairs, judge_rationale, mismatch hashes), `blame_set`, `counterfactual_fixes`, `structural_diff`, `failure_features`, `affected_judge`. The Lever 6 prompt-construction code reads these enrichment fields; under replay the prompt comes out 12K chars (cluster metadata + empty placeholders) vs production's 34K chars (full failure evidence). The historic Lever 6 LLM response in the tape can't be matched against the under-fed replay prompt.

Three plausible directions, none cheap:

- **G1 — recapture clusters with full enrichment in the export.** Change the production export serializer to preserve all cluster fields. Re-run anchor jobs. Multi-day engineering work touching production code and every downstream export consumer.
- **G2 — replay-feed the historic prompt itself, bypassing prompt construction.** The tape has the 34K-char prompts already (as `entries`). Add a new replay seam at the prompt-build layer. Smaller than G1 but introduces a new harness boundary with its own design discussion.
- **G3 — synthetic "no candidate" Lever 6 stub.** Fastest but loses the ability to validate Phase 0+1+2's lever-6 path under replay. Risky because we'd guess at which `skipped_reason` to inject.

None of these need to happen now. The choice depends on whether proposal-generation validation becomes a priority (Thread B below).

## Two-thread plan for what was going to be Phase 4

**Thread A — Operational (immediate, low-cost):**

Deploy Phase 0+1+2 to the two anchor spaces (`./scripts/deploy.sh --update`) and observe anchor accuracy. Three possible outcomes:

| Outcome | Interpretation | Next step |
|---|---|---|
| Anchor accuracy improves | Phase 0+1+2 was sufficient; G1/G2/G3 don't need pursuing | Close Phase 4 as a no-op or scope-shrunk; possibly a small monitoring-only plan |
| Accuracy unchanged, postmortems richer | Typed markers give a sharper diagnostic; next root cause is the actual root cause, not noise | Phase 4 starts on the new root cause, not on what we hypothesized at session start |
| Accuracy unchanged AND postmortems still vague | We need G1 to validate proposal-generation paths under replay | Phase 3.7 starts with G1 |

**Thread B — Engineering (deferred, conditional on Thread A):**

If Thread A shows we need G1, Phase 3.7's scope: "extend export serializer to preserve cluster enrichment fields, recapture anchor tapes, drive proposal-generation under replay." Estimated 3-4 days. Do not draft yet.

## How to extend the regression matrix

When a new anchor failure shape is documented:

1. Capture a tape from the new run:
   ```bash
   python scripts/capture_tape_from_mlflow.py \
       --experiment-id $EXPERIMENT_ID \
       --filter-string 'tags."genie.optimization_run_id" = "<new-run-uuid>"' \
       --export-json docs/runid_analysis/<runid>/evidence/replay_fixture_from_latest_export_*.json \
       --out tests/replay/active/fixtures/production_tapes/<short_id>.json \
       --miss-policy prompt_sha_only --tape-id <short_id>
   ```
2. Add a `test_<short_id>_*` block to `test_phase3_anchor_tape_replay.py` mirroring the airline / 7now structure.
3. Run locally; the new test should slot into the 6-passing matrix.
4. PR: the `phase3_6_passing_bar` CI job picks it up automatically.

The bar grows; the contract stays the same.

## Phase 3.6 commit history (`feat/gso-cycle12` branch)

For audit reference, the work this note closes spans:

```
e4405cc7  feat(tape): Phase 3.6.2 E5b — tape-serve pre-computed clusters + defensive cluster rendering (F1)
2b7a2d82  docs(tape): Phase 3.6 — classifier divergence diagnosis (R1/R2/R3 checks)
dc248651  feat(tape): Phase 3.6.2 E4 — empty-stubs for state.load_stages / load_provenance + protocol doc
0a7f99ff  feat(tape): Phase 3.6.2 E3 — load_latest_state_iteration / load_all_full_iterations / load_run stubs
568d8393  feat(tape): Phase 3.6.2 E2 — replay stubs state.load_latest_full_iteration
604c08c5  feat(tape): Phase 3.6.2 E1 — tape format v3 + iteration_payloads
1e4bbdad  fix(tape): Phase 3.6.1 — tape side-tables must be 0-indexed (C1)
0bf0c268  fix(tape): Phase 3.6.1 — allowlist empty payload is "{}" not "" (D1)
cc6de168  feat(tape): Phase 3.6 hygiene — pre-loop helper allowlist on tape miss
a6f80a07  docs(tape): Phase 3.6 — harness data-source audit (Path 1 diagnosis)
6b5215f6  feat(tape): Phase 3.6 — capture_tape_from_mlflow walks all sibling MLflow runs
47e81a26  test(tape): Phase 3.6 Task 6 — historic tapes for airline 59a173d3 + 7Now ab65fefe
```

Plus this commit (close-note + CI gate + skip markers).

## Anti-treadmill record

Five stop-and-reports during this session. Each one prevented a wrong patch:

1. After the multi-run capture (E2), the 6 anchor tests still failed — stop diagnosed indexing mismatch (C1) rather than guessing at clustering.
2. C1 alone surfaced the allowlist `""` contract bug — stop produced D1 (return `"{}"`) rather than mutating production code.
3. After C1+D1, baseline_iter still None — stop produced Path 1 audit identifying 6 unstubbed `state.load_*` loaders.
4. After E2 unblocked iteration progression, `all_qids = already_passing` — stop produced the R1/R2/R3 classifier diagnosis (R2 excluded; R1 fires; G1/G2/G3 trade-off documented).
5. After E5b regressed 4 smoke tests, stop diagnosed `c['affected_judge']` as a defensive-accessor defect, not a boundary problem. F1 landed alongside.

The harness boundary principle (tape-served = upstream of decisions; harness-derived = decision logic) survived every stop-and-report and is now the documented contract in `tape-replay-protocol.md`.
