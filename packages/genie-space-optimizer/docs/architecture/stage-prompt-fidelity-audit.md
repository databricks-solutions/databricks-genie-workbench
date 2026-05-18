# Stage Prompt-Fidelity Audit (Phase 3.7 Task 1)

**Date:** 2026-05-18
**Branch:** `feat/gso-cycle12`
**Status:** lock-in. Identifies the single stage that needs `historic_inject`; all
others remain `rebuild_and_match`.

This audit answers one question for every span_name in `_KNOWN_STAGES`:

> Under tape replay, does the rebuilt prompt SHA equal the historic prompt SHA?

A "yes" means the historic export preserves every field the prompt-builder reads,
and the existing default replay mode (`rebuild_and_match`) works. A "no" means the
export drops fields that the prompt-builder reads, the SHA diverges, the tape
lookup misses, and the stage needs the new `historic_inject` mode.

## Methodology

Empirical, not formal. The Phase 3.6 closure bar runs the real `harness._run_lever_loop`
against the two production anchor tapes (`airline_run_59a173d3`,
`seven_now_run_ab65fefe`). Both tapes use `miss_policy: prompt_sha_only`, so a
SHA mismatch raises `TapeMissError` at lookup time. Any stage whose rebuilt prompt
SHA diverges from its captured SHA would visibly fail under the 6 passing tests
(it would either crash the loop or surface a typed marker). The 6 tests pass →
those stages match.

## Per-stage table

Order: stages actually fired in the two anchor tapes, then the rest of
`_KNOWN_STAGES` for completeness.

| Stage | In anchor tapes? | Empirical fidelity (6 tests) | Prompt-builder cluster-field deps | Verdict |
|---|---|---|---|---|
| `preflight_example_synthesis` | ✓ (27 / 34) | matches | `format_afs(cluster)` + archetype + allowlist — afs fields, but stripped to closed schema | `rebuild_and_match` |
| `lever_5b_example_sql` | ✓ (20 / 24) | matches | `format_afs(cluster)` + archetype + allowlist | `rebuild_and_match` |
| `lever6_llm` | ✓ (8 / 10) | **does not match** — falls through to TapeMissError → caught → `missing_rca_card` marker | `format_afs(cluster)` AND `_format_raw_evidence_block(raw_evidence)` where `raw_evidence` is a tuple of per-question (sql, judge_rationale, mismatch_hash) dicts read directly from the cluster | **`historic_inject`** |
| `lever_5a_instructions` | ✓ (5 / 6) | matches | `format_afs(cluster)` + allowlist (per-cluster, no `raw_evidence`) | `rebuild_and_match` |
| `stage_1_discovery` | ✓ (5 / 7) | matches | benchmark + space-level metadata, no per-cluster enrichment | `rebuild_and_match` |
| `adaptive_strategy` | ✓ (2 / 3) | matches | benchmark + lever-history aggregates | `rebuild_and_match` |
| `sql_expression_seeding_llm` | ✓ (1 / 1) | matches | seed-questions block; cluster-independent | `rebuild_and_match` |
| `lever_4_join_discovery` | ✓ (0 / 2) | matches | schema graph; cluster-independent | `rebuild_and_match` |
| `lever_4_join_discovery_repair` | — (not fired in anchor tapes) | n/a (no captured entries) | same as lever_4 | `rebuild_and_match` (presumed) |
| `cluster_driven_example_synthesis` | — | n/a | per-cluster, uses `format_afs` | `rebuild_and_match` (presumed) |
| `cluster_driven_example_synthesis_retry` | — | n/a | same as above + last-attempt error | `rebuild_and_match` (presumed) |
| `cluster_driven_synthesis` | — | n/a | legacy alias | `rebuild_and_match` (presumed) |
| `archetype_learning.synthesize_provisional` | — | n/a | archetype-only, cluster-independent | `rebuild_and_match` (presumed) |
| `phase_1a_triage`, `phase_1b_detail_*` | — | n/a | per-AG router, not per-cluster | `rebuild_and_match` (presumed) |
| `lever1_rca_proposal` | — | n/a | per-cluster, AFS-only (no raw_evidence at this stage) | `rebuild_and_match` (presumed) |
| `prose_rule_mining`, `prose_rule_mining_retry` | — | n/a | space-level, cluster-independent | `rebuild_and_match` (presumed) |
| `lever_{1,2,3}_*` (table/column/MV/TVF) | — | n/a | metadata-only, no `raw_evidence` | `rebuild_and_match` (presumed) |
| `monolithic_strategy_fallback` | — | n/a | benchmark + space-level | `rebuild_and_match` (presumed) |
| `generate_space_description`, `generate_sample_questions` | — | n/a (create-flow, never in lever loop) | space-level | `rebuild_and_match` (presumed) |

## Why only lever6_llm

Both `lever6_llm` and the per-cluster synthesis stages call
`format_afs(cluster)` — that function reads `affected_judge`, `asi_blame_set`,
`failure_features`, `counterfactual_fixes`, `structural_diff` from the cluster
dict. All of those fields are dropped by the historic export. The downstream
prompt-builders survive the loss because `format_afs` returns a closed-schema
dict with empty-list defaults; the rendered AFS JSON is small either way and the
*rest* of those prompts (archetype, allowlist, schema, snippets) is determined
by `metadata_snapshot` which IS preserved in the export.

What distinguishes `lever6_llm` is the **`raw_evidence` block**:
`_format_raw_evidence_block(raw_evidence)` renders a tuple of per-question
(sql, judge_rationale, mismatch_hash) tuples — typically 15 questions per
cluster × ~1.5KB each ≈ 22KB. The historic export does not preserve
`question_traces`, so under replay `raw_evidence` arrives as `()` and the
block collapses to an empty string. That single substitution accounts for
the documented 34K (production) → 12K (replay) prompt size delta and the
SHA mismatch. No other lever-loop stage takes a `raw_evidence` (or equivalent)
argument; `_format_raw_evidence_block` is only called from `lever6_llm`'s
prompt-build path.

## Forward risk and gate

Iteration 0 calls only stages that match today, so the 6 passing tests pass.
Once `historic_inject` unblocks `lever6_llm`, the loop progresses into
iteration 1+ which may invoke stages that did not fire in iteration 0. If
any of them have analogous fidelity issues, Task 7's un-skipped tests will
TapeMiss — and per the anti-treadmill protocol, that is a STOP-and-report
event, not a patch-forward event.

The "presumed" rows above are not assumed safe under replay; they're flagged
"presumed" because the existing 6-test bar doesn't exercise them. If a Task 7
failure points at one of them, the protocol is: extend `replay_mode_by_stage`
to flag the additional stage as `historic_inject`, recapture, re-run. No
silent fallback.

## Lock-in

For Phase 3.7, `replay_mode_by_stage` for the airline and 7now anchor tapes is:

```json
{"lever6_llm": "historic_inject"}
```

All other stages are absent from the dict and default to `rebuild_and_match`.
This is the minimum-blast-radius design; new stages need a separate
recapture + audit + lock-in revision.
