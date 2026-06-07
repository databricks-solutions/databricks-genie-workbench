# Canonical Anchors (current parent_run_ids for live trials)

> Authoritative source for the two anchor spaces' **current** Databricks
> parent job runs used by every `/goal` live trial via `gso-lever-loop-replay`.
> When the harness needs to know which `parent_run_id` to replay against,
> read this file — never hardcode the IDs in AGENTS.md, source code, or
> launch scripts.

## Why anchor parent runs rotate

Databricks Jobs enforces a hard **250 `taskValues.set` writes per parent
job run** ceiling (cumulative across all task attempts). Each full
`gso-lever-loop-replay` against an anchor publishes ~45 task values
(`lever_loop` ~12 + `preflight` ~10 + `baseline_eval` ~6 + enrichment ~7
+ `finalize` ~10). Pre-Trial-25 (per-key fan-out), a parent runs out of
budget after ~5-6 anchor replays and dies with
`PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250` — see
[`docs/skills/gso-postmortem/SKILL.md`](../../docs/skills/gso-postmortem/SKILL.md)
under "Recent failure-mode guardrails" for the full signature.

Post-Trial-25 the compact-publish flag (`GSO_TRIAL25_HANDOFF_COMPACT`)
plus the W25.5 pre-trigger budget gate together push the practical
budget to ~25+ replays per parent. **Even with Trial 25 active, a parent
will eventually exhaust** — rotation is not a one-time event.

## Anchor spaces (stable — these never change)

| Anchor name | Genie Space ID                            |
|-------------|-------------------------------------------|
| airline     | `e94376a3-d8a6-4570-a605-9fe231e5f99c`    |
| 7now        | `d13938e7-405d-4444-833a-03f5ac9f7523`    |

## Current parent job runs (rotate when budget exhausted)

| Anchor  | job_id              | parent_run_id        | Status  | Triggered (UTC)     | Notes |
|---------|---------------------|----------------------|---------|---------------------|-------|
| airline | `488860692117207`   | `450001766723999`    | ACTIVE  | 2026-06-06 22:22    | Cold-start after Trial 26 W26.7 (compact-aware handoff readers) deploy. **W29.4 replay 2026-06-07 (repair `1068216194302846`, task `653857084564329`, TERMINATED/SUCCESS, `force_lever_loop=true`): PARTIAL.** The W29.1 lane fired 1× (`gs_009`, `top_n_cardinality_collapse`, rejected `lever-5`) and accuracy held at 95.65% — but no `behavioral_diff != "unchanged"` patch and `gs_009` was dropped from the iters 3-4 Stage-3 `target_qids_union` so the mechanism-switch was never exercised. Verdict `TRIAL29_W29_4_PARTIAL_NO_BEHAVIORAL_DELTA`, `architecture_invariants_held=false` (5 gaps → Trial 30 W30.1-W30.4). Postmortem: `runid_analysis/e94376a3-…/postmortem_653857084564329.md`. Parent now has 4 REPAIR entries — next replay MUST include `latest_repair_id=1068216194302846`. Re-check budget before re-replay (was 0/200 pre-W29.4). Prior cold-start `335104024979293` retired (W26.7 baseline_eval regression, fixed by W26.7). |
| 7now    | `488860692117207`   | `517826776610889`    | ACTIVE  | 2026-06-06 22:22    | Cold-start after Trial 26 W26.7 deploy. **W29.4 replay 2026-06-07 (repair `990840611944843`, task `1075987815374793`, TERMINATED/SUCCESS, no `force_lever_loop`): PARTIAL.** The W29.1 lane fired 2× (`gs_013`/`wrong_column` + `gs_026`/`top_n_cardinality_collapse`, both rejected `lever-5`) and accuracy held at 91.3% — but Stage 3 re-emitted the same rejected `lever-5`/`add_example_sql` in iters 2/3/4 (never selected the `lever-6` fallback), so no `behavioral_diff != "unchanged"`. Verdict `TRIAL29_W29_4_PARTIAL_NO_BEHAVIORAL_DELTA`, `architecture_invariants_held=true`. Postmortem: `runid_analysis/d13938e7-…/postmortem.md`. This is the cleanest reproduction of the W30.1 "advisory-not-enforced" gap (no QID-drop confound — the strategist simply re-picked the rejected mechanism). Parent now has 4 REPAIR entries — next replay MUST include `latest_repair_id=990840611944843`. Prior cold-start `631994889494024` retired (W26.7 baseline_eval regression, fixed by W26.7). |

## Retired parent job runs (do NOT replay against these)

| Anchor  | parent_run_id        | Retired (UTC)     | Reason                                             |
|---------|----------------------|-------------------|----------------------------------------------------|
| airline | `501649560474489`    | 2026-06-06 06:48  | `PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250` after 22 replays. Latest failed task_run_id `502350180589777`. Postmortem bundle: `packages/genie-space-optimizer/docs/runid_analysis/502350180589777/`. |
| 7now    | `807620338215711`    | 2026-06-06 06:45  | `PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250` after 21 replays. Latest failed task_run_id `12690262598700`. Postmortem bundle: `packages/genie-space-optimizer/docs/runid_analysis/12690262598700_7now_lever_loop_failure/`. |

## How the harness reads this file

Both `gso-lever-loop-replay` and the `/goal` Next-Plan Playbook (AGENTS.md
step 7b) build their `parent_runs` list by parsing the "Current parent
job runs" table above. The parse contract is:

- Rows with `Status = ACTIVE` (or any non-PENDING / non-RETIRED status)
  contribute one `{job_id, parent_run_id}` entry per row.
- Rows with `Status = PENDING` are treated as a hard error — the harness
  MUST trigger fresh parents via `gso-lever-loop-trigger` (or surface the
  blockage to the operator) before attempting any replay.
- Rows in the "Retired" table are never used.

The literal column names — `Anchor`, `job_id`, `parent_run_id`, `Status` —
are stable; do not rename them without also updating the
[`gso-lever-loop-replay`](../../docs/skills/gso-lever-loop-replay/SKILL.md)
SKILL.md.

## Rotation procedure (operator runbook)

1. **Pre-rotation check.** A parent is "near budget" when
   `databricks jobs get-run <parent_run_id> --include-resolved-values -o json`
   shows accumulated `taskValues` count > 200 (Trial 25 W25.5 gate
   enforces this automatically when wired up). If the count is below
   200, do NOT rotate — keep using the existing parent.
2. **Trigger fresh parents.** For each anchor that needs rotation, run
   the `gso-lever-loop-trigger` skill — that is the COLD-START path
   (creates a new parent job run for the same anchor space). Capture the
   returned `run_id`.
3. **Update this file** in a SINGLE edit per anchor:
    - Move the current row from the "Current" table to the "Retired"
      table with the rotation date and reason.
    - Add the new `parent_run_id` to the "Current" table with
      `Status = ACTIVE`, the triggered-at timestamp, and any notes
      (e.g., "rotated after Trial 25 deploy of compact-publish").
4. **Commit the edit** before the next `/goal` invocation. The harness
   will pick up the new IDs on its next read.
5. **Do NOT delete retired rows** — they are part of the audit trail and
   referenced from postmortem bundles.
