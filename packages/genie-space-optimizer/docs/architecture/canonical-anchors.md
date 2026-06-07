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
| airline | `488860692117207`   | `450001766723999`    | ACTIVE  | 2026-06-06 22:22    | Cold-start after Trial 26 W26.7 (compact-aware handoff readers) deploy. Absorbed one Trial-26 lever-loop pass — verdict `LEVER_LOOP_SKIPPED_POST_ENRICHMENT_MEETS_THRESHOLDS` (baseline 95.65%, gate skipped before kit gate could fire). Budget gate 2026-06-07: `task_value_count=0`, well under 200 — replay-safe. **After Trial 29 W29.1 + W29.5 deploy (2026-06-07 16:12, commit `d16b46fe`)**, replay airline with `job_parameters={"force_lever_loop":"true"}` to engage the W27.3 override (airline's baseline already meets thresholds; the override is mandatory to reach the kit gate). W29.4 verification target: ≥1 acceptance with `decision="kit_forced_inert_reroute"` (marker `GSO_TRIAL29_INERT_PATCH_REROUTE_V1`) followed by Stage 3 picking a different structural mechanism in the next iteration and yielding `behavioral_diff != "unchanged"`. Prior cold-start `335104024979293` retired (W26.7 baseline_eval regression, fixed by W26.7). |
| 7now    | `488860692117207`   | `517826776610889`    | ACTIVE  | 2026-06-06 22:22    | Cold-start after Trial 26 W26.7 deploy. Absorbed one Trial-26 lever-loop pass — verdict `PLAN11_STAGE3_PROMPT_TOO_LARGE_RUN_STARVED` (final 91.3%, Stage 3 prompt 73170 tokens vs 40000 cap). Budget gate 2026-06-07: `task_value_count=0`, well under 200 — replay-safe. **After Trial 29 W29.1 + W29.5 deploy (2026-06-07 16:12, commit `d16b46fe`)**, replay 7now WITHOUT `force_lever_loop` (gate is not skipped on 7now); the W27.1 partitioned re-dispatch already de-starves Stage 3 and Trial 28 W28.1 grounded `unknown_kind`. W29.4 verification target: 7now is the most likely anchor to produce the first live `kit_forced_inert_reroute` because its hard QIDs include `gs_026` (plural top-N collapse on a kit-forced RCA) and Trial 28 W28.1 grounding lifted `unknown_kind` from 71.4% → 0% so the kit gate is now reachable. Watch for `GSO_TRIAL29_INERT_PATCH_REROUTE_V1` followed by a different-mechanism patch with `behavioral_diff != "unchanged"`. Prior cold-start `631994889494024` retired (W26.7 baseline_eval regression, fixed by W26.7). |

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
