# Trial 29 W29.4 — Live Verification Runbook (resume-from-IP-block)

> **Why this doc exists.** Trial 29 W29.1 + W29.5 deployed cleanly at
> `2026-06-07 16:12 UTC` (commit `73ebd7b7`). The live re-verification
> phase fired parallel `gso-lever-loop-replay` against both anchors at
> `2026-06-07 16:37–16:43 UTC`. Both replays mutated state cleanly and
> submitted `databricks jobs repair-run` into the small healthy window
> BEFORE an account-level Databricks IP ACL change blocked source IP
> `107.194.218.231` (the workspace-7474646443183435 and
> workspace-7474656657532371 ACLs both fire the same
> `Source IP address: 107.194.218.231 is blocked by Databricks IP ACL`
> error). Verification of the running jobs is therefore deferred to
> the next time the operator runs from an allowlisted IP (or after the
> ACL is updated). This doc captures everything needed to resume.

## Pre-block state (verified, audit-trail-ready)

| Field | airline | 7now |
|---|---|---|
| `parent_run_id` | `450001766723999` | `517826776610889` |
| `optimization_run_id` | `e94376a3-d8a6-4570-a605-9fe231e5f99c` | `d13938e7-405d-4444-833a-03f5ac9f7523` |
| `space_id` (Genie Space) | `01f143dfbeec15a3a0e87ced8662f4ed` | `01f128aea2c210559cffb663d9c58282` |
| post-enrichment `model_id` | `m-69561bdf50194fac93bfb46cb54de436` | `m-d823768913f34947932bc278f97c216a` |
| post-enrichment accuracy | 95.65% | 91.3% |
| backup table prefix | `…__gso_replay_backup_e94376a3_20260607163807__*` | `…__gso_replay_backup_d13938e7_20260607163728__*` |
| Trial 25 W25.5 budget gate | PASS (`task_value_count=0`) | PASS (`task_value_count=0`) |
| Genie restore | ✅ OK (artifact-based) | ✅ OK (artifact-based) |
| Delta rewind | ✅ OK (counts matched plan) | ✅ OK (counts matched plan) |
| `force_lever_loop` | **`true`** (W27.3 override) | `false` (gate not skipped on 7now) |
| `latest_repair_id` in JSON | `838289694458133` (3rd repair — see note) | not included (first repair) |
| Repair `repair-run` POST | submitted `16:41:21Z`, response lost to IP block | returned cleanly: `repair_id=990840611944843` |
| `lever_loop_task_run_id` | unverified (response lost) | `1075987815374793` |

> **airline `repair_history` note.** The operator-supplied parameters
> said "first repair, no `latest_repair_id`", but
> `databricks jobs get-run 450001766723999 --include-history` actually
> reported 2 prior REPAIR entries (latest id `838289694458133`). The
> airline replay subagent correctly detected this in Step 1, deviated
> from the operator brief, and included `latest_repair_id` in the JSON
> body (called out in the dry-run summary). This is the canonical
> handling for a non-first repair per the `gso-lever-loop-replay`
> skill.

## What you need to do once on an allowlisted IP

### Step 1 — Confirm IP unblock

```bash
databricks jobs get-run 450001766723999 --profile fevm-prashanth \
  -o json | head -5
```

If you see `"state":` JSON output, you're unblocked. If you see
`Source IP address: ... is blocked by Databricks IP ACL`, you're still
blocked.

### Step 2 — Confirm airline's missing repair landed

```bash
databricks jobs get-run 450001766723999 --include-history \
  --profile fevm-prashanth -o json \
  | python3 -c "
import sys, json
r = json.load(sys.stdin)
print('overall state:', r['state'])
print('repair_history:')
for h in (r.get('repair_history') or []):
    print(f'  type={h.get(\"type\"):<8} id={h.get(\"id\"):<18} state={h.get(\"state\")} start={h.get(\"start_time\")}')
"
```

Expect: a 3rd `REPAIR` entry with `start_time` ≈ `16:41:21Z` (Unix-ms
near `1749319281000`). Capture its `id` as
`airline_repair_id`.

To get the `lever_loop_task_run_id` for that repair:

```bash
databricks jobs get-run 450001766723999 --include-history \
  --profile fevm-prashanth -o json \
  | python3 -c "
import sys, json
r = json.load(sys.stdin)
# pick the latest task with task_key=='lever_loop' whose start_time is >= 16:41:00Z
candidates = [t for t in r.get('tasks', []) if t.get('task_key') == 'lever_loop']
candidates.sort(key=lambda t: t.get('start_time', 0), reverse=True)
if candidates:
    t = candidates[0]
    print('lever_loop task_run_id:', t.get('run_id'))
    print('  state:', t.get('state'))
    print('  start_time (ms):', t.get('start_time'))
    print('  end_time (ms):', t.get('end_time'))
"
```

Capture as `airline_lever_loop_task_run_id`.

### Step 3 — Confirm 7now's `LEVER_LOOP_STARTED` row

```bash
databricks sql query --profile fevm-prashanth \
  --warehouse 3b1be27d7a807e80 \
  --query "
SELECT stage, status, started_at, completed_at, message
FROM prashanth_subrahmanyam_catalog.genie_space_optimizer.genie_opt_stages
WHERE run_id = 'd13938e7-405d-4444-833a-03f5ac9f7523'
  AND task_key = 'lever_loop'
  AND started_at >= TIMESTAMP '2026-06-07 16:40:00'
ORDER BY started_at;
"
```

Expect: at least one `LEVER_LOOP_STARTED` row from the
`16:37–16:43Z` repair (`repair_id=990840611944843`,
`task_run_id=1075987815374793`).

Repeat for airline using
`run_id = 'e94376a3-d8a6-4570-a605-9fe231e5f99c'` and the airline
`task_run_id` from Step 2.

### Step 4 — Wait for both jobs to terminate (or confirm already terminated)

Both repairs started at ~`16:41Z`. Typical `lever_loop` wall-clock is
30–45 min, so by the time you're reading this they have almost
certainly terminated. Quick check:

```bash
for run_id in 450001766723999 517826776610889; do
  state=$(databricks jobs get-run "$run_id" --profile fevm-prashanth -o json \
    | python3 -c "import sys, json; r=json.load(sys.stdin); s=r['state']; print(f\"{s.get('life_cycle_state')}/{s.get('result_state','PENDING')}\")")
  echo "  $run_id: $state"
done
```

If either is still `RUNNING`/`PENDING`, poll every 2–5 min until
`TERMINATED/SUCCESS` (or `TERMINATED/FAILED`/`TERMINATED/CANCELED`).

### Step 5 — Fan out postmortem subagents

Once both runs are terminal, drive the W29.4 verdict via parallel
`gso-postmortem` subagent invocations. Re-paste this into the agent
session:

> "Both Trial 29 W29.4 anchors are terminal. Fan out two parallel
> `Task subagent_type=generalPurpose` invocations of the
> `gso-postmortem` skill — one per anchor — with these inputs:
>
> **airline:**
> - `job_id`: `488860692117207`
> - `parent_run_id`: `450001766723999`
> - `lever_loop_task_run_id`: `<from Step 2>`
> - `profile`: `fevm-prashanth`
> - W29.4-specific evidence to extract:
>   - count of `GSO_TRIAL29_INERT_PATCH_REROUTE_V1` markers in the
>     lever_loop log (must be ≥1 to satisfy W29.4 criterion 1)
>   - count of accepted patches with
>     `behavioral_diff != "unchanged"` (must be ≥1 to satisfy criterion 3)
>   - final accuracy vs post-enrichment baseline 95.65% (criterion 4
>     — gain or hold)
>
> **7now:**
> - `job_id`: `488860692117207`
> - `parent_run_id`: `517826776610889`
> - `lever_loop_task_run_id`: `1075987815374793`
> - `profile`: `fevm-prashanth`
> - W29.4-specific evidence to extract: (same three items as airline)
>
> Each postmortem subagent returns the gso-postmortem verdict block.
> Then compose the unified W29.4 verdict and mark
> `lever-loop-iteration-tracker.md` W29.4 [x] with evidence."

### Step 6 — Update tracker once W29.4 verdict is in

Edit `packages/genie-space-optimizer/docs/architecture/lever-loop-iteration-tracker.md`:

- Status checklist:
  ```diff
  - [ ] W29.4 — live re-verification: ≥1 `behavioral_diff != "unchanged"` patch + measurable accuracy gain on an anchor whose lever loop runs (BLOCKED on W29.1 deploy + cold-start anchor replay)
  + [x] W29.4 — live re-verification: <evidence> (verified <date> on <anchor>, postmortem at <path>)
  ```

- Watch Marker row for `GSO_TRIAL29_INERT_PATCH_REROUTE_V1`: add a
  brief "first observed in live run" note with the postmortem path.

- Update `canonical-anchors.md` if either parent's
  `task_value_count` is now near 200 — rotate the parent via
  `gso-lever-loop-trigger` before the next replay.

## Failure paths

| Scenario | Action |
|---|---|
| Airline shows no 3rd REPAIR | Resubmit the airline repair (the JSON-body POST never landed). Per the `gso-lever-loop-replay` Step 6 contract for a non-first repair, include `latest_repair_id` (now the 2nd, `838289694458133`, since no 3rd exists). |
| Either run `TERMINATED/FAILED` | Run `gso-postmortem` against the failed task_run_id; the postmortem will produce a failure-mode classification. If the failure is `PARENT_RUN_TASK_VALUE_BUDGET_EXHAUSTED_250`, rotate via `gso-lever-loop-trigger` then re-run W29.4 on the fresh parents. |
| `LEVER_LOOP_STARTED` row never appeared for either anchor (≥30 min after repair submit) | The repair JSON was rejected post-submit — query the task's `get-run-output` for the error. Common cause: missing `job_parameters` (this should NOT happen — both subagents passed explicit `job_parameters`). |
| `kit_forced_inert_reroute` count = 0 on both anchors | The Trial 29 W29.1 lane never fired live. Possible causes: (a) no patch produced `behavioral_diff="unchanged"` on a kit-forced RCA this run (try a 2nd replay; the LLM may have synthesized differently); (b) feature flag off (verify `GSO_TRIAL29_BEHAVIOR_DELTA` and `GSO_TRIAL29_INERT_REROUTE` in app.yaml of deployed app); (c) `_kit_for_rca_companions` returned `None` for all observed RCAs (check the canonical RCA labels in the run's `genie_eval_lever_loop_decisions`). |

## Cross-references

- Deploy event: commit `73ebd7b7` on `feat/gso-cycle13`, deployed
  `2026-06-07 16:12 UTC`, app URL
  `https://genie-workbench-7474646443183435.aws.databricksapps.com`.
- Trial 29 W29.1 + W29.5 implementation: 12 commits between
  `c529a841` (design) and `d16b46fe` (tracker mark).
- Workstream definition + acceptance criteria:
  [`lever-loop-iteration-tracker.md`](./lever-loop-iteration-tracker.md)
  § "Trial 29 — Behaviour-changing structural lever for kit-forced
  RCAs …".
- Replay skill: [`../skills/gso-lever-loop-replay/SKILL.md`](../skills/gso-lever-loop-replay/SKILL.md).
- Postmortem skill: [`../skills/gso-postmortem/SKILL.md`](../skills/gso-postmortem/SKILL.md).

## IP ACL incident timeline (for posterity)

| UTC time | Event |
|---|---|
| `16:12` | Trial 29 W29.1 + W29.5 deployed (`./scripts/deploy.sh --update`) |
| `16:37` | 7now replay subagent kicked off (`gso-lever-loop-replay` Step 1) |
| `16:38` | airline replay subagent kicked off |
| `16:41:21` | airline `databricks jobs repair-run` POST started |
| `~16:43:50` | First `Source IP address: 107.194.218.231 is blocked by Databricks IP ACL` error observed on workspace `7474646443183435` |
| `16:37–17:08` | 7now subagent's repair POST returned cleanly (`repair_id=990840611944843`) inside the small healthy window, then polling timed out under the ACL block |
| `~19:14` | airline subagent surfaced `VERIFICATION_BLOCKED_BY_IP_ACL` after 90+ min of retry attempts |
| `~19:31` | Main thread independently confirmed the IP block from a fresh shell (`fevm-prashanth` + unrelated `fevm-serverless` both block) |
| `~19:33` | 7now subagent interrupted by main thread to stop the retry loop |
| (TBD) | Operator resolves IP ACL (VPN to allowlisted IP, or account ACL update) |
| (TBD) | Operator runs Steps 1–6 of this runbook |
