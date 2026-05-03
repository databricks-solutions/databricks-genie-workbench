---
name: gso-replay-cycle-intake
description: Use when ingesting a fresh Genie Space Optimizer lever-loop fixture into the airline-replay burn-down ledger after a real run completes. Drives the Phase 5 per-cycle runbook from 2026-05-02-run-replay-per-iteration-fix-plan.md — verify the deploy SHA contains Track D (commit c5f5a59), preserve the raw fixture as airline_real_v1_cycleN_raw.json, verify canonical QIDs (no tr-... shaped IDs), promote to airline_real_v1.json, re-run run_replay to measure violations + by_kind composition, append a row to 2026-05-02-phase-a-burndown-log.md, decide tighten/hold/regress on BURNDOWN_BUDGET, and bundle into one cycle commit. Triggers on "intake cycle N", "advance the burn-down", "promote the fixture", "tighten the budget", or after a Phase E pilot or any post-Track-D real-run completes.
---

# GSO Replay Cycle Intake

Use this skill when asked to advance the airline-replay burn-down ledger by one cycle after a fresh lever-loop run produces a new replay fixture. This is a **write-side ops runbook**: it overwrites test fixtures, modifies the `BURNDOWN_BUDGET` test constant, appends to the burn-down log, and creates a single git commit per cycle. It is the operational counterpart to `gso-lever-loop-run-analysis`, which is read-only.

## Required Inputs

- `cycle_number`: Integer N. The cycle being intaken (e.g. `8`, `9`, `10`). Cycle 7 is already in the ledger as the Phase 4 baseline.
- `fixture_source`: One of:
  - **Local path**: e.g. `~/Downloads/airline_real_v1.json` (or `local://<absolute_path>`).
  - **MLflow artifact**: `mlflow://<run_id>/phase_a/replay_fixture.json`.
  - **Databricks job/run ref**: `databricks://<job_id>/<run_id>` — defer fetch to `gso-lever-loop-run-analysis`.
  - **Evidence bundle**: `bundle://<opt_run_id>` — copy from `packages/genie-space-optimizer/docs/runid_analysis/<opt_run_id>/evidence/replay_fixture.json`. Requires that an evidence bundle has been built for this `opt_run_id` and that the bundle's manifest has `artifacts_pulled.replay_fixture` set (i.e., the harness emitted the `===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===` block). The intake skill **must not** mutate any file under `runid_analysis/`.
  If omitted, ask the user for the source.

## Optional Inputs

- `repo_root`: Path to the repo. Default: workspace root.
- `branch`: Deploy branch to verify the Track D commit against. Default: current `git branch --show-current`.
- `notes`: One-line "what changed since cycle N-1" string for the ledger row's Notes column. If omitted, derive from a `gso-lever-loop-run-analysis` postmortem of the same run, or ask the user.
- `analysis_run_id`: Job Run ID of the same run. If supplied, the intake report will link to (or trigger) the analysis-skill report for that run instead of asking for `notes` directly.
- `auto_apply`: Boolean. Default `false`. When `true`, file writes / git commits proceed without per-step confirmation. Treat with care; prefer `false` for the first cycle in a series.
- `prior_budget`: Integer. Default: read literal from `tests/replay/test_lever_loop_replay.py`. Override only when the test file has been refactored.

## Required Related Skills

Use these skills as needed:

- `gso-lever-loop-run-analysis` — peer skill; required when (a) `fixture_source` is a `databricks://` URL, (b) Step 4 surfaces a violation regression and triage is needed, (c) `notes` is unset and a postmortem can supply the one-liner.
- `databricks-jobs` — only when `gso-lever-loop-run-analysis` itself defers to it for job-run inspection.
- `retrieving-mlflow-traces` — only when `fixture_source` is `mlflow://...` and the artifact must be downloaded.
- `systematic-debugging` — when Step 4 returns a regression and the cause must be bisected.

## Operating Principle

Phase 5 is a **deterministic 7-step cycle** with one quantitative decision point (Step 6, budget movement). The cycle either holds the line (no new violations) or surfaces a real harness regression that must be triaged before promotion. Two iron rules:

1. **Never promote on regression.** If Step 4's violation count is higher than the prior `BURNDOWN_BUDGET`, revert Step 3 and hand off to `gso-lever-loop-run-analysis` for triage. Do not commit until violations are at-or-below the prior budget.
2. **Single commit per cycle.** Raw fixture, canonical baseline, ledger row, and (optional) budget tightening land in one commit. Mid-cycle commits fragment the audit trail.

Run the seven steps in order; do not skip Steps 1, 2, or 5 even when the run is "obviously fine" — they are the audit trail.

## Pre-flight: Confirm deploy SHA contains Track D

A fresh lever-loop will only emit canonical-QID fixtures if the deployed branch contains commit `c5f5a59` (Track D `_baseline_row_qid`). Without it, `eval_rows[*].question_id` will again be MLflow trace IDs (`tr-...`).

```bash
# Confirm the commit exists locally
git log --oneline c5f5a59 -1

# Confirm it's on the deployed branch
git log --oneline origin/$(git branch --show-current) | grep -F c5f5a59
```

If the second command returns nothing, **stop**. Push and redeploy first; do not start a new ~2 hour lever-loop. Document the failed pre-flight and ask the user how to proceed.

## Cycle Intake Workflow

For each fresh cycle, run all seven steps. Substitute `<N>` with the integer cycle number throughout.

### Step 1 — Save the raw cycle-N fixture as forensic evidence

```bash
# fixture_source is a local path:
cp <fixture_source> packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1_cycle<N>_raw.json
```

For non-local sources:
- `bundle://<opt_run_id>` (preferred, new):
   ```bash
   SRC="packages/genie-space-optimizer/docs/runid_analysis/<opt_run_id>/evidence/replay_fixture.json"
   test -f "$SRC" || { echo "no fixture in bundle; rebuild bundle or fall back to databricks://"; exit 1; }
   cp "$SRC" packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1_cycle<N>_raw.json
   ```
   The intake skill **must not** mutate any file under `runid_analysis/` — the bundle is read-only from this skill's perspective.
- `mlflow://<run_id>/phase_a/replay_fixture.json` → fetch via `retrieving-mlflow-traces` first, then copy.
- `databricks://<job_id>/<run_id>` → call `gso-lever-loop-run-analysis` with that job/run; once it has resolved the MLflow artifact path, fetch and copy.

The raw file is forensic evidence and is **never** overwritten in any subsequent cycle.

### Step 2 — Verify the raw fixture has canonical QIDs (no `tr-...` IDs)

```bash
N=<N>
python - <<PY
import json, sys
path = "packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1_cycle${N}_raw.json"
fx = json.load(open(path))
qids = [r.get("question_id", "") for it in fx["iterations"] for r in (it.get("eval_rows") or [])]
trace_qids = [q for q in qids if q.startswith("tr-")]
print(f"iterations: {len(fx['iterations'])}")
print(f"total eval_row qids: {len(qids)}")
print(f"trace-id-shaped qids: {len(trace_qids)}")
sys.exit(0 if not trace_qids else 1)
PY
echo "exit=$?"
```

Expected: `trace-id-shaped qids: 0` and `exit=0`. If non-zero, the deploy SHA did not include Track D — go back to pre-flight. As a last-resort fallback, run the reconstruction notebook `notebooks/reconstruct_cycle7_fixture.py` adapted for the new cycle's `OPT_RUN_ID` + paths.

### Step 3 — Promote the cycle-N raw fixture to the canonical baseline

```bash
N=<N>
cp packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1_cycle<N>_raw.json \
   packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json
```

This overwrites the prior canonical baseline. Do not stage or commit yet.

### Step 4 — Re-run replay against the new canonical fixture

```bash
cd packages/genie-space-optimizer
uv run python - <<'PY'
import json
from pathlib import Path
from collections import Counter

from genie_space_optimizer.optimization.lever_loop_replay import run_replay

fx = json.loads(Path("tests/replay/fixtures/airline_real_v1.json").read_text())
result = run_replay(fx)

print(f"is_valid={result.validation.is_valid}")
print(f"missing_qids={list(result.validation.missing_qids)}")
print(f"violations={len(result.validation.violations)}")
by_kind = Counter(v.kind for v in result.validation.violations)
print(f"by_kind={dict(by_kind)}")
by_detail = Counter(
    v.detail for v in result.validation.violations if v.kind == "illegal_transition"
)
print(f"top_illegal_transitions={dict(by_detail.most_common(10))}")
print("first 10 violations:")
for v in result.validation.violations[:10]:
    print(f"  qid={v.question_id} kind={v.kind} detail={v.detail}")
PY
```

Capture the printed `violations=<count>` and `by_kind=<dict>` into the intake report. These two values feed Steps 5 and 6.

### Step 5 — Append a row to the burn-down ledger

Open `packages/genie-space-optimizer/docs/2026-05-02-phase-a-burndown-log.md`. Append (do not edit prior rows; the ledger is append-only):

```markdown
| <N> | <YYYY-MM-DD> | <iter count> | <Step 4 count> | <Step 4 by_kind> | <one-line: what changed since cycle N-1> |
```

The Notes column should answer: "What harness/exporter change between cycle N-1 and cycle N is responsible for the violations going up, down, or holding?" Acceptable sources, in order:

1. The user's `notes` input.
2. A `gso-lever-loop-run-analysis` postmortem of the same run (Recommended Next Actions section).
3. `git log --oneline <prior cycle commit>..HEAD -- packages/genie-space-optimizer/src/genie_space_optimizer/optimization/` — read the harness/exporter touch-points.

If none of the three yields a one-liner, write `(no harness change since cycle N-1)` rather than fabricating.

### Step 6 — Decide on budget movement

Compare Step 4's `violations` count against the current `BURNDOWN_BUDGET` literal in `packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py`:

| Step 4 count vs current budget | Action |
|---|---|
| **Lower** | Tighten: edit `test_lever_loop_replay.py` and set `BURNDOWN_BUDGET = <Step 4 count>`. Single literal change. |
| **Equal** | Hold the budget. No edit. |
| **Higher** | **Regression. Stop the cycle.** Do not commit. Revert Step 3 with `git checkout -- packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json`. **Hand off to `gso-lever-loop-run-analysis`** with the cycle-N raw fixture path and the prior cycle's commit SHA — ask it to identify the regressing emit pattern. Open a separate fix branch and bisect; do not return to this runbook until the fix lands and a fresh re-run brings violations back at-or-below the prior budget. |

### Step 7 — Commit cycle-N intake (single commit, all artefacts together)

Run the full replay suite first to confirm green:

```bash
cd packages/genie-space-optimizer
uv run pytest tests/replay/ -v 2>&1 | tail -15
```

Expected: all tests pass, including `test_run_replay_airline_real_v1_within_burndown_budget` against the new (lower or equal) budget.

Then commit:

```bash
N=<N>
git add packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1_cycle<N>_raw.json
git add packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json
git add packages/genie-space-optimizer/docs/2026-05-02-phase-a-burndown-log.md
git add packages/genie-space-optimizer/tests/replay/test_lever_loop_replay.py  # only if budget tightened
git commit -m "$(cat <<EOF
test(replay): cycle ${N} intake — airline_real_v1 burn-down at <count> violations

* Raw cycle-${N} fixture preserved as airline_real_v1_cycle<N>_raw.json
* Canonical baseline promoted to cycle-${N}
* Burn-down log updated
* CI budget <tightened from <prior> to <new> | held at <budget>>

Refs docs/2026-05-02-run-replay-per-iteration-fix-plan.md Phase 5.
EOF
)"
```

Substitute `<N>`, `<count>`, `<prior>`, `<new>`, `<budget>` literals before running. Do not push automatically; let the user choose when to push.

## Verification Helpers

These checks may be run independently of a full cycle (e.g. when validating a downloaded fixture before deciding to intake it):

- **Pre-flight Track D check** — see Pre-flight section.
- **Canonical-QID shape check** — see Step 2.
- **Iteration count sanity** — `len(fx['iterations'])` should match the deployed lever-loop's planned iteration count (typically `>= 2`; cycle 8 onwards expects 5).
- **Schema sanity** — every iteration must contain at minimum `eval_rows`, `journey_events`, and (post Phase 4.5) `journey_validation`. Missing keys signal an exporter-side regression and warrant `gso-lever-loop-run-analysis` rather than promotion.

## Cross-Skill Hand-offs

This skill **calls** `gso-lever-loop-run-analysis` in three explicit cases:

1. **Fixture acquisition for `databricks://` source** — the analysis skill knows how to resolve a job/run to its MLflow run + artifact path. Pass `job_id` and `run_id`; expect a fixture path back.
2. **Step 6 regression triage** — when violations rose vs the prior budget, hand off the cycle-N raw fixture path, the prior cycle's commit SHA, and the prior budget. The analysis skill produces a postmortem that the burn-down log's Notes column will reference.
3. **Notes derivation** — when the user did not supply `notes` and the cycle landed clean, optionally invoke the analysis skill for a one-paragraph summary of "what changed since cycle N-1" rather than asking the user.

This skill **is called by** `gso-lever-loop-run-analysis` in two cases:

1. **Post-analysis intake request** — after the analysis skill completes its postmortem, if the user asks to "advance the burn-down" or "intake this cycle", hand off here with `cycle_number`, the `databricks://` source, and the postmortem's one-line summary as `notes`.
2. **Phase E pilot completion** — when a Phase E pilot run scores `READY_TO_MERGE` per the analysis skill's Phase E checklist, the user may want to intake that fixture as cycle N to lock the burn-down at the merge baseline. Hand off here.

**Bundle source of truth.** When `fixture_source=bundle://<opt_run_id>`, the analysis skill (`gso-lever-loop-run-analysis`) and this intake skill share a single on-disk evidence base. The analysis skill produces the postmortem; this skill consumes the same fixture without re-fetching.

Both directions of hand-off are **explicit** — never invoke the peer skill silently. Always tell the user "Handing off to `gso-lever-loop-run-analysis` for regression triage" or similar so the audit trail is clear.

## Failure Modes And Recovery

| Failure | Recovery |
|---|---|
| Pre-flight Track D check fails | Stop. Surface to user. Recommend redeploy. Do not run any cycle steps. |
| Step 1 source path missing | Ask user for the correct path; do not proceed with a guess. |
| Step 2 finds `tr-...` qids | Stop. Likely deploy-SHA mismatch. As fallback: run the reconstruction notebook for cycle N (substitute `OPT_RUN_ID`). Document which path was taken. |
| Step 3 fails (disk / permissions) | Stop. Surface error. Do not retry without diagnosing. |
| Step 4 raises an exception | Stop. The fixture is malformed or `run_replay` regressed. Hand off to `gso-lever-loop-run-analysis`. |
| Step 4 reports `is_valid=True` but the prior cycle was non-zero | Suspicious. Verify by re-reading the violation count. If genuine, congratulate the user and tighten the budget to 0 — but capture the diff between cycle N-1 and N raw fixtures in the Notes column. |
| Step 5 ledger file format drift | The ledger is append-only and has a fixed table. If the schema differs, stop and ask the user before reformatting. |
| Step 6 regression | See Step 6 row. Mandatory revert + analysis-skill hand-off. |
| Step 7 pytest run finds an unrelated failure | Stop. Do not commit. Surface the failing test to the user. |
| `auto_apply=true` and any step fails | Auto-apply does not waive the "stop on failure" rule. Halt at first failure. |

## Safety

- **Writes test fixtures.** This skill overwrites `airline_real_v1.json` on every cycle. The raw `cycleN_raw.json` is the only un-overwritten record; preserving it is non-negotiable.
- **Modifies test budget literal.** Tightening `BURNDOWN_BUDGET` is a permanent commitment. Loosening it is forbidden — the budget is monotonically non-increasing over the burn-down's life.
- **Creates one git commit per cycle.** Never amend a prior cycle's commit. Never force-push.
- **Never deletes prior raw fixtures.** Cycles 7, 8, 9, ... raw fixtures are forensic evidence in perpetuity.
- **Does not push.** The skill stops at `git commit`. The user decides when to push.
- **No destructive Databricks commands.** This skill does not cancel, rerun, repair, or delete jobs.
- **Does not mutate `2026-05-02-phase-a-burndown-log.md` retroactively.** Only appends new rows.
- **Does not include tokens or secrets in commit messages or ledger rows.**

## Degraded Operation Rules

- If `fixture_source` is unreachable, write the intake report with `FIXTURE_UNRESOLVED` and ask the user for a usable path. Do not fabricate.
- If `prior_budget` cannot be read (test file refactored), ask the user before guessing.
- If `notes` cannot be derived from any of the three sources (user input, postmortem, git log), use the literal `(no harness change since cycle N-1)` rather than invent.
- If the cycle was started but not finished (e.g. user aborted between Step 3 and Step 7), restore the prior canonical baseline with `git checkout -- packages/genie-space-optimizer/tests/replay/fixtures/airline_real_v1.json` and document the abort in the next cycle's Notes column.
- If the burn-down ledger row count and the raw-fixture file count diverge by more than one, the ledger has skipped a cycle — surface this to the user before proceeding.

## Report Format

Every cycle intake produces a markdown intake report at:

```
packages/genie-space-optimizer/docs/runid_analysis/cycle<N>_intake.md
```

Containing:

```markdown
# GSO Replay Cycle <N> Intake

## Pre-flight

- Track D commit (c5f5a59) on local: PASS / FAIL
- Track D commit on deploy branch: PASS / FAIL

## Steps

| Step | Action | Result |
|---|---|---|
| 1 | Save raw fixture | `airline_real_v1_cycle<N>_raw.json` written |
| 2 | Canonical-QID verify | `trace-id-shaped qids: 0` (PASS) |
| 3 | Promote to canonical | overwritten |
| 4 | Re-run replay | `violations=<count>`, `by_kind=<dict>` |
| 5 | Ledger row appended | row `<N>` with notes `<one-liner>` |
| 6 | Budget decision | TIGHTENED `<prior>` → `<new>` / HELD at `<budget>` / REGRESSION (cycle aborted) |
| 7 | Commit | `<git sha>` (or N/A on regression) |

## Verdict

One of:
- `INTAKEN_AND_TIGHTENED` — cycle landed; budget reduced.
- `INTAKEN_AND_HELD` — cycle landed; budget unchanged.
- `REGRESSION_TRIAGE_REQUIRED` — cycle aborted; analysis-skill hand-off in progress.
- `PRE_FLIGHT_FAILED` — Track D not deployed; intake skipped.

## Hand-offs

- (link to `gso-lever-loop-run-analysis` postmortem if regression or notes-derivation occurred)
```
