# Local Lever-Loop Workbench

> Developer-only tooling. Lives under
> `packages/genie-space-optimizer/devtools/local_lever_workbench/`.
> **Not deployed**, not part of the optimizer wheel, and never invoked
> by Databricks Apps in production.

## Why this exists

Trial 11, Trial 12, and the Trial 13 architectural pause all surfaced
the same shape of regression: a code path that looked green in the
harness shipped to production, the deploy ran, the lever loop produced
zero applied patches, and the postmortem traced the failure to an
input-card / row-shape / archetype-coverage drift that was never on a
local test path. Each one of those iterations cost a full
build-deploy-run-wait cycle (≈ 45 minutes) just to find out the
optimizer would not advance past initial state.

The workbench collapses that loop. With a captured run bundle (or the
committed sanitized production replay corpus) it can:

1. Run the **production** Stage 1 evidence-card preflight against
   real-shaped rows and report violations in seconds.
2. Run the **production** state machine end-to-end with either
   captured SM tape replay, a Stage-1-only live LLM probe, or full
   live Databricks model-serving calls.
3. Record what the optimizer **would have shipped** as a Genie PATCH
   without touching a real Genie Space.
4. Emit a JSON + Markdown funnel report listing deepest stage, marker
   counts, terminal reasons, and surprises worth promoting into CI.

The contract: a green workbench result on a captured production
bundle means the next deploy should reach at least `APPLIED` for the
covered QIDs. A red workbench result names the exact funnel stage and
marker that will fail.

## Codebase boundary — the workbench is an orchestrator only

```
packages/genie-space-optimizer/
├── src/genie_space_optimizer/       # PRODUCTION. Deployed.
├── devtools/local_lever_workbench/  # DEV-ONLY. NOT deployed.
├── tests/integration/               # Production contract tests.
└── tests/workbench/                 # Dev-only workbench tests.
```

* Production code under `src/genie_space_optimizer/` **must not**
  import from `devtools/`, `tests/`, or `tests.workbench`. An
  import-boundary test (`tests/workbench/test_local_workbench_import_boundary.py`)
  enforces this.
* Workbench code under `devtools/local_lever_workbench/` **may**
  import from `src/genie_space_optimizer/` (it is a consumer) and from
  `tests/` (it is a developer tool, not a wheel surface).
* Generated workbench outputs default to
  `devtools/local_lever_workbench/runs/` and are gitignored.

Three rules keep the workbench an *orchestrator* rather than a parallel
optimizer implementation:

1. **No workbench-side copies of optimizer logic.** Every Stage 1/2/3
   helper, contract, and transformer the workbench exercises is
   imported verbatim from `src/genie_space_optimizer/`. The workbench
   does only orchestrator-shaped work: bundle deserialization, fake
   workspace client wiring, recording applier, JSON/Markdown report.
2. **The probe delegates to the runtime helper.**
   `stage1_probe.probe_case` calls the same
   `_build_failing_qid_payload` (`state_machine/transformers/diagnose_llm.py`)
   that `_invoke_stage1_llm` calls. A drift-prevention test
   (`tests/workbench/test_local_workbench_stage1_probe.py::test_probe_matches_sm_canonical_lane_runtime_helper`)
   pins them together — if a refactor changes the runtime card body
   without teaching the probe, that test fires.
3. **New production silent decline modes become workbench tests.**
   When a deploy surfaces a silent decline mode, the fix lands in
   `src/` and a guard lands under `tests/workbench/` asserting the
   committed sanitized production-replay corpus exercises that path
   before the next deploy. The corpus is committed alongside the
   workbench so the guard travels with the code.

## Operator workflow

All commands assume the working directory is
`packages/genie-space-optimizer/` and a `uv sync --frozen` venv.

### 1. Prepare a workbench input bundle

The simplest path uses the committed sanitized production replay
corpus (no live workspace needed):

```bash
uv run python devtools/local_lever_workbench/cli.py prepare \
    --source production-replay \
    --output devtools/local_lever_workbench/runs/example/bundle.json
```

To rehearse a specific captured run that lives under
`docs/runid_analysis/<optimization_run_id>/`:

```bash
uv run python devtools/local_lever_workbench/cli.py prepare \
    --source run-analysis \
    --bundle-dir docs/runid_analysis/<optimization_run_id> \
    --output devtools/local_lever_workbench/runs/<run_id>/bundle.json
```

The loader admits hard rows using the same `admit_eval_rows` helper the
production state machine uses, so the workbench operates on the exact
QID set the deploy would.

### 2. Run the Stage 1 preflight (fastest, no LLM)

```bash
uv run python devtools/local_lever_workbench/cli.py probe \
    --input devtools/local_lever_workbench/runs/example/bundle.json
```

A non-zero exit code means at least one hard QID would fail the
production Stage 1 contract. The output names the violation
(`question_text_empty`, `evidence_card_empty`, …) and the resolved row
path per field. This is the cheapest possible "will the next deploy
die at initial state?" check.

### 3. Run an SM-tape replay (deterministic, no network)

Capture a tape ahead of time with
`scripts/capture_tape_from_mlflow.py`, then:

```bash
uv run python devtools/local_lever_workbench/cli.py run \
    --input devtools/local_lever_workbench/runs/example/bundle.json \
    --llm-mode sm-tape \
    --tape-path <path-to-tape.jsonl> \
    --output-dir devtools/local_lever_workbench/runs/example/<timestamp>
```

The workbench drives every Plan 11 stage via the same
`TapeReplayHarness` the integration tests use. The recording applier
captures the PATCH payloads the optimizer would have shipped.

### 4. Run a live LLM probe (real model serving, no Genie apply)

Requires a Databricks CLI profile with model-serving access and
`DATABRICKS_HOST` / OAuth or PAT credentials.

```bash
uv run python devtools/local_lever_workbench/cli.py run \
    --input devtools/local_lever_workbench/runs/example/bundle.json \
    --llm-mode stage1-only \
    --profile <databricks-cli-profile> \
    --llm-model databricks-claude-sonnet-4-6 \
    --output-dir devtools/local_lever_workbench/runs/example/<timestamp>
```

For a full HARD_QID_SEEN → APPLIED rehearsal:

```bash
uv run python devtools/local_lever_workbench/cli.py run \
    --input devtools/local_lever_workbench/runs/example/bundle.json \
    --llm-mode live-databricks \
    --profile <databricks-cli-profile> \
    --llm-model databricks-claude-sonnet-4-6 \
    --output-dir devtools/local_lever_workbench/runs/example/<timestamp>
```

The workbench will never apply a PATCH against a live Genie Space in
V1; `--apply-mode` only accepts `fake-record`.

### 5. Read the report

Every `run` produces two artefacts under `--output-dir`:

* `result.json` — full structured output, suitable for diffing across
  workbench runs.
* `result.md` — a PR-ready summary: deepest stage, per-QID funnel
  progress, marker counts, terminal reasons, and surprises worth
  promoting into CI.

If the workbench detects a surprise (e.g. `question_text_empty`,
empty-synthesis with no typed reason, `applyable` without a recorded
PATCH), the CLI exits non-zero so a CI wrapper can treat the
workbench as a gate.

## What the workbench catches

The workbench has caught (and now guards against) the following
production silent decline modes:

* **Stage 1 input card empty — `question_text_empty`** (Trial 12,
  run `98ec8950-…` / Trial 12, run `dc89d1a9-…`). The probe surfaces
  this without any LLM call. Test gate:
  `tests/workbench/test_local_workbench_stage1_probe.py::test_probe_flags_question_text_empty_for_bare_row`.
* **Stage 1 typed RCA evidence drop —
  `evidence_card_empty:blame_set_empty,rca_evidence_empty`** (Trial 12,
  Trial 13). The SM canonical lane consumed Plan 12 typed evidence
  off `TransformerContext.rca_evidence_typed` only after this PR; the
  workbench surfaces the regression because the bundle commits typed
  evidence alongside each hard case. Test gates:
  `test_sm_canonical_lane_accepts_full_production_replay_corpus`
  (probe-level) and
  `test_full_production_replay_corpus_reaches_applied`
  (full-funnel via `sm-tape`).
* **`diagnose_returned_no_matching_qid` from tape order drift**
  (workbench v0.1 finding 4). The SM tape harness now routes each
  request to the tape entry whose `qid` is embedded in
  `request.call_id`, with a fallback to arrival order when entries
  carry no `qid`. Test gate:
  `test_tape_harness_qid_routing_survives_dispatch_order_drift`.

When the next deploy surfaces a new silent decline mode, the workflow
is: add a workbench-side guard against the committed production-replay
corpus → fix in `src/` until the guard passes → ship. The corpus
travels with the package so the guard is permanent.

## Safety limits

* **No live Genie apply.** V1 supports only `--apply-mode fake-record`.
  Adding a live-apply mode requires a separate review because it would
  mutate a customer-shaped space.
* **No live post-apply evaluation.** The workbench omits
  `evaluated_gate` and `acceptance_gate` because they require the
  Databricks SQL warehouse + judge tape harnesses.
* **No production-code dependency.** Production optimizer code under
  `src/genie_space_optimizer/` must continue to work without the
  workbench installed. The import-boundary test
  (`tests/workbench/test_local_workbench_import_boundary.py`) is the
  guard.
* **No silent zero-row runs.** The bundle loader raises rather than
  returning an empty hard-case set, mirroring the post-Trial 12
  `failing_qids_count > 0` invariant.

## Verification

```bash
# Workbench-only test slice — fast.
cd packages/genie-space-optimizer
uv run pytest tests/workbench/ -m workbench -q

# Full workbench tape-mode integration test.
uv run pytest tests/workbench/test_local_workbench_runner_tape_mode.py -q
```

The workbench tests share the `integration` marker with the
production state-machine forward-pipeline tests so they pick up the
same SM tape factories. The added `workbench` marker exists so a CI
job can opt in or out of the workbench slice independently.

## Future work

* Live post-apply evaluation against captured baseline / candidate
  eval rows once the workbench has a clean way to inject a judge
  tape.
* MLflow-trace-only bundle inputs (today the workbench requires either
  an export artefact or an explicit eval-row JSON).
* Promote new surprises from `result.json:surprises[]` into typed
  contract tests under `tests/integration/` so they fail in CI rather
  than only in the workbench.
