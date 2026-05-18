# Lever-Loop Tape Replay Protocol

**Status:** Active as of 2026-05-17 (Phase 3).

## Purpose

The tape replay protocol lets the actual ``harness._run_lever_loop`` execute against frozen LLM responses captured from production runs. It is the deterministic-behavior contract: every change to the lever loop must keep the four 2026-05-17 anchor tapes (airline gs_009/gs_024, 7now gs_013/gs_026) green.

The protocol does NOT validate accuracy. It validates that, given fixed inputs (eval rows, cluster definitions, RCA cards, LLM responses), the harness produces the expected markers, decision records, and terminal state.

## On-disk tape format

```json
{
  "tape_id": "<short stable id>",
  "source_run_id": "<production runId>",
  "captured_at": "<UTC ISO8601>",
  "entries": [
    {
      "key": {
        "stage": "adaptive_strategy" | "cluster_driven_synthesis" | ...,
        "iteration": <int>,
        "ag_id": "" | "<AG_xxx>",
        "cluster_id": "" | "<H001>" | "<...gs_009>",
        "prompt_sha256": "<hex>"
      },
      "prompt": "<verbatim user-message body>",
      "response_text": "<verbatim recorded LLM output>",
      "response_metadata": { "source": "lever_loop_export" | "mlflow" }
    }
  ],
  "evals_by_iteration": { "0": [<eval row>, ...], "1": [...] },
  "clusters_by_iteration": { "0": [<cluster>, ...] },
  "rca_cards_by_cluster": { "H001": { ... } },
  "miss_policy": "raise" | "warn"
}
```

## Stage vocabulary

| Stage name | Source call site | Bound to |
|------------|------------------|----------|
| ``adaptive_strategy`` | ``optimizer._call_llm_for_adaptive_strategy`` via ``_traced_llm_call(span_name="adaptive_strategy")`` | iteration |
| ``cluster_driven_synthesis`` | ``cluster_driven_synthesis.run_cluster_driven_synthesis_for_single_cluster`` → ``synthesize_preflight_candidate`` → ``_traced_llm_call`` | iteration + ag_id + cluster_id |
| ``preflight_arbiter`` | ``preflight_synthesis._gate_genie_agreement`` arbiter callable (when wired through ``_traced_llm_call``) | iteration + ag_id |
| ``rca_card_normalize_rationale`` | ``rca_card_llm.normalize_card_rationale`` | iteration + cluster_id |

Each stage name corresponds to the ``span_name`` passed to ``_traced_llm_call``. New stages are added by registering a new ``span_name`` at the call site and capturing it in the next tape capture run.

## Interception mechanism

The ``_LLM_CALLER_OVERRIDE: ContextVar`` is consulted at the top of ``optimizer._traced_llm_call``. When set, the override returns ``(response_text, response_metadata)`` and ``_traced_llm_call`` returns immediately without calling the OpenAI SDK or opening an MLflow span.

The override is installed by ``LeverLoopReplayHarness.__enter__`` and reset on exit. ContextVar isolation guarantees that production code paths in concurrent asyncio tasks or threads are not affected.

## Side-effect stubs

The replay harness also patches the following production functions so replay never touches external systems:

| Function | Production behavior | Replay behavior |
|----------|---------------------|-----------------|
| ``optimization.evaluation.run_evaluation`` | Runs MLflow evaluation against a Genie space | Returns ``{"eval_rows": tape.evals_by_iteration[i], ...}`` |
| ``common.genie_client.patch_space_config`` | PATCHes the Genie API | Captures call args; returns ``{"replay": True}`` |
| ``optimization.state.write_stage`` | Writes a Delta row | Captures call args; returns ``None`` |
| ``mlflow.start_run`` (+ log_param/log_metric/log_text/log_dict/log_artifact/set_tag/set_tags/set_experiment/set_tracking_uri/end_run/active_run/last_active_run) | Opens / writes to the MLflow tracking backend | Synthetic-Run contextmanager / no-op |

If new external side effects are added to the lever loop, they MUST be patched in ``LeverLoopReplayHarness.__enter__`` and listed here. The CI gate (Phase 3 anchor gate) enforces this by failing PRs that modify critical-path files without updating this document.

## Binding hook

``harness._TAPE_BINDING_HOOK`` is a module-level callable invoked at the top of each iteration and at the start of each AG processing block. The default is a no-op. The replay harness installs a hook that updates the active ``TapeCallContext`` so subsequent LLM calls resolve their ``(iteration, ag_id, cluster_id)`` key correctly.

If you add a new per-cluster or per-AG LLM call site to the lever loop, ensure ``_tape_binding_set_ag`` is called immediately before the LLM call so the tape lookup uses the right key.

## Capture paths

There are two complementary ways to build a ``LeverLoopTape``:

### Live capture (Phase 3.5)

For any optimization run executed by the post-3.5 build:

1. ``_run_lever_loop`` installs an ``InMemoryLLMCallRecorder`` for the duration of the loop.
2. ``optimizer._traced_llm_call`` appends every successful call to the recorder.
3. End of loop: the harness drains the buffer and routes each call to ``_replay_fixture_iterations[i]["llm_call_log"]``.
4. ``journey_fixture_exporter`` serializes the log into the export JSON.
5. ``scripts/capture_lever_loop_tape_from_export.py --export ... --out ...`` reads ``llm_call_log`` and writes a tape.

Tapes from this path carry full iteration / ag_id / cluster_id binding and use ``miss_policy="raise"`` (strict, full-key match).

### Historic capture (Phase 3.6)

For any run with active MLflow tracing — including runs that predate Phase 3.5 (no ``llm_call_log`` in the export):

1. ``MlflowClient.search_traces(experiment_ids=[...], run_id=...)`` fetches the trace tree.
2. ``mlflow_trace_extractor.extract_llm_calls_from_traces(traces)`` walks every CHAIN span in ``_KNOWN_STAGES`` with a CHAT_MODEL child and emits call dicts.
3. ``scripts/capture_tape_from_mlflow.py --experiment-id ... --run-id ... --export-json ... --out ...`` assembles a tape from the extractor output plus the export JSON's eval/cluster side-tables.

Post-Phase-3.6 traces carry ``iteration`` / ``ag_id`` / ``cluster_id`` as span breadcrumbs (set by ``_traced_llm_call``), so historic-style extraction is bit-exact equivalent to live capture. Pre-3.6 traces have no breadcrumbs; their tapes use ``miss_policy="prompt_sha_only"`` and match on (stage, prompt_sha256) alone.

## Pre-loop helper allowlist

``tape_llm_caller.PRE_LOOP_HELPER_STAGES_ALLOWLIST`` is a frozenset of ``span_name`` values whose ``_traced_llm_call`` sites were added to the codebase AFTER the historic anchor tapes were captured. These calls fire during replay (typically space-setup helpers invoked once per run, before the lever-loop iteration loop opens) but the captured tape has no entry for them — they predate the call site.

Under any miss policy, a tape miss whose ``span_name`` is in the allowlist returns ``("", {"tape_metadata": {"replay_no_op": True}})`` instead of raising ``TapeMissError``. The production call sites for these helpers wrap their ``_traced_llm_call`` invocations in ``try/except`` and treat an empty/failed response as a benign no-op (see ``optimizer._generate_proactive_instructions`` at ``optimizer.py:4253``), so the lever loop stays on its postmortem trajectory.

Current allowlist:

| ``span_name`` | Call site | When added |
|---|---|---|
| ``generate_sample_questions`` | ``optimizer._generate_sample_questions`` | post-2026-05-09 |
| ``generate_space_description`` | ``optimizer._generate_space_description`` | post-2026-05-09 |
| ``generate_proactive_instructions`` | ``optimizer._generate_proactive_instructions`` | post-2026-05-09 |

Adding a new entry to the allowlist is intentionally a code change (not a tape flag) so the choice is reviewable: an unmatched call that *should* have been captured is a real coverage gap and must surface as a ``TapeMissError``.

## Miss policies

| Policy | When to use | Lookup semantics |
|---|---|---|
| ``raise`` (default) | Live captures and post-3.6 historic captures with breadcrumbs | Strict full-key match on (stage, iteration, ag_id, cluster_id, prompt_sha256); miss raises ``TapeMissError`` |
| ``warn`` | Deliberate prompt-template-drift testing | Match on (stage, iteration, ag_id, cluster_id); accept first sibling regardless of prompt SHA, log WARNING |
| ``prompt_sha_only`` (Phase 3.6) | Pre-3.6 historic captures without breadcrumbs | Match on (stage, prompt_sha256); ignore iteration/ag/cluster on the candidate key |

Prompt content is iteration-+-AG-unique in practice (Stage 1 / Stage 2 prompts include the iteration's clusters, RCA cards, and AG context), so ``prompt_sha_only`` is safe for historic tapes.

## Capture workflow (Phase 3.5)

1. **Production run records every LLM call.** ``_run_lever_loop`` installs an ``InMemoryLLMCallRecorder`` for the duration of the loop. Every call routed through ``optimizer._traced_llm_call`` (Stage 1, Stage 2 per lever, synthesis, fallback strategist) is appended to the recorder's buffer with binding ``(iteration, ag_id, cluster_id)`` captured at call time.
2. **Drain at loop exit.** After the main for-loop completes, the harness drains the recorder buffer and routes each call into ``_replay_fixture_iterations[i]["llm_call_log"]`` by matching the recorded ``iteration`` field.
3. **Journey exporter serializes.** ``journey_fixture_exporter._ALLOWED_ITERATION_KEYS`` includes ``llm_call_log``; the per-call shape is enforced by ``_ALLOWED_LLM_CALL_KEYS``.
4. **Capture script reads the export.** ``scripts/capture_lever_loop_tape_from_export.py`` walks ``iteration["llm_call_log"]`` and emits a canonical ``LeverLoopTape`` JSON.

```bash
python scripts/capture_lever_loop_tape_from_export.py \
    --export docs/runid_analysis/<runid>/lever_loop_latest_export.json \
    --out    tests/replay/active/tapes/<short_id>.json \
    --tape-id <short_id>
```

5. **Legacy fallback for pre-Phase-3.5 exports.** If the export lacks ``llm_call_log``, the script logs a WARNING and falls back to the old ``strategist_prompt`` + ``strategist_response`` path where present. When neither is present, the script emits an empty tape (with a WARNING) rather than raising.
6. **(Future) From MLflow CHAT_MODEL spans:** extend the capture script with an ``--mlflow-run-id`` flag that walks the MLflow trace tree and emits ``TapeEntry`` records from each ``CHAT_MODEL`` span. Not implemented.

## Stage vocabulary

``tape._KNOWN_STAGES`` is a frozenset enumerating every ``span_name`` literal currently emitted by ``src/genie_space_optimizer/``. Tapes that reference a stage outside this set load successfully (forward compat) but emit a WARNING at load time so capture-side typos or drift are visible.

A CI gate (``tests/ci/test_known_stages_matches_source.py``) re-derives the production vocabulary via ``grep`` and refuses any PR that introduces a new ``span_name`` literal without updating ``_KNOWN_STAGES``.

Stages by category (current):

- **Stage 1 (three-stage pipeline):** ``stage_1_discovery``
- **Stage 2 / per-lever:** ``lever_1_table_column_description``, ``lever_2_mv_column_refinement``, ``lever_3_tvf_routing``, ``lever_4_join_discovery``, ``lever_4_join_discovery_repair``, ``lever_5a_instructions``, ``lever_5b_example_sql``, ``lever_5b_example_sql_for_rca``, ``lever_6_sql_expression``
- **Synthesis:** ``cluster_driven_example_synthesis``, ``cluster_driven_example_synthesis_retry``, ``cluster_driven_synthesis``, ``preflight_example_synthesis``, ``archetype_learning.synthesize_provisional``
- **Legacy strategist:** ``adaptive_strategy``, ``monolithic_strategy_fallback``
- **Other proposal-stage:** ``phase_1a_triage``, ``lever1_rca_proposal``, ``lever6_llm``, ``prose_rule_mining``, ``prose_rule_mining_retry``, ``sql_expression_seeding_llm``
- **Space-setup:** ``generate_space_description``, ``generate_sample_questions``

## Refresh policy

Anchor tapes are refreshed when:

- A code change intentionally alters strategist or synthesis prompts (the existing tape's ``prompt_sha256`` no longer matches the new prompt). In that case, capture a fresh tape from a production rerun of the same Genie Space.
- A new failure shape is added to the anchor set in ``tests/fixtures/failure_cluster_anchors/``.

Anchor tapes MUST NOT be hand-edited. Treat them as captured artifacts.

## What this protocol guarantees

- ``_traced_llm_call`` always consults ``_LLM_CALLER_OVERRIDE`` first.
- ``LeverLoopReplayHarness`` patches every external side effect listed above.
- Every typed marker (NSC, ABORTED, etc.) emitted during replay survives the Phase 1 refuse-on-empty invariant.
- The collision guard on the terminal-signature axis (Phase 0 Task 1) fires correctly for the four anchor shapes.

## What this protocol does NOT guarantee

- That production accuracy improves. The tape is fixed; the only behavior under test is the harness's reaction to it.
- That LLM responses captured at time T remain valid at time T+ΔT. Prompt drift surfaces as a ``TapeMissError`` — that is intentional and is the user's signal to refresh the tape.
