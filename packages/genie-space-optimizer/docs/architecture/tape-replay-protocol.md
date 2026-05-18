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

## Capture workflow

1. **From a production run's lever-loop export:**
   ```bash
   python scripts/capture_lever_loop_tape_from_export.py \
       --input  docs/runid_analysis/<runid>/lever_loop_latest_export.json \
       --output tests/replay/active/tapes/<short_id>.json \
       --tape-id <short_id>
   ```
2. **(Future) From MLflow CHAT_MODEL spans:** extend the capture script with an ``--mlflow-run-id`` flag that walks the MLflow trace tree, reads ``inputs.messages`` and ``outputs.choices[0].message.content`` from each ``CHAT_MODEL`` span, and emits ``TapeEntry`` records. Not implemented in this iteration.

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
