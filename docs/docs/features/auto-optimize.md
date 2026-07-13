---
sidebar_position: 3
description: "Benchmark-driven Genie optimization with a four-task, auditable hill-climbing pipeline."
---

# Auto-Optimize (GSO)

Auto-Optimize measures a Genie Space against its benchmark corpus, diagnoses failures, and tests targeted configuration changes. It is powered by the Genie Space Optimizer package in `packages/genie-space-optimizer/`.

Unlike the [IQ Scanner](/docs/features/iq-scanner), which produces an instant rule-based readiness score, Auto-Optimize runs a bounded optimization job against the live Space. Every attempt is evaluated, accepted only when accuracy improves, and persisted for audit.

## Four-task pipeline

The current Lakeflow Job is a linear four-task DAG:

```mermaid
flowchart LR
    intake["1 · Intake & Snapshot"] --> qc["2 · Benchmark QC & Repair"]
    qc --> optimize["3 · Optimize"]
    optimize --> publish["4 · Publish & Audit"]
```

| # | Task | Responsibility |
|---|------|----------------|
| 1 | **Intake & Snapshot** | Validate the run envelope, retain the trigger-time rollback snapshot, and write the `run_manifest` artifact. |
| 2 | **Benchmark QC & Repair** | Validate, repair, deduplicate, or prune benchmarks within a bounded retry budget, then persist `benchmark_qc`. |
| 3 | **Optimize** | Apply low-risk Space quality enrichment, run the baseline evaluation, and execute the bounded patch/evaluation loop. |
| 4 | **Publish & Audit** | Resolve the stamped terminal reason, mark the champion when eligible, generate a best-effort audit summary, capture postflight IQ, and write terminal run state. |

Each task receives the complete job parameter set and exchanges durable state through Delta by `run_id`. There is no notebook chaining or task-value handoff.

## Optimization loop

The Optimize task uses a full-benchmark hill-climbing loop:

```mermaid
flowchart LR
    eval["Evaluate current config"] --> diagnose["Diagnose failures"]
    diagnose --> propose["Propose one bounded patch set"]
    propose --> safety["Validate & apply"]
    safety --> prove["Run full evaluation"]
    prove --> decision{"Accuracy improved?"}
    decision -->|Yes| keep["Accept as best"]
    decision -->|No| rollback["Rollback & record reflection"]
    keep --> eval
    rollback --> eval
```

Iteration 0 is the baseline. Later rows are patch attempts. A candidate is accepted only when its full-evaluation accuracy is strictly greater than the best accepted accuracy; otherwise its patches and iteration are marked rolled back and the live serialized configuration is restored.

The controller stores attempt mode, hypothesis, decision, decision reason, best accuracy, retry memory, terminal reason, and champion state on `genie_opt_iterations`. Terminal stamping adds the terminal reason without overwriting the attempt's existing accept/reject decision.

## Levers

The strategist selects from the configured levers for each attempt:

| Lever | Area | Typical changes |
|-------|------|-----------------|
| 1 | Tables & columns | Table selection, column descriptions, synonyms, entity matching |
| 2 | Metric views | Governed metric definitions and routing |
| 3 | Table-valued functions | Parameterized query patterns |
| 4 | Join specifications | Preferred relationships and join keys |
| 5 | Instructions | Business vocabulary, routing rules, constraints, examples |
| 6 | SQL expressions | Reusable filters, measures, expressions, and worked SQL |

Before baseline evaluation, a narrow Space-quality phase may also fill low-risk curation gaps such as an empty top-level Space description or thin instructions. Its post-enrichment description is persisted separately because Genie stores `description` as Space metadata, outside `serialized_space`.

## Evaluation and leakage safety

Current runs use Genie's native benchmark evaluation surface and persist the official evaluation run identifiers, status, question counts, correctness counts, and needs-review counts. Headline accuracy is `num_correct / num_questions` for native evaluation rows.

Benchmark expected SQL is evaluation truth and must never become inference-visible configuration. The optimizer's leakage firewall blocks patches that copy or closely echo benchmark answer material into instructions, examples, descriptions, or other Space content.

## Terminal outcomes

The loop stamps one of the typed reasons below. Publish & Audit uses that stamped reason and never re-derives it from accuracy.

| Terminal reason | Run status | Champion publish |
|-----------------|------------|------------------|
| `TARGET_REACHED` | `CONVERGED` | Yes |
| `MAX_ATTEMPTS` | `MAX_ITERATIONS` | Yes |
| `NO_NEW_HYPOTHESIS` | `STALLED` | No |
| `EVAL_INVALID` | `FAILED` | No |
| `EVAL_BUDGET_EXHAUSTED` | `STALLED` | No |
| Missing or unknown | `STALLED` | No, fail closed |

Publishing is an idempotent Delta champion mark. Accepted patches are already applied to the live Space by the loop; Publish & Audit does not replay them. Audit-summary generation and postflight IQ capture are soft-failing and cannot prevent the final status write.

## History, revert, and discard

Optimization History exposes two different rollback contracts:

- **Revert to champion/baseline** changes live Space configuration without changing the historical run status. It preserves the Space's current benchmark block so reverting history cannot roll benchmark ground truth backward. Champion reverts restore the captured post-enrichment description when available; legacy runs preserve the current description.
- **Discard** restores the complete trigger-time snapshot, including the original benchmark block and top-level description, then marks the run `DISCARDED` only after rollback succeeds.

Both paths snapshot live state before a two-part serialized-config/description mutation. If the description update fails after the serialized config succeeds, the optimizer attempts compensation and never reports success for the partial operation.

History revert is disabled while any run for the same Space is active. The backend also reconciles all same-Space runs and returns a conflict if one remains `QUEUED`, `IN_PROGRESS`, or `RUNNING`.

## Permission model

The app service principal owns optimizer Delta state and executes the Lakeflow Job. A history action therefore reads internal state with the SP, but separately authorizes the requesting OBO user against the target Genie Space. The user must have `CAN_EDIT` or `CAN_MANAGE`; the SP's broader access never substitutes for user authorization.

See [Authentication & Permissions](/docs/platform/authentication) for setup and required grants.

## Durable state

The main current-run sources of truth are:

| Table | Contents |
|-------|----------|
| `genie_opt_runs` | Run envelope, trigger snapshot, status, champion pointer, terminal reason |
| `genie_opt_stages` | Four-task and nested-stage timeline |
| `genie_opt_iterations` | Baseline/attempt evaluation and controller state |
| `genie_opt_patches` | Applied and rolled-back patch records |
| `genie_opt_provenance` | Root-cause and proposal provenance |
| `genie_eval_lever_loop_decisions` | Ordered safety and controller decisions |
| `genie_opt_benchmark_mutations` | Benchmark QC additions, removals, and changes |
| `genie_opt_artifacts` | `run_manifest`, `benchmark_qc`, `space_quality_enrichment`, and `publish_record` payloads |
| `genie_opt_scan_snapshots` | Optional paired preflight/postflight IQ snapshots |

The Workbench prefers Lakebase synced reads for UI views and falls back to direct Delta reads where needed. Mutating integration paths use the configured SQL Warehouse and SP-owned state.

## Triggering from the UI

1. Open a Space and select **Optimize**.
2. Configure levers, target accuracy, attempt budget, and model.
3. Start the run. The UI submits `POST /api/auto-optimize/trigger` and polls the run status.
4. Review the attempt ladder, question results, patches, benchmark QC, audit summary, and terminal outcome.
5. Keep the accepted state, discard it to the trigger snapshot, or later use Optimization History to revert to a past champion or baseline.

## Source files

- `packages/genie-space-optimizer/databricks.yml` — four-task job definition
- `packages/genie-space-optimizer/src/genie_space_optimizer/jobs/` — task notebooks
- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/unified_loop.py` — optimizer controller
- `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/publish.py` — publish and audit
- `packages/genie-space-optimizer/src/genie_space_optimizer/integration/` — trigger, apply, discard, and history revert
- `backend/routers/auto_optimize.py` — Workbench API bridge

## Related documentation

- [Authentication & Permissions](/docs/platform/authentication)
- [IQ Scanner](/docs/features/iq-scanner)
- [Operations Guide](/docs/platform/operations)
