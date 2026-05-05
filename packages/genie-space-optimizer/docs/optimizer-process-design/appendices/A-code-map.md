# Appendix A — Code Map

This appendix lists the source-of-truth files behind every concept in the documentation set. Use it when you need to verify a behavior in code or extend the optimizer.

> All paths are relative to the repo root unless otherwise noted.

## Pipeline Definition

| Concept | File |
|--------|------|
| Six-task DAG (Job definition, task entrypoints, dependencies) | [`packages/genie-space-optimizer/databricks.yml`](../../../databricks.yml) |
| Job entrypoints | [`packages/genie-space-optimizer/src/genie_space_optimizer/jobs/`](../../../src/genie_space_optimizer/jobs/) |
| Cross-env deploy job | [`packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_cross_env_deploy.py`](../../../src/genie_space_optimizer/jobs/run_cross_env_deploy.py) |
| Lever-loop job | [`packages/genie-space-optimizer/src/genie_space_optimizer/jobs/run_lever_loop.py`](../../../src/genie_space_optimizer/jobs/run_lever_loop.py) |

## Orchestration

| Concept | Function | File |
|---------|----------|------|
| Pipeline orchestrator | `optimize_genie_space` | [`optimization/harness.py`](../../../src/genie_space_optimizer/optimization/harness.py) |
| Preflight | `_run_preflight` | `optimization/harness.py` |
| Baseline | `_run_baseline` | `optimization/harness.py` |
| Enrichment | `_run_enrichment` | `optimization/harness.py` |
| Lever loop | `_run_lever_loop`, `_resume_lever_loop` | `optimization/harness.py` |
| Finalize | `_run_finalize` | `optimization/harness.py` |
| Deploy validate | `deploy_check` | `optimization/harness.py` |
| Deploy apply | `deploy_execute` | `optimization/harness.py` |
| Reflection entry | `_build_reflection_entry` | `optimization/harness.py` |

## Process Spine

| Concept | File |
|---------|------|
| Internal stage registry (canonical 9-stage in-process order) | [`optimization/stages/_registry.py`](../../../src/genie_space_optimizer/optimization/stages/_registry.py) |
| Operator-facing 11-stage process order | `PROCESS_STAGE_ORDER` in [`optimization/run_output_contract.py`](../../../src/genie_space_optimizer/optimization/run_output_contract.py) |
| Run-output contract verification | [`optimization/run_output_contract.py`](../../../src/genie_space_optimizer/optimization/run_output_contract.py) |
| Bundle root + artifact path builders | [`optimization/run_output_contract.py`](../../../src/genie_space_optimizer/optimization/run_output_contract.py) |

## Preflight Helpers

| Concept | Function | File |
|---------|----------|------|
| Fetch space config | `preflight_fetch_config` | [`optimization/preflight.py`](../../../src/genie_space_optimizer/optimization/preflight.py) |
| Run IQ scan | `preflight_run_iq_scan` | `optimization/preflight.py` |
| Collect UC metadata | `preflight_collect_uc_metadata` | `optimization/preflight.py` |
| Generate / refresh benchmarks | `preflight_generate_benchmarks` | `optimization/preflight.py` |

## Evaluation And Scoring

| Concept | Function / Object | File |
|---------|-------------------|------|
| Evaluation entrypoint | `run_evaluation` | [`optimization/evaluation.py`](../../../src/genie_space_optimizer/optimization/evaluation.py) |
| Repeatability evaluator | `run_repeatability_evaluation` | `optimization/evaluation.py` |
| Retry handling | `_run_evaluate_with_retries` | `optimization/evaluation.py` |
| Trace-level gate feedback | `log_gate_feedback_on_traces` | `optimization/evaluation.py` |
| Trace-level strategist feedback | `log_asi_feedback_on_traces` | `optimization/evaluation.py` |
| Scorer composition (default panel) | `make_all_scorers` | [`optimization/scorers/__init__.py`](../../../src/genie_space_optimizer/optimization/scorers/__init__.py) |
| Repeatability scorers | `make_repeatability_scorers` | `optimization/scorers/__init__.py` |

## RCA, Strategy, Acceptance

| Concept | Function | File |
|---------|----------|------|
| Build RCA ledger | `build_rca_ledger` | [`optimization/rca.py`](../../../src/genie_space_optimizer/optimization/rca.py) |
| RCA execution plans | `build_rca_execution_plans` | [`optimization/rca_execution.py`](../../../src/genie_space_optimizer/optimization/rca_execution.py) |
| Cluster failures | `cluster_failures` | [`optimization/optimizer.py`](../../../src/genie_space_optimizer/optimization/optimizer.py) |
| Rank clusters | `rank_clusters` | `optimization/optimizer.py` |
| Adaptive strategy LLM call | `_call_llm_for_adaptive_strategy` | `optimization/optimizer.py` |
| Translate strategy → proposals | `generate_proposals_from_strategy` | `optimization/optimizer.py` |
| Control-plane acceptance | `decide_control_plane_acceptance` | [`optimization/control_plane.py`](../../../src/genie_space_optimizer/optimization/control_plane.py) |
| Acceptance policy | `decide_acceptance` | [`optimization/acceptance_policy.py`](../../../src/genie_space_optimizer/optimization/acceptance_policy.py) |

## Patching

| Concept | Function | File |
|---------|----------|------|
| Proposal → patch operations | `proposals_to_patches` | [`optimization/applier.py`](../../../src/genie_space_optimizer/optimization/applier.py) |
| Apply patch set | `apply_patch_set` | `optimization/applier.py` |
| Rollback | `rollback` | `optimization/applier.py` |
| Cross-env config patch | `patch_space_config` | [`jobs/run_cross_env_deploy.py`](../../../src/genie_space_optimizer/jobs/run_cross_env_deploy.py) |

## Levers And Configuration

| Concept | Symbol | File |
|---------|--------|------|
| Canonical lever names | `LEVER_NAMES` | [`common/config.py`](../../../src/genie_space_optimizer/common/config.py) |
| Proposal stage (where lever proposals are produced) | stage module | [`optimization/stages/proposals.py`](../../../src/genie_space_optimizer/optimization/stages/proposals.py) |
| Proposal grounding + safety | grounding module | [`optimization/proposal_grounding.py`](../../../src/genie_space_optimizer/optimization/proposal_grounding.py) |
| Phase H anchor | postmortem bundle anchor | [`optimization/phase_h_anchor.py`](../../../src/genie_space_optimizer/optimization/phase_h_anchor.py) |
| MLflow artifact anchor | postmortem upload | [`tools/mlflow_artifact_anchor.py`](../../../src/genie_space_optimizer/tools/mlflow_artifact_anchor.py) |

## Tests Of Note

| Concept | File |
|---------|------|
| Lever-loop pretty-print integration test | [`tests/integration/test_lever_loop_pretty_print.py`](../../../tests/integration/test_lever_loop_pretty_print.py) |
| Harness pretty-print return | [`tests/unit/test_harness_pretty_print_return.py`](../../../tests/unit/test_harness_pretty_print_return.py) |
| MLflow artifact anchor unit test | [`tests/unit/test_mlflow_artifact_anchor.py`](../../../tests/unit/test_mlflow_artifact_anchor.py) |
| Phase H anchor unit test | [`tests/unit/test_phase_h_anchor.py`](../../../tests/unit/test_phase_h_anchor.py) |

## How To Read The Code In This Order

If you are new to GSO and want a hands-on tour:

1. [`databricks.yml`](../../../databricks.yml) — see the six tasks and their `depends_on` chain.
2. [`optimization/harness.py`](../../../src/genie_space_optimizer/optimization/harness.py) — read `optimize_genie_space` then each `_run_*` helper top-to-bottom.
3. [`optimization/run_output_contract.py`](../../../src/genie_space_optimizer/optimization/run_output_contract.py) — see the `PROCESS_STAGE_ORDER` and bundle paths.
4. [`optimization/stages/_registry.py`](../../../src/genie_space_optimizer/optimization/stages/_registry.py) — see the in-process stage tuple and the artifact contract per stage.
5. [`optimization/evaluation.py`](../../../src/genie_space_optimizer/optimization/evaluation.py) — see how evaluation is called.
6. [`optimization/scorers/__init__.py`](../../../src/genie_space_optimizer/optimization/scorers/__init__.py) — see the scorer panel.
7. [`optimization/rca.py`](../../../src/genie_space_optimizer/optimization/rca.py) and [`rca_execution.py`](../../../src/genie_space_optimizer/optimization/rca_execution.py) — see RCA in detail.
8. [`optimization/optimizer.py`](../../../src/genie_space_optimizer/optimization/optimizer.py) — see clustering, the strategist call, and proposal generation.
9. [`optimization/applier.py`](../../../src/genie_space_optimizer/optimization/applier.py) — see how patches apply and roll back.
10. [`optimization/acceptance_policy.py`](../../../src/genie_space_optimizer/optimization/acceptance_policy.py) — see the acceptance criterion.
11. [`jobs/run_cross_env_deploy.py`](../../../src/genie_space_optimizer/jobs/run_cross_env_deploy.py) — see cross-workspace promotion.

This linear path mirrors the order in which concepts appear in the documentation set above.
