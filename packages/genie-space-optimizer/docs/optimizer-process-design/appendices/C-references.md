# Appendix C — References

This appendix gathers the internal documents, code anchors, and external references that back the documentation set.

## Internal Documentation

### Workbench-level

- [`docs/00-index.md`](../../../../../docs/00-index.md) — repo-root documentation hub.
- [`docs/07-auto-optimize.md`](../../../../../docs/07-auto-optimize.md) — operator-level overview of the auto-optimize feature in the Workbench app.
- [`docs/03-authentication-and-permissions.md`](../../../../../docs/03-authentication-and-permissions.md) — OBO + SP dual auth model relied on by deploy.
- [`docs/04-create-agent.md`](../../../../../docs/04-create-agent.md) — Create Agent flow that produces the spaces GSO optimizes.
- [`docs/gsl-instruction-schema.md`](../../../../../docs/gsl-instruction-schema.md) — near-term GSL instruction schema; Lever 5 must comply.
- [`docs/appendices/A-api-reference.md`](../../../../../docs/appendices/A-api-reference.md) — API reference for the Workbench backend.

### Package-level

- [`packages/genie-space-optimizer/AGENTS.md`](../../../AGENTS.md) — package invariants and Bug #1–#4 history.
- [`packages/genie-space-optimizer/docs/`](../..) — historical plans and run postmortems referenced from this set:
  - `2026-05-03-gso-run-output-contract-plan.md` — the canonical run output contract design.
  - `2026-05-04-cycle-5-process-spine-plan.md` — the 11-stage process spine introduction.
  - `2026-05-04-terminal-success-transcript-override-plan.md` — operator transcript handling on success paths.
  - `2026-05-04-journey-validation-successful-ag-plan.md` — successful action-group journey validation.

## Code Anchors (cross-link to Appendix A)

For every concept-to-file mapping referenced from the numbered docs, see [Appendix A — Code Map](A-code-map.md). Highlights:

- Pipeline definition: [`databricks.yml`](../../../databricks.yml)
- Orchestration: [`optimization/harness.py`](../../../src/genie_space_optimizer/optimization/harness.py)
- Process spine + run-output contract: [`optimization/run_output_contract.py`](../../../src/genie_space_optimizer/optimization/run_output_contract.py), [`optimization/stages/_registry.py`](../../../src/genie_space_optimizer/optimization/stages/_registry.py)
- Evaluation + scorers: [`optimization/evaluation.py`](../../../src/genie_space_optimizer/optimization/evaluation.py), [`optimization/scorers/__init__.py`](../../../src/genie_space_optimizer/optimization/scorers/__init__.py)
- RCA + strategy: [`optimization/rca.py`](../../../src/genie_space_optimizer/optimization/rca.py), [`optimization/rca_execution.py`](../../../src/genie_space_optimizer/optimization/rca_execution.py), [`optimization/optimizer.py`](../../../src/genie_space_optimizer/optimization/optimizer.py)
- Patching + acceptance: [`optimization/applier.py`](../../../src/genie_space_optimizer/optimization/applier.py), [`optimization/control_plane.py`](../../../src/genie_space_optimizer/optimization/control_plane.py), [`optimization/acceptance_policy.py`](../../../src/genie_space_optimizer/optimization/acceptance_policy.py)
- Levers: `LEVER_NAMES` in [`common/config.py`](../../../src/genie_space_optimizer/common/config.py); proposal stage in [`optimization/stages/proposals.py`](../../../src/genie_space_optimizer/optimization/stages/proposals.py); grounding in [`optimization/proposal_grounding.py`](../../../src/genie_space_optimizer/optimization/proposal_grounding.py)
- Cross-env deploy: [`jobs/run_cross_env_deploy.py`](../../../src/genie_space_optimizer/jobs/run_cross_env_deploy.py)
- Phase H + artifact anchors: [`optimization/phase_h_anchor.py`](../../../src/genie_space_optimizer/optimization/phase_h_anchor.py), [`tools/mlflow_artifact_anchor.py`](../../../src/genie_space_optimizer/tools/mlflow_artifact_anchor.py)

## External References

### Databricks

- [Genie conversation API — `serialized_space` field](https://docs.databricks.com/aws/en/genie/conversation-api#understanding-the-serialized_space-field) — authoritative Genie Space schema.
- [Genie conversation API — validation rules](https://docs.databricks.com/aws/en/genie/conversation-api#validation-rules-for-serialized_space) — ID format, sorting, uniqueness, size limits.
- [Genie best practices](https://docs.databricks.com/aws/en/genie/best-practices) — table selection, instructions, SQL snippets.
- [Unity Catalog metric views](https://docs.databricks.com/aws/en/data-warehousing/sql/metric-views.html) — Lever 2 substrate.
- [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) — DAB framework used by `databricks.yml`.

### MLflow (GenAI)

- [MLflow GenAI overview](https://mlflow.org/docs/latest/genai/) — top-level introduction.
- [`mlflow.genai.evaluate`](https://mlflow.org/docs/latest/python_api/mlflow.genai.html#mlflow.genai.evaluate) — evaluation entrypoint used by `run_evaluation`.
- [`mlflow.genai.datasets`](https://mlflow.org/docs/latest/python_api/mlflow.genai.html#mlflow.genai.datasets) — UC-governed evaluation datasets.
- [`@mlflow.trace`](https://mlflow.org/docs/latest/llms/tracing/) — trace decorator wrapping Genie Space invocations.
- [`mlflow.log_feedback`](https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_feedback) — trace-level feedback API.
- [Custom scorers](https://mlflow.org/docs/latest/llms/scorers/) — `@scorer` patterns used by `make_all_scorers`.
- [MLflow Models — LoggedModel](https://mlflow.org/docs/latest/models.html) — versioning primitive used for accepted candidates and champion promotion.
- [GenAI review sessions](https://mlflow.org/docs/latest/genai/) — finalize-stage review experience.

### Inspirational reference materials

- `building-genai-apps-with-mlflow.md` and `building-genai-apps-with-mlflow.html` (in `~/Projects/Genie_Space_Optimizer/docs/`) — narrative inspiration for the MLflow story and the interactive HTML aesthetic.

## How To Cite This Doc Set

When citing a section to a stakeholder, use the path-prefixed name, e.g. `optimizer-process-design/04-lever-loop-rca-process-spine.md#stage-9—acceptance--rollback`. Anchors are derived from headings; GitHub/Cursor markdown renderers honor them.

## Versioning Note

The optimizer evolves; this documentation set targets the GSO codebase as of the date the documents were authored. When the codebase changes:

- File path references in [Appendix A](A-code-map.md) are the first thing to update.
- `PROCESS_STAGE_ORDER` in [`run_output_contract.py`](../../../src/genie_space_optimizer/optimization/run_output_contract.py) is the canonical spine ordering — if it changes, [04 — Lever Loop](../04-lever-loop-rca-process-spine.md) needs updating in lockstep.
- `LEVER_NAMES` in [`common/config.py`](../../../src/genie_space_optimizer/common/config.py) is the canonical lever roster — if it changes, [05 — Six Levers](../05-six-optimization-levers.md) needs updating.
