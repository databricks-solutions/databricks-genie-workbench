"""Local Lever-Loop Workbench — developer-only tooling.

This package lives under ``packages/genie-space-optimizer/devtools/`` and is
NOT part of the deployable optimizer wheel. It exists to rehearse a
captured production run locally — through the same state machine, the
same Databricks model-serving LLM path, and the same applier surface —
without deploying to Databricks and without mutating any Genie Space.

Codebase boundary
-----------------
* Production optimizer code under ``src/genie_space_optimizer/`` MUST NOT
  import from this package. An import-boundary test enforces this.
* This package MAY import production optimizer code (state machine,
  contract, accessor helpers, etc.) because it is a consumer, not a
  shipped component.
* Tests for this package live under ``tests/workbench/`` so they are
  visually separate from production SM contract tests.

Entry point
-----------
The CLI is invoked by path::

    uv run python devtools/local_lever_workbench/cli.py run \\
        --input <bundle.json> \\
        --llm-mode <stage1-only|sm-tape|live-databricks>

See ``docs/architecture/local-lever-loop-workbench.md`` for the operator
workflow.
"""
from __future__ import annotations

__all__: list[str] = []
