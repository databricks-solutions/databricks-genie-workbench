"""Pin attribution kwargs to build_question_regression_rows."""

from __future__ import annotations

import inspect

from genie_space_optimizer.optimization import harness


def test_call_passes_attribution() -> None:
    src = inspect.getsource(harness._run_lever_loop)
    call_block = src.split("build_question_regression_rows(", 1)[1].split(")", 1)[0]
    for kw in ("cluster_ids_by_qid=", "proposal_ids_by_qid=", "applied_patch_ids="):
        assert kw in call_block, f"missing {kw}"
