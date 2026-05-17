"""Phase 2 Hotfix — regression test for ``update_iteration_reflection``
(``state.py:~781``) which also calls ``json.dumps`` on a
``reflection_json`` dict that may contain a ``TerminalSignature``.

The per-iteration reflection_json write happens inside
``_run_lever_loop`` after each iteration completes. Without the
encoder, those writes would crash with the same frozenset TypeError
as the end-of-run write_stage.
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pandas as pd

from genie_space_optimizer.optimization.state import (
    update_iteration_reflection,
)
from genie_space_optimizer.optimization.terminal_reason import (
    TerminalReason,
)
from genie_space_optimizer.optimization.terminal_signature import (
    build_terminal_signature,
)


class _RecordingSpark:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []

    def sql(self, statement: str) -> Any:
        self.executed_sql.append(statement)
        df = MagicMock()
        df.toPandas.return_value = pd.DataFrame()
        return df


def test_update_iteration_reflection_handles_terminal_signature():
    """Reproduces what would crash the per-iteration write if a
    non-accepted iteration's reflection_json carried a Phase-2
    TerminalSignature."""
    spark = _RecordingSpark()
    sig = build_terminal_signature(
        root_cause="propagation_lag",
        blame_set=["cat.sch.tbl.col"],
        lever_set=[5, 6],
        target_qids=["gs_009"],
        terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
    )
    reflection_json = {
        "iteration": 1,
        "ag_id": "AG_DECOMPOSED_H001",
        "accepted": False,
        "terminal_signature": sig,
    }

    update_iteration_reflection(
        spark=spark,
        run_id="hotfix-update-run",
        iteration=1,
        reflection_json=reflection_json,
        catalog="main",
        schema="genie_space_optimizer",
    )

    updates = [
        s for s in spark.executed_sql
        if s.lstrip().upper().startswith("UPDATE")
    ]
    assert len(updates) == 1, (
        f"Expected one UPDATE statement, got {len(updates)}: "
        f"{updates!r}"
    )
    rendered = updates[0]
    assert "frozenset" not in rendered, (
        "update_iteration_reflection rendered the raw frozenset() repr."
    )
    assert '"lever_set": [5, 6]' in rendered, (
        f"Expected sorted list form for lever_set. Got: {rendered}"
    )
    assert '"target_qids": ["gs_009"]' in rendered, (
        f"Expected sorted list form for target_qids. Got: {rendered}"
    )
