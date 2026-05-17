"""Phase 2 Hotfix — regression test for the visible crash at
``harness.py:~31346`` → ``state.py:524`` (``write_stage``).

The reproducer constructs a reflection-buffer entry shaped exactly
like what ``_build_reflection_entry`` produces (a dict with a
``terminal_signature`` key whose value is a ``TerminalSignature``
NamedTuple containing frozensets), then asserts ``write_stage``
produces valid JSON for the ``detail_json`` column.

The test stubs the Spark side (``write_stage``'s actual Delta INSERT)
via a recording fake, so the test runs in-process and exercises only
the JSON-serialization branch.
"""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pandas as pd

from genie_space_optimizer.optimization.state import write_stage
from genie_space_optimizer.optimization.terminal_reason import (
    TerminalReason,
)
from genie_space_optimizer.optimization.terminal_signature import (
    build_terminal_signature,
)


class _RecordingSpark:
    """Minimal Spark stub that captures every executed SQL string.

    ``write_stage`` calls ``sql`` (for the started_at lookup) and
    then issues an INSERT. ``executed_sql`` exposes both so the test
    can inspect the rendered ``detail_json`` literal."""

    def __init__(self) -> None:
        self.executed_sql: list[str] = []

    def sql(self, statement: str) -> Any:
        self.executed_sql.append(statement)
        df = MagicMock()
        df.toPandas.return_value = pd.DataFrame()
        return df


def _phase_2_shaped_reflection_buffer() -> list[dict]:
    sig = build_terminal_signature(
        root_cause="propagation_lag",
        blame_set=["cat.sch.tbl.col"],
        lever_set=[5, 6],
        target_qids=["gs_009"],
        terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY,
    )
    return [
        {
            "iteration": 1,
            "ag_id": "AG_DECOMPOSED_H001",
            "accepted": False,
            "rollback_reason": "no_proposals",
            "terminal_signature": sig,
            "cluster_signature": (("c1", ("gs_009",)),),
            "emitted_patch_shape": "none",
            "levers": [5],
            "target_objects": [],
        }
    ]


def test_write_stage_serializes_terminal_signature_without_crashing():
    """Reproduces the Trial-5 2026-05-17 05:44 UTC crash. With the
    hotfix in place, ``write_stage`` serializes the buffer via
    ``GsoJsonEncoder`` instead of bare ``json.dumps``."""
    spark = _RecordingSpark()
    detail = {
        "levers_attempted": [5, 6],
        "levers_accepted": [],
        "levers_rolled_back": [5],
        "reflection_buffer": _phase_2_shaped_reflection_buffer(),
    }

    write_stage(
        spark=spark,
        run_id="hotfix-test-run",
        stage="LEVER_LOOP_STARTED",
        status="COMPLETE",
        task_key="lever_loop",
        detail=detail,
        catalog="main",
        schema="genie_space_optimizer",
    )

    inserts = [
        s for s in spark.executed_sql
        if s.lstrip().upper().startswith("INSERT")
    ]
    assert len(inserts) == 1, (
        f"Expected exactly one INSERT, got {len(inserts)}: {inserts}"
    )
    assert "frozenset" not in inserts[0], (
        "write_stage rendered the raw frozenset() repr — the encoder "
        "is not wired in."
    )
    assert '"lever_set": [5, 6]' in inserts[0], (
        f"Expected sorted list form for lever_set in detail_json. "
        f"Got: {inserts[0]}"
    )
    assert '"target_qids": ["gs_009"]' in inserts[0], (
        f"Expected sorted list form for target_qids in detail_json. "
        f"Got: {inserts[0]}"
    )
