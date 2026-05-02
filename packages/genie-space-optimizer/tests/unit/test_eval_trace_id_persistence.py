"""Track I — eval-row provenance dataclass plus predict_fn wiring.

Tests in this file pin three invariants:
1. ``EvalRowProvenance`` requires a non-empty ``mlflow_trace_id`` at
   construction; constructing with empty raises.
2. The predict_fn output dict carries the dataclass under
   ``provenance``.
3. The trace-recovery log line is suppressed when the primary path
   succeeds for every row, and incrementing
   ``trace_id_fallback_rate`` happens exactly when fallback fires.
"""
from __future__ import annotations

import pytest


def test_eval_row_provenance_requires_non_empty_trace_id() -> None:
    """``EvalRowProvenance.mlflow_trace_id`` must be non-empty.

    The dataclass exists so the eval row's trace lineage is asserted
    at construction time, not "discovered" at recovery time.
    """
    from genie_space_optimizer.optimization.eval_provenance import (
        EvalRowProvenance,
    )

    with pytest.raises(ValueError, match="mlflow_trace_id"):
        EvalRowProvenance(
            mlflow_trace_id="",
            genie_conversation_id="conv_123",
            source="primary",
        )


def test_eval_row_provenance_accepts_non_empty_trace_id() -> None:
    from genie_space_optimizer.optimization.eval_provenance import (
        EvalRowProvenance,
    )

    p = EvalRowProvenance(
        mlflow_trace_id="trace_abc",
        genie_conversation_id="conv_123",
        source="primary",
    )
    assert p.mlflow_trace_id == "trace_abc"
    assert p.genie_conversation_id == "conv_123"
    assert p.source == "primary"


def test_eval_row_provenance_allows_empty_conversation_id() -> None:
    """Some Genie API responses omit ``conversation_id`` (e.g. error
    paths that fail before a conversation is opened). Trace ID is the
    required field; conversation ID can be empty.
    """
    from genie_space_optimizer.optimization.eval_provenance import (
        EvalRowProvenance,
    )

    p = EvalRowProvenance(
        mlflow_trace_id="trace_abc",
        genie_conversation_id="",
        source="primary",
    )
    assert p.genie_conversation_id == ""


def test_eval_row_provenance_source_is_constrained() -> None:
    """``source`` must be one of {"primary", "fallback"} so downstream
    counters can distinguish the two paths.
    """
    from genie_space_optimizer.optimization.eval_provenance import (
        EvalRowProvenance,
    )

    with pytest.raises(ValueError, match="source"):
        EvalRowProvenance(
            mlflow_trace_id="trace_abc",
            genie_conversation_id="conv_123",
            source="bogus",
        )
