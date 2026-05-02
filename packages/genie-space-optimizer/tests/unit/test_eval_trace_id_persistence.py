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


def test_predict_fn_output_carries_eval_row_provenance() -> None:
    """The predict_fn at ``evaluation.py:4699-5354`` must construct an
    ``EvalRowProvenance`` from the active MLflow span's trace id and
    attach it to the output dict under ``provenance``. This is the
    primary-path persistence Track I requires.
    """
    import inspect

    from genie_space_optimizer.optimization import evaluation

    src = inspect.getsource(evaluation.make_predict_fn)
    # Must import and reference EvalRowProvenance.
    assert "EvalRowProvenance" in src, (
        "predict_fn does not import or reference EvalRowProvenance"
    )
    # Must read the active span's trace id (or last active trace id)
    # at predict_fn time.
    assert (
        "get_current_active_span" in src
        or "get_last_active_trace_id" in src
    ), (
        "predict_fn must read the active span's trace id; "
        "neither mlflow.get_current_active_span nor "
        "mlflow.get_last_active_trace_id appears in the source"
    )
    # Must attach provenance to the returned output dict.
    assert '"provenance"' in src, (
        'predict_fn output dict missing the "provenance" key'
    )


def test_recovery_log_suppressed_when_primary_path_succeeds() -> None:
    """When the primary path produces a non-empty trace_map for every
    row, the ``[Eval] Recovered N/N trace IDs via fallback strategies``
    line must NOT print and the fallback counter must not increment.
    """
    import inspect

    from genie_space_optimizer.optimization import evaluation

    src = inspect.getsource(evaluation)
    anchor = "Recovered"
    idx = src.find(anchor)
    assert idx >= 0, (
        "evaluation.py no longer contains the Recovered anchor; "
        "did the recovery block move?"
    )
    pre = src[max(0, idx - 1200) : idx]
    assert "if not trace_map" in pre, (
        "Recovered log line is no longer guarded by 'if not trace_map'; "
        "the line will print on every iteration regardless of fallback"
    )
    assert "record_fallback_recovery" in src, (
        "evaluation.py does not call record_fallback_recovery; the "
        "trace_id_fallback_rate counter cannot increment"
    )
    block_after = src[idx : idx + 600]
    assert "logger.warning" in block_after or "logger.warn" in block_after, (
        "Recovery line still uses print(); spec requires logger.warning"
    )


def test_record_fallback_recovery_increments_counter() -> None:
    """``record_fallback_recovery`` must update the in-memory counter."""
    from genie_space_optimizer.optimization.eval_provenance import (
        record_fallback_recovery,
        reset_fallback_counter,
        trace_id_fallback_rate,
    )

    reset_fallback_counter()
    assert trace_id_fallback_rate() == 0.0

    record_fallback_recovery(recovered_count=3, total_rows=10)
    rate = trace_id_fallback_rate()
    assert 0.0 < rate <= 1.0, f"unexpected rate: {rate}"
    assert abs(rate - 0.3) < 1e-9, f"expected 0.3, got {rate}"
