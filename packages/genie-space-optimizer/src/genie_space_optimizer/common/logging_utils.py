"""Shared logging utilities (Tier 3.7).

``quiet_grpc_logs`` was previously only usable from ``benchmarks.py``; moving
it here lets ``scorers/syntax_validity.py`` (and any other EXPLAIN / spark.sql
call site) wrap the same helper so every path produces at most one digest
line per gRPC failure — not three stack traces from reattach retries.

The helper is import-safe under test environments that don't have the
``pyspark`` logger registered; it acts as a no-op if the logger is missing.
"""

from __future__ import annotations

import io
import logging
from contextlib import contextmanager
from typing import Iterator


class _Summary:
    """Object returned by ``quiet_grpc_logs``; exposes ``.get()``.

    Kept as a class rather than a closure so the type shows up clearly in
    stack traces and doctests.
    """

    def __init__(self, buf: io.StringIO) -> None:
        self._buf = buf

    def get(self) -> str:
        raw = self._buf.getvalue()
        if not raw:
            return ""
        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
        if len(lines) <= 1:
            return lines[0] if lines else ""
        return f"{lines[0]} (+{len(lines) - 1} more gRPC errors)"


@contextmanager
def quiet_grpc_logs() -> Iterator[_Summary]:
    """Capture ``pyspark.sql.connect.logging`` into a buffer.

    Yields a summary object whose ``.get()`` returns a one-line digest of
    any captured gRPC errors. Safe no-op when the pyspark logger can't be
    obtained (e.g. unit tests on a machine without pyspark installed).
    """
    try:
        grpc_logger = logging.getLogger("pyspark.sql.connect.logging")
    except Exception:
        yield _Summary(io.StringIO())
        return

    prev_propagate = grpc_logger.propagate
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message).200s"))
    grpc_logger.addHandler(handler)
    grpc_logger.propagate = False
    try:
        yield _Summary(buf)
    finally:
        grpc_logger.removeHandler(handler)
        grpc_logger.propagate = prev_propagate
        handler.close()
        buf.close()
