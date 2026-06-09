"""Process-wide serialization for shared Spark Connect access.

The MLflow eval harness runs scorers across a thread pool (up to
``scorer_workers`` threads). Spark Connect's client session and the
underlying gRPC / pyarrow C extensions are not safe to drive from
multiple threads concurrently — doing so can corrupt native state and
crash the kernel with a SIGSEGV (exit code 139).

Any code path that issues ``spark.sql(...)`` from within a thread-pooled
scorer (or that could otherwise overlap with one) should wrap the call in
``spark_serialized()`` so only one thread touches the shared session at a
time. The lock is module-level (one per process), so it serializes across
every caller that uses it.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator

# One lock per process. All callers share it, so Spark Connect is never
# driven by two threads at once.
_SPARK_LOCK = threading.Lock()


@contextmanager
def spark_serialized() -> Iterator[None]:
    """Hold the global Spark lock for the duration of the block.

    Wrap any ``spark.sql(...)`` / ``.toPandas()`` call that may run
    concurrently with another Spark Connect call (e.g. inside a
    thread-pooled scorer) to prevent native-extension crashes.
    """
    with _SPARK_LOCK:
        yield
