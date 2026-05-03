"""Backward-compatible shim for the renamed failure-bucketing module.

The seed catalog and enum live in ``failure_bucketing`` (singular,
roadmap-aligned) as of Phase D Failure-Bucketing T1. This shim
preserves the legacy plural import path until Phase E removes it.
"""
from __future__ import annotations

import warnings

warnings.warn(
    "genie_space_optimizer.optimization.failure_buckets is deprecated; "
    "import from genie_space_optimizer.optimization.failure_bucketing "
    "instead. The shim is removed in Phase E.",
    DeprecationWarning,
    stacklevel=2,
)

from genie_space_optimizer.optimization.failure_bucketing import (  # noqa: E402,F401
    BucketingSeedPattern,
    FailureBucket,
    SEED_CATALOG,
    match_pattern_id,
)
