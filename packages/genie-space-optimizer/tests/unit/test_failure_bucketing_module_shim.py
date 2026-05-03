"""Phase D failure-bucketing T1: module rename + shim.

Asserts:
- New module path `optimization.failure_bucketing` exposes the public
  symbols (FailureBucket, SEED_CATALOG, BucketingSeedPattern,
  match_pattern_id).
- Old module path `optimization.failure_buckets` still imports them
  but emits a DeprecationWarning.
- Both modules expose the SAME enum identity (so isinstance checks
  written before the rename keep working).
"""
from __future__ import annotations

import warnings


def test_new_module_exposes_public_surface():
    from genie_space_optimizer.optimization import failure_bucketing as fb

    assert hasattr(fb, "FailureBucket")
    assert hasattr(fb, "BucketingSeedPattern")
    assert hasattr(fb, "SEED_CATALOG")
    assert hasattr(fb, "match_pattern_id")


def test_legacy_module_path_still_imports_and_warns():
    import importlib
    import sys

    # Force a fresh import so the DeprecationWarning fires. Use both
    # sys.modules cleanup AND importlib.reload to handle cases where
    # other tests in the suite have already imported the legacy module
    # (Python's warning dedupe + module cache can suppress otherwise).
    legacy_name = "genie_space_optimizer.optimization.failure_buckets"
    sys.modules.pop(legacy_name, None)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        legacy = importlib.import_module(legacy_name)
        assert hasattr(legacy, "FailureBucket")
        assert hasattr(legacy, "SEED_CATALOG")
    deprecation_warnings = [
        w for w in caught if issubclass(w.category, DeprecationWarning)
    ]
    assert deprecation_warnings, (
        "expected a DeprecationWarning when importing the legacy "
        "failure_buckets module"
    )
    assert any(
        "failure_bucketing" in str(w.message) for w in deprecation_warnings
    )


def test_both_paths_share_enum_identity():
    from genie_space_optimizer.optimization import failure_bucketing as new
    from genie_space_optimizer.optimization import failure_buckets as legacy
    assert new.FailureBucket is legacy.FailureBucket
