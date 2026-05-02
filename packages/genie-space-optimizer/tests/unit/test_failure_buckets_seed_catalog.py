"""Track 7 — failure-bucketing seed catalog. The Phase C classifier
will consume this catalog. Tests pin:

  * The four bucket-name constants exist.
  * Every seed entry has the four required fields.
  * The catalog covers all 16 patterns from the high-level plan's
    Phase C catalog table (lines 439-455).
  * ``match_pattern_id`` returns a stable identifier per pattern.
"""
from __future__ import annotations


def test_failure_bucket_enum_has_four_top_level_values() -> None:
    from genie_space_optimizer.optimization.failure_buckets import (
        FailureBucket,
    )

    names = {b.name for b in FailureBucket}
    assert names == {
        "GATE_OR_CAP_GAP",
        "EVIDENCE_GAP",
        "PROPOSAL_GAP",
        "MODEL_CEILING",
    }


def test_seed_catalog_contains_sixteen_patterns() -> None:
    from genie_space_optimizer.optimization.failure_buckets import SEED_CATALOG

    assert len(SEED_CATALOG) == 16, (
        f"expected 16 seed patterns, got {len(SEED_CATALOG)}"
    )


def test_every_seed_entry_has_required_fields() -> None:
    from genie_space_optimizer.optimization.failure_buckets import (
        BucketingSeedPattern,
        FailureBucket,
        SEED_CATALOG,
    )

    for entry in SEED_CATALOG:
        assert isinstance(entry, BucketingSeedPattern), entry
        assert entry.pattern_id, f"empty pattern_id on {entry}"
        assert entry.description, f"empty description on {entry}"
        assert isinstance(entry.bucket, FailureBucket), entry
        assert entry.sub_bucket, f"empty sub_bucket on {entry}"
        assert entry.source_run, f"empty source_run on {entry}"


def test_pattern_ids_are_unique_across_catalog() -> None:
    from genie_space_optimizer.optimization.failure_buckets import SEED_CATALOG

    ids = [p.pattern_id for p in SEED_CATALOG]
    assert len(ids) == len(set(ids)), (
        f"duplicate pattern_id detected; ids = {ids}"
    )


def test_match_pattern_id_returns_pattern_when_id_matches() -> None:
    from genie_space_optimizer.optimization.failure_buckets import (
        SEED_CATALOG,
        match_pattern_id,
    )

    first = SEED_CATALOG[0]
    found = match_pattern_id(first.pattern_id)
    assert found is first


def test_match_pattern_id_returns_none_when_unknown() -> None:
    from genie_space_optimizer.optimization.failure_buckets import (
        match_pattern_id,
    )

    assert match_pattern_id("nonexistent_pattern") is None


def test_catalog_covers_each_bucket_at_least_once() -> None:
    from genie_space_optimizer.optimization.failure_buckets import (
        FailureBucket,
        SEED_CATALOG,
    )

    buckets_seen = {p.bucket for p in SEED_CATALOG}
    for bucket in FailureBucket:
        assert bucket in buckets_seen, (
            f"FailureBucket.{bucket.name} has zero seed patterns"
        )
