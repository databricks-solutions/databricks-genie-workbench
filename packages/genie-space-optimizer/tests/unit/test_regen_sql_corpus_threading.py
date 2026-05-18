"""WU-2 — SQL-corpus threading in _regenerate_rca_for_cluster."""
from __future__ import annotations

from unittest.mock import patch


def test_flag_default_on() -> None:
    from genie_space_optimizer.common.config import (
        rca_regen_thread_sql_corpora_enabled,
    )
    assert rca_regen_thread_sql_corpora_enabled() is True


def test_flag_off_when_explicit_zero(monkeypatch) -> None:
    from genie_space_optimizer.common.config import (
        rca_regen_thread_sql_corpora_enabled,
    )
    monkeypatch.setenv("GSO_RCA_REGEN_THREAD_SQL_CORPORA", "0")
    assert rca_regen_thread_sql_corpora_enabled() is False


# ── stash helper ─────────────────────────────────────────────────────


def test_stash_populates_both_keys_when_flag_on() -> None:
    """Helper stamps _generated_sql_by_qid and _reference_sql_by_qid
    onto metadata_snapshot in place."""
    from genie_space_optimizer.optimization.harness import (
        stash_sql_corpora_on_metadata,
    )

    ms: dict = {}
    stash_sql_corpora_on_metadata(
        metadata_snapshot=ms,
        qid_to_generated_sql={"gs_009": "SELECT * FROM r RANK()"},
        qid_to_reference_sql={"gs_009": "SELECT * FROM r LIMIT 10"},
    )
    assert ms["_generated_sql_by_qid"] == {"gs_009": "SELECT * FROM r RANK()"}
    assert ms["_reference_sql_by_qid"] == {"gs_009": "SELECT * FROM r LIMIT 10"}


def test_stash_is_noop_when_flag_off(monkeypatch) -> None:
    """With GSO_RCA_REGEN_THREAD_SQL_CORPORA=0, the helper does
    not write either key (byte-stable with pre-WU-2)."""
    from genie_space_optimizer.optimization.harness import (
        stash_sql_corpora_on_metadata,
    )

    monkeypatch.setenv("GSO_RCA_REGEN_THREAD_SQL_CORPORA", "0")
    ms: dict = {}
    stash_sql_corpora_on_metadata(
        metadata_snapshot=ms,
        qid_to_generated_sql={"gs_009": "SELECT 1"},
        qid_to_reference_sql={"gs_009": "SELECT 2"},
    )
    assert "_generated_sql_by_qid" not in ms
    assert "_reference_sql_by_qid" not in ms


def test_stash_tolerates_none_inputs() -> None:
    """Helper handles None inputs without raising."""
    from genie_space_optimizer.optimization.harness import (
        stash_sql_corpora_on_metadata,
    )

    ms: dict = {}
    stash_sql_corpora_on_metadata(
        metadata_snapshot=ms,
        qid_to_generated_sql=None,
        qid_to_reference_sql=None,
    )
    assert ms["_generated_sql_by_qid"] == {}
    assert ms["_reference_sql_by_qid"] == {}


def test_stash_tolerates_non_dict_metadata() -> None:
    """When metadata_snapshot is not a dict, helper is a no-op."""
    from genie_space_optimizer.optimization.harness import (
        stash_sql_corpora_on_metadata,
    )

    # Should not raise.
    stash_sql_corpora_on_metadata(
        metadata_snapshot=None,
        qid_to_generated_sql={"gs_009": "SELECT 1"},
        qid_to_reference_sql={"gs_009": "SELECT 2"},
    )


# ── _regenerate_rca_for_cluster forwarding ───────────────────────────


def test_regen_forwards_sql_corpora_to_build_rca_card(monkeypatch) -> None:
    """When _generated_sql_by_qid / _reference_sql_by_qid are on
    metadata_snapshot, the recovery helper forwards them to
    build_rca_card."""
    from genie_space_optimizer.optimization.harness import (
        _regenerate_rca_for_cluster,
    )

    captured = []

    def fake_build(**kwargs):
        captured.append(kwargs)
        return {"rca_id": ""}  # Force second attempt too.

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.rca.build_rca_card", fake_build
    )

    metadata = {
        "_failure_buckets": {"sig": ["gs_009"]},
        "_asi_metadata": {"gs_009": {"failure_type": "plural_top_n_collapse"}},
        "_generated_sql_by_qid": {"gs_009": "SELECT * FROM routes RANK()"},
        "_reference_sql_by_qid": {"gs_009": "SELECT * FROM routes LIMIT 10"},
    }

    failure_cluster = {
        "cluster_id": "H001",
        "question_ids": ("gs_009",),
    }

    _ = _regenerate_rca_for_cluster(
        spark=None,
        run_id="run_x",
        cluster=failure_cluster,
        metadata_snapshot=metadata,
    )

    # Both attempts (failure_buckets then asi) must forward the corpora.
    assert len(captured) == 2
    for call_kwargs in captured:
        assert call_kwargs.get("generated_sql_by_qid") == {
            "gs_009": "SELECT * FROM routes RANK()"
        }
        assert call_kwargs.get("reference_sql_by_qid") == {
            "gs_009": "SELECT * FROM routes LIMIT 10"
        }


def test_regen_handles_missing_sql_keys(monkeypatch) -> None:
    """When the keys are absent, helper passes empty dicts so
    build_rca_card's defaults kick in."""
    from genie_space_optimizer.optimization.harness import (
        _regenerate_rca_for_cluster,
    )

    captured = []

    def fake_build(**kwargs):
        captured.append(kwargs)
        return {"rca_id": ""}

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.rca.build_rca_card", fake_build
    )

    _ = _regenerate_rca_for_cluster(
        spark=None,
        run_id="run_x",
        cluster={"cluster_id": "H001", "question_ids": ("gs_009",)},
        metadata_snapshot={},
    )

    assert len(captured) == 2
    for call_kwargs in captured:
        assert call_kwargs.get("generated_sql_by_qid") == {}
        assert call_kwargs.get("reference_sql_by_qid") == {}


def test_regen_flag_off_skips_sql_corpora(monkeypatch) -> None:
    """With the flag OFF, the helper does NOT forward SQL corpora
    even if they're present on metadata_snapshot (byte-stable)."""
    from genie_space_optimizer.optimization.harness import (
        _regenerate_rca_for_cluster,
    )

    captured = []

    def fake_build(**kwargs):
        captured.append(kwargs)
        return {"rca_id": ""}

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.rca.build_rca_card", fake_build
    )
    monkeypatch.setenv("GSO_RCA_REGEN_THREAD_SQL_CORPORA", "0")

    _ = _regenerate_rca_for_cluster(
        spark=None,
        run_id="run_x",
        cluster={"cluster_id": "H001", "question_ids": ("gs_009",)},
        metadata_snapshot={
            "_generated_sql_by_qid": {"gs_009": "SELECT 1"},
            "_reference_sql_by_qid": {"gs_009": "SELECT 2"},
        },
    )

    for call_kwargs in captured:
        # Flag-off path: helper does not pass the kwargs.
        assert "generated_sql_by_qid" not in call_kwargs
        assert "reference_sql_by_qid" not in call_kwargs
