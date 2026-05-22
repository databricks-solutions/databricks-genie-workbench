"""narrow_skipped_no_original_patch_type is now a contract failure, not an outcome."""
import pytest

from genie_space_optimizer.optimization.blast_radius_drop_record import (
    BlastRadiusDropRecord,
)
from genie_space_optimizer.optimization.stages.narrow_replacement import (
    NarrowReplacementContractError,
    require_complete_drop_record,
)


def _rec(**overrides) -> BlastRadiusDropRecord:
    base = dict(
        intent_id="i",
        original_patch_type="t",
        original_patch_body={"name": "b"},
        causal_target="ct",
        failing_sql_anchor="a",
        target_qids=("q",),
        collateral_qids=("c",),
        protected_sql_by_qid={},
        rca_card_id="r",
        cluster_id="H001",
        ag_id="AG",
    )
    base.update(overrides)
    return BlastRadiusDropRecord(**base)


def test_complete_drop_record_passes():
    require_complete_drop_record(_rec())  # no raise


def test_missing_original_patch_type_raises():
    rec = _rec(original_patch_type="")
    with pytest.raises(NarrowReplacementContractError, match="original_patch_type"):
        require_complete_drop_record(rec)


def test_missing_original_patch_body_raises():
    rec = _rec(original_patch_body={})
    with pytest.raises(NarrowReplacementContractError, match="original_patch_body"):
        require_complete_drop_record(rec)


def test_missing_failing_sql_anchor_raises():
    rec = _rec(failing_sql_anchor="")
    with pytest.raises(NarrowReplacementContractError, match="failing_sql_anchor"):
        require_complete_drop_record(rec)


def test_missing_target_qids_raises():
    rec = _rec(target_qids=())
    with pytest.raises(NarrowReplacementContractError, match="target_qids"):
        require_complete_drop_record(rec)
