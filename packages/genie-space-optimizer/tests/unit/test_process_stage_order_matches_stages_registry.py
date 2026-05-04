"""Phase H Task 2: PROCESS_STAGE_ORDER ↔ STAGES reconciliation.

The 9 STAGES registry keys must appear in PROCESS_STAGE_ORDER in the
same relative order. PROCESS_STAGE_ORDER may have additional entries
(post_patch_evaluation, contract_health) that don't correspond to
distinct STAGES entries — those are transcript-only.
"""

from __future__ import annotations


_TRANSCRIPT_ONLY_KEYS = {
    "post_patch_evaluation",
    "contract_health",
}


def test_every_stages_key_appears_in_process_stage_order_in_order() -> None:
    from genie_space_optimizer.optimization.run_output_contract import (
        PROCESS_STAGE_ORDER,
    )
    from genie_space_optimizer.optimization.stages import STAGES

    process_keys = [stage.key for stage in PROCESS_STAGE_ORDER]
    stages_keys = [entry.stage_key for entry in STAGES]

    process_keys_excl_transcript_only = [
        k for k in process_keys if k not in _TRANSCRIPT_ONLY_KEYS
    ]
    assert process_keys_excl_transcript_only == stages_keys, (
        "PROCESS_STAGE_ORDER's executable subset must match STAGES order:\n"
        f"  PROCESS (executable subset): {process_keys_excl_transcript_only}\n"
        f"  STAGES                     : {stages_keys}"
    )


def test_transcript_only_keys_are_documented() -> None:
    """Catches the case where a new transcript-only key is added without
    updating _TRANSCRIPT_ONLY_KEYS in this test."""
    from genie_space_optimizer.optimization.run_output_contract import (
        PROCESS_STAGE_ORDER,
    )
    from genie_space_optimizer.optimization.stages import STAGES

    stages_keys = {entry.stage_key for entry in STAGES}
    transcript_only = {
        s.key for s in PROCESS_STAGE_ORDER if s.key not in stages_keys
    }
    assert transcript_only == _TRANSCRIPT_ONLY_KEYS, (
        f"transcript-only keys drift: actual={transcript_only}, "
        f"expected={_TRANSCRIPT_ONLY_KEYS}"
    )
