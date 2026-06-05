"""P4 C8 unit tests — Stage 3 prompt size budget primitives."""
from __future__ import annotations

import json

import pytest

from genie_space_optimizer.optimization.stage3_prompt_sizer import (
    STAGE3_PROMPT_TOTAL_CAP,
    CatalogEntry,
    HistoryEntry,
    RcaCardEntry,
    Stage3PromptInput,
    Stage3PromptSegment,
    Stage3PromptSizeBreakdown,
    build_stage3_prompt_budget,
    default_segment_caps,
    estimate_tokens,
    partition_for_sub_cluster_split,
    slice_segments,
    stage3_prompt_size_breakdown_marker,
)


# ---------------------------------------------------------------------
# Trial 21 W3 — slice_segments
# ---------------------------------------------------------------------


def test_slice_segments_no_op_when_under_cap():
    """When the inputs are already under the cap, slicing is a no-op."""
    sliced = slice_segments(
        system_msg_tokens=2_000,
        user_prompt_tokens=10_000,
        cacheable_block_tokens=4_000,
        cap=STAGE3_PROMPT_TOTAL_CAP,
    )
    assert sliced["over_cap"] is False
    assert sliced["observe_only"] is False
    assert sliced["sub_cluster_split_needed"] is False
    assert sliced["user_prompt_tokens"] == 10_000
    assert sliced["total_tokens"] == 16_000


def test_slice_segments_shrinks_user_prompt_to_fit_cap():
    """Run B's 104k payload: user prompt absorbs the entire reduction
    so system + cacheable stay whole and total <= cap post-slice."""
    sliced = slice_segments(
        system_msg_tokens=3_221,
        user_prompt_tokens=96_810,
        cacheable_block_tokens=4_465,
        cap=STAGE3_PROMPT_TOTAL_CAP,
    )
    assert sliced["over_cap"] is False
    assert sliced["observe_only"] is False
    assert sliced["sub_cluster_split_needed"] is True
    assert sliced["system_msg_tokens"] == 3_221
    assert sliced["cacheable_block_tokens"] == 4_465
    # 40000 - 3221 - 4465 = 32314.
    assert sliced["user_prompt_tokens"] == 32_314
    assert sliced["total_tokens"] == STAGE3_PROMPT_TOTAL_CAP


def test_slice_segments_over_cap_when_non_negotiable_alone_exceeds_budget():
    """If the system + cacheable blocks alone exceed the cap, the
    slicer cannot fit anything else; user_prompt drops to 0 and
    over_cap=True propagates so the Actuator escalates."""
    sliced = slice_segments(
        system_msg_tokens=30_000,
        user_prompt_tokens=10_000,
        cacheable_block_tokens=20_000,
        cap=STAGE3_PROMPT_TOTAL_CAP,
    )
    assert sliced["over_cap"] is True
    assert sliced["observe_only"] is False
    assert sliced["sub_cluster_split_needed"] is True
    assert sliced["user_prompt_tokens"] == 0


def test_slice_segments_observe_only_is_always_false():
    """Trial 21 enforce-mode: ``observe_only`` MUST always be False on
    return. The Actuator reads this and drops over-cap proposals."""
    for system in (0, 1_000, 30_000):
        for user in (0, 50_000):
            for cache in (0, 10_000):
                sliced = slice_segments(
                    system_msg_tokens=system,
                    user_prompt_tokens=user,
                    cacheable_block_tokens=cache,
                )
                assert sliced["observe_only"] is False


def test_segment_caps_sum_to_total_cap():
    caps = default_segment_caps()
    assert sum(caps.values()) == STAGE3_PROMPT_TOTAL_CAP


def test_segment_enum_exact_set():
    assert {s.value for s in Stage3PromptSegment} == {
        "history",
        "rca_cards",
        "lever_menu",
        "archetype_catalog",
        "schema_columns",
    }


def test_estimate_tokens_is_monotonic_and_byte_stable():
    short = estimate_tokens("a" * 100)
    longer = estimate_tokens("a" * 1000)
    assert longer > short
    assert estimate_tokens("a" * 100) == short
    assert estimate_tokens("") == 0


def _input_under_cap() -> Stage3PromptInput:
    return Stage3PromptInput(
        cluster_id="cluster_A",
        cluster_qids=("gs_001",),
        history=(HistoryEntry(iteration=1, text="x" * 400),),  # 100 tokens
        rca_cards=(
            RcaCardEntry(qid="gs_001", is_target=True, text="r" * 400),
        ),  # 100 tokens
        lever_menu_text="L" * 400,  # 100 tokens
        archetype_catalog=(CatalogEntry(name="a", text="x" * 400),),
        schema_columns=(CatalogEntry(name="col", text="x" * 400),),
    )


def test_under_cap_returns_total_below_total_cap():
    breakdown, slices = build_stage3_prompt_budget(_input_under_cap())
    assert breakdown.total_tokens <= STAGE3_PROMPT_TOTAL_CAP
    assert not breakdown.sub_cluster_split_needed
    assert breakdown.history_tokens > 0
    assert breakdown.rca_card_tokens > 0
    assert breakdown.lever_menu_tokens > 0
    assert breakdown.archetype_catalog_tokens > 0
    assert breakdown.schema_column_tokens > 0


def test_schema_columns_clamped_to_segment_cap():
    """When schema_columns alone is 50k tokens, post-slice must be <= 10k."""
    huge_text = "x" * (50_000 * 4)  # ~50k tokens
    inp = Stage3PromptInput(
        cluster_id="cluster_huge",
        cluster_qids=("gs_001",),
        history=(),
        rca_cards=(),
        lever_menu_text="",
        archetype_catalog=(),
        schema_columns=(CatalogEntry(name="all", text=huge_text),),
    )
    breakdown, _ = build_stage3_prompt_budget(inp)
    caps = default_segment_caps()
    # Single huge entry can't fit; the entire entry is dropped
    # because it exceeds the segment cap on its own.
    assert breakdown.schema_column_tokens <= caps[Stage3PromptSegment.SCHEMA_COLUMNS]
    assert breakdown.total_tokens <= STAGE3_PROMPT_TOTAL_CAP


def test_history_slice_keeps_most_recent_first():
    history = (
        HistoryEntry(iteration=1, text="iter1 " * 100),
        HistoryEntry(iteration=5, text="iter5 " * 100),
        HistoryEntry(iteration=3, text="iter3 " * 100),
    )
    inp = Stage3PromptInput(
        cluster_id="cluster_h",
        cluster_qids=(),
        history=history,
        rca_cards=(),
        lever_menu_text="",
        archetype_catalog=(),
        schema_columns=(),
    )
    breakdown, slices = build_stage3_prompt_budget(inp)
    # All three fit under 6k tokens budget (3 * ~150 tokens).
    assert breakdown.history_tokens > 0
    # Iter 5 must come first in the kept slice (most-recent-first).
    assert slices.history_entries[0].iteration == 5


def test_rca_cards_target_first_then_lexicographic():
    cards = (
        RcaCardEntry(qid="zzz", is_target=False, text="zzz card"),
        RcaCardEntry(qid="aaa", is_target=False, text="aaa card"),
        RcaCardEntry(qid="bbb", is_target=True, text="bbb target"),
        RcaCardEntry(qid="ccc", is_target=False, text="ccc card"),
    )
    inp = Stage3PromptInput(
        cluster_id="cluster_r",
        cluster_qids=("aaa",),  # aaa is also a target via membership
        history=(),
        rca_cards=cards,
        lever_menu_text="",
        archetype_catalog=(),
        schema_columns=(),
    )
    breakdown, slices = build_stage3_prompt_budget(inp)
    # First two should be targets (aaa via membership, bbb via flag).
    target_qids = [c.qid for c in slices.rca_card_entries[:2]]
    assert set(target_qids) == {"aaa", "bbb"}


def test_slicing_is_byte_stable():
    """Same input two runs in a row must produce identical breakdowns."""
    inp = _input_under_cap()
    b1, _ = build_stage3_prompt_budget(inp)
    b2, _ = build_stage3_prompt_budget(inp)
    assert b1.to_json() == b2.to_json()


def test_sub_cluster_split_partition_is_deterministic():
    inp = Stage3PromptInput(
        cluster_id="cluster_s",
        cluster_qids=("gs_009", "gs_001", "gs_013"),
        history=(),
        rca_cards=(),
        lever_menu_text="",
        archetype_catalog=(),
        schema_columns=(),
    )
    partitions = partition_for_sub_cluster_split(inp, max_qids_per_partition=1)
    # Lex-sorted: gs_001, gs_009, gs_013, one per partition.
    assert partitions == (("gs_001",), ("gs_009",), ("gs_013",))


def test_sub_cluster_split_with_larger_partition():
    inp = Stage3PromptInput(
        cluster_id="cluster_s",
        cluster_qids=("gs_009", "gs_001", "gs_013", "gs_005"),
        history=(),
        rca_cards=(),
        lever_menu_text="",
        archetype_catalog=(),
        schema_columns=(),
    )
    partitions = partition_for_sub_cluster_split(inp, max_qids_per_partition=2)
    assert partitions == (("gs_001", "gs_005"), ("gs_009", "gs_013"))


def test_marker_payload_pins_required_fields():
    inp = _input_under_cap()
    breakdown, _ = build_stage3_prompt_budget(inp)
    line = stage3_prompt_size_breakdown_marker(
        optimization_run_id="run_xyz",
        iteration=3,
        breakdown=breakdown,
    )
    name, _, payload_json = line.partition(" ")
    assert name == "GSO_STAGE3_PROMPT_SIZE_BREAKDOWN_V1"
    payload = json.loads(payload_json)
    for required in (
        "cluster_id",
        "history_tokens",
        "rca_card_tokens",
        "lever_menu_tokens",
        "archetype_catalog_tokens",
        "schema_column_tokens",
        "total_tokens",
        "cap",
        "cacheable_block_tokens",
        "sub_cluster_split_needed",
        "segment_caps",
        "optimization_run_id",
        "iteration",
    ):
        assert required in payload, f"missing {required!r} in marker"
    assert payload["cap"] == STAGE3_PROMPT_TOTAL_CAP


def test_breakdown_json_round_trip():
    inp = _input_under_cap()
    breakdown, _ = build_stage3_prompt_budget(inp)
    blob = breakdown.to_json()
    restored = Stage3PromptSizeBreakdown.from_json(blob)
    assert restored == breakdown


def test_d139_104k_simulation_drops_below_cap():
    """Regression for the d139 production case: peak 96,807 tokens.

    Construct a Stage 3 input whose raw segments would sum to ~104k
    tokens (similar to d139). Post-budget, total_tokens MUST drop to
    <= 40k. Either via segment caps alone or via sub_cluster_split.
    """
    # 30k tokens of schema columns (one huge entry won't fit, but many
    # small entries will be sliced to the 10k cap).
    schema = tuple(
        CatalogEntry(name=f"col_{i:04d}", text="x" * 400)  # ~100 tokens each
        for i in range(300)  # ~30k tokens total raw
    )
    # 25k tokens of archetype catalog
    archetypes = tuple(
        CatalogEntry(name=f"arche_{i:04d}", text="y" * 400)
        for i in range(250)
    )
    # 20k tokens of RCA cards spanning many QIDs
    rca = tuple(
        RcaCardEntry(qid=f"gs_{i:04d}", is_target=False, text="z" * 400)
        for i in range(200)
    )
    # 8k tokens of history
    history = tuple(
        HistoryEntry(iteration=i, text="h" * 400) for i in range(80)
    )
    # 5k tokens of lever menu
    lever_menu = "L" * (5_000 * 4)
    # 16k tokens of cacheable prefix (counted separately)
    cacheable = 16_000

    inp = Stage3PromptInput(
        cluster_id="cluster_d139",
        cluster_qids=("gs_0001",),
        history=history,
        rca_cards=rca,
        lever_menu_text=lever_menu,
        archetype_catalog=archetypes,
        schema_columns=schema,
        cacheable_prefix_tokens=cacheable,
    )
    breakdown, _ = build_stage3_prompt_budget(inp)
    caps = default_segment_caps()
    assert breakdown.history_tokens <= caps[Stage3PromptSegment.HISTORY]
    assert breakdown.rca_card_tokens <= caps[Stage3PromptSegment.RCA_CARDS]
    assert breakdown.lever_menu_tokens <= caps[Stage3PromptSegment.LEVER_MENU]
    assert (
        breakdown.archetype_catalog_tokens
        <= caps[Stage3PromptSegment.ARCHETYPE_CATALOG]
    )
    assert (
        breakdown.schema_column_tokens
        <= caps[Stage3PromptSegment.SCHEMA_COLUMNS]
    )
    assert breakdown.total_tokens <= STAGE3_PROMPT_TOTAL_CAP
    assert breakdown.cacheable_block_tokens == 16_000
