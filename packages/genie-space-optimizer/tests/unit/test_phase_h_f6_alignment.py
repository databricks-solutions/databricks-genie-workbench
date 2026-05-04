"""Phase H Completion Task 3: F6 GATE_PIPELINE_ORDER must match
harness's actual inline gate firing order so F6 wire-up becomes a
1:1 additive observability call (no algorithm change, byte-stable
trivially)."""

from genie_space_optimizer.optimization.stages.gates import (
    GATE_PIPELINE_ORDER,
)


def test_gate_pipeline_order_matches_harness_inline_order() -> None:
    expected = (
        "lever5_structural",
        "rca_groundedness",
        "blast_radius",
        "content_fingerprint_dedup",
        "dead_on_arrival",
    )
    assert GATE_PIPELINE_ORDER == expected, (
        f"F6 module order {GATE_PIPELINE_ORDER} disagrees with harness "
        f"inline gate order {expected}; see Phase H Completion Task 3"
    )
