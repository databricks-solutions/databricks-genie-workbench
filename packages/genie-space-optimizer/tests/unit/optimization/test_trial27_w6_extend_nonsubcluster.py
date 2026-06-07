"""Trial 27 W27.1 — extend W6 partitioned re-dispatch to non-subcluster
clusters.

Pre-Trial-27 the Trial 23 W6 partition path was gated on
``_w6_is_subcluster`` (cluster_id contains ``"subcluster"``), so the
7now live verification's regular clusters overflowed Stage 3 with
``prompt_too_large`` and never produced a structural proposal. W27.1
relaxes that gate: any cluster with ``sub_cluster_split_needed=True``
now gets the partitioned re-dispatch, behind sub-flag
``GSO_TRIAL27_W6_EXTEND_NONSUBCLUSTER`` (default ON when master
``GSO_TRIAL27_STAGE3_DESTARVE`` is ON).

Pins:

* Non-subcluster cluster with split needed AND W27.1 flag ON →
  partitioned re-dispatch fires, ``GSO_TRIAL27_W6_EXTENDED_V1``
  marker emitted alongside the existing
  ``GSO_TRIAL23_SUBCLUSTER_REAL_SLICE_V1`` marker.
* Non-subcluster cluster with split needed AND W27.1 flag OFF →
  byte-stable: ONE oversized LLM call, no W6 marker, no W27
  marker (matches pre-Trial-27 behaviour).
* Subcluster cluster (regression): existing W6 path fires; the
  W27.1 ``EXTENDED`` marker MUST NOT appear (the path was not
  extended for subcluster clusters — they always took it).
* Bright-line #5 — non-subcluster cluster where the partition
  returns ``len(parts) == 1`` (cannot split further): falls
  through to the single-call branch, no W6 marker, no W27 marker.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningResponse,
)


def _resp(succeeded, proposals=None, declined=None, t_in=10, t_out=5):
    return LlmReasoningResponse(
        call_id="c",
        skill_id="plan11_synthesize",
        succeeded=succeeded,
        parsed_output=({"proposals": proposals or []} if succeeded else None),
        declined=declined,
        raw_text="{}",
        tokens_input=t_in,
        tokens_output=t_out,
        duration_ms=3,
        error=None,
    )


def _ok_response():
    return _resp(
        True,
        [
            {
                "intent_name": "x",
                "intent_description": "y",
                "repair_hypothesis": "z",
                "patch_type": "add_example_sql",
                "rationale": "r",
                "confidence": "high",
                "patch_body": {
                    "example_question": "q?",
                    "example_sql": "SELECT 1",
                },
                "blame_set": [],
                "target_qids": ["gs_001"],
            }
        ],
    )


def _nonsubcluster_cluster():
    """Cluster whose id does NOT contain ``"subcluster"``.

    This is the production-shape that 7now live verification exposed
    as overflowing Stage 3 (regular failure cluster, not an RCA
    subcluster slice).
    """
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )
    return FailureCluster(
        cluster_id="cluster_top_n_collapse_001",
        semantic_theme="theme",
        member_qids=("gs_001", "gs_002"),
        unifying_evidence="evidence",
        repair_hypothesis="hyp",
        primary_blame_set=("cat.sch.orders.amount",),
        confidence="high",
        root_cause="",
    )


def _subcluster_cluster():
    """Regression fixture mirroring the existing W6 test path."""
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )
    return FailureCluster(
        cluster_id="H001_subcluster_a",
        semantic_theme="theme",
        member_qids=("gs_001", "gs_002"),
        unifying_evidence="evidence",
        repair_hypothesis="hyp",
        primary_blame_set=("cat.sch.orders.amount",),
        confidence="high",
        root_cause="",
    )


def _force_split_sizer(monkeypatch, *, partitions):
    """Monkeypatch the sizer to force ``sub_cluster_split_needed=True``
    and return the requested partition shape.
    """
    import genie_space_optimizer.optimization.stage3_prompt_sizer as sizer

    monkeypatch.setattr(
        sizer,
        "slice_segments",
        lambda **kw: {
            "system_msg_tokens": 1,
            "cacheable_block_tokens": 1,
            "user_prompt_tokens": 1,
            "total_tokens": 3,
            "cap": 40000,
            "over_cap": False,
            "observe_only": False,
            "sub_cluster_split_needed": True,
        },
    )
    monkeypatch.setattr(
        sizer,
        "partition_rca_subcluster_by_token_budget",
        lambda **kw: partitions,
    )


def _invoke_synthesis(cluster, *, invoke_side_effect):
    invoke = MagicMock(side_effect=invoke_side_effect)
    with patch(
        "genie_space_optimizer.optimization.stages.synthesize.LlmReasoningCall"
    ) as MockCall:
        MockCall.return_value.invoke = invoke
        from genie_space_optimizer.optimization.stages.synthesize import (
            run_plan11_synthesis_for_single_cluster,
        )
        run_plan11_synthesis_for_single_cluster(
            cluster=cluster,
            schema_slice={},
            history=[],
            optimization_run_id="run_x",
            iteration=2,
            ag_id="AG_H001",
            w=MagicMock(),
        )
    return invoke


# ---- W27.1 extension fires on non-subcluster clusters --------------


def test_nonsubcluster_split_needed_fires_w27_extension(
    capsys, monkeypatch
):
    """Non-subcluster cluster with ``sub_cluster_split_needed=True``
    triggers the W6 partitioned re-dispatch and emits the new
    ``GSO_TRIAL27_W6_EXTENDED_V1`` marker (alongside the existing
    ``GSO_TRIAL23_SUBCLUSTER_REAL_SLICE_V1`` marker).
    """
    monkeypatch.delenv("GSO_TRIAL27_STAGE3_DESTARVE", raising=False)
    monkeypatch.delenv(
        "GSO_TRIAL27_W6_EXTEND_NONSUBCLUSTER", raising=False
    )

    _force_split_sizer(
        monkeypatch, partitions=(("gs_001",), ("gs_002",))
    )

    invoke = _invoke_synthesis(
        _nonsubcluster_cluster(),
        invoke_side_effect=[_ok_response(), _ok_response()],
    )

    assert invoke.call_count == 2, (
        "W27.1: non-subcluster cluster with split_needed must issue "
        "one LLM call per partition"
    )
    out = capsys.readouterr().out
    assert "GSO_TRIAL23_SUBCLUSTER_REAL_SLICE_V1" in out, (
        "existing W6 real-dispatch marker must still fire on the "
        "extended path"
    )
    assert "GSO_TRIAL27_W6_EXTENDED_V1" in out, (
        "W27.1: a new marker must signal that the extension path "
        "engaged on a non-subcluster cluster"
    )


def test_nonsubcluster_split_needed_flag_off_falls_back_to_single_call(
    capsys, monkeypatch
):
    """W27.1 flag OFF restores pre-Trial-27 behaviour byte-stably:
    one oversized LLM call, no W6 marker, no W27 marker."""
    monkeypatch.delenv("GSO_TRIAL27_STAGE3_DESTARVE", raising=False)
    monkeypatch.setenv("GSO_TRIAL27_W6_EXTEND_NONSUBCLUSTER", "0")

    _force_split_sizer(
        monkeypatch, partitions=(("gs_001",), ("gs_002",))
    )

    invoke = _invoke_synthesis(
        _nonsubcluster_cluster(),
        invoke_side_effect=[_ok_response()],
    )

    assert invoke.call_count == 1, (
        "rollback: with W27.1 flag OFF, non-subcluster clusters "
        "must take the single oversized call path"
    )
    out = capsys.readouterr().out
    assert "GSO_TRIAL27_W6_EXTENDED_V1" not in out
    assert "GSO_TRIAL23_SUBCLUSTER_REAL_SLICE_V1" not in out


def test_nonsubcluster_split_needed_master_off_disables(
    capsys, monkeypatch
):
    """Master OFF forces the W27.1 sub-flag OFF."""
    monkeypatch.setenv("GSO_TRIAL27_STAGE3_DESTARVE", "0")
    monkeypatch.setenv("GSO_TRIAL27_W6_EXTEND_NONSUBCLUSTER", "1")

    _force_split_sizer(
        monkeypatch, partitions=(("gs_001",), ("gs_002",))
    )

    invoke = _invoke_synthesis(
        _nonsubcluster_cluster(),
        invoke_side_effect=[_ok_response()],
    )

    assert invoke.call_count == 1, (
        "master OFF forces sub-flag OFF regardless of env"
    )
    out = capsys.readouterr().out
    assert "GSO_TRIAL27_W6_EXTENDED_V1" not in out


# ---- Bright-line #5 preservation -----------------------------------


def test_nonsubcluster_partition_single_part_falls_through(
    capsys, monkeypatch
):
    """Bright-line #5 — when the partition returns len(parts)==1
    (cannot split further), the path falls through to the single-call
    branch. No W6 dispatch, no markers — preserves the working H001-
    shape clusters that succeed as a single oversized call.
    """
    monkeypatch.delenv("GSO_TRIAL27_STAGE3_DESTARVE", raising=False)
    monkeypatch.delenv(
        "GSO_TRIAL27_W6_EXTEND_NONSUBCLUSTER", raising=False
    )

    _force_split_sizer(
        monkeypatch, partitions=(("gs_001", "gs_002"),)
    )

    invoke = _invoke_synthesis(
        _nonsubcluster_cluster(),
        invoke_side_effect=[_ok_response()],
    )

    assert invoke.call_count == 1, (
        "single-partition result must fall through to one LLM call "
        "(bright-line #5: H001-shape preservation)"
    )
    out = capsys.readouterr().out
    assert "GSO_TRIAL23_SUBCLUSTER_REAL_SLICE_V1" not in out
    assert "GSO_TRIAL27_W6_EXTENDED_V1" not in out


# ---- Subcluster regression -----------------------------------------


def test_subcluster_path_does_not_emit_w27_extended_marker(
    capsys, monkeypatch
):
    """Existing subcluster path must NOT emit the W27.1 marker —
    the marker signals 'the extension engaged', and the extension
    only applies to non-subcluster clusters (subcluster clusters
    always took this path).
    """
    monkeypatch.delenv("GSO_TRIAL27_STAGE3_DESTARVE", raising=False)
    monkeypatch.delenv(
        "GSO_TRIAL27_W6_EXTEND_NONSUBCLUSTER", raising=False
    )

    _force_split_sizer(
        monkeypatch, partitions=(("gs_001",), ("gs_002",))
    )

    invoke = _invoke_synthesis(
        _subcluster_cluster(),
        invoke_side_effect=[_ok_response(), _ok_response()],
    )

    assert invoke.call_count == 2, (
        "subcluster regression: existing W6 path must still fire"
    )
    out = capsys.readouterr().out
    assert "GSO_TRIAL23_SUBCLUSTER_REAL_SLICE_V1" in out, (
        "existing W6 marker must remain on subcluster path "
        "(byte-stable regression)"
    )
    assert "GSO_TRIAL27_W6_EXTENDED_V1" not in out, (
        "W27.1 marker MUST NOT fire on subcluster clusters — it "
        "signals only the non-subcluster extension"
    )
