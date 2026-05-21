"""Plan 12 PR 4 deferred wire-in — workspace client threading test.

The PR 4 helper ``_attempt_llm_narrow_replacement_and_emit_outcome``
bails out unless BOTH conditions hold:

  1. ``GSO_PLAN12_LIVE_NARROW_REPLACEMENT=1``
  2. ``w`` (workspace client) is non-None

Production callers must thread ``w`` through
``_run_narrow_l6_replacement_loop`` so the LLM path can fire. The
deferred wire-in adds ``w=w`` to the two callsites in the harness's
``_run_lever_loop`` body (harness.py:~26871 and ~27443).

This test proves the threading is in place: when
``_run_narrow_l6_replacement_loop`` is invoked with ``w=<sentinel>``,
the inner helper sees the sentinel; when ``w=None`` (legacy callers
who haven't been threaded), the inner helper bails out cleanly.
"""
from unittest.mock import patch


def _drop_record_with_intent_id() -> dict:
    return {
        "reason": "high_collateral_risk_flagged",
        "original_patch": {
            "intent_id": "intent_test",
            "patch_type": "add_sql_snippet_filter",
            "patch_body": {
                "name": "test_filter",
                "sql_expression": "order_date >= CURRENT_DATE",
            },
            "target_qids": ["q1"],
            "causal_target": "catalog.schema.orders.order_date",
            "cluster_id": "H001",
            "ag_id": "AG1",
        },
        "patch_type": "add_sql_snippet_filter",
        "proposal_id": "P001",
        "collateral_qids": ["q_collateral"],
    }


def test_w_threaded_to_helper_when_supplied(monkeypatch, capsys):
    """When the harness threads ``w=<client>`` through, the inner
    helper sees a non-None w and (with the flag on) attempts the LLM
    call. We patch ``narrow_replacement_with_llm`` to a stub so the
    test doesn't need a real workspace client."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_NARROW_REPLACEMENT", "1")

    from genie_space_optimizer.optimization.harness import (
        _attempt_llm_narrow_replacement_and_emit_outcome,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )
    from genie_space_optimizer.optimization.stages import (
        narrow_replacement as _narrow_mod,
    )

    reset_patch_outcome_emitter()
    monkeypatch.setattr(
        _narrow_mod,
        "narrow_replacement_with_llm",
        lambda patch, **kwargs: None,  # LLM declined; still counts as attempt
    )

    diag = {
        "applicable": False,
        "reason": "narrow_skipped_no_original_patch_type",
        "original_patch_type": "",
    }
    sentinel_w = object()
    attempted = _attempt_llm_narrow_replacement_and_emit_outcome(
        drop=_drop_record_with_intent_id(),
        diag=diag,
        ag_target_qids=("q1",),
        ag_root_cause="missing_filter",
        run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        w=sentinel_w,  # threaded
    )
    assert attempted is True
    out = capsys.readouterr().out
    assert "GSO_PATCH_OUTCOME_V1" in out
    assert '"narrow_outcome":"exhausted"' in out


def test_w_none_bails_out_legacy_callers_unchanged(monkeypatch):
    """A legacy caller (or test) that passes ``w=None`` must NOT
    invoke the LLM path. The helper returns False with no marker
    emission — preserves byte-stable replay for any call site that
    hasn't been threaded."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_NARROW_REPLACEMENT", "1")

    from genie_space_optimizer.optimization.harness import (
        _attempt_llm_narrow_replacement_and_emit_outcome,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        reset_patch_outcome_emitter,
    )

    reset_patch_outcome_emitter()
    diag = {
        "applicable": False,
        "reason": "narrow_skipped_no_original_patch_type",
        "original_patch_type": "",
    }
    attempted = _attempt_llm_narrow_replacement_and_emit_outcome(
        drop=_drop_record_with_intent_id(),
        diag=diag,
        ag_target_qids=("q1",),
        ag_root_cause="missing_filter",
        run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        w=None,  # legacy caller
    )
    assert attempted is False


def test_run_narrow_l6_replacement_loop_accepts_and_forwards_w(monkeypatch):
    """The outer loop function must accept ``w`` and forward it to
    every inner ``_attempt_llm_narrow_replacement_and_emit_outcome``
    invocation. Spy on the inner helper to verify the threading."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_NARROW_REPLACEMENT", "1")

    from genie_space_optimizer.optimization import harness as _harness_mod

    captured: dict = {}

    def _spy(*, drop, diag, ag_target_qids, ag_root_cause, run_id,
             iteration, ag_id, cluster_id, w):
        captured["w"] = w
        captured["drop_reason"] = diag.get("reason")
        return False

    monkeypatch.setattr(
        _harness_mod,
        "_attempt_llm_narrow_replacement_and_emit_outcome",
        _spy,
    )

    # A drop record that goes through the inapplicable diag path so
    # the helper fires. The legacy emit_narrow_replacement_diagnostic
    # path also runs but doesn't affect this test's spy.
    drop = {
        "reason": "high_collateral_risk_flagged",
        "original_patch": {
            "intent_id": "intent_test",
            "patch_type": "",  # empty → narrow_skipped_no_original_patch_type
            "patch_body": {},
            "target_qids": ["q1"],
        },
        "patch_type": "",
        "proposal_id": "P001",
    }

    sentinel_w = object()
    _harness_mod._run_narrow_l6_replacement_loop(
        blast_dropped=[drop],
        blast_target_qids=("q1",),
        ag_root_cause="missing_filter",
        run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        iter_inputs={},
        qid_to_question_text={},
        qid_to_reference_sql={},
        w=sentinel_w,
    )
    assert captured.get("w") is sentinel_w, (
        f"_run_narrow_l6_replacement_loop must forward w to the inner "
        f"helper; got {captured.get('w')!r}"
    )


def test_both_harness_callsites_thread_w_to_narrow_loop():
    """Belt-and-suspenders source check: every harness callsite of
    ``_run_narrow_l6_replacement_loop`` MUST pass ``w=w`` so the LLM
    path can fire on production runs. Catches regressions where a
    future edit accidentally drops the kwarg.
    """
    import re
    from pathlib import Path

    harness_text = (
        Path(__file__).parent.parent.parent
        / "src" / "genie_space_optimizer" / "optimization"
        / "harness.py"
    ).read_text()

    # Scan for callsites that open with
    # ``_narrow_kept = _run_narrow_l6_replacement_loop(``, then
    # consume balanced parens to grab the full multi-line argument
    # list (regex can't do balanced-paren matching; we do it
    # manually).
    open_marker = "_narrow_kept = _run_narrow_l6_replacement_loop("
    callsites: list[str] = []
    pos = 0
    while True:
        start = harness_text.find(open_marker, pos)
        if start == -1:
            break
        depth = 1
        i = start + len(open_marker)
        while i < len(harness_text) and depth > 0:
            ch = harness_text[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            i += 1
        callsites.append(harness_text[start + len(open_marker): i - 1])
        pos = i

    assert len(callsites) >= 2, (
        f"expected >= 2 narrow-loop callsites in harness body; got "
        f"{len(callsites)}"
    )
    for idx, body in enumerate(callsites):
        assert "w=w" in body, (
            f"narrow-loop callsite #{idx} missing w=w kwarg; the "
            f"deferred wire-in was dropped. Body excerpt:\n"
            f"{body[:400]}..."
        )


def test_run_narrow_l6_replacement_loop_default_w_is_none(monkeypatch):
    """Pre-existing callers that don't pass w MUST continue to work —
    the default is None, and the inner helper bails out cleanly."""
    monkeypatch.setenv("GSO_PLAN12_LIVE_NARROW_REPLACEMENT", "0")

    from genie_space_optimizer.optimization import harness as _harness_mod

    captured: dict = {}

    def _spy(*, w, **_kwargs):
        captured["w"] = w
        return False

    monkeypatch.setattr(
        _harness_mod,
        "_attempt_llm_narrow_replacement_and_emit_outcome",
        _spy,
    )

    drop = {
        "reason": "high_collateral_risk_flagged",
        "original_patch": {
            "intent_id": "intent_test",
            "patch_type": "",
            "patch_body": {},
            "target_qids": ["q1"],
        },
        "patch_type": "",
        "proposal_id": "P001",
    }
    # Call WITHOUT w → default None.
    _harness_mod._run_narrow_l6_replacement_loop(
        blast_dropped=[drop],
        blast_target_qids=("q1",),
        ag_root_cause="missing_filter",
        run_id="run_x",
        iteration=1,
        ag_id="AG1",
        cluster_id="H001",
        iter_inputs={},
        qid_to_question_text={},
        qid_to_reference_sql={},
    )
    assert captured.get("w") is None
