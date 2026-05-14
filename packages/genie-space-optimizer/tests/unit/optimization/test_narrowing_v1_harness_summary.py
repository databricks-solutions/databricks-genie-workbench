"""Verifies that the narrowing summary call in _run_lever_loop is
defensive and does not raise even if the capture sink is uninitialized."""
from __future__ import annotations

from genie_space_optimizer.common import config as cfg


def test_dump_narrowing_capture_summary_is_safe_when_no_hits():
    """Default state: no flag set, no hits — must return a well-formed
    dict, not raise."""
    cfg._NARROWING_CAPTURE_SINK.reset_for_test()  # noqa: SLF001
    snap = cfg.dump_narrowing_capture_summary()
    assert isinstance(snap, dict)
    assert "hits" in snap
    assert all(v == 0 for v in snap["hits"].values())
    assert snap["all_sites_exercised"] is False
    # The set of skill_ids must equal the registry.
    assert set(snap["hits"].keys()) == set(cfg._NON_CAUSAL_PROMPT_NAMES)  # noqa: SLF001
