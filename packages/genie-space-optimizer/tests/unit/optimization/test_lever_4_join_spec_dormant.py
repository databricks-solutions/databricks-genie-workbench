"""Pin the decision on the dormant lever-4-join-spec prompt.

Plan: docs/prompt_improvements/2026-05-17-lever-4-join-discovery-hardening.md
Decision (Task 15): mark as DEPRECATED, do not delete in this PR. If
the decision is revisited (delete vs keep-deprecated), update this
test in lockstep.
"""
from __future__ import annotations

import pathlib
import re

from genie_space_optimizer.common.config import (
    LEVER_PROMPTS,
    _DEPRECATED_PROMPT_NAMES,
)


def test_lever_4_join_spec_is_in_deprecated_set():
    assert "lever_4_join_spec" in _DEPRECATED_PROMPT_NAMES


def test_lever_4_join_spec_still_in_registry_but_unused():
    """Deprecated prompts remain in LEVER_PROMPTS for trace-history
    lookups but should NOT have any live _traced_llm_call sites.
    """
    assert "lever_4_join_spec" in LEVER_PROMPTS

    repo_root = pathlib.Path(__file__).resolve().parents[3]
    src_root = repo_root / "src" / "genie_space_optimizer"
    hits: list[str] = []
    for py in src_root.rglob("*.py"):
        text = py.read_text(encoding="utf-8", errors="ignore")
        for m in re.finditer(
            r"_traced_llm_call\([^)]*span_name=['\"]lever_4_join_spec",
            text,
        ):
            hits.append(f"{py}:{m.start()}")
    assert not hits, (
        "Found live _traced_llm_call for the deprecated "
        "lever_4_join_spec span:\n" + "\n".join(hits)
    )
