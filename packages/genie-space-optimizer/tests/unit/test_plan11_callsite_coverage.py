"""Plan 11 — verify every legacy synthesis import in optimizer.py is
paired with a ``plan11_llm_first_enabled()`` branch.

Prevents silent mixed-mode rollouts where a callsite is added without
a flag branch (the legacy synthesizer would run in production even when
``GSO_PLAN11_LLM_FIRST=true``).
"""
from __future__ import annotations

import re
from pathlib import Path

OPTIMIZER_PATH = (
    Path(__file__).parent.parent.parent
    / "src/genie_space_optimizer/optimization/optimizer.py"
)

LEGACY_IMPORT_PATTERN = re.compile(
    r"from genie_space_optimizer\.optimization\.(cluster_driven_synthesis|"
    r"forced_synthesis_dispatch|three_stage_pipeline)"
)

PLAN11_FLAG_CALL = "plan11_llm_first_enabled()"


def test_every_legacy_synthesis_import_has_flag_branch():
    """For every line that imports a legacy synthesis module, the
    surrounding ±30-line window must also contain a
    ``plan11_llm_first_enabled()`` call.
    """
    source = OPTIMIZER_PATH.read_text()
    lines = source.splitlines()

    legacy_import_lines = [
        i for i, line in enumerate(lines) if LEGACY_IMPORT_PATTERN.search(line)
    ]
    assert legacy_import_lines, "Expected at least one legacy synthesis import"

    violations: list[tuple[int, str]] = []
    for line_num in legacy_import_lines:
        context_start = max(0, line_num - 30)
        context_end = min(len(lines), line_num + 30)
        context = "\n".join(lines[context_start:context_end])
        if PLAN11_FLAG_CALL not in context:
            violations.append((line_num + 1, lines[line_num].strip()))

    assert not violations, (
        "These legacy synthesis imports in optimizer.py lack a "
        "plan11_llm_first_enabled() branch within 30 lines:\n"
        + "\n".join(f"  line {ln}: {txt}" for ln, txt in violations)
    )
