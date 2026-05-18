"""Phase 3.5 Task 7 — CI gate: _KNOWN_STAGES covers every span_name in src/.

This test re-derives the production vocabulary by greppping for
``span_name="..."`` literals across ``src/genie_space_optimizer/`` and
refuses if any literal is missing from ``tape._KNOWN_STAGES``. New
LLM call sites must register their span name in the closed vocab in
the same PR that introduces them.
"""
from __future__ import annotations

import re
from pathlib import Path

from genie_space_optimizer.optimization.tape import _KNOWN_STAGES

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "src" / "genie_space_optimizer"
# Match real span_name literals only — identifiers plus dots (we
# allow ``archetype_learning.synthesize_provisional``). Bracket /
# regex characters that appear inside ``tape.py``'s own grep-pattern
# comment are excluded.
PATTERN = re.compile(r'span_name\s*=\s*"([A-Za-z0-9_.]+)"')


def test_known_stages_covers_every_source_span_name():
    found: set[str] = set()
    for py in SRC.rglob("*.py"):
        text = py.read_text(encoding="utf-8", errors="ignore")
        for m in PATTERN.finditer(text):
            found.add(m.group(1))

    missing = found - _KNOWN_STAGES
    assert not missing, (
        f"_KNOWN_STAGES is missing {len(missing)} span_name "
        f"literal(s) present in src/: {sorted(missing)}. Add them to "
        f"tape._KNOWN_STAGES with a one-line comment describing the "
        f"call site."
    )

    # The reverse direction (KNOWN minus FOUND) is informational, not
    # fatal — a stage may have been deleted upstream but still appear
    # in old tapes loaded by replay tests.
    stale = _KNOWN_STAGES - found
    if stale:
        print(
            f"INFO: _KNOWN_STAGES contains {len(stale)} stage(s) not "
            f"currently in src/ (probably retired or used in tests): "
            f"{sorted(stale)}"
        )
