"""The gap report's line counts must match live source (MV-D9, mechanised).

MV-D9 makes a stale gap report a hard stop, and four of nine hand-typed counts
drifted inside one prompt. The rule is unchanged — this only moves the arithmetic
off the human, who was never adding value on it. Byte-matching fenced quotes and
re-reading anchors stays manual, because a quote can go stale by *position* with
its content unchanged, which no count check can see.

Failure here is a two-second fix: ``python scripts/gap_report_counts.py --write``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
SCRIPT = REPO_ROOT / "scripts" / "gap_report_counts.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("gap_report_counts", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def script():
    if not SCRIPT.is_file():
        pytest.skip(f"{SCRIPT} is absent — nothing to enforce")
    return _load_script()


def test_the_generated_layout_block_matches_live_line_counts(script):
    report = script.GAP_REPORT.read_text(encoding="utf-8")
    _, body, _ = script._split_report(report)

    assert body.strip("\n") == script.render_block(), (
        "the gap report's package-layout block is stale; run "
        "`python scripts/gap_report_counts.py --write`"
    )


def test_no_line_count_claim_outside_the_block_is_stale(script):
    report = script.GAP_REPORT.read_text(encoding="utf-8")
    head, _, tail = script._split_report(report)

    findings = script.audit_other_claims(f"{head}{tail}")

    assert findings == [], "stale or ambiguous (N L) claims in the gap report:\n" + "\n".join(
        findings
    )
