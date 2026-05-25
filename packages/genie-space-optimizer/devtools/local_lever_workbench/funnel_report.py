"""Stdout marker counting for the local workbench funnel report.

Counts selected ``GSO_GATE_REASONING_V1`` gate/verdict combinations so
operators see at-a-glance how evaluated_gate and acceptance_gate fired
without grepping full transcripts.
"""
from __future__ import annotations

_GATE_VERDICT_PATTERNS: tuple[str, ...] = (
    "GSO_GATE_REASONING_V1 gate=evaluated_gate verdict=accepted",
    "GSO_GATE_REASONING_V1 gate=evaluated_gate verdict=rejected",
    "GSO_GATE_REASONING_V1 gate=acceptance_gate verdict=accepted",
    "GSO_GATE_REASONING_V1 gate=acceptance_gate verdict=rejected",
)

_MARKER_PATTERNS: tuple[str, ...] = _GATE_VERDICT_PATTERNS


def _line_matches_pattern(line: str, pattern: str) -> bool:
    """True when every space-delimited token in *pattern* appears in *line*."""
    return all(token in line for token in pattern.split())


def _count_markers(stdout: str) -> dict[str, int]:
    """Count registered marker patterns in workbench stdout.

    Each pattern is a space-separated token sequence (e.g.
    ``GSO_GATE_REASONING_V1 gate=evaluated_gate verdict=accepted``).
    A line matches when it contains every token — ``qid``, ``reason``, and
    ``predicate_inputs`` may appear between them, matching production emission
    shape.
    """
    counts = {pattern: 0 for pattern in _MARKER_PATTERNS}
    for line in (stdout or "").splitlines():
        for pattern in _MARKER_PATTERNS:
            if _line_matches_pattern(line, pattern):
                counts[pattern] += 1
    return counts


__all__ = ["_count_markers"]
