"""Subset-first gate question-id selection (GSO Optimizer v2, Phase 1 §3.4).

The lever loop's subset-first gate sequencing (slice → P0 → full) is orchestrated
inline in ``harness._run_gate_checks`` against the official Benchmark Eval-Run API.
This module owns only the *subset selection* helpers those gates use to pick the
slice and P0 question-id subsets from the failing-cluster / priority id lists.
"""

from __future__ import annotations

import logging
from typing import Sequence

from genie_space_optimizer.common import config as _config

logger = logging.getLogger(__name__)


def _ordered_unique(ids: Sequence[str]) -> list[str]:
    """De-duplicate while preserving first-seen order (deterministic subsets)."""
    seen: set[str] = set()
    out: list[str] = []
    for i in ids:
        s = str(i)
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def select_slice_qids(
    failing_cluster_qids: Sequence[str],
    all_qids: Sequence[str] | None = None,
    *,
    max_questions: int | None = None,
) -> list[str]:
    """Pick the slice gate's question ids — the failing-cluster questions, capped.

    Falls back to (a prefix of) ``all_qids`` only when no failing-cluster ids are
    supplied, so the slice gate is never empty when there is anything to evaluate.
    """
    cap = int(max_questions if max_questions is not None else _config.SLICE_GATE_MAX_QUESTIONS)
    picked = _ordered_unique(failing_cluster_qids)[:cap]
    if not picked and all_qids:
        picked = _ordered_unique(all_qids)[:cap]
    return picked


def select_p0_qids(
    priority_qids: Sequence[str],
    all_qids: Sequence[str] | None = None,
    *,
    max_questions: int | None = None,
) -> list[str]:
    """Pick the P0 gate's question ids — the priority questions, capped."""
    cap = int(max_questions if max_questions is not None else _config.P0_GATE_MAX_QUESTIONS)
    picked = _ordered_unique(priority_qids)[:cap]
    if not picked and all_qids:
        picked = _ordered_unique(all_qids)[:cap]
    return picked


