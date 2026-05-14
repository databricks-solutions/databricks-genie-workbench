"""Plan 4 — raw evidence: extraction, diverse sampling, per-skill projection.

The Plan 3 ``ActivationBundle`` left ``raw_evidence`` as ``()`` always.
Plan 4 fills it with N diverse ``(question, actual_sql, expected_sql,
judge_rationale)`` triples per cluster, for every Stage-2 skill EXCEPT
``lever-5b-example-sql`` (which stays evidence-free behind the
output-side leakage firewall).

Three layers:

  1. ``extract_raw_evidence_from_cluster(cluster) -> list[dict]`` —
     reads the pre-AFS cluster's ``question_traces`` field and emits
     one normalized triple per question that has at least one
     ``failed_judges`` entry.
  2. ``select_diverse_examples(triples, n, w) -> list[dict]`` — picks
     the N most diverse triples by question-text embedding cosine
     distance when the embedding endpoint is reachable, falling back
     to n-gram-Jaccard distance when ``w`` is None or the endpoint
     fails. Greedy farthest-point selection.
  3. ``project_evidence_for_skill(skill_id, clusters, w, n) ->
     tuple[dict, ...]`` — entry point used by
     ``activation_bundle.build_activation_bundle``. Looks up the
     per-skill projector in ``_PROJECTOR_TABLE`` and applies it.

Per-skill projection is policy-driven: most skills are pass-through
(all four fields visible); ``lever-5b-example-sql`` is excluded
unconditionally; ``stage-1-discovery`` is excluded by virtue of not
being in the table at all.

The projector functions intentionally do NOT call the leakage
firewall — the firewall is OUTPUT-side. Per-skill projection here
controls what raw evidence reaches the LLM, not what the LLM is
allowed to emit.
"""
from __future__ import annotations

import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


# ── Layer 1: extraction ───────────────────────────────────────────────


def extract_raw_evidence_from_cluster(cluster: dict) -> list[dict]:
    """Read a pre-AFS cluster's ``question_traces`` and emit one
    normalized triple per question that has at least one
    ``failed_judges`` entry.

    Output shape: ``list[{"question_id", "trace_id", "question",
    "actual_sql", "expected_sql", "judge_rationale"}]``. Field
    rename: cluster's ``generated_sql`` → triple's ``actual_sql``
    (clearer name for prompt rendering). Joined ``judge_rationale``
    is built from every failed-judge ``rationale`` (preferred) or
    ``rationale_snippet`` (fallback), separated by ``" | "``.

    Defensive against malformed cluster shapes — non-dict entries in
    ``question_traces`` are silently skipped.
    """
    if not isinstance(cluster, dict):
        return []
    out: list[dict] = []
    for qt in cluster.get("question_traces") or []:
        if not isinstance(qt, dict):
            continue
        failed = qt.get("failed_judges") or []
        if not failed:
            continue
        rationales: list[str] = []
        for fj in failed:
            if not isinstance(fj, dict):
                continue
            text = (fj.get("rationale") or fj.get("rationale_snippet") or "").strip()
            if text:
                rationales.append(text[:300])
        out.append({
            "question_id": str(qt.get("question_id") or ""),
            "trace_id": str(qt.get("trace_id") or ""),
            "question": str(qt.get("question") or ""),
            "actual_sql": str(qt.get("generated_sql") or qt.get("actual_sql") or ""),
            "expected_sql": str(qt.get("expected_sql") or ""),
            "judge_rationale": " | ".join(rationales),
        })
    return out


# ── Layer 2: diverse sampling ────────────────────────────────────────


def _jaccard_distance(a: str, b: str) -> float:
    """1 - Jaccard similarity over whitespace token sets. Range [0,1].

    Returns 1.0 when either string is empty (treated as maximally
    different so empty inputs don't anchor selection).
    """
    ta = set((a or "").lower().split())
    tb = set((b or "").lower().split())
    if not ta or not tb:
        return 1.0
    inter = len(ta & tb)
    union = len(ta | tb)
    return 1.0 - (inter / union if union else 0.0)


def _select_farthest_point_jaccard(triples: list[dict], n: int) -> list[dict]:
    """Greedy farthest-point selection by question-text Jaccard distance.

    Deterministic: starts from index 0, then repeatedly picks the
    triple with the largest minimum distance to the already-selected
    set. Same input always produces the same output ordering.
    """
    if n <= 0 or not triples:
        return []
    if n >= len(triples):
        return list(triples)
    selected: list[dict] = [triples[0]]
    remaining: list[dict] = list(triples[1:])
    while len(selected) < n and remaining:
        best_i = 0
        best_d = -1.0
        for i, cand in enumerate(remaining):
            min_d = min(
                _jaccard_distance(cand.get("question", ""),
                                   sel.get("question", ""))
                for sel in selected
            )
            if min_d > best_d:
                best_d = min_d
                best_i = i
        selected.append(remaining.pop(best_i))
    return selected


def _select_farthest_point_embedding(
    triples: list[dict], n: int, w: Any,
) -> list[dict] | None:
    """Same greedy farthest-point but distances are computed over
    question embeddings via ``optimization.leakage.get_embedding``.

    Returns ``None`` when ANY question's embedding fails to compute —
    caller falls back to the Jaccard path.
    """
    from genie_space_optimizer.optimization.leakage import (
        _cosine_similarity, get_embedding,
    )
    embeddings: list[list[float]] = []
    for t in triples:
        emb = get_embedding(t.get("question", ""), w)
        if emb is None:
            return None
        embeddings.append(emb)
    if n <= 0 or not triples:
        return []
    if n >= len(triples):
        return list(triples)
    selected_idx: list[int] = [0]
    remaining_idx: list[int] = list(range(1, len(triples)))
    while len(selected_idx) < n and remaining_idx:
        best_i = 0
        best_d = -1.0
        for i, ci in enumerate(remaining_idx):
            min_d = min(
                1.0 - _cosine_similarity(embeddings[ci], embeddings[si])
                for si in selected_idx
            )
            if min_d > best_d:
                best_d = min_d
                best_i = i
        selected_idx.append(remaining_idx.pop(best_i))
    return [triples[i] for i in selected_idx]


def select_diverse_examples(
    triples: list[dict], n: int, w: Any = None,
) -> list[dict]:
    """Return up to ``n`` triples chosen for maximum pairwise diversity.

    Strategy:
      1. If ``w`` is provided, try embedding-based farthest-point
         selection. Returns its result on success.
      2. Otherwise (or on embedding failure), use n-gram-Jaccard
         farthest-point. Always deterministic.

    Edge cases: ``n <= 0`` → ``[]``; empty input → ``[]``; ``n >=
    len(triples)`` → input returned unchanged (full passthrough).
    """
    if n <= 0 or not triples:
        return []
    if n >= len(triples):
        return list(triples)
    if w is not None:
        emb_result = _select_farthest_point_embedding(triples, n, w)
        if emb_result is not None:
            return emb_result
    return _select_farthest_point_jaccard(triples, n)


# ── Layer 3: per-skill projection ────────────────────────────────────


def _project_pass_through(triples: tuple[dict, ...]) -> tuple[dict, ...]:
    """All four fields visible. Used by L1, L2, L3, L4, L5a, L6.

    Defensive copy: returns new dicts so downstream consumers can
    mutate without affecting the original.
    """
    return tuple({
        "question_id": t.get("question_id", ""),
        "trace_id": t.get("trace_id", ""),
        "question": t.get("question", ""),
        "actual_sql": t.get("actual_sql", ""),
        "expected_sql": t.get("expected_sql", ""),
        "judge_rationale": t.get("judge_rationale", ""),
    } for t in triples)


# Plan-3 skills that get raw-evidence projections. Skills NOT in this
# table OR in _EXCLUDED_SKILLS get empty projections (defensive
# default).
_PROJECTOR_TABLE: dict[str, Callable[[tuple[dict, ...]], tuple[dict, ...]]] = {
    "lever-1-table-column-description": _project_pass_through,
    "lever-2-mv-column-refinement": _project_pass_through,
    "lever-3-tvf-routing": _project_pass_through,
    "lever-4-join-discovery": _project_pass_through,
    "lever-5a-instructions": _project_pass_through,
    "lever-6-sql-expression": _project_pass_through,
}


# Plan-3 skills explicitly excluded from raw evidence. The dispatcher
# uses this set to distinguish "deliberately excluded" from "unknown
# skill" (both return empty, but the former is on-path and the latter
# is logged as a configuration error).
_EXCLUDED_SKILLS: frozenset[str] = frozenset({
    "lever-5b-example-sql",
})


def project_evidence_for_skill(
    skill_id: str,
    clusters: list[dict],
    w: Any = None,
    n: int = 3,
) -> tuple[dict, ...]:
    """Plan 4 — entry point used by ``activation_bundle.build_activation_bundle``.

    Pipeline:
      1. ``extract_raw_evidence_from_cluster`` for each cluster, then
         flatten.
      2. ``select_diverse_examples`` to pick the top N.
      3. Per-skill projector lookup in ``_PROJECTOR_TABLE``.

    Excluded skills (``_EXCLUDED_SKILLS``, currently
    ``{"lever-5b-example-sql"}``) return ``()`` unconditionally.
    Unknown skills (not in either table) return ``()`` and are logged
    as a configuration error — caller continues so a missing entry
    here cannot break the pipeline.
    """
    if skill_id in _EXCLUDED_SKILLS:
        return ()
    projector = _PROJECTOR_TABLE.get(skill_id)
    if projector is None:
        logger.info(
            "raw_evidence: no projector registered for skill_id=%s — "
            "returning empty projection. Add to _PROJECTOR_TABLE or "
            "_EXCLUDED_SKILLS to silence this log.", skill_id,
        )
        return ()
    if n <= 0:
        return ()

    all_triples: list[dict] = []
    for cluster in clusters or []:
        all_triples.extend(extract_raw_evidence_from_cluster(cluster))
    if not all_triples:
        return ()

    diverse = select_diverse_examples(all_triples, n=n, w=w)
    return projector(tuple(diverse))
