"""Cross-cluster intent-collision detection and conditional disambiguation patches."""

from __future__ import annotations

from typing import Any


def _normalise_term(term: str) -> str:
    return str(term or "").strip().lower()


def detect_intent_collisions(clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Find business terms that resolve to different columns across clusters.

    A collision exists when the same term appears in ``asi_intent_terms`` of
    two or more clusters AND those clusters disagree on the canonical column
    (as reflected in ``asi_blame_set``).

    Returns one entry per colliding term:
      {
        "term": "region",
        "column_choices": {"region_name", "region_combination"},
        "clusters_by_column": {"region_name": ["H001"], "region_combination": ["H002"]},
        "questions_by_column": {"region_name": ["gs_002"], "region_combination": ["gs_008"]},
      }
    """
    by_term_column: dict[str, dict[str, dict[str, list[str]]]] = {}
    for cluster in clusters or []:
        cid = str(cluster.get("cluster_id") or "").strip()
        if not cid:
            continue
        terms = [_normalise_term(t) for t in cluster.get("asi_intent_terms", []) or [] if str(t)]
        columns = [str(c).strip() for c in cluster.get("asi_blame_set", []) or [] if str(c)]
        qids = [str(q) for q in cluster.get("question_ids", []) or [] if str(q)]
        for term in terms:
            if not term:
                continue
            term_entry = by_term_column.setdefault(term, {})
            for column in columns:
                col_entry = term_entry.setdefault(
                    column, {"clusters": [], "questions": []},
                )
                if cid not in col_entry["clusters"]:
                    col_entry["clusters"].append(cid)
                for q in qids:
                    if q not in col_entry["questions"]:
                        col_entry["questions"].append(q)

    collisions: list[dict[str, Any]] = []
    for term, columns in by_term_column.items():
        if len(columns) < 2:
            continue
        collisions.append({
            "term": term,
            "column_choices": set(columns.keys()),
            "clusters_by_column": {col: list(d["clusters"]) for col, d in columns.items()},
            "questions_by_column": {col: list(d["questions"]) for col, d in columns.items()},
        })
    return collisions
