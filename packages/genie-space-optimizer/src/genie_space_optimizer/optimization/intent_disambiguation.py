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


def _intent_phrase_for_questions(questions: list[str]) -> str:
    """Render a short phrase characterising the intent of a question group."""
    if not questions:
        return ""
    sample = " | ".join(q.lower() for q in questions[:3])
    keywords = [
        ("flow", "sales flow / hierarchy queries"),
        ("hierarchy", "sales flow / hierarchy queries"),
        ("trend", "trend / time-series queries"),
        ("ticket", "per-transaction questions"),
        ("avg", "average / mean questions"),
        ("by region", "simple slice-by-region queries"),
        ("by market", "simple slice-by-market queries"),
        ("top", "top-N ranking questions"),
    ]
    for keyword, phrase in keywords:
        if keyword in sample:
            return phrase
    return f"queries like: {questions[0]}"


def build_conditional_disambiguation_patch(
    *,
    collision: dict,
    representatives: dict[str, str],
    proposal_id: str,
) -> dict:
    """Render a deterministic conditional-disambiguation patch.

    The patch's ``proposed_value`` body is a numbered rule that names the
    business term, the per-intent column mapping, and the question phrasing
    that triggers each branch. Designed to be applied via Lever 5.
    """
    term = str(collision["term"])
    mappings: dict[str, str] = {}
    target_qids: list[str] = []
    body_lines = [f"COLUMN DISAMBIGUATION — '{term}':"]
    for column, qids in sorted(collision["questions_by_column"].items()):
        intent_questions = [representatives.get(q, "") for q in qids if representatives.get(q)]
        intent_phrase = _intent_phrase_for_questions(intent_questions)
        mappings[column] = intent_phrase
        target_qids.extend(qids)
        body_lines.append(
            f"- For {intent_phrase}, '{term}' refers to column '{column}'."
        )
    body_lines.append(
        "When the question's phrasing matches one of the above intents, use the "
        "corresponding column. When ambiguous, prefer the most specific match."
    )
    return {
        "proposal_id": proposal_id,
        "type": "add_conditional_disambiguation_instruction",
        "patch_type": "add_conditional_disambiguation_instruction",
        "lever": 5,
        "scope": "genie_config",
        "term": term,
        "mappings": mappings,
        "target_qids": sorted(set(target_qids)),
        "proposed_value": "\n".join(body_lines),
        "change_description": (
            f"Conditional disambiguation rule for term '{term}' "
            f"({len(mappings)} intent branches)"
        ),
    }
