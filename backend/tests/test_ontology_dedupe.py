"""Deterministic dedupe (spec §11): {finance, Finance, finances} collapses to one
collision group with a reuse suggestion; a zero-assignment tag flags orphan.
No embeddings (MV-D40 is a later phase)."""

from __future__ import annotations

from backend.ontology.services import dedupe


def _graph(tags):
    return {"tags": tags, "as_of": "2026-08-29T00:00:00Z"}


def test_case_and_plural_collapse_to_one_collision():
    graph = _graph([
        {"tag_key": "finance", "allowed_values": [], "assignment_count": 12, "members": []},
        {"tag_key": "Finance", "allowed_values": [], "assignment_count": 3, "members": []},
        {"tag_key": "finances", "allowed_values": [], "assignment_count": 1, "members": []},
    ])
    collisions = dedupe.find_collisions(graph)
    assert len(collisions) == 1
    c = collisions[0]
    assert set(c.members) == {"finance", "Finance", "finances"}
    # Canonical is the most-assigned key.
    assert "reuse `finance`" in c.suggestion
    assert c.kind in {"fuzzy_case", "fuzzy_plural", "fuzzy_token"}


def test_distinct_tags_do_not_collide():
    graph = _graph([
        {"tag_key": "finance", "allowed_values": [], "assignment_count": 4, "members": []},
        {"tag_key": "marketing", "allowed_values": [], "assignment_count": 2, "members": []},
    ])
    assert dedupe.find_collisions(graph) == []


def test_zero_assignment_tag_flags_orphan():
    graph = _graph([
        {"tag_key": "Ops_legacy_unused", "allowed_values": [], "assignment_count": 0, "members": []},
    ])
    cleanup = dedupe.find_cleanup(graph)
    flags = {c.tag_key: c.flag for c in cleanup}
    assert flags["Ops_legacy_unused"] == "orphan"


def test_near_empty_and_deprecated_flags():
    graph = _graph([
        {"tag_key": "Finance/Audit", "allowed_values": [], "assignment_count": 1, "members": []},
        {"tag_key": "Ops_legacy", "allowed_values": [], "assignment_count": 3, "members": []},
    ])
    flags = {c.tag_key: c.flag for c in dedupe.find_cleanup(graph)}
    assert flags["Finance/Audit"] == "near_empty"
    # 'Ops_legacy' matches the deprecation naming heuristic and is still assigned.
    assert flags["Ops_legacy"] == "deprecated_but_assigned"
