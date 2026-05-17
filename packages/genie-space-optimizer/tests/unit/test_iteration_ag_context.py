"""Phase 1 — AG-context capture helper unit tests."""

from __future__ import annotations

from genie_space_optimizer.optimization.iteration_ag_context import (
    capture_iter_ag_context,
)


def test_full_ag_populates_all_five_fields():
    ag = {
        "id": "AG1",
        "source_cluster_ids": ["c_001", "c_002"],
        "target_qids": ["gs_013"],
        "affected_questions": ["gs_013", "gs_014"],
        "lever_directives": {"6": {}, "1": {}},
        "root_cause_summary": "Missing join in customers→orders path",
    }

    ctx = capture_iter_ag_context(ag=ag, ag_id="AG1")

    assert ctx == {
        "ag_id": "AG1",
        "cluster_ids": ("c_001", "c_002"),
        "target_qids": ("gs_013",),
        "levers": (1, 6),  # sorted ascending
        "root_cause": "Missing join in customers→orders path",
        # Phase 2 (2026-05-16) — blame_set defaults to () when neither
        # ``blame_set`` nor ``blamed_assets`` is present on the AG.
        "blame_set": (),
    }


def test_sparse_ag_falls_back_to_affected_questions():
    ag = {
        "id": "AG2",
        "source_cluster_ids": ["c_005"],
        "affected_questions": ["gs_020", "gs_021"],
        "lever_directives": {"6": {}},
    }

    ctx = capture_iter_ag_context(ag=ag, ag_id="AG2")

    assert ctx["ag_id"] == "AG2"
    assert ctx["cluster_ids"] == ("c_005",)
    assert ctx["target_qids"] == ("gs_020", "gs_021")
    assert ctx["levers"] == (6,)
    assert ctx["root_cause"] == ""


def test_empty_ag_yields_defaults():
    ctx = capture_iter_ag_context(ag={}, ag_id="")

    assert ctx == {
        "ag_id": "",
        "cluster_ids": (),
        "target_qids": (),
        "levers": (),
        "root_cause": "",
        # Phase 2 (2026-05-16) — blame_set always present in helper output.
        "blame_set": (),
    }


def test_ag_id_arg_wins_over_dict_id():
    ag = {"id": "DICT_AG_ID", "source_cluster_ids": ["c_001"]}

    ctx = capture_iter_ag_context(ag=ag, ag_id="HARNESS_AG_ID")

    assert ctx["ag_id"] == "HARNESS_AG_ID"


def test_levers_are_sorted_integers():
    ag = {"lever_directives": {"6": {}, "1": {}, "3": {}}}

    ctx = capture_iter_ag_context(ag=ag, ag_id="X")

    assert ctx["levers"] == (1, 3, 6)


def test_non_string_cluster_ids_are_coerced():
    ag = {"source_cluster_ids": [1, "c_002", None, ""]}

    ctx = capture_iter_ag_context(ag=ag, ag_id="X")

    assert ctx["cluster_ids"] == ("1", "c_002")
