"""Phase C Task 1 — every canonical-qid call site uses the same
extractor as ``_qid_extraction.extract_question_id``.

Cycle 8 Bug 2 closed the divergence between
``harness._baseline_row_qid`` and
``ground_truth_corrections._extract_question_id``. This test pins the
remaining two call sites (``labeling._extract_question_id`` and
``regression_mining._extract_question_id``) to the same canonical
helper so the four-way divergence cannot recur.

Plan: ``docs/2026-05-03-phase-c-rca-loop-contract-and-residuals-plan.md`` Task 1.
"""
from __future__ import annotations

import json


_FIXTURE_ROWS: list[dict] = [
    # Top-level canonical key
    {"question_id": "q_top_1"},
    # Inputs-namespaced flat key
    {"inputs.question_id": "q_inputs_flat_2"},
    # Inputs-namespaced nested dict
    {"inputs": {"question_id": "q_inputs_dict_3"}},
    # Request envelope (dict shape)
    {"request": {"kwargs": {"question_id": "q_request_4"}}},
    # Request envelope (JSON-encoded string shape)
    {"request": json.dumps({"kwargs": {"question_id": "q_request_str_5"}})},
    # Metadata-only shape
    {"metadata": {"question_id": "q_metadata_6"}},
    # Trace-id-only fallback (canonical helper returns trace_fallback)
    {"client_request_id": "tr-7"},
    # Empty
    {"foo": "bar"},
]


def test_labeling_extractor_matches_canonical_helper() -> None:
    from genie_space_optimizer.optimization import labeling

    actual: list[str] = []
    for row in _FIXTURE_ROWS:
        # ``labeling._extract_question_id`` is called against a row's
        # ``request`` field, not the whole row. Mirror that call shape.
        actual.append(labeling._extract_question_id(row.get("request")))

    expected: list[str] = []
    for row in _FIXTURE_ROWS:
        # ``request`` may be a dict, a JSON string, or absent. The
        # canonical helper handles all three when given the wrapper
        # ``{"request": request_val}`` shape.
        request_val = row.get("request")
        if request_val is None:
            expected.append("")
            continue
        from genie_space_optimizer.optimization._qid_extraction import (
            extract_question_id,
        )
        qid, _src = extract_question_id({"request": request_val})
        expected.append(qid)

    assert actual == expected, (
        f"labeling._extract_question_id diverged from canonical helper: "
        f"actual={actual} expected={expected}"
    )


def test_regression_mining_extractor_matches_canonical_helper() -> None:
    from genie_space_optimizer.optimization import _qid_extraction, regression_mining

    actual: list[str] = [regression_mining._extract_question_id(r) for r in _FIXTURE_ROWS]
    expected: list[str] = [
        _qid_extraction.extract_question_id(r)[0] for r in _FIXTURE_ROWS
    ]
    assert actual == expected, (
        f"regression_mining._extract_question_id diverged from canonical "
        f"helper: actual={actual} expected={expected}"
    )


def test_all_four_call_sites_agree() -> None:
    """The four canonical-qid call sites (harness baseline, ground-truth
    corrections, labeling, regression mining) all return the same qid
    for the same row, modulo each site's call shape. Failures here mean
    a future cycle will silently route some rows to a different qid
    than others — exactly the bug class Cycle 8 closed for two sites.
    """
    from genie_space_optimizer.optimization import (
        ground_truth_corrections,
        labeling,
        regression_mining,
    )
    from genie_space_optimizer.optimization._qid_extraction import (
        extract_question_id,
    )
    from genie_space_optimizer.optimization.harness import _baseline_row_qid

    for row in _FIXTURE_ROWS:
        canonical, _ = extract_question_id(row)
        assert _baseline_row_qid(row) == canonical
        assert ground_truth_corrections._extract_question_id(row) == canonical
        assert regression_mining._extract_question_id(row) == canonical
        # labeling takes ``request_val``, not the whole row.
        rv = row.get("request")
        wrapped, _ = extract_question_id({"request": rv} if rv is not None else {})
        assert labeling._extract_question_id(rv) == wrapped
