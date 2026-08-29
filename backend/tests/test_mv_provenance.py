"""Serve-time provenance-id resolution (Prompt 15.9 item d).

The advisor stamps each evidence occurrence with a prefixed id — ``sql_snippet:``
/ ``trusted_asset:`` / ``gso_patch:`` — or, for a benchmark answer, a bare id.
Rendering those raw at the user was the defect item (d) closes. These pin the
backend resolver that turns each id into a human label from the CURRENT space
config (so existing proposals gain labels with no re-scan), and that a bare id
is left for the card's "curated query" count (never a bare id in the label list).

Tested at the seam (pure functions, no Databricks): the id → label mapping and
the in-place attach that resolves a whole proposal list from one config read.
"""

from __future__ import annotations

from backend.models import MvProposal, MvProposalMeasure
from backend.routers import auto_optimize


def _config_with(snippet_id: str, example_id: str) -> dict:
    """A minimal space config carrying one measure snippet and one example."""
    return {
        "instructions": {
            "sql_snippets": {
                "measures": [
                    {
                        "id": snippet_id,
                        "display_name": "Booking value",
                        "sql": ["SUM(booking_value)"],
                    }
                ]
            },
            "example_question_sqls": [
                {
                    "id": example_id,
                    "question": ["What is total revenue by region?"],
                    "sql": ["SELECT region, SUM(revenue) FROM t GROUP BY region"],
                }
            ],
        }
    }


def test_resolve_sql_snippet_id_uses_display_name_and_expression():
    """``sql_snippet:<collection>:<id>`` → the snippet's display name, with its
    expression carried as the detail (the mono line under the label)."""
    index = auto_optimize._mv_provenance_index(_config_with("snip1", "ex1"))
    label = auto_optimize._resolve_provenance_label("sql_snippet:measures:snip1", index)
    assert label is not None
    assert label.kind == "sql_snippet"
    assert label.label == "Booking value"
    assert label.detail == "SUM(booking_value)"


def test_resolve_trusted_asset_id_uses_example_question_text():
    """``trusted_asset:<id>`` → the example question's text (no detail line)."""
    index = auto_optimize._mv_provenance_index(_config_with("snip1", "ex1"))
    label = auto_optimize._resolve_provenance_label("trusted_asset:ex1", index)
    assert label is not None
    assert label.kind == "trusted_asset"
    assert label.label == "What is total revenue by region?"
    assert label.detail is None


def test_resolve_gso_patch_id_returns_none_no_config_text():
    """A ``gso_patch`` id has no config-derived text — a generic "generated-SQL
    match" repeated per occurrence is the noise the deployed review flagged, so it
    resolves to ``None`` and the card's count chip carries it instead of a label."""
    index = auto_optimize._mv_provenance_index({})
    assert auto_optimize._resolve_provenance_label("gso_patch:3:instructions:0", index) is None


def test_resolve_unknown_prefixed_id_returns_none():
    """A prefixed id that resolves to nothing yields ``None`` — never a generic
    "curated snippet" / "trusted asset" row. A label is emitted ONLY when it says
    something specific; the count chip conveys the rest."""
    index = auto_optimize._mv_provenance_index({})
    assert auto_optimize._resolve_provenance_label("sql_snippet:measures:missing", index) is None
    assert auto_optimize._resolve_provenance_label("trusted_asset:missing", index) is None


def test_resolve_bare_benchmark_id_returns_none():
    """A bare id is a benchmark/curated-query answer — counted as a curated query,
    it needs no per-id label (returning None keeps it out of the label list)."""
    index = auto_optimize._mv_provenance_index({})
    assert auto_optimize._resolve_provenance_label("q_bare_1234", index) is None


def test_provenance_index_unwraps_parsed_space_envelope():
    """``fetch_space_config`` returns the export envelope with the real config under
    ``_parsed_space`` — the index must read through it (reading ``config.instructions``
    off the envelope was the bug that made every id resolve to a generic label)."""
    envelope = {"_parsed_space": _config_with("snip1", "ex1")}
    index = auto_optimize._mv_provenance_index(envelope)
    label = auto_optimize._resolve_provenance_label("trusted_asset:ex1", index)
    assert label is not None
    assert label.label == "What is total revenue by region?"


def test_attach_labels_resolves_member_ids_in_order_and_dedupes():
    """The in-place attach gathers the members' ids (deduped, ordered) and sets
    ``provenance_labels`` — one config read for the whole list. A bare id
    contributes no label; the two resolvable ids do."""
    proposal = MvProposal(
        suggestion_id="s1",
        dedup_fingerprint="fp1",
        target_space_id="space-1",
        run_id=None,
        candidate_type="NEW_METRIC_VIEW",
        confidence_score=None,
        tier=None,
        uncapped_tier=None,
        tier_capped_by_coverage=None,
        proposed_object="cat.sch.rev",
        measures=[
            MvProposalMeasure(
                display_name="booking_value",
                expr="SUM(booking_value)",
                dedup_fingerprint="m1",
                benchmark_question_ids=[
                    "sql_snippet:measures:snip1",
                    "trusted_asset:ex1",
                    "q_bare_1234",
                    "sql_snippet:measures:snip1",  # duplicate → collapsed
                ],
            )
        ],
        checks=None,
        approved_for_rerun=False,
    )
    auto_optimize._mv_attach_provenance_labels([proposal], _config_with("snip1", "ex1"))
    labels = proposal.provenance_labels
    assert labels is not None
    # Two resolvable ids (deduped), the bare id contributes nothing.
    assert [l.label for l in labels] == ["Booking value", "What is total revenue by region?"]


def test_attach_labels_none_when_nothing_resolves():
    """A proposal whose only evidence is a bare id (no resolvable prefixed id) gets
    ``provenance_labels = None`` — the card falls back to counts + raw-ids."""
    proposal = MvProposal(
        suggestion_id="s2",
        dedup_fingerprint="fp2",
        target_space_id="space-1",
        run_id=None,
        candidate_type="NEW_METRIC_VIEW",
        confidence_score=None,
        tier=None,
        uncapped_tier=None,
        tier_capped_by_coverage=None,
        proposed_object="cat.sch.rev",
        measures=[
            MvProposalMeasure(
                display_name="x",
                expr="SUM(x)",
                dedup_fingerprint="m2",
                benchmark_question_ids=["q_bare_1"],
            )
        ],
        checks=None,
        approved_for_rerun=False,
    )
    auto_optimize._mv_attach_provenance_labels([proposal], _config_with("snip1", "ex1"))
    assert proposal.provenance_labels is None


def test_attach_labels_falls_back_to_bundle_evidence_ids():
    """A legacy one-element row carries its ids on ``evidence`` not the members;
    the gather falls back there so the label list still resolves."""
    proposal = MvProposal(
        suggestion_id="s3",
        dedup_fingerprint="fp3",
        target_space_id="space-1",
        run_id=None,
        candidate_type="NEW_METRIC_VIEW",
        confidence_score=None,
        tier=None,
        uncapped_tier=None,
        tier_capped_by_coverage=None,
        proposed_object="cat.sch.rev",
        measures=[],
        checks=None,
        approved_for_rerun=False,
        evidence={"benchmark_question_ids": ["trusted_asset:ex1"]},
    )
    auto_optimize._mv_attach_provenance_labels([proposal], _config_with("snip1", "ex1"))
    assert proposal.provenance_labels is not None
    assert proposal.provenance_labels[0].label == "What is total revenue by region?"
