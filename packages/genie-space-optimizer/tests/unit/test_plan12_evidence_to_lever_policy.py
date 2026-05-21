"""Plan 12 — evidence_kind → eligible_lever_families policy.

Non-generating lanes (Lever 1: add_column_description) MUST be refused
for evidence types that demand structural generation.
"""


def test_wrong_aggregation_refuses_lever_1():
    from genie_space_optimizer.optimization.evidence_to_lever_policy import (
        eligible_lever_families,
    )
    eligible = eligible_lever_families("wrong_aggregation")
    assert 1 not in eligible
    # 5b (example sql) preferred first, then 6 (sql snippet)
    assert eligible[0] == "5b"


def test_missing_filter_refuses_lever_1():
    from genie_space_optimizer.optimization.evidence_to_lever_policy import (
        eligible_lever_families,
    )
    assert 1 not in eligible_lever_families("missing_filter")


def test_column_disambiguation_refuses_lever_1():
    from genie_space_optimizer.optimization.evidence_to_lever_policy import (
        eligible_lever_families,
    )
    assert 1 not in eligible_lever_families("column_disambiguation")


def test_top_n_collapse_refuses_lever_1():
    from genie_space_optimizer.optimization.evidence_to_lever_policy import (
        eligible_lever_families,
    )
    assert 1 not in eligible_lever_families("top_n_collapse")
    assert 1 not in eligible_lever_families("plural_top_n_collapse")


def test_metadata_only_evidence_accepts_lever_1():
    """Truly metadata-only failures (e.g. ambiguous column name without
    SQL structural issue) MAY route to Lever 1."""
    from genie_space_optimizer.optimization.evidence_to_lever_policy import (
        eligible_lever_families,
    )
    eligible = eligible_lever_families("ambiguous_column_description")
    assert 1 in eligible


def test_unknown_evidence_falls_back_to_generating_lane():
    """Unknown evidence_kinds default to 5b (most forgiving generating
    lane). NEVER default to Lever 1."""
    from genie_space_optimizer.optimization.evidence_to_lever_policy import (
        eligible_lever_families,
    )
    eligible = eligible_lever_families("some_brand_new_kind")
    assert eligible[0] == "5b"
    assert 1 not in eligible


def test_refuses_non_generating_lane_helper():
    from genie_space_optimizer.optimization.evidence_to_lever_policy import (
        refuses_non_generating_lane,
    )
    assert refuses_non_generating_lane("wrong_aggregation") is True
    assert refuses_non_generating_lane("missing_filter") is True
    assert refuses_non_generating_lane("ambiguous_column_description") is False
    assert refuses_non_generating_lane("some_unknown_kind") is True


def test_evidence_kind_normalization():
    """The policy normalizes whitespace + case so callers don't have
    to (root_cause / asi_failure_type strings come in varied shapes
    from upstream)."""
    from genie_space_optimizer.optimization.evidence_to_lever_policy import (
        eligible_lever_families,
        refuses_non_generating_lane,
    )
    assert eligible_lever_families("  WRONG_AGGREGATION  ")[0] == "5b"
    assert refuses_non_generating_lane("Missing_Filter") is True
    # Empty / None falls into the unknown-defaults bucket (generating
    # lanes only).
    assert eligible_lever_families("") == ("5b", "6")
    assert eligible_lever_families(None) == ("5b", "6")  # type: ignore[arg-type]
