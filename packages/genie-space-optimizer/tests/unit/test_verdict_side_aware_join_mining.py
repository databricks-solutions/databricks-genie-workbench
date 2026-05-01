"""Verdict-side-aware proven join extraction.

- both_correct       -> may pull from genie SQL or GT SQL
- genie_correct      -> may pull from genie SQL only
- ground_truth_correct -> may pull from GT SQL only
- synthetic_example  -> handled by Task 4 (require corroboration)
"""
from genie_space_optimizer.optimization import feature_mining


def test_trusted_sides_for_both_correct():
    sides = feature_mining.trusted_sql_sides_for_verdict("both_correct")
    assert sides == ("genie", "ground_truth")


def test_trusted_sides_for_genie_correct():
    sides = feature_mining.trusted_sql_sides_for_verdict("genie_correct")
    assert sides == ("genie",)


def test_trusted_sides_for_ground_truth_correct():
    sides = feature_mining.trusted_sql_sides_for_verdict("ground_truth_correct")
    assert sides == ("ground_truth",)


def test_trusted_sides_for_unknown_verdict():
    assert feature_mining.trusted_sql_sides_for_verdict("both_wrong") == ()
    assert feature_mining.trusted_sql_sides_for_verdict("synthetic_example") == ()
    assert feature_mining.trusted_sql_sides_for_verdict("") == ()
