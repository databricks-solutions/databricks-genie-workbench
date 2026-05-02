"""Track 5 — SQL-shape patch quality predicates."""
from __future__ import annotations


def test_is_unrequested_is_not_null_filter_detects_tautology() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_unrequested_is_not_null_filter,
    )

    weak_patch = {
        "type": "add_sql_snippet_filter",
        "snippet": "WHERE customer_id IS NOT NULL OR customer_id IS NULL",
        "root_cause": "missing_filter",
    }
    assert is_unrequested_is_not_null_filter(weak_patch) is True


def test_is_unrequested_is_not_null_filter_detects_bare_is_not_null() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_unrequested_is_not_null_filter,
    )

    weak_patch = {
        "type": "add_sql_snippet_filter",
        "snippet": "WHERE zone_combination IS NOT NULL",
        "root_cause": "extra_defensive_filter",
    }
    assert is_unrequested_is_not_null_filter(weak_patch) is True


def test_is_unrequested_is_not_null_filter_allows_justified_predicate() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_unrequested_is_not_null_filter,
    )

    justified_patch = {
        "type": "add_sql_snippet_filter",
        "snippet": "WHERE customer_id IS NOT NULL",
        "root_cause": "missing_filter",
        "justification": (
            "Question asks for the count of customers who placed an "
            "order; rows without a customer_id are not customers and "
            "must be excluded."
        ),
    }
    assert is_unrequested_is_not_null_filter(justified_patch) is False


def test_is_unrequested_currency_filter_detects_already_correct_currency() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_unrequested_currency_filter,
    )

    weak_patch = {
        "type": "add_sql_snippet_filter",
        "snippet": "WHERE currency = 'USD'",
        "root_cause": "missing_filter",
        "metric_native_currency": "USD",
        "question_requested_currency": "USD",
    }
    assert is_unrequested_currency_filter(weak_patch) is True


def test_is_unrequested_currency_filter_allows_currency_mismatch() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_unrequested_currency_filter,
    )

    legit_patch = {
        "type": "add_sql_snippet_filter",
        "snippet": "WHERE currency = 'USD'",
        "root_cause": "missing_filter",
        "metric_native_currency": "EUR",
        "question_requested_currency": "USD",
    }
    assert is_unrequested_currency_filter(legit_patch) is False


def test_is_rank_when_limit_n_required_detects_rank_misuse() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_rank_when_limit_n_required,
    )

    weak_patch = {
        "type": "add_sql_snippet_calculation",
        "snippet": "SELECT *, RANK() OVER (ORDER BY revenue DESC) AS rn FROM t",
        "root_cause": "plural_top_n_collapse",
        "question_requests_exact_top_n": True,
    }
    assert is_rank_when_limit_n_required(weak_patch) is True


def test_is_rank_when_limit_n_required_allows_row_number() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_rank_when_limit_n_required,
    )

    canonical_patch = {
        "type": "add_sql_snippet_calculation",
        "snippet": (
            "SELECT *, ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rn "
            "FROM t QUALIFY rn <= 5"
        ),
        "root_cause": "plural_top_n_collapse",
        "question_requests_exact_top_n": True,
    }
    assert is_rank_when_limit_n_required(canonical_patch) is False


def test_is_rank_when_limit_n_required_allows_rank_when_ties_intended() -> None:
    from genie_space_optimizer.optimization.sql_shape_quality import (
        is_rank_when_limit_n_required,
    )

    ties_intended_patch = {
        "type": "add_sql_snippet_calculation",
        "snippet": "SELECT *, RANK() OVER (ORDER BY revenue DESC) AS rn FROM t",
        "root_cause": "plural_top_n_collapse",
        "question_requests_exact_top_n": False,
    }
    assert is_rank_when_limit_n_required(ties_intended_patch) is False


def test_select_grounded_proposals_demotes_weak_snippet_when_scoped_instruction_exists() -> None:
    """Track 5 — when a weak SQL snippet patch competes with a scoped
    instruction patch covering the same root_cause and qids,
    ``select_patch_bundle`` must drop the snippet with
    ``_drop_reason="weak_sql_shape_quality"`` so the cap budget can
    fund the instruction.
    """
    from genie_space_optimizer.optimization.proposal_grounding import (
        select_patch_bundle,
    )

    weak_snippet = {
        "id": "P_WEAK",
        "type": "add_sql_snippet_filter",
        "snippet": "WHERE col IS NOT NULL OR col IS NULL",
        "root_cause": "extra_defensive_filter",
        "target_qids": ["q1"],
        "relevance_score": 0.9,
    }
    scoped_instruction = {
        "id": "P_SCOPED",
        "type": "add_instruction",
        "section_name": "QUERY RULES",
        "value": "Do not add IS NOT NULL filters unless the question asks for them.",
        "root_cause": "extra_defensive_filter",
        "target_qids": ["q1"],
        "relevance_score": 0.5,
    }

    grounded = select_patch_bundle(
        [weak_snippet, scoped_instruction],
        max_patches=2,
    )
    grounded_ids = {p.get("id") for p in grounded}

    assert "P_SCOPED" in grounded_ids, (
        "scoped instruction was dropped; proposal grounding must keep "
        "it as a Track 5 replacement for the weak snippet"
    )
    assert "P_WEAK" not in grounded_ids, (
        "weak SQL snippet was kept despite a scoped-instruction "
        "replacement; cap budget will be wasted"
    )
    assert weak_snippet.get("_drop_reason") == "weak_sql_shape_quality", (
        f"weak snippet missing drop reason; got "
        f"{weak_snippet.get('_drop_reason')!r}"
    )


def test_select_grounded_proposals_keeps_weak_snippet_when_no_replacement() -> None:
    """When no scoped instruction patch covers the same qids and
    root_cause, the weak snippet stays — Track 5 is conservative.
    """
    from genie_space_optimizer.optimization.proposal_grounding import (
        select_patch_bundle,
    )

    weak_snippet = {
        "id": "P_WEAK",
        "type": "add_sql_snippet_filter",
        "snippet": "WHERE col IS NOT NULL OR col IS NULL",
        "root_cause": "extra_defensive_filter",
        "target_qids": ["q1"],
        "relevance_score": 0.9,
    }
    grounded = select_patch_bundle([weak_snippet], max_patches=2)
    assert any(p.get("id") == "P_WEAK" for p in grounded), (
        "weak snippet was dropped even though no scoped instruction "
        "replacement exists"
    )
