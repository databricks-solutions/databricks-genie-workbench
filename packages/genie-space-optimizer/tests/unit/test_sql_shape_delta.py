from genie_space_optimizer.optimization.sql_shape_delta import compute_sql_shape_delta


def test_sql_shape_delta_detects_removed_extra_filter_and_remaining_window_delta():
    accepted = """
    SELECT payment_method, SUM(PAYMENT_AMT)
    FROM cat.sch.tkt_payment
    WHERE PAYMENT_CURRENCY_CD = 'USD'
      AND transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), 29) AND CURRENT_DATE()
    GROUP BY payment_method
    """
    candidate = """
    SELECT payment_method, SUM(PAYMENT_AMT)
    FROM cat.sch.tkt_payment
    WHERE transaction_date BETWEEN DATE_SUB(CURRENT_DATE(), 29) AND CURRENT_DATE()
    GROUP BY payment_method
    """
    ground_truth = """
    SELECT payment_method, SUM(PAYMENT_AMT)
    FROM cat.sch.tkt_payment
    WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY payment_method
    """

    delta = compute_sql_shape_delta(
        target_qid="gs_026",
        accepted_sql=accepted,
        candidate_sql=candidate,
        ground_truth_sql=ground_truth,
        accepted_row_count=4,
        candidate_row_count=7,
    )

    assert delta["target_qid"] == "gs_026"
    assert "removed_filter: PAYMENT_CURRENCY_CD = 'USD'" in delta["improved"]
    assert "row_count: 4 -> 7" in delta["improved"]
    assert "date_window: 29_vs_30" in delta["remaining"]
    assert "predicate_form: between_vs_gte" in delta["remaining"]
    assert delta["next_hint"] == (
        "teach recent_window_days archetype with DATE_SUB(CURRENT_DATE(), 30)"
    )
