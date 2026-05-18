"""Airline gs_024 — missing_filter anchor.

Source: optimization run 59a173d3-f71f-4901-90ad-e10f1084cd7f,
iteration 2 + 4, AG_DECOMPOSED_H002. ASI blame contains
filter/column phrases (not table FQNs), so blame_set_normalized
is empty.
"""

from genie_space_optimizer.optimization.failure_cluster import (
    FailureCluster,
)

AIRLINE_GS_024 = FailureCluster(
    cluster_id="H002",
    target_qids=("airline_ticketing_and_fare_analysis_gs_024",),
    root_cause="missing_filter",
    asi_failure_type="missing_filter",
    failure_keys=("missing_filter", "wrong_aggregation"),
    blame_set_raw=("PAYMENT_CURRENCY_CD",),
    blame_set_normalized=(),
    rca_card_id="",
    rca_card_summary="",
    is_grounded=False,
)
