"""7now gs_013 — wrong_filter_condition anchor.

Source: optimization run ab65fefe-9bb5-411c-9818-f62633ec9cfd,
iteration 1/2/4, AG_DECOMPOSED_H001. Iteration 1 produced an L6
candidate that landed and was rolled back. Iterations 2 and 4
returned no_structural_candidate. The ASI blame for this cluster
relates to a same-store/time-window filter.
"""

from genie_space_optimizer.optimization.failure_cluster import (
    FailureCluster,
)

SEVEN_NOW_GS_013 = FailureCluster(
    cluster_id="H001",
    target_qids=("7now_delivery_analytics_space_gs_013",),
    root_cause="wrong_filter_condition",
    asi_failure_type="other",
    failure_keys=("wrong_filter_condition", "wrong_aggregation"),
    blame_set_raw=("time_window", "same_store"),
    blame_set_normalized=(),
    rca_card_id="",
    rca_card_summary="",
    is_grounded=False,
)
