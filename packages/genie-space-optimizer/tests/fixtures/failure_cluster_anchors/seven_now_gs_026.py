"""7now gs_026 — plural_top_n_collapse anchor.

Source: optimization run ab65fefe-9bb5-411c-9818-f62633ec9cfd,
iteration 3, AG_DECOMPOSED_H002. Requested lever was 5
(instruction-only) for a SQL-shape failure — the
question-shape→lever routing defect. ASI blame contains
top-N/rank-style tokens that do not resolve to UC assets.
"""

from genie_space_optimizer.optimization.failure_cluster import (
    FailureCluster,
)

SEVEN_NOW_GS_026 = FailureCluster(
    cluster_id="H002",
    target_qids=("7now_delivery_analytics_space_gs_026",),
    root_cause="plural_top_n_collapse",
    asi_failure_type="plural_top_n_collapse",
    failure_keys=("plural_top_n_collapse",),
    blame_set_raw=("[RANK]",),
    blame_set_normalized=(),
    rca_card_id="",
    rca_card_summary="",
    is_grounded=False,
)
