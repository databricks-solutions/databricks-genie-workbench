"""Airline gs_009 — plural_top_n_collapse anchor.

Source: optimization run 59a173d3-f71f-4901-90ad-e10f1084cd7f,
iteration 1 + 3, AG_DECOMPOSED_H001. Captured ASI blame is
"[RANK]" (a SQL-shape token that does not map to any UC asset),
which is why _resolve_asset_by_identifier returned no resolved
assets and synthesis exited with attempted_archetypes empty.
"""

from genie_space_optimizer.optimization.failure_cluster import (
    FailureCluster,
)

AIRLINE_GS_009 = FailureCluster(
    cluster_id="H001",
    target_qids=("airline_ticketing_and_fare_analysis_gs_009",),
    root_cause="plural_top_n_collapse",
    asi_failure_type="plural_top_n_collapse",
    failure_keys=("plural_top_n_collapse", "wrong_aggregation"),
    blame_set_raw=("[RANK]",),
    blame_set_normalized=(),
    rca_card_id="",
    rca_card_summary="",
    is_grounded=False,
)
