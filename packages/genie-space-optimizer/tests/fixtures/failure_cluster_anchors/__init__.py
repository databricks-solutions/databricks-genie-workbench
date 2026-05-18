"""Phase 2 — live-shape FailureCluster anchor fixtures.

Each fixture is a frozen FailureCluster constructed from the
exact ASI output captured in a real failed run. These are the
only acceptance criterion for changes touching the synthesis
typed-contract path (Phase 1 stages). Synthetic dict fixtures
are forbidden in this directory.

Live anchors:
- airline_gs_009: plural_top_n_collapse, run 59a173d3-...
- airline_gs_024: missing_filter, run 59a173d3-...
- seven_now_gs_013: wrong_filter_condition, run ab65fefe-...
- seven_now_gs_026: plural_top_n_collapse, run ab65fefe-...
"""

from .airline_gs_009 import AIRLINE_GS_009
from .airline_gs_024 import AIRLINE_GS_024
from .seven_now_gs_013 import SEVEN_NOW_GS_013
from .seven_now_gs_026 import SEVEN_NOW_GS_026

ALL_ANCHORS = (
    AIRLINE_GS_009,
    AIRLINE_GS_024,
    SEVEN_NOW_GS_013,
    SEVEN_NOW_GS_026,
)
