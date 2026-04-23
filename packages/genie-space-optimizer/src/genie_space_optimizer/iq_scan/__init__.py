"""IQ Scan: static quality scoring for Genie Space configurations.

This package contains the pure scoring engine (no IO, no Lakebase, no SDK calls)
used by both the backend scanner service and the GSO optimizer preflight.
"""

from genie_space_optimizer.iq_scan.rls_audit import collect_rls_audit
from genie_space_optimizer.iq_scan.scoring import (
    CONFIG_CHECK_COUNT,
    calculate_score,
    get_maturity_label,
)

__all__ = [
    "CONFIG_CHECK_COUNT",
    "calculate_score",
    "collect_rls_audit",
    "get_maturity_label",
]
