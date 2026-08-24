"""MV-D23 guardrail (ii): the run-kind exclusion is pinned, not a convention.

The register's stated failure mode is a filter forgotten in one place. So every
run-listing query over ``genie_opt_runs`` must route through the single
``MV_ADVICE_RUN_EXCLUSION`` constant, and this test asserts each site does — the
same spirit as the MV-D21 column pin. A new run-listing site that inlines its
own filter (or forgets one) fails here rather than silently leaking a sentinel
advice run into optimization history or accuracy.
"""

from __future__ import annotations

import inspect


def test_the_exclusion_constant_excludes_advice_and_keeps_legacy_rows():
    from genie_space_optimizer.common.config import (
        MV_ADVICE_RUN_EXCLUSION,
        MV_RUN_KIND_ADVICE,
        MV_RUN_KIND_OPTIMIZATION,
    )

    assert MV_RUN_KIND_ADVICE in MV_ADVICE_RUN_EXCLUSION
    assert "COALESCE" in MV_ADVICE_RUN_EXCLUSION
    # A legacy NULL run_kind resolves to optimization, so history never hides a
    # real run written before the column existed.
    assert MV_RUN_KIND_OPTIMIZATION in MV_ADVICE_RUN_EXCLUSION


def test_every_run_listing_site_routes_through_the_pinned_predicate():
    from backend.routers.auto_optimize import load_runs_with_fallback
    from backend.services.gso_lakebase import load_gso_runs_for_space
    from backend.services.scanner import scan_space

    sites = {
        "gso_lakebase.load_gso_runs_for_space": load_gso_runs_for_space,
        "auto_optimize.load_runs_with_fallback": load_runs_with_fallback,
        "scanner.scan_space (GSO Delta fallback)": scan_space,
    }
    for label, fn in sites.items():
        source = inspect.getsource(fn)
        assert "MV_ADVICE_RUN_EXCLUSION" in source, (
            f"{label} lists genie_opt_runs without routing through the pinned "
            "MV_ADVICE_RUN_EXCLUSION predicate (MV-D23 guardrail ii)"
        )


def test_point_lookup_by_run_id_is_not_filtered():
    # An advice run must still be readable by id (its results screen); only
    # LISTING and accuracy-aggregation exclude it.
    from backend.services.gso_lakebase import load_gso_run

    source = inspect.getsource(load_gso_run)
    assert "MV_ADVICE_RUN_EXCLUSION" not in source
