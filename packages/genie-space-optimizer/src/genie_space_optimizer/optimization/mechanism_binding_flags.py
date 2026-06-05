"""Mechanism-binding flags — promote mechanism learning from
observe-only to binding (e943 / d139 plan, Phase 2 item #10).

The mechanism-coverage check (``mechanism_coverage.py``) and the
Trial-23 RCA→mechanism route (``rca_mechanism_routing.py``) were both
*observe-only*: they emitted audit markers but never removed an
inadequate proposal from the slate. The postmortems showed the
optimizer repeatedly shipping a mechanism that cannot fix the RCA,
burning an iteration, and learning nothing.

These flags promote those two surfaces to *binding* — they DROP an
inadequate proposal. They were introduced default OFF for a byte-stable
opt-in; both were promoted to **default ON** (mirrors the
``_flag_default_on`` pattern in ``common/config.py``). The Plan-12
AG-retry pivot is already binding behind its own
``plan12_live_ag_retry_pivot_mutate_enabled`` flag, so it is
intentionally not re-gated here.

The binding logic NEVER empties the slate: dropping the sole surviving
proposal would re-create the all-dropped flatline the design explicitly
guards against (see ``synthesize.py`` "central design tension"). The
pure decision lives in :mod:`mechanism_binding`; these flags only gate
whether it runs.

Default-on semantics: only ``0`` / ``false`` / ``no`` / ``off`` (case
insensitive) disables. Anything else — including unset — is ON.
"""
from __future__ import annotations

import os


_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_default_on(env_name: str) -> bool:
    return (
        os.environ.get(env_name, "").strip().lower() not in _FLAG_OFF_VALUES
    )


def mechanism_coverage_binding_enabled() -> bool:
    """When ON, an ``uncovered`` mechanism-coverage verdict DROPS the
    proposal from the slate (instead of only emitting the observe-only
    ``GSO_MECHANISM_COVERAGE_V1`` marker), provided at least one
    adequately-covered proposal survives. Default ON.

    Disable with ``export GSO_MECHANISM_COVERAGE_BINDING=0``.
    """
    return _flag_default_on("GSO_MECHANISM_COVERAGE_BINDING")


def rca_mechanism_route_binding_enabled() -> bool:
    """When ON, a fired ``rca_mechanism_defaulted_to_example_sql``
    reason BLOCKS the defaulted proposals from the slate (instead of
    only emitting the observe-and-route anti-success marker), provided
    at least one non-defaulted proposal survives. Default ON.

    Disable with ``export GSO_RCA_MECHANISM_ROUTE_BINDING=0``.
    """
    return _flag_default_on("GSO_RCA_MECHANISM_ROUTE_BINDING")


def instruction_route_binding_enabled() -> bool:
    """When ON, a fired ``rca_mechanism_defaulted_to_instruction_text``
    reason BLOCKS the lone-instruction proposals from the slate for a
    SQL-shape RCA (Track B / B1), provided at least one non-defaulted
    proposal survives. The natural-language instruction cannot change the
    generated SQL shape for ``top_n_cardinality_collapse`` /
    ``canonical_dimension_missed``, so shipping it burns an iteration and
    learns nothing. Default ON.

    Disable with ``export GSO_INSTRUCTION_ROUTE_BINDING=0``.
    """
    return _flag_default_on("GSO_INSTRUCTION_ROUTE_BINDING")


__all__ = [
    "mechanism_coverage_binding_enabled",
    "rca_mechanism_route_binding_enabled",
    "instruction_route_binding_enabled",
]
