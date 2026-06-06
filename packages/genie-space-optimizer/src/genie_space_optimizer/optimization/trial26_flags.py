"""Trial 26 — Kit-gate reachability (RCA-kind normalisation + kit-map coverage
+ applier ``name``-field fix).

Module mirrors :mod:`trial24_flags` so a single ``export
GSO_TRIAL26_KIT_GATE_REACHABLE=0`` rolls back every Trial 26 path
byte-stably and each sub-flag is surgically rollable per Trial 24's
``_subflag_opt_out`` shape.

Trial 26 closes the gap exposed by the Trial 24 live invalidation
(``TRIAL24_KIT_FORCED_V1=0`` on both anchors despite the kit-at-source
synthesis being architecturally correct). The gate that decides "is
this RCA a Trial 24 candidate?" never fires on the live RCA population
because:

1. The kit map (`_TRIAL24_KIT_FOR_RCA`) covers only
   ``extra_defensive_filter`` + ``top_n_cardinality_collapse``; the
   live distribution on airline is ``wrong_aggregation``,
   ``wrong_column``, ``plural_top_n_collapse``. W26.2 expands the map.
2. RCA-kind values arrive as free-form English labels (e.g.
   ``"Top-N cardinality collapse via spurious RANK()=1 filter"``) and
   the existing normaliser is only ``.strip().lower()``. W26.1 adds a
   canonical reducer.
3. ``add_sql_snippet_filter`` patches that DO reach the API are
   rejected with ``Invalid serialized_space: Unknown field 'name'``
   because the producer stamps ``name`` instead of ``display_name``.
   W26.3 fixes the producer field.

Opt-out semantics (default ON): any of ``0`` / ``false`` / ``no`` /
``off`` (case insensitive) disables. Env unset, empty, or any other
value enables. Mirrors Trial 23 / Trial 24 default-on pattern.
"""
from __future__ import annotations

import os


_TRIAL26_FLAG_OFF_VALUES = frozenset({"0", "false", "no", "off"})


def _flag_enabled(env_name: str) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    return raw not in _TRIAL26_FLAG_OFF_VALUES


def trial26_kit_gate_reachable_enabled() -> bool:
    """Trial 26 master flag. Default ON.

    Opt out with ``export GSO_TRIAL26_KIT_GATE_REACHABLE=0`` for
    emergency rollback. When OFF, every Trial 26 sub-flag is forced
    OFF regardless of its own env var, so the kit map stays at its
    Trial-24 shape, the RCA-kind normaliser stays at
    ``.strip().lower()`` only, and the ``add_sql_snippet_filter``
    producer keeps emitting the rejected ``name`` field
    (byte-stable rollback).
    """
    return _flag_enabled("GSO_TRIAL26_KIT_GATE_REACHABLE")


def _subflag_opt_out(env_name: str) -> bool:
    """Shared opt-out helper for Trial 26 sub-flags.

    Returns True (enabled) when the master is on AND the sub-flag
    has not been explicitly disabled. Only the explicit OFF
    vocabulary (``0`` / ``false`` / ``no`` / ``off``) disables it,
    so a sub-flag defaults ON whenever the master is ON.
    """
    raw = os.environ.get(env_name, "").strip().lower()
    off = raw in _TRIAL26_FLAG_OFF_VALUES
    return trial26_kit_gate_reachable_enabled() and not off


def trial26_rca_kind_canonical_normalise_enabled() -> bool:
    """W26.1 — typed canonical RCA-kind normaliser.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE=0``.

    Adds a deterministic-first, LLM-validated reduction layer over
    free-form English labels emitted by the Stage 1 diagnosis. The
    output is a closed enum (canonical kit-map key, alias key, or
    the explicit sentinel ``unknown_kind``). The pre-Trial-26
    normaliser (`_normalize_rca_kind`: ``.strip().lower()`` only)
    is preserved when the flag is OFF.
    """
    return _subflag_opt_out("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE")


def trial26_kit_map_expanded_enabled() -> bool:
    """W26.2 — extend `_TRIAL24_KIT_FOR_RCA` and
    `RCA_KIND_TO_FIXING_MECHANISMS` to cover the live airline RCA
    distribution.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL26_KIT_MAP_EXPANDED=0``.

    The Trial 24 kit map covered two keys
    (``extra_defensive_filter`` and ``top_n_cardinality_collapse``)
    so zero airline RCAs (``wrong_aggregation``, ``wrong_column``,
    ``plural_top_n_collapse``) could trip the kit gate even with a
    perfect normaliser. W26.2 adds matched mechanism families +
    lever kits for those keys following the existing Trial 23
    mechanism-family / Trial 24 lever-projection shape. When OFF,
    the map shrinks back to the Trial-24 keys.
    """
    return _subflag_opt_out("GSO_TRIAL26_KIT_MAP_EXPANDED")


def trial26_applier_snippet_name_fix_enabled() -> bool:
    """W26.3 — fix ``add_sql_snippet_filter`` patches emitting the
    rejected ``name`` field.

    Default ON when the master is ON. Opt out with
    ``export GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX=0``.

    The producer was stamping ``name`` on the nested
    ``sql_snippet`` body in three places
    (``producer_snippet_validator``, ``sql_snippet_finalizer``,
    ``stages/validate_patch``). The canonical
    ``serialized_space`` schema for ``sql_snippets.filters`` /
    ``.expressions`` / ``.measures`` only allows ``display_name``
    (no ``name``), so the Databricks Genie API rejected the patch
    with ``Invalid serialized_space: Unknown field 'name'``. This
    flag rewrites those producers to emit ``display_name``
    (matching the canonical schema); when OFF the legacy ``name``
    field is restored so any caller that relied on the rejection
    payload can be regressed.
    """
    return _subflag_opt_out("GSO_TRIAL26_APPLIER_SNIPPET_NAME_FIX")


__all__ = [
    "trial26_applier_snippet_name_fix_enabled",
    "trial26_kit_gate_reachable_enabled",
    "trial26_kit_map_expanded_enabled",
    "trial26_rca_kind_canonical_normalise_enabled",
]
