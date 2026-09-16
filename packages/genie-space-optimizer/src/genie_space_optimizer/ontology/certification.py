"""Certification/deprecation authority map (Signal Authority Stage 2, MV-D94) — pure, I/O-free.

Turns per-asset certification-tag rows into the ``{fqn -> "certified" | "deprecated"}`` map
Stage 2 feeds into two seams:

  * ``certified`` → the ``curated`` (0.6) governance rung (``materialize._governance_map``
    emits ``curated`` for a certified FQN not already ``governed`` — ``_governance_factor``
    then takes the max rung, so a certified *and* governed asset stays ``governed`` 1.0).
  * ``deprecated`` → a **rank-time firewall** (``rank._score_row`` blocks any proposal whose
    assets include a deprecated one, ``block_reason="deprecated_asset"``, ``surfaced=false``)
    — a deprecated asset is *steered away from*, not merely ranked low.

**Two surfaces.** A row may name certification either as the native
``system.certification_status`` tag (whose value is ``certified`` or ``deprecated``) or as the
boolean ``certified`` tag (``true`` → certified). Both are read into the one status vocabulary.

**Honest-gap (MV-D43).** ``certified=false``, an unrecognized value, or any absent tag ⇒ the
FQN is **OMITTED** (absent, never inferred ``ungoverned``). Missing certification only lowers
coverage; it is never a positive "uncertified" mark.

**Conflict resolution.** ``deprecated`` is the stronger safety signal, so if an FQN carries
both a deprecation and a certification signal (contradictory tags), ``deprecated`` wins — the
asset is steered away from regardless. Resolution is order-independent, so the map is
deterministic no matter what order the reader hands the rows in.

**Deterministic (MV-D82).** Sorted output, no wall-clock — the same rows always yield the same
map. Nothing here writes a governed tag or reads I/O; it feeds the ranker a better input, it
does not change the blend, the tiers, or the naming denylist.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

# The native certification tag (value-carrying) and the boolean certified tag, matched
# case-insensitively. Kept OUT of any naming path — ``transforms.is_domain_entity_tag`` /
# ``domain_facet_denylist`` still drop these so certification never names a domain (MV-D51).
_NATIVE_TAG = "system.certification_status"
_BOOLEAN_TAG = "certified"


def _value_of(row: Mapping[str, Any]) -> str:
    """The per-assignment tag value, lower-cased and stripped (``tag_value`` or ``value``)."""
    for k in ("tag_value", "value"):
        v = row.get(k)
        if v is not None and str(v) != "":
            return str(v).strip().lower()
    return ""


def _status_of(tag_name: str, value: str) -> str | None:
    """The certification status one row asserts, or ``None`` if it asserts nothing.

    ``system.certification_status`` carries the status as its value (``certified`` /
    ``deprecated``); the boolean ``certified`` tag asserts ``certified`` only when ``true``.
    Any other value (``certified=false``, an unrecognized status) ⇒ ``None`` (omit)."""
    if tag_name == _NATIVE_TAG:
        return value if value in ("certified", "deprecated") else None
    if tag_name == _BOOLEAN_TAG:
        return "certified" if value == "true" else None
    return None


def certification_map(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    """Reduce certification-tag rows into a sorted ``{fqn -> "certified" | "deprecated"}`` map.

    ``rows`` are ``{fqn, tag_name, tag_value}`` (the reader builds the FQN and selects only
    the two certification tag names). An FQN with no recognized certification signal is
    absent (honest-gap); ``deprecated`` wins over ``certified`` when both are present
    (order-independent). Empty or fully-unrecognized input ⇒ ``{}``.
    """
    out: dict[str, str] = {}
    for row in rows or ():
        fqn = str(row.get("fqn") or "").strip()
        if not fqn:
            continue
        status = _status_of(str(row.get("tag_name") or "").strip().lower(), _value_of(row))
        if status is None:
            continue  # certified=false / absent / unrecognized ⇒ OMIT (never "ungoverned")
        # ``deprecated`` is sticky: once an FQN is deprecated it stays deprecated even if a
        # later row certifies it (steer away from a stale asset regardless). This makes the
        # reduction order-independent → deterministic.
        if out.get(fqn) == "deprecated":
            continue
        out[fqn] = status
    return dict(sorted(out.items()))
