"""Firewall-by-class for the external-context tier (Phase 4, MV-D38) — PURE.

The single home of the influence vocabulary and the ``influence_allows`` gate.
Both the backend registry (``backend/ontology/services/context_sources.py``) and
the wheel resolver (``context_pack.py``) import from here so there is exactly ONE
definition of "which target may a source of this class steer" — no drift between
the preflight tier and the batch resolver (MV-D38/D47).

The carve is structural: a ``membership | measure | certification`` target is
NEVER reachable by any source, and that check runs FIRST, so a new source class
can never accidentally open a structural writer. External / overlay sources reach
only the naming family; internal UC-backed sources add the two structural targets.

This module holds NO egress, NO registry, and NO I/O — it is a lookup table plus
one predicate, wheel-native (importable on the job cluster, never importing
``backend.*``).
"""

from __future__ import annotations

from typing import Any

# ── Firewall-by-class target vocabulary (§4/§7) ──────────────────────────────
# The ONLY targets any external / overlay source may reach. All classes share these.
_EXTERNAL_TARGETS: frozenset[str] = frozenset(
    {"naming", "description", "synonym", "gap_hypothesis", "recent_context"}
)
# Internal UC-backed sources may ADDITIONALLY reach these structural targets.
_INTERNAL_EXTRA_TARGETS: frozenset[str] = frozenset({"structural_signal", "validation"})
# NEVER reachable by any source — structurally firewalled (MV-D38). The positive
# guard test proves no source id can be threaded into one of these writers.
FORBIDDEN_TARGETS: frozenset[str] = frozenset({"membership", "measure", "certification"})


def influence_allows(entry: Any, target: str) -> bool:
    """Firewall-by-class (MV-D38): may ``entry`` influence ``target``?

    ``entry`` is any object carrying a ``.klass`` of ``"internal"`` | ``"external"``
    (a :class:`~genie_space_optimizer.ontology.context_registry.ContextSource`, or a
    plain shim). External / overlay sources reach ONLY the naming-family targets;
    internal UC-backed sources add the structural targets. A membership / measure /
    certification target is **never** reachable by any source — that carve is
    structural (checked first), so a new class can never accidentally open it."""
    if target in FORBIDDEN_TARGETS:
        return False
    allowed = _EXTERNAL_TARGETS
    if getattr(entry, "klass", "external") == "internal":
        allowed = _EXTERNAL_TARGETS | _INTERNAL_EXTRA_TARGETS
    return target in allowed


__all__ = [
    "FORBIDDEN_TARGETS",
    "influence_allows",
]
