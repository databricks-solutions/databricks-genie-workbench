"""Phase 2 Action 2.2 — RepairKit dataclass and grouping helpers.

A ``RepairKit`` is a strategist-emitted bundle of patches with shared:
* ``repair_archetype`` — string label from ``REPAIR_ARCHETYPES`` (or
  ``"_implicit"`` for legacy unstamped patches).
* ``target_qids`` — tuple of qids the kit claims to fix.
* ``expected_causal_effect`` — free-text declaration of the mechanism.

Kits are the unit of safety evaluation in the kit-level gate. The
existing patch-flat-list path remains usable when no archetype stamping
has happened upstream — ``group_patches_into_kits`` collapses such
patches into one ``_implicit`` kit per ``target_qids`` tuple so the
gate sees a uniform interface.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Iterable


@dataclass(frozen=True)
class RepairKit:
    kit_id: str
    repair_archetype: str
    target_qids: tuple[str, ...]
    expected_causal_effect: str
    patches: tuple[dict, ...] = field(default_factory=tuple)


def _kit_key(patch: dict) -> tuple[str, tuple[str, ...], str]:
    archetype = str(patch.get("_repair_archetype") or "_implicit")
    target_qids = tuple(
        sorted(str(q) for q in (patch.get("target_qids") or ()) if str(q))
    )
    expected_effect = str(patch.get("_expected_causal_effect") or "")
    return (archetype, target_qids, expected_effect)


def _make_kit_id(key: tuple[str, tuple[str, ...], str]) -> str:
    blob = json.dumps(
        {
            "archetype": key[0],
            "target_qids": list(key[1]),
            "expected_effect": key[2],
        },
        sort_keys=True,
        ensure_ascii=True,
    ).encode("utf-8")
    return "kit_" + hashlib.sha1(blob).hexdigest()[:10]


def group_patches_into_kits(patches: Iterable[dict]) -> list[RepairKit]:
    """Group ``patches`` into ``RepairKit``s by shared
    ``(_repair_archetype, target_qids, _expected_causal_effect)``.

    Patches missing any of those fields are grouped under the
    ``_implicit`` archetype so the kit-level gate has a uniform input
    even for legacy unstamped patches.

    Returns the kits in deterministic order (sorted by kit_id) so
    downstream byte-stability is preserved.
    """
    by_key: dict[tuple[str, tuple[str, ...], str], list[dict]] = {}
    for p in patches or []:
        key = _kit_key(p)
        by_key.setdefault(key, []).append(p)

    kits: list[RepairKit] = []
    for key, members in by_key.items():
        kits.append(RepairKit(
            kit_id=_make_kit_id(key),
            repair_archetype=key[0],
            target_qids=key[1],
            expected_causal_effect=key[2],
            patches=tuple(members),
        ))
    return sorted(kits, key=lambda k: k.kit_id)
