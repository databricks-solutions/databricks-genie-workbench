"""Asset/blame alignment for proposal grounding.

Drops proposals whose target assets are disjoint from the cluster's
lineage (blame + reference + lineage assets) unless a
``cross_asset_justification`` is explicitly present on the patch.
"""

from __future__ import annotations

from typing import Any


_ASSET_FIELDS = ("blame_assets", "reference_assets", "lineage_assets")
_PROPOSAL_TARGET_KEYS = (
    "target_object",
    "target_table",
    "target",
    "patch_target",
    "object_full_name",
)


def _bare_name(fqn: str) -> str:
    """Return the last dotted segment of a fully-qualified name."""
    if "." in fqn:
        return fqn.rsplit(".", 1)[-1]
    return fqn


def _normalise(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def cluster_lineage_assets(cluster: dict | None) -> tuple[str, ...]:
    """Return the union of blame/reference/lineage assets for a cluster.

    Both fully-qualified names and bare table names are returned so
    proposals can match either form.
    """
    if not isinstance(cluster, dict):
        return ()
    seen: list[str] = []
    for field in _ASSET_FIELDS:
        for raw in cluster.get(field, []) or []:
            value = _normalise(raw)
            if not value:
                continue
            if value not in seen:
                seen.append(value)
            bare = _bare_name(value)
            if bare and bare not in seen:
                seen.append(bare)
    return tuple(seen)


def proposal_target_assets(patch: dict | None) -> tuple[str, ...]:
    """Return the asset identifiers a proposal claims to write."""
    if not isinstance(patch, dict):
        return ()
    seen: list[str] = []
    for key in _PROPOSAL_TARGET_KEYS:
        value = _normalise(patch.get(key))
        if not value:
            continue
        if value not in seen:
            seen.append(value)
        bare = _bare_name(value)
        if bare and bare not in seen:
            seen.append(bare)
        # column-stripped table form: only strip when the value looks
        # like a column-qualified table (4+ dotted segments such as
        # ``cat.sch.tbl.col``). Stripping a 3-segment FQN like
        # ``cat.sch.tbl`` would emit ``cat.sch`` and ``sch`` as
        # asset tokens, which pollutes downstream lineage matching.
        if value.count(".") >= 3:
            parent = value.rsplit(".", 1)[0]
            parent_bare = _bare_name(parent) if parent else ""
            if parent and parent not in seen:
                seen.append(parent)
            if parent_bare and parent_bare not in seen:
                seen.append(parent_bare)
    return tuple(seen)


def proposal_aligns_with_cluster(
    patch: dict | None,
    cluster: dict | None,
) -> dict:
    """Return the alignment decision for a proposal against a cluster."""
    proposal_assets = proposal_target_assets(patch)
    cluster_assets = cluster_lineage_assets(cluster)

    if (
        patch
        and isinstance(patch.get("cross_asset_justification"), str)
        and patch.get("cross_asset_justification").strip()
    ):
        return {
            "aligned": True,
            "reason": "cross_asset_justification_present",
            "proposal_assets": proposal_assets,
            "cluster_assets": cluster_assets,
        }

    if not cluster_assets:
        return {
            "aligned": True,
            "reason": "no_lineage_constraint",
            "proposal_assets": proposal_assets,
            "cluster_assets": cluster_assets,
        }

    if not proposal_assets:
        return {
            "aligned": True,
            "reason": "no_lineage_constraint",
            "proposal_assets": proposal_assets,
            "cluster_assets": cluster_assets,
        }

    cluster_set = set(cluster_assets)
    if any(a in cluster_set for a in proposal_assets):
        return {
            "aligned": True,
            "reason": "asset_in_cluster_lineage",
            "proposal_assets": proposal_assets,
            "cluster_assets": cluster_assets,
        }

    return {
        "aligned": False,
        "reason": "asset_not_in_cluster_lineage",
        "proposal_assets": proposal_assets,
        "cluster_assets": cluster_assets,
    }
