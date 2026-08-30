"""PURE ontology transforms — the single source of truth shared by the Phase-1
routes and the Phase-2 batch materializer.

These functions have **no I/O and no ``backend.*`` imports** (the wheel runs on a
job cluster). They take plain row dicts / graph structures and return plain
JSON-friendly dicts. The Phase-1 backend services import them and wrap the output
in Pydantic models; the batch job imports them and writes the output to Delta.
Because both paths call the *same* functions, ``mirror`` output == ``live`` output
(the parity guarantee, Phase-2 §11).

Extracted verbatim (behavior-preserving) from the Phase-1 services
``tag_graph.py`` / ``taxonomy.py`` / ``dedupe.py`` so the Phase-1 route contracts
and tests are unchanged.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

# ─────────────────────────────────────────────────────────────────────────
# Raw governed-tag row extraction + tag-graph assembly (from tag_graph.py)
# ─────────────────────────────────────────────────────────────────────────


def tag_key_of(row: dict[str, Any]) -> str | None:
    for k in ("tag_name", "tag_key", "name", "key"):
        v = row.get(k)
        if v:
            return str(v)
    return None


def allowed_values_of(row: dict[str, Any]) -> list[str]:
    """Extract allowed values from a governed_tags row across schema variants."""
    for k in ("allowed_values", "tag_values", "values"):
        v = row.get(k)
        if not v:
            continue
        if isinstance(v, list):
            return [str(x) for x in v if x is not None]
        if isinstance(v, str):
            s = v.strip().strip("[]")
            return [p.strip().strip('"').strip("'") for p in s.split(",") if p.strip()]
    return []


def member_fqn_of(row: dict[str, Any]) -> str | None:
    cat = row.get("catalog_name")
    sch = row.get("schema_name")
    tbl = row.get("table_name")
    parts = [p for p in (cat, sch, tbl) if p]
    return ".".join(str(p) for p in parts) if parts else None


def assemble_tag_graph(
    catalog_rows: list[dict[str, Any]],
    assign_rows: list[dict[str, Any]],
    as_of: str,
) -> dict[str, Any]:
    """Assemble the tag-graph structure from raw catalog + assignment rows.

    Returns ``{"tags": [{"tag_key", "allowed_values", "assignment_count",
    "members"}], "as_of"}`` — identical to the Phase-1 ``build_graph`` output.
    """
    tags: dict[str, dict[str, Any]] = {}
    for r in catalog_rows:
        key = tag_key_of(r)
        if not key:
            continue
        tags.setdefault(key, {"tag_key": key, "allowed_values": allowed_values_of(r), "members": []})

    for r in assign_rows:
        key = tag_key_of(r)
        fqn = member_fqn_of(r)
        if not key or not fqn:
            continue
        entry = tags.setdefault(key, {"tag_key": key, "allowed_values": [], "members": []})
        entry["members"].append({"fqn": fqn, "asset_type": "table"})

    out_tags = []
    for key, t in sorted(tags.items()):
        seen: set[str] = set()
        members = []
        for m in t["members"]:
            if m["fqn"] not in seen:
                seen.add(m["fqn"])
                members.append(m)
        out_tags.append({
            "tag_key": key,
            "allowed_values": t["allowed_values"],
            "assignment_count": len(members),
            "members": members,
        })
    return {"tags": out_tags, "as_of": as_of}


# ─────────────────────────────────────────────────────────────────────────
# Domain / sub-domain classification (from taxonomy.py) — the `/` convention
# ─────────────────────────────────────────────────────────────────────────


def domain_part(tag_key: str) -> str:
    return tag_key.split("/", 1)[0]


def subdomain_part(tag_key: str) -> str | None:
    return tag_key.split("/", 1)[1] if "/" in tag_key else None


def is_subdomain_key(tag_key: str) -> bool:
    return "/" in tag_key


def domain_keys(all_keys: list[str]) -> set[str]:
    """Top-level keys that act as domains: those with ≥1 sub-domain child."""
    parents: set[str] = set()
    for k in all_keys:
        if "/" in k:
            parents.add(domain_part(k))
    return parents


def acts_as_domain(tag_key: str, all_keys: list[str]) -> bool:
    return "/" not in tag_key and tag_key in domain_keys(all_keys)


def acts_as_subdomain(tag_key: str) -> bool:
    return is_subdomain_key(tag_key)


def _member_dicts(raw: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [{"fqn": m["fqn"], "asset_type": m.get("asset_type", "table")} for m in raw]


def build_taxonomy_dict(
    graph: dict[str, Any],
    metric_views: list[str],
    genie_agents: list[str],
) -> dict[str, Any]:
    """Assemble the Domain → Sub-Domain tree + ungrouped bucket as a plain dict
    matching the ``OntologyTaxonomy`` JSON shape (Phase-1 §4)."""
    tags = {t["tag_key"]: t for t in graph.get("tags", [])}
    all_keys = list(tags.keys())
    doms = domain_keys(all_keys)

    tagged_fqns: set[str] = set()
    domains: list[dict[str, Any]] = []
    for dkey in sorted(doms):
        direct_raw = tags.get(dkey, {}).get("members", [])
        for m in direct_raw:
            tagged_fqns.add(m["fqn"])

        subdomains: list[dict[str, Any]] = []
        for skey in sorted(k for k in all_keys if "/" in k and domain_part(k) == dkey):
            sraw = tags[skey].get("members", [])
            for m in sraw:
                tagged_fqns.add(m["fqn"])
            sval = subdomain_part(skey) or skey
            subdomains.append({
                "tag_value": sval,
                "name": sval,
                "member_count": len(sraw),
                "members": _member_dicts(sraw),
            })

        member_count = len(direct_raw) + sum(s["member_count"] for s in subdomains)
        domains.append({
            "tag_key": dkey,
            "name": dkey,
            "member_count": member_count,
            "subdomains": subdomains,
            "members": _member_dicts(direct_raw),
        })

    ungrouped = {
        "metric_views": [
            {"fqn": fqn, "asset_type": "metric_view"}
            for fqn in metric_views
            if fqn not in tagged_fqns
        ],
        "genie_agents": [
            {"fqn": a, "asset_type": "genie_agent"}
            for a in genie_agents
            if a not in tagged_fqns
        ],
    }

    as_of = graph.get("as_of") or datetime.now(timezone.utc).isoformat()
    return {"domains": domains, "ungrouped": ungrouped, "as_of": as_of}


def governed_tag_rows(graph: dict[str, Any]) -> list[dict[str, Any]]:
    """Per-tag GovernedTag fields (the 17.0c table rows / genie_ont_tag_graph rows)."""
    all_keys = [t["tag_key"] for t in graph.get("tags", [])]
    return [
        {
            "tag_key": t["tag_key"],
            "allowed_values": list(t.get("allowed_values") or []),
            "assignment_count": int(t.get("assignment_count") or 0),
            "acts_as_domain": acts_as_domain(t["tag_key"], all_keys),
            "acts_as_subdomain": acts_as_subdomain(t["tag_key"]),
        }
        for t in graph.get("tags", [])
    ]


# ─────────────────────────────────────────────────────────────────────────
# Dedupe + cleanup (from dedupe.py) — exact + fuzzy, NO embeddings
# ─────────────────────────────────────────────────────────────────────────

_NEAR_EMPTY_FLOOR = 2
_DEPRECATED_VALUES = {"deprecated", "retired", "legacy"}
_DEPRECATED_KEY_RE = re.compile(r"(^|[_/-])(deprecated|legacy|retired)$", re.IGNORECASE)


def _tokens(tag_key: str) -> list[str]:
    raw = [t for t in re.split(r"[^0-9a-zA-Z]+", tag_key) if t]
    return [_singularize(t.casefold()) for t in raw]


def _singularize(token: str) -> str:
    if len(token) > 3 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 3 and token.endswith("s") and not token.endswith("ss"):
        return token[:-1]
    return token


def token_set_sig(tag_key: str) -> str:
    return "|".join(sorted(_tokens(tag_key)))


def collision_kind(members: list[str]) -> str:
    lowered = {m.casefold() for m in members}
    if len(lowered) == 1:
        return "exact" if len(set(members)) == 1 else "fuzzy_case"
    singular_whole = {" ".join(_tokens(m)) for m in members}
    if len(singular_whole) == 1:
        return "fuzzy_plural"
    return "fuzzy_token"


def find_collisions_dict(graph: dict[str, Any]) -> list[dict[str, Any]]:
    """Group near-duplicate tag keys; one collision per group of ≥2 keys."""
    counts = {t["tag_key"]: int(t.get("assignment_count") or 0) for t in graph.get("tags", [])}
    groups: dict[str, list[str]] = {}
    for key in counts:
        groups.setdefault(token_set_sig(key), []).append(key)

    collisions: list[dict[str, Any]] = []
    for _, members in sorted(groups.items()):
        if len(members) < 2:
            continue
        members = sorted(members)
        canonical = sorted(members, key=lambda k: (-counts[k], len(k), k))[0]
        others = [m for m in members if m != canonical]
        suggestion = (
            f"reuse `{canonical}` instead of creating " + ", ".join(f"`{o}`" for o in others)
        )
        collisions.append({
            "kind": collision_kind(members),
            "members": members,
            "suggestion": suggestion,
        })
    return collisions


def _is_deprecated(tag: dict[str, Any]) -> bool:
    if _DEPRECATED_KEY_RE.search(tag.get("tag_key", "")):
        return True
    return any(str(v).casefold() in _DEPRECATED_VALUES for v in tag.get("allowed_values", []))


def find_cleanup_dict(graph: dict[str, Any]) -> list[dict[str, Any]]:
    """Flag orphan / near-empty / deprecated-but-assigned governed tags."""
    out: list[dict[str, Any]] = []
    for t in sorted(graph.get("tags", []), key=lambda x: x["tag_key"]):
        key = t["tag_key"]
        count = int(t.get("assignment_count") or 0)
        if _is_deprecated(t) and count > 0:
            out.append({
                "tag_key": key,
                "flag": "deprecated_but_assigned",
                "detail": f"deprecated governed tag still assigned to {count} asset(s)",
            })
            continue
        if count == 0:
            out.append({
                "tag_key": key,
                "flag": "orphan",
                "detail": "governed tag with no in-scope assignments",
            })
        elif count < _NEAR_EMPTY_FLOOR:
            out.append({
                "tag_key": key,
                "flag": "near_empty",
                "detail": f"only {count} in-scope assignment(s)",
            })
    return out


# ─────────────────────────────────────────────────────────────────────────
# Phase 3a: identity-map + embedding-backed collision assembly (pure).
# Duck-typed on ER verdicts (``.members``/``.verdict``/``.method``/``.score``/
# ``.reason``/``.canonical_id``) so this module never imports ``er`` (no cycle).
# ─────────────────────────────────────────────────────────────────────────


def _collision_kind_for_method(members: list[str], method: str) -> str:
    """Map an ER merge to a frozen ``CollisionKind`` (TagLens contract stays byte-
    identical). String/exact merges keep their precise string kind; semantic
    (embedding/LLM) merges surface as the loosest existing kind, ``fuzzy_token``."""
    if method in ("exact", "string"):
        return collision_kind(members)
    return "fuzzy_token"


def collisions_from_er_verdicts(verdicts: list[Any], counts: dict[str, int]) -> list[dict[str, Any]]:
    """Convert ER merge verdicts (tag members) into TagCollision-shaped dicts.

    Superset of the Phase-2 string-only ``find_collisions_dict``: string/exact
    merges reproduce the same groups; embedding/LLM merges add new (semantic) ones.
    ``kind`` stays within the frozen 4-value vocabulary — content enriched, shape
    unchanged.
    """
    out: list[dict[str, Any]] = []
    for v in verdicts:
        members = sorted(getattr(v, "members", ()) or ())
        if getattr(v, "verdict", None) != "merge" or len(members) < 2:
            continue
        canonical = sorted(members, key=lambda k: (-int(counts.get(k, 0)), len(k), k))[0]
        others = [m for m in members if m != canonical]
        suggestion = f"reuse `{canonical}` instead of creating " + ", ".join(f"`{o}`" for o in others)
        out.append({
            "kind": _collision_kind_for_method(members, getattr(v, "method", "string")),
            "members": members,
            "suggestion": suggestion,
        })
    return out


def identity_map_rows(
    verdicts: list[Any],
    *,
    workspace_id: str,
    run_id: str,
    as_of: str,
    member_kind_by_ref: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Expand ER verdicts into per-member ``genie_ont_identity`` rows.

    One row per (workspace_id, canonical_id, member_ref) — the derived PK, so the
    idempotent MERGE never duplicates and a member that left a group is deleted.
    """
    kinds = member_kind_by_ref or {}
    rows: list[dict[str, Any]] = []
    for v in verdicts:
        for ref in getattr(v, "members", ()) or ():
            rows.append({
                "workspace_id": workspace_id,
                "canonical_id": getattr(v, "canonical_id", ""),
                "member_ref": ref,
                "member_kind": kinds.get(ref, "tag"),
                "verdict": getattr(v, "verdict", "distinct"),
                "method": getattr(v, "method", "string"),
                "score": float(getattr(v, "score", 0.0) or 0.0),
                "reason": getattr(v, "reason", None),
                "run_id": run_id,
                "as_of": as_of,
            })
    return rows
