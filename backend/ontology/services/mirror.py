"""Lakebase mirror reads for the ontology snapshots (Phase 2).

Mirrors ``backend/services/gso_lakebase.py`` exactly — the one read interface,
**Delta-via-SQL-warehouse now, auto-upgrading to Lakebase synced tables later**.
Today ``gso_lakebase._SYNCED_TABLES_ENABLED`` is ``False``, so the synced-table
(Postgres) path is off and reads fall through to the source Delta tables via the
SQL warehouse (SP). When the flag flips and the three ``genie_ont_*`` snapshot
tables are provisioned as synced tables (registered in
``scripts/setup_synced_tables.py``), the same functions read Postgres and the
routers never see the difference.

Do NOT invent a new read path. These are read-only; no UC writes here.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import backend.services.gso_lakebase as _gso
from backend.services.auth import get_service_principal_client
from genie_space_optimizer.ontology import transforms

logger = logging.getLogger(__name__)


def _gso_fqn(table: str) -> str:
    catalog = os.environ.get("GSO_CATALOG", "")
    schema = os.environ.get("GSO_SCHEMA", "genie_space_optimizer")
    return f"{catalog}.{schema}.{table}"


def _synced_pool():
    """The Postgres synced-table pool, or None (synced disabled today — mirrors
    the exact gso_lakebase gate so both light up together on the flip)."""
    return _gso._get_pool()


def _delta_query(sql: str, params: list | None = None) -> list[dict[str, Any]]:
    """Read a snapshot Delta table via the SQL warehouse as the SP (best-effort).

    Returns [] on any failure (no warehouse, table absent, permission) so the
    reader swap degrades to the Phase-1 live path — never blocks or raises.
    """
    warehouse_id = os.environ.get("SQL_WAREHOUSE_ID", "").strip()
    if not warehouse_id:
        return []
    from databricks.sdk.service.sql import StatementState

    try:
        client = get_service_principal_client()
        resp = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=sql, parameters=params or [], wait_timeout="30s",
        )
        statement_id = resp.statement_id if resp else None
        deadline = time.monotonic() + 40
        while resp and resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
            if time.monotonic() > deadline or not statement_id:
                return []
            time.sleep(1.0)
            resp = client.statement_execution.get_statement(statement_id=statement_id)
        if resp is None or resp.status is None or resp.status.state != StatementState.SUCCEEDED:
            return []
        if not resp.result or not resp.result.data_array:
            return []
        schema = resp.manifest.schema if resp.manifest else None
        if schema is None or schema.columns is None:
            return []
        cols = [c.name for c in schema.columns]
        return [{cols[i]: row[i] for i in range(len(cols))} for row in resp.result.data_array]
    except Exception as e:  # noqa: BLE001 — mirror never raises; degrade to live
        logger.info("mirror delta read failed: %s", e)
        return []


def _as_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(v) for v in parsed]
        except (ValueError, TypeError):
            pass
        s = value.strip().strip("[]")
        return [p.strip().strip('"').strip("'") for p in s.split(",") if p.strip()]
    return []


async def latest_run(metastore_id: str) -> dict[str, Any] | None:
    """Most recent run header for a metastore (any state), or None."""
    pool = _synced_pool()
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT * FROM "{_gso._GSO_PG_SCHEMA}"."genie_ont_runs_synced" '
                    "WHERE metastore_id = $1 ORDER BY started_at DESC LIMIT 1",
                    metastore_id,
                )
                return dict(row) if row else None
        except Exception:
            logger.info("mirror synced read failed for genie_ont_runs", exc_info=True)
            return None
    import asyncio
    rows = await asyncio.to_thread(
        _delta_query,
        f"SELECT * FROM {_gso_fqn('genie_ont_runs')} "
        f"WHERE metastore_id = '{metastore_id}' ORDER BY started_at DESC LIMIT 1",
    )
    return rows[0] if rows else None


async def latest_succeeded_run(metastore_id: str) -> dict[str, Any] | None:
    """Most recent *succeeded* run header (backs mirror freshness), or None."""
    pool = _synced_pool()
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT * FROM "{_gso._GSO_PG_SCHEMA}"."genie_ont_runs_synced" '
                    "WHERE metastore_id = $1 AND state = 'succeeded' ORDER BY as_of DESC LIMIT 1",
                    metastore_id,
                )
                return dict(row) if row else None
        except Exception:
            logger.info("mirror synced read failed for genie_ont_runs (succeeded)", exc_info=True)
            return None
    import asyncio
    rows = await asyncio.to_thread(
        _delta_query,
        f"SELECT * FROM {_gso_fqn('genie_ont_runs')} "
        f"WHERE metastore_id = '{metastore_id}' AND state = 'succeeded' ORDER BY as_of DESC LIMIT 1",
    )
    return rows[0] if rows else None


async def read_taxonomy_tree(metastore_id: str) -> dict[str, Any] | None:
    """The serialized taxonomy tree for a metastore (OntologyTaxonomy JSON), or None."""
    pool = _synced_pool()
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT tree FROM "{_gso._GSO_PG_SCHEMA}"."genie_ont_taxonomy_snapshot_synced" '
                    "WHERE metastore_id = $1",
                    metastore_id,
                )
            tree = row["tree"] if row else None
        except Exception:
            logger.info("mirror synced read failed for taxonomy", exc_info=True)
            return None
    else:
        import asyncio
        rows = await asyncio.to_thread(
            _delta_query,
            f"SELECT tree FROM {_gso_fqn('genie_ont_taxonomy_snapshot')} WHERE metastore_id = '{metastore_id}'",
        )
        tree = rows[0].get("tree") if rows else None
    if not tree:
        return None
    try:
        return json.loads(tree)
    except (ValueError, TypeError):
        return None


async def read_tag_graph(metastore_id: str) -> dict[str, Any] | None:
    """Reconstruct the tag-graph structure from the mirror rows, or None.

    Members are not stored per tag (they live in the taxonomy tree), so the
    reconstructed graph carries empty ``members`` — the tags-lens transforms
    (governed_tag_rows / collisions / cleanup) do not need them, so the route
    output is identical whether the graph came from the mirror or the live path.
    """
    pool = _synced_pool()
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                rows = [
                    dict(r) for r in await conn.fetch(
                        f'SELECT tag_key, allowed_values, assignment_count, dedupe_verdicts, as_of '
                        f'FROM "{_gso._GSO_PG_SCHEMA}"."genie_ont_tag_graph_synced" WHERE metastore_id = $1',
                        metastore_id,
                    )
                ]
        except Exception:
            logger.info("mirror synced read failed for tag_graph", exc_info=True)
            return None
    else:
        import asyncio
        rows = await asyncio.to_thread(
            _delta_query,
            f"SELECT tag_key, allowed_values, assignment_count, dedupe_verdicts, as_of "
            f"FROM {_gso_fqn('genie_ont_tag_graph')} WHERE metastore_id = '{metastore_id}'",
        )
    if not rows:
        return None
    as_of = max((str(r.get("as_of") or "") for r in rows), default="")
    tags = []
    for r in rows:
        if not r.get("tag_key"):
            continue
        tag: dict[str, Any] = {
            "tag_key": r.get("tag_key"),
            "allowed_values": _as_list(r.get("allowed_values")),
            "assignment_count": int(r.get("assignment_count") or 0),
            "members": [],
        }
        # Phase-3a: the embedding-backed per-tag dedupe verdicts (JSON), when present,
        # so the tags route can surface enriched collisions through the frozen contract.
        verdicts = r.get("dedupe_verdicts")
        if verdicts:
            try:
                tag["dedupe_verdicts"] = json.loads(verdicts) if isinstance(verdicts, str) else verdicts
            except (ValueError, TypeError):
                pass
        tags.append(tag)
    tags.sort(key=lambda t: t["tag_key"])
    return {"tags": tags, "as_of": as_of}


# ── Phase 3d: ranked draft readers (surfaced-only, tiered) ──────────────────
# ``read_domain_drafts`` / ``read_page_drafts`` read the L6-scored proposal rows
# from the mirror, keep only ``surfaced=true`` rows (dismissed / blocked /
# sub-threshold are excluded by the wheel), tier them via the SHARED
# ``transforms.tier_of`` (so mirror order == the wheel's order), and assemble the
# zero-burden ``why``/``reason`` strings + evidence chips SERVER-SIDE (MV-D23 — the
# card assembles nothing). Never raises: any failure degrades to [] so the route
# returns a typed empty payload (MV-D43).


async def _read_table(table: str, metastore_id: str) -> list[dict[str, Any]]:
    """Read all rows of an ontology proposal table for a metastore (synced pool when
    live, else Delta-via-warehouse). Returns [] on any failure (degrade-not-hang)."""
    pool = _synced_pool()
    if pool is not None:
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    f'SELECT * FROM "{_gso._GSO_PG_SCHEMA}"."{table}_synced" WHERE metastore_id = $1',
                    metastore_id,
                )
            return [dict(r) for r in rows]
        except Exception:
            logger.info("mirror synced read failed for %s", table, exc_info=True)
            return []
    import asyncio

    return await asyncio.to_thread(
        _delta_query,
        f"SELECT * FROM {_gso_fqn(table)} WHERE metastore_id = '{metastore_id}'",
    )


def _evidence_of(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("evidence")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except (ValueError, TypeError):
            return {}
    return {}


def _tier_of_row(row: dict[str, Any], evidence: dict[str, Any]) -> str | None:
    """The served tier — the wheel's coverage-capped tier from evidence when present,
    else the pure ``transforms.tier_of(score)`` (the shared thresholds). Lowercase
    ``DraftTier`` or None (sub-threshold, never served)."""
    rank = evidence.get("rank") or {}
    tier = rank.get("tier")
    if tier in ("high", "medium", "low"):
        return tier
    try:
        return transforms.tier_of(float(row.get("score") or 0.0))
    except (TypeError, ValueError):
        return None


_TIER_RANK = {"high": 3, "medium": 2, "low": 1}


def _factor_present(evidence: dict[str, Any], name: str) -> bool:
    factors = (evidence.get("rank") or {}).get("factors") or {}
    return bool((factors.get(name) or {}).get("present"))


def _domain_chips(evidence: dict[str, Any], conflict_tag: str | None) -> list[dict[str, str]]:
    """Evidence chips for a Domain draft (MV-D35: facts lead, no rendered percent)."""
    chips: list[dict[str, str]] = []
    if conflict_tag:
        chips.append({"label": f"Overlaps the “{conflict_tag}” tag", "kind": "conflict"})
    if _factor_present(evidence, "centrality"):
        chips.append({"label": "Central to how the data connects", "kind": "centrality"})
    if _factor_present(evidence, "usage"):
        chips.append({"label": "Actively queried", "kind": "usage"})
    if _factor_present(evidence, "governance"):
        chips.append({"label": "Built on governed data", "kind": "governance"})
    spine = evidence.get("shared_spine") or []
    if len(spine) >= 2:
        chips.append({"label": f"Shares a spine across {len(spine)} tables", "kind": "centrality"})
    return chips


def _domain_why(kind: str, name: str, tag_decision: str, member_count: int, conflict_tag: str | None) -> str:
    if tag_decision == "reassign" and conflict_tag:
        return (
            f"Some assets under the “{conflict_tag}” tag look like they belong with "
            f"“{name}” instead — worth confirming which grouping is right."
        )
    if tag_decision == "reuse":
        return f"These {member_count} assets already share a tag — making “{name}” explicit keeps the estate organized."
    return f"“{name}” groups {member_count} related assets that aren't organized under a shared domain yet."


def _assemble_domain_draft(
    row: dict[str, Any],
    members: list[dict[str, str]],
    subdomain_names: list[str],
    tier: str,
) -> dict[str, Any]:
    evidence = _evidence_of(row)
    kind = transforms.proposal_kind_of(row)
    conflict = (evidence.get("conflict") or {}).get("existing_tag")
    conflict_tag = str(conflict) if conflict else None
    name = str(row.get("name") or row.get("tag_value") or "")
    tag_decision = str(row.get("tag_decision") or "create")
    return {
        "proposal_id": str(row.get("domain_id") or ""),
        "kind": kind,
        "name": name,
        "description": str(row.get("description") or ""),
        "tag_decision": tag_decision,
        "conflict_tag": conflict_tag,
        "subdomains": subdomain_names,
        "members": members,
        "why": _domain_why(kind, name, tag_decision, len(members), conflict_tag),
        "evidence": _domain_chips(evidence, conflict_tag),
        "tier": tier,
    }


async def read_domain_drafts(metastore_id: str) -> list[dict[str, Any]]:
    """Surfaced, tiered Domain / Sub-Domain / reassign drafts for a metastore, ordered
    HIGH → LOW. Fully-assembled ``DomainDraft`` dicts (MV-D23). [] on any failure."""
    domain_rows = await _read_table("genie_ont_domains", metastore_id)
    if not domain_rows:
        return []
    member_rows = await _read_table("genie_ont_members", metastore_id)
    members_by_domain: dict[str, list[dict[str, str]]] = {}
    for m in member_rows:
        did = str(m.get("domain_id") or "")
        members_by_domain.setdefault(did, []).append(
            {"fqn": str(m.get("asset_fqn") or ""), "asset_type": str(m.get("asset_type") or "table")}
        )
    # Sub-domain names hang off their parent (parent_id → child names).
    subdomains_by_parent: dict[str, list[str]] = {}
    for r in domain_rows:
        parent = r.get("parent_id")
        if parent:
            subdomains_by_parent.setdefault(str(parent), []).append(str(r.get("name") or ""))

    drafts: list[dict[str, Any]] = []
    for row in domain_rows:
        evidence = _evidence_of(row)
        if not evidence.get("surfaced"):
            continue
        tier = _tier_of_row(row, evidence)
        if tier is None:
            continue
        did = str(row.get("domain_id") or "")
        drafts.append(
            _assemble_domain_draft(
                row,
                sorted(members_by_domain.get(did, []), key=lambda m: m["fqn"]),
                sorted(subdomains_by_parent.get(did, [])),
                tier,
            )
        )
    drafts.sort(key=lambda d: (-_TIER_RANK.get(d["tier"], 0), d["name"], d["proposal_id"]))
    return drafts


_PAGE_REASON = {
    "Routing": 'Answer “{concept}” from the governed metric view instead of hand-written SQL.',
    "Disambiguation": '“{concept}” means different things across the estate — confirm which is meant before answering.',
    "Guardrail": '“{concept}” is easy to compute the wrong way — this pins the correct method.',
    "Taxonomy": '“{concept}” uses coded values — this decodes them so answers read in plain language.',
}


def _page_concept(title: str) -> str:
    """The concept a Page title names, with the ``[Archetype]`` prefix stripped."""
    t = str(title or "").strip()
    if t.startswith("[") and "]" in t:
        return t[t.index("]") + 1:].strip() or t
    return t


def _page_chips(evidence: dict[str, Any], certify: bool) -> list[dict[str, str]]:
    chips: list[dict[str, str]] = []
    corr = evidence.get("corroboration")
    try:
        corr_n = int(corr) if corr is not None else 0
    except (TypeError, ValueError):
        corr_n = 0
    if corr_n >= 2:
        chips.append({"label": f"Backed by {corr_n} sources", "kind": "corroboration"})
    if str(evidence.get("status") or "") == "CONFLICT":
        chips.append({"label": "Conflicts with an existing instruction", "kind": "conflict"})
    if _factor_present(evidence, "governance"):
        chips.append({"label": "Built on governed data", "kind": "governance"})
    if _factor_present(evidence, "usage"):
        chips.append({"label": "Actively queried", "kind": "usage"})
    if certify:
        chips.append({"label": "Ready to certify", "kind": "governance"})
    return chips


def _assemble_page_draft(row: dict[str, Any], tier: str) -> dict[str, Any]:
    evidence = _evidence_of(row)
    archetype = str(row.get("archetype") or "Routing")
    concept = _page_concept(str(row.get("title") or ""))
    certify = bool(row.get("certify"))
    reason = _PAGE_REASON.get(archetype, "{concept}").format(concept=concept)
    return {
        "proposal_id": str(row.get("page_id") or ""),
        "archetype": archetype,
        "title": str(row.get("title") or ""),
        "reason": reason,
        "body": str(row.get("body") or ""),
        "synonyms": _as_list(row.get("synonyms")),
        "related_fqns": _as_list(row.get("related_fqns")),
        "source_fqns": _as_list(row.get("source_fqns")),
        "certify": certify,
        "evidence": _page_chips(evidence, certify),
        "tier": tier,
    }


async def read_page_drafts(metastore_id: str) -> list[dict[str, Any]]:
    """Surfaced, tiered Page drafts for a metastore, ordered HIGH → LOW. Fully-assembled
    ``PageDraft`` dicts (MV-D23). [] on any failure (degrade-not-hang)."""
    page_rows = await _read_table("genie_ont_pages", metastore_id)
    if not page_rows:
        return []
    drafts: list[dict[str, Any]] = []
    for row in page_rows:
        evidence = _evidence_of(row)
        if not evidence.get("surfaced"):
            continue
        tier = _tier_of_row(row, evidence)
        if tier is None:
            continue
        drafts.append(_assemble_page_draft(row, tier))
    drafts.sort(key=lambda d: (-_TIER_RANK.get(d["tier"], 0), d["title"], d["proposal_id"]))
    return drafts
