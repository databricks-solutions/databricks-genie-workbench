"""Join Advisor — data-grounded candidate joins as ADVICE to the optimizer.

The Semantic Blueprint's Join Advisor (v4 §7) proposes candidate joins that the
Auto-Optimize run **validates and adds itself** (via ``add_join_spec``). Nothing
in this module edits ``serialized_space``: the Workbench never makes ad-hoc Genie
Agent config edits — that is the Genie product UI's job. A seeded candidate
persists as advice (``lakebase.save_join_advice``) and is carried into the next
run as run input; the optimizer re-probes it and only adds the ones that hold.

Candidates are grounded, in increasing strength, in:
  (a) name+type matching of key-like columns shared across two tables,
  (b) declared Unity Catalog foreign keys, and
  (c) a warehouse **containment probe** — the fraction of distinct
      ``from.from_col`` values present in ``to.to_col`` — which is the score the
      inset renders as the validated/partial/unverified verdict bar.

Everything here is read-only: column metadata comes from
``system.information_schema`` and the containment probe is a single ``SELECT``
per candidate (guarded by ``sql_executor``'s read-only validator). No warehouse,
or a probe that errors, yields ``probe=None`` — honest-empty, never a silent 0.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from backend.sql_executor import execute_sql

logger = logging.getLogger(__name__)

# Cap the number of live containment probes per request. Candidate generation is
# quadratic in shared key-like columns; the probe is one warehouse round-trip
# each, so we rank and probe only the most promising handful.
_MAX_PROBES = 12
# Cap the candidate pool the UI renders even when unprobed.
_MAX_CANDIDATES = 24

# A column name is "key-like" (a plausible join key) when it ends in one of these
# suffixes or IS one of these bare names. Join keys are the only columns we ever
# propose on — this is what keeps the advisor from suggesting joins on free-text
# or measure columns.
_KEY_SUFFIXES = ("_id", "_key", "_code", "_no", "_num", "_sk", "_fk", "_pk")
_KEY_BARE = frozenset({"id", "key", "code"})


def _short_name(identifier: str) -> str:
    """Last dotted segment of a UC identifier, backticks stripped."""
    cleaned = (identifier or "").replace("`", "").strip()
    return cleaned.split(".")[-1] if cleaned else cleaned


def _fqn_parts(identifier: str) -> tuple[str, str, str] | None:
    """``(catalog, schema, table)`` for a three-part UC name, else ``None``."""
    parts = [p for p in (identifier or "").replace("`", "").strip().split(".") if p]
    return (parts[0], parts[1], parts[2]) if len(parts) == 3 else None


def is_key_like(column: str) -> bool:
    """True when ``column`` looks like a join key (suffix or bare-name heuristic)."""
    col = (column or "").strip().lower()
    if not col:
        return False
    if col in _KEY_BARE:
        return True
    return col.endswith(_KEY_SUFFIXES)


def _norm_type(type_text: str) -> str:
    """Collapse a column type to a coarse family so ``BIGINT`` matches ``INT`` and
    ``VARCHAR(64)`` matches ``STRING`` — join keys survive width/precision drift."""
    t = (type_text or "").strip().lower()
    t = re.sub(r"\(.*?\)", "", t)  # drop precision/width
    if t in {"int", "integer", "bigint", "smallint", "tinyint", "long", "short"}:
        return "int"
    if t in {"string", "varchar", "char", "text"}:
        return "string"
    if t in {"double", "float", "real", "decimal", "numeric"}:
        return "number"
    return t


def _table_identifiers(space_data: dict) -> list[str]:
    """The ``data_sources.tables[].identifier`` list, config order, de-duplicated."""
    ds = space_data.get("data_sources") if isinstance(space_data, dict) else None
    tables = (ds or {}).get("tables") if isinstance(ds, dict) else None
    out: list[str] = []
    seen: set[str] = set()
    for t in tables or []:
        ident = t.get("identifier") if isinstance(t, dict) else None
        if ident and _fqn_parts(ident) and ident.lower() not in seen:
            seen.add(ident.lower())
            out.append(ident)
    return out


def _declared_pairs(space_data: dict) -> set[frozenset[str]]:
    """Undirected ``{fromTableShort.col, toTableShort.col}`` pairs already declared
    as ``join_specs`` — so the advisor never re-proposes an existing relationship.

    Keyed on short-name.column (the alias-qualified form the ON predicate uses),
    lower-cased, so it matches the endpoints candidate generation produces."""
    instructions = space_data.get("instructions") if isinstance(space_data, dict) else None
    join_specs = (instructions or {}).get("join_specs") if isinstance(instructions, dict) else None
    pairs: set[frozenset[str]] = set()
    _eq = re.compile(r"([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s*=\s*([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)")
    for j in join_specs or []:
        if not isinstance(j, dict):
            continue
        sql = j.get("sql") if isinstance(j.get("sql"), list) else []
        predicate = re.sub(r"\s+", " ", (sql[0] if sql else "").replace("`", "")).strip()
        for q1, c1, q2, c2 in _eq.findall(predicate):
            pairs.add(frozenset({f"{q1.lower()}.{c1.lower()}", f"{q2.lower()}.{c2.lower()}"}))
    return pairs


def _columns_by_table(identifiers: list[str]) -> dict[str, list[dict[str, str]]]:
    """``{identifier: [{name, type}]}`` for the in-scope tables via information_schema.

    One batched query across all (catalog, schema, table) triples. Returns ``{}``
    on any failure (no warehouse, no access) — the caller degrades to honest-empty.
    """
    triples = [p for p in (_fqn_parts(i) for i in identifiers) if p]
    if not triples:
        return {}
    # Build an OR of (catalog, schema, table) equality triples, escaped.
    def esc(v: str) -> str:
        return v.replace("'", "''")

    conds = " OR ".join(
        f"(lower(table_catalog)='{esc(c.lower())}' AND lower(table_schema)='{esc(s.lower())}' "
        f"AND lower(table_name)='{esc(t.lower())}')"
        for c, s, t in triples
    )
    sql = (
        "SELECT table_catalog, table_schema, table_name, column_name, data_type "
        "FROM system.information_schema.columns "
        f"WHERE {conds} ORDER BY table_catalog, table_schema, table_name, ordinal_position"
    )
    result = execute_sql(sql)
    if result.get("error"):
        logger.info("join_advisor: column metadata query failed: %s", result["error"])
        return {}
    # Map lower-cased fqn back to the original identifier casing.
    by_lower = {f"{c}.{s}.{t}".lower(): i for i, (c, s, t) in zip(identifiers, (p for p in (_fqn_parts(i) for i in identifiers) if p))}
    out: dict[str, list[dict[str, str]]] = {}
    for row in result.get("data", []):
        if len(row) < 5:
            continue
        fqn_lower = f"{row[0]}.{row[1]}.{row[2]}".lower()
        ident = by_lower.get(fqn_lower)
        if not ident:
            continue
        out.setdefault(ident, []).append({"name": row[3], "type": row[4] or ""})
    return out


def candidate_pairs(
    columns_by_table: dict[str, list[dict[str, str]]],
    declared: set[frozenset[str]],
) -> list[dict[str, Any]]:
    """Generate name+type join candidates from key-like columns shared across tables.

    Pure and deterministic. For every unordered pair of distinct tables and every
    column name they share with a compatible coarse type AND a key-like name, emit
    one candidate. Direction (``from`` = the many/child side, ``to`` = the
    one/parent side) is inferred from the column stem: a ``customer_id`` column
    points ``to`` the table whose short name matches ``customer`` (singular or
    plural); when neither side matches, tables are ordered lexically for a stable
    result. Already-declared endpoint pairs are skipped."""
    idents = list(columns_by_table.keys())
    out: list[dict[str, Any]] = []
    for i in range(len(idents)):
        for k in range(i + 1, len(idents)):
            a, b = idents[i], idents[k]
            a_cols = {c["name"].lower(): c for c in columns_by_table[a]}
            b_cols = {c["name"].lower(): c for c in columns_by_table[b]}
            shared = sorted(set(a_cols) & set(b_cols))
            for col in shared:
                if not is_key_like(col):
                    continue
                if _norm_type(a_cols[col]["type"]) != _norm_type(b_cols[col]["type"]):
                    continue
                # Skip an already-declared endpoint pair (undirected, short-name.col).
                key = frozenset({f"{_short_name(a).lower()}.{col}", f"{_short_name(b).lower()}.{col}"})
                if key in declared:
                    continue
                frm, to = _orient(a, b, col)
                out.append({
                    "id": f"jc:{_short_name(frm).lower()}.{col}->{_short_name(to).lower()}.{col}",
                    "from": frm,
                    "fromCol": _actual_col(columns_by_table[frm], col),
                    "to": to,
                    "toCol": _actual_col(columns_by_table[to], col),
                    "rel": "N:1",
                    "match": "name-type",
                    "probe": None,
                    "note": None,
                })
    return out


def _orient(a: str, b: str, col: str) -> tuple[str, str]:
    """Decide ``(from, to)`` for a shared key column by matching the column stem
    to a table's short name; fall back to lexical order for determinism."""
    stem = re.sub(r"(_id|_key|_code|_no|_num|_sk|_fk|_pk)$", "", col.lower()).strip("_")
    a_short, b_short = _short_name(a).lower(), _short_name(b).lower()

    def matches(short: str) -> bool:
        # dim_customer / customers / customer all match stem "customer"
        base = re.sub(r"^(dim|fact|f|d)_", "", short)
        return bool(stem) and (base == stem or base == stem + "s" or base.rstrip("s") == stem)

    a_is_parent, b_is_parent = matches(a_short), matches(b_short)
    if b_is_parent and not a_is_parent:
        return a, b
    if a_is_parent and not b_is_parent:
        return b, a
    return (a, b) if a.lower() <= b.lower() else (b, a)


def _actual_col(cols: list[dict[str, str]], lower_name: str) -> str:
    """The column's real (original-case) name for a lower-cased match."""
    for c in cols:
        if c["name"].lower() == lower_name:
            return c["name"]
    return lower_name


def foreign_key_pairs(identifiers: list[str]) -> set[frozenset[str]]:
    """Declared UC foreign keys among the in-scope tables, as undirected
    ``{childShort.col, parentShort.col}`` endpoint pairs (best-effort).

    Databricks exposes informational FK constraints in
    ``system.information_schema``. A candidate coinciding with one of these is
    upgraded to ``match="fk"`` — the strongest structural grounding. Any failure
    returns an empty set; name-type candidates still stand."""
    triples = [p for p in (_fqn_parts(i) for i in identifiers) if p]
    if not triples:
        return set()

    def esc(v: str) -> str:
        return v.replace("'", "''")

    schemas = sorted({(c.lower(), s.lower()) for c, s, _ in triples})
    scope = " OR ".join(
        f"(lower(rc.constraint_catalog)='{esc(c)}' AND lower(rc.constraint_schema)='{esc(s)}')"
        for c, s in schemas
    )
    # Join referential_constraints -> child key_column_usage + parent
    # constraint_column_usage to recover (child.col) references (parent.col).
    sql = (
        "SELECT kcu.table_name AS child_table, kcu.column_name AS child_col, "
        "ccu.table_name AS parent_table, ccu.column_name AS parent_col "
        "FROM system.information_schema.referential_constraints rc "
        "JOIN system.information_schema.key_column_usage kcu "
        "  ON rc.constraint_catalog = kcu.constraint_catalog "
        "  AND rc.constraint_schema = kcu.constraint_schema "
        "  AND rc.constraint_name = kcu.constraint_name "
        "JOIN system.information_schema.constraint_column_usage ccu "
        "  ON rc.unique_constraint_catalog = ccu.constraint_catalog "
        "  AND rc.unique_constraint_schema = ccu.constraint_schema "
        "  AND rc.unique_constraint_name = ccu.constraint_name "
        f"WHERE {scope}"
    )
    result = execute_sql(sql)
    if result.get("error"):
        logger.info("join_advisor: FK discovery query failed: %s", result["error"])
        return set()
    pairs: set[frozenset[str]] = set()
    for row in result.get("data", []):
        if len(row) < 4 or not all(row):
            continue
        child_t, child_c, parent_t, parent_c = (str(v) for v in row[:4])
        pairs.add(frozenset({
            f"{child_t.lower()}.{child_c.lower()}",
            f"{parent_t.lower()}.{parent_c.lower()}",
        }))
    return pairs


def containment_sql(from_fqn: str, from_col: str, to_fqn: str, to_col: str) -> str:
    """A single read-only ``SELECT`` computing the containment probe.

    Returns ``from_distinct`` (distinct non-null ``from.from_col`` values) and
    ``matched`` (how many of those exist in ``to.to_col``). The containment ratio
    is ``matched / from_distinct``. Also returns ``to_unique`` so a fully-unique
    parent key can be rendered ``1:1`` rather than ``N:1``. Identifiers are
    backtick-quoted; ``from_col``/``to_col`` come from information_schema (never
    user free-text)."""
    def q(fqn: str) -> str:
        return ".".join(f"`{p}`" for p in fqn.replace("`", "").split("."))

    fc, tc = from_col.replace("`", ""), to_col.replace("`", "")
    return (
        "SELECT COUNT(DISTINCT f.k) AS from_distinct, "
        "COUNT(DISTINCT CASE WHEN t.k IS NOT NULL THEN f.k END) AS matched, "
        "(SELECT (COUNT(*) = COUNT(DISTINCT `" + tc + "`)) "
        f"FROM {q(to_fqn)} WHERE `{tc}` IS NOT NULL) AS to_unique "
        f"FROM (SELECT `{fc}` AS k FROM {q(from_fqn)} WHERE `{fc}` IS NOT NULL) f "
        f"LEFT JOIN (SELECT DISTINCT `{tc}` AS k FROM {q(to_fqn)} WHERE `{tc}` IS NOT NULL) t "
        "ON f.k = t.k"
    )


def _probe_candidate(cand: dict[str, Any]) -> None:
    """Run the containment probe for one candidate, mutating ``probe``/``rel`` in
    place. Best-effort: any failure leaves ``probe=None`` (honest-unverified)."""
    try:
        sql = containment_sql(cand["from"], cand["fromCol"], cand["to"], cand["toCol"])
        result = execute_sql(sql)
        if result.get("error") or not result.get("data"):
            return
        row = result["data"][0]
        from_distinct = int(row[0] or 0)
        matched = int(row[1] or 0)
        to_unique = bool(row[2]) if len(row) > 2 and row[2] is not None else False
        if from_distinct <= 0:
            return
        cand["probe"] = round(matched / from_distinct, 4)
        # A fully-unique key on BOTH sides is 1:1; otherwise many-to-one.
        if to_unique and cand["probe"] >= 0.99:
            cand["rel"] = "1:1"
    except Exception:
        logger.info("join_advisor: containment probe failed for %s", cand.get("id"), exc_info=True)


def _probe_rank(cand: dict[str, Any]) -> tuple:
    """Ranking key for choosing which candidates to spend a probe on: FK first,
    then key-ish stems (``*_id`` before bare), then a stable id order."""
    match_rank = 0 if cand["match"] == "fk" else 1
    id_rank = 0 if str(cand["fromCol"]).lower().endswith("_id") else 1
    return (match_rank, id_rank, cand["id"])


def discover_candidates(space_data: dict) -> dict[str, Any]:
    """Discover Join Advisor candidates for a space (the ``/join-candidates`` core).

    Pipeline: read in-scope table columns → generate name+type candidates →
    upgrade FK-backed ones to ``match="fk"`` → containment-probe the top
    ``_MAX_PROBES``. Returns ``{"status", "candidates"}`` where ``status`` is the
    honest-empty discriminator the inset renders. Never raises — degrades to a
    truthful empty/unverified state."""
    identifiers = _table_identifiers(space_data)
    if len(identifiers) < 2:
        return {"status": "fully_connected" if identifiers else "no_candidates", "candidates": []}

    columns_by_table = _columns_by_table(identifiers)
    if not columns_by_table:
        return {"status": "no_warehouse", "candidates": []}

    declared = _declared_pairs(space_data)
    cands = candidate_pairs(columns_by_table, declared)
    if not cands:
        return {"status": "fully_connected", "candidates": []}

    # Upgrade FK-backed candidates to match="fk" (strongest grounding).
    fks = foreign_key_pairs(identifiers)
    if fks:
        for c in cands:
            key = frozenset({
                f"{_short_name(c['from']).lower()}.{c['fromCol'].lower()}",
                f"{_short_name(c['to']).lower()}.{c['toCol'].lower()}",
            })
            if key in fks:
                c["match"] = "fk"

    cands.sort(key=_probe_rank)
    cands = cands[:_MAX_CANDIDATES]

    probed_any = False
    for c in cands[:_MAX_PROBES]:
        _probe_candidate(c)
        if c["probe"] is not None:
            probed_any = True

    # Candidates exist but not one could be probed (warehouse down / no access):
    # honest "unverified" state rather than pretending containment is 0.
    if not probed_any:
        return {"status": "no_warehouse", "candidates": cands}
    return {"status": "ok", "candidates": cands}
