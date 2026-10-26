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

Everything here is read-only: table columns and foreign keys come from the Unity
Catalog Tables API (one ``tables.get`` per table, as the requesting user — no
warehouse involved), and the containment probe is a single ``SELECT`` per
candidate (guarded by ``sql_executor``'s read-only validator). No warehouse, or a
probe that errors, yields ``probe=None`` — honest-empty, never a silent 0.
"""

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from databricks.sdk.service.catalog import TableInfo

from backend.services.auth import get_workspace_client
from backend.sql_executor import execute_sql

logger = logging.getLogger(__name__)

# Cap the number of live containment probes per request. Candidate generation is
# quadratic in shared key-like columns; the probe is one warehouse round-trip
# each, so we rank and probe only the most promising handful.
_MAX_PROBES = 12
# Cap the candidate pool the UI renders even when unprobed.
_MAX_CANDIDATES = 24
# Parallel ``tables.get`` calls per request (one per in-scope table).
_UC_FETCH_WORKERS = 8

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


def _fqn_key(identifier: str) -> str:
    """Case- and quote-insensitive comparison key for a UC identifier."""
    return (identifier or "").replace("`", "").strip().lower()


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


def _join_side_identifier(spec: dict, side: str) -> str:
    """A join spec side's table identifier — the ``left``/``right`` dict, or the
    legacy ``*_table_name`` field — read the way the optimizer's applier reads it."""
    entry = spec.get(side)
    if isinstance(entry, dict):
        return str(entry.get("identifier") or "")
    return str(spec.get(f"{side}_table_name") or "")


def _declared_pairs(space_data: dict) -> set[frozenset[str]]:
    """Undirected ``{fqn_a, fqn_b}`` TABLE pairs already joined by a ``join_spec``,
    as ``_fqn_key`` values — so the advisor never proposes a join on them.

    Keyed on the table pair, not the column pair, to match the optimizer: its
    applier skips ``add_join_spec`` for any pair already joined, so a second key
    between the same two tables could never land. Sides come from the spec's
    ``left``/``right`` identifiers. The ON predicate's qualifiers are aliases
    (``dim_date_2``), so they are only a fallback for a spec without identifiers,
    resolved through the in-scope tables' short names."""
    instructions = space_data.get("instructions") if isinstance(space_data, dict) else None
    join_specs = (instructions or {}).get("join_specs") if isinstance(instructions, dict) else None
    by_short = {_short_name(i).lower(): _fqn_key(i) for i in _table_identifiers(space_data)}
    pairs: set[frozenset[str]] = set()
    _eq = re.compile(r"([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s*=\s*([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)")
    for j in join_specs or []:
        if not isinstance(j, dict):
            continue
        left, right = _join_side_identifier(j, "left"), _join_side_identifier(j, "right")
        if left and right:
            pairs.add(frozenset({_fqn_key(left), _fqn_key(right)}))
            continue
        sql = j.get("sql") if isinstance(j.get("sql"), list) else []
        predicate = re.sub(r"\s+", " ", (sql[0] if sql else "").replace("`", "")).strip()
        for q1, _c1, q2, _c2 in _eq.findall(predicate):
            a, b = by_short.get(q1.lower()), by_short.get(q2.lower())
            if a and b:
                pairs.add(frozenset({a, b}))
    return pairs


def _table_infos(identifiers: list[str]) -> dict[str, TableInfo]:
    """``{identifier: TableInfo}`` for the in-scope tables via the UC Tables API.

    One ``tables.get`` per table, fetched in parallel with the requesting user's
    client (resolved here, on the calling thread — the per-request client lives
    in a contextvar the pool threads do not inherit). A table that cannot be read
    (missing, no access) is skipped; this never raises."""
    if not identifiers:
        return {}
    try:
        client = get_workspace_client()
    except Exception:
        logger.info("join_advisor: no workspace client for table metadata", exc_info=True)
        return {}

    def fetch(ident: str) -> tuple[str, TableInfo | None]:
        try:
            return ident, client.tables.get(full_name=ident.replace("`", ""))
        except Exception as exc:
            logger.info("join_advisor: tables.get failed for %s: %s", ident, exc)
            return ident, None

    with ThreadPoolExecutor(max_workers=min(_UC_FETCH_WORKERS, len(identifiers))) as pool:
        fetched = list(pool.map(fetch, identifiers))
    return {ident: info for ident, info in fetched if info is not None}


def _columns_by_table(infos: dict[str, TableInfo]) -> dict[str, list[dict[str, str]]]:
    """``{identifier: [{name, type}]}`` from each table's UC column metadata.

    ``type`` is the column's type text (``bigint``, ``decimal(10,2)``), falling
    back to its type name (``LONG``); ``_norm_type`` folds both into one family.
    Tables with no readable columns are dropped — the caller degrades to
    honest-empty when none remain."""
    out: dict[str, list[dict[str, str]]] = {}
    for ident, info in infos.items():
        cols = []
        for c in info.columns or []:
            if not c.name:
                continue
            type_name = getattr(c.type_name, "value", c.type_name) or ""
            cols.append({"name": c.name, "type": c.type_text or str(type_name)})
        if cols:
            out[ident] = cols
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
    result. A table pair already joined (``declared``, from ``_declared_pairs``)
    is skipped entirely."""
    idents = list(columns_by_table.keys())
    out: list[dict[str, Any]] = []
    for i in range(len(idents)):
        for k in range(i + 1, len(idents)):
            a, b = idents[i], idents[k]
            if frozenset({_fqn_key(a), _fqn_key(b)}) in declared:
                continue
            a_cols = {c["name"].lower(): c for c in columns_by_table[a]}
            b_cols = {c["name"].lower(): c for c in columns_by_table[b]}
            shared = sorted(set(a_cols) & set(b_cols))
            for col in shared:
                if not is_key_like(col):
                    continue
                if _norm_type(a_cols[col]["type"]) != _norm_type(b_cols[col]["type"]):
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


def foreign_key_pairs(infos: dict[str, TableInfo]) -> set[frozenset[str]]:
    """Declared UC foreign keys on the in-scope tables, as undirected
    ``{childShort.col, parentShort.col}`` endpoint pairs.

    Read from each child table's informational ``table_constraints``. A candidate
    coinciding with one of these is upgraded to ``match="fk"`` — the strongest
    structural grounding. Composite keys are skipped: a candidate joins one
    column on each side."""
    pairs: set[frozenset[str]] = set()
    for ident, info in infos.items():
        for constraint in info.table_constraints or []:
            fk = constraint.foreign_key_constraint
            if not fk or not fk.parent_table:
                continue
            child_cols, parent_cols = fk.child_columns or [], fk.parent_columns or []
            if len(child_cols) != 1 or len(parent_cols) != 1:
                continue
            pairs.add(frozenset({
                f"{_short_name(ident).lower()}.{child_cols[0].lower()}",
                f"{_short_name(fk.parent_table).lower()}.{parent_cols[0].lower()}",
            }))
    return pairs


def containment_sql(from_fqn: str, from_col: str, to_fqn: str, to_col: str) -> str:
    """A single read-only ``SELECT`` computing the containment probe.

    Returns ``from_distinct`` (distinct non-null ``from.from_col`` values) and
    ``matched`` (how many of those exist in ``to.to_col``). The containment ratio
    is ``matched / from_distinct``. Also returns ``to_unique`` so a fully-unique
    parent key can be rendered ``1:1`` rather than ``N:1``. Identifiers are
    backtick-quoted; ``from_col``/``to_col`` come from UC column metadata (never
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

    Pipeline: read in-scope table metadata once from Unity Catalog (columns and
    FK constraints) → generate name+type candidates → upgrade FK-backed ones to
    ``match="fk"`` → containment-probe the top ``_MAX_PROBES``. Returns
    ``{"status", "candidates"}`` where ``status`` is the honest-empty
    discriminator the inset renders. Never raises — degrades to a truthful
    empty/unverified state."""
    identifiers = _table_identifiers(space_data)
    if len(identifiers) < 2:
        return {"status": "fully_connected" if identifiers else "no_candidates", "candidates": []}

    infos = _table_infos(identifiers)
    columns_by_table = _columns_by_table(infos)
    if not columns_by_table:
        return {"status": "no_metadata", "candidates": []}

    declared = _declared_pairs(space_data)
    cands = candidate_pairs(columns_by_table, declared)
    if not cands:
        return {"status": "fully_connected", "candidates": []}

    # Upgrade FK-backed candidates to match="fk" (strongest grounding).
    fks = foreign_key_pairs(infos)
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
