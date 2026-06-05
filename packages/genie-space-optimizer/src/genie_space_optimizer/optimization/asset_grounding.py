"""Trial 23 W5 — pre-generation asset grounding (correct at source).

The repair-diagnosis sufficiency gate
(``state_machine/transformers/diagnose_llm.py``) records
``missing implicated_assets`` as *observe-only*: it never abstains, so
generation falls through to a generic shape and the LLM emits snippets
that name assets the applier cannot resolve. The d139/e943 postmortems
trace inert patches back to exactly this ungrounded synthesis.

W5 promotes the signal without re-creating the all-dropped flatline:
instead of blocking when ``implicated_assets`` is missing, it RESOLVES
the cluster's ``primary_blame_set`` against the schema slice the prompt
already carries and injects the resolved ``catalog.schema.table[.column]``
references as an explicit grounding directive. The synthesizer is then
told to anchor SQL-shape repairs to assets that provably exist. The
hard-block promotion stays behind
``trial23_asset_grounding_blocking_enabled`` (default OFF) until the
W7-W9 repair paths land (see plan "central design tension").

This module is the *pure* resolver + payload builder. It has no I/O and
no flag reads; the synthesizer owns the flag gate and the marker print.
"""
from __future__ import annotations

import json
from collections.abc import Iterable, Mapping


def _iter_asset_entries(schema_slice: Mapping | None) -> Iterable[dict]:
    """Yield each table / metric-view entry from a Genie schema slice."""
    ds = (schema_slice or {}).get("data_sources") or {}
    if not isinstance(ds, Mapping):
        return
    for key in ("tables", "metric_views"):
        for entry in ds.get(key) or []:
            if isinstance(entry, dict):
                yield entry


def _columns_of(entry: dict) -> set[str]:
    """Column names for one asset, tolerant of both serialized shapes.

    Production ``serialized_space`` stores columns under
    ``column_configs`` with ``column_name``; the legacy / test shape
    uses ``columns`` with ``name``. Metric views additionally expose
    ``measures`` / ``dimensions``. We union all of them so a blame
    reference resolves regardless of which shape the snapshot carries.
    """
    out: set[str] = set()
    for c in entry.get("columns") or []:
        if isinstance(c, dict):
            name = c.get("name") or c.get("column_name")
            if name:
                out.add(str(name))
    for c in entry.get("column_configs") or []:
        if isinstance(c, dict):
            name = c.get("column_name") or c.get("name")
            if name:
                out.add(str(name))
    for key in ("measures", "dimensions"):
        for c in entry.get(key) or []:
            if isinstance(c, dict) and c.get("name"):
                out.add(str(c["name"]))
            elif isinstance(c, str) and c:
                out.add(c)
    return out


def _asset_index(
    schema_slice: Mapping | None,
) -> tuple[dict[str, set[str]], dict[str, str]]:
    """Build (identifier -> columns) and (last_segment -> identifier).

    The last-segment map powers the 2-part ``table.column`` fallback for
    blame entries that omit the catalog/schema (matching the synthesis
    skill's blame_set convention). When two identifiers share a last
    segment the first one wins; the ambiguity is rare and the resolver
    stays deterministic on arrival order.
    """
    by_id: dict[str, set[str]] = {}
    by_last: dict[str, str] = {}
    for entry in _iter_asset_entries(schema_slice):
        ident = entry.get("identifier")
        if not isinstance(ident, str) or not ident.strip():
            continue
        ident = ident.strip()
        by_id[ident] = _columns_of(entry)
        last = ident.split(".")[-1]
        by_last.setdefault(last, ident)
    return by_id, by_last


def _resolve_one(
    raw: str,
    by_id: dict[str, set[str]],
    by_last: dict[str, str],
) -> str:
    """Resolve a single blame reference to its canonical asset form.

    Returns ``""`` when the reference does not resolve against the
    slice. Resolution order:

      1. Whole reference is a known table/MV identifier → table ref.
      2. ``<table_prefix>.<column>`` where the prefix resolves (full
         identifier or by last-segment) and the column exists → column
         ref ``<identifier>.<column>``.
      3. Whole reference matches a table's last segment → table ref.
    """
    ref = str(raw or "").strip()
    if not ref:
        return ""
    if ref in by_id:
        return ref
    if "." in ref:
        prefix, _, last = ref.rpartition(".")
        ident = ""
        if prefix in by_id:
            ident = prefix
        elif prefix.split(".")[-1] in by_last:
            ident = by_last[prefix.split(".")[-1]]
        if ident and last in by_id.get(ident, set()):
            return f"{ident}.{last}"
    if ref in by_last:
        return by_last[ref]
    return ""


def resolve_assets_from_schema_slice(
    schema_slice: Mapping | None,
    blame_set: Iterable[str],
) -> tuple[str, ...]:
    """Resolve ``blame_set`` references against the schema slice.

    Returns the canonical ``catalog.schema.table[.column]`` references
    that provably exist in the slice, deduplicated and in first-seen
    order. Unresolvable entries are dropped — they are exactly the
    references W6 (real slice) / W7 (snippet repair) handle downstream.
    """
    by_id, by_last = _asset_index(schema_slice)
    if not by_id:
        return ()
    out: list[str] = []
    seen: set[str] = set()
    for raw in blame_set or ():
        ref = _resolve_one(raw, by_id, by_last)
        if ref and ref not in seen:
            seen.add(ref)
            out.append(ref)
    return tuple(out)


def needs_asset_grounding(
    *,
    implicated_assets: Iterable[str],
    root_cause: str,
    sql_shape_delta: str,
) -> bool:
    """True when a SQL-shape repair lacks resolved implicated assets.

    Grounding is needed only when (a) no implicated assets are already
    resolved AND (b) there is a shape intent to ground — a non-empty
    RCA kind or a non-empty ``sql_shape_delta``. Clusters with neither
    have nothing shape-y to anchor, so injection would be noise.
    """
    if any(str(a or "").strip() for a in (implicated_assets or ())):
        return False
    has_shape_intent = bool(
        str(root_cause or "").strip() or str(sql_shape_delta or "").strip()
    )
    return has_shape_intent


def build_asset_grounding(
    *,
    schema_slice: Mapping | None,
    blame_set: Iterable[str],
    implicated_assets: Iterable[str],
    root_cause: str,
    sql_shape_delta: str,
) -> dict | None:
    """Build the grounding payload to inject, or ``None`` when not
    applicable.

    Returns ``None`` when grounding is not needed (assets already
    resolved / no shape intent) OR when nothing in ``blame_set``
    resolves against the slice (no concrete assets to inject — leave it
    to the downstream repair paths). Otherwise returns a dict carrying
    the resolved assets and an imperative directive for the prompt.
    """
    if not needs_asset_grounding(
        implicated_assets=implicated_assets,
        root_cause=root_cause,
        sql_shape_delta=sql_shape_delta,
    ):
        return None
    resolved = resolve_assets_from_schema_slice(schema_slice, blame_set)
    if not resolved:
        return None
    return {
        "resolved_assets": list(resolved),
        "directive": (
            "Trial 23 W5 — asset grounding. The repair diagnosis did not "
            "carry resolved implicated_assets, so the framework resolved "
            "the cluster's blame_set against the schema slice. ANCHOR every "
            "SQL-shape repair (sql_snippet / example_sql / measure) to "
            "these exact catalog.schema.table[.column] references; do NOT "
            "invent table or column names that are absent from this list."
        ),
    }


def asset_grounding_injected_marker(
    *,
    optimization_run_id: str,
    iteration: int,
    cluster_id: str,
    root_cause: str,
    resolved_assets: Iterable[str],
) -> str:
    """Build the ``GSO_TRIAL23_ASSET_GROUNDING_INJECTED_V1`` marker line.

    Records that pre-generation grounding fired and pins the resolved
    asset slice so postmortems can prove the synthesizer was given
    concrete assets to anchor on (and audit whether it used them).
    """
    assets = [str(a) for a in (resolved_assets or ()) if str(a or "").strip()]
    payload = {
        "optimization_run_id": str(optimization_run_id),
        "iteration": int(iteration),
        "cluster_id": str(cluster_id),
        "root_cause": str(root_cause or ""),
        "resolved_assets": assets,
        "resolved_count": len(assets),
    }
    return (
        "GSO_TRIAL23_ASSET_GROUNDING_INJECTED_V1 "
        + json.dumps(payload, sort_keys=True)
    )


__all__ = [
    "resolve_assets_from_schema_slice",
    "needs_asset_grounding",
    "build_asset_grounding",
    "asset_grounding_injected_marker",
]
