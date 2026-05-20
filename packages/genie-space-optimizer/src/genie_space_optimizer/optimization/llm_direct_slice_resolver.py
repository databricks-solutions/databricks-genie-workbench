"""Plan 9 Task 6 — LLM-direct slice resolver.

Resolves a tuple of TargetObjects (LLM-emitted) into a concrete
AssetSlice (tables, metric_view, columns, join_spec) by looking up
each identifier in metadata_snapshot. Replaces the archetype-driven
slice derivation in _derive_asset_slice_from_afs at
cluster_driven_synthesis.py:637-720 when a typed RepairProposal
with non-empty target_objects is available.

Pure function. No LLM call. No archetype dependency.

Note on AssetSlice.columns shape:
  ``AssetSlice.columns: list[tuple[str, str]]`` is
  ``[(table_identifier, column_name), ...]`` — see
  preflight_synthesis.py:177. The resolver MUST produce that
  shape so downstream consumers (existing per-lever generators)
  keep working unchanged.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.preflight_synthesis import (
    AssetSlice,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
    TargetObject,
)


class UnknownTargetObjectError(KeyError):
    """Raised when a TargetObject identifier does not appear in
    metadata_snapshot. The LLM must only emit identifiers it was
    shown in the schema; this error catches synthesizer drift."""


def _find_table(
    identifier: str, metadata_snapshot: dict,
) -> dict | None:
    ds = metadata_snapshot.get("data_sources", {}) or {}
    norm = identifier.strip().lower()
    for t in ds.get("tables", []) or []:
        if str(t.get("identifier", "")).strip().lower() == norm:
            return t
    return None


def _find_metric_view(
    identifier: str, metadata_snapshot: dict,
) -> dict | None:
    ds = metadata_snapshot.get("data_sources", {}) or {}
    norm = identifier.strip().lower()
    for mv in ds.get("metric_views", []) or []:
        if str(mv.get("identifier", "")).strip().lower() == norm:
            return mv
    return None


def _find_join_spec(
    left_id: str, right_id: str, metadata_snapshot: dict,
) -> dict | None:
    instructions = metadata_snapshot.get("instructions", {}) or {}
    js_list = instructions.get("join_specs") or []
    want = {left_id.strip().lower(), right_id.strip().lower()}
    for js in js_list:
        left = (js.get("left") or {}).get("identifier", "").strip().lower()
        right = (js.get("right") or {}).get("identifier", "").strip().lower()
        if {left, right} == want:
            return js
    return None


def resolve_target_objects_to_asset_slice(
    targets: tuple[TargetObject, ...],
    metadata_snapshot: dict,
) -> AssetSlice:
    """Build an AssetSlice from the LLM-emitted target_objects.

    Empty targets return an empty AssetSlice (caller decides).

    Raises UnknownTargetObjectError if any identifier is not in
    metadata_snapshot — the LLM must ground every identifier in
    the schema it was shown.
    """
    if not targets:
        return AssetSlice()

    tables: list[dict] = []
    metric_view: dict | None = None
    columns: list[tuple[str, str]] = []

    for t in targets:
        if t.asset_kind == AssetKind.TABLE:
            tbl = _find_table(t.identifier, metadata_snapshot)
            if tbl is None:
                raise UnknownTargetObjectError(
                    f"TargetObject (TABLE) identifier {t.identifier!r} "
                    f"not in metadata_snapshot. LLM emitted an identifier "
                    f"not present in the schema it was shown."
                )
            tables.append(tbl)
            # Project the LLM-named columns into the slice as
            # (table_identifier, column_name) tuples — the canonical
            # AssetSlice.columns shape.
            if t.columns:
                tbl_id = str(tbl.get("identifier") or t.identifier)
                tbl_col_names = {
                    str(c.get("name", "")) for c in (tbl.get("columns") or [])
                }
                for col_name in t.columns:
                    if col_name in tbl_col_names:
                        columns.append((tbl_id, col_name))
        elif t.asset_kind == AssetKind.METRIC_VIEW:
            mv = _find_metric_view(t.identifier, metadata_snapshot)
            if mv is None:
                raise UnknownTargetObjectError(
                    f"TargetObject (METRIC_VIEW) identifier "
                    f"{t.identifier!r} not in metadata_snapshot."
                )
            if metric_view is None:
                metric_view = mv
        elif t.asset_kind == AssetKind.COLUMN:
            # COLUMN-kind: identifier is catalog.schema.table.column.
            # Split off the trailing column name; look up the table.
            parts = t.identifier.rsplit(".", 1)
            if len(parts) != 2:
                raise UnknownTargetObjectError(
                    f"TargetObject (COLUMN) identifier {t.identifier!r} "
                    f"must be 'catalog.schema.table.column' shape."
                )
            tbl_id, col_name = parts
            tbl = _find_table(tbl_id, metadata_snapshot)
            if tbl is None:
                raise UnknownTargetObjectError(
                    f"TargetObject (COLUMN): parent table {tbl_id!r} "
                    f"not in metadata_snapshot for {t.identifier!r}."
                )
            tbl_col_names = {
                str(c.get("name", "")) for c in (tbl.get("columns") or [])
            }
            if col_name not in tbl_col_names:
                raise UnknownTargetObjectError(
                    f"TargetObject (COLUMN) column {col_name!r} "
                    f"not in table {tbl_id!r}."
                )
            columns.append((tbl_id, col_name))

    join_spec = None
    if len(tables) >= 2:
        left = (tables[0].get("identifier") or "")
        right = (tables[1].get("identifier") or "")
        join_spec = _find_join_spec(left, right, metadata_snapshot)

    return AssetSlice(
        tables=tables,
        metric_view=metric_view,
        columns=columns,
        join_spec=join_spec,
    )
