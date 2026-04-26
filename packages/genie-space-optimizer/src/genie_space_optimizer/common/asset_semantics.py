"""Unified asset-semantics contract for Genie space assets (PR 27).

A single source of truth for *what kind of asset* every Genie ref is —
ordinary table, plain view, metric view, or unknown — plus the measure /
dimension columns and detection provenance that downstream stages
(unified SQL synthesis, preflight synthesis, join discovery, validation
and repair) must agree on. Until PR 27, those consumers each re-derived
MV identity independently from one of three signals:

* the explicit ``data_sources.metric_views`` shelf,
* a column-config ``column_type='measure'`` / ``is_measure`` flag on a
  ``data_sources.tables`` entry,
* the catalog ``_metric_view_yaml`` cache populated by
  :func:`metric_view_catalog.detect_metric_views_via_catalog_with_outcomes`.

When any one of those signals failed silently — for example a DBR
runtime where ``DESCRIBE ... AS JSON`` is unsupported, or a Genie
serializer that strips both YAML and ``is_measure`` flags — different
stages reached different conclusions, so the unified pipeline would
auto-wrap MEASURE() correctly while join discovery still emitted direct
metric-view joins. The semantics layer normalises every available signal
into one structure (``config["_asset_semantics"]``) so each consumer can
read the same answer.

Contract:

* The container is keyed by **fully-qualified, lower-cased** identifier
  (``catalog.schema.name``) so callers don't have to worry about
  case-folding the same way upstream did.
* Each entry exposes ``kind``, ``identifier``, ``short_name``,
  ``measures``, ``dimensions``, ``provenance``, ``outcome``,
  ``detection_errors`` and an optional ``metric_view_yaml`` payload.
* ``kind`` is one of ``"table"``, ``"view"``, ``"metric_view"``, or
  ``"unknown"``. MV identity (kind) is recorded separately from MV
  measure availability — a metric view can be classified via structural
  signals even when its measure list is empty (e.g. JSON envelope
  emitted by a non-owner). SQL repair must distinguish "this is an MV
  with no known measures" from "this is not an MV".
* ``provenance`` is the *list* of signals that contributed to the
  classification (``"genie_metric_views"``, ``"column_flags"``,
  ``"catalog"``, ``"profile_reclassified"``). Multi-signal MVs surface
  every contributor so log readers can see redundancy.
* ``outcome`` is the catalog detection outcome code (``OUTCOME_*``) for
  this ref, when catalog probing was attempted; absent otherwise. This
  is the single key non-detect refs preserve in semantics so banner
  diagnostics survive the build pass without re-querying the outcomes
  dict.

The layer is intentionally *additive*: writers stamp the contract under
``config["_asset_semantics"]`` (and mirror onto ``_parsed_space`` for
config snapshots that round-trip through Delta). Legacy consumers reading
``_metric_view_yaml`` continue to work unchanged because PR 28+ only
*reads* through the new helpers; the cache continues to be populated by
the existing detection paths.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Iterable

logger = logging.getLogger(__name__)


# ── Public kind constants ───────────────────────────────────────────
# Stable strings — banner / log readers and downstream gates pivot on
# these. Treat them as part of the contract.
KIND_TABLE = "table"
KIND_VIEW = "view"
KIND_METRIC_VIEW = "metric_view"
KIND_UNKNOWN = "unknown"

KNOWN_KINDS: frozenset[str] = frozenset(
    {KIND_TABLE, KIND_VIEW, KIND_METRIC_VIEW, KIND_UNKNOWN}
)


# ── Public provenance constants ─────────────────────────────────────
# Multi-signal classification — every contributor lands in the
# ``provenance`` list so callers can see *why* a ref was classified the
# way it was without re-deriving from scratch.
PROVENANCE_GENIE_METRIC_VIEWS = "genie_metric_views"
PROVENANCE_COLUMN_FLAGS = "column_flags"
PROVENANCE_CATALOG = "catalog"
PROVENANCE_PROFILE_RECLASSIFIED = "profile_reclassified"
PROVENANCE_GENIE_TABLES = "genie_tables"
PROVENANCE_GENIE_VIEWS = "genie_views"


@dataclass
class AssetSemantics:
    """Normalized semantics record for a single asset.

    Stored as a plain dict in ``config["_asset_semantics"]`` (see
    :func:`stamp_asset_semantics`) so it can survive Delta serialization
    without pulling the dataclass module into snapshots.
    """

    identifier: str
    short_name: str
    kind: str = KIND_UNKNOWN
    measures: list[str] = field(default_factory=list)
    dimensions: list[str] = field(default_factory=list)
    provenance: list[str] = field(default_factory=list)
    outcome: str | None = None
    detection_errors: list[str] = field(default_factory=list)
    metric_view_yaml: dict | None = None

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "identifier": self.identifier,
            "short_name": self.short_name,
            "kind": self.kind,
            "measures": list(self.measures),
            "dimensions": list(self.dimensions),
            "provenance": list(self.provenance),
            "detection_errors": list(self.detection_errors),
        }
        if self.outcome is not None:
            out["outcome"] = self.outcome
        if self.metric_view_yaml is not None:
            out["metric_view_yaml"] = self.metric_view_yaml
        return out


# ── Builder ─────────────────────────────────────────────────────────


def _norm_identifier(raw: str) -> str:
    return str(raw or "").strip()


def _short_name(identifier: str) -> str:
    if not identifier:
        return ""
    return identifier.split(".")[-1]


def _entry_measures_from_column_configs(entry: dict) -> tuple[list[str], list[str]]:
    """Return (measures, dimensions) extracted from an entry's column_configs.

    The Genie ``column_configs`` payload is the canonical source for
    column-level measure / dimension signals on entries serialized as
    tables. Entries without any column_config simply yield empty lists
    so callers can still populate dimensions from the YAML payload.
    """
    measures: list[str] = []
    dimensions: list[str] = []
    for cc in entry.get("column_configs", []) or []:
        if not isinstance(cc, dict):
            continue
        col_name = str(cc.get("column_name") or "").strip()
        if not col_name:
            continue
        col_type = str(cc.get("column_type", "")).lower()
        is_measure_flag = bool(cc.get("is_measure"))
        if col_type == "measure" or is_measure_flag:
            measures.append(col_name)
        else:
            # Treat anything non-measure as a dimension candidate; this
            # mirrors how Spark thinks about MV columns without
            # over-claiming on tables (which simply read empty here for
            # downstream callers that gate on ``kind`` first).
            dimensions.append(col_name)
    return measures, dimensions


def _yaml_measures_dimensions(yaml_doc: dict | None) -> tuple[list[str], list[str]]:
    """Extract (measures, dimensions) from an MV YAML doc.

    Tolerant of the synthesized-skeleton shape produced by
    :func:`metric_view_catalog._synthesize_yaml_skeleton` and the real
    ``DESCRIBE ... AS JSON`` payload.
    """
    if not isinstance(yaml_doc, dict):
        return [], []
    measures: list[str] = []
    dimensions: list[str] = []
    for m in yaml_doc.get("measures") or []:
        if isinstance(m, dict):
            name = str(m.get("name") or "").strip()
            if name:
                measures.append(name)
    for d in yaml_doc.get("dimensions") or []:
        if isinstance(d, dict):
            name = str(d.get("name") or "").strip()
            if name:
                dimensions.append(name)
    return measures, dimensions


def _dedup_lower(values: Iterable[str]) -> list[str]:
    """De-duplicate values case-insensitively, preserving first-seen order."""
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        s = str(v or "").strip()
        if not s:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
    return out


def build_asset_semantics(
    config: dict,
    *,
    table_refs: list[tuple[str, str, str]] | None = None,
    catalog_yamls: dict[str, dict] | None = None,
    catalog_outcomes: dict[str, str] | None = None,
    catalog_diagnostic_samples: dict[str, str] | None = None,
    uc_columns: list[dict] | None = None,
) -> dict[str, dict]:
    """Build the unified asset-semantics map.

    Reads every available signal in priority order:

    1. ``config["_parsed_space"].data_sources.metric_views`` — Genie's
       explicit MV shelf. Strongest declarative signal.
    2. ``config["_parsed_space"].data_sources.tables`` with
       ``column_type='measure'`` / ``is_measure`` flags. Legacy MV
       serialization.
    3. ``catalog_yamls`` — runtime catalog detection cache (or a fresh
       result passed in by preflight/harness). Picks up MVs missing from
       (1) and (2) via DESCRIBE.
    4. ``catalog_outcomes`` — non-detect outcomes per ref; preserved on
       the entry so banner diagnostics don't lose them.
    5. ``table_refs`` — every ref the run knows about. Refs missing from
       (1)-(3) are stamped with ``kind="unknown"`` so downstream
       consumers see all refs even when no signal classified them.
    6. ``uc_columns`` — surfaces the asset's column list when the entry
       lacks ``column_configs``; populates ``dimensions`` for non-MVs.

    The returned mapping is keyed by lower-cased fully-qualified
    identifier and stores plain dicts (``AssetSemantics.to_dict()``) so
    snapshots round-trip cleanly.
    """
    parsed = config.get("_parsed_space")
    if not isinstance(parsed, dict):
        parsed = config if isinstance(config, dict) else {}
    ds = parsed.get("data_sources") if isinstance(parsed, dict) else {}
    if not isinstance(ds, dict):
        ds = {}

    out: dict[str, dict] = {}

    def _stamp_entry(record: AssetSemantics) -> None:
        """Insert / merge a record into ``out`` keyed by lower FQN."""
        key = record.identifier.lower()
        if not key:
            return
        existing = out.get(key)
        if existing is None:
            out[key] = record.to_dict()
            return
        # Merge: prefer the more confident kind, union provenance and
        # measures/dimensions, keep first-seen yaml.
        kind_priority = {
            KIND_METRIC_VIEW: 3,
            KIND_VIEW: 2,
            KIND_TABLE: 1,
            KIND_UNKNOWN: 0,
        }
        old_kind = existing.get("kind", KIND_UNKNOWN)
        new_kind = record.kind
        if kind_priority.get(new_kind, 0) > kind_priority.get(old_kind, 0):
            existing["kind"] = new_kind
        existing["provenance"] = _dedup_lower(
            list(existing.get("provenance", [])) + list(record.provenance),
        )
        existing["measures"] = _dedup_lower(
            list(existing.get("measures", [])) + list(record.measures),
        )
        existing["dimensions"] = _dedup_lower(
            list(existing.get("dimensions", [])) + list(record.dimensions),
        )
        existing["detection_errors"] = list(
            dict.fromkeys(
                list(existing.get("detection_errors", []))
                + list(record.detection_errors),
            ),
        )
        if record.outcome and not existing.get("outcome"):
            existing["outcome"] = record.outcome
        if record.metric_view_yaml and not existing.get("metric_view_yaml"):
            existing["metric_view_yaml"] = record.metric_view_yaml

    # 1. Explicit metric_views shelf.
    for mv in ds.get("metric_views", []) or []:
        if not isinstance(mv, dict):
            continue
        ident = _norm_identifier(mv.get("identifier") or mv.get("name") or "")
        if not ident:
            continue
        measures, dims = _entry_measures_from_column_configs(mv)
        _stamp_entry(AssetSemantics(
            identifier=ident,
            short_name=_short_name(ident),
            kind=KIND_METRIC_VIEW,
            measures=measures,
            dimensions=dims,
            provenance=[PROVENANCE_GENIE_METRIC_VIEWS],
        ))

    # 2. Tables shelf — column-flag heuristic for MVs misfiled as tables,
    # else stamp as kind=table.
    for tbl in ds.get("tables", []) or []:
        if not isinstance(tbl, dict):
            continue
        ident = _norm_identifier(tbl.get("identifier") or tbl.get("name") or "")
        if not ident:
            continue
        measures, dims = _entry_measures_from_column_configs(tbl)
        if measures:
            _stamp_entry(AssetSemantics(
                identifier=ident,
                short_name=_short_name(ident),
                kind=KIND_METRIC_VIEW,
                measures=measures,
                dimensions=dims,
                provenance=[PROVENANCE_COLUMN_FLAGS],
            ))
        else:
            _stamp_entry(AssetSemantics(
                identifier=ident,
                short_name=_short_name(ident),
                kind=KIND_TABLE,
                measures=[],
                dimensions=dims,
                provenance=[PROVENANCE_GENIE_TABLES],
            ))

    # 3. Catalog YAML cache.
    yamls = catalog_yamls
    if yamls is None:
        cache = config.get("_metric_view_yaml")
        if not isinstance(cache, dict):
            ps_cache = parsed.get("_metric_view_yaml") if isinstance(parsed, dict) else None
            if isinstance(ps_cache, dict):
                cache = ps_cache
        yamls = cache if isinstance(cache, dict) else {}

    if isinstance(yamls, dict):
        for fq, yaml_doc in yamls.items():
            ident = _norm_identifier(fq)
            if not ident:
                continue
            measures, dims = _yaml_measures_dimensions(
                yaml_doc if isinstance(yaml_doc, dict) else None,
            )
            _stamp_entry(AssetSemantics(
                identifier=ident,
                short_name=_short_name(ident),
                kind=KIND_METRIC_VIEW,
                measures=measures,
                dimensions=dims,
                provenance=[PROVENANCE_CATALOG],
                metric_view_yaml=(
                    yaml_doc if isinstance(yaml_doc, dict) else None
                ),
            ))

    # 4. Catalog outcomes — record per-ref outcome diagnostics even on
    # non-detected refs so the banner block can show why a ref is
    # classified the way it is.
    if isinstance(catalog_outcomes, dict):
        for fq_lower, code in catalog_outcomes.items():
            key = str(fq_lower or "").strip().lower()
            if not key:
                continue
            entry = out.get(key)
            if entry is None:
                # Refs the catalog probed but no other signal touched yet
                # — stamp them as unknown with the outcome attached so the
                # diagnostic block surfaces them.
                ident = str(fq_lower).strip()
                _stamp_entry(AssetSemantics(
                    identifier=ident,
                    short_name=_short_name(ident),
                    kind=KIND_UNKNOWN,
                    provenance=[],
                    outcome=code,
                ))
            elif not entry.get("outcome"):
                entry["outcome"] = code

    # 4b. Diagnostic samples — short text per non-detect ref captured by
    # ``detect_metric_views_via_catalog_with_outcomes``. Stash on the
    # entry's ``detection_errors`` list so the banner block has at
    # least one actionable example without re-querying.
    if isinstance(catalog_diagnostic_samples, dict):
        for fq_lower, sample in catalog_diagnostic_samples.items():
            key = str(fq_lower or "").strip().lower()
            if not key or not isinstance(sample, str) or not sample:
                continue
            entry = out.get(key)
            if entry is None:
                ident = str(fq_lower).strip()
                _stamp_entry(AssetSemantics(
                    identifier=ident,
                    short_name=_short_name(ident),
                    kind=KIND_UNKNOWN,
                    provenance=[],
                    detection_errors=[sample],
                ))
            else:
                errs = entry.setdefault("detection_errors", [])
                if sample not in errs:
                    errs.append(sample)

    # 5. Backfill from table_refs and ``_tables`` so refs the run knows
    # about always appear (kind=unknown if no signal matched).
    if table_refs:
        for cat, sch, name in table_refs:
            cat = (cat or "").strip()
            sch = (sch or "").strip()
            name = (name or "").strip()
            if not (cat and sch and name):
                continue
            ident = f"{cat}.{sch}.{name}"
            _stamp_entry(AssetSemantics(
                identifier=ident,
                short_name=_short_name(ident),
                kind=KIND_UNKNOWN,
                provenance=[],
            ))

    for ident in (config.get("_tables") or []):
        ident_s = _norm_identifier(ident)
        if not ident_s:
            continue
        # If unknown, we leave it unknown — only refs that have at least
        # one signal claim a non-unknown kind.
        _stamp_entry(AssetSemantics(
            identifier=ident_s,
            short_name=_short_name(ident_s),
            kind=KIND_UNKNOWN,
            provenance=[],
        ))

    # 6. UC column metadata — if an entry has no measures/dimensions
    # populated yet, surface the column list so downstream prompt builders
    # can render dimensions even when ``column_configs`` is empty.
    if uc_columns:
        cols_by_table: dict[str, list[str]] = {}
        for col in uc_columns:
            if not isinstance(col, dict):
                continue
            tbl_name = str(
                col.get("table_name")
                or col.get("table")
                or col.get("full_name")
                or "",
            ).strip().lower()
            col_name = str(
                col.get("column_name") or col.get("column") or "",
            ).strip()
            if not (tbl_name and col_name):
                continue
            cols_by_table.setdefault(tbl_name, []).append(col_name)
        for key, entry in out.items():
            if entry.get("dimensions") or entry.get("measures"):
                continue
            cols = cols_by_table.get(key)
            if cols:
                entry["dimensions"] = _dedup_lower(cols)

    return out


def stamp_asset_semantics(
    config: dict,
    semantics: dict[str, dict],
    *,
    mirror_parsed: bool = True,
) -> None:
    """Stamp the semantics map onto ``config`` and (optionally) the
    parsed-space mirror.

    Idempotent: replacing an existing map drops any stale entries. We
    don't merge with the previous map because callers that re-stamp are
    *intentionally* publishing a fresh view (e.g. after a profile
    reclassification round-trip).
    """
    if not isinstance(config, dict):
        return
    config["_asset_semantics"] = semantics
    if mirror_parsed:
        parsed = config.get("_parsed_space")
        if isinstance(parsed, dict):
            parsed["_asset_semantics"] = semantics


# ── Reader helpers ──────────────────────────────────────────────────


def get_asset_semantics(config: dict) -> dict[str, dict]:
    """Return the semantics map from ``config`` (or ``_parsed_space``).

    Returns an empty dict when the contract has not been stamped yet so
    callers can call this unconditionally.
    """
    if not isinstance(config, dict):
        return {}
    sem = config.get("_asset_semantics")
    if isinstance(sem, dict):
        return sem
    parsed = config.get("_parsed_space")
    if isinstance(parsed, dict):
        sem = parsed.get("_asset_semantics")
        if isinstance(sem, dict):
            return sem
    return {}


def asset_kind(config: dict, identifier: str) -> str:
    """Return the kind string for ``identifier`` (case-insensitive lookup).

    Returns :data:`KIND_UNKNOWN` when the asset is not in the semantics
    map so callers can treat "absent" and "unknown" the same way.
    """
    sem = get_asset_semantics(config)
    key = (identifier or "").strip().lower()
    if not key:
        return KIND_UNKNOWN
    entry = sem.get(key)
    if not isinstance(entry, dict):
        # Tolerate short-name lookups when only short was provided. We
        # look for entries whose short_name matches; first hit wins.
        if "." not in key:
            for ent in sem.values():
                if not isinstance(ent, dict):
                    continue
                if str(ent.get("short_name") or "").lower() == key:
                    return str(ent.get("kind") or KIND_UNKNOWN)
        return KIND_UNKNOWN
    return str(entry.get("kind") or KIND_UNKNOWN)


def is_metric_view(config: dict, identifier: str) -> bool:
    """Convenience: True iff ``identifier`` resolves to ``kind=metric_view``."""
    return asset_kind(config, identifier) == KIND_METRIC_VIEW


def metric_view_identifiers(config: dict) -> set[str]:
    """Return the set of fully-qualified identifiers classified as MVs.

    Identifiers are returned in their original (mixed-case) form, mirroring
    :func:`evaluation.effective_metric_view_identifiers_with_catalog` so
    callers can substitute the helpers without case-fold churn.
    """
    out: set[str] = set()
    for entry in get_asset_semantics(config).values():
        if not isinstance(entry, dict):
            continue
        if entry.get("kind") == KIND_METRIC_VIEW:
            ident = str(entry.get("identifier") or "").strip()
            if ident:
                out.add(ident)
    return out


def metric_view_measures_by_short_name(config: dict) -> dict[str, set[str]]:
    """Return ``{lowered_short_name: {measure_col, ...}}`` from semantics.

    Mirrors the contract of
    :func:`evaluation.build_metric_view_measures` so callers can adopt
    the semantics layer without touching the rewriter's public API.
    Measures with empty sets are dropped, matching the legacy contract.
    """
    out: dict[str, set[str]] = {}
    for entry in get_asset_semantics(config).values():
        if not isinstance(entry, dict):
            continue
        if entry.get("kind") != KIND_METRIC_VIEW:
            continue
        short = str(entry.get("short_name") or "").lower()
        if not short:
            continue
        measures = {
            str(m).lower() for m in (entry.get("measures") or []) if isinstance(m, str) and m
        }
        if measures:
            existing = out.setdefault(short, set())
            existing.update(measures)
    return out


# ── Banner / observability ──────────────────────────────────────────


def summarize_semantics(semantics: dict[str, dict]) -> dict[str, int]:
    """Count entries by kind plus a few cross-cutting diagnostics.

    Returns a dict with ``table``, ``view``, ``metric_view``, ``unknown``,
    ``total``, ``with_outcome``, ``mv_with_measures``, ``mv_without_measures``.
    Always present so banner formatters can write a stable column layout.
    """
    counts = {
        KIND_TABLE: 0,
        KIND_VIEW: 0,
        KIND_METRIC_VIEW: 0,
        KIND_UNKNOWN: 0,
        "total": 0,
        "with_outcome": 0,
        "mv_with_measures": 0,
        "mv_without_measures": 0,
    }
    if not isinstance(semantics, dict):
        return counts
    for entry in semantics.values():
        if not isinstance(entry, dict):
            continue
        kind = entry.get("kind") or KIND_UNKNOWN
        if kind in counts:
            counts[kind] += 1
        else:
            counts[KIND_UNKNOWN] += 1
        counts["total"] += 1
        if entry.get("outcome"):
            counts["with_outcome"] += 1
        if kind == KIND_METRIC_VIEW:
            if entry.get("measures"):
                counts["mv_with_measures"] += 1
            else:
                counts["mv_without_measures"] += 1
    return counts


def format_semantics_block(semantics: dict[str, dict], *, max_samples: int = 5) -> list[str]:
    """Render a multi-line ``ASSET SEMANTICS`` block for log/banner display.

    Output is a list of pre-formatted lines (no trailing newline) so
    callers can splice them into existing banner builders without taking
    a stance on indentation. The block surfaces:

    * the per-kind count,
    * the count of metric views classified without measures,
    * a few sample identifiers per kind,
    * a few sample non-detect outcomes (to make catalog detection
      failures legible).

    Designed to be safe to call when the semantics map is empty —
    returns a single "(no asset semantics stamped)" diagnostic.
    """
    lines: list[str] = []
    if not isinstance(semantics, dict) or not semantics:
        lines.append("ASSET SEMANTICS: (no asset semantics stamped)")
        return lines
    counts = summarize_semantics(semantics)
    lines.append(
        "ASSET SEMANTICS: "
        f"total={counts['total']}, "
        f"tables={counts[KIND_TABLE]}, "
        f"views={counts[KIND_VIEW]}, "
        f"metric_views={counts[KIND_METRIC_VIEW]} "
        f"(with measures: {counts['mv_with_measures']}, "
        f"without measures: {counts['mv_without_measures']}), "
        f"unknown={counts[KIND_UNKNOWN]}"
    )

    by_kind: dict[str, list[dict]] = {}
    for entry in semantics.values():
        if not isinstance(entry, dict):
            continue
        by_kind.setdefault(entry.get("kind") or KIND_UNKNOWN, []).append(entry)

    for kind in (KIND_METRIC_VIEW, KIND_TABLE, KIND_VIEW, KIND_UNKNOWN):
        entries = by_kind.get(kind) or []
        if not entries:
            continue
        sample = sorted(
            (str(e.get("identifier") or "") for e in entries[:max_samples])
        )
        lines.append(
            f"  {kind}: {', '.join(sample) if sample else '(none)'}"
            + (f" (+{len(entries) - len(sample)} more)" if len(entries) > len(sample) else "")
        )

    # Surface a few non-detect outcomes so catalog probe failures are visible.
    outcome_samples: list[tuple[str, str, str]] = []
    for entry in semantics.values():
        if not isinstance(entry, dict):
            continue
        oc = entry.get("outcome")
        if not oc or oc.startswith("detected"):
            continue
        errs = entry.get("detection_errors") or []
        first_err = ""
        if isinstance(errs, list) and errs:
            first_err = str(errs[0])[:160]
        outcome_samples.append(
            (str(entry.get("identifier") or ""), str(oc), first_err),
        )
        if len(outcome_samples) >= max_samples:
            break
    if outcome_samples:
        lines.append("  catalog non-detect outcomes:")
        for ident, oc, err in outcome_samples:
            tail = f" — {err}" if err else ""
            lines.append(f"    {ident}: {oc}{tail}")

    return lines


def build_and_stamp_from_run(
    config: dict,
    *,
    table_refs: list[tuple[str, str, str]] | None = None,
    catalog_yamls: dict[str, dict] | None = None,
    catalog_outcomes: dict[str, str] | None = None,
    catalog_diagnostic_samples: dict[str, str] | None = None,
    uc_columns: list[dict] | None = None,
    mirror_parsed: bool = True,
) -> dict[str, dict]:
    """Convenience: build the semantics map and stamp it onto ``config``.

    Returns the semantics map so callers can immediately format a banner
    or run additional gates without re-reading the cache.
    """
    semantics = build_asset_semantics(
        config,
        table_refs=table_refs,
        catalog_yamls=catalog_yamls,
        catalog_outcomes=catalog_outcomes,
        catalog_diagnostic_samples=catalog_diagnostic_samples,
        uc_columns=uc_columns,
    )
    stamp_asset_semantics(config, semantics, mirror_parsed=mirror_parsed)
    return semantics


def invariant_warning_lines(
    semantics: dict[str, dict],
    rejection_counters: dict[str, Any] | None,
) -> list[str]:
    """Return invariant-warning lines for a run with mv_* rejections but
    zero metric views in semantics.

    Empty when no warning applies. Callers splice the result into the
    banner builder so the warning shows up exactly where readers look
    for it (next to the MVs detected line).
    """
    lines: list[str] = []
    counts = summarize_semantics(semantics)
    if counts.get(KIND_METRIC_VIEW, 0) > 0:
        return lines
    rc = rejection_counters or {}
    sub = rc.get("explain_or_execute_subbuckets") if isinstance(rc, dict) else None
    has_mv_reject = False
    if isinstance(sub, dict):
        for reason in sub:
            if isinstance(reason, str) and reason.startswith("mv_"):
                has_mv_reject = True
                break
    if not has_mv_reject:
        return lines
    lines.append(
        "INVARIANT WARNING: 0 metric views in asset semantics but "
        "mv_* rejections present — catalog detection silently failed "
        "or _asset_semantics was not stamped before SQL generation",
    )
    # Surface a sample of describe_error outcomes so the warning is
    # actionable without grepping.
    sample: list[str] = []
    for entry in semantics.values():
        if not isinstance(entry, dict):
            continue
        oc = entry.get("outcome")
        if oc and not oc.startswith("detected"):
            errs = entry.get("detection_errors") or []
            tail = ""
            if isinstance(errs, list) and errs:
                tail = f" — {str(errs[0])[:160]}"
            sample.append(f"{entry.get('identifier')}: {oc}{tail}")
        if len(sample) >= 3:
            break
    for s in sample:
        lines.append(f"  sample non-detect outcome: {s}")
    return lines
