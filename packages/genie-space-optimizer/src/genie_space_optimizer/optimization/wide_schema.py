"""Wide-schema inventory, evidence, selection, and adaptive-plan contracts.

The full Unity Catalog inventory is deliberately separate from the bounded
working set sent to profilers and LLMs.  All helpers in this module are pure so
the four notebook entrypoints can persist their results without depending on
notebook-local state.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

SELECTOR_VERSION = "wide_schema_selector_v2"
CONTRACT_VERSION = 1

MAX_ACTIVE_COLUMNS_PER_ASSET = 50
MAX_CUMULATIVE_PROFILED_COLUMNS_PER_ASSET = 50
INITIAL_ACTIVE_TARGET = 45
INITIAL_CORE_TARGET = 40
ADAPTIVE_RESERVE = 5
EXPLORATION_TARGET = 5
MIN_INITIAL_TARGET = 8
MAX_ELIGIBLE_ASSETS = 20
MAX_AGGREGATE_EXPRESSIONS_PER_STATEMENT = 10
MAX_VALUE_LIST_COLUMNS_PER_ASSET = 10
MAX_CONCURRENT_PROFILING_STATEMENTS = 3
MAX_PROFILING_STATEMENTS_PER_ASSET = 30
MAX_PROFILING_STATEMENTS_PER_RUN = 600
PROFILING_STAGE_DEADLINE_SECONDS = 30 * 60
PROFILING_STATEMENT_DEADLINE_SECONDS = 50
MAX_LLM_REQUEST_CHARS = 60_000

PROFILE_STATUSES = frozenset({
    "pending",
    "profiled",
    "partial",
    "timed_out",
    "metadata_only",
    "not_selected",
})

PRIORITY_BY_REASON = {
    "REPAIR_FAILURE": 0,
    "BENCHMARK_SQL": 1,
    "CONFIG_SQL": 2,
    "JOIN_KEY": 2,
    "USER_PIN": 2,
    "METRIC_FIELD": 3,
    "QUERY_HISTORY": 3,
    "SEMANTIC_MATCH": 4,
    "STRUCTURAL_COVERAGE": 5,
    "EXPLORATION": 6,
}

HARD_REQUIRED_REASONS = frozenset({
    "REPAIR_FAILURE",
    "BENCHMARK_SQL",
    "CONFIG_SQL",
    "JOIN_KEY",
    "USER_PIN",
})

# These settings explicitly request column-level value behavior. Descriptions
# and synonyms are valuable Genie metadata, but are too common to mean that a
# column must consume one of the bounded profiling slots.
USER_PIN_FIELDS = (
    "enable_entity_matching",
    "enable_format_assistance",
    "build_value_dictionary",
    "get_example_values",
)

QUERY_ROLE_WEIGHTS = {
    "join": 6.0,
    "filter": 5.0,
    "group": 4.0,
    "aggregate": 4.0,
    "order": 2.0,
    "projection": 1.0,
}

_NUMERIC_TYPES = frozenset({
    "byte", "short", "smallint", "int", "integer", "long", "bigint",
    "float", "double", "decimal", "numeric", "number",
})
_DATE_TYPES = frozenset({"date", "timestamp", "timestamp_ntz"})
_COMPLEX_PREFIXES = ("array", "map", "struct", "binary", "variant")
_STOP_WORDS = frozenset({
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
    "how", "in", "is", "it", "of", "on", "or", "show", "that", "the",
    "to", "what", "when", "where", "which", "with",
})


def stable_json(value: Any) -> str:
    return json.dumps(value, default=str, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def content_hash(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()


def normalize_component(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] == "`":
        text = text[1:-1].replace("``", "`")
    return text.casefold()


def quote_component(value: Any) -> str:
    return f"`{str(value or '').replace('`', '``')}`"


def render_identifier(parts: Iterable[Any]) -> str:
    return ".".join(quote_component(part) for part in parts)


def _identifier_parts(identifier: Any) -> tuple[str, ...]:
    """Split a SQL identifier while preserving dots inside backtick components."""
    raw = str(identifier or "").strip()
    if not raw:
        return ()
    parts: list[str] = []
    current: list[str] = []
    quoted = False
    i = 0
    while i < len(raw):
        char = raw[i]
        if char == "`":
            if quoted and i + 1 < len(raw) and raw[i + 1] == "`":
                current.append("`")
                i += 2
                continue
            quoted = not quoted
        elif char == "." and not quoted:
            parts.append(normalize_component("".join(current)))
            current = []
        else:
            current.append(char)
        i += 1
    parts.append(normalize_component("".join(current)))
    return tuple(part for part in parts if part)


def _parsed_space(config: dict[str, Any]) -> dict[str, Any]:
    parsed = config.get("_parsed_space")
    if isinstance(parsed, dict):
        return parsed
    serialized = config.get("serialized_space")
    if isinstance(serialized, dict):
        return serialized
    if isinstance(serialized, str):
        try:
            loaded = json.loads(serialized)
        except (TypeError, ValueError):
            loaded = None
        if isinstance(loaded, dict):
            return loaded
    return config if isinstance(config, dict) else {}


def _asset_entries(config: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    parsed = _parsed_space(config)
    sources = parsed.get("data_sources") if isinstance(parsed.get("data_sources"), dict) else {}
    entries: list[tuple[str, dict[str, Any]]] = []
    for key, kind in (("tables", "table"), ("metric_views", "metric_view")):
        for raw in sources.get(key) or []:
            if isinstance(raw, dict):
                entries.append((kind, raw))
    known = {_identifier_parts(entry.get("identifier")) for _, entry in entries}
    for kind_key, kind in (("_tables", "table"), ("_metric_views", "metric_view")):
        for identifier in config.get(kind_key) or []:
            parts = _identifier_parts(identifier)
            if parts and parts not in known:
                entries.append((kind, {"identifier": identifier}))
                known.add(parts)
    return entries


def _column_config_index(config: dict[str, Any]) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    out: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for _kind, asset in _asset_entries(config):
        asset_key = _identifier_parts(asset.get("identifier"))
        if len(asset_key) != 3:
            continue
        for raw in asset.get("column_configs") or asset.get("columns") or []:
            if not isinstance(raw, dict):
                continue
            name = normalize_component(raw.get("column_name") or raw.get("name"))
            if name:
                out[(*asset_key, name)] = raw
    return out


def _metric_role(
    column_key: tuple[str, str, str, str],
    asset_type: str,
    column_config: dict[str, Any] | None,
    config: dict[str, Any],
) -> str | None:
    if asset_type != "metric_view":
        return None
    cfg = column_config or {}
    if str(cfg.get("column_type") or "").casefold() == "measure" or cfg.get("is_measure"):
        return "measure"
    yaml_cache = config.get("_metric_view_yaml") or {}
    if isinstance(yaml_cache, dict):
        asset_id = ".".join(column_key[:3])
        mv_yaml = yaml_cache.get(asset_id) or yaml_cache.get(render_identifier(column_key[:3]))
        if isinstance(mv_yaml, dict):
            name = column_key[3]
            measures = {
                normalize_component(v.get("name"))
                for v in mv_yaml.get("measures") or [] if isinstance(v, dict)
            }
            if name in measures:
                return "measure"
    return "dimension"


def _metric_expression(
    column_key: tuple[str, str, str, str],
    column_config: dict[str, Any] | None,
    config: dict[str, Any],
) -> str | None:
    cfg = column_config or {}
    raw = cfg.get("expression") or cfg.get("sql")
    if isinstance(raw, list):
        raw = " ".join(str(value) for value in raw)
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    yaml_cache = config.get("_metric_view_yaml") or {}
    if not isinstance(yaml_cache, dict):
        return None
    asset_id = ".".join(column_key[:3])
    mv_yaml = yaml_cache.get(asset_id) or yaml_cache.get(render_identifier(column_key[:3]))
    if not isinstance(mv_yaml, dict):
        return None
    for measure in mv_yaml.get("measures") or []:
        if not isinstance(measure, dict):
            continue
        if normalize_component(measure.get("name")) != column_key[3]:
            continue
        expression = measure.get("expr") or measure.get("expression")
        return str(expression).strip() if expression else None
    return None


def _constraint_role_index(
    foreign_keys: list[dict[str, Any]],
    config: dict[str, Any],
) -> dict[tuple[str, str, str, str], set[str]]:
    roles: dict[tuple[str, str, str, str], set[str]] = defaultdict(set)
    for fk in foreign_keys or []:
        if not isinstance(fk, dict):
            continue
        child = _identifier_parts(fk.get("child_table"))
        parent = _identifier_parts(fk.get("parent_table"))
        if len(child) == 3:
            for name in fk.get("child_columns") or []:
                roles[(*child, normalize_component(name))].add("foreign_key")
        if len(parent) == 3:
            for name in fk.get("parent_columns") or []:
                roles[(*parent, normalize_component(name))].add("primary_key")

    parsed = _parsed_space(config)
    instructions = parsed.get("instructions") if isinstance(parsed.get("instructions"), dict) else {}
    sources = parsed.get("data_sources") if isinstance(parsed.get("data_sources"), dict) else {}
    joins = instructions.get("join_specs") or sources.get("join_specs") or []
    inventory_stub = {"assets": []}
    for join in joins:
        if not isinstance(join, dict):
            continue
        left = _identifier_parts((join.get("left") or {}).get("identifier"))
        right = _identifier_parts((join.get("right") or {}).get("identifier"))
        sql_text = " ".join(str(v) for v in join.get("sql") or [])
        for asset_key, alias in ((left, (join.get("left") or {}).get("alias")), (right, (join.get("right") or {}).get("alias"))):
            if len(asset_key) != 3:
                continue
            candidates = {asset_key[-1], normalize_component(alias)} - {""}
            for qualifier, column in re.findall(r"`?([^`.\s]+)`?\s*\.\s*`?([^`\s=]+)`?", sql_text):
                if normalize_component(qualifier) in candidates:
                    roles[(*asset_key, normalize_component(column))].add("join_key")
    return roles


def build_inventory(
    uc_columns: list[dict[str, Any]],
    config: dict[str, Any],
    *,
    foreign_keys: list[dict[str, Any]] | None = None,
    captured_at: str | None = None,
) -> dict[str, Any]:
    """Build the immutable, complete inventory contract from UC metadata."""
    entries = _asset_entries(config)
    kind_by_key = {
        _identifier_parts(asset.get("identifier")): kind
        for kind, asset in entries
        if len(_identifier_parts(asset.get("identifier"))) == 3
    }
    yaml_cache = config.get("_metric_view_yaml") or {}
    if isinstance(yaml_cache, dict):
        for identifier in yaml_cache:
            key = _identifier_parts(identifier)
            if key in kind_by_key:
                kind_by_key[key] = "metric_view"
    for raw in uc_columns or []:
        if not isinstance(raw, dict):
            continue
        key = (
            normalize_component(raw.get("catalog_name")),
            normalize_component(raw.get("schema_name")),
            normalize_component(raw.get("table_name") or raw.get("table")),
        )
        raw_type = str(raw.get("table_type") or "").casefold()
        if key not in kind_by_key:
            continue
        if raw_type == "metric_view":
            kind_by_key[key] = "metric_view"
        elif kind_by_key[key] != "metric_view" and "view" in raw_type:
            kind_by_key[key] = "view"

    column_configs = _column_config_index(config)
    constraints = _constraint_role_index(foreign_keys or [], config)
    by_asset: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)

    for fallback_ordinal, raw in enumerate(uc_columns or [], start=1):
        if not isinstance(raw, dict):
            continue
        asset_key = (
            normalize_component(raw.get("catalog_name")),
            normalize_component(raw.get("schema_name")),
            normalize_component(raw.get("table_name") or raw.get("table")),
        )
        name = normalize_component(raw.get("column_name") or raw.get("column"))
        if not all(asset_key) or not name:
            continue
        if kind_by_key and asset_key not in kind_by_key:
            continue
        column_key = (*asset_key, name)
        cfg = column_configs.get(column_key)
        raw_roles = raw.get("constraint_roles") or []
        constraint_roles = {
            str(v) for v in raw_roles
            if str(v) in {"primary_key", "foreign_key", "join_key"}
        }
        constraint_roles.update(constraints.get(column_key, set()))
        if raw.get("is_primary_key"):
            constraint_roles.add("primary_key")
        description = raw.get("comment") or raw.get("description") or ""
        metric_role = _metric_role(
            column_key,
            kind_by_key.get(asset_key, "table"),
            cfg,
            config,
        )
        column_payload = {
            "column_key": list(column_key),
            "column_id": render_identifier(column_key),
            "name": name,
            "data_type": str(raw.get("data_type") or raw.get("type_text") or "").strip(),
            "description": str(description or ""),
            "constraint_roles": sorted(constraint_roles),
            "metric_role": metric_role,
            "ordinal": int(raw.get("ordinal_position") or raw.get("ordinal") or fallback_ordinal),
        }
        if metric_role == "measure":
            column_payload["measure_expression"] = _metric_expression(
                column_key, cfg, config,
            )
        by_asset[asset_key].append(column_payload)

    assets: list[dict[str, Any]] = []
    all_asset_keys = sorted(set(kind_by_key) | set(by_asset))
    for asset_key in all_asset_keys:
        columns = sorted(by_asset.get(asset_key, []), key=lambda c: (c["ordinal"], c["column_id"]))
        assets.append({
            "asset_key": list(asset_key),
            "asset_id": render_identifier(asset_key),
            "asset_type": kind_by_key.get(asset_key, "table"),
            "columns": columns,
        })
    payload = {
        "contract_version": CONTRACT_VERSION,
        "captured_at": captured_at or datetime.now(timezone.utc).isoformat(),
        "assets": assets,
    }
    payload["inventory_hash"] = content_hash({"contract_version": CONTRACT_VERSION, "assets": assets})
    validate_inventory(payload)
    return payload


def collect_inventory(
    w: Any,
    spark: Any,
    config: dict[str, Any],
    refs: list[tuple[str, str, str]],
    *,
    prefetched: dict[str, Any] | None = None,
    warehouse_id: str = "",
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Collect the complete UC inventory inputs without running value profiles."""
    from genie_space_optimizer.common.uc_metadata import (
        get_columns_for_tables,
        get_columns_for_tables_rest,
        get_foreign_keys_for_tables,
        get_foreign_keys_for_tables_rest,
    )

    eligible_asset_keys = {
        _identifier_parts(asset.get("identifier"))
        for _kind, asset in _asset_entries(config)
        if len(_identifier_parts(asset.get("identifier"))) == 3
    }
    refs = [
        ref for ref in refs
        if tuple(normalize_component(value) for value in ref) in eligible_asset_keys
    ]
    if not refs:
        raise RuntimeError("Cannot collect wide-schema inventory: Genie space has no eligible assets")
    # The OBO prefetch uses an inline Statement Execution result and therefore
    # cannot prove completeness for very large schemas. Always recollect the
    # durable inventory through per-table UC REST calls (or information_schema)
    # so a truncated prefetch can never become the immutable source of truth.
    _ = prefetched
    try:
        uc_columns = get_columns_for_tables_rest(w, refs)
    except Exception:
        uc_columns = []
    covered_refs = {
        (
            normalize_component(row.get("catalog_name")),
            normalize_component(row.get("schema_name")),
            normalize_component(row.get("table_name") or row.get("table")),
        )
        for row in uc_columns
        if isinstance(row, dict)
    }
    missing_refs = [
        ref for ref in refs
        if tuple(normalize_component(value) for value in ref) not in covered_refs
    ]
    if missing_refs:
        frame = get_columns_for_tables(spark, missing_refs)
        fallback_rows = [row.asDict(recursive=True) for row in frame.collect()]
        existing_columns = {
            (
                normalize_component(row.get("catalog_name")),
                normalize_component(row.get("schema_name")),
                normalize_component(row.get("table_name") or row.get("table")),
                normalize_component(row.get("column_name") or row.get("column")),
            )
            for row in uc_columns
            if isinstance(row, dict)
        }
        uc_columns.extend(
            row for row in fallback_rows
            if isinstance(row, dict)
            and (
                normalize_component(row.get("catalog_name")),
                normalize_component(row.get("schema_name")),
                normalize_component(row.get("table_name") or row.get("table")),
                normalize_component(row.get("column_name") or row.get("column")),
            ) not in existing_columns
        )

    # Older prefetched snapshots omitted catalog/schema. Fill them only when a
    # leaf table name resolves to exactly one configured asset.
    refs_by_leaf: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for ref in refs:
        refs_by_leaf[normalize_component(ref[2])].append(ref)
    for row in uc_columns:
        if not isinstance(row, dict):
            continue
        if row.get("catalog_name") and row.get("schema_name"):
            continue
        matches = refs_by_leaf.get(normalize_component(row.get("table_name")), [])
        if len(matches) == 1:
            row["catalog_name"], row["schema_name"], _ = matches[0]

    foreign_keys: list[dict[str, Any]] = []
    try:
        foreign_keys = get_foreign_keys_for_tables_rest(w, refs)
    except Exception:
        foreign_keys = []
    if not foreign_keys:
        foreign_keys = get_foreign_keys_for_tables(spark, refs)

    try:
        from genie_space_optimizer.common.metric_view_catalog import (
            detect_metric_views_via_catalog_with_outcomes,
        )

        _detected_mvs, metric_view_yamls, _outcomes = (
            detect_metric_views_via_catalog_with_outcomes(
                spark,
                refs,
                w=w,
                warehouse_id=warehouse_id,
            )
        )
    except Exception:
        metric_view_yamls = {}
    if metric_view_yamls:
        merged_yamls = copy.deepcopy(config.get("_metric_view_yaml") or {})
        merged_yamls.update(metric_view_yamls)
        config["_metric_view_yaml"] = merged_yamls
        parsed = config.get("_parsed_space")
        if isinstance(parsed, dict):
            parsed["_metric_view_yaml"] = copy.deepcopy(merged_yamls)

    inventory = build_inventory(uc_columns, config, foreign_keys=foreign_keys)
    asset_columns = {tuple(asset["asset_key"]): len(asset.get("columns") or []) for asset in inventory["assets"]}
    empty = [render_identifier(key) for key, count in asset_columns.items() if count == 0]
    if empty or len(asset_columns) < len({tuple(normalize_component(v) for v in ref) for ref in refs}):
        raise RuntimeError(f"Complete UC inventory could not be collected for all assets; empty={empty}")
    return inventory, uc_columns, foreign_keys


def validate_inventory(inventory: dict[str, Any]) -> None:
    if inventory.get("contract_version") != CONTRACT_VERSION:
        raise ValueError("unsupported wide-schema inventory contract version")
    assets = inventory.get("assets")
    if not isinstance(assets, list):
        raise ValueError("wide-schema inventory assets must be a list")
    expected = content_hash({"contract_version": CONTRACT_VERSION, "assets": assets})
    if inventory.get("inventory_hash") != expected:
        raise ValueError("wide-schema inventory hash mismatch")
    seen_assets: set[tuple[str, ...]] = set()
    seen_columns: set[tuple[str, ...]] = set()
    for asset in assets:
        key = tuple(asset.get("asset_key") or [])
        if len(key) != 3 or key in seen_assets:
            raise ValueError(f"invalid or duplicate asset key: {key}")
        if asset.get("asset_type") not in {"table", "view", "metric_view"}:
            raise ValueError(f"invalid asset type for {key}")
        seen_assets.add(key)
        for column in asset.get("columns") or []:
            column_key = tuple(column.get("column_key") or [])
            if len(column_key) != 4 or column_key[:3] != key or column_key in seen_columns:
                raise ValueError(f"invalid or duplicate column key: {column_key}")
            if column.get("metric_role") not in {None, "dimension", "measure"}:
                raise ValueError(f"invalid metric role for {column_key}")
            if not set(column.get("constraint_roles") or []) <= {
                "primary_key", "foreign_key", "join_key"
            }:
                raise ValueError(f"invalid constraint role for {column_key}")
            seen_columns.add(column_key)


def inventory_indexes(inventory: dict[str, Any]) -> tuple[dict[tuple[str, str, str], dict], dict[tuple[str, str, str, str], dict]]:
    validate_inventory(inventory)
    assets: dict[tuple[str, str, str], dict] = {}
    columns: dict[tuple[str, str, str, str], dict] = {}
    for asset in inventory.get("assets") or []:
        asset_key = tuple(asset["asset_key"])
        assets[asset_key] = asset
        for column in asset.get("columns") or []:
            columns[tuple(column["column_key"])] = column
    return assets, columns


def _tokens(value: Any) -> set[str]:
    return set(_token_sequence(value))


def _token_sequence(value: Any) -> list[str]:
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", str(value or ""))
    tokens = re.findall(r"[a-z0-9]+", text.casefold().replace("_", " "))
    return [token for token in tokens if len(token) > 1 and token not in _STOP_WORDS]


def _space_text(config: dict[str, Any]) -> str:
    parsed = _parsed_space(config)
    values: list[str] = []
    for key in ("title", "name", "description"):
        values.append(str(parsed.get(key) or config.get(key) or ""))
    cfg = parsed.get("config") if isinstance(parsed.get("config"), dict) else {}
    for q in cfg.get("sample_questions") or []:
        if isinstance(q, dict):
            question = q.get("question")
            if isinstance(question, list):
                values.extend(str(v) for v in question)
            elif question:
                values.append(str(question))
    instructions = parsed.get("instructions") if isinstance(parsed.get("instructions"), dict) else {}
    for item in instructions.get("text_instructions") or []:
        if isinstance(item, dict):
            content = item.get("content")
            if isinstance(content, list):
                values.extend(str(v) for v in content)
            elif content:
                values.append(str(content))
    for question in (parsed.get("benchmarks") or {}).get("questions", []) if isinstance(parsed.get("benchmarks"), dict) else []:
        if isinstance(question, dict):
            text = question.get("question")
            if isinstance(text, list):
                values.extend(str(v) for v in text)
            elif text:
                values.append(str(text))
    return "\n".join(values)


def _resolve_asset(
    parts: tuple[str, ...],
    asset_index: dict[tuple[str, str, str], dict],
    diagnostics: dict[str, int] | None = None,
) -> tuple[str, str, str] | None:
    if len(parts) == 3:
        return parts if parts in asset_index else None
    matches = [key for key in asset_index if key[-len(parts):] == parts]
    if len(matches) > 1 and diagnostics is not None:
        diagnostics["ambiguous_asset_references"] = (
            diagnostics.get("ambiguous_asset_references", 0) + 1
        )
    return matches[0] if len(matches) == 1 else None


def sql_column_evidence(
    sql: str,
    inventory: dict[str, Any],
    *,
    diagnostics: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    """Resolve SQL columns to canonical inventory keys using sqlglot."""
    if not str(sql or "").strip():
        return []
    try:
        import sqlglot
        from sqlglot import expressions as exp

        tree = sqlglot.parse_one(sql, read="databricks")
    except Exception:
        return []
    asset_index, column_index = inventory_indexes(inventory)
    aliases: dict[str, tuple[str, str, str]] = {}
    resolved_assets: set[tuple[str, str, str]] = set()
    cte_names = {
        normalize_component(cte.alias_or_name)
        for cte in tree.find_all(exp.CTE)
        if cte.alias_or_name
    }
    cte_aliases: dict[str, str] = {}
    for table in tree.find_all(exp.Table):
        parts = tuple(normalize_component(v) for v in (table.catalog, table.db, table.name) if v)
        asset_key = _resolve_asset(parts, asset_index, diagnostics)
        if not asset_key:
            leaf = normalize_component(table.name)
            if leaf in cte_names:
                cte_aliases[normalize_component(table.alias_or_name)] = leaf
                cte_aliases[leaf] = leaf
            continue
        resolved_assets.add(asset_key)
        aliases[normalize_component(table.alias_or_name)] = asset_key
        aliases[asset_key[-1]] = asset_key

    cte_outputs: dict[tuple[str, str], tuple[str, str, str, str]] = {}
    for cte in tree.find_all(exp.CTE):
        cte_name = normalize_component(cte.alias_or_name)
        select = cte.this if isinstance(cte.this, exp.Select) else cte.this.find(exp.Select)
        if not isinstance(select, exp.Select):
            continue
        local_assets: set[tuple[str, str, str]] = set()
        local_aliases: dict[str, tuple[str, str, str]] = {}
        for table in select.find_all(exp.Table):
            parts = tuple(
                normalize_component(value)
                for value in (table.catalog, table.db, table.name)
                if value
            )
            asset_key = _resolve_asset(parts, asset_index, diagnostics)
            if asset_key:
                local_assets.add(asset_key)
                local_aliases[normalize_component(table.alias_or_name)] = asset_key
                local_aliases[asset_key[-1]] = asset_key
        for expression in select.expressions:
            if isinstance(expression, exp.Star):
                if diagnostics is not None:
                    diagnostics["select_star_references"] = (
                        diagnostics.get("select_star_references", 0) + 1
                    )
                continue
            output_name = normalize_component(expression.alias_or_name)
            resolved_keys: set[tuple[str, str, str, str]] = set()
            for column in expression.find_all(exp.Column):
                if isinstance(column.this, exp.Star) or column.name == "*":
                    continue
                name = normalize_component(column.name)
                qualifier = normalize_component(column.table)
                asset_key = local_aliases.get(qualifier) if qualifier else None
                if asset_key is None:
                    candidates = [
                        asset for asset in local_assets
                        if (*asset, name) in column_index
                    ]
                    if len(candidates) == 1:
                        asset_key = candidates[0]
                key = (*asset_key, name) if asset_key else None
                if key in column_index:
                    resolved_keys.add(key)
            if output_name and len(resolved_keys) == 1:
                cte_outputs[(cte_name, output_name)] = next(iter(resolved_keys))

    results: dict[tuple[str, str, str, str], str] = {}
    role_rank = {"join": 0, "filter": 1, "group": 2, "aggregate": 3, "order": 4, "projection": 5}

    def _role(node: Any) -> str:
        parent = node.parent
        while parent is not None:
            if isinstance(parent, exp.Join):
                return "join"
            if isinstance(parent, exp.Where) or isinstance(parent, exp.Having):
                return "filter"
            if isinstance(parent, exp.Group):
                return "group"
            if isinstance(parent, exp.AggFunc):
                return "aggregate"
            if isinstance(parent, exp.Order):
                return "order"
            if isinstance(parent, exp.Select):
                return "projection"
            parent = parent.parent
        return "projection"

    for column in tree.find_all(exp.Column):
        if isinstance(column.this, exp.Star) or column.name == "*":
            if diagnostics is not None:
                diagnostics["select_star_references"] = (
                    diagnostics.get("select_star_references", 0) + 1
                )
            continue
        name = normalize_component(column.name)
        qualifier = normalize_component(column.table)
        asset_key = aliases.get(qualifier) if qualifier else None
        column_key: tuple[str, str, str, str] | None = None
        cte_name = cte_aliases.get(qualifier) if qualifier else None
        if cte_name:
            column_key = cte_outputs.get((cte_name, name))
        elif not qualifier:
            cte_candidates = {
                key for (candidate_cte, output_name), key in cte_outputs.items()
                if output_name == name and candidate_cte in cte_names
            }
            if len(cte_candidates) == 1:
                column_key = next(iter(cte_candidates))
        if asset_key is None:
            candidates = [asset for asset in resolved_assets if (*asset, name) in column_index]
            if len(candidates) == 1:
                asset_key = candidates[0]
            elif len(candidates) > 1 and column_key is None and diagnostics is not None:
                diagnostics["ambiguous_column_references"] = (
                    diagnostics.get("ambiguous_column_references", 0) + 1
                )
        if column_key is None:
            column_key = (*asset_key, name) if asset_key else None
        if column_key not in column_index:
            continue
        role = _role(column)
        previous = results.get(column_key)
        if previous is None or role_rank[role] < role_rank[previous]:
            results[column_key] = role
    return [
        {"column_key": list(key), "column_id": render_identifier(key), "sql_role": role}
        for key, role in sorted(results.items())
    ]


def _iter_sql_values(value: Any, path: tuple[str, ...] = ()) -> Iterable[tuple[tuple[str, ...], str]]:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = (*path, str(key))
            if str(key).casefold() in {"sql", "expected_sql", "expr", "expression"}:
                if isinstance(child, list):
                    yield child_path, " ".join(str(v) for v in child)
                elif isinstance(child, str):
                    yield child_path, child
            else:
                yield from _iter_sql_values(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _iter_sql_values(child, (*path, str(index)))


def build_local_evidence(config: dict[str, Any], inventory: dict[str, Any]) -> dict[str, Any]:
    """Extract normalized config, benchmark, semantic, and structural evidence."""
    _assets, columns = inventory_indexes(inventory)
    evidence: dict[tuple[str, str, str, str], dict[str, Any]] = {
        key: {"column_key": list(key), "column_id": render_identifier(key), "reason_codes": set(), "scores": defaultdict(float)}
        for key in columns
    }

    parsed = _parsed_space(config)
    for path, sql in _iter_sql_values(parsed):
        reason = "BENCHMARK_SQL" if "benchmark" in ".".join(path).casefold() or "example_question_sqls" in path else "CONFIG_SQL"
        for resolved in sql_column_evidence(sql, inventory):
            key = tuple(resolved["column_key"])
            evidence[key]["reason_codes"].add(reason)
            evidence[key]["scores"][reason] += QUERY_ROLE_WEIGHTS.get(resolved["sql_role"], 1.0)
            if "join" in path or resolved["sql_role"] == "join":
                evidence[key]["reason_codes"].add("JOIN_KEY")
                evidence[key]["scores"]["JOIN_KEY"] += 1.0

    for key, cfg in _column_config_index(config).items():
        if key not in evidence:
            continue
        if any(cfg.get(field) for field in USER_PIN_FIELDS):
            evidence[key]["reason_codes"].add("USER_PIN")
            evidence[key]["scores"]["USER_PIN"] += 1.0
        if columns[key].get("metric_role") in {"measure", "dimension"}:
            evidence[key]["reason_codes"].add("METRIC_FIELD")
            evidence[key]["scores"]["METRIC_FIELD"] += 1.0

    text = _space_text(config)
    normalized_text = " ".join(_token_sequence(text))
    text_tokens = _tokens(text)
    concepts = {
        "date": {"date", "day", "month", "quarter", "year", "timestamp"},
        "amount": {"amount", "price", "revenue", "sales", "cost", "margin"},
        "quantity": {"quantity", "qty", "count", "units"},
        "status": {"status", "state", "stage"},
        "geography": {"country", "region", "city", "state", "location"},
        "category": {"category", "type", "segment", "class"},
        "identifier": {"id", "key", "identifier", "code"},
    }
    for key, column in columns.items():
        name = column["name"]
        name_tokens = _tokens(name)
        semantic_score = 0.0
        normalized_name = " ".join(_token_sequence(name))
        if normalized_name and normalized_name in normalized_text:
            semantic_score += 8.0
        semantic_score += min(8.0, 2.0 * len(name_tokens & text_tokens))
        semantic_score += min(4.0, float(len(_tokens(column.get("description")) & text_tokens)))
        dtype = str(column.get("data_type") or "").casefold()
        if set(column.get("constraint_roles") or []) & {
            "primary_key", "foreign_key", "join_key"
        }:
            evidence[key]["reason_codes"].add("JOIN_KEY")
            evidence[key]["scores"]["JOIN_KEY"] += 1.0
        if any(name_tokens & words for words in concepts.values()):
            semantic_score += 2.0
        if semantic_score:
            evidence[key]["reason_codes"].add("SEMANTIC_MATCH")
            evidence[key]["scores"]["SEMANTIC_MATCH"] += semantic_score

        dtype_base = dtype.split("(", 1)[0]
        structural = 1.0
        if set(column.get("constraint_roles") or []) & {"primary_key", "foreign_key", "join_key"}:
            structural = 5.0
        elif dtype_base in _DATE_TYPES:
            structural = 4.0
        elif dtype_base in _NUMERIC_TYPES:
            structural = 3.0
        elif dtype_base in {"string", "boolean"}:
            structural = 2.0
        evidence[key]["reason_codes"].add("STRUCTURAL_COVERAGE")
        evidence[key]["scores"]["STRUCTURAL_COVERAGE"] += structural

    rows: list[dict[str, Any]] = []
    for key in sorted(evidence):
        item = evidence[key]
        rows.append({
            "column_key": item["column_key"],
            "column_id": item["column_id"],
            "reason_codes": sorted(item["reason_codes"], key=lambda reason: (PRIORITY_BY_REASON[reason], reason)),
            "scores": dict(sorted(item["scores"].items())),
        })
    return {
        "contract_version": CONTRACT_VERSION,
        "inventory_hash": inventory["inventory_hash"],
        "source_mode": "none",
        "source_scope": [],
        "coverage": {},
        "degradation_counts": {},
        "columns": rows,
    }


def merge_query_history_evidence(local: dict[str, Any], history: dict[str, Any]) -> dict[str, Any]:
    """Merge normalized query-history aggregates into the evidence contract."""
    merged = copy.deepcopy(local)
    by_key = {tuple(row["column_key"]): row for row in merged.get("columns") or []}
    for row in history.get("columns") or []:
        key = tuple(row.get("column_key") or [])
        target = by_key.get(key)
        if target is None:
            continue
        score = float(row.get("evidence_score") or 0.0)
        if score > 0:
            reasons = set(target.get("reason_codes") or [])
            reasons.add("QUERY_HISTORY")
            target["reason_codes"] = sorted(reasons, key=lambda reason: (PRIORITY_BY_REASON[reason], reason))
            target.setdefault("scores", {})["QUERY_HISTORY"] = score
            target["query_history"] = {
                key: copy.deepcopy(value)
                for key, value in row.items()
                if key not in {"column_key", "column_id", "evidence_score"}
            }
    merged["source_mode"] = history.get("source_mode", "none")
    merged["source_scope"] = copy.deepcopy(history.get("source_scope") or [])
    merged["coverage"] = copy.deepcopy(history.get("coverage") or {})
    merged["degradation_counts"] = copy.deepcopy(history.get("degradation_counts") or {})
    merged["source_attempts"] = copy.deepcopy(history.get("source_attempts") or {})
    merged["source_errors"] = copy.deepcopy(history.get("source_errors") or {})
    merged["warnings"] = copy.deepcopy(history.get("warnings") or [])
    return merged


def _structural_category(column: dict[str, Any]) -> str:
    roles = set(column.get("constraint_roles") or [])
    dtype = str(column.get("data_type") or "").casefold().split("(", 1)[0]
    if roles & {"primary_key", "foreign_key", "join_key"}:
        return "identifier"
    if dtype in _DATE_TYPES:
        return "date"
    if dtype in _NUMERIC_TYPES:
        return "numeric"
    if dtype in {"string", "boolean"}:
        return "categorical"
    return "other"


def build_selection_plan(
    inventory: dict[str, Any],
    evidence: dict[str, Any],
    *,
    run_id: str,
    revision: int = 1,
    parent_plan_hash: str | None = None,
) -> dict[str, Any]:
    """Build deterministic selection-plan revision 1."""
    assets, columns = inventory_indexes(inventory)
    if evidence.get("inventory_hash") != inventory.get("inventory_hash"):
        raise ValueError("evidence inventory hash mismatch")
    evidence_by_key = {tuple(row.get("column_key") or []): row for row in evidence.get("columns") or []}
    asset_plans: list[dict[str, Any]] = []

    for asset_key, asset in sorted(assets.items()):
        ranked: list[dict[str, Any]] = []
        for column in asset.get("columns") or []:
            key = tuple(column["column_key"])
            item = evidence_by_key.get(key, {})
            reasons = list(item.get("reason_codes") or ["STRUCTURAL_COVERAGE"])
            scores = item.get("scores") or {}
            hard_reasons = [
                reason for reason in reasons
                if reason in HARD_REQUIRED_REASONS
            ]
            priority = min(
                (PRIORITY_BY_REASON[reason] for reason in hard_reasons),
                default=3,
            )
            hard_score = sum(
                float(scores.get(reason) or 0.0)
                for reason in hard_reasons
                if PRIORITY_BY_REASON[reason] == priority
            )
            history_score = float(scores.get("QUERY_HISTORY") or 0.0)
            metric_score = float(scores.get("METRIC_FIELD") or 0.0)
            semantic_score = float(scores.get("SEMANTIC_MATCH") or 0.0)
            structural_score = float(scores.get("STRUCTURAL_COVERAGE") or 0.0)
            ranked.append({
                "column_key": list(key),
                "column_id": column["column_id"],
                "name": column["name"],
                "data_type": column.get("data_type", ""),
                "metric_role": column.get("metric_role"),
                "constraint_roles": copy.deepcopy(column.get("constraint_roles") or []),
                "priority": priority,
                "evidence_score": sum(
                    float(value or 0.0) for value in scores.values()
                ),
                "hard_evidence_score": hard_score,
                "query_history_score": history_score,
                "metric_field_score": metric_score,
                "semantic_score": semantic_score,
                "structural_score": structural_score,
                "reason_codes": reasons,
                "structural_category": _structural_category(column),
            })
        ranked.sort(key=lambda item: (
            item["priority"],
            -item["hard_evidence_score"],
            -item["query_history_score"],
            -item["metric_field_score"],
            -item["semantic_score"],
            -item["structural_score"],
            item["column_id"],
        ))
        for index, item in enumerate(ranked, start=1):
            item["stable_rank"] = index

        directly_required = [
            item for item in ranked
            if HARD_REQUIRED_REASONS & set(item["reason_codes"])
        ]
        required_overflow_count = max(0, len(directly_required) - MAX_ACTIVE_COLUMNS_PER_ASSET)
        selected: list[dict[str, Any]] = directly_required[:MAX_ACTIVE_COLUMNS_PER_ASSET]
        selected_ids = {item["column_id"] for item in selected}
        if len(selected) < INITIAL_CORE_TARGET:
            for item in ranked:
                if item["column_id"] in selected_ids or item["priority"] == 6:
                    continue
                selected.append(item)
                selected_ids.add(item["column_id"])
                if len(selected) >= INITIAL_CORE_TARGET:
                    break

        exploration_slots = max(0, min(EXPLORATION_TARGET, INITIAL_ACTIVE_TARGET - len(selected)))
        remaining = [item for item in ranked if item["column_id"] not in selected_ids]
        chosen_categories = {item["structural_category"] for item in selected}
        def exploration_hash(item: dict[str, Any]) -> str:
            return hashlib.sha256(
                f"{run_id}{item['column_id']}".encode("utf-8")
            ).hexdigest()
        exploration_order: list[dict[str, Any]] = []
        for category in ("identifier", "date", "numeric", "categorical", "other"):
            if category in chosen_categories:
                continue
            candidates = [
                item for item in remaining
                if item["structural_category"] == category
            ]
            if candidates:
                exploration_order.append(min(candidates, key=exploration_hash))
        exploration_ids = {item["column_id"] for item in exploration_order}
        exploration_order.extend(sorted(
            (
                item for item in remaining
                if item["column_id"] not in exploration_ids
            ),
            key=exploration_hash,
        ))
        for item in exploration_order[:exploration_slots]:
            item["reason_codes"] = sorted(set(item["reason_codes"]) | {"EXPLORATION"}, key=lambda reason: (PRIORITY_BY_REASON[reason], reason))
            selected.append(item)
            selected_ids.add(item["column_id"])
            chosen_categories.add(item["structural_category"])

        minimum = min(MIN_INITIAL_TARGET, len(ranked))
        for item in ranked:
            if len(selected) >= minimum or len(selected) >= INITIAL_ACTIVE_TARGET:
                break
            if item["column_id"] not in selected_ids:
                selected.append(item)
                selected_ids.add(item["column_id"])

        selected = selected[:MAX_ACTIVE_COLUMNS_PER_ASSET]
        selected_ids = {item["column_id"] for item in selected}
        rows: list[dict[str, Any]] = []
        for item in ranked:
            active = item["column_id"] in selected_ids
            rows.append({
                **item,
                "active": active,
                "activation_reason": "initial_selection" if active else None,
                "eviction_reason": None,
                "profile_status": "metadata_only" if active and item.get("metric_role") == "measure" else ("pending" if active else "not_selected"),
                "available_metrics": [],
                "cumulatively_value_profiled": False,
            })
        asset_plans.append({
            "asset_key": list(asset_key),
            "asset_id": asset["asset_id"],
            "asset_type": asset["asset_type"],
            "columns": rows,
            "active_count": len(selected),
            "cumulative_value_profiled_count": 0,
            "selected_count": len(selected),
            "omitted_count": max(0, len(ranked) - len(selected)),
            "required_overflow_count": required_overflow_count,
            "metadata_only_count": sum(1 for row in rows if row["active"] and row["profile_status"] == "metadata_only"),
        })

    plan = {
        "contract_version": CONTRACT_VERSION,
        "selector_version": SELECTOR_VERSION,
        "run_id": run_id,
        "revision": revision,
        "parent_plan_hash": parent_plan_hash,
        "inventory_hash": inventory["inventory_hash"],
        "source_mode": evidence.get("source_mode", "none"),
        "evidence_coverage": copy.deepcopy(evidence.get("coverage") or {}),
        "evidence_degradation_counts": copy.deepcopy(
            evidence.get("degradation_counts") or {}
        ),
        "evidence_source_attempts": copy.deepcopy(
            evidence.get("source_attempts") or {}
        ),
        "evidence_source_errors": copy.deepcopy(
            evidence.get("source_errors") or {}
        ),
        "evidence_warnings": copy.deepcopy(evidence.get("warnings") or []),
        "profiling_budget": {
            "submitted_statements": 0,
            "elapsed_ms": 0,
            "asset_statement_counts": {},
        },
        "assets": asset_plans,
    }
    plan["plan_hash"] = content_hash(plan)
    validate_selection_plan(plan, inventory_hash=inventory["inventory_hash"])
    return plan


def validate_selection_plan(plan: dict[str, Any], *, inventory_hash: str | None = None) -> None:
    raw = copy.deepcopy(plan)
    supplied_hash = raw.pop("plan_hash", None)
    if supplied_hash != content_hash(raw):
        raise ValueError("wide-schema selection plan hash mismatch")
    if inventory_hash and plan.get("inventory_hash") != inventory_hash:
        raise ValueError("wide-schema selection plan inventory hash mismatch")
    profiling_budget = plan.get("profiling_budget") or {}
    if int(profiling_budget.get("submitted_statements") or 0) > MAX_PROFILING_STATEMENTS_PER_RUN:
        raise ValueError("run-wide profiling statement budget exceeded")
    if any(
        int(count or 0) > MAX_PROFILING_STATEMENTS_PER_ASSET
        for count in (profiling_budget.get("asset_statement_counts") or {}).values()
    ):
        raise ValueError("per-asset profiling statement budget exceeded")
    for asset in plan.get("assets") or []:
        columns = asset.get("columns") or []
        active = [row for row in columns if row.get("active")]
        cumulative = [row for row in columns if row.get("cumulatively_value_profiled")]
        if len(active) > MAX_ACTIVE_COLUMNS_PER_ASSET:
            raise ValueError(f"active-column budget exceeded for {asset.get('asset_id')}")
        if len(cumulative) > MAX_CUMULATIVE_PROFILED_COLUMNS_PER_ASSET:
            raise ValueError(f"cumulative profile budget exceeded for {asset.get('asset_id')}")
        if any(row.get("profile_status") not in PROFILE_STATUSES for row in columns):
            raise ValueError(f"invalid profile status for {asset.get('asset_id')}")


def active_column_keys(plan: dict[str, Any]) -> set[tuple[str, str, str, str]]:
    validate_selection_plan(plan)
    return {
        tuple(row["column_key"])
        for asset in plan.get("assets") or []
        for row in asset.get("columns") or []
        if row.get("active")
    }


def project_active_inventory(inventory: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Return legacy UC-column rows for active prompt/profile consumers."""
    active = active_column_keys(plan)
    plan_rows = {
        tuple(row["column_key"]): row
        for asset in plan.get("assets") or []
        for row in asset.get("columns") or []
    }
    projected: list[dict[str, Any]] = []
    for asset in inventory.get("assets") or []:
        for column in asset.get("columns") or []:
            key = tuple(column["column_key"])
            if key not in active:
                continue
            projected.append({
                "catalog_name": key[0],
                "schema_name": key[1],
                "table_name": key[2],
                "column_name": key[3],
                "data_type": column.get("data_type", ""),
                "comment": column.get("description", ""),
                "column_type": column.get("metric_role"),
                "ordinal_position": column.get("ordinal"),
                "stable_rank": plan_rows.get(key, {}).get("stable_rank"),
                "reason_codes": copy.deepcopy(
                    plan_rows.get(key, {}).get("reason_codes") or []
                ),
                "profile_status": plan_rows.get(key, {}).get("profile_status"),
            })
    return projected


def project_full_inventory(inventory: dict[str, Any]) -> list[dict[str, Any]]:
    """Return legacy UC rows for deterministic validation only.

    Callers must never use this projection to construct an LLM request.  It is
    intentionally a separate API from :func:`project_active_inventory` so a
    prompt allowlist cannot silently become the full deterministic allowlist.
    """
    validate_inventory(inventory)
    projected: list[dict[str, Any]] = []
    for asset in inventory.get("assets") or []:
        for column in asset.get("columns") or []:
            key = tuple(column["column_key"])
            projected.append({
                "catalog_name": key[0],
                "schema_name": key[1],
                "table_name": key[2],
                "column_name": key[3],
                "data_type": column.get("data_type", ""),
                "comment": column.get("description", ""),
                "column_type": column.get("metric_role"),
                "ordinal_position": column.get("ordinal"),
            })
    return projected


def revise_plan_for_column(
    plan: dict[str, Any],
    inventory: dict[str, Any],
    column_key: tuple[str, str, str, str],
    *,
    reason: str = "REPAIR_FAILURE",
    protected_column_keys: Iterable[tuple[str, str, str, str]] = (),
) -> dict[str, Any]:
    """Append-ready operation revision activating an omitted valid column."""
    validate_selection_plan(plan, inventory_hash=inventory["inventory_hash"])
    _assets, columns = inventory_indexes(inventory)
    if column_key not in columns:
        raise ValueError(f"column is absent from full inventory: {render_identifier(column_key)}")
    revised = copy.deepcopy(plan)
    parent_hash = revised.pop("plan_hash")
    revised["revision"] = int(revised.get("revision") or 0) + 1
    revised["parent_plan_hash"] = parent_hash
    target_asset = next((a for a in revised["assets"] if tuple(a["asset_key"]) == column_key[:3]), None)
    if target_asset is None:
        raise ValueError("selection plan is missing target asset")
    target = next((row for row in target_asset["columns"] if tuple(row["column_key"]) == column_key), None)
    if target is None:
        raise ValueError("selection plan is missing target column")
    if not target.get("active"):
        protected = set(protected_column_keys) | {column_key}
        active = [row for row in target_asset["columns"] if row.get("active")]
        if len(active) >= MAX_ACTIVE_COLUMNS_PER_ASSET:
            evictable = [
                row for row in active
                if tuple(row["column_key"]) not in protected
                and not ({"USER_PIN", "JOIN_KEY"} & set(row.get("reason_codes") or []))
            ]
            if not evictable:
                raise ValueError("active working set is full and has no evictable column")
            victim = max(
                evictable,
                key=lambda row: (
                    int(row.get("stable_rank") or 999999),
                    row["column_id"],
                ),
            )
            victim["active"] = False
            victim["eviction_reason"] = f"promoted:{target['column_id']}"
            if not victim.get("cumulatively_value_profiled"):
                victim["profile_status"] = "not_selected"
        target["active"] = True
        target["activation_reason"] = reason.casefold()
        reasons = set(target.get("reason_codes") or []) | {reason}
        target["reason_codes"] = sorted(reasons, key=lambda value: (PRIORITY_BY_REASON.get(value, 99), value))
        target["priority"] = min(PRIORITY_BY_REASON.get(value, 99) for value in reasons)
        cumulative_count = sum(1 for row in target_asset["columns"] if row.get("cumulatively_value_profiled"))
        if target.get("cumulatively_value_profiled"):
            # A previously profiled column keeps its outcome when reactivated;
            # reactivation must not submit duplicate value-profile SQL.
            pass
        elif target.get("metric_role") == "measure" or cumulative_count >= MAX_CUMULATIVE_PROFILED_COLUMNS_PER_ASSET:
            target["profile_status"] = "metadata_only"
        else:
            target["profile_status"] = "pending"
    _refresh_asset_counts(target_asset)
    revised["plan_hash"] = content_hash(revised)
    validate_selection_plan(revised, inventory_hash=inventory["inventory_hash"])
    return revised


def revise_plan_with_profile_outcomes(
    plan: dict[str, Any],
    inventory: dict[str, Any],
    outcomes: dict[tuple[str, str, str, str], dict[str, Any]],
    *,
    profiling_budget: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create the subsequent immutable revision recording profile outcomes."""
    validate_selection_plan(plan, inventory_hash=inventory["inventory_hash"])
    revised = copy.deepcopy(plan)
    parent_hash = revised.pop("plan_hash")
    revised["revision"] = int(revised.get("revision") or 0) + 1
    revised["parent_plan_hash"] = parent_hash
    for asset in revised.get("assets") or []:
        for row in asset.get("columns") or []:
            key = tuple(row["column_key"])
            outcome = outcomes.get(key)
            if outcome is None:
                continue
            status = str(outcome.get("profile_status") or "metadata_only")
            if status not in PROFILE_STATUSES:
                raise ValueError(f"invalid profile outcome status: {status}")
            submitted = bool(outcome.get("submitted"))
            if submitted and not row.get("cumulatively_value_profiled"):
                current_count = sum(1 for item in asset["columns"] if item.get("cumulatively_value_profiled"))
                if current_count >= MAX_CUMULATIVE_PROFILED_COLUMNS_PER_ASSET:
                    raise ValueError("profile outcome would exceed cumulative budget")
                row["cumulatively_value_profiled"] = True
            row["profile_status"] = status
            row["available_metrics"] = sorted(set(str(v) for v in outcome.get("available_metrics") or []))
        _refresh_asset_counts(asset)
    if profiling_budget is not None:
        revised["profiling_budget"] = copy.deepcopy(profiling_budget)
    revised["plan_hash"] = content_hash(revised)
    validate_selection_plan(revised, inventory_hash=inventory["inventory_hash"])
    return revised


def _refresh_asset_counts(asset: dict[str, Any]) -> None:
    columns = asset.get("columns") or []
    active = [row for row in columns if row.get("active")]
    asset["active_count"] = len(active)
    asset["selected_count"] = len(active)
    asset["omitted_count"] = max(0, len(columns) - len(active))
    asset["cumulative_value_profiled_count"] = sum(1 for row in columns if row.get("cumulatively_value_profiled"))
    asset["metadata_only_count"] = sum(1 for row in active if row.get("profile_status") == "metadata_only")


def validate_column_reference(inventory: dict[str, Any], column_key: tuple[str, str, str, str]) -> bool:
    """Deterministic validation always consults the complete inventory."""
    _assets, columns = inventory_indexes(inventory)
    return column_key in columns
