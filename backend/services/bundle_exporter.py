"""
Genie Space -> Databricks Asset Bundle (DAB) exporter.

Pure transform: given a fetched serialized_space (the dict returned by
``get_serialized_space``) plus a little target metadata, produce the set of
files that make up a deployable DAB whose data-source references are
parameterized via ``${var.catalog}.${var.schema}``.

The transform has no Databricks dependency — it takes a dict and returns
``{relative_path: file_contents}``. The router layer is responsible for
fetching the space and zipping the result.

Why parameterize at all: Databricks Asset Bundles support Genie spaces
(CLI v1.3.0+, ``engine: direct``). Variable interpolation (``${var.x}``)
resolves inside an inlined ``serialized_space`` across every section — data
sources, example SQL, join specs, and benchmarks — so the same space
definition can deploy against a different catalog/schema in another
workspace just by overriding the target's variables. That is what turns a
hand-authored space into something promotable across environments.
"""

from __future__ import annotations

import logging
import re
from collections import Counter

import yaml

logger = logging.getLogger(__name__)

_VAR_PREFIX = "${var.catalog}.${var.schema}"

# Three-part dotted identifier, each part optionally backticked.
_THREE_PART = re.compile(
    r"`?([A-Za-z0-9_]+)`?\.`?([A-Za-z0-9_]+)`?\.`?([A-Za-z0-9_]+)`?"
)


# ── YAML emit ─────────────────────────────────────────────────────────────────
# Emit a hand-authored-looking bundle: unquoted ${var.x} tokens, block-literal
# style for multi-line SQL bodies. DABs interpolates the token regardless of
# quoting, but unquoted reads cleanly and matches the proven artifact.


class _BundleDumper(yaml.SafeDumper):
    pass


def _str_representer(dumper: yaml.Dumper, data: str):
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


_BundleDumper.add_representer(str, _str_representer)


def _dump_yaml(obj: dict) -> str:
    return yaml.dump(
        obj,
        Dumper=_BundleDumper,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=10_000,  # don't wrap long descriptions / SQL
    )


# ── Prefix detection ──────────────────────────────────────────────────────────


def _walk_strings(obj):
    """Yield every string leaf in a nested dict/list structure."""
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _walk_strings(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk_strings(item)


def table_identifiers(space: dict) -> list[str]:
    """Return all declared table / metric-view identifiers in the space."""
    ds = space.get("data_sources", {}) or {}
    out: list[str] = []
    for key in ("tables", "metric_views"):
        for t in ds.get(key, []) or []:
            if isinstance(t, dict) and isinstance(t.get("identifier"), str):
                out.append(t["identifier"])
    return out


def detect_prefixes(space: dict) -> Counter:
    """Count ``(catalog, schema)`` prefixes across every three-part identifier
    found anywhere in the space.

    Detection is anchored on declared table identifiers first (authoritative),
    then sweeps all string leaves to catch references buried in example SQL,
    join specs, snippets, and benchmarks.
    """
    counts: Counter = Counter()
    for ident in table_identifiers(space):
        m = _THREE_PART.match(ident.strip())
        if m:
            counts[(m.group(1), m.group(2))] += 1
    for s in _walk_strings(space):
        for m in _THREE_PART.finditer(s):
            counts[(m.group(1), m.group(2))] += 1
    return counts


def pick_source_prefix(space: dict) -> tuple[str, str] | None:
    """Return the dominant ``(catalog, schema)`` prefix, or None if the space
    has no three-part identifiers at all."""
    counts = detect_prefixes(space)
    if not counts:
        return None
    return counts.most_common(1)[0][0]


# ── Parameterization ──────────────────────────────────────────────────────────


def _parameterize_string(s: str, src_catalog: str, src_schema: str) -> str:
    """Replace the source ``catalog.schema`` prefix with the var token, covering
    both plain and per-part-backticked forms. Anchored on the known source
    prefix so prose like ``a.b.c`` is never mangled."""
    s = s.replace(f"{src_catalog}.{src_schema}.", f"{_VAR_PREFIX}.")
    s = s.replace(f"`{src_catalog}`.`{src_schema}`.", f"{_VAR_PREFIX}.")
    return s


def _parameterize(obj, src_catalog: str, src_schema: str):
    if isinstance(obj, str):
        return _parameterize_string(obj, src_catalog, src_schema)
    if isinstance(obj, dict):
        return {k: _parameterize(v, src_catalog, src_schema) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_parameterize(v, src_catalog, src_schema) for v in obj]
    return obj


# ── Bundle assembly ────────────────────────────────────────────────────────────


def _slug(text: str, fallback: str) -> str:
    """Make a safe lowercase identifier for bundle/resource names."""
    s = re.sub(r"[^a-z0-9_]+", "_", (text or "").lower()).strip("_")
    return s or fallback


def export_space_as_bundle(
    serialized_space: dict,
    *,
    title: str,
    description: str,
    source_catalog: str,
    source_schema: str,
    dev_host: str,
    dev_warehouse_id: str,
    bundle_name: str | None = None,
    resource_key: str | None = None,
    prod_host: str | None = None,
    prod_catalog: str | None = None,
    prod_schema: str | None = None,
    prod_warehouse_id: str | None = None,
) -> dict[str, str]:
    """Produce ``{relative_path: contents}`` for a parameterized Genie-space bundle.

    The ``dev`` target points back at the source workspace/catalog/schema (the
    dev-first assumption — you author in dev, then promote). A ``prod`` target
    is emitted only when all ``prod_*`` arguments are supplied.

    Args:
        serialized_space: The space config dict (from ``get_serialized_space``).
        title / description: Space metadata for the resource block.
        source_catalog / source_schema: The prefix to parameterize away. Use
            ``pick_source_prefix`` to detect this from the space.
        dev_host / dev_warehouse_id: The source workspace + warehouse.
        bundle_name / resource_key: Optional explicit names; derived from the
            title when omitted.
        prod_*: Optional second target for promotion.

    Returns:
        Mapping of bundle-relative path -> file contents (UTF-8 text).
    """
    bundle_name = bundle_name or _slug(title, "genie_space_bundle")
    resource_key = resource_key or _slug(title, "genie_space")

    parameterized = _parameterize(serialized_space, source_catalog, source_schema)

    # ---- resources/<key>.genie_space.yml ----
    resource_doc = {
        "resources": {
            "genie_spaces": {
                resource_key: {
                    "title": title,
                    "description": description or "Exported by Genie Workbench",
                    "warehouse_id": "${var.warehouse_id}",
                    "serialized_space": parameterized,
                }
            }
        }
    }

    # ---- databricks.yml ----
    targets: dict = {
        "dev": {
            "default": True,
            "mode": "development",
            "workspace": {"host": dev_host},
            "variables": {
                "catalog": source_catalog,
                "schema": source_schema,
                "warehouse_id": str(dev_warehouse_id),
            },
        }
    }
    if prod_host and prod_catalog and prod_schema and prod_warehouse_id:
        targets["prod"] = {
            "mode": "production",
            "workspace": {"host": prod_host},
            "variables": {
                "catalog": prod_catalog,
                "schema": prod_schema,
                "warehouse_id": str(prod_warehouse_id),
            },
        }

    databricks_doc = {
        "bundle": {
            "name": bundle_name,
            # Genie spaces require the direct deployment engine (CLI v1.3.0+).
            "engine": "direct",
        },
        "include": ["resources/*.yml"],
        "variables": {
            "catalog": {"description": "Unity Catalog catalog holding the tables."},
            "schema": {"description": "Schema (database) holding the tables."},
            "warehouse_id": {
                "description": "SQL warehouse Genie uses to run generated queries."
            },
        },
        "targets": targets,
    }

    readme = _render_readme(
        title=title,
        has_prod=("prod" in targets),
        n_tables=len(table_identifiers(serialized_space)),
        source_catalog=source_catalog,
        source_schema=source_schema,
    )

    return {
        "databricks.yml": _dump_yaml(databricks_doc),
        f"resources/{resource_key}.genie_space.yml": (
            "# Generated by Genie Workbench — exported from a live Genie space.\n"
            "# All catalog/schema references parameterized via "
            "${var.catalog}.${var.schema}.\n"
            + _dump_yaml(resource_doc)
        ),
        "README.md": readme,
    }


def _render_readme(
    *,
    title: str,
    has_prod: bool,
    n_tables: int,
    source_catalog: str,
    source_schema: str,
) -> str:
    prod_line = (
        "databricks bundle deploy -t prod   # promote to the prod workspace\n"
        if has_prod
        else ""
    )
    prod_note = (
        ""
        if has_prod
        else (
            "\nThis export only defines a `dev` target (the source workspace). To "
            "promote to another workspace, add a `prod` target block with that "
            "workspace's `host`, `catalog`, `schema`, and `warehouse_id`.\n"
        )
    )
    return f"""# {title} — Genie Space bundle

Exported by **Genie Workbench**. This Databricks Asset Bundle deploys the Genie
space **{title}** ({n_tables} source tables) and parameterizes every
catalog/schema reference so the same definition can target different workspaces.

The source space referenced `{source_catalog}.{source_schema}.*`; that prefix is
now `${{var.catalog}}.${{var.schema}}.*`, overridden per target below.

## Prerequisites
- Databricks CLI **v1.3.0+** (`databricks --version`)
- Auth configured for each target workspace (`databricks auth login`)

## Deploy
```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev    # deploy to the source (dev) workspace
{prod_line}```
{prod_note}
## How parameterization works
`databricks.yml` declares `catalog`, `schema`, and `warehouse_id` variables.
Each target overrides them, and DABs interpolates the tokens inside the inlined
`serialized_space` — across data sources, example SQL, join specs, and
benchmarks. Promoting to a new environment is just a new target block; the space
definition itself never changes.

> **Round-trip note:** edits made later in the Genie UI do **not** flow back into
> this bundle. Re-export from Genie Workbench (or run `databricks bundle
> generate`) to refresh it.
"""
