"""Maturity config loader — default YAML + admin overrides from Lakebase."""

import copy
import json
import logging
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

_CONFIG_YAML = Path(__file__).resolve().parent.parent / "maturity_config_default.yaml"

# Cached default config (loaded once at import time)
_default_config: dict | None = None


def _load_default() -> dict:
    """Load the default maturity config from the bundled YAML file."""
    global _default_config
    if _default_config is None:
        with open(_CONFIG_YAML) as f:
            _default_config = yaml.safe_load(f)
        logger.info("Loaded default maturity config (version %s)", _default_config.get("version"))
    return _default_config


def get_default_config() -> dict:
    """Return a deep copy of the default config."""
    return copy.deepcopy(_load_default())


def merge_config(base: dict, overrides: dict) -> dict:
    """Merge admin overrides into the base config.

    Override rules:
      - stages: replaced wholesale if present in overrides
      - criteria: merged by id — override fields win, new ids are appended
      - version: overrides win
    """
    merged = copy.deepcopy(base)

    if "version" in overrides:
        merged["version"] = overrides["version"]

    if "stages" in overrides:
        merged["stages"] = overrides["stages"]

    if "criteria" in overrides:
        # Build lookup from base
        base_by_id = {c["id"]: c for c in merged.get("criteria", [])}

        for override_criterion in overrides["criteria"]:
            cid = override_criterion["id"]
            if cid in base_by_id:
                # Merge fields — override wins
                base_by_id[cid].update(override_criterion)
            else:
                # New criterion from admin
                base_by_id[cid] = override_criterion

        merged["criteria"] = list(base_by_id.values())

    return merged


async def get_active_config() -> dict:
    """Get the active maturity config (default + admin overrides).

    Tries to load admin overrides from Lakebase. Falls back to default
    if Lakebase is unavailable or no overrides are stored.
    """
    base = get_default_config()

    try:
        overrides = await _load_admin_overrides()
        if overrides:
            return merge_config(base, overrides)
    except Exception as e:
        logger.warning("Failed to load admin config overrides: %s", e)

    return base


async def save_admin_overrides(overrides: dict) -> None:
    """Persist admin config overrides to Lakebase."""
    from backend.services.lakebase import _pool, _lakebase_available, _memory_store

    config_json = json.dumps(overrides)

    if not _lakebase_available or _pool is None:
        _memory_store.setdefault("maturity_config", {})
        _memory_store["maturity_config"]["overrides"] = overrides
        logger.info("Saved maturity config overrides (in-memory)")
        return

    async with _pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO maturity_config (id, config_json, updated_at)
            VALUES ('active', $1, NOW())
            ON CONFLICT (id) DO UPDATE SET
                config_json = EXCLUDED.config_json,
                updated_at = NOW()
        """, config_json)
    logger.info("Saved maturity config overrides to Lakebase")


async def _load_admin_overrides() -> Optional[dict]:
    """Load admin config overrides from Lakebase."""
    from backend.services.lakebase import _pool, _lakebase_available, _memory_store

    if not _lakebase_available or _pool is None:
        return _memory_store.get("maturity_config", {}).get("overrides")

    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT config_json FROM maturity_config WHERE id = 'active'"
        )
        if row:
            return json.loads(row["config_json"])
    return None
