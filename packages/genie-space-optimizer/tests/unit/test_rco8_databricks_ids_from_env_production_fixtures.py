"""RCO-8 — production-shape fixtures for ``_databricks_ids_from_env``.

Pin the helper's current behavior on three production-shape paths:

  * env-only: all three IDs come from environment variables.
  * sentinel-only: no env, dbutils import fails → ``"unknown"`` for
    every field.
  * mixed: partial env + dbutils tags resolve the rest.

D-5 in C14-V regressed because the unit test exercised the API but not
the production Databricks Jobs runtime. These fixtures inject both
sides (env via monkeypatch, dbutils via ``sys.modules``) so the
production-shape behavior is locked.

The plan is explicit: do NOT change ``_databricks_ids_from_env``. If
a fixture surfaces a defect, file a separate plan.
"""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest

from genie_space_optimizer.optimization.harness import (
    _databricks_ids_from_env,
)
from tests.unit.fixtures.rco8._loader import load_json_pairs


_ENV_KEYS = (
    "DATABRICKS_JOB_ID",
    "DATABRICKS_RUN_ID",
    "DATABRICKS_JOB_RUN_ID",
    "DATABRICKS_TASK_RUN_ID",
)

_CASES = load_json_pairs("databricks_ids_from_env")


def _case_id(case: tuple[str, dict, dict]) -> str:
    return case[0]


def _install_fake_dbutils(monkeypatch, tags: dict[str, str]) -> None:
    """Inject a fake ``pyspark.dbutils`` + ``pyspark.sql`` chain so the
    helper's dbutils path resolves to the supplied tag dict.

    The chain mirrors what ``_databricks_ids_from_env`` walks at
    harness.py:295-308: DBUtils → entry_point → getDbutils → notebook →
    getContext → tags → get(key) → isDefined() / get().
    """

    class _StubVal:
        def __init__(self, value: str) -> None:
            self._value = value
            self._defined = bool(value)

        def isDefined(self) -> bool:
            return self._defined

        def get(self) -> str:
            return self._value

    class _StubTags:
        def __init__(self, tags: dict[str, str]) -> None:
            self._tags = tags

        def get(self, key: str) -> _StubVal:
            return _StubVal(str(self._tags.get(key, "")))

    class _StubContext:
        def __init__(self, tags: dict[str, str]) -> None:
            self._tags = tags

        def tags(self) -> _StubTags:
            return _StubTags(self._tags)

    class _StubNotebook:
        def __init__(self, tags: dict[str, str]) -> None:
            self._tags = tags

        def getContext(self) -> _StubContext:
            return _StubContext(self._tags)

    class _StubEntryDbutils:
        def __init__(self, tags: dict[str, str]) -> None:
            self._tags = tags

        def notebook(self) -> _StubNotebook:
            return _StubNotebook(self._tags)

    class _StubEntry:
        def __init__(self, tags: dict[str, str]) -> None:
            self._tags = tags

        def getDbutils(self) -> _StubEntryDbutils:
            return _StubEntryDbutils(self._tags)

    class _StubNotebookFacade:
        def __init__(self, tags: dict[str, str]) -> None:
            self.entry_point = _StubEntry(tags)

    class _StubDBUtilsRoot:
        def __init__(self, tags: dict[str, str]) -> None:
            self.notebook = _StubNotebookFacade(tags)

    class _StubDBUtils:  # mimics pyspark.dbutils.DBUtils
        def __new__(cls, _spark) -> _StubDBUtilsRoot:  # type: ignore[misc]
            return _StubDBUtilsRoot(tags)

    class _StubSparkBuilder:
        def getOrCreate(self):
            return object()  # the helper does not introspect spark

    class _StubSparkSession:
        builder = _StubSparkBuilder()

    fake_dbutils_mod = types.ModuleType("pyspark.dbutils")
    fake_dbutils_mod.DBUtils = _StubDBUtils  # type: ignore[attr-defined]

    fake_sql_mod = types.ModuleType("pyspark.sql")
    fake_sql_mod.SparkSession = _StubSparkSession  # type: ignore[attr-defined]

    fake_pyspark_mod = types.ModuleType("pyspark")

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark_mod)
    monkeypatch.setitem(sys.modules, "pyspark.dbutils", fake_dbutils_mod)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_mod)


def _force_dbutils_import_failure(monkeypatch) -> None:
    """Make every ``import pyspark.dbutils`` raise so the helper's
    ``except Exception`` branch fires and the sentinel path runs."""
    monkeypatch.setitem(sys.modules, "pyspark", None)
    monkeypatch.setitem(sys.modules, "pyspark.dbutils", None)
    monkeypatch.setitem(sys.modules, "pyspark.sql", None)


@pytest.mark.parametrize("case", _CASES, ids=_case_id)
def test_databricks_ids_from_env_matches_production_shape(
    case, monkeypatch
) -> None:
    name, payload, expected = case
    env: dict[str, str] = payload.get("env") or {}
    dbutils_tags: dict[str, str] | None = payload.get("dbutils_tags")

    # Scrub all env vars the helper consults, then set only what the
    # fixture specifies. This guarantees the test is independent of
    # the developer's local environment.
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, str(value))

    if dbutils_tags is None:
        _force_dbutils_import_failure(monkeypatch)
    else:
        _install_fake_dbutils(monkeypatch, dbutils_tags)

    actual = _databricks_ids_from_env()
    assert actual == expected, (
        f"RCO-8 fixture '{name}' drifted. "
        f"Expected={expected!r} Actual={actual!r}. "
        f"If this drift is intentional, update "
        f"tests/unit/fixtures/rco8/databricks_ids_from_env/{name}/"
        f"expected_output.json deliberately."
    )


def test_databricks_ids_from_env_fixtures_exist() -> None:
    """RCO-8 floor — the three anchored resolution paths must be
    present so the D-5 regression pattern is covered."""
    case_names = {c[0] for c in _CASES}
    required = {
        "env_path_all_resolved",
        "sentinel_path_no_env_no_dbutils",
        "mixed_path_partial_env_full_dbutils",
    }
    missing = required - case_names
    assert not missing, (
        f"RCO-8 floor not met: missing fixture cases {sorted(missing)}"
    )
