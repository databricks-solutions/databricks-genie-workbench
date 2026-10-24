from __future__ import annotations

from types import SimpleNamespace

import pytest
from databricks.sdk.service.catalog import Privilege
from genie_space_optimizer.common import sp_permissions
from genie_space_optimizer.common.sp_permissions import (
    missing_read_schemas,
    probe_sp_required_access,
    read_grant_sql,
)

_READ = [Privilege.USE_CATALOG, Privilege.USE_SCHEMA, Privilege.SELECT]


def _assignments(*privileges: Privilege, principal: str = "sp") -> SimpleNamespace:
    return SimpleNamespace(privilege_assignments=[SimpleNamespace(
        principal=principal,
        privileges=[SimpleNamespace(privilege=p) for p in privileges],
    )])


class _Grants:
    def __init__(self, effective: dict[str, object]) -> None:
        self._effective = effective

    def get_effective(self, *, securable_type: str, full_name: str):
        value = self._effective.get(full_name, _assignments())
        if isinstance(value, Exception):
            raise value
        return value


@pytest.fixture
def sp_ws(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(sp_permissions, "get_sp_principal_aliases", lambda _ws: {"sp"})

    def make(effective: dict[str, object]) -> SimpleNamespace:
        return SimpleNamespace(grants=_Grants(effective))

    return make


def test_granted_schema_is_neither_missing_nor_unknown(sp_ws) -> None:
    ws = sp_ws({"cat.sch": _assignments(*_READ)})

    assert missing_read_schemas(ws, {("cat", "sch")}) == ([], [])
    assert probe_sp_required_access(ws, {("cat", "sch")}) == ({("cat", "sch")}, set())


def test_schema_without_read_grants_is_missing(sp_ws) -> None:
    ws = sp_ws({"cat.sch": _assignments(Privilege.USE_SCHEMA)})

    assert missing_read_schemas(ws, {("Cat", "Sch")}) == ([("cat", "sch")], [])


def test_schema_whose_grants_read_raises_is_unknown(sp_ws) -> None:
    ws = sp_ws({"cat.sch": RuntimeError("grants API unavailable")})

    assert missing_read_schemas(ws, {("cat", "sch")}) == ([], [("cat", "sch")])
    assert probe_sp_required_access(ws, {("cat", "sch")}) == (set(), set())


def test_catalog_grants_read_failure_makes_its_ungranted_schemas_unknown(sp_ws) -> None:
    ws = sp_ws({"cat": RuntimeError("grants API unavailable")})

    assert missing_read_schemas(ws, {("cat", "a"), ("cat", "b")}) == ([], [("cat", "a"), ("cat", "b")])


def test_catalog_level_grant_covers_a_schema_whose_own_read_raised(sp_ws) -> None:
    ws = sp_ws({"cat": _assignments(*_READ), "cat.sch": RuntimeError("grants API unavailable")})

    assert missing_read_schemas(ws, {("cat", "sch")}) == ([], [])


def test_unresolved_sp_identity_makes_every_schema_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sp_permissions, "get_sp_principal_aliases", lambda _ws: set())
    ws = SimpleNamespace(grants=_Grants({"cat.sch": _assignments(*_READ)}))

    assert missing_read_schemas(ws, {("cat", "sch")}) == ([], [("cat", "sch")])


def test_read_grant_sql_lists_the_four_schema_read_grants() -> None:
    assert read_grant_sql("app-sp", "cat", "sch") == (
        "GRANT USE CATALOG ON CATALOG `cat` TO `app-sp`;\n"
        "GRANT USE SCHEMA ON SCHEMA `cat`.`sch` TO `app-sp`;\n"
        "GRANT SELECT ON SCHEMA `cat`.`sch` TO `app-sp`;\n"
        "GRANT EXECUTE ON SCHEMA `cat`.`sch` TO `app-sp`;"
    )
