"""Tests for the metric view entitlement probe and consent service (Prompt 5).

The probe's job is to be right about two things and honest about a third: which
privileges the *signed-in user* holds, which runtime capabilities the compute
can prove, and which of those it cannot decide. The suite therefore pins the
statuses row by row rather than only the verdict, and asserts the probe never
writes — a fake WorkspaceClient here raises on any DDL-capable surface.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from genie_space_optimizer.optimization.mv_yaml import CapabilityRow, _capability_map

from backend.models import MvCapabilityRow
from backend.services import auth, mv_entitlement
from backend.services.mv_entitlement import MvProbeError

_REAL_OBSERVED_RUNTIME = mv_entitlement._observed_runtime

_ALL_PRIVILEGES = {
    ("CATALOG", "finance"): {"USE_CATALOG"},
    ("SCHEMA", "finance.sales"): {"USE_SCHEMA", "CREATE_TABLE"},
    ("TABLE", "finance.sales.orders"): {"SELECT"},
}


class _Privilege:
    def __init__(self, name: str) -> None:
        self.value = name


class _EffectivePrivilege:
    def __init__(self, name: str) -> None:
        self.privilege = _Privilege(name)


class _Assignment:
    def __init__(self, principal: str, names: set[str]) -> None:
        self.principal = principal
        self.privileges = [_EffectivePrivilege(n) for n in sorted(names)]


class _Effective:
    def __init__(self, assignments: list[_Assignment]) -> None:
        self.privilege_assignments = assignments


class _NotFound(Exception):
    pass


class _Grants:
    def __init__(self, granted: dict[tuple[str, str], set[str]], missing_securables: set[str]):
        self._granted = granted
        self._missing = missing_securables
        self.calls: list[tuple[str, str, str | None]] = []

    def get_effective(self, securable_type, full_name, principal=None):
        self.calls.append((securable_type, full_name, principal))
        if full_name in self._missing:
            raise _NotFound(f"Schema '{full_name}' does not exist.")
        names = self._granted.get((securable_type, full_name), set())
        return _Effective([_Assignment(principal or "someone@example.com", names)])


class _Me:
    user_name = "analyst@example.com"
    groups: list = []


class _CurrentUser:
    def me(self):
        return _Me()


class _ForbiddenSurface:
    """Any attribute access is a test failure — proves no write path is taken."""

    def __init__(self, label: str) -> None:
        self._label = label

    def __getattr__(self, item):
        raise AssertionError(f"probe touched {self._label}.{item}")


class _OwnerReads:
    """Read-only owner metadata: ``.get(full_name).owner``.

    The probe reads ownership (OBO-readable metadata) to authorize an owner
    without a grant read. ``get`` is the only allowed method — every other
    attribute is a write path and stays forbidden, preserving the "a probe never
    writes" guarantee.
    """

    def __init__(self, label: str, owners: dict[str, str], missing: set[str]) -> None:
        self._label = label
        self._owners = owners
        self._missing = missing

    def get(self, full_name):
        if full_name in self._missing:
            raise _NotFound(f"'{full_name}' does not exist.")
        return SimpleNamespace(owner=self._owners.get(full_name))

    def __getattr__(self, item):
        raise AssertionError(f"probe touched {self._label}.{item}")


class _FakeWorkspaceClient:
    def __init__(
        self,
        *,
        granted: dict[tuple[str, str], set[str]] | None = None,
        missing_securables: set[str] | None = None,
        owners: dict[str, str] | None = None,
    ) -> None:
        missing = missing_securables or set()
        self.grants = _Grants(
            granted if granted is not None else dict(_ALL_PRIVILEGES),
            missing,
        )
        self.current_user = _CurrentUser()
        # A trial CREATE would have to go through statement_execution.
        self.statement_execution = _ForbiddenSurface("statement_execution")
        # Owner reads are OBO-readable metadata (read-only ``.get``); the probe
        # uses them to authorize an owner without a grant read.
        owners = owners or {}
        self.catalogs = _OwnerReads("catalogs", owners, missing)
        self.schemas = _OwnerReads("schemas", owners, missing)
        self.tables = _OwnerReads("tables", owners, missing)


@pytest.fixture
def obo_client(monkeypatch):
    """Install a fake OBO client and neutralize every off-box dependency."""
    client = _FakeWorkspaceClient()

    def _use(ws):
        token = auth._obo_client.set(ws)
        return token

    token = _use(client)
    monkeypatch.setattr(mv_entitlement, "get_service_principal_client", lambda: client)
    monkeypatch.setattr(
        mv_entitlement, "_observed_runtime", lambda ws, warehouse_id: ("DBR", "17.3"),
    )
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.user_can_manage_space",
        lambda *a, **k: True,
    )
    try:
        yield client
    finally:
        auth._obo_client.reset(token)


def _probe(**overrides):
    kwargs = {
        "catalog": "finance",
        "schema": "sales",
        "space_id": "01ef_genie",
        "source_tables": ["finance.sales.orders"],
        "warehouse_id": "wh1",
    }
    kwargs.update(overrides)
    return mv_entitlement.probe(**kwargs)


# ── All granted ──────────────────────────────────────────────────────────


def test_all_granted_is_sufficient(obo_client):
    result = _probe()

    assert result.verdict == "SUFFICIENT"
    assert result.missing == []
    assert result.remediation_sql is None
    assert result.checked_as == "analyst@example.com"
    assert result.auth_identity == "OBO"
    assert result.target == "finance.sales"
    assert all(row.status == "GRANTED" for row in result.privileges)
    assert result.results["CREATE TABLE on finance.sales"] == "GRANTED"
    assert result.results["CAN MANAGE on Genie Agent 01ef_genie"] == "GRANTED"


def test_effective_read_asks_for_the_signed_in_user(obo_client):
    """Group-inherited privileges only count if the read is principal-scoped."""
    _probe()

    assert obo_client.grants.calls
    assert {call[2] for call in obo_client.grants.calls} == {"analyst@example.com"}
    # One read per securable, not one per check: CREATE TABLE and USE SCHEMA
    # both resolve from the same schema read.
    assert [call[1] for call in obo_client.grants.calls] == [
        "finance", "finance.sales", "finance.sales.orders",
    ]


def test_all_privileges_satisfies_every_specific_check(obo_client):
    obo_client.grants._granted = {
        ("CATALOG", "finance"): {"ALL_PRIVILEGES"},
        ("SCHEMA", "finance.sales"): {"ALL_PRIVILEGES"},
        ("TABLE", "finance.sales.orders"): {"ALL_PRIVILEGES"},
    }

    assert _probe().verdict == "SUFFICIENT"


# ── One privilege missing at a time ──────────────────────────────────────


@pytest.mark.parametrize(
    ("key", "privilege", "label", "expected_grant"),
    [
        (
            ("CATALOG", "finance"), "USE_CATALOG", "USE CATALOG on finance",
            "GRANT USE CATALOG ON CATALOG `finance` TO `analyst@example.com`;",
        ),
        (
            ("SCHEMA", "finance.sales"), "USE_SCHEMA", "USE SCHEMA on finance.sales",
            "GRANT USE SCHEMA ON SCHEMA `finance`.`sales` TO `analyst@example.com`;",
        ),
        (
            ("SCHEMA", "finance.sales"), "CREATE_TABLE", "CREATE TABLE on finance.sales",
            "GRANT CREATE TABLE ON SCHEMA `finance`.`sales` TO `analyst@example.com`;",
        ),
        (
            ("TABLE", "finance.sales.orders"), "SELECT",
            "SELECT on finance.sales.orders",
            "GRANT SELECT ON TABLE `finance`.`sales`.`orders` "
            "TO `analyst@example.com`;",
        ),
    ],
)
def test_single_missing_privilege_is_insufficient_and_remediable(
    obo_client, key, privilege, label, expected_grant,
):
    obo_client.grants._granted = {
        securable: set(names) for securable, names in _ALL_PRIVILEGES.items()
    }
    obo_client.grants._granted[key].discard(privilege)

    result = _probe()

    assert result.verdict == "INSUFFICIENT"
    assert result.missing == [label]
    assert result.results[label] == "DENIED"
    assert expected_grant in (result.remediation_sql or "")


def test_missing_space_manage_is_insufficient_without_grant_sql(obo_client, monkeypatch):
    """No GRANT closes a Genie ACL gap, so remediation SQL must not invent one."""
    monkeypatch.setattr(
        "genie_space_optimizer.common.genie_client.user_can_manage_space",
        lambda *a, **k: False,
    )

    result = _probe()

    assert result.verdict == "INSUFFICIENT"
    assert result.missing == ["CAN MANAGE on Genie Agent 01ef_genie"]
    assert result.remediation_sql is None


def test_space_check_is_skipped_when_no_space_is_given(obo_client):
    result = _probe(space_id="")

    assert all(row.privilege != "CAN_MANAGE" for row in result.privileges)
    assert result.verdict == "SUFFICIENT"


# ── Schema not found ─────────────────────────────────────────────────────


def test_schema_not_found_is_unknown_not_denied(obo_client):
    """A grant read that fails is UNKNOWN, not a false "insufficient".

    UC returns NOT_FOUND both when a schema is absent and when the OBO token
    cannot see it — the same NOT_FOUND a missing UC-grants scope produces. Since
    the user may in fact have access (ownership read is also unavailable here),
    reporting UNKNOWN is honest where DENIED would wrongly tell them to ask an
    admin to GRANT.
    """
    obo_client.grants._missing = {"finance.sales"}
    obo_client.schemas._missing = {"finance.sales"}

    result = _probe()

    assert result.verdict == "UNKNOWN"
    assert result.results["USE SCHEMA on finance.sales"] == "UNKNOWN"
    assert result.results["CREATE TABLE on finance.sales"] == "UNKNOWN"
    detail = next(
        row.detail for row in result.privileges if row.privilege == "CREATE_TABLE"
    )
    assert "not found or not visible" in (detail or "")


def test_unreadable_privileges_are_unknown_with_the_error_text(obo_client):
    def _boom(securable_type, full_name, principal=None):
        raise RuntimeError("upstream unavailable")

    obo_client.grants.get_effective = _boom

    result = _probe()

    assert result.verdict == "UNKNOWN"
    catalog_row = next(
        row for row in result.privileges if row.privilege == "USE_CATALOG"
    )
    assert catalog_row.status == "UNKNOWN"
    assert "upstream unavailable" in (catalog_row.detail or "")


def test_schema_owner_is_sufficient_without_any_grant_read(obo_client):
    """A schema owner can create in their schema even when no grant read works.

    Regression pin for the deployed-review false negative: the signed-in user
    owns the target schema, the OBO token cannot read UC grants (get_effective
    raises), yet the probe must authorize — ownership is the OBO-readable truth,
    and owning the schema also satisfies the parent USE CATALOG check.
    """
    def _boom(securable_type, full_name, principal=None):
        raise RuntimeError("OBO token not scoped for the UC grants API")

    obo_client.grants.get_effective = _boom
    obo_client.schemas._owners = {"finance.sales": "analyst@example.com"}
    obo_client.tables._owners = {"finance.sales": "analyst@example.com"}

    result = _probe()

    assert result.verdict == "SUFFICIENT"
    assert result.results["USE CATALOG on finance"] == "GRANTED"
    assert result.results["USE SCHEMA on finance.sales"] == "GRANTED"
    assert result.results["CREATE TABLE on finance.sales"] == "GRANTED"
    assert result.results["SELECT on finance.sales.orders"] == "GRANTED"
    assert result.remediation_sql is None


@pytest.mark.parametrize("catalog", ["", "fin ance", "finance;DROP", "fin`ance"])
def test_invalid_identifiers_are_rejected_before_any_read(obo_client, catalog):
    with pytest.raises(MvProbeError):
        _probe(catalog=catalog)
    assert obo_client.grants.calls == []


def test_non_three_part_source_table_is_rejected(obo_client):
    with pytest.raises(MvProbeError, match="three-part"):
        _probe(source_tables=["sales.orders"])


# ── Capability rows (MV-D8) ──────────────────────────────────────────────


def _capabilities(runtime_kind, version):
    return {
        row.capability: row for row in mv_entitlement._capability_rows(runtime_kind, version)
    }


def test_capability_rows_are_always_present_for_every_floor(obo_client):
    result = _probe()

    assert [row.capability for row in result.capabilities] == [
        "mv_create_edit", "mv_nested_joins", "mv_fields_agg_window_offset",
    ]
    assert [row.required_dbr for row in result.capabilities] == ["17.3", "17.1", "18.1"]
    for row in result.capabilities:
        assert result.results[row.capability] == row.status


def test_dbr_above_every_floor_grants_every_capability():
    rows = _capabilities("DBR", "18.2")

    assert {row.status for row in rows.values()} == {"GRANTED"}
    assert rows["mv_nested_joins"].optional is True
    assert rows["mv_create_edit"].optional is False


def test_dbr_between_floors_splits_the_rows():
    rows = _capabilities("DBR", "17.3")

    assert rows["mv_create_edit"].status == "GRANTED"
    assert rows["mv_nested_joins"].status == "GRANTED"
    assert rows["mv_fields_agg_window_offset"].status == "DENIED"
    assert "18.1" in (rows["mv_fields_agg_window_offset"].detail or "")


def test_dbr_below_the_create_floor_blocks_the_verdict(obo_client, monkeypatch):
    monkeypatch.setattr(
        mv_entitlement, "_observed_runtime", lambda ws, warehouse_id: ("DBR", "16.4"),
    )

    result = _probe()

    assert result.verdict == "INSUFFICIENT"
    assert result.missing == [
        "Create or edit a metric view (requires DBR 17.3)",
    ]
    # A runtime floor is not a grantable privilege.
    assert result.remediation_sql is None


def test_optional_capability_below_floor_does_not_block_the_verdict(
    obo_client, monkeypatch,
):
    """Below 18.1 the generator drops a feature; the user is still entitled."""
    monkeypatch.setattr(
        mv_entitlement, "_observed_runtime", lambda ws, warehouse_id: ("DBR", "17.3"),
    )

    result = _probe()

    assert result.verdict == "SUFFICIENT"
    assert result.missing == []


def test_warehouse_only_runtime_is_unknown_not_a_guess():
    rows = _capabilities("DBSQL", "2026.15")

    assert {row.status for row in rows.values()} == {"UNKNOWN"}
    assert all(row.runtime_kind == "DBSQL" for row in rows.values())
    assert "no mapping exists" in (rows["mv_nested_joins"].detail or "")


def test_unknown_create_capability_never_blocks_authorization(obo_client, monkeypatch):
    monkeypatch.setattr(
        mv_entitlement, "_observed_runtime", lambda ws, warehouse_id: ("DBSQL", "2026.15"),
    )

    result = _probe()

    assert result.results["mv_create_edit"] == "UNKNOWN"
    assert result.verdict == "SUFFICIENT"
    assert result.missing == []


def test_missing_runtime_read_is_unknown():
    rows = _capabilities("UNAVAILABLE", None)

    assert {row.status for row in rows.values()} == {"UNKNOWN"}
    assert all(row.observed_version is None for row in rows.values())


# ── Consent verification ─────────────────────────────────────────────────


def _consent(**overrides):
    consent = {
        "probe_id": "p1",
        "granted_by": "analyst@example.com",
        "target_catalog": "finance",
        "target_schema": "sales",
        "verdict": "SUFFICIENT",
        "probe_results": {
            "capabilities": [
                {"capability": "mv_create_edit", "observed_warehouse_id": "wh1"},
            ],
        },
    }
    consent.update(overrides)
    return consent


def test_verify_keeps_create_and_attach_when_nothing_changed(obo_client):
    verification = mv_entitlement.verify(_consent(), _probe())

    assert verification.effective_mode == "create_and_attach"
    assert verification.downgrade_reason is None
    assert verification.verdict == "SUFFICIENT"


def test_verify_downgrades_when_a_privilege_was_revoked(obo_client):
    """The revoked-between-config-and-trigger case: consent stands, grants do not."""
    obo_client.grants._granted = {
        securable: set(names) for securable, names in _ALL_PRIVILEGES.items()
    }
    obo_client.grants._granted[("SCHEMA", "finance.sales")].discard("CREATE_TABLE")

    verification = mv_entitlement.verify(_consent(), _probe())

    assert verification.effective_mode == "suggest_only"
    assert verification.verdict == "INSUFFICIENT"
    assert "CREATE TABLE on finance.sales" in (verification.downgrade_reason or "")


def test_verify_downgrades_on_identity_mismatch(obo_client):
    verification = mv_entitlement.verify(
        _consent(granted_by="someone.else@example.com"), _probe(),
    )

    assert verification.effective_mode == "suggest_only"
    assert "someone.else@example.com" in (verification.downgrade_reason or "")


def test_verify_downgrades_on_target_mismatch(obo_client):
    verification = mv_entitlement.verify(
        _consent(target_schema="marketing"), _probe(),
    )

    assert verification.effective_mode == "suggest_only"
    assert "finance.marketing" in (verification.downgrade_reason or "")


def test_verify_never_upgrades_a_non_sufficient_consent(obo_client):
    verification = mv_entitlement.verify(_consent(verdict="INSUFFICIENT"), _probe())

    assert verification.effective_mode == "suggest_only"
    assert "INSUFFICIENT" in (verification.downgrade_reason or "")


@pytest.mark.parametrize("missing", [None, {}])
def test_verify_treats_a_missing_consent_row_as_a_downgrade(obo_client, missing):
    """Best-effort persistence makes the missing row reachable, not theoretical.

    So "no record" has to mean "not authorized": a warehouse hiccup during the
    probe must not widen what the trigger may do.
    """
    fresh = _probe()
    assert fresh.verdict == "SUFFICIENT"

    verification = mv_entitlement.verify(missing, fresh)

    assert verification.effective_mode == "suggest_only"
    assert verification.downgrade_reason == "no consent record was found for this probe"
    assert verification.probe_id == fresh.probe_id


def test_capability_rows_record_the_warehouse_they_were_observed_against(obo_client):
    result = _probe(warehouse_id="wh1")

    assert {row.observed_warehouse_id for row in result.capabilities} == {"wh1"}


def test_verify_downgrades_when_re_verification_used_a_different_warehouse(obo_client):
    """A capability observed on warehouse A says nothing about a write on B."""
    verification = mv_entitlement.verify(_consent(), _probe(warehouse_id="wh2"))

    assert verification.effective_mode == "suggest_only"
    assert "wh1" in (verification.downgrade_reason or "")
    assert "wh2" in (verification.downgrade_reason or "")
    # The privileges themselves are untouched, so this is the stale-consent class
    # and not a permission failure.
    assert verification.verdict == "SUFFICIENT"


def test_verify_accepts_the_same_warehouse(obo_client):
    verification = mv_entitlement.verify(_consent(), _probe(warehouse_id="wh1"))

    assert verification.effective_mode == "create_and_attach"


# ── Consent persistence ──────────────────────────────────────────────────


def test_record_consent_writes_the_probe_and_defaults_materialize_off(
    obo_client, monkeypatch,
):
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "genie_space_optimizer")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh1")
    captured: dict = {}

    def _fake_upsert(ws, warehouse_id, **kwargs):
        captured.update(kwargs)
        return kwargs["probe_id"]

    monkeypatch.setattr(
        "genie_space_optimizer.common.warehouse.wh_upsert_mv_consent", _fake_upsert,
    )

    result = _probe()
    assert mv_entitlement.record_consent(result) is True

    assert captured["probe_id"] == result.probe_id
    assert captured["granted_by"] == "analyst@example.com"
    assert captured["target_catalog"] == "finance"
    assert captured["target_schema"] == "sales"
    assert captured["verdict"] == "SUFFICIENT"
    assert captured["materialize_consented"] is False
    assert captured["run_id"] is None
    # Capability rows must survive into the stored probe: Prompt 5.5 reads them.
    stored = captured["probe_results"]
    assert [row["capability"] for row in stored["capabilities"]] == [
        "mv_create_edit", "mv_nested_joins", "mv_fields_agg_window_offset",
    ]


def test_record_consent_failure_does_not_lose_the_probe(obo_client, monkeypatch):
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "genie_space_optimizer")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh1")

    def _boom(*a, **k):
        raise RuntimeError("warehouse asleep")

    monkeypatch.setattr(
        "genie_space_optimizer.common.warehouse.wh_upsert_mv_consent", _boom,
    )

    assert mv_entitlement.record_consent(_probe()) is False


def test_record_consent_is_skipped_when_storage_is_unconfigured(obo_client, monkeypatch):
    monkeypatch.delenv("GSO_CATALOG", raising=False)
    monkeypatch.delenv("GSO_SCHEMA", raising=False)

    assert mv_entitlement.record_consent(_probe()) is False


# ── No write path ────────────────────────────────────────────────────────


def test_probe_requires_obo_and_never_falls_back_to_the_service_principal(monkeypatch):
    auth.clear_obo_user_token()
    monkeypatch.setattr(
        auth,
        "_get_default_client",
        lambda: (_ for _ in ()).throw(AssertionError("must not use app identity")),
    )

    with pytest.raises(RuntimeError, match="user authorization"):
        _probe()


# ── Cross-package contract ───────────────────────────────────────────────


def test_mv_capability_row_satisfies_the_gso_protocol():
    """The engine reads these rows structurally, so the shape is the contract.

    ``mv_yaml.validate`` types its ``capabilities`` argument against
    ``CapabilityRow``, a Protocol declared in the engine because the dependency
    arrow runs backend → engine and never back. Nothing imports this model there,
    so renaming or dropping one of the three fields would be silent — this test is
    the only thing that makes it loud. It lives in the backend suite for the same
    reason: the engine cannot import the model to check it.
    """
    row = MvCapabilityRow(
        capability="mv_nested_joins",
        label="Nested joins",
        required_dbr="17.1",
        status="GRANTED",
        optional=True,
    )

    assert isinstance(row, CapabilityRow)
    # isinstance on a runtime_checkable Protocol only proves the attributes are
    # present, so also assert the engine reads the values it expects to.
    assert _capability_map([row]) == {"mv_nested_joins": "GRANTED"}

    # And the dict form a persisted probe_results payload round-trips as.
    assert _capability_map([row.model_dump(mode="json")]) == {"mv_nested_joins": "GRANTED"}

    # Negative control: the Protocol must actually discriminate, or the assertion
    # above proves nothing. Drop one of the three fields and conformance fails.
    class _MissingOptional:
        capability = "mv_nested_joins"
        status = "GRANTED"

    assert not isinstance(_MissingOptional(), CapabilityRow)


def test_probe_capability_rows_are_consumable_by_the_engine(obo_client):
    """End to end: a real probe's rows drive the engine's capability lookup."""
    result = _probe()

    assert result.capabilities
    assert all(isinstance(row, CapabilityRow) for row in result.capabilities)
    resolved = _capability_map(result.capabilities)
    assert set(resolved) == {row.capability for row in result.capabilities}
    # The flat results map is keyed by capability id too, so both inputs agree.
    assert all(resolved[k] == result.results[k] for k in resolved)


def test_no_code_path_issues_ddl(obo_client, monkeypatch):
    """The runtime read is the only SQL the probe runs, and it is a SELECT."""
    executed: list[str] = []

    def _fake_query(ws, warehouse_id, sql):
        executed.append(sql)
        import pandas as pd

        return pd.DataFrame([{"dbr": "17.3", "dbsql": None}])

    monkeypatch.setattr(
        "genie_space_optimizer.common.warehouse.sql_warehouse_query", _fake_query,
    )
    monkeypatch.setattr(mv_entitlement, "_observed_runtime", _REAL_OBSERVED_RUNTIME)

    result = _probe()

    assert result.capabilities[0].status == "GRANTED"
    assert executed and all(sql.lstrip().upper().startswith("SELECT") for sql in executed)
    forbidden = ("CREATE ", "ALTER ", "DROP ", "GRANT ", "INSERT ", "MERGE ")
    assert not any(word in sql.upper() for sql in executed for word in forbidden)
