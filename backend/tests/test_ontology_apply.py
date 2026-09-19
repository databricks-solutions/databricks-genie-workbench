"""Ontology apply — Phase 5 (17i) plan builder + consent gate + mirror hydration.

Covers the OFFLINE-testable surface of the subsystem's ONLY governed-tag write path:

- ``build_apply_plan`` expands APPROVED consents to the three write shapes
  (``reuse`` → SET TAG; ``create`` → CREATE GOVERNED TAG + SET TAG; ``reassign`` →
  UNSET + SET), excludes page consents (MV-D27), and produces a deterministic
  ``plan_hash`` — writing nothing (dry-run purity).
- The execute route enforces the two-step consent gate: ``confirm=true`` AND the
  preview's ``plan_hash`` (409 on mismatch, and the 409 must surface, not degrade),
  and attributes the write to the OBO caller (MV-D50).
- The mirror readers hydrate a bare consent key from its ``genie_ont_domains``
  proposal row (metastore grain, MV-D49) and scope reassign moves conservatively.

No warehouse, no UC write, no OBO round trip — the live apply is deploy-gated.
"""

from __future__ import annotations

import types

import pytest
from databricks.sdk.service.sql import StatementState
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.ontology import models
from backend.ontology.routers.apply import router as apply_router
from backend.ontology.services import apply as apply_service
from backend.ontology.services import grants, mirror, ont_settings


# ── helpers ──────────────────────────────────────────────────────────────────


def _patch_mirror(monkeypatch, *, consents, members_by_domain, tag_members=None):
    """Stub the three Phase-5 mirror readers apply_service calls, plus the two read-only
    OBO probes (grant + current-value) so the shape tests are hermetic and deterministic
    regardless of the local Databricks env (the probes fail-soft to executable/no-value)."""

    async def _consents(_ms):
        return list(consents)

    async def _domain_members(_ms, domain_id):
        return list(members_by_domain.get(domain_id, []))

    async def _tag_members(_ms, conflict_tag):
        return list((tag_members or {}).get(conflict_tag, []))

    monkeypatch.setattr(mirror, "read_approved_consents", _consents)
    monkeypatch.setattr(mirror, "read_domain_members", _domain_members)
    monkeypatch.setattr(mirror, "read_tag_members", _tag_members)
    monkeypatch.setattr(grants, "membership_write_probe", lambda *a, **k: (True, []))
    monkeypatch.setattr(grants, "current_tag_value", lambda *a, **k: None)


def _statements(plan):
    return [i.statement for i in plan.items]


# ── build_apply_plan: the three write shapes ──────────────────────────────────


async def test_reuse_domain_emits_set_tag_only(monkeypatch):
    _patch_mirror(
        monkeypatch,
        consents=[
            {
                "proposal_kind": "domain",
                "proposal_id": "sug_a",
                "name": "Revenue",
                "tag_decision": "reuse",
                "tag_key": "business_domain",
                "tag_value": "Revenue",
                "conflict_tag": "",
            }
        ],
        members_by_domain={"sug_a": [{"asset_fqn": "cat.sch.orders"}, {"asset_fqn": "cat.sch.line_items"}]},
    )
    plan = await apply_service.build_apply_plan("ms1")
    stmts = _statements(plan)
    assert not any(s.startswith("CREATE GOVERNED TAG") for s in stmts)
    # Verified syntax (STEP 0): SET TAG ON <securable> `fqn` `key` = `value` (value is an identifier).
    assert "SET TAG ON TABLE `cat`.`sch`.`orders` `business_domain` = `Revenue`" in stmts
    assert "SET TAG ON TABLE `cat`.`sch`.`line_items` `business_domain` = `Revenue`" in stmts
    assert plan.executable_count == 2
    assert plan.source == "mirror"


async def test_create_subdomain_emits_create_then_set(monkeypatch):
    _patch_mirror(
        monkeypatch,
        consents=[
            {
                "proposal_kind": "subdomain",
                "proposal_id": "sug_b",
                "name": "Revenue/Billing",
                "tag_decision": "create",
                "tag_key": "business_subdomain",
                "tag_value": "Revenue/Billing",
                "conflict_tag": "",
            }
        ],
        members_by_domain={"sug_b": [{"asset_fqn": "cat.sch.invoices"}]},
    )
    plan = await apply_service.build_apply_plan("ms1")
    stmts = _statements(plan)
    # Verified syntax (STEP 0): CREATE GOVERNED TAG `key` VALUES ('literal'); the sub-domain
    # value follows the {parent}/{child} convention and its slash rides a backtick identifier.
    assert stmts[0] == "CREATE GOVERNED TAG `business_subdomain` VALUES ('Revenue/Billing')"
    assert "SET TAG ON TABLE `cat`.`sch`.`invoices` `business_subdomain` = `Revenue/Billing`" in stmts


async def test_reassign_emits_unset_then_set(monkeypatch):
    _patch_mirror(
        monkeypatch,
        consents=[
            {
                "proposal_kind": "reassign",
                "proposal_id": "sug_c",
                "name": "Revenue",
                "tag_decision": "reassign",
                "tag_key": "business_domain",
                "tag_value": "Revenue",
                "conflict_tag": "legacy_domain",
            }
        ],
        members_by_domain={},
        tag_members={"legacy_domain": [{"asset_fqn": "cat.sch.txns"}]},
    )
    plan = await apply_service.build_apply_plan("ms1")
    stmts = _statements(plan)
    unset = "UNSET TAG ON TABLE `cat`.`sch`.`txns` `legacy_domain`"
    set_new = "SET TAG ON TABLE `cat`.`sch`.`txns` `business_domain` = `Revenue`"
    assert unset in stmts
    assert set_new in stmts
    # order: unset precedes set for the moved asset
    assert stmts.index(unset) < stmts.index(set_new)


async def test_page_consent_excluded(monkeypatch):
    # Even if a page consent slips into the list, build_apply_plan skips it (MV-D27).
    _patch_mirror(
        monkeypatch,
        consents=[{"proposal_kind": "page", "proposal_id": "pg_x", "tag_key": "", "tag_value": None}],
        members_by_domain={},
    )
    plan = await apply_service.build_apply_plan("ms1")
    assert plan.items == []


async def test_plan_hash_is_deterministic(monkeypatch):
    cfg = dict(
        consents=[
            {
                "proposal_kind": "domain",
                "proposal_id": "sug_a",
                "name": "Revenue",
                "tag_decision": "reuse",
                "tag_key": "business_domain",
                "tag_value": "Revenue",
                "conflict_tag": "",
            }
        ],
        members_by_domain={"sug_a": [{"asset_fqn": "cat.sch.orders"}]},
    )
    _patch_mirror(monkeypatch, **cfg)
    h1 = (await apply_service.build_apply_plan("ms1")).plan_hash
    _patch_mirror(monkeypatch, **cfg)
    h2 = (await apply_service.build_apply_plan("ms1")).plan_hash
    assert h1 and h1 == h2


async def test_no_consents_yields_empty_mirror_plan(monkeypatch):
    _patch_mirror(monkeypatch, consents=[], members_by_domain={})
    plan = await apply_service.build_apply_plan("ms1")
    assert plan.items == [] and plan.source == "mirror"


async def test_reader_failure_degrades_to_cold(monkeypatch):
    async def _boom(_ms):
        raise RuntimeError("mirror down")

    monkeypatch.setattr(mirror, "read_approved_consents", _boom)
    plan = await apply_service.build_apply_plan("ms1")
    assert plan.items == [] and plan.source == "cold"


# ── execute route: the two-step consent gate + OBO attribution ────────────────


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(ont_settings, "_metastore_id", lambda: "ms1")
    monkeypatch.setattr(ont_settings, "_workspace_id", lambda: "ws1")
    app = FastAPI()
    app.include_router(apply_router)
    return TestClient(app)


def test_execute_requires_confirm(client):
    resp = client.post("/api/ontology/apply/execute", json={"plan_hash": "abc", "confirm": False})
    assert resp.status_code == 400


def test_execute_409_on_plan_hash_mismatch(client, monkeypatch):
    async def _plan(_ms):
        return models.ApplyPlan(
            items=[], executable_count=0, blocked_count=0, plan_hash="real", source="mirror", as_of="t"
        )

    monkeypatch.setattr(apply_service, "build_apply_plan", _plan)
    resp = client.post("/api/ontology/apply/execute", json={"plan_hash": "stale", "confirm": True})
    assert resp.status_code == 409


def test_execute_runs_and_attributes_to_obo_caller(client, monkeypatch):
    async def _plan(_ms):
        return models.ApplyPlan(
            items=[], executable_count=0, blocked_count=0, plan_hash="h", source="mirror", as_of="t"
        )

    captured: dict = {}

    async def _exec(*, plan, metastore_id, workspace_id, applied_by):
        captured.update(metastore_id=metastore_id, workspace_id=workspace_id, applied_by=applied_by)
        return models.ApplyResult(as_of="t")

    monkeypatch.setattr(apply_service, "build_apply_plan", _plan)
    monkeypatch.setattr(apply_service, "execute_apply_plan", _exec)
    resp = client.post(
        "/api/ontology/apply/execute",
        json={"plan_hash": "h", "confirm": True},
        headers={"x-forwarded-email": "curator@databricks.com"},
    )
    assert resp.status_code == 200
    assert captured == {"metastore_id": "ms1", "workspace_id": "ws1", "applied_by": "curator@databricks.com"}


# ── mirror hydration: bare consent key → proposal tag fields ──────────────────


async def test_read_approved_consents_hydrates_from_domain_row(monkeypatch):
    captured: dict = {}

    def _delta(sql, params=None):
        captured["sql"] = sql
        return [{"proposal_kind": "reassign", "proposal_id": "sug_c"}]

    async def _read_table(table, _ms):
        assert table == "genie_ont_domains"
        return [
            {
                "domain_id": "sug_c",
                "name": "Revenue",
                "tag_decision": "reassign",
                "tag_key": "business_domain",
                "tag_value": "Revenue",
                "evidence": {"conflict": {"existing_tag": "legacy_domain"}},
            }
        ]

    monkeypatch.setattr(mirror, "_delta_query", _delta)
    monkeypatch.setattr(mirror, "_read_table", _read_table)
    out = await mirror.read_approved_consents("ms1")
    # The read is metastore-scoped, approved-only, and page-excluded (the SQL filter).
    assert "state = 'approved'" in captured["sql"]
    assert "proposal_kind IN ('domain', 'subdomain', 'reassign')" in captured["sql"]
    assert out == [
        {
            "proposal_kind": "reassign",
            "proposal_id": "sug_c",
            "name": "Revenue",
            "tag_decision": "reassign",
            "tag_key": "business_domain",
            "tag_value": "Revenue",
            "conflict_tag": "legacy_domain",
        }
    ]


async def test_read_approved_consents_skips_aged_out_proposal(monkeypatch):
    monkeypatch.setattr(mirror, "_delta_query", lambda sql, params=None: [{"proposal_kind": "domain", "proposal_id": "gone"}])

    async def _empty(_table, _ms):
        return []

    monkeypatch.setattr(mirror, "_read_table", _empty)
    assert await mirror.read_approved_consents("ms1") == []


async def test_read_domain_members_filters_by_domain(monkeypatch):
    async def _rows(_table, _ms):
        return [
            {"domain_id": "sug_a", "asset_fqn": "cat.sch.a", "asset_type": "view"},
            {"domain_id": "sug_b", "asset_fqn": "cat.sch.b"},
            {"domain_id": "sug_a", "asset_fqn": ""},  # empty fqn dropped
        ]

    monkeypatch.setattr(mirror, "_read_table", _rows)
    # asset_type rides along (drives the SET TAG securable keyword); missing → "table".
    assert await mirror.read_domain_members("ms1", "sug_a") == [
        {"asset_fqn": "cat.sch.a", "asset_type": "view"}
    ]


async def test_read_tag_members_scopes_to_conflicted_proposals(monkeypatch):
    calls = {"n": 0}

    async def _read_table(table, _ms):
        calls["n"] += 1
        if table == "genie_ont_domains":
            return [
                {"domain_id": "sug_c", "evidence": {"conflict": {"existing_tag": "legacy_domain"}}},
                {"domain_id": "sug_d", "evidence": {"conflict": {"existing_tag": "other"}}},
            ]
        return [
            {"domain_id": "sug_c", "asset_fqn": "cat.sch.txns"},
            {"domain_id": "sug_d", "asset_fqn": "cat.sch.other"},
        ]

    monkeypatch.setattr(mirror, "_read_table", _read_table)
    out = await mirror.read_tag_members("ms1", "legacy_domain")
    # only the conflicted proposal's members; asset_type defaults to "table" when absent
    assert out == [{"asset_fqn": "cat.sch.txns", "asset_type": "table"}]
    assert await mirror.read_tag_members("ms1", "") == []  # empty tag → no read


# ── MV-D101: an already-100%-governed reuse is a no-op → dropped from Drafts ───


def test_reuse_fully_governed_predicate():
    from genie_space_optimizer.ontology import transforms

    full = {"reuse_coverage": {"already_tagged": 3, "total": 3}}
    part = {"reuse_coverage": {"already_tagged": 1, "total": 3}}
    assert transforms.reuse_fully_governed("reuse", full) is True
    assert transforms.reuse_fully_governed("reuse", part) is False       # partial still surfaces
    assert transforms.reuse_fully_governed("create", full) is False      # only reuse is gated
    assert transforms.reuse_fully_governed("reassign", full) is False
    assert transforms.reuse_fully_governed("reuse", {}) is False         # no coverage bag
    assert transforms.reuse_fully_governed("reuse", {"reuse_coverage": {"already_tagged": 0, "total": 0}}) is False


async def test_read_domain_drafts_drops_fully_governed_reuse(monkeypatch):
    surfaced = {"surfaced": True, "rank": {"tier": "high"}}

    async def _read_table(table, _ms):
        if table == "genie_ont_domains":
            return [
                {"domain_id": "d_full", "name": "Full", "tag_decision": "reuse", "score": 90.0,
                 "evidence": {**surfaced, "reuse_coverage": {"already_tagged": 3, "total": 3}}},
                {"domain_id": "d_part", "name": "Partial", "tag_decision": "reuse", "score": 90.0,
                 "evidence": {**surfaced, "reuse_coverage": {"already_tagged": 1, "total": 3}}},
                {"domain_id": "d_new", "name": "New", "tag_decision": "create", "score": 90.0,
                 "evidence": dict(surfaced)},
            ]
        return []  # no members needed for the surfacing gate

    monkeypatch.setattr(mirror, "_read_table", _read_table)
    drafts = await mirror.read_domain_drafts("ms1")
    names = {d["name"] for d in drafts}
    # The pure no-op reuse is gone; the partial reuse and the create still surface.
    assert names == {"Partial", "New"}


# ── STEP 0 / BUILD A: statement builders escape identifiers + literals safely ──


def test_set_tag_statement_uses_verified_syntax_and_securable_keyword():
    # A view member yields a VIEW securable keyword; a 3-part FQN is quoted per segment.
    assert apply_service._set_tag_statement("c.s.orders", "view", "biz", "Revenue") == (
        "SET TAG ON VIEW `c`.`s`.`orders` `biz` = `Revenue`"
    )
    # Sub-domain value with a slash rides a backtick identifier (the {parent}/{child} form).
    assert apply_service._set_tag_statement("c.s.t", "table", "biz", "Finance/Tax") == (
        "SET TAG ON TABLE `c`.`s`.`t` `biz` = `Finance/Tax`"
    )


def test_set_tag_value_with_quote_or_backtick_is_injection_safe():
    # The SET TAG value is an IDENTIFIER position: a single quote is inert inside backticks,
    # and an embedded backtick is doubled — no way to break out of the value.
    stmt = apply_service._set_tag_statement("c.s.t", "table", "k", "a'b`c")
    assert stmt == "SET TAG ON TABLE `c`.`s`.`t` `k` = `a'b``c`"


def test_create_tag_statement_escapes_string_literal():
    # The CREATE GOVERNED TAG allowed value IS a string literal → a single quote is doubled.
    assert apply_service._create_tag_statement("k", "a'b") == "CREATE GOVERNED TAG `k` VALUES ('a''b')"
    assert apply_service._create_tag_statement("k", None) == "CREATE GOVERNED TAG `k`"


# ── BUILD B: the grant probe blocks an item → copy-ready, not executed ─────────


async def test_grant_probe_blocks_item_with_copy_ready_grants(monkeypatch):
    _patch_mirror(
        monkeypatch,
        consents=[
            {
                "proposal_kind": "domain",
                "proposal_id": "sug_a",
                "name": "Revenue",
                "tag_decision": "reuse",
                "tag_key": "business_domain",
                "tag_value": "Revenue",
                "conflict_tag": "",
            }
        ],
        members_by_domain={"sug_a": [{"asset_fqn": "cat.sch.orders"}]},
    )
    # A probe that positively finds a missing grant blocks the item (copy-ready lines).
    monkeypatch.setattr(
        grants,
        "membership_write_probe",
        lambda *a, **k: (False, ["GRANT APPLY TAG ON TABLE `cat`.`sch`.`orders` TO `you`"]),
    )
    plan = await apply_service.build_apply_plan("ms1")
    assert plan.executable_count == 0 and plan.blocked_count == 1
    item = plan.items[0]
    assert item.executable is False
    assert item.blocked_reason
    assert item.required_grants == ["GRANT APPLY TAG ON TABLE `cat`.`sch`.`orders` TO `you`"]


async def test_blocked_item_is_copy_ready_not_executed(monkeypatch):
    """execute skips a blocked item (no statement runs for it) and returns it as copy-ready."""
    executed: list[str] = []

    class _Resp:
        statement_id = "sid"
        status = types.SimpleNamespace(state=StatementState.SUCCEEDED, error=None)

    class _Exec:
        def execute_statement(self, warehouse_id, statement, wait_timeout=None, parameters=None):
            executed.append(statement)
            return _Resp()

        def get_statement(self, statement_id):
            return _Resp()

    fake_client = types.SimpleNamespace(statement_execution=_Exec())
    monkeypatch.setattr(apply_service, "require_obo_workspace_client", lambda: fake_client)
    monkeypatch.setattr(apply_service, "_write_audit_row", lambda **k: None)
    monkeypatch.setattr(apply_service, "_flip_consents", lambda **k: None)
    monkeypatch.setenv("SQL_WAREHOUSE_ID", "wh1")

    ok_item = models.ApplyItem(
        proposal_id="s", proposal_kind="domain", shape="set_tag", target_fqn="c.s.a",
        tag_key="biz", tag_value="Rev", statement="SET TAG ON TABLE `c`.`s`.`a` `biz` = `Rev`",
        executable=True,
    )
    blocked_item = models.ApplyItem(
        proposal_id="s", proposal_kind="domain", shape="set_tag", target_fqn="c.s.b",
        tag_key="biz", tag_value="Rev", statement="SET TAG ON TABLE `c`.`s`.`b` `biz` = `Rev`",
        executable=False, blocked_reason="needs a grant",
        required_grants=["GRANT APPLY TAG ON TABLE `c`.`s`.`b` TO `you`"],
    )
    plan = models.ApplyPlan(
        items=[ok_item, blocked_item], executable_count=1, blocked_count=1,
        plan_hash="h", source="mirror", as_of="t",
    )

    result = await apply_service.execute_apply_plan(
        plan=plan, metastore_id="ms1", workspace_id="ws1", applied_by="u@x"
    )
    # Only the executable statement ran; the blocked one was never sent to the warehouse.
    assert executed == ["SET TAG ON TABLE `c`.`s`.`a` `biz` = `Rev`"]
    assert [o.target_fqn for o in result.applied] == ["c.s.a"]
    assert [o.target_fqn for o in result.blocked] == ["c.s.b"]
    assert result.failed == []


# ── BUILD C: current_value drives add-vs-move ──────────────────────────────────


async def test_current_value_add_vs_move(monkeypatch):
    cfg = dict(
        consents=[
            {
                "proposal_kind": "domain", "proposal_id": "sug_a", "name": "Revenue",
                "tag_decision": "reuse", "tag_key": "business_domain",
                "tag_value": "Revenue", "conflict_tag": "",
            }
        ],
        members_by_domain={"sug_a": [{"asset_fqn": "cat.sch.orders"}]},
    )
    # No current value → an "add" (current_value stays None).
    _patch_mirror(monkeypatch, **cfg)
    plan = await apply_service.build_apply_plan("ms1")
    assert plan.items[0].current_value is None

    # A differing current value → a "move" (current_value carried through for the diff).
    _patch_mirror(monkeypatch, **cfg)
    monkeypatch.setattr(grants, "current_tag_value", lambda *a, **k: "Legacy")
    plan = await apply_service.build_apply_plan("ms1")
    assert plan.items[0].current_value == "Legacy"


# ── BUILD A: audit INSERT + consent flip bind every value (no interpolation) ───


def test_audit_insert_binds_values_no_interpolation(monkeypatch):
    captured: dict = {}

    class _Resp:
        statement_id = None
        status = None

    class _Exec:
        def execute_statement(self, warehouse_id, statement, wait_timeout=None, parameters=None):
            captured["sql"] = statement
            captured["params"] = parameters
            return _Resp()

    import backend.services.auth as auth_mod

    monkeypatch.setattr(
        auth_mod, "get_service_principal_client",
        lambda: types.SimpleNamespace(statement_execution=_Exec()),
    )
    monkeypatch.setenv("SQL_WAREHOUSE_ID", "wh1")

    nasty = "SET TAG ON TABLE `c`.`s`.`t` `k` = `O'Brien`"  # a value carrying a single quote
    apply_service._write_audit_row(
        metastore_id="ms1", apply_id="ap_1", workspace_id="ws1", proposal_kind="domain",
        proposal_id="s", shape="set_tag", statement=nasty, target_fqn="c.s.t",
        tag_key="k", tag_value="O'Brien", prev_value=None, state="applied",
        applied_by="u@x", error=None,
    )
    # The statement text is a bound parameter, never interpolated into the INSERT SQL.
    assert ":statement" in captured["sql"]
    assert nasty not in captured["sql"]
    by_name = {p.name: p.value for p in captured["params"]}
    assert by_name["statement"] == nasty
    assert by_name["applied_by"] == "u@x"
    assert by_name["tag_value"] == "O'Brien"
    assert by_name["run_ref"] is None


def test_consent_flip_binds_values_and_in_list(monkeypatch):
    captured: dict = {}

    class _Resp:
        statement_id = None
        status = None

    class _Exec:
        def execute_statement(self, warehouse_id, statement, wait_timeout=None, parameters=None):
            captured["sql"] = statement
            captured["params"] = parameters
            return _Resp()

    import backend.services.auth as auth_mod

    monkeypatch.setattr(
        auth_mod, "get_service_principal_client",
        lambda: types.SimpleNamespace(statement_execution=_Exec()),
    )
    monkeypatch.setenv("SQL_WAREHOUSE_ID", "wh1")

    apply_service._flip_consents(
        metastore_id="ms1", workspace_id="ws1", proposal_ids={"a", "b"}, applied_by="u@x"
    )
    sql = captured["sql"]
    assert ":metastore_id" in sql and ":workspace_id" in sql
    assert ":pid0" in sql and ":pid1" in sql  # one named marker per proposal id (bound IN list)
    by_name = {p.name: p.value for p in captured["params"]}
    assert by_name["metastore_id"] == "ms1"
    assert sorted([by_name["pid0"], by_name["pid1"]]) == ["a", "b"]


# ══ Phase 5 (17j): UNDO ════════════════════════════════════════════════════════
# The inverse of a recorded genie_ont_applied membership row. Offline-testable: the
# inverse-statement table, create_tag exclusion + note, the unset pre-value capture
# (BUILD A), the no-op guard, the SP consent re-flip, and the new-append audit model.


def _patch_applied(monkeypatch, rows):
    """Stub the applied-audit reader the undo plan builder consumes."""

    async def _applied(_ms, _ids):
        return list(rows)

    monkeypatch.setattr(mirror, "read_applied_memberships", _applied)


def _applied_row(**over):
    row = {
        "proposal_kind": "domain",
        "proposal_id": "sug_a",
        "shape": "set_tag",
        "statement": "SET TAG ON TABLE `c`.`s`.`t` `biz` = `Rev`",
        "target_fqn": "c.s.t",
        "tag_key": "biz",
        "tag_value": "Rev",
        "prev_value": None,
    }
    row.update(over)
    return row


# ── §2 inverse-statement table ────────────────────────────────────────────────


async def test_undo_inverse_statement_table(monkeypatch):
    _patch_applied(
        monkeypatch,
        [
            # set_tag + prev NULL (an add) → UNSET
            _applied_row(
                shape="set_tag", target_fqn="c.s.orders", tag_key="biz", tag_value="Rev",
                prev_value=None, statement="SET TAG ON TABLE `c`.`s`.`orders` `biz` = `Rev`",
            ),
            # set_tag + prev <old> (a move) → SET = `<old>` (securable keyword recovered = VIEW)
            _applied_row(
                shape="set_tag", target_fqn="c.s.v", tag_key="biz", tag_value="Rev",
                prev_value="Legacy", statement="SET TAG ON VIEW `c`.`s`.`v` `biz` = `Rev`",
            ),
            # unset_tag + prev <old> → SET = `<old>`
            _applied_row(
                shape="unset_tag", target_fqn="c.s.t", tag_key="legacy", tag_value=None,
                prev_value="OldVal", statement="UNSET TAG ON TABLE `c`.`s`.`t` `legacy`",
            ),
        ],
    )
    plan = await apply_service.build_undo_plan("ms1", ["sug_a"])
    stmts = _statements(plan)
    assert "UNSET TAG ON TABLE `c`.`s`.`orders` `biz`" in stmts       # add → remove
    assert "SET TAG ON VIEW `c`.`s`.`v` `biz` = `Legacy`" in stmts     # move → restore (VIEW kw)
    assert "SET TAG ON TABLE `c`.`s`.`t` `legacy` = `OldVal`" in stmts # unset → re-add
    assert plan.source == "mirror"


async def test_undo_inverse_escapes_restored_identifier(monkeypatch):
    # The restored value rides a backtick identifier — an embedded backtick is doubled.
    _patch_applied(
        monkeypatch,
        [_applied_row(shape="unset_tag", tag_key="k", tag_value=None, prev_value="a`b",
                      statement="UNSET TAG ON TABLE `c`.`s`.`t` `k`")],
    )
    plan = await apply_service.build_undo_plan("ms1", ["sug_a"])
    assert _statements(plan) == ["SET TAG ON TABLE `c`.`s`.`t` `k` = `a``b`"]


# ── §3 create_tag is not undoable (excluded + noted) ──────────────────────────


async def test_undo_excludes_create_tag_and_notes_it(monkeypatch):
    _patch_applied(
        monkeypatch,
        [
            _applied_row(shape="create_tag", target_fqn="biz", tag_key="biz", tag_value=None,
                         statement="CREATE GOVERNED TAG `biz`"),
            _applied_row(shape="set_tag", target_fqn="c.s.t", tag_key="biz", tag_value="Rev",
                         prev_value=None, statement="SET TAG ON TABLE `c`.`s`.`t` `biz` = `Rev`"),
        ],
    )
    plan = await apply_service.build_undo_plan("ms1", ["sug_a"])
    # Only the member set_tag is reversed (→ UNSET); the create_tag is left in place.
    assert _statements(plan) == ["UNSET TAG ON TABLE `c`.`s`.`t` `biz`"]
    assert plan.notes and "1 grouping" in plan.notes[0]
    # The note is plain-language (never the tag/DDL mechanics — MV-D23 zero-burden).
    assert "governed tag" not in plan.notes[0].lower()


async def test_undo_skips_unset_with_no_captured_pre_value(monkeypatch):
    # An unset whose pre-value was never captured (NULL) is honestly non-undoable → dropped.
    _patch_applied(
        monkeypatch,
        [_applied_row(shape="unset_tag", tag_key="k", tag_value=None, prev_value=None,
                      statement="UNSET TAG ON TABLE `c`.`s`.`t` `k`")],
    )
    plan = await apply_service.build_undo_plan("ms1", ["sug_a"])
    assert plan.items == []


async def test_undo_empty_when_no_applied_rows(monkeypatch):
    _patch_applied(monkeypatch, [])
    plan = await apply_service.build_undo_plan("ms1", ["sug_a"])
    assert plan.items == [] and plan.source == "mirror"


# ── BUILD A: the unset pre-value is now captured on the apply plan ────────────


async def test_reassign_captures_unset_pre_value(monkeypatch):
    _patch_mirror(
        monkeypatch,
        consents=[
            {
                "proposal_kind": "reassign", "proposal_id": "sug_c", "name": "Revenue",
                "tag_decision": "reassign", "tag_key": "business_domain",
                "tag_value": "Revenue", "conflict_tag": "legacy_domain",
            }
        ],
        members_by_domain={},
        tag_members={"legacy_domain": [{"asset_fqn": "cat.sch.txns"}]},
    )
    # The current value of the tag we UNSET is captured as the unset item's current_value,
    # so the audit row's prev_value persists and the unset is reversible (17j BUILD A).
    monkeypatch.setattr(
        grants, "current_tag_value",
        lambda _c, _fqn, key, **k: "LegacyVal" if key == "legacy_domain" else None,
    )
    plan = await apply_service.build_apply_plan("ms1")
    unset = next(i for i in plan.items if i.shape == "unset_tag")
    assert unset.current_value == "LegacyVal"


# ── the no-op guard + new-append audit model + OBO/SP split ───────────────────


class _OkResp:
    statement_id = "sid"
    status = types.SimpleNamespace(state=StatementState.SUCCEEDED, error=None)


def _obo_exec(monkeypatch, executed):
    class _Exec:
        def execute_statement(self, warehouse_id, statement, wait_timeout=None, parameters=None):
            executed.append(statement)
            return _OkResp()

        def get_statement(self, statement_id):
            return _OkResp()

    monkeypatch.setattr(
        apply_service, "require_obo_workspace_client",
        lambda: types.SimpleNamespace(statement_execution=_Exec()),
    )
    monkeypatch.setenv("SQL_WAREHOUSE_ID", "wh1")


async def test_undo_noop_guard_skips_when_already_at_post_value(monkeypatch):
    executed: list[str] = []
    audited: list[dict] = []
    _obo_exec(monkeypatch, executed)
    monkeypatch.setattr(apply_service, "_write_audit_row", lambda **k: audited.append(k))
    monkeypatch.setattr(apply_service, "_reflip_consents", lambda **k: None)
    # The asset is ALREADY at the post-undo value → the inverse is a no-op (double-undo safe).
    monkeypatch.setattr(grants, "current_tag_value", lambda *a, **k: "Legacy")

    item = models.ApplyItem(
        proposal_id="s", proposal_kind="reassign", shape="set_tag", target_fqn="c.s.t",
        tag_key="k", tag_value="Legacy", current_value="Rev",
        statement="SET TAG ON TABLE `c`.`s`.`t` `k` = `Legacy`", executable=True,
    )
    plan = models.ApplyPlan(
        items=[item], executable_count=1, blocked_count=0, plan_hash="h", source="mirror", as_of="t"
    )
    result = await apply_service.execute_undo_plan(
        plan=plan, metastore_id="ms1", workspace_id="ws1", applied_by="u@x"
    )
    assert executed == []                       # nothing was written to the warehouse
    assert audited and audited[0]["state"] == "noop"  # the no-op is recorded in the audit trail
    assert [o.state for o in result.applied] == ["applied"]  # user-facing: already reverted


async def test_undo_appends_new_audit_row_with_inverse_shape(monkeypatch):
    executed: list[str] = []
    audited: list[dict] = []
    _obo_exec(monkeypatch, executed)
    monkeypatch.setattr(apply_service, "_write_audit_row", lambda **k: audited.append(k))
    monkeypatch.setattr(apply_service, "_reflip_consents", lambda **k: None)
    # The asset currently carries "Rev" (what the original add set); target None → UNSET runs.
    monkeypatch.setattr(grants, "current_tag_value", lambda *a, **k: "Rev")

    # The inverse of an add: UNSET, tag_value None, current_value = the value reverted FROM.
    item = models.ApplyItem(
        proposal_id="s", proposal_kind="domain", shape="unset_tag", target_fqn="c.s.t",
        tag_key="biz", tag_value=None, current_value="Rev",
        statement="UNSET TAG ON TABLE `c`.`s`.`t` `biz`", executable=True,
    )
    plan = models.ApplyPlan(
        items=[item], executable_count=1, blocked_count=0, plan_hash="h", source="mirror", as_of="t"
    )
    result = await apply_service.execute_undo_plan(
        plan=plan, metastore_id="ms1", workspace_id="ws1", applied_by="u@x"
    )
    assert executed == ["UNSET TAG ON TABLE `c`.`s`.`t` `biz`"]
    assert [o.state for o in result.applied] == ["applied"]
    row = audited[0]
    assert row["shape"] == "unset_tag"          # the NEW row carries the inverse shape
    assert row["state"] == "applied"
    assert row["prev_value"] == "Rev"           # the value reverted FROM
    # The inverse apply_id is derived from the inverse shape/value (idempotent on repeat).
    assert row["apply_id"] == apply_service._apply_id("s", "unset_tag", "c.s.t", None)


async def test_undo_blocked_item_is_copy_ready_not_executed(monkeypatch):
    executed: list[str] = []
    _obo_exec(monkeypatch, executed)
    monkeypatch.setattr(apply_service, "_write_audit_row", lambda **k: None)
    monkeypatch.setattr(apply_service, "_reflip_consents", lambda **k: None)
    monkeypatch.setattr(grants, "current_tag_value", lambda *a, **k: "x")

    blocked = models.ApplyItem(
        proposal_id="s", proposal_kind="domain", shape="unset_tag", target_fqn="c.s.b",
        tag_key="biz", tag_value=None, current_value="Rev",
        statement="UNSET TAG ON TABLE `c`.`s`.`b` `biz`", executable=False,
        blocked_reason="needs a grant",
        required_grants=["GRANT APPLY TAG ON TABLE `c`.`s`.`b` TO `you`"],
    )
    plan = models.ApplyPlan(
        items=[blocked], executable_count=0, blocked_count=1, plan_hash="h", source="mirror", as_of="t"
    )
    result = await apply_service.execute_undo_plan(
        plan=plan, metastore_id="ms1", workspace_id="ws1", applied_by="u@x"
    )
    assert executed == []                                   # the blocked inverse never ran
    assert [o.target_fqn for o in result.blocked] == ["c.s.b"]
    assert result.applied == [] and result.failed == []


def test_reflip_consents_binds_values_and_flips_applied_to_approved(monkeypatch):
    captured: dict = {}

    class _Resp:
        statement_id = None
        status = None

    class _Exec:
        def execute_statement(self, warehouse_id, statement, wait_timeout=None, parameters=None):
            captured["sql"] = statement
            captured["params"] = parameters
            return _Resp()

    import backend.services.auth as auth_mod

    monkeypatch.setattr(
        auth_mod, "get_service_principal_client",
        lambda: types.SimpleNamespace(statement_execution=_Exec()),
    )
    monkeypatch.setenv("SQL_WAREHOUSE_ID", "wh1")

    apply_service._reflip_consents(
        metastore_id="ms1", workspace_id="ws1", proposal_ids={"a", "b"}, applied_by="u@x"
    )
    sql = captured["sql"]
    # The inverse of _flip_consents: applied → approved (re-surfaces the proposal).
    assert "SET state = 'approved'" in sql
    assert "state = 'applied'" in sql
    assert ":pid0" in sql and ":pid1" in sql  # one bound marker per id
    by_name = {p.name: p.value for p in captured["params"]}
    assert by_name["metastore_id"] == "ms1"
    assert sorted([by_name["pid0"], by_name["pid1"]]) == ["a", "b"]


# ── mirror: the applied-audit reader is metastore + applied scoped ────────────


async def test_read_applied_memberships_scopes_applied_and_ids(monkeypatch):
    captured: dict = {}

    def _delta(sql, params=None):
        captured["sql"] = sql
        return [_applied_row()]

    monkeypatch.setattr(mirror, "_delta_query", _delta)
    out = await mirror.read_applied_memberships("ms1", ["sug_a"])
    assert "state = 'applied'" in captured["sql"]
    assert "proposal_id IN ('sug_a')" in captured["sql"]
    assert out and out[0]["shape"] == "set_tag"
    # Empty id list → no read (byte-identical to "nothing to undo").
    assert await mirror.read_applied_memberships("ms1", []) == []


# ── undo routes: dry-run purity + the two-step consent gate ───────────────────


def test_undo_preview_writes_nothing(client, monkeypatch):
    calls = {"build": 0}

    async def _plan(_ms, _ids):
        calls["build"] += 1
        return models.ApplyPlan(
            items=[], executable_count=0, blocked_count=0, plan_hash="h", source="mirror", as_of="t"
        )

    def _no_exec(**k):
        raise AssertionError("undo-preview must never execute")

    monkeypatch.setattr(apply_service, "build_undo_plan", _plan)
    monkeypatch.setattr(apply_service, "execute_undo_plan", _no_exec)
    resp = client.post(
        "/api/ontology/apply/undo-preview",
        json={"plan_hash": "", "confirm": False, "proposal_ids": ["sug_a"]},
    )
    assert resp.status_code == 200
    assert calls["build"] == 1


def test_undo_requires_confirm(client):
    resp = client.post(
        "/api/ontology/apply/undo",
        json={"plan_hash": "abc", "confirm": False, "proposal_ids": ["sug_a"]},
    )
    assert resp.status_code == 400


def test_undo_409_on_plan_hash_mismatch(client, monkeypatch):
    async def _plan(_ms, _ids):
        return models.ApplyPlan(
            items=[], executable_count=0, blocked_count=0, plan_hash="real", source="mirror", as_of="t"
        )

    monkeypatch.setattr(apply_service, "build_undo_plan", _plan)
    resp = client.post(
        "/api/ontology/apply/undo",
        json={"plan_hash": "stale", "confirm": True, "proposal_ids": ["sug_a"]},
    )
    assert resp.status_code == 409


def test_undo_runs_and_attributes_to_obo_caller(client, monkeypatch):
    async def _plan(_ms, _ids):
        return models.ApplyPlan(
            items=[], executable_count=0, blocked_count=0, plan_hash="h", source="mirror", as_of="t"
        )

    captured: dict = {}

    async def _exec(*, plan, metastore_id, workspace_id, applied_by):
        captured.update(metastore_id=metastore_id, workspace_id=workspace_id, applied_by=applied_by)
        return models.ApplyResult(as_of="t")

    monkeypatch.setattr(apply_service, "build_undo_plan", _plan)
    monkeypatch.setattr(apply_service, "execute_undo_plan", _exec)
    resp = client.post(
        "/api/ontology/apply/undo",
        json={"plan_hash": "h", "confirm": True, "proposal_ids": ["sug_a"]},
        headers={"x-forwarded-email": "curator@databricks.com"},
    )
    assert resp.status_code == 200
    assert captured == {"metastore_id": "ms1", "workspace_id": "ws1", "applied_by": "curator@databricks.com"}
