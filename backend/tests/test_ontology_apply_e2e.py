"""Ontology apply→undo E2E (Phase 5 / 17j) — offline, mocked SDK.

The full round trip the live write path never had an automated test for:

    approve → preview → execute → (audit row asserted) → undo-preview → undo →
    asset back to pre-state, undo audit row present, consent back to `approved`.

Everything is in-memory: a tiny estate store (the on-asset governed-tag value), the
`genie_ont_applied` audit "table", and the consent ledger state. The OBO warehouse is a
fake that mutates the estate by parsing the SET/UNSET TAG statement the service emits, so
the pre-state assertion after undo reflects the statements that actually ran. No warehouse,
no UC write, no OBO round trip — the identity split + gates are exercised by the service.
"""

from __future__ import annotations

import types

from databricks.sdk.service.sql import StatementState

from backend.ontology import models
from backend.ontology.services import apply as apply_service
from backend.ontology.services import grants, mirror


class _OkResp:
    statement_id = "sid"
    status = types.SimpleNamespace(state=StatementState.SUCCEEDED, error=None)


class _FakeWarehouse:
    """Applies a SET/UNSET TAG statement to an in-memory estate keyed by plain FQN."""

    def __init__(self, assets: dict[str, str]):
        self.assets = assets
        self.executed: list[str] = []

    def execute_statement(self, warehouse_id, statement, wait_timeout=None, parameters=None):
        self.executed.append(statement)
        toks = statement.split()
        fqn = toks[4].replace("`", "")  # `cat`.`sch`.`orders` → cat.sch.orders
        if statement.startswith("SET TAG"):
            self.assets[fqn] = statement.split("= `", 1)[1][:-1].replace("``", "`")
        elif statement.startswith("UNSET TAG"):
            self.assets.pop(fqn, None)
        return _OkResp()

    def get_statement(self, statement_id):
        return _OkResp()


async def test_ontology_apply_e2e(monkeypatch):
    # ── in-memory estate + ledgers (mocked SDK) ──
    assets: dict[str, str] = {}          # fqn → on-asset governed-tag value
    applied_rows: list[dict] = []        # the genie_ont_applied audit table
    consent_state = {"sug_a": "approved"}  # the consent ledger state

    async def _consents(_ms):
        if consent_state.get("sug_a") != "approved":
            return []  # only an approved consent expands (the plan is empty once applied)
        return [
            {
                "proposal_kind": "domain", "proposal_id": "sug_a", "name": "Revenue",
                "tag_decision": "reuse", "tag_key": "business_domain",
                "tag_value": "Revenue", "conflict_tag": "",
            }
        ]

    async def _members(_ms, domain_id):
        return [{"asset_fqn": "cat.sch.orders", "asset_type": "table"}] if domain_id == "sug_a" else []

    async def _applied(_ms, ids):
        want = set(ids)
        return [dict(r) for r in applied_rows if r["state"] == "applied" and r["proposal_id"] in want]

    monkeypatch.setattr(mirror, "read_approved_consents", _consents)
    monkeypatch.setattr(mirror, "read_domain_members", _members)
    monkeypatch.setattr(mirror, "read_applied_memberships", _applied)
    monkeypatch.setattr(grants, "membership_write_probe", lambda *a, **k: (True, []))
    # The on-asset value drives the preview diff AND the undo no-op guard.
    monkeypatch.setattr(grants, "current_tag_value", lambda _c, fqn, _k, **kw: assets.get(fqn))

    # SP bookkeeping: append audit rows (idempotent on apply_id), flip the consent ledger.
    def _audit(**row):
        applied_rows[:] = [r for r in applied_rows if r["apply_id"] != row["apply_id"]]
        applied_rows.append(row)

    def _flip(**k):
        for pid in k["proposal_ids"]:
            consent_state[pid] = "applied"

    def _reflip(**k):
        for pid in k["proposal_ids"]:
            consent_state[pid] = "approved"

    monkeypatch.setattr(apply_service, "_write_audit_row", _audit)
    monkeypatch.setattr(apply_service, "_flip_consents", _flip)
    monkeypatch.setattr(apply_service, "_reflip_consents", _reflip)

    obo = _FakeWarehouse(assets)
    monkeypatch.setattr(
        apply_service, "require_obo_workspace_client",
        lambda: types.SimpleNamespace(statement_execution=obo),
    )
    monkeypatch.setenv("SQL_WAREHOUSE_ID", "wh1")

    # ── approve → preview → execute ──
    plan = await apply_service.build_apply_plan("ms1")
    assert [i.shape for i in plan.items] == ["set_tag"]
    assert plan.items[0].current_value is None  # a plain add (no prior value)

    result = await apply_service.execute_apply_plan(
        plan=plan, metastore_id="ms1", workspace_id="ws1", applied_by="curator@databricks.com"
    )
    assert [o.state for o in result.applied] == ["applied"]
    assert assets["cat.sch.orders"] == "Revenue"                # the tag landed
    assert any(                                                 # an applied audit row exists
        r["shape"] == "set_tag" and r["state"] == "applied" and r["target_fqn"] == "cat.sch.orders"
        for r in applied_rows
    )
    assert consent_state["sug_a"] == "applied"                  # consent flipped

    # ── undo-preview → undo ──
    undo_plan = await apply_service.build_undo_plan("ms1", ["sug_a"])
    assert [i.shape for i in undo_plan.items] == ["unset_tag"]  # the inverse of an add
    undo_hash = undo_plan.plan_hash

    undo_result = await apply_service.execute_undo_plan(
        plan=undo_plan, metastore_id="ms1", workspace_id="ws1", applied_by="curator@databricks.com"
    )
    assert [o.state for o in undo_result.applied] == ["applied"]
    assert "cat.sch.orders" not in assets                       # asset back to pre-state
    # A NEW undo audit row exists (inverse shape) AND the original set_tag row is untouched.
    assert any(r["shape"] == "unset_tag" and r["state"] == "applied" for r in applied_rows)
    assert any(r["shape"] == "set_tag" and r["state"] == "applied" for r in applied_rows)
    assert consent_state["sug_a"] == "approved"                 # consent re-surfaced
    assert undo_hash  # the inverse plan was fingerprinted for the confirm gate

    # Idempotent / double-undo safe: re-running the SAME inverse plan is a no-op — the asset
    # is already at pre-state, so the no-op guard runs NO new statement (only the apply's SET
    # and the first undo's UNSET ever hit the warehouse).
    again = await apply_service.execute_undo_plan(
        plan=undo_plan, metastore_id="ms1", workspace_id="ws1", applied_by="curator@databricks.com"
    )
    assert [o.state for o in again.applied] == ["applied"]  # recorded as already-reverted
    assert "cat.sch.orders" not in assets
    assert obo.executed == [
        "SET TAG ON TABLE `cat`.`sch`.`orders` `business_domain` = `Revenue`",
        "UNSET TAG ON TABLE `cat`.`sch`.`orders` `business_domain`",
    ]
