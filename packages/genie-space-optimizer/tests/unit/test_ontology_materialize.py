"""Ontology materializer — offline unit tests (Phase-2 §11).

Covers mirror-vs-live parity (same shared transforms → byte-identical JSON),
idempotent re-run (MERGE + NOT-MATCHED-BY-SOURCE delete, no duplicates), the
"only snapshot tables written / Phase-3 empty" guarantee, and the DDL + MERGE
shapes. All offline — the job's pure logic runs without a cluster via fakes.
"""

from __future__ import annotations

import json

from genie_space_optimizer.ontology import ddl, graph, materialize, transforms

_AS_OF = "2026-08-30T00:00:00+00:00"


def _fixture_rows():
    catalog_rows = [
        {"tag_name": "Finance"},
        {"tag_name": "Finance/Tax"},
        {"tag_name": "finance"},          # lowercase collision + orphan (0 assigns)
        {"tag_name": "sensitivity", "allowed_values": ["public", "internal"]},
    ]
    assign_rows = [
        {"tag_name": "Finance", "catalog_name": "finance", "schema_name": "core", "table_name": "ledger"},
        {"tag_name": "Finance/Tax", "catalog_name": "finance", "schema_name": "tax", "table_name": "filings"},
        {"tag_name": "sensitivity", "catalog_name": "finance", "schema_name": "core", "table_name": "ledger"},
    ]
    return catalog_rows, assign_rows


class _FakeReader:
    def __init__(self, catalog_rows, assign_rows, metric_views, agents):
        self._c, self._a, self._mv, self._ag = catalog_rows, assign_rows, metric_views, agents

    def governed_tags(self):
        return self._c

    def assignments(self, allowlist):
        return self._a

    def metric_view_fqns(self, allowlist):
        return self._mv

    def agents(self):
        return self._ag

    def lineage_edges(self, allowlist):
        return [("finance.core.ledger", "finance.tax.filings")]


class _FakeWriter:
    """In-memory Delta MERGE semantics: upsert by key + NOT-MATCHED-BY-SOURCE
    delete scoped to metastore_id (mirrors §7.2, re-grained MV-D49). A row of a
    different metastore is never touched by this run."""

    def __init__(self):
        self.tables: dict[str, dict] = {}
        self.runs: dict[str, dict] = {}
        self.ensured = 0

    def ensure_tables(self):
        self.ensured += 1

    def upsert_run(self, row):
        self.runs[row["run_id"]] = dict(row)

    def merge(self, table, rows, key_cols, metastore_id):
        store = self.tables.setdefault(table, {})
        src_keys = {tuple(r[k] for k in key_cols) for r in rows}
        for k in list(store):
            if store[k].get("metastore_id") == metastore_id and k not in src_keys:
                del store[k]
        for r in rows:
            store[tuple(r[k] for k in key_cols)] = dict(r)


def _run(reader, writer, *, run_id, metastore_id="ms1", workspace_id="ws1", **kw):
    from datetime import datetime, timezone

    return materialize.run_materialize(
        reader, writer, metastore_id=metastore_id, workspace_id=workspace_id,
        trigger="on_demand", allowlist=["finance"], run_id=run_id,
        now=datetime.fromisoformat(_AS_OF).astimezone(timezone.utc), **kw,
    )


# ── Phase 3c: Page mining through the materializer (§11) ────────────────────


class _PageReader(_FakeReader):
    """A reader that also surfaces the L5 miner inputs (measure/column signals)."""

    def __init__(self, catalog_rows, assign_rows, metric_views, agents, *, measures=(), columns=(), instructions=()):
        super().__init__(catalog_rows, assign_rows, metric_views, agents)
        self._measures, self._columns, self._instr = list(measures), list(columns), list(instructions)

    def measure_signals(self, allowlist):
        return self._measures

    def coded_column_signals(self, allowlist):
        return self._columns

    def space_instructions(self):
        return self._instr


def _page_drafter(facts):
    lines = [f"Description: {facts['description']}", "", "Definition:", f"  {facts['definition']}"]
    if facts["rules"]:
        lines.append("")
        lines.append("Rules:")
        lines.extend(f"  - {r}" for r in facts["rules"])
    return "\n".join(lines)


def _rev_measures():
    from genie_space_optimizer.ontology.pages import MeasureSignal

    # Same concept (total_revenue) in two DIFFERENT sub-domains → ONE Page.
    return [
        MeasureSignal(mv_fqn="finance.core.rev_mv", name="total_revenue", expression="SUM(a)",
                      comment="TR; net sales; revenue booked", source_fqns=("finance.core.ledger",),
                      agent_fqns=("Sales · 01ef",), domain_id="sug_finance"),
        MeasureSignal(mv_fqn="finance.tax.rev_mv", name="total_revenue", expression="SUM(a)",
                      comment="TR; net sales; revenue booked", source_fqns=("finance.tax.filings",),
                      agent_fqns=("Tax · 02aa",), domain_id="sug_tax"),
    ]


def _page_reader(**over):
    catalog_rows, assign_rows = _fixture_rows()
    kw = dict(measures=_rev_measures())
    kw.update(over)
    return _PageReader(catalog_rows, assign_rows, [], [], **kw)


def test_pages_mined_metastore_keyed_and_concept_collapsed():
    writer = _FakeWriter()
    run = _run(_page_reader(), writer, run_id="r1", page_drafter=_page_drafter)
    assert run["state"] == "succeeded"
    pages_tbl = writer.tables["genie_ont_pages"]
    # Two measures, same concept, two sub-domains → ONE Page (canonical-concept keying).
    routing = [v for v in pages_tbl.values() if v["archetype"] == "Routing"]
    assert len(routing) == 1
    row = routing[0]
    # Keyed (metastore_id, page_id); workspace_id is provenance, never in the key.
    for key in pages_tbl:
        assert key[0] == "ms1" and "ws1" not in key
    assert row["workspace_id"] == "ws1"
    # Sources aggregate BOTH sub-domains' MVs.
    assert {"finance.core.rev_mv", "finance.tax.rev_mv"} <= set(row["source_fqns"])
    # page_count on the ledger.
    assert run["page_count"] == len(pages_tbl)


def test_pages_idempotent_rerun_stable_page_ids():
    writer = _FakeWriter()
    _run(_page_reader(), writer, run_id="r1", page_drafter=_page_drafter)
    keys1 = set(writer.tables["genie_ont_pages"])
    _run(_page_reader(), writer, run_id="r2", page_drafter=_page_drafter)
    keys2 = set(writer.tables["genie_ont_pages"])
    assert keys1 == keys2 and keys1  # same concept-anchored page_ids, no dups


def test_page_losing_all_signal_is_deleted_not_matched_by_source():
    writer = _FakeWriter()
    _run(_page_reader(), writer, run_id="r1", page_drafter=_page_drafter)
    assert writer.tables["genie_ont_pages"]
    # Re-run with no measure signal → the concept loses all signal → Page removed.
    _run(_page_reader(measures=[]), writer, run_id="r2", page_drafter=_page_drafter)
    assert writer.tables["genie_ont_pages"] == {}


def test_page_metastore_scoped_delete_leaves_other_metastore_intact():
    writer = _FakeWriter()
    _run(_page_reader(), writer, run_id="r1", metastore_id="ms1", page_drafter=_page_drafter)
    _run(_page_reader(), writer, run_id="r2", metastore_id="ms2", page_drafter=_page_drafter)
    ms2_pages = {k for k in writer.tables["genie_ont_pages"] if k[0] == "ms2"}
    assert ms2_pages
    # Re-run ms1 with no signal → ms1's Pages go, ms2's survive (metastore-scoped delete).
    _run(_page_reader(measures=[]), writer, run_id="r3", metastore_id="ms1", page_drafter=_page_drafter)
    remaining = set(writer.tables["genie_ont_pages"])
    assert remaining == ms2_pages


def test_page_mining_error_records_failed_but_keeps_earlier_snapshots(monkeypatch):
    import pytest

    from genie_space_optimizer.ontology import pages as pages_mod

    writer = _FakeWriter()

    def _boom(*a, **k):
        raise RuntimeError("page miner boom")

    monkeypatch.setattr(pages_mod, "mine_pages", _boom)
    with pytest.raises(RuntimeError):
        _run(_page_reader(), writer, run_id="rP", page_drafter=_page_drafter)

    # Additive-LAST (§8): everything MERGEd before page mining survives intact.
    assert writer.tables.get("genie_ont_tag_graph")
    assert writer.tables.get("genie_ont_identity")
    assert writer.tables.get("genie_ont_domains")
    # No pages table written, and the run is recorded failed.
    assert "genie_ont_pages" not in writer.tables
    assert writer.runs["rP"]["state"] == "failed"
    assert "page miner boom" in (writer.runs["rP"]["error"] or "")


def test_mirror_vs_live_parity_tree_and_tag_graph():
    catalog_rows, assign_rows = _fixture_rows()
    metric_views, agents = ["finance.rep.untagged_mv"], ["Sales · 01ef"]
    writer = _FakeWriter()
    run = _run(_FakeReader(catalog_rows, assign_rows, metric_views, agents), writer, run_id="r1")
    assert run["state"] == "succeeded"

    # Live path: the transforms the Phase-1 route calls, with the run's as_of.
    graph_struct = transforms.assemble_tag_graph(catalog_rows, assign_rows, _AS_OF)
    tree_live = transforms.build_taxonomy_dict(graph_struct, metric_views, agents)
    gtags_live = transforms.governed_tag_rows(graph_struct)

    # Mirror path: what the materializer wrote.
    stored_tree = json.loads(writer.tables["genie_ont_taxonomy_snapshot"][("ms1",)]["tree"])
    assert stored_tree == tree_live  # byte-identical JSON

    stored_tags = writer.tables["genie_ont_tag_graph"]
    assert {k[1] for k in stored_tags} == {g["tag_key"] for g in gtags_live}
    by_key = {g["tag_key"]: g for g in gtags_live}
    for (_, tag_key), row in stored_tags.items():
        g = by_key[tag_key]
        assert row["assignment_count"] == g["assignment_count"]
        assert row["acts_as_domain"] == g["acts_as_domain"]
        assert row["acts_as_subdomain"] == g["acts_as_subdomain"]


def test_idempotent_rerun_no_duplicates():
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1")
    keys_after_1 = set(writer.tables["genie_ont_tag_graph"])
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r2")
    keys_after_2 = set(writer.tables["genie_ont_tag_graph"])
    assert keys_after_1 == keys_after_2  # same rows, no duplicates
    # One taxonomy row per workspace across re-runs.
    assert len(writer.tables["genie_ont_taxonomy_snapshot"]) == 1


def test_not_matched_by_source_delete_and_add_once():
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1")
    assert ("ms1", "sensitivity") in writer.tables["genie_ont_tag_graph"]

    # Remove a tag between runs → it is deleted (NOT MATCHED BY SOURCE).
    trimmed = [r for r in catalog_rows if r.get("tag_name") != "sensitivity"]
    trimmed_assign = [r for r in assign_rows if r.get("tag_name") != "sensitivity"]
    _run(_FakeReader(trimmed, trimmed_assign, [], []), writer, run_id="r2")
    assert ("ms1", "sensitivity") not in writer.tables["genie_ont_tag_graph"]

    # Add a new tag → it appears exactly once.
    added = trimmed + [{"tag_name": "Ops"}, {"tag_name": "Ops/Fulfillment"}]
    added_assign = trimmed_assign + [
        {"tag_name": "Ops/Fulfillment", "catalog_name": "finance", "schema_name": "ops", "table_name": "orders"},
    ]
    _run(_FakeReader(added, added_assign, [], []), writer, run_id="r3")
    ops_keys = [k for k in writer.tables["genie_ont_tag_graph"] if k[1] == "Ops"]
    assert ops_keys == [("ms1", "Ops")]


def test_consents_suppressions_never_written_pages_now_written():
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1")
    written = set(writer.tables)
    # Phase 3c now MERGEs genie_ont_pages too (even when the estate mines zero Pages —
    # the empty MERGE clears stale rows). Only the 17g ledger tables stay empty.
    assert written == {
        "genie_ont_tag_graph", "genie_ont_taxonomy_snapshot", "genie_ont_identity",
        "genie_ont_domains", "genie_ont_members", "genie_ont_pages",
    }
    for t in ddl.PHASE3_TABLES:
        assert t not in written
    assert ddl.PHASE3_TABLES == ("genie_ont_consents", "genie_ont_suppressions")
    # A fixture with no measure/column signal mines zero Pages, but the MERGE still ran.
    assert writer.tables["genie_ont_pages"] == {}


# ── Empty-scope guard (MV-D49 safety): no scope never wipes a good snapshot ──

def test_empty_allowlist_is_skipped_and_preserves_snapshot():
    """A refresh with no catalog scope must not clear the last good mirror.

    The first (scoped) run lands a snapshot; a second run with an empty allowlist
    records a terminal ``skipped`` header and issues NO MERGE, so every derived table —
    and the prior ``succeeded`` run the UI serves from — is preserved byte-for-byte.
    """
    from datetime import datetime, timezone

    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    good = _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1")
    assert good["state"] == "succeeded"
    before = {t: dict(rows) for t, rows in writer.tables.items()}
    assert before["genie_ont_domains"]  # the scoped run produced Domain rows

    skipped = materialize.run_materialize(
        _FakeReader(catalog_rows, assign_rows, [], []), writer,
        metastore_id="ms1", workspace_id="ws1", trigger="on_demand",
        allowlist=[], run_id="r2",
        now=datetime.fromisoformat(_AS_OF).astimezone(timezone.utc),
    )

    # The empty-scope run is a terminal skip that touched no snapshot table.
    assert skipped["state"] == "skipped"
    assert "empty catalog allowlist" in (skipped["error"] or "")
    assert writer.tables == before  # no MERGE issued → snapshot preserved
    # The ledger keeps the prior succeeded run (what latest_succeeded_run serves).
    assert writer.runs["r1"]["state"] == "succeeded"
    assert writer.runs["r2"]["state"] == "skipped"


def test_whitespace_only_allowlist_is_also_skipped():
    """A whitespace/empty-string allowlist is still no scope (guards sloppy config)."""
    from datetime import datetime, timezone

    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    run = materialize.run_materialize(
        _FakeReader(catalog_rows, assign_rows, [], []), writer,
        metastore_id="ms1", workspace_id="ws1", trigger="scheduled",
        allowlist=["", "   "], run_id="r1",
        now=datetime.fromisoformat(_AS_OF).astimezone(timezone.utc),
    )
    assert run["state"] == "skipped"
    assert writer.tables == {}  # nothing written at all on a first empty run


def test_has_scope_helper():
    assert materialize._has_scope(["finance"]) is True
    assert materialize._has_scope(["", "  ", "ops"]) is True
    assert materialize._has_scope([]) is False
    assert materialize._has_scope(None) is False
    assert materialize._has_scope(["", "   "]) is False


def test_failed_run_records_error_and_reraises():
    class _Boom(_FakeReader):
        def governed_tags(self):
            raise RuntimeError("system.tags.governed_tags not readable")

    writer = _FakeWriter()
    import pytest
    with pytest.raises(RuntimeError):
        _run(_Boom([], [], [], []), writer, run_id="rX")
    assert writer.runs["rX"]["state"] == "failed"
    assert "not readable" in (writer.runs["rX"]["error"] or "")


def test_run_ledger_one_row_per_run_and_running_then_terminal():
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    run = _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1")
    assert list(writer.runs) == ["r1"]  # one header per run_id (upsert)
    assert run["state"] == "succeeded"
    # Stage 2 (MV-D54): the governed ``Finance/Tax`` slash sub-tag surfaces a Tax
    # sub-domain under the Finance domain (explicit boundary), so the proposal set is
    # the Finance domain + its Tax sub-domain = 2 rows.
    assert run["tag_count"] == 4 and run["domain_count"] == 2

    # A second run keeps the first run's header — the ledger is history, not a
    # single latest row (guards the upsert-only MERGE for genie_ont_runs).
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r2")
    assert set(writer.runs) == {"r1", "r2"}
    assert writer.runs["r1"]["run_id"] == "r1" and writer.runs["r2"]["run_id"] == "r2"


def test_identity_map_idempotent_stable_canonical_no_dups():
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1")
    keys1 = set(writer.tables["genie_ont_identity"])
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r2")
    keys2 = set(writer.tables["genie_ont_identity"])
    # Stable canonical_id (fingerprint of members) → identical (cid, member_ref) keys.
    assert keys1 == keys2
    # Every member appears exactly once (no duplicates).
    member_refs = [k[2] for k in keys2]
    assert len(member_refs) == len(set(member_refs))
    # Finance + finance resolve to one canonical (exact-casefold merge).
    id_rows = writer.tables["genie_ont_identity"]
    cid = {k[2]: k[1] for k in id_rows}
    assert cid["Finance"] == cid["finance"]


def test_identity_member_removed_is_deleted():
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1")
    assert any(k[2] == "finance" for k in writer.tables["genie_ont_identity"])

    trimmed = [r for r in catalog_rows if r.get("tag_name") != "finance"]
    trimmed_assign = [r for r in assign_rows if r.get("tag_name") != "finance"]
    _run(_FakeReader(trimmed, trimmed_assign, [], []), writer, run_id="r2")
    # NOT MATCHED BY SOURCE: the removed member no longer appears in the map.
    assert not any(k[2] == "finance" for k in writer.tables["genie_ont_identity"])
    assert any(k[2] == "Finance" for k in writer.tables["genie_ont_identity"])


def test_domain_member_proposals_idempotent_no_dups():
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1")
    d1 = set(writer.tables["genie_ont_domains"])
    m1 = set(writer.tables["genie_ont_members"])
    assert d1  # the fixture yields at least one Domain proposal
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r2")
    # Stable domain_id (fingerprint of sorted members) -> identical rows, no dups.
    assert set(writer.tables["genie_ont_domains"]) == d1
    assert set(writer.tables["genie_ont_members"]) == m1
    # Every domain row's evidence is JSON and tag_decision is in the frozen vocabulary.
    for row in writer.tables["genie_ont_domains"].values():
        assert row["tag_decision"] in ("reuse", "create", "reassign")
        ev = json.loads(row["evidence"])
        # Phase 3d (17g): L6 ranking now writes a numeric score + a rank block + the
        # surfaced flag into evidence (was 0.0 / absent pre-17g). Deterministic, so the
        # idempotent re-run above still produced identical rows.
        assert isinstance(row["score"], float) and 0.0 <= row["score"] <= 100.0
        assert "rank" in ev and "surfaced" in ev
        assert ev["rank"]["tier"] in ("high", "medium", "low", None)


# ── Stage 3: legitimacy bar + facet-denylist config through the materializer ──


def _top_domain(writer):
    for row in writer.tables["genie_ont_domains"].values():
        if row.get("parent_id") is None:
            return row
    return None


def test_run_param_less_uses_default_legitimacy_bar():
    # A param-less run uses the shipped moderate defaults (≥3 tables / ≥2 schemas). The
    # fixture's Finance domain spans 2 tables / 2 schemas → kept but not surfaced.
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1")
    ev = json.loads(_top_domain(writer)["evidence"])
    assert ev["rank"]["legitimate"] is False
    assert ev["surfaced"] is False
    assert ev["gate_hint"].startswith("add to existing domain:")


def test_run_param_driven_bar_lets_small_domain_pass():
    # A param-driven run lowers the bar; the same 2-table Finance domain now clears it.
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1",
         domain_min_tables=1, domain_min_schemas=1, domain_require_connection=False)
    ev = json.loads(_top_domain(writer)["evidence"])
    assert ev["rank"]["legitimate"] is True
    assert "gate_hint" not in ev


def test_facet_denylist_config_routes_tag_out_of_domains():
    from datetime import datetime, timezone

    catalog_rows = [{"tag_name": "widget_kind"}]
    assign_rows = [
        {"tag_name": "widget_kind", "catalog_name": "c", "schema_name": "s", "table_name": "t1"},
        {"tag_name": "widget_kind", "catalog_name": "c", "schema_name": "s", "table_name": "t2"},
    ]

    def go(denylist):
        writer = _FakeWriter()
        materialize.run_materialize(
            _LineageReader(catalog_rows, assign_rows, [], [], lineage=[("c.s.t1", "c.s.t2")]),
            writer, metastore_id="ms1", workspace_id="ws1", trigger="on_demand",
            allowlist=["c"], run_id="r1",
            now=datetime.fromisoformat(_AS_OF).astimezone(timezone.utc), facet_denylist=denylist,
        )
        return writer.tables.get("genie_ont_domains", {})

    # Without the denylist, widget_kind binds a Domain (reuse); with it, it's a facet
    # and routed OUT of domain candidacy (the structural community still forms, but as
    # a create — never reusing the facet tag).
    assert any((r.get("tag_key") or "") == "widget_kind" for r in go(None).values())
    assert all((r.get("tag_key") or "") != "widget_kind" for r in go(["widget_kind"]).values())


class _LineageReader(_FakeReader):
    """A reader with a custom lineage edge set (the base reader hard-codes one pair)."""

    def __init__(self, catalog_rows, assign_rows, metric_views, agents, *, lineage):
        super().__init__(catalog_rows, assign_rows, metric_views, agents)
        self._lineage = lineage

    def lineage_edges(self, allowlist):
        return self._lineage


def test_stale_domains_deleted_when_graph_changes():
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1")
    before = set(writer.tables["genie_ont_domains"])

    # Add a disjoint, STRUCTURALLY-connected Ops domain (two assets joined by lineage —
    # a tag alone never solo-creates a Domain, MV-D52). The community set changes, so old
    # fingerprints vanish; NOT MATCHED BY SOURCE deletes the stale domain rows (no orphans).
    added = catalog_rows + [{"tag_name": "Ops"}]
    added_assign = assign_rows + [
        {"tag_name": "Ops", "catalog_name": "ops", "schema_name": "core", "table_name": "events"},
        {"tag_name": "Ops", "catalog_name": "ops", "schema_name": "core", "table_name": "summary"},
    ]
    reader = _LineageReader(added, added_assign, [], [], lineage=[
        ("finance.core.ledger", "finance.tax.filings"),
        ("ops.core.events", "ops.core.summary"),
    ])
    _run(reader, writer, run_id="r2")
    after = set(writer.tables["genie_ont_domains"])
    # No stale domain_id survives that is not in the new run's output.
    r2_ids = {k for k, v in writer.tables["genie_ont_domains"].items() if v["run_id"] == "r2"}
    assert after == r2_ids
    assert after != before  # the graph changed


def test_clustering_error_records_failed_but_keeps_snapshots(monkeypatch):
    import pytest

    from genie_space_optimizer.ontology import cluster as cluster_mod

    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()

    def _boom(*a, **k):
        raise RuntimeError("leiden boom")

    monkeypatch.setattr(cluster_mod, "cluster", _boom)
    with pytest.raises(RuntimeError):
        _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="rC")

    # Additive-LAST (§8): the snapshots written BEFORE clustering survive intact.
    assert writer.tables.get("genie_ont_tag_graph")
    assert writer.tables.get("genie_ont_taxonomy_snapshot")
    assert writer.tables.get("genie_ont_identity")
    # No partial proposal rows, and the run is recorded failed.
    assert "genie_ont_domains" not in writer.tables
    assert writer.runs["rC"]["state"] == "failed"
    assert "leiden boom" in (writer.runs["rC"]["error"] or "")


def test_merge_sql_shape_not_matched_by_source_scoped():
    sql = ddl.build_snapshot_merge_sql(
        catalog="c", schema="s", table="genie_ont_tag_graph", source_view="v",
        key_cols=materialize.TAG_GRAPH_KEYS, update_cols=materialize.TAG_GRAPH_UPDATE_COLS,
        metastore_id="ms1",
    )
    assert "MERGE INTO c.s.genie_ont_tag_graph" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    # Re-grained (MV-D49): the delete predicate is METASTORE-scoped, never workspace.
    assert "WHEN NOT MATCHED BY SOURCE AND t.metastore_id = 'ms1' THEN DELETE" in sql
    # workspace_id is never part of the delete predicate (it rides along as an
    # update col / provenance only).
    assert "BY SOURCE AND t.workspace_id" not in sql
    # The key leads with metastore_id; workspace_id rides along as an update col.
    assert materialize.TAG_GRAPH_KEYS[0] == "metastore_id"
    assert "workspace_id" in materialize.TAG_GRAPH_UPDATE_COLS


def test_merge_sql_empty_update_cols_omits_matched_clause():
    sql = ddl.build_snapshot_merge_sql(
        catalog="c", schema="s", table="genie_ont_tag_graph", source_view="v",
        key_cols=["metastore_id", "tag_key"], update_cols=[], metastore_id="ms1",
    )
    assert "WHEN MATCHED THEN UPDATE SET" not in sql
    assert "WHEN NOT MATCHED BY SOURCE" in sql


def test_merge_sql_delete_unmatched_false_is_upsert_only():
    """The run ledger (genie_ont_runs) MUST be upsert-only: run_id is unique per
    run, so a source-diff delete would wipe every prior run's header. Snapshots
    keep the default (delete_unmatched=True) to prune stale entities."""
    upsert_only = ddl.build_snapshot_merge_sql(
        catalog="c", schema="s", table="genie_ont_runs", source_view="v",
        key_cols=["run_id"], update_cols=["state", "finished_at"], metastore_id="ms1",
        delete_unmatched=False,
    )
    assert "WHEN MATCHED THEN UPDATE SET" in upsert_only
    assert "WHEN NOT MATCHED THEN INSERT" in upsert_only
    assert "WHEN NOT MATCHED BY SOURCE" not in upsert_only  # no prior-run wipe

    # Default keeps the source-diff delete (stale-entity prune) for snapshots.
    default_sql = ddl.build_snapshot_merge_sql(
        catalog="c", schema="s", table="genie_ont_runs", source_view="v",
        key_cols=["run_id"], update_cols=["state"], metastore_id="ms1",
    )
    assert "WHEN NOT MATCHED BY SOURCE AND t.metastore_id = 'ms1' THEN DELETE" in default_sql


def test_ddl_shape_exactly_nine_tables_no_deferred_tokens():
    rendered = ddl.all_ddl("maincat", "gso_schema")
    assert set(rendered) == (
        set(ddl.SNAPSHOT_TABLES) | set(ddl.PROPOSAL_TABLES)
        | set(ddl.PAGE_TABLES) | set(ddl.PHASE3_TABLES)
    )
    # 4 snapshot + 2 proposal + 1 page (now written) + 2 still-empty (consents/suppressions).
    assert len(rendered) == 9
    joined = "\n".join(rendered.values()).lower()
    for stmt in rendered.values():
        assert stmt.startswith("CREATE TABLE IF NOT EXISTS maincat.gso_schema.")
    # No Phase-3/4 substrate, no governed-tag writes anywhere in the DDL.
    for tok in ("lakebase_vector", "lakebase_text", "web_search", "set tag", "create governed tag"):
        assert tok not in joined


# ── Re-grain to metastore (MV-D49, spec §11) ────────────────────────────────


def test_convergence_same_metastore_two_workspaces_one_row_set():
    """Two installs of the SAME metastore (different provenance workspace_id) must
    converge on ONE row set — no duplicate Domain=Finance — and the second run's
    provenance is recorded without forking the metastore-led key."""
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()

    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1",
         metastore_id="ms1", workspace_id="wsA")
    d1 = set(writer.tables["genie_ont_domains"])
    tg1 = set(writer.tables["genie_ont_tag_graph"])

    # Second install, same metastore, DIFFERENT provenance workspace.
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r2",
         metastore_id="ms1", workspace_id="wsB")

    # Convergent: identical key sets, no duplicate rows.
    assert set(writer.tables["genie_ont_domains"]) == d1
    assert set(writer.tables["genie_ont_tag_graph"]) == tg1
    # One taxonomy row for the metastore (not one per workspace).
    assert len(writer.tables["genie_ont_taxonomy_snapshot"]) == 1
    # Provenance updated to the latest install without forking the key.
    tax_row = writer.tables["genie_ont_taxonomy_snapshot"][("ms1",)]
    assert tax_row["workspace_id"] == "wsB"
    for row in writer.tables["genie_ont_tag_graph"].values():
        assert row["workspace_id"] == "wsB"


def test_metastore_scoped_idempotency_other_metastore_not_deleted():
    """A run scoped to one metastore must NOT delete/alter another metastore's
    rows (NOT-MATCHED-BY-SOURCE is metastore-scoped)."""
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()

    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1",
         metastore_id="ms1", workspace_id="wsA")
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r2",
         metastore_id="ms2", workspace_id="wsB")

    tg = writer.tables["genie_ont_tag_graph"]
    # Both metastores hold their own Finance row — neither run wiped the other.
    assert ("ms1", "Finance") in tg and ("ms2", "Finance") in tg
    ms2_before = {k for k in tg if k[0] == "ms2"}

    # Re-running ms1 with a TRIMMED graph deletes ms1's stale row but leaves ms2 intact.
    trimmed = [r for r in catalog_rows if r.get("tag_name") != "sensitivity"]
    trimmed_assign = [r for r in assign_rows if r.get("tag_name") != "sensitivity"]
    _run(_FakeReader(trimmed, trimmed_assign, [], []), writer, run_id="r3",
         metastore_id="ms1", workspace_id="wsA")
    tg = writer.tables["genie_ont_tag_graph"]
    # ms2's full row set survived the ms1 re-run untouched…
    assert {k for k in tg if k[0] == "ms2"} == ms2_before
    assert ("ms2", "sensitivity") in tg
    # …while ms1's stale row was deleted (metastore-scoped NOT-MATCHED-BY-SOURCE).
    assert ("ms1", "sensitivity") not in tg


def test_provenance_retained_but_never_in_a_key():
    """workspace_id is present on every written row (provenance) but never appears
    in a key tuple or the delete predicate."""
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1",
         metastore_id="ms1", workspace_id="wsProv")

    for table in ("genie_ont_tag_graph", "genie_ont_taxonomy_snapshot",
                  "genie_ont_identity", "genie_ont_domains", "genie_ont_members"):
        rows = writer.tables[table]
        assert rows, f"{table} should have rows"
        for key, row in rows.items():
            # Provenance present on the row…
            assert row["workspace_id"] == "wsProv"
            # …but the metastore leads the key and the provenance value is not in it.
            assert key[0] == "ms1"
            assert "wsProv" not in key
    # Run ledger carries both metastore_id (grain) and workspace_id (provenance).
    ledger = writer.runs["r1"]
    assert ledger["metastore_id"] == "ms1" and ledger["workspace_id"] == "wsProv"


def test_degraded_metastore_id_still_runs_and_keys_on_stable_id():
    """MV-D43: a degraded/stable metastore id ('default') still yields a valid,
    convergent run keyed on that id."""
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    run = _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1",
               metastore_id="default", workspace_id="wsA")
    assert run["state"] == "succeeded"
    assert ("default",) in writer.tables["genie_ont_taxonomy_snapshot"]
    assert any(k[0] == "default" for k in writer.tables["genie_ont_tag_graph"])


def test_signal_graph_scaffold_nodes_and_edges_no_clustering():
    catalog_rows, assign_rows = _fixture_rows()
    g = transforms.assemble_tag_graph(catalog_rows, assign_rows, _AS_OF)
    sig = graph.build_signal_graph(g, [("finance.core.ledger", "finance.tax.filings")])
    kinds = {n["kind"] for n in sig["nodes"]}
    assert "tag" in kinds
    edge_kinds = {e["kind"] for e in sig["edges"]}
    assert edge_kinds == {"tag_assignment", "lineage_adjacency"}
    # Pure structure — no cluster/community keys.
    assert "clusters" not in sig and "communities" not in sig


def test_assemble_tag_graph_threads_tag_value_additively():
    """Stage 2 (MV-D54): a value-carrying assignment threads ``tag_value`` onto its
    member dict; a value-free assignment yields the exact ``{fqn, asset_type}`` shape
    (byte-identical), and ``build_taxonomy_dict`` still drops the extra key."""
    catalog_rows = [{"tag_name": "mvm_subdomain"}, {"tag_name": "Finance"}]
    assign_rows = [
        {"tag_name": "mvm_subdomain", "catalog_name": "rev", "schema_name": "core",
         "table_name": "fares", "tag_value": "fare_pricing"},
        {"tag_name": "Finance", "catalog_name": "finance", "schema_name": "core", "table_name": "ledger"},
    ]
    g = transforms.assemble_tag_graph(catalog_rows, assign_rows, _AS_OF)
    by_key = {t["tag_key"]: t for t in g["tags"]}
    assert by_key["mvm_subdomain"]["members"][0] == {
        "fqn": "rev.core.fares", "asset_type": "table", "tag_value": "fare_pricing"}
    # Value-free assignment is byte-identical to the pre-Stage-2 member shape.
    assert by_key["Finance"]["members"][0] == {"fqn": "finance.core.ledger", "asset_type": "table"}
    # The taxonomy tree drops the extra key (value-carrying tags are not domain tags).
    tree = transforms.build_taxonomy_dict(g, [], [])
    for dom in tree["domains"]:
        for m in dom["members"]:
            assert set(m) == {"fqn", "asset_type"}


def test_coerce_scalar_fits_explicit_schema_types():
    """SparkSnapshotWriter._df_for passes the target Delta schema to
    createDataFrame (serverless can't infer all-None columns). _coerce_scalar
    turns the row builders' ISO strings into native datetime/date for
    TIMESTAMP/DATE columns; everything else (incl. None) passes through."""
    from datetime import date, datetime

    # ISO strings → native objects for timestamp/date columns.
    ts = materialize._coerce_scalar("2026-08-30T00:00:00+00:00", "timestamp")
    assert isinstance(ts, datetime) and ts.tzinfo is not None
    assert materialize._coerce_scalar("2026-08-30T00:00:00Z", "timestamp") == (
        datetime.fromisoformat("2026-08-30T00:00:00+00:00")
    )
    assert materialize._coerce_scalar("2026-08-30", "date") == date(2026, 8, 30)

    # All-None column (the bug trigger) and every other type pass through.
    assert materialize._coerce_scalar(None, "timestamp") is None
    assert materialize._coerce_scalar("Finance", "string") == "Finance"
    assert materialize._coerce_scalar(5, "integer") == 5
    assert materialize._coerce_scalar(["a", "b"], "array") == ["a", "b"]
    assert materialize._coerce_scalar(True, "boolean") is True

    # An already-native datetime is left untouched (idempotent).
    now = datetime(2026, 8, 30)
    assert materialize._coerce_scalar(now, "timestamp") is now


# ── SparkSnapshotWriter MERGE generation (offline, via a fake Spark) ─────────
# The real writer builds each MERGE from the row's keys; nothing offline used to
# exercise it, which let the L6 report-count keys (and a page_count absent on a
# pre-existing table) leak into the run-ledger SQL and fail the LIVE run with
# UNRESOLVED_COLUMN. These tests pin the schema-projection fix without a cluster.
import types as _types


def _field(name, type_name="string"):
    return _types.SimpleNamespace(
        name=name, dataType=_types.SimpleNamespace(typeName=lambda tn=type_name: tn)
    )


class _FakeDF:
    def createOrReplaceTempView(self, name):
        return None


class _FakeSpark:
    """Minimal Spark double: ``.table(fqn).schema`` + ``.createDataFrame`` + ``.sql``
    (captured), enough to drive ``SparkSnapshotWriter`` MERGE generation offline."""

    def __init__(self, schemas: dict):
        self._schemas = schemas
        self.sqls: list[str] = []

    def table(self, fqn):
        name = fqn.split(".")[-1]
        return _types.SimpleNamespace(schema=list(self._schemas[name]))

    def createDataFrame(self, data, struct):
        return _FakeDF()

    def sql(self, query):
        self.sqls.append(query)
        return _FakeDF()


def test_upsert_run_drops_report_keys_and_absent_columns_from_merge_sql():
    """Terminal run row carries non-DDL report counts (surfaced/suppressed/blocked)
    and — on a table created before the column existed — page_count. The generated
    MERGE must reference ONLY real columns, else the live run fails UNRESOLVED_COLUMN."""
    runs_cols = [  # live-shaped schema WITHOUT page_count (the deployed state)
        "run_id", "metastore_id", "workspace_id", "trigger", "state",
        "scope_allowlist", "started_at", "finished_at", "as_of", "tag_count",
        "domain_count", "ungrouped_count", "identity_count", "error",
    ]
    spark = _FakeSpark({"genie_ont_runs": [_field(c) for c in runs_cols]})
    writer = materialize.SparkSnapshotWriter(spark, "cat", "sch")
    writer.upsert_run({
        "run_id": "r1", "metastore_id": "ms1", "workspace_id": "wsP", "state": "succeeded",
        "as_of": _AS_OF, "domain_count": 3, "identity_count": 2,
        "page_count": 4,                                              # absent from schema
        "surfaced_count": 5, "suppressed_count": 1, "blocked_count": 0,  # never DDL
    })
    sql = spark.sqls[-1]
    for banned in ("page_count", "surfaced_count", "suppressed_count", "blocked_count"):
        assert banned not in sql, f"{banned} leaked into the run-ledger MERGE"
    assert "domain_count" in sql and "identity_count" in sql  # real cols still written
    assert "not matched by source" not in sql.lower()         # upsert-only ledger


def test_upsert_run_keeps_page_count_once_the_column_exists():
    """After the ALTER / fresh DDL, page_count IS a real column and gets written."""
    runs_cols = ["run_id", "metastore_id", "state", "as_of", "domain_count", "page_count"]
    spark = _FakeSpark({"genie_ont_runs": [_field(c) for c in runs_cols]})
    writer = materialize.SparkSnapshotWriter(spark, "cat", "sch")
    writer.upsert_run({
        "run_id": "r1", "metastore_id": "ms1", "state": "succeeded", "as_of": _AS_OF,
        "domain_count": 3, "page_count": 4, "surfaced_count": 9,
    })
    sql = spark.sqls[-1]
    assert "page_count" in sql
    assert "surfaced_count" not in sql


def test_merge_projects_proposal_rows_to_schema():
    """merge() projects rows to the target schema too, so a stray non-DDL key on a
    proposal row cannot produce an unresolved-column MERGE."""
    dom_cols = ["metastore_id", "domain_id", "name", "score", "run_id", "as_of"]
    spark = _FakeSpark({"genie_ont_domains": [_field(c) for c in dom_cols]})
    writer = materialize.SparkSnapshotWriter(spark, "cat", "sch")
    writer.merge(
        "genie_ont_domains",
        [{"metastore_id": "ms1", "domain_id": "d1", "name": "Finance", "score": 1.0,
          "run_id": "r1", "as_of": _AS_OF, "surfaced_count": 7}],  # stray non-DDL key
        ["metastore_id", "domain_id"], "ms1",
    )
    sql = spark.sqls[-1]
    assert "surfaced_count" not in sql
    assert "score" in sql
