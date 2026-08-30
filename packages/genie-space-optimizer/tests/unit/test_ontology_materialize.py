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
    delete scoped to workspace_id (mirrors §7.2)."""

    def __init__(self):
        self.tables: dict[str, dict] = {}
        self.runs: dict[str, dict] = {}
        self.ensured = 0

    def ensure_tables(self):
        self.ensured += 1

    def upsert_run(self, row):
        self.runs[row["run_id"]] = dict(row)

    def merge(self, table, rows, key_cols, workspace_id):
        store = self.tables.setdefault(table, {})
        src_keys = {tuple(r[k] for k in key_cols) for r in rows}
        for k in list(store):
            if store[k].get("workspace_id") == workspace_id and k not in src_keys:
                del store[k]
        for r in rows:
            store[tuple(r[k] for k in key_cols)] = dict(r)


def _run(reader, writer, *, run_id):
    from datetime import datetime, timezone

    return materialize.run_materialize(
        reader, writer, workspace_id="ws1", trigger="on_demand",
        allowlist=["finance"], run_id=run_id,
        now=datetime.fromisoformat(_AS_OF).astimezone(timezone.utc),
    )


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
    stored_tree = json.loads(writer.tables["genie_ont_taxonomy_snapshot"][("ws1",)]["tree"])
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
    assert ("ws1", "sensitivity") in writer.tables["genie_ont_tag_graph"]

    # Remove a tag between runs → it is deleted (NOT MATCHED BY SOURCE).
    trimmed = [r for r in catalog_rows if r.get("tag_name") != "sensitivity"]
    trimmed_assign = [r for r in assign_rows if r.get("tag_name") != "sensitivity"]
    _run(_FakeReader(trimmed, trimmed_assign, [], []), writer, run_id="r2")
    assert ("ws1", "sensitivity") not in writer.tables["genie_ont_tag_graph"]

    # Add a new tag → it appears exactly once.
    added = trimmed + [{"tag_name": "Ops"}, {"tag_name": "Ops/Fulfillment"}]
    added_assign = trimmed_assign + [
        {"tag_name": "Ops/Fulfillment", "catalog_name": "finance", "schema_name": "ops", "table_name": "orders"},
    ]
    _run(_FakeReader(added, added_assign, [], []), writer, run_id="r3")
    ops_keys = [k for k in writer.tables["genie_ont_tag_graph"] if k[1] == "Ops"]
    assert ops_keys == [("ws1", "Ops")]


def test_pages_consents_suppressions_never_written():
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1")
    written = set(writer.tables)
    # Phase 3b now writes the Domain/Member proposal tables too; only the 17f/17g
    # tables (pages/consents/suppressions) stay empty.
    assert written == {
        "genie_ont_tag_graph", "genie_ont_taxonomy_snapshot", "genie_ont_identity",
        "genie_ont_domains", "genie_ont_members",
    }
    for t in ddl.PHASE3_TABLES:
        assert t not in written


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
    assert run["tag_count"] == 4 and run["domain_count"] == 1


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
        json.loads(row["evidence"])
        assert row["score"] == 0.0  # L6 ranking is 17g


def test_stale_domains_deleted_when_graph_changes():
    catalog_rows, assign_rows = _fixture_rows()
    writer = _FakeWriter()
    _run(_FakeReader(catalog_rows, assign_rows, [], []), writer, run_id="r1")
    before = set(writer.tables["genie_ont_domains"])

    # Add a disjoint tagged asset -> the community set changes, so old fingerprints
    # vanish; NOT MATCHED BY SOURCE deletes the stale domain rows (no orphans).
    added = catalog_rows + [{"tag_name": "Ops"}]
    added_assign = assign_rows + [
        {"tag_name": "Ops", "catalog_name": "ops", "schema_name": "core", "table_name": "events"},
    ]
    _run(_FakeReader(added, added_assign, [], []), writer, run_id="r2")
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
        workspace_id="ws1",
    )
    assert "MERGE INTO c.s.genie_ont_tag_graph" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "WHEN NOT MATCHED BY SOURCE AND t.workspace_id = 'ws1' THEN DELETE" in sql


def test_merge_sql_empty_update_cols_omits_matched_clause():
    sql = ddl.build_snapshot_merge_sql(
        catalog="c", schema="s", table="genie_ont_tag_graph", source_view="v",
        key_cols=["workspace_id", "tag_key"], update_cols=[], workspace_id="ws1",
    )
    assert "WHEN MATCHED THEN UPDATE SET" not in sql
    assert "WHEN NOT MATCHED BY SOURCE" in sql


def test_ddl_shape_exactly_nine_tables_no_deferred_tokens():
    rendered = ddl.all_ddl("maincat", "gso_schema")
    assert set(rendered) == set(ddl.SNAPSHOT_TABLES) | set(ddl.PROPOSAL_TABLES) | set(ddl.PHASE3_TABLES)
    # 4 snapshot tables + 2 written proposal tables + 3 still-empty (pages/consents/suppressions).
    assert len(rendered) == 9
    joined = "\n".join(rendered.values()).lower()
    for stmt in rendered.values():
        assert stmt.startswith("CREATE TABLE IF NOT EXISTS maincat.gso_schema.")
    # No Phase-3/4 substrate, no governed-tag writes anywhere in the DDL.
    for tok in ("lakebase_vector", "lakebase_text", "web_search", "set tag", "create governed tag"):
        assert tok not in joined


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
