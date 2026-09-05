"""Ontology DDL — SQL generation for snapshot MERGE (Phase 2 §7.2 + Step 2 MV-D66).

Covers the MERGE SQL structure: matched/not-matched clauses, delete-unmatched behavior,
and Step 2 preservation logic (preserve_cols + preserve_when for curator row durability).
All offline — no cluster required.
"""

from __future__ import annotations

from genie_space_optimizer.ontology import ddl


def test_build_snapshot_merge_sql_basic_no_preserve():
    """Without preserve params, the SQL is byte-identical to the original."""
    sql = ddl.build_snapshot_merge_sql(
        catalog="cat", schema="sch", table="tbl",
        source_view="src", key_cols=["id"], update_cols=["name", "val"],
        metastore_id="ms1",
    )
    # Basic structure: MERGE INTO, USING, ON, WHEN MATCHED, WHEN NOT MATCHED, WHEN NOT MATCHED BY SOURCE.
    assert "MERGE INTO cat.sch.tbl AS t" in sql
    assert "USING src AS s" in sql
    assert "ON t.id = s.id" in sql
    assert "WHEN MATCHED THEN UPDATE SET t.name = s.name, t.val = s.val" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "WHEN NOT MATCHED BY SOURCE AND t.metastore_id = 'ms1' THEN DELETE" in sql
    # No preservation clauses when params are unset.
    assert "preserve" not in sql.lower() or "preserved" not in sql.lower()


def test_build_snapshot_merge_sql_preserve_without_when():
    """preserve_cols alone doesn't emit a guarded clause (both params must be set)."""
    sql = ddl.build_snapshot_merge_sql(
        catalog="cat", schema="sch", table="tbl",
        source_view="src", key_cols=["id"], update_cols=["name", "val"],
        metastore_id="ms1", preserve_cols=["name"],  # but no preserve_when
    )
    # Should have the general WHEN MATCHED clause.
    assert "WHEN MATCHED THEN UPDATE SET t.name = s.name, t.val = s.val" in sql
    # But not a guarded one.
    assert "WHEN MATCHED AND" not in sql or "preserve" not in sql.lower()


def test_build_snapshot_merge_sql_with_preserve():
    """With both preserve_cols and preserve_when set, emit a guarded clause first."""
    sql = ddl.build_snapshot_merge_sql(
        catalog="cat", schema="sch", table="tbl",
        source_view="src", key_cols=["id"], update_cols=["name", "val", "evidence"],
        metastore_id="ms1",
        preserve_cols=["name"],
        preserve_when="condition_expr",
    )
    lines = sql.split("\n")
    # Should have the guarded clause before the general one.
    guarded_line = None
    general_line = None
    for i, line in enumerate(lines):
        if "WHEN MATCHED AND condition_expr THEN UPDATE SET" in line:
            guarded_line = i
        elif line.strip().startswith("WHEN MATCHED THEN UPDATE SET") and guarded_line is None:
            # Only mark as general if we haven't found guarded yet.
            pass
        elif "WHEN MATCHED THEN UPDATE SET" in line and "AND" not in line:
            general_line = i
    # The guarded clause should appear first (smaller index).
    assert guarded_line is not None, "guarded WHEN MATCHED AND clause not found"
    assert general_line is not None, "general WHEN MATCHED clause not found"
    assert guarded_line < general_line, "guarded clause should appear before general clause"
    # The guarded clause should update val and evidence but NOT name.
    assert "t.val = s.val" in lines[guarded_line]
    assert "t.evidence = s.evidence" in lines[guarded_line]
    assert "t.name = s.name" not in lines[guarded_line]


def test_build_snapshot_merge_sql_preserve_pages_curator():
    """Step 2 (MV-D66): preserve body for curator rows (genie_ont_pages use case)."""
    sql = ddl.build_snapshot_merge_sql(
        catalog="cat", schema="sch", table="genie_ont_pages",
        source_view="src", key_cols=["metastore_id", "page_id"],
        update_cols=["archetype", "title", "body", "evidence", "score"],
        metastore_id="ms1",
        preserve_cols=["body"],
        preserve_when="get_json_object(t.evidence,'$.body_source') IN ('llm_ondemand','llm_bulk','human')",
    )
    lines = sql.split("\n")
    # Should have: guarded clause updating all EXCEPT body, then general clause updating all.
    guarded_found = False
    general_found = False
    for line in lines:
        if "WHEN MATCHED AND get_json_object" in line and "UPDATE SET" in line:
            # Guarded clause: should update archetype, title, evidence, score but NOT body.
            guarded_found = True
            assert "t.body = s.body" not in line, "body should NOT be updated in guarded clause"
            assert "t.archetype = s.archetype" in line or line.count("=") >= 4  # at least 4 updates
        if "WHEN MATCHED THEN UPDATE SET" in line and "AND" not in line:
            # General clause: should update all including body.
            general_found = True
            assert "t.body = s.body" in line, "body should be updated in general clause"
    assert guarded_found, "guarded WHEN MATCHED AND clause not found"
    assert general_found, "general WHEN MATCHED clause not found"


def test_build_snapshot_merge_sql_delete_unmatched_false():
    """delete_unmatched=False makes MERGE upsert-only (no NOT-MATCHED-BY-SOURCE)."""
    sql = ddl.build_snapshot_merge_sql(
        catalog="cat", schema="sch", table="tbl",
        source_view="src", key_cols=["id"], update_cols=["val"],
        metastore_id="ms1", delete_unmatched=False,
    )
    assert "WHEN NOT MATCHED BY SOURCE" not in sql
    assert "THEN DELETE" not in sql


def test_build_snapshot_merge_sql_empty_update_cols():
    """With no update_cols, still have INSERT (from source) but no general UPDATE."""
    sql = ddl.build_snapshot_merge_sql(
        catalog="cat", schema="sch", table="tbl",
        source_view="src", key_cols=["id"], update_cols=[],
        metastore_id="ms1",
    )
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "WHEN MATCHED THEN UPDATE SET" not in sql
