"""Phase 4a — extended self-check failure evidence."""
from __future__ import annotations

from genie_space_optimizer.optimization.rca import RcaKind, build_rca_card


def test_self_check_failure_record_carries_ungrounded_terms():
    """Force ungrounded_term via sql_diff atoms that don't appear in
    the SQL corpus. blame_set is empty, so the proposed atoms have
    no fallback grounding channel.
    """
    metadata_snapshot: dict = {}
    asi = {
        "qid_x": {
            "failure_type": "missing_filter",
            "blame_set": [],  # empty so blame channel doesn't ground
            "counterfactual_fix": "",
            # sql_diff payload — extract_filter_terms reads filters.actual
            # for ungrounded proposed terms.
            "sql_diff": {
                "filters": {
                    "actual": ["t.unobserved_col = 'X'"],
                    "expected": [],
                }
            },
        }
    }
    # SQL corpus does NOT contain unobserved_col → self-grounding
    # will reject the proposed term.
    result = build_rca_card(
        cluster_id="C_x",
        qids=("qid_x",),
        asi_metadata=asi,
        generated_sql_by_qid={"qid_x": "SELECT 1 FROM tbl"},
        reference_sql_by_qid={"qid_x": "SELECT 1 FROM tbl"},
        metadata_snapshot=metadata_snapshot,
    )
    assert result["rca_id"] == ""
    failures = metadata_snapshot["_rca_card_self_check_failures"]
    assert len(failures) == 1
    record = failures[0]
    assert record["cluster_id"] == "C_x"
    assert record["qids"] == ["qid_x"]
    # Phase 4a additions — present regardless of which failure_reason fired.
    assert "dominant_root_cause" in record
    assert record["dominant_root_cause"] == RcaKind.FILTER_LOGIC_MISMATCH.value
    assert "ungrounded_terms" in record
    # At least one term proposed by the sql_diff filter extraction
    # should be in the ungrounded set.
    assert len(record["ungrounded_terms"]) >= 1


def test_self_check_failure_record_evidence_v2_off_keeps_legacy_shape(monkeypatch):
    """When GSO_RCA_CARD_SELF_CHECK_EVIDENCE_V2=0, the record stays
    the legacy 3-field shape."""
    monkeypatch.setenv("GSO_RCA_CARD_SELF_CHECK_EVIDENCE_V2", "0")
    # Reload config to pick up the env override.
    import importlib
    from genie_space_optimizer.common import config
    importlib.reload(config)

    metadata_snapshot: dict = {}
    asi = {
        "qid_x": {
            "failure_type": "missing_filter",
            "blame_set": [],
            "counterfactual_fix": "",
            "sql_diff": {
                "filters": {
                    "actual": ["t.unobserved_col = 'X'"],
                    "expected": [],
                }
            },
        }
    }
    result = build_rca_card(
        cluster_id="C_x",
        qids=("qid_x",),
        asi_metadata=asi,
        generated_sql_by_qid={"qid_x": "SELECT 1 FROM tbl"},
        reference_sql_by_qid={"qid_x": "SELECT 1 FROM tbl"},
        metadata_snapshot=metadata_snapshot,
    )
    assert result["rca_id"] == ""
    failures = metadata_snapshot["_rca_card_self_check_failures"]
    assert len(failures) == 1
    record = failures[0]
    # Legacy 3 fields present; v2 fields absent.
    assert set(record.keys()) == {"cluster_id", "qids", "failure_reason"}
