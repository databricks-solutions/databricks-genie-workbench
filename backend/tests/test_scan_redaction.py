"""Stored IQ scans quote space content in three strings; viewers get none of the quotes,
and history rows carry none of the scan (MV-D110)."""

import json

from backend.services import lakebase
from backend.services.scanner import calculate_score, redact_for_viewer

_NOISY = ["etl_batch_id", "raw_payload_json", "debug_flag", "audit_user", "col_1", "load_timestamp"]
_INSTRUCTION = "Use SELECT * FROM zq_orders WHERE region = 'AMER' for American orders."


def _scan() -> dict:
    cols = [{"name": f"business_col_{i}", "description": "Useful business column"} for i in range(14)]
    cols[0]["enable_entity_matching"] = True
    cols += [{"name": name} for name in _NOISY]
    return calculate_score({
        "data_sources": {"tables": [{"name": "zq_fact_table", "row_filter": "true", "columns": cols}]},
        "instructions": {"text_instructions": [{"content": [_INSTRUCTION]}]},
        "benchmarks": {},
    })


def test_the_instruction_excerpt_is_dropped_and_the_warning_kept():
    scan = _scan()
    assert any("zq_orders" in w for w in scan["warnings"])  # positive control

    redacted = redact_for_viewer(scan)

    sql_warnings = [w for w in redacted["warnings"] if w.startswith("SQL patterns found")]
    assert sql_warnings == [
        "SQL patterns found in text instructions — move to Example SQLs or SQL Expressions."
    ]


def test_the_noisy_column_names_are_dropped_and_the_count_kept():
    scan = _scan()
    assert any("etl_batch_id" in f for f in scan["findings"])  # positive control

    redacted = redact_for_viewer(scan)

    assert "6/20 visible columns look internal/noisy" in redacted["findings"]


def test_the_row_level_security_table_names_are_dropped_and_the_warning_kept():
    scan = _scan()
    assert any("row-level security" in w and "zq_fact_table" in w for w in scan["warnings"])  # positive control

    redacted = redact_for_viewer(scan)

    rls_warnings = [w for w in redacted["warnings"] if "row-level security" in w]
    assert rls_warnings == ["Tables with row-level security — entity matching is silently disabled for these"]


def test_no_space_content_survives_anywhere_in_the_redacted_scan():
    scan = _scan()
    details = [c["detail"] for c in scan["checks"]]
    assert "6/20 visible columns look internal/noisy (30%)" in details  # positive control

    redacted = json.dumps(redact_for_viewer(scan))
    for literal in [*_NOISY, "zq_orders", "AMER", "zq_fact_table"]:
        assert literal not in redacted, literal
    assert redact_for_viewer(None) is None


def test_a_finding_with_no_viewer_safe_form_is_blank_and_keeps_its_remediation():
    scan = _scan()
    scan = {**scan, "findings": ["Legacy text naming zq_secret", *scan["findings"][1:]]}
    redacted = redact_for_viewer(scan)
    assert redacted["findings"][0] == ""
    assert redacted["next_steps"] == scan["next_steps"]
    assert len(redacted["findings"]) == len(scan["findings"])


def test_a_warning_with_no_viewer_safe_form_is_blank_in_place():
    scan = _scan()
    scan = {**scan, "warnings": ["zq_secret advisory", *scan["warnings"]],
            "warning_next_steps": ["a step", *scan["warning_next_steps"]]}
    redacted = redact_for_viewer(scan)
    assert redacted["warnings"][0] == ""
    assert redacted["warning_next_steps"] == scan["warning_next_steps"]


def test_check_details_are_viewer_safe_and_labels_kept():
    scan = _scan()
    checks = [*scan["checks"], {"label": "Agent description", "passed": False,
                                "detail": "zq_secret detail", "severity": "fail"}]
    redacted = redact_for_viewer({**scan, "checks": checks})
    assert [c["label"] for c in redacted["checks"]] == [c["label"] for c in checks]
    assert redacted["checks"][-1]["detail"] is None
    assert any(c["detail"] for c in redacted["checks"])  # positive control: safe details kept
    assert "zq_secret" not in json.dumps(redacted)


def test_an_older_scorer_s_wording_is_blank_not_shown():
    scan = {**_scan(), "findings": ["Missing or placeholder space description"],
            "next_steps": ["Add a space description"]}
    assert redact_for_viewer(scan)["findings"] == [""]


async def test_memory_mode_history_rows_carry_only_score_maturity_accuracy_and_time(monkeypatch):
    monkeypatch.setattr(lakebase, "_lakebase_available", False)
    monkeypatch.setattr(lakebase, "_pool", None)
    monkeypatch.setattr(lakebase, "_memory_store", {**lakebase._memory_store, "scans": {}, "history": {}, "seen": set()})
    scan = {**_scan(), "optimization_accuracy": 0.8}
    assert scan["findings"] and scan["warnings"] and scan["checks"]  # positive control

    await lakebase.save_scan_result("s1", scan)
    rows = await lakebase.get_score_history("s1")

    assert rows == [{
        "score": scan["score"], "maturity": scan["maturity"],
        "optimization_accuracy": 0.8, "scanned_at": scan["scanned_at"],
    }]
