from genie_space_optimizer.optimization.rca import (
    RcaKind,
    rca_findings_from_eval_rows,
    rca_themes_from_findings,
)


def test_rca_detects_extra_equality_filter_absent_from_expected_sql():
    row = {
        "inputs/question_id": "gs_026",
        "expected_sql": "SELECT SUM(PAYMENT_AMT) FROM tkt_payment",
        "generated_sql": (
            "SELECT SUM(PAYMENT_AMT) FROM tkt_payment "
            "WHERE PAYMENT_CURRENCY_CD = 'USD'"
        ),
    }

    findings = rca_findings_from_eval_rows([row])

    assert any(f.rca_kind is RcaKind.EXTRA_DEFENSIVE_FILTER for f in findings)
    finding = next(f for f in findings if f.rca_kind is RcaKind.EXTRA_DEFENSIVE_FILTER)
    assert "PAYMENT_CURRENCY_CD = 'USD'" in finding.actual_objects


def test_rca_theme_for_extra_filter_teaches_null_group_preservation_and_amount_semantics():
    row = {
        "inputs/question_id": "gs_026",
        "expected_sql": "SELECT SUM(PAYMENT_AMT) FROM tkt_payment",
        "generated_sql": (
            "SELECT SUM(PAYMENT_AMT) FROM tkt_payment "
            "WHERE PAYMENT_CURRENCY_CD = 'USD' AND PAYMENT_METHOD IS NOT NULL"
        ),
    }
    findings = rca_findings_from_eval_rows([row])
    themes = rca_themes_from_findings(findings)
    patches = [patch for theme in themes for patch in theme.patches]
    intents = " ".join(str(p.get("intent") or p.get("instruction") or "") for p in patches)

    assert "do not add unrequested equality filters" in intents
    assert "preserve null groups" in intents
    assert "amount column already encodes the measure" in intents
