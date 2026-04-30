from genie_space_optimizer.optimization.reflection_retry import (
    patch_retry_signature,
    retry_allowed_after_rollback,
)


def test_patch_retry_signature_includes_column_and_instruction_section():
    patch = {
        "type": "update_column_description",
        "table": "cat.sch.tkt_payment",
        "column": "PAYMENT_AMT",
        "instruction_section": "QUERY CONSTRUCTION",
    }

    assert patch_retry_signature(patch) == (
        "update_column_description",
        "cat.sch.tkt_payment",
        "PAYMENT_AMT",
        frozenset({"QUERY CONSTRUCTION"}),
    )


def test_retry_not_blocked_for_different_column_on_same_table():
    previous = {
        "type": "update_column_description",
        "target_table": "cat.sch.tkt_payment",
        "column": "PAYMENT_CURRENCY_CD",
    }
    current = {
        "type": "update_column_description",
        "target_table": "cat.sch.tkt_payment",
        "column": "PAYMENT_AMT",
    }

    decision = retry_allowed_after_rollback(
        current_patch=current,
        rolled_back_patches=[previous],
        rollback_cause="insufficient_gain",
    )

    assert decision.allowed is True
    assert decision.reason == "new_precise_patch_signature"


def test_retry_allowed_when_bundle_adds_direct_l6_behavior_patch():
    previous = {
        "type": "update_column_description",
        "target_table": "cat.sch.tkt_payment",
        "column": "PAYMENT_AMT",
    }
    current = {
        "type": "add_sql_snippet_filter",
        "lever": 6,
        "target_table": "cat.sch.tkt_payment",
        "column": "PAYMENT_CURRENCY_CD",
        "root_cause": "wrong_filter_condition",
    }

    decision = retry_allowed_after_rollback(
        current_patch=current,
        rolled_back_patches=[previous],
        rollback_cause="target_still_hard",
    )

    assert decision.allowed is True
    assert decision.reason == "adds_direct_behavior_shape"
