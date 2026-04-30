def test_previous_sql_and_repeatability_are_meta_zero_weight():
    from genie_space_optimizer.optimization.judge_classes import (
        SignalClass,
        judge_signal_class,
        judge_weight_for_root_cause,
    )

    assert judge_signal_class("previous_sql") is SignalClass.META
    assert judge_signal_class("repeatability") is SignalClass.META
    assert judge_weight_for_root_cause("previous_sql") == 0.0
    assert judge_weight_for_root_cause("repeatability") == 0.0
