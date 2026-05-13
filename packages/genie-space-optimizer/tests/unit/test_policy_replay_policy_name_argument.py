"""Phase 1 Task 8 — policy_replay CLI accepts --policy-name and a
``attribution_drift_policy_pilot_default`` registry entry.

Tests the argument parsing + registry without invoking the full CLI
on disk fixtures (that's covered by the integration test in Task 10).
"""

from __future__ import annotations

import pytest


def test_policy_registry_contains_both_pilot_policies() -> None:
    from genie_space_optimizer.tools.policy_replay import _POLICY_REGISTRY

    assert "regression_debt_policy_pilot_default" in _POLICY_REGISTRY
    assert "attribution_drift_policy_pilot_default" in _POLICY_REGISTRY


def test_policy_registry_factories_return_correct_pilot_policies() -> None:
    from genie_space_optimizer.optimization.acceptance_policy import (
        attribution_drift_policy_pilot_default,
        regression_debt_policy_pilot_default,
    )
    from genie_space_optimizer.tools.policy_replay import _POLICY_REGISTRY

    assert (
        _POLICY_REGISTRY["regression_debt_policy_pilot_default"]()
        == regression_debt_policy_pilot_default()
    )
    assert (
        _POLICY_REGISTRY["attribution_drift_policy_pilot_default"]()
        == attribution_drift_policy_pilot_default()
    )


def test_cli_parser_has_policy_name_argument_with_default() -> None:
    from genie_space_optimizer.tools.policy_replay import _build_arg_parser

    parser = _build_arg_parser()
    namespace = parser.parse_args(
        [
            "--fixtures-dir",
            "/tmp/fakedir",
            "--predictions",
            "/tmp/predictions.json",
        ]
    )
    assert namespace.policy_name == "regression_debt_policy_pilot_default"


def test_cli_parser_accepts_explicit_policy_name() -> None:
    from genie_space_optimizer.tools.policy_replay import _build_arg_parser

    parser = _build_arg_parser()
    namespace = parser.parse_args(
        [
            "--fixtures-dir",
            "/tmp/fakedir",
            "--predictions",
            "/tmp/predictions.json",
            "--policy-name",
            "attribution_drift_policy_pilot_default",
        ]
    )
    assert namespace.policy_name == "attribution_drift_policy_pilot_default"


def test_cli_parser_rejects_unknown_policy_name(capsys) -> None:
    from genie_space_optimizer.tools.policy_replay import _build_arg_parser

    parser = _build_arg_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--fixtures-dir",
                "/tmp/fakedir",
                "--predictions",
                "/tmp/predictions.json",
                "--policy-name",
                "no_such_policy",
            ]
        )
    captured = capsys.readouterr()
    assert "no_such_policy" in captured.err or "invalid choice" in captured.err
