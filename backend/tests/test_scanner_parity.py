"""Parity tests for the extracted IQ scoring module.

Ensures that backend/services/scanner.py and
genie_space_optimizer.iq_scan.scoring produce byte-identical output for the
canonical space-config fixtures defined in backend/tests/conftest.py.

These tests guard against drift after the PR-0 extraction: if the two paths
diverge, the scoring logic has accidentally been forked.
"""

import pytest

from backend.services import scanner as backend_scanner
from genie_space_optimizer.iq_scan import scoring as gso_scoring


def _normalize(result: dict) -> dict:
    """Drop the wall-clock ``scanned_at`` field so results can be compared byte-for-byte."""
    return {k: v for k, v in result.items() if k != "scanned_at"}


def test_backend_scanner_reexports_gso_scoring():
    """The backend shim must delegate to the exact same callable."""
    assert backend_scanner.calculate_score is gso_scoring.calculate_score
    assert backend_scanner.get_maturity_label is gso_scoring.get_maturity_label
    assert backend_scanner.CONFIG_CHECK_COUNT == gso_scoring.CONFIG_CHECK_COUNT


class TestScoringParity:
    """Same inputs → same outputs across both import paths."""

    def test_full_space_with_accuracy(self, full_space_data):
        backend = backend_scanner.calculate_score(full_space_data, optimization_run={"accuracy": 0.90})
        gso = gso_scoring.calculate_score(full_space_data, optimization_run={"accuracy": 0.90})
        assert _normalize(backend) == _normalize(gso)
        assert backend["score"] == 12

    def test_full_space_no_optimization_run(self, full_space_data):
        backend = backend_scanner.calculate_score(full_space_data, optimization_run=None)
        gso = gso_scoring.calculate_score(full_space_data, optimization_run=None)
        assert _normalize(backend) == _normalize(gso)

    def test_empty_space(self, empty_space_data):
        backend = backend_scanner.calculate_score(empty_space_data)
        gso = gso_scoring.calculate_score(empty_space_data)
        assert _normalize(backend) == _normalize(gso)
        assert backend["score"] == 0
        assert backend["maturity"] == "Not Ready"

    def test_metric_view_only_space(self, metric_view_only_space):
        backend = backend_scanner.calculate_score(metric_view_only_space)
        gso = gso_scoring.calculate_score(metric_view_only_space)
        assert _normalize(backend) == _normalize(gso)

    def test_low_accuracy_run(self, full_space_data):
        backend = backend_scanner.calculate_score(full_space_data, optimization_run={"accuracy": 0.50})
        gso = gso_scoring.calculate_score(full_space_data, optimization_run={"accuracy": 0.50})
        assert _normalize(backend) == _normalize(gso)
        # 10 config checks + optimization run recorded = 11; accuracy check fails.
        assert backend["score"] == 11
