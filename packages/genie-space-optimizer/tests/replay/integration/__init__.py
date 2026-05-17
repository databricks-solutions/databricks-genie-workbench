"""Phase 4.5 (2026-05-16) — harness-integration tests with stubbed I/O.

These tests exercise the real ``_run_lever_loop`` against fixture
inputs with deterministic stubs in place of WorkspaceClient,
SparkSession, the LLM client, and MLflow. They are slower than
the wiring-site tests in ``tests/replay/active/`` and require
substantial stub plumbing — see the Phase 4.5 implementation plan
for the cost/value framing and the abandonment criteria.
"""
