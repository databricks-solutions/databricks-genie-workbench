"""
Optimization engine — native benchmark evaluation, state tracking, failure
analysis, patch application, benchmark management, and the active unified
optimization loop.

Sub-modules:
  - ``evaluation``: predict function, helpers, MLflow integration, benchmark gen
  - ``scorers``: 8 judges assembled via ``make_all_scorers()``
  - ``repeatability``: SQL generation consistency testing
  - ``state``: Delta-backed state machine for optimization runs
  - ``optimizer``: failure clustering, proposal generation, conflict detection
  - ``applier``: patch rendering, application, rollback
  - ``benchmarks``: benchmark loading, validation, corpus normalization, corrections
  - ``unified_loop``: active eval -> analyze -> patch -> eval optimizer loop
  - ``harness``: retired compatibility module for historical tests/replay fixtures
  - ``preflight``: extracted Stage 1 logic (config, metadata, benchmarks)
  - ``models``: MLflow LoggedModel management (create, promote, rollback)
  - ``report``: comprehensive Markdown report generation
"""
