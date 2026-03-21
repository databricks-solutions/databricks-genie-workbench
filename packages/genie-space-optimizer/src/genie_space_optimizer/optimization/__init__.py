"""
Optimization engine — evaluation, scoring, repeatability, state tracking,
failure analysis, patch application, benchmark management, and the
multi-task optimization harness.

Sub-modules:
  - ``evaluation``: predict function, helpers, MLflow integration, benchmark gen
  - ``scorers``: 8 judges assembled via ``make_all_scorers()``
  - ``repeatability``: SQL generation consistency testing
  - ``state``: Delta-backed state machine for optimization runs
  - ``optimizer``: failure clustering, proposal generation, conflict detection
  - ``applier``: patch rendering, application, rollback
  - ``benchmarks``: benchmark loading, validation, splitting, corrections
  - ``harness``: 5 stage functions + ``optimize_genie_space()`` convenience fn
  - ``preflight``: extracted Stage 1 logic (config, metadata, benchmarks)
  - ``models``: MLflow LoggedModel management (create, promote, rollback)
  - ``report``: comprehensive Markdown report generation
"""
