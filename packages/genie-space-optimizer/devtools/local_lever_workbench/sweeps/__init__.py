"""Live workbench sweeps (Trial 16 v1.8).

Sweep scripts under this folder issue real Databricks calls and are
meant to be invoked manually by an operator — not from pytest. They
loop the workbench over the production-replay corpus (or any other
bundle source), apply the v1.7 invariant fuzzer to each run, and
write a markdown bug-discovery report.

Each sweep prints a one-line summary per QID so an operator can
follow along without tailing the report file.
"""
from __future__ import annotations
