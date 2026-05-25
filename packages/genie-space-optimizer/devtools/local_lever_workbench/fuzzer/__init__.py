"""Local lever-loop workbench fuzzer (v1.7).

Two layers on top of the v1.6 workbench:

* :mod:`invariants` — 12 pure-Python predicates over the state-machine
  contract. Always-on in ``tests/workbench/test_state_machine_invariants.py``.
* :mod:`generators` (chunks 2-3) — permutation + synthetic input
  generators for procedural exploration.
* :mod:`shrinker` (chunk 4) — greedy drop-one shrinker that minimises a
  triggering input on invariant violation.

The fuzzer is deliberately dependency-free: deterministic-seeded
``random`` only, no ``hypothesis`` or third-party tooling. See
``docs/llmdrivenarchitecture/v5/workbench_v1_7_invariant_fuzzer_acceb17f.plan.md``
for the architectural rationale.
"""
from __future__ import annotations

from local_lever_workbench.fuzzer.invariants import (
    InvariantResult,
    InvariantViolation,
    check_all_invariants,
)
from local_lever_workbench.fuzzer.shrinker import ShrinkResult, shrink_bundle

__all__ = [
    "InvariantResult",
    "InvariantViolation",
    "ShrinkResult",
    "check_all_invariants",
    "shrink_bundle",
]
