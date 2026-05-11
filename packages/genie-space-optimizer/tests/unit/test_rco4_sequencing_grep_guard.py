"""RCO-4 Task 10 — production firing-order grep guard.

The Stage-6 production gate firing order in ``harness.py`` is:

    propagation_wait
      → blast_radius
        → narrow_replacement (Branch A / Branch C orchestration)
          → applyability
            → slice_gate
              → p0_gate
                → full_eval_acceptance

This test asserts two groups of line-ordering invariants:

Group 1 — The three RCO-4 extracted helpers appear in delegation-
comment order inside the lever loop (all three are at line ~20000+
in the harness, BEFORE ``_run_gate_checks`` is called):

    blast_radius ("RCO-4 Task 6")
      → narrow_replacement ("RCO-4 Task 7")
        → applyability ("RCO-4 Task 9")

Group 2 — Inside ``_run_gate_checks`` (the per-iteration gate chain
that fires before the lever loop's Stage-6 region), the gate_name=
audit strings appear in their sub-ordering:

    propagation_wait → slice_gate → p0_gate → full_eval_acceptance

Note: ``_run_gate_checks`` is *called* at line ~22906 (after the
Stage-6 lever-loop region), but its *body* is defined at ~12732.
Grep-guard tests work on definition order, which is stable. The
actual call-site ordering is enforced by the harness's control flow
and is not easily grep-guarded without running a full lever loop.

The intent is *order preservation within each group*, not global
gate ordering. A future plan (RCO-4b) that decomposes
``_run_gate_checks`` will replace this grep-guard with a real
sequencing test.
"""

from __future__ import annotations

from pathlib import Path


_HARNESS = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "genie_space_optimizer"
    / "optimization"
    / "harness.py"
)

# Group 1 — RCO-4 lever-loop Stage-6 delegation sentinels.
# These appear in the lever-loop body in the correct firing order.
_RCO4_LEVER_LOOP_SENTINELS: tuple[tuple[str, str], ...] = (
    ("blast_radius", "RCO-4 Task 6"),         # blast-radius delegation comment
    ("narrow_replacement", "RCO-4 Task 7"),    # narrow-repl delegation comment
    ("applyability", "RCO-4 Task 9"),          # applyability delegation comment
)

# Group 2 — ``_run_gate_checks`` internal gate_name= sentinels.
# These appear in the function *definition* body in sub-gate order.
_RUN_GATE_CHECKS_SENTINELS: tuple[tuple[str, str], ...] = (
    ("propagation_wait", 'gate_name="propagation_wait"'),
    ("slice_gate", 'gate_name="slice_gate"'),
    ("p0_gate", 'gate_name="p0_gate"'),
    ("full_eval_acceptance", 'gate_name="full_eval_acceptance"'),
)


def _first_line_of(needle: str, text: str) -> int | None:
    for i, line in enumerate(text.splitlines(), start=1):
        if needle in line:
            return i
    return None


def _assert_group_order(
    sentinels: tuple[tuple[str, str], ...],
    text: str,
    group_label: str,
) -> None:
    locations: list[tuple[str, int]] = []
    for name, needle in sentinels:
        line = _first_line_of(needle, text)
        assert line is not None, (
            f"RCO-4 sequencing guard [{group_label}]: sentinel for '{name}' "
            f"({needle!r}) not found in harness.py. If the gate was "
            f"renamed or removed, update sentinels deliberately."
        )
        locations.append((name, line))

    ordered_names = [n for n, _ in sentinels]
    actual_names_by_line_order = [
        name for name, _ in sorted(locations, key=lambda nl: nl[1])
    ]
    assert actual_names_by_line_order == ordered_names, (
        f"RCO-4 sequencing guard [{group_label}]: gate firing order "
        f"drifted. Expected={ordered_names} "
        f"Actual={actual_names_by_line_order}. "
        f"If this drift is intentional, update the sentinel group "
        f"deliberately and add a note in the deferred-gates document."
    )


def test_stage6_rco4_lever_loop_gate_order_is_pinned() -> None:
    """Blast-radius → narrow-replacement → applyability in lever loop."""
    text = _HARNESS.read_text()
    _assert_group_order(_RCO4_LEVER_LOOP_SENTINELS, text, "lever_loop_stage6")


def test_stage6_gate_checks_internal_order_is_pinned() -> None:
    """propagation_wait → slice_gate → p0_gate → full_eval_acceptance
    inside the ``_run_gate_checks`` definition body."""
    text = _HARNESS.read_text()
    _assert_group_order(_RUN_GATE_CHECKS_SENTINELS, text, "_run_gate_checks")
