# RCO-2b — Contract Health Merge Gate Production Posture Flip (Deferred)

## Status

**✅ Landed (May 13, 2026).** The named blocker cleared on the May-12
consolidating trial (two captured ``GSO_CONTRACT_HEALTH_V1`` payloads;
see ``runid_analysis/31ecd96f-…`` and ``runid_analysis/ccf1d60d-…``).
The production posture flip shipped in
``2026-05-13-rco-2b-merge-gate-enforcement-and-strict-mode-flip-plan.md``.
The structural foundation landed in RCO-2a (see
``2026-05-12-rco-2a-contract-health-marker-and-summary-plan.md``).

## Named Blocker (entry criterion)

> **First trial run that emits ``GSO_CONTRACT_HEALTH_V1`` for ≥1
> anchor, with the marker payload showing the expected
> ``merge_gate_status`` for that anchor's known failure mode.**

Concretely, this means: a lever-loop run lands in MLflow whose stdout
contains a single ``GSO_CONTRACT_HEALTH_V1`` line, the
``marker_parser`` round-trips it cleanly, and the
``merge_gate_status`` matches the anchor's pre-classified expectation
(e.g. the F9 anchor at
``runid_analysis/3b050ec5-4032-457f-a785-2d1a3942a097`` should yield
``merge_gate_blocked`` driven by I12).

## RCO-2b Scope

When the blocker clears, RCO-2b will:

1. **Promote the trial marker to a fixture.** Drop the captured
   payload into ``tests/unit/fixtures/rco2b/trial_anchor/`` and assert
   the harness reproduces it byte-for-byte when replaying the anchor.
2. **Flip the lever-loop task exit code.** In
   ``src/genie_space_optimizer/jobs/run_lever_loop.py``, the task
   wrapper consults ``MarkerLog.contract_health.merge_gate_status``
   and exits non-zero when the value is ``merge_gate_blocked``.
3. **Decide ``loop_invariants_strict()`` default.** Same trial run
   provides the evidence needed to decide whether to remove
   ``_os.environ.setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")``
   from ``run_lever_loop.py``. This is a separate decision from
   the merge-gate posture; the evidence overlaps but the rationale
   differs (strict mode raises in-loop; the merge gate fires at
   end-of-run).
4. **Update the structural guard.** The three assertions in
   ``tests/unit/test_rco2a_strict_mode_posture_guard.py`` invert:
   the file becomes ``test_rco2b_strict_mode_posture_flipped.py``
   and asserts the new posture.

## Out of Scope for RCO-2b

- New evidence producers. RCO-2b consumes only what RCO-2a already
  surfaces.
- New invariants. The HIGH-tier set (I9–I13) is fixed by RCO-2a's
  policy doc; new invariants are introduced in their own RCO.
- Anything related to RCO-3 (pilot-gated default-flip closeout),
  RCO-6 (replay/production parity), or RCO-9 (final audit). RCO-2b
  is single-purpose: production posture flip.

## Evidence Sources

The same evidence inputs RCO-2a documents (see
``2026-05-12-rco-2a-contract-health-policy.md``). RCO-2b does not
introduce new ones. However: in RCO-2a, several of these inputs
degrade to ``None`` / ``()`` at the end-of-run emission point because
the relevant evidence locals aren't accumulated into a single
end-of-run-scoped variable. RCO-2b will plumb those accumulators
through ``_phase_b_accounting`` (or an equivalent named container) so
the marker payload reflects real signal rather than perpetually
landing in ``warn`` / ``healthy``.

## Anchor Inventory for Trial

These existing anchors are candidates for the first trial run; one
green-pass + one expected-block is the minimum target:

| Anchor                                  | Expected ``merge_gate_status`` | Driving Evidence    |
|-----------------------------------------|--------------------------------|---------------------|
| ``runid_analysis/3b050ec5-...`` (F9)    | ``merge_gate_blocked``         | I12 (25 illegal trunk transitions) |
| (any green production run, post-C17)    | ``healthy``                    | clean evidence     |

A single trial submission covering both shapes is sufficient to
clear the blocker.

## Disposition (2026-05-13)

The named blocker cleared on the May-12 consolidating trial. Two
captured ``GSO_CONTRACT_HEALTH_V1`` payloads validated the marker
emission pipeline end-to-end:

| Captured anchor                                         | ``merge_gate_status`` | Driving evidence                                          |
|---------------------------------------------------------|------------------------|-----------------------------------------------------------|
| ``31ecd96f-5d56-4b5a-af8e-38e9e5c549af`` (airline)      | ``warn``               | ``phase_h_listing_status=skipped``, ``phase_h_validator_status=skipped`` |
| ``ccf1d60d-d686-467b-bafa-1640131b4393`` (7now)         | ``warn``               | ``phase_h_listing_status=skipped``, ``phase_h_validator_status=skipped`` |

The trial did NOT capture a ``MERGE_GATE_BLOCKED`` payload (the
F9-3b050ec5 blocked anchor was deferred to a future re-trial gated on
the two defect plans, ``2026-05-12-defect-ag-emit-blocks-ungrounded-rca.md``
and ``2026-05-12-defect-forbidden-ag-admission-enforcement.md``). The
RCO-2a ``blocked_anchor`` fixture (``tests/unit/fixtures/rco2a/blocked_anchor/``)
provides the unit-fixture coverage of the blocked path.

Both captured payloads were promoted to byte-stable parity fixtures
under ``tests/unit/fixtures/rco2b/trial_airline_31ecd96f/`` and
``tests/unit/fixtures/rco2b/trial_seven_now_ccf1d60d/``, asserted by
``tests/unit/test_rco2b_trial_anchor_parity.py``.
