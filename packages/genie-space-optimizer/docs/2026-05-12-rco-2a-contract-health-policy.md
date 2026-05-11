# RCO-2a Contract Health + Merge Gate Keystone — Policy

## Status

**Phase A (RCO-2a):** in-flight — ships marker, parser, summary, and
merge-gate categories. Production posture remains warn-and-degrade.

**Phase B (RCO-2b):** deferred — flips production posture once the first
trial run emits ``GSO_CONTRACT_HEALTH_V1`` for ≥1 anchor.

## Severity Tiers (authoritative)

The HIGH tier (merge-gate blocking) contains exactly the five canonical
invariant IDs enumerated in the closeout roadmap:

| Tier   | Invariant IDs                  |
|--------|--------------------------------|
| HIGH   | I9, I10, I11, I12, I13         |
| MEDIUM | I1, I2, I3, I4, I5, I6, I7, I8 |

I5 and I12 fire on the same predicate (replay validity); the duplication
is intentional (see ``invariants.py:712-734``) and the HIGH-tier I12 is
the canonical merge-gate surface.

``I_CHECK_FAILED`` (a synthetic violation emitted when an invariant
check raises) is classified MEDIUM in RCO-2a. RCO-2b may promote it.

## Merge Gate Status Vocabulary

| Status                | Trigger                                                      |
|-----------------------|--------------------------------------------------------------|
| ``HEALTHY``           | zero violations + Phase H ``ok`` + bundle ``complete``       |
| ``WARN``              | MEDIUM-tier violations OR Phase H ``skipped`` OR ``incomplete`` |
| ``MERGE_GATE_BLOCKED``| any HIGH-tier violation OR Phase H ``failed`` OR ``assembly_failed`` |

## RCO-2a vs RCO-2b Boundary

RCO-2a wires the categories (the classifier returns the correct enum
value for every input) but does NOT enforce them. The production job
still returns success on ``MERGE_GATE_BLOCKED`` and
``loop_invariants_strict()`` is still pinned to False via
``run_lever_loop.py:_os.environ.setdefault("GSO_LOOP_INVARIANTS_STRICT", "0")``.

RCO-2b will (a) flip the job exit code on ``MERGE_GATE_BLOCKED`` and
(b) decide whether ``loop_invariants_strict()`` should default to True
in production. Both decisions are gated on the named trial blocker in
``2026-05-12-rco-2b-deferral.md``.

## Evidence Sources Consumed

The pure ``build_contract_health_summary`` consumes evidence already
emitted by upstream stages — no new evidence producers ship in RCO-2a.

1. ``invariant_violations`` — output of ``invariants.run_invariants``
   (list of dicts with ``invariant_id`` field).
2. ``phase_h_strict_validation`` — payload of
   ``GSO_PHASE_H_STRICT_VALIDATION_V1`` (``listing_status`` and
   ``validator_status`` fields).
3. ``bundle_assembly_failed`` / ``bundle_assembly_incomplete`` —
   payloads from the matching markers.
4. ``replay_validation`` — ``is_valid`` and ``violation_count`` fields
   (already projected into invariant evidence by
   ``invariant_projection.project_iter_evidence``).
5. ``manifest_paths`` — completeness signal from
   ``check_i6_manifest_paths`` (surfaced via the I6 violation, not
   directly).
