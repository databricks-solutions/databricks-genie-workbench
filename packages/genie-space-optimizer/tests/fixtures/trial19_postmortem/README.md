# Trial 19 Postmortem Tape Fixtures

Synthetic tape fixtures derived from the airline (634185464201993) and 7now
(953593238005228) lever-loop postmortems that motivated Trial 19. Each
fixture exercises the four Trial 19 enforcement signals:

- **C1/C3** — at least one baseline row where `arbiter == "both_correct"` and
  the raw byte-match `result_correctness` is "no" (the GT-disagrees
  arbiter-correct case Trial 19 routes to `pending_review`).
- **A1** — a `kept_insufficient` outcome with a populated
  `insufficient_repair_signature`. The admission gate must reject a
  same-signature sole-primary re-attempt in the next iteration.
- **B4** — at least one structural-gate "absent" emission against a
  non-empty `intended_patch_shape` (Trial 19 routes to
  `retry_with_typed_feedback`).
- **G5** — final iteration must not terminate on
  `ag_collision_with_forbidden_set` once the regenerator wrapper is wired
  in (it should produce either a different AG or
  `fallback_no_new_strategy`).

The integration test
`tests/integration/test_trial19_postmortem_replay.py` replays each fixture
through the Trial 19 primitives (admission_gate, ground_truth_corrections,
rca_card_builder.dominant_root_cause_label, structural_repair_gate) and
asserts the named success criteria.

## Files

- `airline_634185464201993.json` — airline fixture
- `7now_953593238005228.json` — 7now fixture
