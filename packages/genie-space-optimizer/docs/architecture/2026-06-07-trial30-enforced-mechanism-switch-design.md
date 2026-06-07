# Trial 30 W30.1 + W30.2 — Enforced Inert-Mechanism Switch + Rerouted-QID Carry-Forward

> Design record. Authored 2026-06-07. Status: **DRAFT — pending implementation**.
> Follows `docs/architecture/2026-06-07-trial29-w29-1-inert-patch-reroute-design.md`.
> Tracked in `docs/architecture/lever-loop-iteration-tracker.md::Trial 30`.

## Problem statement

Trial 29 W29.1 added the `kit_forced_inert_reroute` acceptance lane: when a
kit-forced patch applies but yields `behavioral_diff == "unchanged"`, the gate
rejects it and records the `rejected_mechanism`. W29.4 live verification
(airline + 7now) returned **PARTIAL** on both anchors with a single converging
root cause.

| Half | airline | 7now | Verdict |
|---|---|---|---|
| **Detection** — lane fires live, inert mechanism rejected | ✅ 1× (`gs_009`, `top_n_cardinality_collapse`) | ✅ 2× (`gs_013`/`wrong_column`, `gs_026`/`top_n_cardinality_collapse`) | works |
| **Correction** — next iteration picks a *different* structural mechanism | ❌ `gs_009` dropped from iters 3–4 target set | ❌ Stage 3 re-emitted the same `lever-5`/`add_example_sql` | broken |

The hard part — detecting behaviorally-inert kit-forced patches against a live
Genie Space — is proven. Trial 30 converts that detection into an actual
behavior change.

## Root cause (why correction never happened)

The W29.4 postmortem framed the feedback as "advisory." Re-reading the code
shows it was **not even advisory — it was a no-op channel in production**:

| W29.1 artifact | Status in `src/` |
|---|---|
| `harvest_sm_inert_mechanism_history` (`inert_mechanism_history.py:37`) | **Zero callers.** The insufficient-signature sibling is harvested at `harness.py:20712`; the inert one never is. |
| `render_inert_mechanism_history_section` (`synthesize.py:3850`) | **Never called.** Defined + unit-tested only. |
| `TransformerContext.inert_mechanism_history` (`verdict.py:84`) | Field exists; `optimizer.py` **never sets it**. |
| `Stage2BatchInput.inert_mechanism_history` (`cluster_batch.py:52`) | Field exists; `build_stage2_batch_input` **omits it**. |

So in W29.4 the LLM received **nothing** about the rejected mechanism, and it
re-picked it. Separately, the rerouted QID could vanish:

`target_qids_union` (`synthesize.py:2271`) unions only `p.target_qids` on the
synthesized branch. A cluster member the LLM omits from `target_qids`
disappears from the marker even though `cluster.member_qids` still holds it —
this is the airline `gs_009` drop.

## Where the existing code already takes us

`_structural_fix_mechanisms(rca_kind)` already exists
(`rca_mechanism_routing.py:167`) and returns the non-prose `PatchMechanism`
set for an RCA. Both blocked RCA kinds have a structurally-distinct alternative
to `add_example_sql` / `EXAMPLE_SQL`:

| RCA kind | `_structural_fix_mechanisms` | Distinct from `EXAMPLE_SQL`? |
|---|---|---|
| `top_n_cardinality_collapse` | `{SQL_SNIPPET, METADATA_DESCRIPTION}` | Yes — `EXAMPLE_SQL` is not in the set at all |
| `wrong_column` | `{METADATA_DESCRIPTION}` | Yes — kit has no `lever-5`/`5b` |

The post-LLM binding block (`synthesize.py` ~2382) already hosts the W4
(`rca_mechanism_default_reason`), B1 (`rca_instruction_default_reason`), and D3
(sole-lever-in-rejected-family) drop guards — the natural home for an
inert-reroute sibling. The `kept_insufficient` carry-forward path
(harvest → cross-iter accumulator → ctx → prompt; plus `_TERMINATIONS_REQUIRING_PIVOT`
and the same-iteration `_live_insufficient_repair_signatures` bucket) is the
exact mirror W30.2 follows.

## Design

### Component 1 — W30.1a: wire the W29.1 feedback channel

Mirror the `kept_insufficient` harvest, one-for-one:

1. **`harness.py` ~20712** — sibling harvest block next to
   `harvest_sm_insufficient_repair_signatures`: call
   `harvest_sm_inert_mechanism_history(_sm_final_states, qid_rca_pairs=…)` and
   `extend_sm_inert_mechanism_history(...)` into a cross-iteration accumulator
   `_sm_inert_mechanism_history`, in the same `try/except → logger.debug` wrap
   (channel-skip is non-fatal).
2. **`optimizer.py` ~239–319** — pass the accumulator into
   `TransformerContext(inert_mechanism_history=…)`.
3. **`synthesize_llm.py` ~541** + **`run_plan11_synthesis_for_single_cluster`
   (`synthesize.py:852`)** — new kwarg `inert_mechanism_history`, threaded to
   `_build_request`.
4. **`_build_request` (`synthesize.py:237–717`)** — call
   `render_inert_mechanism_history_section(history)` and append next to the
   insufficient-signatures section (~661–681).

**Gate:** `GSO_TRIAL30_INERT_HARVEST_WIRE`.

### Component 2 — W30.1b: deterministic enforcement guard (post-loop, Approach B)

In the post-loop binding block (`synthesize.py` ~2382), after proposals are
built into `RepairProposal` objects (full per-QID slate in hand):

1. Normalize every proposal's mechanism to `PatchMechanism` via
   `mechanism_for_patch_type(patch_type.value)`.
2. Normalize each `rejected_mechanism` lever-id in history to `PatchMechanism`
   (lever → patch_type → mechanism). Comparison vocabulary is **`PatchMechanism`
   enum** (the behavioral unit), not lever-id strings — robust to
   lever-5/5a/5b aliasing.
3. Per `(qid, rca_kind)`: `rejected = {normalized rejected}`;
   `available_fallbacks = _structural_fix_mechanisms(rca) - rejected`.
4. **Drop** a proposal whose mechanism ∈ `rejected` **iff** the surviving slate
   for that QID still contains ≥1 proposal with mechanism ∈
   `available_fallbacks`. Emit `GSO_TRIAL30_ENFORCED_SWITCH_V1`
   (qid, rca_kind, rejected_mechanism, chosen_fallback).
5. If no fallback exists in the slate (or the RCA has no remaining structural
   alternative) → **keep** the proposal and emit
   `GSO_TRIAL30_NO_FALLBACK_AVAILABLE_V1`. The guard NEVER zeroes out a QID.

**Gate:** `GSO_TRIAL30_ENFORCE_GUARD` (independently rollback-able from wiring).

### Component 3 — W30.2: rerouted-QID carry-forward (3 RCA-backed fixes)

- **(a)** `target_qids_union` (`synthesize.py:2271`) — union `cluster.member_qids`
  into the **synthesized** branch so rerouted QIDs can't be dropped from the
  marker.
- **(b)** `_TERMINATIONS_REQUIRING_PIVOT` (`action_groups.py:1007`) — add
  `"kit_forced_inert_reroute"` so the next iteration is forced onto a different
  patch family.
- **(c)** `acceptance_gate.py` kit_forced block (~299–310) — write
  `_live_insufficient_repair_signatures` / `_p2_5_terminal_signature_kit_inputs`
  onto `ctx.extras` (mirror the `kept_insufficient` block at 411–461) for
  same-iteration sibling-cluster visibility.

**Gate:** (a)/(c) under `GSO_TRIAL30_INERT_HARVEST_WIRE`; (b) under the master
`GSO_TRIAL30_ENFORCED_SWITCH`.

### Component 4 — flags (`trial30_flags.py`, mirrors `trial29_flags.py`)

- Master `GSO_TRIAL30_ENFORCED_SWITCH`.
- Sub-flags `GSO_TRIAL30_INERT_HARVEST_WIRE`, `GSO_TRIAL30_ENFORCE_GUARD`.
- Sub-flags AND with the master (same pattern as `trial29_inert_reroute_enabled()`).

## Error handling & invariants

- Every new path emits a typed bail-out marker — no silent drops.
- The guard never zeroes out a QID (fallback-required hard-drop).
- Harvest wrapped in `try/except → logger.debug`, matching the insufficient
  sibling (cumulative-learning channel-skip is non-fatal).
- W30 partially closes `bundle_completeness_invariants_held` (channel now
  wired). Full live persistence (diagnostic JSONL + decisions-table projection)
  is **out of scope** — deferred to W30.3, tracked separately.

## Out of scope (explicit)

- The known `kept_insufficient_count = -1` sentinel and the
  `outcome.py` insufficient-gain / `iteration_terminal` `KEPT_INSUFFICIENT`
  classifiers — not implicated in the W29.4 drop.
- W30.3 live persistence (`persist_inert_patch_diagnostic` wiring +
  `genie_eval_lever_loop_decisions` projection of the new decision literal).

## Testing

**W30.1a (wiring):**
- `harvest_sm_inert_mechanism_history` produces history from
  `kit_forced_inert_reroute` final states.
- `_build_request` renders the inert-history section when history is non-empty;
  omits it when empty.
- ctx / batch-input threading round-trips.

**W30.1b (guard):**
- re-emit + fallback-exists → drop + `GSO_TRIAL30_ENFORCED_SWITCH_V1`.
- re-emit + no fallback → keep + `GSO_TRIAL30_NO_FALLBACK_AVAILABLE_V1`.
- lever-id aliasing (lever-5 vs lever-5a, same `EXAMPLE_SQL` mechanism) caught
  via enum normalization.
- novel mechanism → untouched.

**W30.2 (carry-forward):**
- rerouted QID present in `target_qids_union` even when the LLM omits it from
  `target_qids`.
- `"kit_forced_inert_reroute" ∈ _TERMINATIONS_REQUIRING_PIVOT`.
- same-iteration sibling cluster sees the live bucket signature.

**Offline fixture:** 7now (`gs_026`/`top_n_cardinality_collapse`,
`gs_013`/`wrong_column`) — clean reproduction with no QID-drop confound — is the
integration replay anchor.

## Acceptance criteria (W30 live re-verification, W30.5)

1. ≥1 `GSO_TRIAL30_ENFORCED_SWITCH_V1` fires live on either anchor.
2. ≥1 accepted patch with `behavioral_diff != "unchanged"` on a QID that
   previously went through `kit_forced_inert_reroute`.
3. No rerouted QID dropped from the next iteration's `target_qids_union`.
4. Accuracy gain-or-hold on both anchors (no regression).
