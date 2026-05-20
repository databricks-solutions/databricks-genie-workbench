# Plan 9 — Catalog Removal & LLM-Driven Structural Realization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

---

## Background — Why Plan 9 exists

Plans 1–7 added typed contracts and LLM-driven producers (`PerQidRcaEvidence`, `LlmCluster`, `RepairIntent`, `RepairProposal`, `CritiqueVerdict`, `NextAttemptHypothesis`). Plan 8 added intermediate wire-ins and stamped typed sidecars onto `metadata_snapshot`. The two most recent lever-loop runs (airline `59a173d3`, 7now `ab65fefe`) on the post-Plan-8 wheel produced **0.0pp accuracy delta on both spaces**. Postmortems show the LLM produced grounded RCA cards with correct root causes, but the generated patches were either blocked, decorative, or empty.

Independent validation of two reviewer analyses against the codebase confirmed three structural problems that Plans 1–8 did not solve:

1. **Wire-in gap.** `generate_proposals_from_strategy` (`optimizer.py:15958-15972`) does not accept Plan-5 typed inputs; both harness call sites (`harness.py:23302-23315`, `harness.py:23410-23427`) omit them; `_select_lever_5_holistic_path` (`optimizer.py:17539-17545`) is called without `rca_evidence_typed=` / `llm_cluster_by_cluster_id=` / `ag_id=` / `iteration=`. The Plan-5 LLM intent synthesizer in `_dispatch_lever_5b_for_cluster` (`optimizer.py:10422`) and the Plan-5 L6 intent dispatch in `_generate_lever6_proposal` (`optimizer.py:14122`) are reachable in code but never reached in production. The same `_force_lever6_proposal_for_ag` (`harness.py:23923-23944`) only forwards `w, spark, catalog, gold_schema, warehouse_id, benchmarks` — Plan 5's typed inputs are not in the kwargs.

2. **Materialization decoration.** Even on the rare paths where Plan 5 fires, `lever6_intent_dispatch.py:112-125` calls `_generate_lever6_proposal_legacy_body` to do the actual SQL realization and merely stamps the LLM-emitted `RepairIntent` on the legacy generator's output. `RepairProposal.patch_body` (a fully materializable dict, with a `to_proposal_dict()` projector at `repair_proposal_typed.py:107`) is discarded. The LLM's repair is metadata; the SQL is whatever the legacy catalog-driven generator produced.

3. **Catalog-as-gatekeeper.** `pick_archetype` (`archetypes.py:463`) is doing three load-bearing jobs that gate every structural repair: (a) **slice picking** — `_derive_asset_slice_from_afs` (`cluster_driven_synthesis.py:655`) calls `pick_archetype()` as its first action, returning `None` (no slice) on miss; (b) **prompt template selection** — `archetype.prompt_template` is interpolated into the L5b/L6 generator prompt at `synthesis.py:336`, `preflight_synthesis.py:1915`, `repair_intent.py:298`; (c) **output-shape validation** — `synthesis.py:670-680` reads `requires = archetype.output_shape.get("requires_constructs")` and rejects SQL that does not contain those constructs. Plan 5 only displaced "which lever to pick" — not these three. So even when Plan 5 produces a typed `RepairProposal`, the catalog still owns realization through these three back-channels.

The 21-shape catalog has no entry for `top_n_cardinality_repair`, `defensive_filter_removal`, or `grain_pivot_day_vs_mtd` — the three structural patterns that postmortems show are the actual bottlenecks. Even if Plan 5's LLM synthesis emits a perfect intent for one of these, `pick_archetype` returns `None`, `_derive_asset_slice_from_afs` returns `None`, and the cluster is declined with `NO_ARCHETYPE_OR_SLICE` or `NO_TOP_N_ARCHETYPE`.

**Plan 9 inverts the three jobs.** Slice picking moves from `pick_archetype` to LLM-emitted `RepairProposal.target_objects`. Prompt template selection moves from `archetype.prompt_template` to `RepairShape`-keyed prompt fragments with one free-form `OTHER` fragment as the ultimate safety net. Output-shape validation moves from `archetype.output_shape` to LLM-emitted `RepairProposal.required_constructs` with the deterministic validator reading its contract from the LLM output. Once all three jobs are inverted, `pick_archetype`, `_derive_asset_slice_from_afs`, and `ARCHETYPES` are dead code and get deleted. The catalog goes away; the deterministic safety net (SQL execution, leakage firewall, schema invariants) stays.

Plan 9 also closes the wire-in gap (so Plan 5 actually fires on the live path), the materialization decoration (so `RepairProposal.patch_body` reaches the applier via `to_proposal_dict()` instead of being thrown away), the structural-gate fail-open (so degenerate patches with `emitted_patch_shape=ABSENT` and `repairability=0.0` are rejected regardless of the `intended_patch_shape` string), and adds per-anchor `PLAN5_ANCHOR_ACTIVATION_V1` markers so postmortem can answer "did Plan 5 fire on this anchor and what did it produce?" with zero ambiguity. Plan 7's rollback-learning helper flips to default-on so iteration-N's typed rolled-back hypotheses ground iteration-N+1's intent synthesis.

This is the last plan. After Plan 9, every anchor records exactly one typed activation outcome; the LLM owns slice, prompt, and validator contracts; deterministic helpers are validators (not gatekeepers); and the only remaining open vocabulary is `RepairShape.OTHER` — kept on purpose as the escape hatch the catalog removal depends on.

---

## v1 Pre-Execution Notes (READ FIRST)

Plan 9 v1 was written from a verified snapshot of `harness.py`, `optimizer.py`, `cluster_driven_synthesis.py`, `archetypes.py`, `synthesis.py`, `lever6_intent_dispatch.py`, `repair_intent.py`, `repair_proposal_typed.py`, `structural_repair_gate.py`, and `config.py` taken on 2026-05-19. Anchors below cite line numbers from that snapshot; verify with `rg` before each `StrReplace` and surface mismatches instead of guessing.

**Two PRs:**

- **PR1 — Streams A + B + C (T1–T9).** Lands the three-job inversion (T1–T3), the wire-in (T4–T6), the gate hardening + activation telemetry + forbidden-set timing (T7–T9). Adds a `GSO_PLAN9_LLM_DIRECT_MATERIALIZATION` flag (default ON) so emergency rollback is one env var. Catalog stays present but is bypassed on every LLM-materialization path.
- **PR2 — Streams D + E (T10–T12).** Lands AFTER one validation deploy of PR1 shows healthy activation markers (≥80% of anchors in `plan5_intent_materialized` status; zero `no_archetype_or_slice` markers; zero `lever6_force_llm_declined` markers replaced by typed decline reasons). Deletes `pick_archetype`, `_derive_asset_slice_from_afs`, `ARCHETYPES`. Flips Plan 7 default-on. Updates `roadmap.md`.

**Critical: never split a task across PRs.** T1–T9 land together in PR1; T10–T12 land together in PR2. The cutover flag from PR1 makes PR2 deletable as a single commit because every caller already routes through the LLM-direct path.

**Locals confirmed in scope at each anchor:**
- `harness.py:23302–23315` and `harness.py:23410–23427` (Best-of-N + single-shot proposal generation call sites): `strategy` (dict), `ag` (dict), `ag_id` (str), `metadata_snapshot`, `target_lever` / `lever_int` (int), `apply_mode` (str), `w` (`WorkspaceClient`), `spark`, `catalog`, `schema`, `benchmarks`, `_doa_fingerprint_buffer`, `iteration_counter` (int), `run_id` (str), and (since Plan 8 Task 1 stamping) `metadata_snapshot["_rca_evidence_typed"]` (dict keyed by qid) and `metadata_snapshot["_llm_clusters_by_cluster_id"]` (dict keyed by cluster_id). The typed inputs Plan 9 threads come from these two `metadata_snapshot` keys, regrouped per AG.
- `harness.py:23923–23944` (`_force_l6_call_for_this_ag`): same locals plus `_force_cluster` (dict), `_force_target_qids` (tuple).
- `optimizer.py:17539–17545` (`_select_lever_5_holistic_path` call site): inside `generate_proposals_from_strategy`. `clusters`, `all_lever5_clusters`, `metadata_snapshot`, `lever_changes`, `w`, `benchmarks`. After Plan 9 Task 4, also `rca_evidence_typed`, `llm_cluster_by_cluster_id`, `ag_id`, `iteration` (passed from the harness via T4's signature expansion).
- `lever6_intent_dispatch.py:51–139` (`dispatch_lever_6_with_intent`): `cluster`, `metadata_snapshot`, `w`, `rca_evidence_typed`, `llm_cluster`, `ag_id`, `iteration`, plus pass-through kwargs.
- `cluster_driven_synthesis.py:637-720` (`_derive_asset_slice_from_afs`): `afs` (dict), `metadata_snapshot`. After T6, also `repair_proposal` (optional typed `RepairProposal` from upstream Plan 5 synthesis).

**Existing typed contracts Plan 9 builds on (no new wire-stable types invented in Plan 9 except `TargetObject`):**
- `RepairIntent` (`repair_intent.py:133-224`): gains `target_objects` field in T1.
- `RepairProposal` (`repair_proposal_typed.py:70-217`): gains `target_objects` and `required_constructs` fields in T1+T3. `to_proposal_dict()` projector at line 107 stays unchanged — T6 calls it directly.
- `RepairShape` (`repair_intent.py:37-58`): unchanged; `OTHER` is the escape hatch that makes T2 + T10 safe.
- `PatchType` (`repair_intent.py:61-105`): unchanged.
- `LlmRepairProposalOutput` (Pydantic, in `repair_intent_synthesizer.py`): gains two new fields in T1+T3 with strict-mode JSON Schema additions.
- `EmittedPatchShape` (`terminal_signature.py`): unchanged; T7 reads it to enforce the new structural-gate rule.
- `RepairabilityScore` (`repairability_score.py`): unchanged; T7 reads `score.repairability_score == 0.0` to enforce the new rule.

---

**Goal:** Invert the three load-bearing jobs of `pick_archetype` (slice + prompt + validator) into LLM-emitted typed contracts so Plan 5's `RepairProposal` reaches the applier directly, then retire the catalog. Close the harness wire-in gap, harden the structural gate, default-on Plan 7, and add per-anchor activation telemetry so every anchor records exactly one typed Plan-5 outcome.

**Architecture:** Plan 9 is **structural-realization inversion + wire-in completion + catalog retirement**. Three streams converge on a deployable system:

- **Stream A — Promote three load-bearing fields to typed contracts (T1–T3).** Slice picking moves from `pick_archetype` to LLM-emitted `RepairProposal.target_objects: tuple[TargetObject, ...]`. Prompt template selection moves from `archetype.prompt_template` to `RepairShape`-keyed prompt fragments in a new `prompts/_repair_shape_fragments.py` module with a single free-form `OTHER` fragment as the ultimate safety net. Output-shape validation moves from `archetype.output_shape` to LLM-emitted `RepairProposal.required_constructs: tuple[str, ...]` with the deterministic 5-gate validator at `synthesis.py:670-680` reading its `requires` list from the LLM output instead of the catalog.

- **Stream B — Wire LLM typed inputs into the live harness (T4–T6).** Thread `rca_evidence_typed`, `llm_cluster_by_cluster_id`, `ag_id`, `iteration` through `generate_proposals_from_strategy(...)` and both harness call sites (Best-of-N loop + single-shot) and forward into `_select_lever_5_holistic_path(...)` (T4). Add the same four kwargs to `_force_lever6_proposal_for_ag(...)` and pass them through `**lever6_kwargs` to `_generate_lever6_proposal(...)` so the Plan-5 short-circuit at `optimizer.py:14122` actually receives them (T5). Replace the legacy-body materialization inside `dispatch_lever_6_with_intent` and `_dispatch_lever_5b_for_cluster` with direct `RepairProposal.to_proposal_dict()` materialization; the legacy generator stops being a second LLM call and is downgraded to validator-only (T6).

- **Stream C — Tighten gates + activation telemetry + forbidden-set timing (T7–T9).** Harden `enforce_structural_repair_shape` to reject `emitted_patch_shape == ABSENT` AND `repairability == 0.0` regardless of `intended_patch_shape` string (T7). Emit `PLAN5_ANCHOR_ACTIVATION_V1` marker for every anchor with one of `{plan5_intent_invoked, plan5_intent_declined, plan5_intent_validator_rejected, plan5_intent_routed, plan5_intent_materialized}` (T8). Move forbidden-set filtering ahead of proposal generation so the harness stops wasting iterations on retired signatures (T9).

- **Stream D — Retire the catalog + default-on Plan 7 (T10–T11).** Delete `pick_archetype`, `_derive_asset_slice_from_afs`, the `ARCHETYPES` list, and the `Archetype` class once T1–T3 are in. Keep `_ARCHETYPE_NAME_TO_SHAPE` as deprecated compatibility mapping for postmortem readers of pre-Plan-9 traces. Flip `plan7_rollback_learning_enabled()` to default-on (T11).

- **Stream E — Ratification (T12).** Close all 14 roadmap open questions; mark Plans 1–9 as historical; add a "Post-Plan-9 telemetry checklist" listing the markers operators monitor for the first 5 deploys.

The unifying invariant: **every anchor in the next lever-loop run records exactly one `PLAN5_ANCHOR_ACTIVATION_V1` marker with a typed status; if a candidate fails to materialize, the postmortem reads exactly why from the typed decline reason**. The acceptance bar is observability, not headline accuracy gain — Plan 9 promises the system stops being silently inert. Accuracy follow-through comes from the typed signal Plan 9 unlocks for the next plan (or telemetry-driven prompt tuning, which no longer needs a plan).

**Tech Stack:** Python 3.11+; Plan 1's `RepairIntent` + `RepairShape` + `PatchType` + `IntentOutcome` + `stamp_repair_intent_on_proposal` + `extract_repair_intent_from_proposal` (`optimization/repair_intent.py`); Plan 5's `RepairProposal` + `to_proposal_dict()` + `from_llm_output()` + `to_repair_intent()` + `synthesize_repair_intent_for_cluster` + `LlmRepairProposalOutput` Pydantic schema (`optimization/repair_proposal_typed.py`, `optimization/repair_intent_synthesizer.py`); Plan 5's `_dispatch_lever_5b_for_cluster` short-circuit (`optimization/optimizer.py:10369-10567`); Plan 8 Task 1's `metadata_snapshot["_rca_evidence_typed"]` and `metadata_snapshot["_llm_clusters_by_cluster_id"]` stamping (`optimization/harness.py:16365-16373`); Plan 8 Task 3's `dispatch_lever_6_with_intent` (`optimization/lever6_intent_dispatch.py`); `EmittedPatchShape` + `TerminalSignature` (`optimization/terminal_signature.py`); `RepairabilityScore` + `compute_repairability` (`optimization/repairability_score.py`); `enforce_structural_repair_shape` (`optimization/structural_repair_gate.py`); `marker_line` + `print_marker` (`optimization/run_analysis_contract.py`); `_flag_default_on` (`common/config.py`); pydantic v2 strict JSON Schema mode (existing in `LlmRepairProposalOutput`); pytest 8.x.

**Out of scope (deliberately deferred to post-Plan-9 telemetry):**

- **Closing `RepairShape.OTHER` to a strict enum.** The `OTHER` escape hatch is what makes T10's catalog deletion safe — the free-form structural rewrite fragment fires on `RepairShape.OTHER`. Closing it requires 5+ deploys of telemetry showing `OTHER` usage drops below 5% of all materialized proposals. Re-evaluate post-deployment.
- **Cross-iteration hypothesis ledger.** Plan 7's `FailureCluster.hypothesis_history` (added by Plan 8 Task 8) is per-cluster, in-process only. Persisting hypotheses to UC for cross-run mining is a future plan.
- **AG decomposition lever-routing fix** (`control_plane.py:751`). The deterministic `recommended_levers` downgrade L6→L5 that earlier postmortems flagged becomes advisory after T4-T6 because the LLM owns lever selection via `RepairProposal.patch_type` (the harness already prefers `patch_type` routing when both are present). Re-evaluate post-deployment; if telemetry shows the deterministic downgrade still wins ties, a 1-task follow-up patches `control_plane.py:751`.
- **Persisting `target_objects` slices to UC** for cross-AG slice reuse. Plan 9's `target_objects` is per-intent, in-process.
- **Closing `LlmCluster.semantic_theme` and `NextAttemptHypothesis.failure_mode` open vocabularies.** Deferred from Plan 8 for the same telemetry-first reason; Plan 9 does not change the deferral.
- **Replacing the deterministic SQL execution / leakage firewall / schema invariant validators with LLM-driven ones.** These are the safety net that makes Plan 9's catalog removal safe; replacing them is explicitly a non-goal.
- **Inlining the heuristic body of `_generate_lever6_proposal_legacy_body`** (turning the legacy generator into a thin validator wrapper). After T6, the legacy body is invoked only as the validator layer when `RepairProposal.patch_body` materialization fails — its SQL/leakage/schema checks are reused. Refactoring it into a cleaner validator module is a follow-up.

---

## File Structure

**New source files (4):**

```
packages/genie-space-optimizer/src/genie_space_optimizer/optimization/
├── target_object_typed.py                # NEW — TargetObject frozen dataclass
│                                          # (asset_kind, identifier, columns).
│                                          # Used by RepairProposal.target_objects
│                                          # and RepairIntent.target_objects.
│                                          # Pure data + JsonRoundTrip; no logic.
├── plan9_activation_markers.py           # NEW — PLAN5_ANCHOR_ACTIVATION_V1
│                                          # marker emitter + ActivationStatus
│                                          # StrEnum + emit_plan5_activation()
│                                          # helper that produces a single
│                                          # marker_line per anchor.
├── llm_direct_slice_resolver.py          # NEW — resolves RepairProposal
│                                          # .target_objects → AssetSlice
│                                          # (replaces pick_archetype's slice
│                                          # job from _derive_asset_slice_from_afs).
│                                          # Pure function; reads
│                                          # metadata_snapshot for table/column
│                                          # lookup; no LLM call.
└── prompts/
    └── _repair_shape_fragments.py        # NEW — RepairShape-keyed prompt
                                           # fragment registry. Replaces
                                           # archetype.prompt_template
                                           # interpolation. One fragment per
                                           # RepairShape enum member, plus a
                                           # free-form OTHER fragment that is
                                           # the ultimate safety net after
                                           # catalog removal.
```

**Modified source files (10):**

```
packages/genie-space-optimizer/src/genie_space_optimizer/optimization/
├── repair_intent.py                       # T1 — add target_objects field to
│                                          # RepairIntent dataclass + from_json
│                                          # roundtrip.
├── repair_proposal_typed.py               # T1, T3 — add target_objects and
│                                          # required_constructs fields to
│                                          # RepairProposal + from_llm_output
│                                          # bridge + to_proposal_dict
│                                          # projector.
├── repair_intent_synthesizer.py           # T1, T3 — extend
│                                          # LlmRepairProposalOutput Pydantic
│                                          # schema with target_objects and
│                                          # required_constructs; both fields
│                                          # are LLM-emitted. Wire through
│                                          # synthesize_repair_intent_for_cluster
│                                          # validators.
├── synthesis.py                           # T2, T3, T10 — replace
│                                          # archetype.prompt_template interp
│                                          # (line 336) with
│                                          # repair_shape_fragments.fragment_for
│                                          # (T2); replace archetype.output_shape
│                                          # validator (line 670-680) with
│                                          # proposal.required_constructs
│                                          # validator (T3); delete archetype
│                                          # parameter from synthesize_example_sqls
│                                          # signature (T10).
├── preflight_synthesis.py                 # T2, T3, T10 — same three changes
│                                          # for the preflight path.
├── cluster_driven_synthesis.py            # T6, T10 — replace
│                                          # _derive_asset_slice_from_afs
│                                          # archetype dependency with
│                                          # llm_direct_slice_resolver
│                                          # (T6); delete archetype-derived
│                                          # branches once T10 retires the
│                                          # catalog. Emit new typed decline
│                                          # codes (T8).
├── lever6_intent_dispatch.py              # T6 — replace
│                                          # _generate_lever6_proposal_legacy
│                                          # SQL realization (line 112-125)
│                                          # with proposal.to_proposal_dict()
│                                          # + deterministic validator pass.
│                                          # Legacy generator becomes
│                                          # validator-only.
├── optimizer.py                           # T4 — add Plan-5 typed kwargs to
│                                          # generate_proposals_from_strategy
│                                          # signature + _select_lever_5_holistic_path
│                                          # call site (line 17539-17545).
│                                          # T5 — same for _generate_lever6_proposal
│                                          # signature already has them (Plan 8
│                                          # Task 3); just verify thread-through.
├── harness.py                             # T4, T5 — pass typed kwargs at both
│                                          # generate_proposals_from_strategy
│                                          # call sites and at the
│                                          # _force_l6_call_for_this_ag closure.
│                                          # T8 — emit PLAN5_ANCHOR_ACTIVATION_V1
│                                          # marker at 8 sites in the per-AG
│                                          # loop. T9 — move forbidden-set
│                                          # filtering ahead of proposal
│                                          # generation.
├── structural_repair_gate.py              # T7 — reject when emitted_patch_shape
│                                          # == ABSENT AND repairability == 0.0
│                                          # regardless of intent string.
└── archetypes.py                          # T10 — DELETED (catalog retirement).
                                            # _ARCHETYPE_NAME_TO_SHAPE moves to
                                            # repair_intent.py with a "legacy
                                            # compatibility for pre-Plan-9
                                            # postmortem readers" docstring.
```

**Modified config + flag files (1):**

```
packages/genie-space-optimizer/src/genie_space_optimizer/common/
└── config.py                              # T11 — flip plan7_rollback_learning_enabled
                                            # to _flag_default_on; add
                                            # plan9_llm_direct_materialization_enabled
                                            # (default ON; cutover-flag removed in
                                            # PR2 with archetype deletion).
```

**Modified skill prompts (3):**

```
packages/genie-space-optimizer/src/genie_space_optimizer/skills/
├── repair-intent-synthesis/SKILL.md       # T1, T3 — add <target_objects> and
│                                          # <required_constructs> sections to
│                                          # the system prompt; document the
│                                          # patch_body × target_objects ×
│                                          # required_constructs contract.
├── lever-5b-example-sql/SKILL.md          # T2 — add note that prompt fragments
│                                          # come from repair_shape (LLM picks)
│                                          # not from archetype lookup.
└── lever-6-sql-expression/SKILL.md        # T2 — same note for L6.
```

**Modified roadmap + plan docs (2):**

```
packages/genie-space-optimizer/docs/llmdrivenarchitecture/
├── roadmap.md                              # T12 — close all 14 open questions;
│                                          # mark Plans 1-9 historical; add
│                                          # "Post-Plan-9 telemetry checklist".
└── 2026-05-19-plan-8-deployment-readiness-and-cleanup.md
                                            # T12 — append a short banner noting
                                            # Plan 8's deferred catalog removal
                                            # is superseded by Plan 9.
```

**New tests (~24 files):**

```
packages/genie-space-optimizer/tests/unit/
├── test_target_object_typed.py
├── test_repair_intent_target_objects.py
├── test_repair_proposal_target_objects.py
├── test_repair_proposal_required_constructs.py
├── test_llm_repair_proposal_output_schema_plan9.py
├── test_repair_shape_fragments_registry.py
├── test_repair_shape_fragments_other_safety_net.py
├── test_synthesis_validator_reads_required_constructs.py
├── test_llm_direct_slice_resolver.py
├── test_lever6_intent_dispatch_direct_materialization.py
├── test_l5b_dispatch_direct_materialization.py
├── test_generate_proposals_from_strategy_plan5_kwargs.py
├── test_force_lever6_proposal_plan5_kwargs.py
├── test_structural_repair_gate_rejects_absent_zero_repairability.py
├── test_plan9_activation_markers_emit.py
├── test_plan9_activation_markers_all_anchors_covered.py
├── test_harness_forbidden_set_pre_generation.py
├── test_archetype_catalog_deleted.py
├── test_plan7_rollback_learning_default_on.py

packages/genie-space-optimizer/tests/integration/
├── test_plan9_e2e_l5b_intent_to_proposal.py
├── test_plan9_e2e_l6_intent_to_proposal.py
├── test_plan9_e2e_top_n_cardinality_repair.py
├── test_plan9_e2e_defensive_filter_removal.py
└── test_plan9_e2e_grain_pivot_day_vs_mtd.py
```

---

## Tasks

### Task 1: Add `TargetObject` typed contract + `target_objects` field on `RepairIntent` + `RepairProposal` + `LlmRepairProposalOutput`

**Rationale:** Reviewer's three-job inversion #1. `pick_archetype` → `_derive_asset_slice_from_afs` builds an `AssetSlice(tables, metric_view, columns, join_spec)` from blame_set + archetype-derived schema traits. After Plan 9, the LLM emits the slice directly as a typed `target_objects` tuple, and a pure-function resolver builds `AssetSlice` from `target_objects` + `metadata_snapshot` (no archetype required).

**Files:**

- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/target_object_typed.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent.py:133-224`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_proposal_typed.py:70-217`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent_synthesizer.py` (Pydantic schema)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/skills/repair-intent-synthesis/SKILL.md`
- Test: `packages/genie-space-optimizer/tests/unit/test_target_object_typed.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_repair_intent_target_objects.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_repair_proposal_target_objects.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_llm_repair_proposal_output_schema_plan9.py`

- [ ] **Step 1: Write the failing test for `TargetObject` shape**

Create `packages/genie-space-optimizer/tests/unit/test_target_object_typed.py`:

```python
"""Plan 9 Task 1 — TargetObject typed dataclass.

Verifies that TargetObject is frozen + slots + JsonRoundTrip; covers
table / metric_view / column asset kinds; rejects empty identifier;
rejects unknown asset_kind; round-trips through to_json / from_json.
"""
import pytest

from genie_space_optimizer.optimization.target_object_typed import (
    TargetObject,
    AssetKind,
)


def test_target_object_table_kind_is_immutable_and_round_trips():
    obj = TargetObject(
        asset_kind=AssetKind.TABLE,
        identifier="catalog.schema.orders",
        columns=("order_id", "amount"),
    )
    payload = obj.to_json()
    assert payload == {
        "asset_kind": "table",
        "identifier": "catalog.schema.orders",
        "columns": ["order_id", "amount"],
    }
    reconstructed = TargetObject.from_json(payload)
    assert reconstructed == obj
    with pytest.raises(Exception):
        obj.identifier = "different"  # frozen


def test_target_object_metric_view_kind():
    obj = TargetObject(
        asset_kind=AssetKind.METRIC_VIEW,
        identifier="catalog.schema.daily_orders_mv",
        columns=("order_count_total", "order_amount_sum"),
    )
    assert obj.asset_kind == AssetKind.METRIC_VIEW
    assert obj.to_json()["asset_kind"] == "metric_view"


def test_target_object_column_kind_with_no_columns_is_allowed():
    obj = TargetObject(
        asset_kind=AssetKind.COLUMN,
        identifier="catalog.schema.orders.customer_id",
        columns=(),
    )
    assert obj.columns == ()


def test_target_object_rejects_empty_identifier():
    with pytest.raises(ValueError, match="identifier"):
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="",
            columns=("col",),
        )


def test_target_object_rejects_unknown_asset_kind_via_from_json():
    with pytest.raises(ValueError):
        TargetObject.from_json({
            "asset_kind": "view",  # not in AssetKind
            "identifier": "x.y.z",
            "columns": [],
        })


def test_target_object_columns_are_tuple_immutable():
    obj = TargetObject(
        asset_kind=AssetKind.TABLE,
        identifier="x.y.orders",
        columns=("a", "b"),
    )
    assert isinstance(obj.columns, tuple)
```

- [ ] **Step 2: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_target_object_typed.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'genie_space_optimizer.optimization.target_object_typed'`

- [ ] **Step 3: Create `target_object_typed.py`**

```python
"""Plan 9 Task 1 — TargetObject typed contract.

A TargetObject is the LLM-emitted typed slice that replaces the
archetype-derived AssetSlice. Plan 5's repair-intent-synthesis SKILL
emits a tuple of TargetObjects per RepairProposal; the
llm_direct_slice_resolver (T6) resolves each one to a concrete
table / metric_view / column entry from metadata_snapshot.

Pure data + JsonRoundTrip; no logic, no LLM call, no dependency
on archetypes.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


class AssetKind(StrEnum):
    """Closed set of asset kinds the LLM can name in target_objects."""

    TABLE = "table"
    METRIC_VIEW = "metric_view"
    COLUMN = "column"


@dataclass(frozen=True, slots=True)
class TargetObject(JsonRoundTrip):
    """One typed slice the LLM emits in RepairProposal.target_objects.

    Fields:
      * ``asset_kind`` — TABLE / METRIC_VIEW / COLUMN.
      * ``identifier`` — fully qualified name (catalog.schema.name for
        tables and metric views; catalog.schema.table.column for
        columns). MUST be non-empty.
      * ``columns`` — for TABLE / METRIC_VIEW kinds, the subset of
        columns the LLM intends the repair to touch (often a small
        top-K). Empty tuple is allowed for COLUMN kind (the column
        itself is the identifier).
    """

    asset_kind: AssetKind
    identifier: str
    columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.identifier or not self.identifier.strip():
            raise ValueError(
                "TargetObject.identifier must be non-empty"
            )

    def to_json(self) -> dict:  # type: ignore[override]
        return {
            "asset_kind": self.asset_kind.value,
            "identifier": self.identifier,
            "columns": list(self.columns),
        }

    @classmethod
    def from_json(cls, payload: dict) -> "TargetObject":  # type: ignore[override]
        return cls(
            asset_kind=AssetKind(payload["asset_kind"]),
            identifier=str(payload["identifier"]),
            columns=tuple(str(c) for c in payload.get("columns") or ()),
        )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_target_object_typed.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/target_object_typed.py \
        packages/genie-space-optimizer/tests/unit/test_target_object_typed.py
git commit -m "$(cat <<'EOF'
plan9(t1): add TargetObject typed contract

LLM-emitted typed slice that will replace archetype-derived AssetSlice
in subsequent Plan 9 tasks. Pure data + JsonRoundTrip; no archetype
dependency.

Pre-req for: T1 RepairIntent.target_objects, T1 RepairProposal.target_objects,
T6 llm_direct_slice_resolver, T10 catalog deletion.
EOF
)"
```

- [ ] **Step 6: Write the failing test for `RepairIntent.target_objects`**

Create `packages/genie-space-optimizer/tests/unit/test_repair_intent_target_objects.py`:

```python
"""Plan 9 Task 1 — RepairIntent.target_objects field.

Verifies that RepairIntent gains a tuple of TargetObjects, defaults
to empty tuple for backward compatibility with pre-Plan-9 serialized
intents, and round-trips through from_json / to_json.
"""
import pytest

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairIntent,
    RepairShape,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
    TargetObject,
)


def _make_intent(*, target_objects: tuple[TargetObject, ...] = ()) -> RepairIntent:
    return RepairIntent(
        intent_id="intent_test_001",
        intent_name="test_intent",
        intent_description="A test intent.",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="Cluster needs top-N example.",
        confidence="high",
        source="test",
        cluster_id="cluster_test",
        target_qids=("q_001",),
        blame_set=("catalog.schema.orders",),
        rca_card_id="rca_test",
        ag_id="AG_TEST",
        target_objects=target_objects,
    )


def test_repair_intent_target_objects_defaults_empty():
    intent = _make_intent()
    assert intent.target_objects == ()


def test_repair_intent_target_objects_round_trips_via_json():
    targets = (
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="catalog.schema.orders",
            columns=("order_id", "amount"),
        ),
        TargetObject(
            asset_kind=AssetKind.METRIC_VIEW,
            identifier="catalog.schema.daily_orders_mv",
            columns=("order_count_total",),
        ),
    )
    intent = _make_intent(target_objects=targets)
    payload = intent.to_json()
    assert "target_objects" in payload
    assert payload["target_objects"] == [
        {
            "asset_kind": "table",
            "identifier": "catalog.schema.orders",
            "columns": ["order_id", "amount"],
        },
        {
            "asset_kind": "metric_view",
            "identifier": "catalog.schema.daily_orders_mv",
            "columns": ["order_count_total"],
        },
    ]
    reconstructed = RepairIntent.from_json(payload)
    assert reconstructed.target_objects == targets


def test_repair_intent_from_json_backward_compatible_missing_target_objects():
    """Pre-Plan-9 serialized intents (no target_objects key) must
    still deserialize with target_objects=()."""
    payload = {
        "intent_id": "intent_legacy_001",
        "intent_name": "legacy",
        "intent_description": "Legacy intent.",
        "repair_shape": "top_n_by_metric",
        "patch_type": "add_example_sql",
        "rationale": "Legacy.",
        "confidence": "medium",
        "source": "legacy",
        "cluster_id": "c",
        "target_qids": ["q"],
        "blame_set": [],
        "rca_card_id": "r",
        "ag_id": "AG",
    }
    intent = RepairIntent.from_json(payload)
    assert intent.target_objects == ()
```

- [ ] **Step 7: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_repair_intent_target_objects.py -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'target_objects'`.

- [ ] **Step 8: Add `target_objects` field to `RepairIntent`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent.py:170-224`. After the `rollback_reason: str | None = None` line, add `target_objects` field. Then extend `to_json` (add a `to_json` method if not present — it inherits from `JsonRoundTrip` but we need it explicit since `JsonRoundTrip` may not handle nested dataclasses) and `from_json` to handle it.

Add import at the top of `repair_intent.py`:

```python
from genie_space_optimizer.optimization.target_object_typed import (
    TargetObject,
)
```

Modify the dataclass body (insert after `rollback_reason: str | None = None`):

```python
    target_objects: tuple[TargetObject, ...] = ()
```

Modify the `from_json` classmethod at `repair_intent.py:189-224`:

```python
    @classmethod
    def from_json(cls, payload: dict) -> "RepairIntent":  # type: ignore[override]
        return cls(
            intent_id=str(payload["intent_id"]),
            intent_name=str(payload["intent_name"]),
            intent_description=str(payload["intent_description"]),
            repair_shape=RepairShape(payload["repair_shape"]),
            patch_type=PatchType(payload["patch_type"]),
            rationale=str(payload["rationale"]),
            confidence=str(payload["confidence"]),  # type: ignore[arg-type]
            source=str(payload["source"]),
            cluster_id=str(payload["cluster_id"]),
            target_qids=tuple(payload.get("target_qids") or ()),
            blame_set=tuple(payload.get("blame_set") or ()),
            rca_card_id=str(payload.get("rca_card_id") or ""),
            ag_id=str(payload.get("ag_id") or ""),
            applied_at_iter=(
                int(payload["applied_at_iter"])
                if payload.get("applied_at_iter") is not None
                else None
            ),
            applied_signature=(
                str(payload["applied_signature"])
                if payload.get("applied_signature") is not None
                else None
            ),
            acceptance_outcome=(
                str(payload["acceptance_outcome"])
                if payload.get("acceptance_outcome") is not None
                else None
            ),
            rollback_reason=(
                str(payload["rollback_reason"])
                if payload.get("rollback_reason") is not None
                else None
            ),
            target_objects=tuple(
                TargetObject.from_json(t)
                for t in (payload.get("target_objects") or ())
            ),
        )
```

Add an explicit `to_json` method on the dataclass (if `JsonRoundTrip`'s default doesn't already produce the right shape for `tuple[TargetObject, ...]`):

```python
    def to_json(self) -> dict:  # type: ignore[override]
        payload: dict = {
            "intent_id": self.intent_id,
            "intent_name": self.intent_name,
            "intent_description": self.intent_description,
            "repair_shape": self.repair_shape.value,
            "patch_type": self.patch_type.value,
            "rationale": self.rationale,
            "confidence": self.confidence,
            "source": self.source,
            "cluster_id": self.cluster_id,
            "target_qids": list(self.target_qids),
            "blame_set": list(self.blame_set),
            "rca_card_id": self.rca_card_id,
            "ag_id": self.ag_id,
            "target_objects": [t.to_json() for t in self.target_objects],
        }
        if self.applied_at_iter is not None:
            payload["applied_at_iter"] = self.applied_at_iter
        if self.applied_signature is not None:
            payload["applied_signature"] = self.applied_signature
        if self.acceptance_outcome is not None:
            payload["acceptance_outcome"] = self.acceptance_outcome
        if self.rollback_reason is not None:
            payload["rollback_reason"] = self.rollback_reason
        return payload
```

- [ ] **Step 9: Run the test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_repair_intent_target_objects.py -v`
Expected: 3 passed.

- [ ] **Step 10: Update `intent_from_archetype` to populate `target_objects=()` for the deterministic adapter**

The deterministic adapter at `repair_intent.py:262-310` (`intent_from_archetype`) does not have access to typed slice information (it's archetype-derived). It must continue to produce `RepairIntent` with `target_objects=()` so backward compatibility holds. Verify the existing call site at `repair_intent.py:290-310` continues to omit `target_objects=` — the dataclass default kicks in.

Run the full repair-intent test suite to confirm no regression:

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_repair_intent_archetype_adapter.py tests/unit/test_repair_intent.py -v`
Expected: all passed (the new `target_objects` field defaults to `()` so existing call sites are unaffected).

- [ ] **Step 11: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent.py \
        packages/genie-space-optimizer/tests/unit/test_repair_intent_target_objects.py
git commit -m "$(cat <<'EOF'
plan9(t1): add target_objects field to RepairIntent

LLM-emitted typed slice attached to the typed intent. Defaults to
empty tuple so the deterministic intent_from_archetype adapter and
pre-Plan-9 serialized intents remain compatible.

Pre-req for: T6 llm_direct_slice_resolver, T10 catalog deletion.
EOF
)"
```

- [ ] **Step 12: Write the failing test for `RepairProposal.target_objects`**

Create `packages/genie-space-optimizer/tests/unit/test_repair_proposal_target_objects.py`:

```python
"""Plan 9 Task 1 — RepairProposal.target_objects field.

Verifies that RepairProposal carries the typed slice through from
LLM output to materialization. to_repair_intent propagates
target_objects into the RepairIntent stamped on the proposal dict.
"""
import pytest

from genie_space_optimizer.optimization.failure_cluster import FailureCluster
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
    TargetObject,
)


def _make_cluster() -> FailureCluster:
    return FailureCluster(
        cluster_id="c_test",
        root_cause="plural_top_n_collapse",
        affected_qids=("q_001",),
        blame_set=("catalog.schema.orders",),
        recommended_levers=(5,),
        affected_judge="row",
        asi_failure_type="plural_top_n_collapse",
        semantic_theme="top_n_ranking",
    )


def test_repair_proposal_target_objects_defaults_empty():
    proposal = RepairProposal(
        intent_id="intent_001",
        intent_name="top_n_repair",
        intent_description="Add a top-N example.",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="Cluster blames plural-top-N collapse.",
        confidence="high",
        patch_body={
            "example_question": "What are the top 5 products by revenue?",
            "example_sql": "SELECT product, SUM(amount) FROM orders GROUP BY product ORDER BY 2 DESC LIMIT 5",
        },
        blame_set=("catalog.schema.orders",),
    )
    assert proposal.target_objects == ()


def test_repair_proposal_target_objects_round_trips_via_json():
    targets = (
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="catalog.schema.orders",
            columns=("product", "amount"),
        ),
    )
    proposal = RepairProposal(
        intent_id="intent_002",
        intent_name="top_n_repair",
        intent_description="Top-N example.",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="…",
        confidence="high",
        patch_body={
            "example_question": "Top 5 products by revenue?",
            "example_sql": "SELECT product, SUM(amount) FROM orders GROUP BY product ORDER BY 2 DESC LIMIT 5",
        },
        blame_set=("catalog.schema.orders",),
        target_objects=targets,
    )
    payload = proposal.to_json()
    assert "target_objects" in payload
    reconstructed = RepairProposal.from_json(payload)
    assert reconstructed.target_objects == targets


def test_to_repair_intent_propagates_target_objects():
    targets = (
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="catalog.schema.orders",
            columns=("product", "amount"),
        ),
    )
    proposal = RepairProposal(
        intent_id="intent_003",
        intent_name="top_n_repair",
        intent_description="…",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="…",
        confidence="high",
        patch_body={
            "example_question": "Top 5?",
            "example_sql": "SELECT 1",
        },
        blame_set=("catalog.schema.orders",),
        target_objects=targets,
    )
    intent = proposal.to_repair_intent(
        cluster=_make_cluster(),
        ag_id="AG_TEST",
    )
    assert intent.target_objects == targets
```

- [ ] **Step 13: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_repair_proposal_target_objects.py -v`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'target_objects'`.

- [ ] **Step 14: Add `target_objects` field to `RepairProposal` + propagate through bridges**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_proposal_typed.py`. Add import at the top:

```python
from genie_space_optimizer.optimization.target_object_typed import (
    TargetObject,
)
```

Modify the `RepairProposal` dataclass (around line 70-82). After the `blame_set: tuple[str, ...]` field, add:

```python
    target_objects: tuple[TargetObject, ...] = ()
```

Modify `from_llm_output` (around line 84-105) to bridge `pydantic_inst.target_objects` (added in T1 Step 17 below) into the dataclass:

```python
    @classmethod
    def from_llm_output(
        cls,
        pydantic_inst: Any,
        *,
        intent_id: str,
    ) -> "RepairProposal":
        target_objects_raw = getattr(pydantic_inst, "target_objects", None) or []
        target_objects = tuple(
            TargetObject(
                asset_kind=AssetKind(t.asset_kind),
                identifier=str(t.identifier),
                columns=tuple(str(c) for c in (t.columns or [])),
            )
            for t in target_objects_raw
        )
        return cls(
            intent_id=str(intent_id),
            intent_name=str(pydantic_inst.intent_name),
            intent_description=str(pydantic_inst.intent_description),
            repair_shape=RepairShape(pydantic_inst.repair_shape),
            patch_type=PatchType(pydantic_inst.patch_type),
            rationale=str(pydantic_inst.rationale),
            confidence=pydantic_inst.confidence,
            patch_body=dict(pydantic_inst.patch_body or {}),
            blame_set=tuple(
                str(b) for b in pydantic_inst.blame_set or ()
            ),
            target_objects=target_objects,
        )
```

Add an explicit `to_json` / `from_json` pair on `RepairProposal` (the JsonRoundTrip default may not handle nested dataclasses correctly). Insert after `to_proposal_dict`:

```python
    def to_json(self) -> dict:  # type: ignore[override]
        return {
            "intent_id": self.intent_id,
            "intent_name": self.intent_name,
            "intent_description": self.intent_description,
            "repair_shape": self.repair_shape.value,
            "patch_type": self.patch_type.value,
            "rationale": self.rationale,
            "confidence": self.confidence,
            "patch_body": dict(self.patch_body),
            "blame_set": list(self.blame_set),
            "target_objects": [t.to_json() for t in self.target_objects],
        }

    @classmethod
    def from_json(cls, payload: dict) -> "RepairProposal":  # type: ignore[override]
        return cls(
            intent_id=str(payload["intent_id"]),
            intent_name=str(payload["intent_name"]),
            intent_description=str(payload["intent_description"]),
            repair_shape=RepairShape(payload["repair_shape"]),
            patch_type=PatchType(payload["patch_type"]),
            rationale=str(payload["rationale"]),
            confidence=payload["confidence"],
            patch_body=dict(payload.get("patch_body") or {}),
            blame_set=tuple(str(b) for b in payload.get("blame_set") or ()),
            target_objects=tuple(
                TargetObject.from_json(t)
                for t in (payload.get("target_objects") or ())
            ),
        )
```

Modify `to_repair_intent` to propagate `target_objects`. Find the method body (search `def to_repair_intent`) and ensure the constructed `RepairIntent` includes `target_objects=self.target_objects`:

```python
    def to_repair_intent(
        self,
        *,
        cluster: "FailureCluster",
        ag_id: str,
    ) -> RepairIntent:
        return RepairIntent(
            intent_id=self.intent_id,
            intent_name=self.intent_name,
            intent_description=self.intent_description,
            repair_shape=self.repair_shape,
            patch_type=self.patch_type,
            rationale=self.rationale,
            confidence=self.confidence,
            source="llm_repair_intent_synthesizer",
            cluster_id=cluster.cluster_id,
            target_qids=tuple(cluster.affected_qids),
            blame_set=self.blame_set,
            rca_card_id=getattr(cluster, "rca_card_id", "") or "",
            ag_id=ag_id,
            target_objects=self.target_objects,
        )
```

Add the `AssetKind` import at the top:

```python
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
    TargetObject,
)
```

- [ ] **Step 15: Run the test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_repair_proposal_target_objects.py -v`
Expected: 3 passed.

- [ ] **Step 16: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_proposal_typed.py \
        packages/genie-space-optimizer/tests/unit/test_repair_proposal_target_objects.py
git commit -m "$(cat <<'EOF'
plan9(t1): add target_objects field to RepairProposal

LLM-emitted typed slice flows through RepairProposal → RepairIntent
via to_repair_intent. Pre-req for T6's llm_direct_slice_resolver
and T10's catalog deletion.
EOF
)"
```

- [ ] **Step 17: Write the failing test for the Pydantic LlmRepairProposalOutput schema**

Create `packages/genie-space-optimizer/tests/unit/test_llm_repair_proposal_output_schema_plan9.py`:

```python
"""Plan 9 Task 1 — extend LlmRepairProposalOutput Pydantic schema with
target_objects. Verifies strict-mode JSON Schema accepts the new
field, rejects empty target_objects when patch_type implies a slice
is required (TODO: enforced per patch_type matrix), and bridges
cleanly through from_llm_output.
"""
import pytest
from pydantic import ValidationError

from genie_space_optimizer.optimization.repair_intent_synthesizer import (
    LlmRepairProposalOutput,
)


def test_llm_output_accepts_target_objects():
    out = LlmRepairProposalOutput(
        intent_name="top_n_repair",
        intent_description="…",
        repair_shape="top_n_by_metric",
        patch_type="add_example_sql",
        rationale="…",
        confidence="high",
        patch_body={
            "example_question": "Top 5?",
            "example_sql": "SELECT 1",
        },
        blame_set=["catalog.schema.orders"],
        target_objects=[
            {
                "asset_kind": "table",
                "identifier": "catalog.schema.orders",
                "columns": ["product", "amount"],
            },
        ],
    )
    assert out.target_objects[0].asset_kind == "table"
    assert out.target_objects[0].identifier == "catalog.schema.orders"
    assert list(out.target_objects[0].columns) == ["product", "amount"]


def test_llm_output_target_objects_defaults_to_empty_list():
    """For PR1 backward compat: missing target_objects is allowed.
    PR2 (post-catalog deletion) tightens this to require a non-empty
    target_objects when repair_shape != OTHER."""
    out = LlmRepairProposalOutput(
        intent_name="x",
        intent_description="…",
        repair_shape="other",
        patch_type="add_instruction",
        rationale="…",
        confidence="medium",
        patch_body={"instruction_text": "Do X."},
        blame_set=[],
    )
    assert out.target_objects == []


def test_llm_output_rejects_unknown_asset_kind():
    with pytest.raises(ValidationError):
        LlmRepairProposalOutput(
            intent_name="x",
            intent_description="…",
            repair_shape="top_n_by_metric",
            patch_type="add_example_sql",
            rationale="…",
            confidence="high",
            patch_body={
                "example_question": "?",
                "example_sql": "SELECT 1",
            },
            blame_set=[],
            target_objects=[
                {
                    "asset_kind": "view",  # not in AssetKind
                    "identifier": "x.y.z",
                    "columns": [],
                },
            ],
        )
```

- [ ] **Step 18: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_llm_repair_proposal_output_schema_plan9.py -v`
Expected: FAIL — `target_objects` is not a field on `LlmRepairProposalOutput`.

- [ ] **Step 19: Extend `LlmRepairProposalOutput` Pydantic schema**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent_synthesizer.py`. Find the `LlmRepairProposalOutput` class definition (`grep -n "class LlmRepairProposalOutput"`). Add a nested `LlmTargetObject` model and extend the parent:

```python
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class LlmTargetObject(BaseModel):
    """Plan 9 Task 1 — LLM-emitted typed slice. Bridges to
    TargetObject dataclass in target_object_typed.py."""

    model_config = ConfigDict(extra="forbid", strict=True)

    asset_kind: Literal["table", "metric_view", "column"] = Field(
        description=(
            "The kind of asset this slice points at. "
            "'table' for base tables; 'metric_view' for UC metric views; "
            "'column' for a single named column."
        ),
    )
    identifier: str = Field(
        min_length=1,
        description=(
            "Fully qualified name. For 'table'/'metric_view': "
            "'catalog.schema.name'. For 'column': "
            "'catalog.schema.table.column_name'."
        ),
    )
    columns: list[str] = Field(
        default_factory=list,
        description=(
            "For 'table'/'metric_view': the subset of columns the "
            "repair will touch (typically 1-8). For 'column': empty list."
        ),
    )


# Locate the existing LlmRepairProposalOutput class. Add the target_objects
# field at the end of its body:

class LlmRepairProposalOutput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    # …existing fields kept verbatim…

    target_objects: list[LlmTargetObject] = Field(
        default_factory=list,
        description=(
            "Plan 9 — typed slice replacing archetype-derived "
            "AssetSlice. The LLM emits the assets (tables, metric "
            "views, columns) the repair targets. Empty list is "
            "allowed only when repair_shape == 'other' or "
            "patch_type does not require a slice (e.g. "
            "'add_instruction'). After Plan 9 PR2 (catalog "
            "deletion), this becomes required for shape-keyed "
            "repairs."
        ),
    )
```

- [ ] **Step 20: Update the repair-intent-synthesis SKILL.md prompt**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/skills/repair-intent-synthesis/SKILL.md`. Add a new section after `<patch_body_shapes>`:

```markdown
<target_objects>

For every repair you propose, emit a `target_objects` array listing the assets the repair touches.

Each entry has three fields:
- `asset_kind`: one of `"table"`, `"metric_view"`, `"column"`.
- `identifier`: fully qualified name (`catalog.schema.name` for tables and metric views; `catalog.schema.table.column_name` for columns).
- `columns`: for `"table"` and `"metric_view"`, list the subset of columns the repair will touch (typically 1-8). For `"column"`, leave as an empty array.

The slice must come from the cluster's blame_set and the schema you were shown — do not invent identifiers. Reusing identifiers verbatim is correct and expected; the downstream resolver will fail-loud if an identifier is not present in the schema.

For `repair_shape == "other"` or patch types that do not target a slice (e.g. `add_instruction`), an empty `target_objects` array is allowed.

Examples:

For a top-N example SQL repair on the orders table:
```
"target_objects": [
  {
    "asset_kind": "table",
    "identifier": "main.sales.orders",
    "columns": ["product_id", "amount"]
  }
]
```

For a metric-view refinement adding a new measure:
```
"target_objects": [
  {
    "asset_kind": "metric_view",
    "identifier": "main.sales.daily_orders_mv",
    "columns": ["order_amount_sum"]
  }
]
```
</target_objects>
```

- [ ] **Step 21: Run the Pydantic schema test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_llm_repair_proposal_output_schema_plan9.py -v`
Expected: 3 passed.

- [ ] **Step 22: Run the full repair-intent synthesizer test suite to confirm no regression**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_repair_intent_synthesizer.py tests/unit/test_repair_proposal_typed.py tests/unit/test_llm_repair_proposal_output_schema.py -v`
Expected: all passed.

- [ ] **Step 23: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent_synthesizer.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/skills/repair-intent-synthesis/SKILL.md \
        packages/genie-space-optimizer/tests/unit/test_llm_repair_proposal_output_schema_plan9.py
git commit -m "$(cat <<'EOF'
plan9(t1): extend LlmRepairProposalOutput with target_objects

LLM emits typed asset slices directly. New LlmTargetObject Pydantic
model with strict-mode JSON Schema bridges to the TargetObject
dataclass via RepairProposal.from_llm_output.

SKILL.md updated with <target_objects> section and examples.

Pre-req for: T6 llm_direct_slice_resolver, T10 catalog deletion.
EOF
)"
```

---

### Task 2: Replace `archetype.prompt_template` with `RepairShape`-keyed prompt fragments

**Rationale:** Reviewer's three-job inversion #2. `archetype.prompt_template` is interpolated into the L5b/L6 generator prompt at three sites (`synthesis.py:336`, `preflight_synthesis.py:1915`, `repair_intent.py:298`). After Plan 9, the renderer reads `RepairProposal.repair_shape` and picks the matching fragment from a new `_repair_shape_fragments.py` registry. `RepairShape.OTHER` falls back to a single "free-form structural rewrite" fragment that is the ultimate safety net after catalog removal.

**Files:**

- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompts/_repair_shape_fragments.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/synthesis.py:333-340`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/preflight_synthesis.py:1912-1920`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent.py:296-301`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-5b-example-sql/SKILL.md`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-6-sql-expression/SKILL.md`
- Test: `packages/genie-space-optimizer/tests/unit/test_repair_shape_fragments_registry.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_repair_shape_fragments_other_safety_net.py`

- [ ] **Step 1: Write the failing test for the fragment registry**

Create `packages/genie-space-optimizer/tests/unit/test_repair_shape_fragments_registry.py`:

```python
"""Plan 9 Task 2 — _repair_shape_fragments registry.

Verifies every RepairShape enum member maps to a non-empty prompt
fragment, the registry is complete (no missing shapes), and the
OTHER fragment is the free-form structural rewrite safety net.
"""
import pytest

from genie_space_optimizer.optimization.repair_intent import RepairShape
from genie_space_optimizer.optimization.prompts._repair_shape_fragments import (
    REPAIR_SHAPE_FRAGMENTS,
    fragment_for,
)


def test_every_repair_shape_has_a_fragment():
    """Catalog-drift detector: when a new RepairShape is added,
    the registry MUST have an entry in the same commit."""
    missing = [
        shape for shape in RepairShape
        if shape not in REPAIR_SHAPE_FRAGMENTS
    ]
    assert missing == [], (
        f"Missing fragments for RepairShape members: {missing}. "
        f"Add entries to REPAIR_SHAPE_FRAGMENTS."
    )


def test_every_fragment_is_non_empty_and_str():
    for shape, fragment in REPAIR_SHAPE_FRAGMENTS.items():
        assert isinstance(fragment, str), shape
        assert fragment.strip(), shape


def test_fragment_for_returns_correct_fragment():
    for shape in RepairShape:
        assert fragment_for(shape) == REPAIR_SHAPE_FRAGMENTS[shape]


def test_fragment_for_unknown_string_falls_back_to_other():
    """When repair_shape is not a valid RepairShape value (legacy
    pre-Plan-9 traces), fragment_for falls back to the OTHER
    fragment rather than raising."""
    # Pass a non-enum string; helper should accept str | RepairShape.
    result = fragment_for("not_a_real_shape")
    assert result == REPAIR_SHAPE_FRAGMENTS[RepairShape.OTHER]
```

- [ ] **Step 2: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_repair_shape_fragments_registry.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Create the fragment registry**

Create `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompts/__init__.py` if it does not already exist:

```python
"""Plan 9 — prompt fragment registries.

Repair-shape-keyed fragments replace archetype.prompt_template
interpolation from the pre-Plan-9 catalog. See _repair_shape_fragments.py
for the public registry.
"""
```

Create `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompts/_repair_shape_fragments.py`:

```python
"""Plan 9 Task 2 — RepairShape-keyed prompt fragment registry.

Replaces the pre-Plan-9 archetype.prompt_template interpolation.
Each RepairShape enum member maps to a short natural-language
description of the shape that gets fed into the L5b / L6 generator
prompt. RepairShape.OTHER is the free-form structural rewrite
safety net — the ultimate fallback after catalog removal.

When a new RepairShape is added to repair_intent.py, an entry MUST
be added here in the same commit. Pinned by
test_repair_shape_fragments_registry.test_every_repair_shape_has_a_fragment.

Adding new entries is the path to expressing new repair patterns
without inventing new archetypes — the LLM picks the shape; the
renderer threads the fragment; no catalog gate.
"""
from __future__ import annotations

from typing import Union

from genie_space_optimizer.optimization.repair_intent import RepairShape


REPAIR_SHAPE_FRAGMENTS: dict[RepairShape, str] = {
    RepairShape.TOP_N_BY_METRIC: (
        "Produce a Top-N query: aggregate a numeric column by a "
        "categorical dimension, ORDER BY the aggregate DESC, LIMIT N. "
        "Use the target_objects you emitted to pick the table and "
        "the metric / dimension columns. Do NOT reproduce any benchmark "
        "text; invent a concrete but reasonable question."
    ),
    RepairShape.ORDERED_LIST_BY_METRIC: (
        "Produce a cardinality-preserving ordered-list query for a plural "
        "ranking question. Aggregate a numeric column by a categorical "
        "dimension and ORDER BY the aggregate DESC. Do not filter to "
        "rank = 1 or use SELECT TOP 1 — the question is about ranking, "
        "not about picking a single row. Do NOT reproduce any benchmark "
        "text; invent a concrete but reasonable question."
    ),
    RepairShape.RANK_WITHIN_GROUP: (
        "Rank rows within each group using ROW_NUMBER() or RANK() OVER "
        "(PARTITION BY dim ORDER BY metric DESC). Use the target_objects "
        "to pick the partition dimension and the ranking metric column."
    ),
    RepairShape.PERIOD_OVER_PERIOD: (
        "Compare a metric across two time windows (e.g. this month vs "
        "last month, day vs MTD, current quarter vs prior quarter). "
        "Use DATE_TRUNC or a simple range predicate; pick the time "
        "column from your target_objects. Provide a clear "
        "business-meaningful question."
    ),
    RepairShape.FILTER_COMPOSE: (
        "Compose a named reusable filter as an SQL snippet. Example: "
        "is_active_customer := status = 'active' AND deleted_at IS NULL. "
        "Use the target_objects to pick the columns the filter "
        "references; the filter snippet name should be descriptive."
    ),
    RepairShape.FILTER_REMOVE: (
        "Remove a defensive or overly-narrow filter that excludes rows "
        "the question actually needs. Emit a corrective example SQL "
        "that demonstrates the correct (broader) filter, or emit an "
        "instruction snippet that documents the filter must be removed. "
        "Use target_objects to identify the column(s) the existing "
        "filter wrongly constrains."
    ),
    RepairShape.JOIN_DISCOVERY: (
        "Demonstrate the correct join between two related entities. "
        "Use the foreign-key column names from your target_objects "
        "explicitly (e.g. child.parent_id = parent.id) and pick the "
        "right join type (INNER vs LEFT). Project a small handful of "
        "columns from both sides so the relationship is unambiguous."
    ),
    RepairShape.SQL_EXPRESSION: (
        "Emit a named SQL expression (`add_sql_snippet_expression`) "
        "that computes a derived value from existing columns. Use "
        "target_objects to anchor the expression to the correct "
        "table and columns. The expression should be reusable across "
        "multiple example SQLs."
    ),
    RepairShape.COLUMN_DESCRIPTION: (
        "Add or refine a column description to disambiguate two "
        "columns the LLM is confusing (e.g. prefix-similar columns "
        "like is_prior_year_same_day vs is_one_day_prior_year_same_day). "
        "Use target_objects to pick the COLUMN-kind identifier; the "
        "description should explain when to use this column vs the "
        "confusable alternative."
    ),
    RepairShape.METRIC_VIEW_REFINEMENT: (
        "Refine a metric view: add a missing dimension or measure, "
        "or rename a measure to clarify its semantics. Use "
        "target_objects with asset_kind=metric_view; the columns "
        "list should enumerate the measures/dimensions the refinement "
        "affects."
    ),
    RepairShape.INSTRUCTION: (
        "Emit a natural-language instruction that documents a rule "
        "the LLM keeps violating (e.g. 'always GROUP BY all "
        "non-aggregated SELECT columns'). Instructions do not require "
        "target_objects — leave the array empty."
    ),
    RepairShape.OTHER: (
        # ULTIMATE SAFETY NET — Plan 9's free-form structural rewrite
        # fragment. Fires when the LLM picks RepairShape.OTHER (e.g.
        # because the repair pattern is novel and does not fit any
        # named shape). The fragment instructs the LLM to emit a
        # self-contained example SQL or snippet, justify why no named
        # shape fits, and ground every column reference in
        # target_objects. After catalog removal (T10), this is the
        # only deterministic fallback left.
        "Free-form structural rewrite: emit an example SQL or SQL "
        "snippet that solves the cluster's failure pattern using "
        "ONLY the assets and columns named in your target_objects. "
        "Your rationale MUST explain why none of the named "
        "RepairShape values fit this repair. Ground every column "
        "reference in target_objects; do not invent identifiers. "
        "Keep the SQL self-contained and runnable against the "
        "schema you were shown."
    ),
}


def fragment_for(repair_shape: Union[RepairShape, str]) -> str:
    """Return the prompt fragment for the given RepairShape.

    Accepts RepairShape enum value or raw string (for replay of
    pre-Plan-9 traces). Unknown strings fall back to the OTHER
    fragment.
    """
    if isinstance(repair_shape, str):
        try:
            repair_shape = RepairShape(repair_shape)
        except ValueError:
            return REPAIR_SHAPE_FRAGMENTS[RepairShape.OTHER]
    return REPAIR_SHAPE_FRAGMENTS.get(
        repair_shape,
        REPAIR_SHAPE_FRAGMENTS[RepairShape.OTHER],
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_repair_shape_fragments_registry.py -v`
Expected: 4 passed.

- [ ] **Step 5: Write a focused test for the OTHER safety-net behaviour**

Create `packages/genie-space-optimizer/tests/unit/test_repair_shape_fragments_other_safety_net.py`:

```python
"""Plan 9 Task 2 — verify the OTHER fragment is a complete safety net.

After catalog removal (T10), RepairShape.OTHER is the only
deterministic fragment left when the LLM picks a novel shape.
Test ensures the fragment mentions target_objects, instructs no
benchmark text reproduction, and is non-trivial in length.
"""
from genie_space_optimizer.optimization.repair_intent import RepairShape
from genie_space_optimizer.optimization.prompts._repair_shape_fragments import (
    REPAIR_SHAPE_FRAGMENTS,
)


OTHER = REPAIR_SHAPE_FRAGMENTS[RepairShape.OTHER]


def test_other_fragment_references_target_objects():
    assert "target_objects" in OTHER, (
        "OTHER fragment MUST instruct LLM to ground in target_objects "
        "— it is the only constraint on free-form structural rewrites."
    )


def test_other_fragment_forbids_inventing_identifiers():
    assert "do not invent" in OTHER.lower(), (
        "OTHER fragment MUST forbid inventing identifiers — without "
        "this, free-form rewrites would frequently reference "
        "non-existent columns."
    )


def test_other_fragment_requires_justification():
    assert "rationale" in OTHER.lower(), (
        "OTHER fragment MUST require the LLM to explain why no "
        "named shape fits — without this, every repair would "
        "trivially pick OTHER."
    )


def test_other_fragment_is_substantial():
    """Catch accidental fragment shrinkage — OTHER MUST be at
    least 200 chars to convey the full free-form contract."""
    assert len(OTHER) >= 200, len(OTHER)
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_repair_shape_fragments_other_safety_net.py -v`
Expected: 4 passed.

- [ ] **Step 7: Wire the fragment registry into `synthesis.py:333-340`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/synthesis.py`. Find the block at line 333-340:

Current:
```python
    shape = _ARCHETYPE_NAME_TO_SHAPE.get(archetype.name, RepairShape.OTHER)
    synthetic_intent = SimpleNamespace(
        intent_name=archetype.name,
        intent_description=archetype.prompt_template,
        repair_shape=shape,
        rationale=f"Cluster blames the {archetype.name} shape.",
    )
```

Replacement (uses the fragment registry; archetype reference stays for now — T10 deletes it):
```python
    shape = _ARCHETYPE_NAME_TO_SHAPE.get(archetype.name, RepairShape.OTHER)
    from genie_space_optimizer.optimization.prompts._repair_shape_fragments import (
        fragment_for,
    )
    synthetic_intent = SimpleNamespace(
        intent_name=archetype.name,
        intent_description=fragment_for(shape),
        repair_shape=shape,
        rationale=f"Cluster blames the {archetype.name} shape.",
    )
```

- [ ] **Step 8: Wire the fragment registry into `preflight_synthesis.py:1912-1920`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/preflight_synthesis.py`. Find the block at line 1912-1920:

Current:
```python
        "schema_example_identifier": _first_asset_identifier(context),
        "metric_view_contract": _format_metric_view_contract(context),
        "archetype_name": archetype.name,
        "archetype_prompt_template": archetype.prompt_template,
        "archetype_output_shape": json.dumps(archetype.output_shape),
        "identifier_allowlist": context.to_identifier_allowlist(),
        "existing_questions_list": _format_existing_questions(existing_questions),
        "retry_feedback": retry_block,
```

Replacement (the prompt_template comes from the fragment registry keyed on the repair_shape mapped from the archetype; archetype reference stays for now until T10):
```python
    from genie_space_optimizer.optimization.repair_intent import (
        _ARCHETYPE_NAME_TO_SHAPE,
        RepairShape,
    )
    from genie_space_optimizer.optimization.prompts._repair_shape_fragments import (
        fragment_for,
    )
    _preflight_shape = _ARCHETYPE_NAME_TO_SHAPE.get(
        archetype.name, RepairShape.OTHER,
    )
```

Then immediately below, in the dict:
```python
        "schema_example_identifier": _first_asset_identifier(context),
        "metric_view_contract": _format_metric_view_contract(context),
        "archetype_name": archetype.name,
        "archetype_prompt_template": fragment_for(_preflight_shape),
        "archetype_output_shape": json.dumps(archetype.output_shape),
        "identifier_allowlist": context.to_identifier_allowlist(),
        "existing_questions_list": _format_existing_questions(existing_questions),
        "retry_feedback": retry_block,
```

(T10 deletes the `archetype_name` and `archetype_output_shape` references and renames `archetype_prompt_template` → `repair_shape_fragment`.)

- [ ] **Step 9: Wire the fragment registry into `repair_intent.py:296-301`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent.py`. Find the block at line 296-301 inside `intent_from_archetype`:

Current:
```python
    # intent_description = archetype.prompt_template clipped to one
    # sentence. Plan 2's LLM call replaces this with free-form text.
    description = archetype.prompt_template.split(". ")[0].strip()
    if not description.endswith("."):
        description += "."
```

Replacement:
```python
    # Plan 9 — intent_description comes from the RepairShape fragment
    # registry, clipped to one sentence. The pre-Plan-9 path read
    # archetype.prompt_template directly; this routes through the
    # registry so the deterministic adapter and the LLM-driven path
    # share the same prompt vocabulary.
    from genie_space_optimizer.optimization.prompts._repair_shape_fragments import (
        fragment_for,
    )
    fragment = fragment_for(shape)
    description = fragment.split(". ")[0].strip()
    if not description.endswith("."):
        description += "."
```

(Note: `shape` is already bound earlier in `intent_from_archetype` at the line `shape = _ARCHETYPE_NAME_TO_SHAPE[archetype.name]`.)

- [ ] **Step 10: Update the L5b SKILL.md prompt**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-5b-example-sql/SKILL.md`. Add a note at the top of the system prompt section explaining the prompt-shape contract:

```markdown
## Plan 9 — RepairShape drives the prompt shape

In pre-Plan-9 versions of this skill, the prompt was shaped by a deterministic catalog (`archetype.prompt_template`) selected before the LLM ran. After Plan 9, the LLM picks the `repair_shape` itself (from the `RepairShape` enum) and the renderer threads the matching prompt fragment from `optimization/prompts/_repair_shape_fragments.py`. This means:

- You are not constrained by a fixed catalog of shapes. If none of the named `RepairShape` values fit, pick `RepairShape.OTHER` and emit a self-contained free-form structural rewrite (anchored in `target_objects`).
- `intent_description` you emit should match the shape fragment for your chosen `repair_shape`. The renderer will inline the matching fragment into the system prompt automatically; do not duplicate the fragment text.
- For `RepairShape.OTHER`, your `rationale` MUST explain why no named shape fits.
```

- [ ] **Step 11: Update the L6 SKILL.md prompt**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-6-sql-expression/SKILL.md`. Add the same "Plan 9 — RepairShape drives the prompt shape" note at the top of the system prompt section, adjusted for L6 (sql-expression / sql-snippet patch types).

- [ ] **Step 12: Run the full synthesis + intent test suites to confirm no regression**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_synthesis.py tests/unit/test_preflight_synthesis.py tests/unit/test_repair_intent_archetype_adapter.py -v`
Expected: all passed (the fragment-driven descriptions are byte-stable because the registry text matches the archetype text closely; some snapshot tests may need regeneration — regenerate with `--snapshot-update` if so).

- [ ] **Step 13: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompts/__init__.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompts/_repair_shape_fragments.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/synthesis.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/preflight_synthesis.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-5b-example-sql/SKILL.md \
        packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-6-sql-expression/SKILL.md \
        packages/genie-space-optimizer/tests/unit/test_repair_shape_fragments_registry.py \
        packages/genie-space-optimizer/tests/unit/test_repair_shape_fragments_other_safety_net.py
git commit -m "$(cat <<'EOF'
plan9(t2): replace archetype.prompt_template with RepairShape fragments

LLM-keyed prompt selection. RepairShape.OTHER fragment is the
free-form structural rewrite safety net that makes T10's catalog
deletion safe. archetype.prompt_template references at three sites
(synthesis.py:336, preflight_synthesis.py:1915, repair_intent.py:298)
now route through fragment_for(shape).

Pre-req for: T10 catalog deletion.
EOF
)"
```

---

### Task 3: Add LLM-emitted `RepairProposal.required_constructs` + read from validator

**Rationale:** Reviewer's three-job inversion #3. `archetype.output_shape["requires_constructs"]` is read at `synthesis.py:673` to check the generated SQL contains required clauses (SELECT, GROUP_BY, etc.). After Plan 9, the LLM emits its own `required_constructs: tuple[str, ...]` alongside `patch_body`; the deterministic 5-gate validator reads the contract from the LLM output instead of the catalog.

**Files:**

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_proposal_typed.py` (add `required_constructs` field)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent_synthesizer.py` (extend `LlmRepairProposalOutput`)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/synthesis.py:670-680` (validator reads from proposal)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/skills/repair-intent-synthesis/SKILL.md`
- Test: `packages/genie-space-optimizer/tests/unit/test_repair_proposal_required_constructs.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_synthesis_validator_reads_required_constructs.py`

- [ ] **Step 1: Write the failing test for the new field**

Create `packages/genie-space-optimizer/tests/unit/test_repair_proposal_required_constructs.py`:

```python
"""Plan 9 Task 3 — RepairProposal.required_constructs field.

The LLM emits the contract its own SQL must satisfy. Replaces
archetype.output_shape["requires_constructs"]. Constructs are
case-sensitive SQL clause keywords like 'SELECT', 'GROUP_BY',
'ORDER_BY', 'LIMIT', 'JOIN', 'WHERE', 'WINDOW'.
"""
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)


def test_required_constructs_defaults_empty_tuple():
    proposal = RepairProposal(
        intent_id="i_001",
        intent_name="x",
        intent_description="…",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType.ADD_INSTRUCTION,
        rationale="…",
        confidence="medium",
        patch_body={"instruction_text": "Do X."},
        blame_set=(),
    )
    assert proposal.required_constructs == ()


def test_required_constructs_round_trips_via_json():
    proposal = RepairProposal(
        intent_id="i_002",
        intent_name="top_n",
        intent_description="…",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="…",
        confidence="high",
        patch_body={
            "example_question": "?",
            "example_sql": "SELECT 1",
        },
        blame_set=("a",),
        required_constructs=("SELECT", "GROUP_BY", "ORDER_BY", "LIMIT"),
    )
    payload = proposal.to_json()
    assert payload["required_constructs"] == [
        "SELECT", "GROUP_BY", "ORDER_BY", "LIMIT",
    ]
    reconstructed = RepairProposal.from_json(payload)
    assert reconstructed.required_constructs == proposal.required_constructs
```

- [ ] **Step 2: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_repair_proposal_required_constructs.py -v`
Expected: FAIL — `unexpected keyword argument 'required_constructs'`.

- [ ] **Step 3: Add `required_constructs` field to `RepairProposal`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_proposal_typed.py`. After the `target_objects: tuple[TargetObject, ...] = ()` field (added in T1), add:

```python
    required_constructs: tuple[str, ...] = ()
```

Extend `to_json` (added in T1) — include the field:

```python
    def to_json(self) -> dict:  # type: ignore[override]
        return {
            "intent_id": self.intent_id,
            "intent_name": self.intent_name,
            "intent_description": self.intent_description,
            "repair_shape": self.repair_shape.value,
            "patch_type": self.patch_type.value,
            "rationale": self.rationale,
            "confidence": self.confidence,
            "patch_body": dict(self.patch_body),
            "blame_set": list(self.blame_set),
            "target_objects": [t.to_json() for t in self.target_objects],
            "required_constructs": list(self.required_constructs),
        }
```

Extend `from_json`:

```python
    @classmethod
    def from_json(cls, payload: dict) -> "RepairProposal":  # type: ignore[override]
        return cls(
            intent_id=str(payload["intent_id"]),
            intent_name=str(payload["intent_name"]),
            intent_description=str(payload["intent_description"]),
            repair_shape=RepairShape(payload["repair_shape"]),
            patch_type=PatchType(payload["patch_type"]),
            rationale=str(payload["rationale"]),
            confidence=payload["confidence"],
            patch_body=dict(payload.get("patch_body") or {}),
            blame_set=tuple(str(b) for b in payload.get("blame_set") or ()),
            target_objects=tuple(
                TargetObject.from_json(t)
                for t in (payload.get("target_objects") or ())
            ),
            required_constructs=tuple(
                str(c) for c in (payload.get("required_constructs") or ())
            ),
        )
```

Extend `from_llm_output` to bridge `pydantic_inst.required_constructs`:

```python
    @classmethod
    def from_llm_output(
        cls,
        pydantic_inst: Any,
        *,
        intent_id: str,
    ) -> "RepairProposal":
        target_objects_raw = getattr(pydantic_inst, "target_objects", None) or []
        target_objects = tuple(
            TargetObject(
                asset_kind=AssetKind(t.asset_kind),
                identifier=str(t.identifier),
                columns=tuple(str(c) for c in (t.columns or [])),
            )
            for t in target_objects_raw
        )
        required_constructs = tuple(
            str(c) for c in (
                getattr(pydantic_inst, "required_constructs", None) or []
            )
        )
        return cls(
            intent_id=str(intent_id),
            intent_name=str(pydantic_inst.intent_name),
            intent_description=str(pydantic_inst.intent_description),
            repair_shape=RepairShape(pydantic_inst.repair_shape),
            patch_type=PatchType(pydantic_inst.patch_type),
            rationale=str(pydantic_inst.rationale),
            confidence=pydantic_inst.confidence,
            patch_body=dict(pydantic_inst.patch_body or {}),
            blame_set=tuple(
                str(b) for b in pydantic_inst.blame_set or ()
            ),
            target_objects=target_objects,
            required_constructs=required_constructs,
        )
```

- [ ] **Step 4: Extend `LlmRepairProposalOutput` Pydantic schema**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent_synthesizer.py`. Find `LlmRepairProposalOutput` and add the field:

```python
    required_constructs: list[str] = Field(
        default_factory=list,
        description=(
            "Plan 9 — SQL clause keywords your patch_body's SQL must "
            "contain. The deterministic validator reads this list and "
            "rejects the proposal if the generated SQL is missing any. "
            "Use uppercase clause names: SELECT, FROM, WHERE, GROUP_BY, "
            "ORDER_BY, LIMIT, JOIN, WINDOW, HAVING, CASE. "
            "For patch types that do not produce SQL (e.g. "
            "add_instruction, add_column_description), leave as []."
        ),
    )
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_repair_proposal_required_constructs.py -v`
Expected: 2 passed.

- [ ] **Step 6: Write the failing test for the validator wiring**

Create `packages/genie-space-optimizer/tests/unit/test_synthesis_validator_reads_required_constructs.py`:

```python
"""Plan 9 Task 3 — synthesis.py validator reads required_constructs
from RepairProposal instead of archetype.output_shape.

When a proposal carries non-empty required_constructs, the
output_shape gate must check the generated SQL against the
proposal's contract, not the archetype's. When the proposal carries
empty required_constructs (legacy / instruction path), the gate
falls back to the archetype's contract for backward compatibility
(deleted by T10).
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.synthesis import (
    check_output_shape,
)


def _make_archetype_with_constructs(*constructs):
    """Helper — simulate a minimal Archetype carrying output_shape."""
    return SimpleNamespace(
        output_shape={"requires_constructs": list(constructs)},
    )


def _make_proposal(required_constructs=()):
    return RepairProposal(
        intent_id="i_001",
        intent_name="x",
        intent_description="…",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="…",
        confidence="high",
        patch_body={
            "example_question": "?",
            "example_sql": "SELECT product, SUM(amount) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 5",
        },
        blame_set=(),
        required_constructs=required_constructs,
    )


def test_validator_uses_proposal_required_constructs_when_present():
    proposal = _make_proposal(
        required_constructs=("SELECT", "GROUP_BY", "ORDER_BY", "LIMIT"),
    )
    archetype = _make_archetype_with_constructs("WINDOW")  # wrong shape
    sql_dict = {"example_sql": proposal.patch_body["example_sql"]}

    result = check_output_shape(
        sql_dict, archetype=archetype, proposal=proposal,
    )
    assert result.passed is True


def test_validator_rejects_when_proposal_constructs_missing_from_sql():
    proposal = _make_proposal(
        required_constructs=("SELECT", "GROUP_BY", "WINDOW"),  # WINDOW not in SQL
    )
    archetype = _make_archetype_with_constructs("SELECT")
    sql_dict = {"example_sql": "SELECT 1 FROM orders"}

    result = check_output_shape(
        sql_dict, archetype=archetype, proposal=proposal,
    )
    assert result.passed is False


def test_validator_falls_back_to_archetype_when_proposal_constructs_empty():
    """Pre-T10 backward compat. Once T10 deletes the catalog,
    archetype is None and this branch is gone."""
    proposal = _make_proposal(required_constructs=())  # empty — fallback
    archetype = _make_archetype_with_constructs("SELECT", "LIMIT")
    sql_dict = {"example_sql": "SELECT 1 FROM orders LIMIT 1"}

    result = check_output_shape(
        sql_dict, archetype=archetype, proposal=proposal,
    )
    assert result.passed is True
```

- [ ] **Step 7: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_synthesis_validator_reads_required_constructs.py -v`
Expected: FAIL — `check_output_shape` does not accept `proposal=` kwarg.

- [ ] **Step 8: Modify the validator at `synthesis.py:670-680`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/synthesis.py`. Find the validator function (search for `requires_constructs` near line 673):

Current (paraphrased — verify line numbers):
```python
def check_output_shape(proposal: dict, *, archetype) -> GateResult:
    """…"""
    requires = archetype.output_shape.get("requires_constructs") if archetype else None
    if not requires:
        return GateResult(True, "structural")
    sql = proposal.get("example_sql", "")
    # … construct-by-construct check …
```

Replacement:
```python
def check_output_shape(
    proposal: dict,
    *,
    archetype=None,
    proposal: "RepairProposal | None" = None,  # Plan 9 — typed proposal
) -> GateResult:
    """Plan 9 — validator contract source priority:

      1. proposal.required_constructs if non-empty (LLM-emitted)
      2. archetype.output_shape["requires_constructs"] (legacy
         fallback; deleted by T10 once catalog is retired)
      3. None → pass (no contract)
    """
    requires: list[str] | None = None
    if proposal is not None and proposal.required_constructs:
        requires = list(proposal.required_constructs)
    elif archetype is not None:
        requires = archetype.output_shape.get("requires_constructs")
    if not requires:
        return GateResult(True, "structural")
    sql = proposal_dict_or_typed_sql(proposal)  # see helper below
    # … existing construct-by-construct check …
```

Note: the function signature above has a name collision (both the dict arg and the typed kwarg are called `proposal`). Rename the dict arg to `proposal_dict`:

```python
def check_output_shape(
    proposal_dict: dict,
    *,
    archetype=None,
    proposal: "RepairProposal | None" = None,  # Plan 9 — typed
) -> GateResult:
    requires: list[str] | None = None
    if proposal is not None and proposal.required_constructs:
        requires = list(proposal.required_constructs)
    elif archetype is not None:
        requires = archetype.output_shape.get("requires_constructs")
    if not requires:
        return GateResult(True, "structural")
    sql = (
        proposal.patch_body.get("example_sql", "")
        if proposal is not None
        else proposal_dict.get("example_sql", "")
    )
    # … existing construct-by-construct check ported verbatim …
```

Update every caller of `check_output_shape` in `synthesis.py` to pass the typed `proposal=` when available; legacy callers with no typed proposal pass `proposal=None` and continue using the archetype fallback. Use `rg "check_output_shape\(" packages/genie-space-optimizer/src` to find every call site.

- [ ] **Step 9: Run the test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_synthesis_validator_reads_required_constructs.py -v`
Expected: 3 passed.

- [ ] **Step 10: Update the repair-intent-synthesis SKILL.md prompt**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/skills/repair-intent-synthesis/SKILL.md`. Add a new `<required_constructs>` section after `<target_objects>`:

```markdown
<required_constructs>

For repairs whose `patch_body` produces SQL (example SQLs, SQL snippets, expressions), emit a `required_constructs` array naming the SQL clauses your output MUST contain. The deterministic validator checks the generated SQL against this list and rejects any proposal whose SQL is missing a required clause.

Use uppercase clause names from this vocabulary: `SELECT`, `FROM`, `WHERE`, `GROUP_BY`, `ORDER_BY`, `LIMIT`, `JOIN`, `WINDOW`, `HAVING`, `CASE`.

For patch types that do not produce SQL (e.g. `add_instruction`, `add_column_description`), leave `required_constructs` as `[]`.

Examples:

For a top-N example SQL:
```
"required_constructs": ["SELECT", "GROUP_BY", "ORDER_BY", "LIMIT"]
```

For a free-form structural rewrite using a window function:
```
"required_constructs": ["SELECT", "WINDOW"]
```

For an instruction snippet (no SQL):
```
"required_constructs": []
```
</required_constructs>
```

- [ ] **Step 11: Run the full synthesis test suite to confirm no regression**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_synthesis.py tests/unit/test_repair_intent_synthesizer.py -v`
Expected: all passed.

- [ ] **Step 12: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_proposal_typed.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent_synthesizer.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/synthesis.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/skills/repair-intent-synthesis/SKILL.md \
        packages/genie-space-optimizer/tests/unit/test_repair_proposal_required_constructs.py \
        packages/genie-space-optimizer/tests/unit/test_synthesis_validator_reads_required_constructs.py
git commit -m "$(cat <<'EOF'
plan9(t3): add LLM-emitted required_constructs to RepairProposal

The deterministic output-shape validator reads its contract from
RepairProposal.required_constructs (LLM-emitted) instead of
archetype.output_shape. Falls back to archetype for backward
compat until T10 deletes the catalog.

Pre-req for: T10 catalog deletion.
EOF
)"
```

---

### Task 4: Plumb Plan-5 typed inputs through `generate_proposals_from_strategy` and `_select_lever_5_holistic_path`

**Rationale:** Reviewer's claim #1 fix. The harness call sites at `harness.py:23302-23315` and `harness.py:23410-23427` do not pass `rca_evidence_typed`, `llm_cluster_by_cluster_id`, `ag_id`, or `iteration` to `generate_proposals_from_strategy`. The function signature at `optimizer.py:15958-15972` does not accept them. `_select_lever_5_holistic_path` at `optimizer.py:17539-17545` is called inside the function without them. Plan 8 Task 2 added the kwargs to the downstream `_dispatch_lever_5_split` wrapper but the upstream chain is unwired, so the kwargs always default to `None` and the Plan-5 short-circuit at `optimizer.py:10422` (which checks `if rca_evidence_typed and llm_cluster is not None and ag_id`) never fires on the live path.

**Files:**

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:15958-15972` (signature)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:17539-17545` (`_select_lever_5_holistic_path` call site)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:23302-23315` (Best-of-N call site)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:23410-23427` (single-shot call site)
- Test: `packages/genie-space-optimizer/tests/unit/test_generate_proposals_from_strategy_plan5_kwargs.py`

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_generate_proposals_from_strategy_plan5_kwargs.py`:

```python
"""Plan 9 Task 4 — generate_proposals_from_strategy accepts and
threads Plan-5 typed kwargs.

Verifies the four kwargs (rca_evidence_typed, llm_cluster_by_cluster_id,
ag_id, iteration) are on the function signature, default to None / 0,
and are forwarded into _select_lever_5_holistic_path so the Plan-5
short-circuit can actually fire.
"""
import inspect

from genie_space_optimizer.optimization.optimizer import (
    generate_proposals_from_strategy,
    _select_lever_5_holistic_path,
)


def test_generate_proposals_from_strategy_accepts_plan5_kwargs():
    sig = inspect.signature(generate_proposals_from_strategy)
    assert "rca_evidence_typed" in sig.parameters
    assert "llm_cluster_by_cluster_id" in sig.parameters
    assert "ag_id" in sig.parameters
    assert "iteration" in sig.parameters
    assert sig.parameters["rca_evidence_typed"].default is None
    assert sig.parameters["llm_cluster_by_cluster_id"].default is None
    assert sig.parameters["ag_id"].default is None
    assert sig.parameters["iteration"].default == 0


def test_select_lever_5_holistic_path_accepts_plan5_kwargs():
    sig = inspect.signature(_select_lever_5_holistic_path)
    assert "rca_evidence_typed" in sig.parameters
    assert "llm_cluster_by_cluster_id" in sig.parameters
    assert "ag_id" in sig.parameters
    assert "iteration" in sig.parameters


def test_generate_proposals_forwards_plan5_kwargs_to_select_lever5(monkeypatch):
    """Spy on _select_lever_5_holistic_path; verify the kwargs flow
    through when target_lever=5."""
    captured: dict = {}

    def spy(*args, **kwargs):
        captured.update(kwargs)
        return {
            "instruction_text": "",
            "example_sql_proposals": [],
            "rationale": "",
        }

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.optimizer._select_lever_5_holistic_path",
        spy,
    )

    fake_rca = {"q_001": object()}
    fake_clusters_typed = {"c_001": object()}
    generate_proposals_from_strategy(
        strategy={
            "clusters": [{"cluster_id": "c_001", "root_cause": "x"}],
            "action_groups": [],
        },
        action_group={
            "id": "AG_001", "lever_directives": {},
            "source_cluster_ids": ["c_001"],
        },
        metadata_snapshot={"_rca_evidence_typed": fake_rca},
        target_lever=5,
        rca_evidence_typed=fake_rca,
        llm_cluster_by_cluster_id=fake_clusters_typed,
        ag_id="AG_001",
        iteration=2,
    )

    assert captured.get("rca_evidence_typed") is fake_rca
    assert captured.get("llm_cluster_by_cluster_id") is fake_clusters_typed
    assert captured.get("ag_id") == "AG_001"
    assert captured.get("iteration") == 2
```

- [ ] **Step 2: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_generate_proposals_from_strategy_plan5_kwargs.py -v`
Expected: FAIL — `rca_evidence_typed` not in signature.

- [ ] **Step 3: Extend `generate_proposals_from_strategy` signature**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:15958-15972`. Replace the signature:

Current:
```python
def generate_proposals_from_strategy(
    strategy: dict,
    action_group: dict,
    metadata_snapshot: dict,
    target_lever: int,
    apply_mode: str = APPLY_MODE,
    w: WorkspaceClient | None = None,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    benchmarks: list[dict] | None = None,
    doa_fingerprint_buffer: Any = None,
) -> list[dict]:
```

Replacement:
```python
def generate_proposals_from_strategy(
    strategy: dict,
    action_group: dict,
    metadata_snapshot: dict,
    target_lever: int,
    apply_mode: str = APPLY_MODE,
    w: WorkspaceClient | None = None,
    *,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    benchmarks: list[dict] | None = None,
    doa_fingerprint_buffer: Any = None,
    # Plan 9 Task 4 — thread Plan-5 typed inputs from the harness so
    # the Plan-5 LLM intent short-circuit at _dispatch_lever_5b_for_cluster
    # (optimizer.py:10422) and at _generate_lever6_proposal
    # (optimizer.py:14122) actually fires on the live path.
    rca_evidence_typed: dict | None = None,
    llm_cluster_by_cluster_id: dict | None = None,
    ag_id: str | None = None,
    iteration: int = 0,
) -> list[dict]:
```

- [ ] **Step 4: Forward the kwargs into `_select_lever_5_holistic_path`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:17539-17545`. Find the existing call site:

Current:
```python
        holistic_result = _select_lever_5_holistic_path(
            all_clusters=all_lever5_clusters if all_lever5_clusters else clusters,
            metadata_snapshot=metadata_snapshot,
            lever_changes=lever_changes,
            w=w,
            benchmarks=benchmarks,
        )
```

Replacement:
```python
        holistic_result = _select_lever_5_holistic_path(
            all_clusters=all_lever5_clusters if all_lever5_clusters else clusters,
            metadata_snapshot=metadata_snapshot,
            lever_changes=lever_changes,
            w=w,
            benchmarks=benchmarks,
            # Plan 9 Task 4 — forward Plan-5 typed inputs so the
            # short-circuit at _dispatch_lever_5b_for_cluster fires.
            rca_evidence_typed=rca_evidence_typed,
            llm_cluster_by_cluster_id=llm_cluster_by_cluster_id,
            ag_id=ag_id,
            iteration=iteration,
        )
```

(`_select_lever_5_holistic_path` already accepts these kwargs since Plan 8 Task 2; verify by reading lines 10685-10689 of `optimizer.py`.)

- [ ] **Step 5: Thread Plan-5 kwargs into the L6 short-circuit branch**

Also inside `generate_proposals_from_strategy`, locate the L6 branch (search for `_generate_lever6_proposal` calls). For each such call, add the Plan-5 typed kwargs. The `_generate_lever6_proposal` signature already accepts them since Plan 8 Task 3 (verified at `optimizer.py:14107-14111`).

Use `rg "_generate_lever6_proposal\(" packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py` to find every call site inside `generate_proposals_from_strategy`. For each, derive the `llm_cluster` for the cluster the AG points at:

```python
        # Plan 9 Task 4 — derive per-AG typed cluster from the harness-stamped map.
        _ag_cluster_ids = action_group.get("source_cluster_ids") or []
        _llm_cluster = None
        if llm_cluster_by_cluster_id and _ag_cluster_ids:
            _llm_cluster = llm_cluster_by_cluster_id.get(
                str(_ag_cluster_ids[0])
            )

        proposal = _generate_lever6_proposal(
            cluster, metadata_snapshot,
            strategist_hints=strategist_hints,
            w=w, spark=spark, catalog=catalog,
            gold_schema=gold_schema, warehouse_id=warehouse_id,
            benchmarks=benchmarks, raw_evidence=raw_evidence,
            # Plan 9 Task 4 — Plan-5 typed inputs.
            rca_evidence_typed=rca_evidence_typed,
            llm_cluster=_llm_cluster,
            ag_id=ag_id,
            iteration=iteration,
        )
```

- [ ] **Step 6: Update both harness call sites**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:23302-23315` (Best-of-N loop). Add the four kwargs:

```python
                            _sample = generate_proposals_from_strategy(
                                strategy=strategy,
                                action_group=ag,
                                metadata_snapshot=metadata_snapshot,
                                target_lever=lever_int,
                                apply_mode=apply_mode,
                                w=w,
                                spark=spark,
                                catalog=catalog,
                                gold_schema=schema,
                                warehouse_id=resolve_warehouse_id(""),
                                benchmarks=benchmarks,
                                doa_fingerprint_buffer=_doa_fingerprint_buffer,
                                # Plan 9 Task 4 — thread Plan-5 typed inputs.
                                rca_evidence_typed=metadata_snapshot.get(
                                    "_rca_evidence_typed"
                                ),
                                llm_cluster_by_cluster_id=metadata_snapshot.get(
                                    "_llm_clusters_by_cluster_id"
                                ),
                                ag_id=str(ag_id),
                                iteration=int(iteration_counter),
                            ) or []
```

Edit `harness.py:23410-23427` (single-shot call). Add the same four kwargs:

```python
                    lever_proposals = generate_proposals_from_strategy(
                        strategy=strategy,
                        action_group=ag,
                        metadata_snapshot=metadata_snapshot,
                        target_lever=lever_int,
                        apply_mode=apply_mode,
                        w=w,
                        spark=spark,
                        catalog=catalog,
                        gold_schema=schema,
                        warehouse_id=resolve_warehouse_id(""),
                        benchmarks=benchmarks,
                        doa_fingerprint_buffer=_doa_fingerprint_buffer,
                        # Plan 9 Task 4 — thread Plan-5 typed inputs.
                        rca_evidence_typed=metadata_snapshot.get(
                            "_rca_evidence_typed"
                        ),
                        llm_cluster_by_cluster_id=metadata_snapshot.get(
                            "_llm_clusters_by_cluster_id"
                        ),
                        ag_id=str(ag_id),
                        iteration=int(iteration_counter),
                    )
```

- [ ] **Step 7: Verify `metadata_snapshot["_llm_clusters_by_cluster_id"]` exists**

The Plan-4 LLM clustering stage stamps the typed clusters somewhere on `metadata_snapshot`. Verify the key name. Run: `cd packages/genie-space-optimizer && rg "_llm_clusters_by_cluster_id|llm_cluster_by_cluster_id" src/genie_space_optimizer/optimization/`. If the key is named differently in `stages/clustering.py`, use the actual name in T4 Step 6 above. If no such stamping exists yet, add it in `stages/clustering.py` immediately after `cluster_failures_llm` returns the typed clusters — mirror the existing Plan-8-Task-1 stamping of `_rca_evidence_typed` (`harness.py:16365-16373`).

Reference stamping (insert in `stages/clustering.py` after the `cluster_failures_llm` call returns):

```python
        # Plan 9 — stamp typed clusters on metadata_snapshot so the
        # harness can thread them through generate_proposals_from_strategy.
        metadata_snapshot["_llm_clusters_by_cluster_id"] = {
            str(c.cluster_id): c for c in llm_clusters
        }
```

- [ ] **Step 8: Run the test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_generate_proposals_from_strategy_plan5_kwargs.py -v`
Expected: 3 passed.

- [ ] **Step 9: Run the existing harness + proposal tests to confirm no regression**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_optimizer_generate_proposals_from_strategy.py tests/unit/test_lever_5_holistic_split.py tests/unit/test_dispatch_lever_5b_for_cluster.py -v`
Expected: all passed.

- [ ] **Step 10: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/stages/clustering.py \
        packages/genie-space-optimizer/tests/unit/test_generate_proposals_from_strategy_plan5_kwargs.py
git commit -m "$(cat <<'EOF'
plan9(t4): wire Plan-5 typed inputs through proposal generation

generate_proposals_from_strategy accepts rca_evidence_typed,
llm_cluster_by_cluster_id, ag_id, iteration and forwards them to
_select_lever_5_holistic_path and to every _generate_lever6_proposal
call site inside the function. Both harness call sites (Best-of-N
loop and single-shot) pass the kwargs from metadata_snapshot.

Closes reviewer claim #1: Plan-5 LLM intent short-circuit now
actually receives its activation parameters on the live harness path.

Pre-req: stages/clustering.py stamps _llm_clusters_by_cluster_id
on metadata_snapshot.
EOF
)"
```

---

### Task 5: Plumb Plan-5 typed inputs through `_force_lever6_proposal_for_ag`

**Rationale:** Reviewer's claim #2 fix. `_force_lever6_proposal_for_ag` at `harness.py:3130-3222` accepts `**lever6_kwargs` but the caller at `harness.py:23923-23944` only passes `w, spark, catalog, gold_schema, warehouse_id, benchmarks` — Plan-5 typed inputs are absent. The forced-L6 path goes through `generate_lever6(cluster, metadata_snapshot, **lever6_kwargs)` at `harness.py:3172-3174`, so `_generate_lever6_proposal` never sees `rca_evidence_typed` / `llm_cluster` / `ag_id` and the short-circuit at `optimizer.py:14122` cannot fire. The `lever6_force_llm_declined` marker is therefore misleading — it means the legacy L6 generator returned `None`, not that the Plan-5 LLM intent path was tried.

**Files:**

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:3130-3222` (`_force_lever6_proposal_for_ag` signature + body)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:23923-23944` (`_force_l6_call_for_this_ag` closure)
- Test: `packages/genie-space-optimizer/tests/unit/test_force_lever6_proposal_plan5_kwargs.py`

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_force_lever6_proposal_plan5_kwargs.py`:

```python
"""Plan 9 Task 5 — _force_lever6_proposal_for_ag threads Plan-5
typed inputs into generate_lever6.

Verifies the four kwargs are accepted on _force_lever6_proposal_for_ag
signature and forwarded into the generator call so
_generate_lever6_proposal's Plan-5 short-circuit
(optimizer.py:14122) actually fires when forced.
"""
import inspect

from genie_space_optimizer.optimization.harness import (
    _force_lever6_proposal_for_ag,
)


def test_force_lever6_proposal_for_ag_accepts_plan5_kwargs():
    sig = inspect.signature(_force_lever6_proposal_for_ag)
    assert "rca_evidence_typed" in sig.parameters
    assert "llm_cluster" in sig.parameters
    # iteration + ag_id are already explicit on the signature.
    assert "iteration" in sig.parameters
    assert "ag_id" in sig.parameters


def test_force_lever6_proposal_forwards_plan5_kwargs_to_generator():
    captured: dict = {}

    def fake_generator(cluster, metadata_snapshot, **kwargs):
        captured.update(kwargs)
        return {
            "proposal_id": "p_001",
            "patch_type": "add_sql_snippet_expression",
            "lever": 6,
            "patch_body": {"name": "x", "sql_expression": "1"},
            "provenance": {},
        }

    fake_rca = {"q_001": object()}
    fake_cluster_typed = object()

    proposal = _force_lever6_proposal_for_ag(
        run_id="run_test",
        iteration=3,
        ag_id="AG_001",
        cluster={
            "root_cause": "sql_expression_missing",
            "recommended_levers": [6],
            "cluster_id": "c_001",
        },
        ag_target_qids=("q_001",),
        ag_proposals_so_far=[],
        metadata_snapshot={},
        decision_emit=lambda _: None,
        generate_lever6=fake_generator,
        # Plan 9 Task 5 — typed inputs that must flow through to the generator.
        rca_evidence_typed=fake_rca,
        llm_cluster=fake_cluster_typed,
    )

    assert proposal is not None
    assert captured.get("rca_evidence_typed") is fake_rca
    assert captured.get("llm_cluster") is fake_cluster_typed
    assert captured.get("ag_id") == "AG_001"
    assert captured.get("iteration") == 3
```

- [ ] **Step 2: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_force_lever6_proposal_plan5_kwargs.py -v`
Expected: FAIL — `rca_evidence_typed` not in signature.

- [ ] **Step 3: Extend `_force_lever6_proposal_for_ag` signature**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:3130-3142`. Replace the signature to lift typed inputs from `**lever6_kwargs` to first-class kwargs:

Current:
```python
def _force_lever6_proposal_for_ag(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster: dict,
    ag_target_qids: tuple[str, ...],
    ag_proposals_so_far: list[dict],
    metadata_snapshot: dict,
    decision_emit,
    generate_lever6,
    **lever6_kwargs,
) -> dict | None:
```

Replacement:
```python
def _force_lever6_proposal_for_ag(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster: dict,
    ag_target_qids: tuple[str, ...],
    ag_proposals_so_far: list[dict],
    metadata_snapshot: dict,
    decision_emit,
    generate_lever6,
    # Plan 9 Task 5 — Plan-5 typed inputs lifted from **lever6_kwargs
    # to first-class params so they MUST be passed explicitly and the
    # static type checker catches missing wire-in.
    rca_evidence_typed: dict | None = None,
    llm_cluster: Any = None,
    **lever6_kwargs,
) -> dict | None:
```

Find the generator call (~line 3172-3174):

Current:
```python
    try:
        proposal = generate_lever6(
            cluster, metadata_snapshot, **lever6_kwargs,
        )
```

Replacement:
```python
    try:
        proposal = generate_lever6(
            cluster, metadata_snapshot,
            # Plan 9 Task 5 — forward the typed inputs so
            # _generate_lever6_proposal's Plan-5 short-circuit fires.
            rca_evidence_typed=rca_evidence_typed,
            llm_cluster=llm_cluster,
            ag_id=ag_id,
            iteration=iteration,
            **lever6_kwargs,
        )
```

- [ ] **Step 4: Update the closure at `harness.py:23923-23944`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:23923-23944`. Find `_force_l6_call_for_this_ag`:

Current:
```python
                    def _force_l6_call_for_this_ag():
                        return _force_lever6_proposal_for_ag(
                            run_id=str(run_id),
                            iteration=int(iteration_counter),
                            ag_id=str(ag_id),
                            cluster=dict(_force_cluster),
                            ag_target_qids=_force_target_qids,
                            ag_proposals_so_far=list(all_proposals),
                            metadata_snapshot=metadata_snapshot,
                            decision_emit=lambda _rec: (
                                _current_iter_inputs.setdefault(
                                    "decision_records", []
                                ).append(_rec.to_dict())
                            ),
                            generate_lever6=_generate_lever6_proposal,
                            w=w,
                            spark=spark,
                            catalog=catalog,
                            gold_schema=schema,
                            warehouse_id=resolve_warehouse_id(""),
                            benchmarks=benchmarks,
                        )
```

Replacement:
```python
                    def _force_l6_call_for_this_ag():
                        # Plan 9 Task 5 — derive per-AG typed cluster
                        # from the harness-stamped map.
                        _force_llm_cluster = None
                        _llm_clusters_map = metadata_snapshot.get(
                            "_llm_clusters_by_cluster_id"
                        ) or {}
                        _force_cluster_id = str(
                            _force_cluster.get("cluster_id") or ""
                        )
                        if _force_cluster_id and _llm_clusters_map:
                            _force_llm_cluster = _llm_clusters_map.get(
                                _force_cluster_id
                            )
                        return _force_lever6_proposal_for_ag(
                            run_id=str(run_id),
                            iteration=int(iteration_counter),
                            ag_id=str(ag_id),
                            cluster=dict(_force_cluster),
                            ag_target_qids=_force_target_qids,
                            ag_proposals_so_far=list(all_proposals),
                            metadata_snapshot=metadata_snapshot,
                            decision_emit=lambda _rec: (
                                _current_iter_inputs.setdefault(
                                    "decision_records", []
                                ).append(_rec.to_dict())
                            ),
                            generate_lever6=_generate_lever6_proposal,
                            # Plan 9 Task 5 — typed Plan-5 inputs.
                            rca_evidence_typed=metadata_snapshot.get(
                                "_rca_evidence_typed"
                            ),
                            llm_cluster=_force_llm_cluster,
                            w=w,
                            spark=spark,
                            catalog=catalog,
                            gold_schema=schema,
                            warehouse_id=resolve_warehouse_id(""),
                            benchmarks=benchmarks,
                        )
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_force_lever6_proposal_plan5_kwargs.py -v`
Expected: 2 passed.

- [ ] **Step 6: Run the harness L6 forcing tests to confirm no regression**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_force_lever6_proposal_for_ag.py tests/unit/test_maybe_force_lever6_with_cache.py -v`
Expected: all passed.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/tests/unit/test_force_lever6_proposal_plan5_kwargs.py
git commit -m "$(cat <<'EOF'
plan9(t5): wire Plan-5 typed inputs through forced-L6 path

_force_lever6_proposal_for_ag now accepts rca_evidence_typed +
llm_cluster as first-class params (lifted from **lever6_kwargs)
and forwards them to the generator. The harness closure at
harness.py:23923 passes typed inputs from metadata_snapshot.

Closes reviewer claim #2: the Plan-5 L6 short-circuit at
optimizer.py:14122 now actually receives its activation
parameters on the forced-L6 path. lever6_force_llm_declined
becomes a typed Plan-5 outcome instead of an ambiguous
legacy-generator-returned-None marker.
EOF
)"
```

---

### Task 6: Materialize `RepairProposal.patch_body` directly; legacy generator becomes validator-only

**Rationale:** Reviewer's "materialization decoration" claim. `lever6_intent_dispatch.py:112-125` calls `_generate_lever6_proposal_legacy_body` to produce the actual SQL even when Plan 5 has already synthesized a typed `RepairProposal` with a full `patch_body`. The typed proposal is fully materializable via `RepairProposal.to_proposal_dict()` (existing at `repair_proposal_typed.py:107`), but the dispatcher throws it away. Same shape problem inside `_dispatch_lever_5b_for_cluster` at `optimizer.py:10468-10502`. After Plan 9 Task 6, Plan 5 materializes directly; the legacy generator stops being a second LLM call and is reused as the SQL execution / leakage / schema validator only.

**Files:**

- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_direct_slice_resolver.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever6_intent_dispatch.py:112-139`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:10468-10502` (inside `_dispatch_lever_5b_for_cluster`)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/cluster_driven_synthesis.py:637-720` (bypass `pick_archetype` when typed proposal is present)
- Test: `packages/genie-space-optimizer/tests/unit/test_lever6_intent_dispatch_direct_materialization.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_l5b_dispatch_direct_materialization.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_llm_direct_slice_resolver.py`

- [ ] **Step 1: Write the failing test for the slice resolver**

Create `packages/genie-space-optimizer/tests/unit/test_llm_direct_slice_resolver.py`:

```python
"""Plan 9 Task 6 — llm_direct_slice_resolver.

Resolves a tuple of TargetObjects into a concrete AssetSlice
(tables, metric_view, columns, join_spec) by looking up each
identifier in metadata_snapshot. No archetype dependency.
"""
import pytest

from genie_space_optimizer.optimization.llm_direct_slice_resolver import (
    resolve_target_objects_to_asset_slice,
    UnknownTargetObjectError,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
    TargetObject,
)


def _make_metadata_snapshot():
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.sales.orders",
                    "columns": [
                        {"name": "order_id", "type": "STRING"},
                        {"name": "product_id", "type": "STRING"},
                        {"name": "amount", "type": "DECIMAL"},
                    ],
                },
                {
                    "identifier": "main.sales.products",
                    "columns": [
                        {"name": "id", "type": "STRING"},
                        {"name": "name", "type": "STRING"},
                    ],
                },
            ],
            "metric_views": [
                {
                    "identifier": "main.sales.daily_orders_mv",
                    "columns": [
                        {"name": "order_count_total", "type": "BIGINT"},
                    ],
                },
            ],
        },
        "instructions": {
            "join_specs": [
                {
                    "left": {"identifier": "main.sales.orders"},
                    "right": {"identifier": "main.sales.products"},
                    "on": "orders.product_id = products.id",
                },
            ],
        },
    }


def test_resolve_single_table_target():
    targets = (
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="main.sales.orders",
            columns=("product_id", "amount"),
        ),
    )
    slice_ = resolve_target_objects_to_asset_slice(
        targets, _make_metadata_snapshot(),
    )
    assert len(slice_.tables) == 1
    assert slice_.tables[0]["identifier"] == "main.sales.orders"
    assert slice_.metric_view is None
    assert slice_.join_spec is None
    assert any(c["name"] == "product_id" for c in slice_.columns)


def test_resolve_two_tables_finds_join_spec():
    targets = (
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="main.sales.orders",
            columns=("product_id",),
        ),
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="main.sales.products",
            columns=("id", "name"),
        ),
    )
    slice_ = resolve_target_objects_to_asset_slice(
        targets, _make_metadata_snapshot(),
    )
    assert len(slice_.tables) == 2
    assert slice_.join_spec is not None
    assert slice_.join_spec["on"] == "orders.product_id = products.id"


def test_resolve_metric_view_target():
    targets = (
        TargetObject(
            asset_kind=AssetKind.METRIC_VIEW,
            identifier="main.sales.daily_orders_mv",
            columns=("order_count_total",),
        ),
    )
    slice_ = resolve_target_objects_to_asset_slice(
        targets, _make_metadata_snapshot(),
    )
    assert slice_.metric_view is not None
    assert slice_.metric_view["identifier"] == "main.sales.daily_orders_mv"


def test_resolve_unknown_table_raises():
    targets = (
        TargetObject(
            asset_kind=AssetKind.TABLE,
            identifier="main.sales.does_not_exist",
            columns=(),
        ),
    )
    with pytest.raises(UnknownTargetObjectError):
        resolve_target_objects_to_asset_slice(
            targets, _make_metadata_snapshot(),
        )


def test_resolve_empty_targets_returns_empty_slice():
    """When LLM emits no target_objects (e.g. instruction-only
    repair), resolver returns an empty AssetSlice rather than
    raising. The caller decides whether the empty slice is OK."""
    slice_ = resolve_target_objects_to_asset_slice(
        (), _make_metadata_snapshot(),
    )
    assert slice_.tables == []
    assert slice_.metric_view is None
    assert slice_.columns == []
    assert slice_.join_spec is None
```

- [ ] **Step 2: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_llm_direct_slice_resolver.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Create the slice resolver**

Create `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_direct_slice_resolver.py`:

```python
"""Plan 9 Task 6 — LLM-direct slice resolver.

Resolves a tuple of TargetObjects (LLM-emitted) into a concrete
AssetSlice (tables, metric_view, columns, join_spec) by looking up
each identifier in metadata_snapshot. Replaces the archetype-driven
slice derivation in _derive_asset_slice_from_afs at
cluster_driven_synthesis.py:637-720.

Pure function. No LLM call. No archetype dependency.
"""
from __future__ import annotations

from typing import Any

from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    AssetSlice,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
    TargetObject,
)


class UnknownTargetObjectError(KeyError):
    """Raised when a TargetObject identifier does not appear in
    metadata_snapshot. The LLM must only emit identifiers it was
    shown in the schema; this error catches synthesizer drift."""


def _find_table(
    identifier: str, metadata_snapshot: dict,
) -> dict | None:
    ds = metadata_snapshot.get("data_sources", {}) or {}
    norm = identifier.strip().lower()
    for t in ds.get("tables", []) or []:
        if str(t.get("identifier", "")).strip().lower() == norm:
            return t
    return None


def _find_metric_view(
    identifier: str, metadata_snapshot: dict,
) -> dict | None:
    ds = metadata_snapshot.get("data_sources", {}) or {}
    norm = identifier.strip().lower()
    for mv in ds.get("metric_views", []) or []:
        if str(mv.get("identifier", "")).strip().lower() == norm:
            return mv
    return None


def _find_join_spec(
    left_id: str, right_id: str, metadata_snapshot: dict,
) -> dict | None:
    instructions = metadata_snapshot.get("instructions", {}) or {}
    js_list = instructions.get("join_specs") or []
    want = {left_id.strip().lower(), right_id.strip().lower()}
    for js in js_list:
        left = (js.get("left") or {}).get("identifier", "").strip().lower()
        right = (js.get("right") or {}).get("identifier", "").strip().lower()
        if {left, right} == want:
            return js
    return None


def resolve_target_objects_to_asset_slice(
    targets: tuple[TargetObject, ...],
    metadata_snapshot: dict,
) -> AssetSlice:
    """Build an AssetSlice from the LLM-emitted target_objects.

    Empty targets return an empty AssetSlice (caller decides).

    Raises UnknownTargetObjectError if any identifier is not in
    metadata_snapshot — the LLM must ground every identifier in
    the schema it was shown.
    """
    if not targets:
        return AssetSlice(
            tables=[], metric_view=None, columns=[], join_spec=None,
        )

    tables: list[dict] = []
    metric_view: dict | None = None
    columns: list[dict] = []

    for t in targets:
        if t.asset_kind == AssetKind.TABLE:
            tbl = _find_table(t.identifier, metadata_snapshot)
            if tbl is None:
                raise UnknownTargetObjectError(
                    f"TargetObject (TABLE) identifier {t.identifier!r} "
                    f"not in metadata_snapshot. LLM emitted an "
                    f"identifier not present in the schema it was shown."
                )
            tables.append(tbl)
            # Project the LLM-named columns into the slice.
            if t.columns:
                tbl_cols = {
                    str(c.get("name", "")): c
                    for c in (tbl.get("columns") or [])
                }
                for col_name in t.columns:
                    if col_name in tbl_cols:
                        columns.append(tbl_cols[col_name])
        elif t.asset_kind == AssetKind.METRIC_VIEW:
            mv = _find_metric_view(t.identifier, metadata_snapshot)
            if mv is None:
                raise UnknownTargetObjectError(
                    f"TargetObject (METRIC_VIEW) identifier "
                    f"{t.identifier!r} not in metadata_snapshot."
                )
            if metric_view is None:
                metric_view = mv
        elif t.asset_kind == AssetKind.COLUMN:
            # COLUMN-kind: identifier is catalog.schema.table.column.
            # Split off the trailing column name; look up the table.
            parts = t.identifier.rsplit(".", 1)
            if len(parts) != 2:
                raise UnknownTargetObjectError(
                    f"TargetObject (COLUMN) identifier {t.identifier!r} "
                    f"must be 'catalog.schema.table.column' shape."
                )
            tbl_id, col_name = parts
            tbl = _find_table(tbl_id, metadata_snapshot)
            if tbl is None:
                raise UnknownTargetObjectError(
                    f"TargetObject (COLUMN): parent table {tbl_id!r} "
                    f"not in metadata_snapshot for {t.identifier!r}."
                )
            tbl_cols = {
                str(c.get("name", "")): c
                for c in (tbl.get("columns") or [])
            }
            if col_name not in tbl_cols:
                raise UnknownTargetObjectError(
                    f"TargetObject (COLUMN) column {col_name!r} "
                    f"not in table {tbl_id!r}."
                )
            columns.append(tbl_cols[col_name])

    join_spec = None
    if len(tables) >= 2:
        left = (tables[0].get("identifier") or "")
        right = (tables[1].get("identifier") or "")
        join_spec = _find_join_spec(left, right, metadata_snapshot)

    return AssetSlice(
        tables=tables,
        metric_view=metric_view,
        columns=columns,
        join_spec=join_spec,
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_llm_direct_slice_resolver.py -v`
Expected: 5 passed.

- [ ] **Step 5: Write the failing test for L6 direct materialization**

Create `packages/genie-space-optimizer/tests/unit/test_lever6_intent_dispatch_direct_materialization.py`:

```python
"""Plan 9 Task 6 — dispatch_lever_6_with_intent materializes
RepairProposal.patch_body directly via to_proposal_dict() instead
of delegating to _generate_lever6_proposal_legacy_body for SQL.

Legacy body is reused only as the deterministic validator
(SQL execution + leakage + schema invariants); it is not invoked
to RE-GENERATE SQL the LLM already produced.
"""
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.lever6_intent_dispatch import (
    dispatch_lever_6_with_intent,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
    TargetObject,
)


def _make_synthesizer_proposal():
    return RepairProposal(
        intent_id="intent_h001_001",
        intent_name="add_revenue_expression",
        intent_description="Add a revenue SQL expression.",
        repair_shape=RepairShape.SQL_EXPRESSION,
        patch_type=PatchType.ADD_SQL_SNIPPET_EXPRESSION,
        rationale="Cluster blames missing revenue computation.",
        confidence="high",
        patch_body={
            "name": "revenue_per_order",
            "sql_expression": "amount * quantity",
        },
        blame_set=("main.sales.orders",),
        target_objects=(
            TargetObject(
                asset_kind=AssetKind.TABLE,
                identifier="main.sales.orders",
                columns=("amount", "quantity"),
            ),
        ),
        required_constructs=(),  # snippet, not full SELECT
    )


def test_dispatch_uses_to_proposal_dict_when_proposal_validates(monkeypatch):
    """When patch_body validates against the deterministic checks,
    the dispatcher uses RepairProposal.to_proposal_dict() and does
    NOT call _generate_lever6_proposal_legacy."""
    fake_proposal = _make_synthesizer_proposal()

    def fake_synthesize(**kwargs):
        return fake_proposal

    legacy_calls = []

    def fake_legacy(*args, **kwargs):
        legacy_calls.append(kwargs)
        return None  # If called, the test should fail.

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "synthesize_repair_intent_for_cluster",
        fake_synthesize,
    )
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "_generate_lever6_proposal_legacy",
        fake_legacy,
    )

    fake_cluster = {"cluster_id": "c_h001", "blame_set": ["main.sales.orders"]}
    fake_metadata = {
        "instructions": {"example_question_sqls": []},
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.sales.orders",
                    "columns": [
                        {"name": "amount"},
                        {"name": "quantity"},
                    ],
                },
            ],
        },
    }

    result = dispatch_lever_6_with_intent(
        cluster=fake_cluster,
        metadata_snapshot=fake_metadata,
        w=None,
        rca_evidence_typed={"q": object()},
        llm_cluster=object(),
        ag_id="AG_H001",
        iteration=1,
    )

    assert result is not None
    assert result["patch_type"] == "add_sql_snippet_expression"
    assert result["patch_body"]["sql_expression"] == "amount * quantity"
    # Critical: legacy generator was NOT invoked for SQL generation.
    assert len(legacy_calls) == 0


def test_dispatch_falls_back_to_legacy_when_proposal_validation_fails(monkeypatch):
    """If RepairProposal.to_proposal_dict() raises (missing required
    patch_body field), the dispatcher falls back to the legacy body
    so the cycle does not crash. The fallback is a SAFETY NET, not
    the primary path."""
    bad_proposal = RepairProposal(
        intent_id="intent_bad",
        intent_name="x",
        intent_description="…",
        repair_shape=RepairShape.SQL_EXPRESSION,
        patch_type=PatchType.ADD_SQL_SNIPPET_EXPRESSION,
        rationale="…",
        confidence="high",
        patch_body={},  # Missing required "name" + "sql_expression"
        blame_set=(),
    )

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "synthesize_repair_intent_for_cluster",
        lambda **kwargs: bad_proposal,
    )

    legacy_called = []
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "_generate_lever6_proposal_legacy",
        lambda **kwargs: legacy_called.append(1) or {
            "patch_type": "add_sql_snippet_expression",
            "patch_body": {"name": "x", "sql_expression": "1"},
            "lever": 6,
        },
    )

    result = dispatch_lever_6_with_intent(
        cluster={"cluster_id": "c", "blame_set": []},
        metadata_snapshot={"instructions": {"example_question_sqls": []}},
        w=None,
        rca_evidence_typed={"q": object()},
        llm_cluster=object(),
        ag_id="AG_X",
        iteration=1,
    )

    assert result is not None
    assert len(legacy_called) == 1
```

- [ ] **Step 6: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_lever6_intent_dispatch_direct_materialization.py -v`
Expected: FAIL — the dispatcher currently always calls `_generate_lever6_proposal_legacy`.

- [ ] **Step 7: Rewrite the materialization block in `dispatch_lever_6_with_intent`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever6_intent_dispatch.py`. Replace lines 107-139:

Current:
```python
    if proposal is None:
        return None
    if proposal.patch_type not in _L6_PATCH_TYPES:
        return None

    proposal_dict = _generate_lever6_proposal_legacy(
        cluster=cluster,
        metadata_snapshot=metadata_snapshot,
        strategist_hints=strategist_hints,
        w=w,
        spark=spark,
        catalog=catalog,
        gold_schema=gold_schema,
        warehouse_id=warehouse_id,
        benchmarks=benchmarks,
        raw_evidence=raw_evidence,
    )
    if proposal_dict is None:
        return None

    fc = FailureCluster.from_legacy(cluster)
    intent = proposal.to_repair_intent(cluster=fc, ag_id=ag_id)
    stamp_repair_intent_on_proposal(proposal_dict, intent)
    logger.info(
        "plan5_l6.intent_dispatch cluster_id=%s ag_id=%s intent_id=%s "
        ...
    )
```

Replacement:
```python
    if proposal is None:
        return None
    if proposal.patch_type not in _L6_PATCH_TYPES:
        return None

    # Plan 9 Task 6 — materialize RepairProposal.patch_body directly
    # via to_proposal_dict(). The legacy generator is a SAFETY NET,
    # invoked only when to_proposal_dict() raises (missing required
    # patch_body field) — not as the primary SQL materializer.
    proposal_dict: dict | None = None
    try:
        proposal_dict = proposal.to_proposal_dict()
    except Exception as exc:
        logger.warning(
            "plan9.l6_direct_materialization_failed intent_id=%s err=%s "
            "— falling back to legacy generator (safety net).",
            proposal.intent_id, exc,
        )
        proposal_dict = _generate_lever6_proposal_legacy(
            cluster=cluster,
            metadata_snapshot=metadata_snapshot,
            strategist_hints=strategist_hints,
            w=w,
            spark=spark,
            catalog=catalog,
            gold_schema=gold_schema,
            warehouse_id=warehouse_id,
            benchmarks=benchmarks,
            raw_evidence=raw_evidence,
        )
        if proposal_dict is None:
            return None

    # to_proposal_dict() returns a dict-shaped proposal but does
    # NOT run SQL execution / leakage firewall / schema invariant
    # checks. Invoke the legacy body's VALIDATOR pass on the
    # already-materialized dict.
    try:
        from genie_space_optimizer.optimization.optimizer import (
            _validate_l6_proposal_dict,
        )
        _validate_l6_proposal_dict(
            proposal_dict,
            cluster=cluster,
            metadata_snapshot=metadata_snapshot,
            w=w, spark=spark, catalog=catalog,
            gold_schema=gold_schema, warehouse_id=warehouse_id,
            benchmarks=benchmarks,
        )
    except Exception as exc:
        logger.warning(
            "plan9.l6_direct_validation_failed intent_id=%s err=%s",
            proposal.intent_id, exc,
        )
        return None

    fc = FailureCluster.from_legacy(cluster)
    intent = proposal.to_repair_intent(cluster=fc, ag_id=ag_id)
    stamp_repair_intent_on_proposal(proposal_dict, intent)
    logger.info(
        "plan9.l6_direct_materialized intent_id=%s cluster_id=%s ag_id=%s",
        proposal.intent_id, cluster.get("cluster_id"), ag_id,
    )
    return proposal_dict
```

- [ ] **Step 8: Extract the validator-only path from `_generate_lever6_proposal_legacy_body`**

Add a new helper `_validate_l6_proposal_dict` in `optimizer.py` next to `_generate_lever6_proposal_legacy_body` (the existing legacy body is at `optimizer.py:14145-end`). The helper does the same SQL execution + leakage + schema invariant checks but against an already-materialized `proposal_dict` instead of generating one from scratch.

Extract the validator section (post-LLM-call) from `_generate_lever6_proposal_legacy_body` into the new helper. Use `rg "def _generate_lever6_proposal_legacy_body" packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py` to find the function body. The validator section is the part AFTER the LLM call returns a parsed dict but BEFORE the function returns.

Pseudocode (consult the existing function for exact validator steps):

```python
def _validate_l6_proposal_dict(
    proposal_dict: dict,
    *,
    cluster: dict,
    metadata_snapshot: dict,
    w: WorkspaceClient | None = None,
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    benchmarks: list[dict] | None = None,
) -> None:
    """Plan 9 Task 6 — validator-only extraction from the legacy
    L6 body. Raises if proposal_dict fails any deterministic check:
      * SQL syntax + execution
      * leakage firewall (n-gram against benchmarks)
      * schema invariants (column existence, type compatibility)
      * patch_body required fields per patch_type

    Used by dispatch_lever_6_with_intent after to_proposal_dict()
    materializes a typed proposal. Replaces the second LLM call
    that the legacy body would have made.
    """
    # … port validator steps from _generate_lever6_proposal_legacy_body …
```

- [ ] **Step 9: Run the L6 test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_lever6_intent_dispatch_direct_materialization.py -v`
Expected: 2 passed.

- [ ] **Step 10: Write the failing test for L5b direct materialization**

Create `packages/genie-space-optimizer/tests/unit/test_l5b_dispatch_direct_materialization.py`:

```python
"""Plan 9 Task 6 — _dispatch_lever_5b_for_cluster materializes
RepairProposal.patch_body directly via to_proposal_dict() instead
of calling the archetype-gated cluster_driven_synthesis.

Mirror of test_lever6_intent_dispatch_direct_materialization for
the L5b path.
"""
from genie_space_optimizer.optimization.optimizer import (
    _dispatch_lever_5b_for_cluster,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
    TargetObject,
)


def test_l5b_uses_to_proposal_dict_for_add_example_sql(monkeypatch):
    """When Plan 5 synthesizes a typed RepairProposal with
    patch_type=ADD_EXAMPLE_SQL, the dispatcher materializes the
    proposal_dict directly without calling pick_archetype /
    cluster_driven_synthesis."""
    typed = RepairProposal(
        intent_id="intent_top_n",
        intent_name="top_n_revenue_by_product",
        intent_description="Top-N revenue by product.",
        repair_shape=RepairShape.TOP_N_BY_METRIC,
        patch_type=PatchType.ADD_EXAMPLE_SQL,
        rationale="…",
        confidence="high",
        patch_body={
            "example_question": "What are the top 5 products by revenue?",
            "example_sql": (
                "SELECT product, SUM(amount) AS revenue "
                "FROM main.sales.orders "
                "GROUP BY product "
                "ORDER BY revenue DESC LIMIT 5"
            ),
        },
        blame_set=("main.sales.orders",),
        target_objects=(
            TargetObject(
                asset_kind=AssetKind.TABLE,
                identifier="main.sales.orders",
                columns=("product", "amount"),
            ),
        ),
        required_constructs=("SELECT", "GROUP_BY", "ORDER_BY", "LIMIT"),
    )

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.optimizer."
        "synthesize_repair_intent_for_cluster",
        lambda **kwargs: typed,
    )

    pick_archetype_calls = []

    def spy_pick_archetype(*args, **kwargs):
        pick_archetype_calls.append(1)
        return None

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.cluster_driven_synthesis.pick_archetype",
        spy_pick_archetype,
    )

    fake_cluster = {
        "cluster_id": "c_h001",
        "root_cause": "plural_top_n_collapse",
        "blame_set": ["main.sales.orders"],
        "affected_qids": ["q_001"],
    }
    fake_metadata = {
        "instructions": {"example_question_sqls": []},
        "data_sources": {
            "tables": [{
                "identifier": "main.sales.orders",
                "columns": [
                    {"name": "product"},
                    {"name": "amount"},
                ],
            }],
        },
        "schema_columns": ["product", "amount"],
    }

    proposals = _dispatch_lever_5b_for_cluster(
        cluster=fake_cluster,
        metadata_snapshot=fake_metadata,
        w=None,
        benchmark_corpus=None,
        benchmarks=None,
        rca_evidence_typed={"q_001": type("E", (), {"blame_set": set()})()},
        llm_cluster=object(),
        ag_id="AG_H001",
        iteration=1,
    )

    assert len(proposals) == 1
    assert proposals[0]["patch_type"] == "add_example_sql"
    assert "ORDER BY revenue DESC" in proposals[0]["patch_body"]["example_sql"]
    # Critical: pick_archetype was NOT invoked when typed proposal
    # materialized successfully.
    assert len(pick_archetype_calls) == 0
```

- [ ] **Step 11: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_l5b_dispatch_direct_materialization.py -v`
Expected: FAIL — `_dispatch_lever_5b_for_cluster` still routes through `cluster_driven_synthesis` which calls `pick_archetype`.

- [ ] **Step 12: Modify the L5b dispatcher to materialize directly**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:10468-10502`. Find the Plan-5 short-circuit body inside `_dispatch_lever_5b_for_cluster` (the section after `proposal = synthesize_repair_intent_for_cluster(...)`):

Current:
```python
        if proposal is not None:
            routed = route_to_per_lever_generator(proposal)
            if routed is not None:
                generator, override_event = routed
                proposal_dict = generator(proposal)
                fc = FailureCluster.from_legacy(cluster)
                intent = proposal.to_repair_intent(cluster=fc, ag_id=ag_id)
                stamp_repair_intent_on_proposal(proposal_dict, intent)
                if override_event is not None:
                    proposal_dict["cross_lever_override"] = (
                        override_event.to_dict()
                    )
                # … log + return [proposal_dict] …
```

Replacement:
```python
        if proposal is not None:
            # Plan 9 Task 6 — materialize patch_body directly via
            # to_proposal_dict(). Bypass cluster_driven_synthesis
            # (and its pick_archetype call) entirely. The cross-lever
            # router is still consulted for override events.
            try:
                proposal_dict = proposal.to_proposal_dict()
            except Exception as exc:
                logger.warning(
                    "plan9.l5b_direct_materialization_failed "
                    "intent_id=%s err=%s — falling back to legacy "
                    "cluster_driven_synthesis.",
                    proposal.intent_id, exc,
                )
                proposal_dict = None

            if proposal_dict is not None:
                routed = route_to_per_lever_generator(proposal)
                _override_event = routed[1] if routed else None
                fc = FailureCluster.from_legacy(cluster)
                intent = proposal.to_repair_intent(cluster=fc, ag_id=ag_id)
                stamp_repair_intent_on_proposal(proposal_dict, intent)
                if _override_event is not None:
                    proposal_dict["cross_lever_override"] = (
                        _override_event.to_dict()
                    )
                    logger.info(
                        "plan9.l5b_cross_lever_override intent_id=%s "
                        "from=%s to=%s",
                        _override_event.intent_id,
                        _override_event.from_lever,
                        _override_event.to_lever,
                    )
                logger.info(
                    "plan9.l5b_direct_materialized intent_id=%s "
                    "cluster_id=%s ag_id=%s patch_type=%s",
                    proposal.intent_id,
                    cluster.get("cluster_id"),
                    ag_id,
                    proposal.patch_type.value,
                )
                return [proposal_dict]
            # else: fall through to the legacy path below as a
            # safety net. This branch should be rare; Plan 9
            # telemetry tracks via PLAN5_ANCHOR_ACTIVATION_V1 marker
            # status plan5_intent_validator_rejected.
```

- [ ] **Step 13: Modify `cluster_driven_synthesis._derive_asset_slice_from_afs` to accept a typed proposal**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/cluster_driven_synthesis.py:637-720`. Replace the signature + body to bypass `pick_archetype` when a typed `repair_proposal` is provided:

Current:
```python
def _derive_asset_slice_from_afs(
    afs: dict,
    metadata_snapshot: dict,
    *,
    column_k: int = PREFLIGHT_COLUMN_COVERAGE_K,
) -> tuple[AssetSlice, Archetype] | None:
    """…"""
    archetype = pick_archetype(afs, metadata_snapshot)
    if archetype is None:
        return None
    # … existing body builds AssetSlice from archetype + blame_set …
```

Replacement:
```python
def _derive_asset_slice_from_afs(
    afs: dict,
    metadata_snapshot: dict,
    *,
    column_k: int = PREFLIGHT_COLUMN_COVERAGE_K,
    repair_proposal: "RepairProposal | None" = None,  # Plan 9
) -> tuple[AssetSlice, "Archetype | None"] | None:
    """Plan 9 — when ``repair_proposal`` is provided AND carries
    non-empty target_objects, build the AssetSlice from the
    LLM-emitted target_objects (via llm_direct_slice_resolver) and
    return ``(slice, None)`` — no archetype required.

    Falls back to the pre-Plan-9 archetype-driven path when
    repair_proposal is None or has empty target_objects (legacy
    pre-Plan-9 callers; deleted entirely by T10).
    """
    if repair_proposal is not None and repair_proposal.target_objects:
        from genie_space_optimizer.optimization.llm_direct_slice_resolver import (
            resolve_target_objects_to_asset_slice,
            UnknownTargetObjectError,
        )
        try:
            slice_ = resolve_target_objects_to_asset_slice(
                repair_proposal.target_objects, metadata_snapshot,
            )
            return (slice_, None)  # No archetype; Plan 9 path.
        except UnknownTargetObjectError as exc:
            logger.warning(
                "plan9.l5b_target_objects_resolution_failed err=%s "
                "— falling back to archetype path.",
                exc,
            )

    # Legacy archetype-driven path (deleted by T10).
    archetype = pick_archetype(afs, metadata_snapshot)
    if archetype is None:
        return None
    # … existing body kept verbatim …
```

Update every caller of `_derive_asset_slice_from_afs` in `cluster_driven_synthesis.py` to thread `repair_proposal=` when one is available. Use `rg "_derive_asset_slice_from_afs\(" packages/genie-space-optimizer/src` to find every call site.

- [ ] **Step 14: Update downstream branches in `cluster_driven_synthesis.synthesize_for_cluster` that depend on the returned archetype**

The pre-Plan-9 caller expected `(AssetSlice, Archetype)` and used `archetype.prompt_template` and `archetype.output_shape` downstream. After Plan 9, `archetype` may be `None`. Wrap every `archetype.` access in a `None` check; when `None`, use `repair_proposal.repair_shape` for the prompt fragment (T2) and `repair_proposal.required_constructs` for the validator (T3):

Pseudocode for the affected section:
```python
        slice_, archetype = result  # archetype may be None in Plan 9 path
        if archetype is None and repair_proposal is not None:
            shape_fragment = fragment_for(repair_proposal.repair_shape)
            output_constructs = list(repair_proposal.required_constructs)
        elif archetype is not None:
            shape_fragment = archetype.prompt_template
            output_constructs = archetype.output_shape.get(
                "requires_constructs", []
            )
        else:
            # No archetype, no typed proposal — emit the typed decline.
            return ClusterSynthesisResult(
                proposal=None,
                attempted_archetypes=(),
                skipped_reason="no_archetype_and_no_typed_proposal",
            )
```

(T10 removes the `elif archetype is not None` branch.)

- [ ] **Step 15: Run the L5b test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_l5b_dispatch_direct_materialization.py -v`
Expected: 1 passed.

- [ ] **Step 16: Run the full L5b + L6 integration test suites to confirm no regression**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_dispatch_lever_5b_for_cluster.py tests/unit/test_lever6_intent_dispatch.py tests/unit/test_cluster_driven_synthesis.py -v`
Expected: all passed.

- [ ] **Step 17: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/llm_direct_slice_resolver.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever6_intent_dispatch.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/cluster_driven_synthesis.py \
        packages/genie-space-optimizer/tests/unit/test_llm_direct_slice_resolver.py \
        packages/genie-space-optimizer/tests/unit/test_lever6_intent_dispatch_direct_materialization.py \
        packages/genie-space-optimizer/tests/unit/test_l5b_dispatch_direct_materialization.py
git commit -m "$(cat <<'EOF'
plan9(t6): materialize RepairProposal.patch_body directly

Both L5b and L6 dispatchers now route through
RepairProposal.to_proposal_dict() when a typed proposal is
available. The legacy generators become validator-only safety
nets, invoked only when to_proposal_dict() raises.

cluster_driven_synthesis._derive_asset_slice_from_afs accepts an
optional repair_proposal kwarg; when target_objects is non-empty,
the slice is built via llm_direct_slice_resolver and the
archetype is None. Closes the materialization-decoration loop:
LLM-emitted patch_body now reaches the applier instead of being
discarded.

New: llm_direct_slice_resolver, _validate_l6_proposal_dict
(extracted from legacy body).

Pre-req for: T10 catalog deletion (archetype now optional, the
deletion is a simple remove-the-fallback-branch change).
EOF
)"
```

---

### Task 6.1: Finalize `ADD_SQL_SNIPPET_*` proposal dicts (validators + applier-expected shape)

**Status when this task was added:** Post-T6 review (commit ba5d4ced). T6 deferred validator extraction as a quality improvement. Audit revealed it's a deploy-blocker for L6 direct materialization — see "Why T6.1 exists" below.

**Why T6.1 exists (read before starting):**

`RepairProposal.to_proposal_dict()` for the three `ADD_SQL_SNIPPET_*` patch types returns a flat shape `{name, sql_expression, usage_guidance}` (`repair_proposal_typed.py:173-179`). The applier expects two things that this shape doesn't carry:

1. **Nested `sql_snippet` object with `id`** (`applier.py:3162-3163`) — `_render_patch` reads `patch.get("sql_snippet", {})` and `snippet.get("id", "")`. Flat shape → empty snippet ID.
2. **`validation_passed=True` stamp** (`applier.py:3171-3181`) — Tier-2.8 hard assertion raises `RuntimeError` and the patch is dropped into `early_dropped_patches`. The legacy body sets this only after `validate_sql_snippet` returns a clean `EXPLAIN+execute` result (`optimizer.py:14358-14467`).

Net result without T6.1: every Plan-9 direct L6 patch (which is the only `RepairProposal` shape that survives T5's wire-in for `_force_lever6_proposal_for_ag`) is dropped at the applier. Postmortem shows `plan9_materialization_source=plan9_direct` firing, but zero patches land. Strictly worse than the pre-Plan-9 inertness Plan 9 was meant to fix.

`ADD_EXAMPLE_SQL` does NOT need this finalizer — the applier has no equivalent `validation_passed` requirement for it, and synthesis.py's gate runs upstream. `ADD_INSTRUCTION`, `UPDATE_INSTRUCTION`, `ADD_JOIN_SPEC`, `ADD_COLUMN_DESCRIPTION` likewise need no finalizer.

**Scope:** ONE module + ONE helper + TWO dispatcher call sites + FOUR tests. Lift the validator + proposal-dict-builder block verbatim from `_generate_lever6_proposal_legacy_body:14323-14468`. Do NOT broaden scope (don't refactor the legacy body, don't touch the safety-net fallback path).

**Files:**

- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/sql_snippet_finalizer.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever6_intent_dispatch.py:107-160` (insert finalizer call after `to_proposal_dict()` succeeds)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:10486-10555` (insert finalizer call in `_dispatch_lever_5b_for_cluster` after `to_proposal_dict()` succeeds)
- Test: `packages/genie-space-optimizer/tests/unit/test_sql_snippet_finalizer.py` (new)
- Test: `packages/genie-space-optimizer/tests/unit/test_lever6_intent_dispatch_finalizer_integration.py` (new)

- [ ] **Step 1: Write the failing finalizer unit test**

Create `packages/genie-space-optimizer/tests/unit/test_sql_snippet_finalizer.py`:

```python
"""Plan 9 Task 6.1 — sql_snippet_finalizer tests.

The finalizer must:
  1. Wrap the flat to_proposal_dict() output into the nested
     sql_snippet shape the applier reads at applier.py:3162.
  2. Run _validate_sql_identifiers against the metadata allowlist;
     return None on failure (caller treats as decline).
  3. When (w, warehouse_id) are provided, run validate_sql_snippet
     and stamp validation_passed accordingly; otherwise stamp
     validation_passed=False (the applier-gate will drop it, which
     is the correct safe default for no-backend dev paths).
  4. Fabricate missing applier fields from the RepairProposal:
     snippet_type from patch_type, display_name from intent_name,
     target_table from first TABLE in target_objects, rationale
     from RepairProposal.rationale, etc.
"""
from __future__ import annotations

from genie_space_optimizer.optimization.repair_intent import (
    PatchType, RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.sql_snippet_finalizer import (
    finalize_sql_snippet_proposal_dict,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind, TargetObject,
)


def _make_proposal(patch_type: PatchType = PatchType.ADD_SQL_SNIPPET_MEASURE) -> RepairProposal:
    return RepairProposal(
        intent_id="abc12345",
        intent_name="Total revenue measure",
        intent_description="Add a measure for total revenue.",
        repair_shape=RepairShape.AGGREGATION,
        patch_type=patch_type,
        rationale="Cluster lacks aggregation primitive.",
        confidence=0.8,
        patch_body={
            "name": "total_revenue",
            "sql_expression": "SUM(orders.revenue)",
            "usage_guidance": "Use to compute total revenue across orders.",
        },
        blame_set=("main.sales.orders",),
        target_objects=(
            TargetObject(
                asset_kind=AssetKind.TABLE,
                identifier="main.sales.orders",
                columns=("revenue",),
            ),
        ),
        required_constructs=(),
    )


def _make_metadata_snapshot() -> dict:
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.sales.orders",
                    "columns": [
                        {"name": "revenue", "type": "double"},
                        {"name": "order_id", "type": "string"},
                    ],
                }
            ],
        },
        "sql_snippets": {"measures": []},
    }


def _make_cluster() -> dict:
    return {
        "cluster_id": "C1",
        "root_cause": "missing_measure",
        "question_ids": ["q1", "q2"],
        "question_traces": [{"qid": "q1"}, {"qid": "q2"}],
    }


def test_finalizer_returns_nested_sql_snippet_shape():
    proposal = _make_proposal()
    base_dict = proposal.to_proposal_dict()
    out = finalize_sql_snippet_proposal_dict(
        proposal,
        base_dict,
        cluster=_make_cluster(),
        metadata_snapshot=_make_metadata_snapshot(),
        w=None, spark=None,
        catalog="main", gold_schema="sales",
        warehouse_id="",
    )
    assert out is not None
    # Applier reads patch["sql_snippet"]; nested shape is required.
    assert "sql_snippet" in out
    snippet = out["sql_snippet"]
    assert snippet["name"] == "total_revenue"
    assert snippet["sql"] == "SUM(orders.revenue)"
    assert snippet["id"]  # non-empty


def test_finalizer_stamps_snippet_type_from_patch_type():
    out = finalize_sql_snippet_proposal_dict(
        _make_proposal(PatchType.ADD_SQL_SNIPPET_MEASURE),
        _make_proposal(PatchType.ADD_SQL_SNIPPET_MEASURE).to_proposal_dict(),
        cluster=_make_cluster(),
        metadata_snapshot=_make_metadata_snapshot(),
        w=None, spark=None,
        catalog="main", gold_schema="sales", warehouse_id="",
    )
    assert out["snippet_type"] == "measure"

    out_f = finalize_sql_snippet_proposal_dict(
        _make_proposal(PatchType.ADD_SQL_SNIPPET_FILTER),
        _make_proposal(PatchType.ADD_SQL_SNIPPET_FILTER).to_proposal_dict(),
        cluster=_make_cluster(),
        metadata_snapshot=_make_metadata_snapshot(),
        w=None, spark=None,
        catalog="main", gold_schema="sales", warehouse_id="",
    )
    assert out_f["snippet_type"] == "filter"


def test_finalizer_validation_passed_false_when_no_backend():
    """No w/warehouse_id and no spark → cannot EXPLAIN/execute → must
    stamp validation_passed=False so the applier-gate drops the
    patch (safe default, matches legacy body line 14396)."""
    out = finalize_sql_snippet_proposal_dict(
        _make_proposal(),
        _make_proposal().to_proposal_dict(),
        cluster=_make_cluster(),
        metadata_snapshot=_make_metadata_snapshot(),
        w=None, spark=None,
        catalog="main", gold_schema="sales", warehouse_id="",
    )
    assert out["validation_passed"] is False


def test_finalizer_returns_none_on_invalid_identifier():
    """The LLM emitted an identifier not in the allowlist — the
    finalizer must reject (return None) so the dispatcher falls
    through to the safety-net legacy generator."""
    proposal = _make_proposal()
    bad = _make_proposal()
    bad.patch_body["sql_expression"] = "SUM(nonexistent.table.col)"
    out = finalize_sql_snippet_proposal_dict(
        bad,
        bad.to_proposal_dict(),
        cluster=_make_cluster(),
        metadata_snapshot=_make_metadata_snapshot(),
        w=None, spark=None,
        catalog="main", gold_schema="sales", warehouse_id="",
    )
    assert out is None
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_sql_snippet_finalizer.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'genie_space_optimizer.optimization.sql_snippet_finalizer'`

- [ ] **Step 3: Implement the finalizer module**

Create `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/sql_snippet_finalizer.py`:

```python
"""Plan 9 Task 6.1 — finalize ADD_SQL_SNIPPET_* proposal dicts.

Bridges RepairProposal.to_proposal_dict()'s flat shape to the
nested sql_snippet shape applier.py:3162 reads, and stamps
validation_passed after running validate_sql_snippet.

Lift-and-shift from _generate_lever6_proposal_legacy_body
(optimizer.py:14323-14468) — fields named identically so the
applier renders the Plan-9 direct path the same way it renders
the legacy body's output.

Only called for ADD_SQL_SNIPPET_{MEASURE, FILTER, EXPRESSION}.
Other patch types do not need finalization (no applier hard
assertion + no validate_sql_snippet step).
"""
from __future__ import annotations

import hashlib
import logging
from typing import Any

from genie_space_optimizer.optimization.repair_intent import PatchType
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind,
)

logger = logging.getLogger(__name__)


_PATCH_TYPE_TO_SNIPPET_TYPE = {
    PatchType.ADD_SQL_SNIPPET_MEASURE: "measure",
    PatchType.ADD_SQL_SNIPPET_FILTER: "filter",
    PatchType.ADD_SQL_SNIPPET_EXPRESSION: "expression",
}


def _first_table_identifier(proposal: RepairProposal) -> str:
    for t in proposal.target_objects:
        if t.asset_kind == AssetKind.TABLE:
            return t.identifier
    # Fallback to the first blame_set entry that looks like a table.
    for b in proposal.blame_set:
        if "." in str(b):
            return str(b)
    return ""


def _snippet_id_for(proposal: RepairProposal, sql_expression: str) -> str:
    """Stable snippet id — hash of (intent_id, sql) so re-emissions
    are idempotent and the applier can locate it for rollback."""
    h = hashlib.sha256()
    h.update(proposal.intent_id.encode("utf-8"))
    h.update(b"\0")
    h.update(sql_expression.encode("utf-8"))
    return h.hexdigest()[:16]


def finalize_sql_snippet_proposal_dict(
    proposal: RepairProposal,
    base_dict: dict[str, Any],
    *,
    cluster: dict,
    metadata_snapshot: dict,
    w: Any,
    spark: Any,
    catalog: str,
    gold_schema: str,
    warehouse_id: str,
) -> dict[str, Any] | None:
    """Wrap base_dict (flat to_proposal_dict() output) into the
    nested applier-expected shape and stamp validation fields.

    Returns None when the SQL fails identifier validation; caller
    treats this as a decline → falls through to safety-net legacy
    generator.
    """
    if proposal.patch_type not in _PATCH_TYPE_TO_SNIPPET_TYPE:
        # Defensive — caller should only invoke for these types.
        return dict(base_dict)

    snippet_type = _PATCH_TYPE_TO_SNIPPET_TYPE[proposal.patch_type]
    sql_expression = str(base_dict.get("sql_expression", ""))
    name = str(base_dict.get("name", ""))
    usage_guidance = str(base_dict.get("usage_guidance", ""))

    if not sql_expression or not name:
        logger.warning(
            "plan9.finalizer.missing_field intent_id=%s name_empty=%s sql_empty=%s",
            proposal.intent_id, not name, not sql_expression,
        )
        return None

    # Identifier validation against the metadata allowlist (cheap,
    # no backend required). Mirrors legacy body line 14349.
    from genie_space_optimizer.optimization.optimizer import (
        _build_identifier_allowlist,
        _validate_sql_identifiers,
    )
    id_allowlist = _build_identifier_allowlist(metadata_snapshot)
    sql_ok, violations = _validate_sql_identifiers(
        sql_expression, id_allowlist,
    )
    if not sql_ok:
        logger.warning(
            "plan9.finalizer.identifier_validation_failed intent_id=%s "
            "violations=%s — treating as decline.",
            proposal.intent_id, violations,
        )
        return None

    target_table = _first_table_identifier(proposal)
    cluster_id = str(cluster.get("cluster_id", "?"))

    # EXPLAIN + execute validation. Mirrors legacy body lines 14358-14392.
    validation_passed = False
    if spark is not None or (w is not None and warehouse_id):
        from genie_space_optimizer.optimization.benchmarks import (
            validate_sql_snippet,
        )
        valid_result = validate_sql_snippet(
            sql_expression, snippet_type, metadata_snapshot,
            spark=spark, catalog=catalog, gold_schema=gold_schema,
            w=w, warehouse_id=warehouse_id,
        )
        if not valid_result[0]:
            logger.info(
                "plan9.finalizer.validate_sql_snippet FAILED "
                "cluster_id=%s kind=%s target=%s reason=%s",
                cluster_id, snippet_type,
                target_table or "n/a", valid_result[1],
            )
            return None
        # Some validators rewrite the SQL (e.g. canonicalize) — use it.
        sql_expression = valid_result[2] if len(valid_result) > 2 else sql_expression
        validation_passed = True
        logger.info(
            "plan9.finalizer.validate_sql_snippet PASSED "
            "cluster_id=%s kind=%s target=%s",
            cluster_id, snippet_type, target_table or "n/a",
        )
    else:
        logger.info(
            "plan9.finalizer.validate_sql_snippet SKIPPED (no backend) "
            "cluster_id=%s kind=%s — applier-gate will drop the patch.",
            cluster_id, snippet_type,
        )

    snippet_id = _snippet_id_for(proposal, sql_expression)
    patch_type_to_applier = {
        PatchType.ADD_SQL_SNIPPET_MEASURE: "add_sql_snippet_measure",
        PatchType.ADD_SQL_SNIPPET_FILTER: "add_sql_snippet_filter",
        PatchType.ADD_SQL_SNIPPET_EXPRESSION: "add_sql_snippet_expression",
    }
    return {
        "patch_type": patch_type_to_applier[proposal.patch_type],
        "lever": 6,
        "snippet_type": snippet_type,
        "display_name": proposal.intent_name or name,
        "alias": "",
        "sql": sql_expression,
        "synonyms": [],
        "instruction": usage_guidance or proposal.rationale,
        "target_table": target_table,
        "rationale": proposal.rationale,
        "affected_questions": list(
            str(q) for q in (cluster.get("question_ids") or [])
        ),
        "confidence": proposal.confidence,
        "questions_fixed": len(cluster.get("question_traces", []) or []),
        "validation_passed": validation_passed,
        # Nested sql_snippet object — applier.py:3162 reads this.
        "sql_snippet": {
            "id": snippet_id,
            "name": name,
            "sql": sql_expression,
            "type": snippet_type,
            "description": usage_guidance or proposal.rationale,
        },
    }
```

- [ ] **Step 4: Re-run the finalizer tests to verify they pass**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_sql_snippet_finalizer.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Write the L6 dispatcher integration test**

Create `packages/genie-space-optimizer/tests/unit/test_lever6_intent_dispatch_finalizer_integration.py`:

```python
"""Plan 9 Task 6.1 — L6 dispatcher must call the finalizer after
to_proposal_dict() succeeds, so the proposal dict the applier
sees is the nested sql_snippet shape with validation_passed
stamped."""
from __future__ import annotations

from genie_space_optimizer.optimization.lever6_intent_dispatch import (
    dispatch_lever_6_with_intent,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType, RepairShape,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.target_object_typed import (
    AssetKind, TargetObject,
)


class _StubCluster:
    cluster_id = "C1"
    target_qids = ("q1", "q2")
    rca_card_id = "rca-1"


def _make_proposal() -> RepairProposal:
    return RepairProposal(
        intent_id="abc12345",
        intent_name="Total revenue measure",
        intent_description="Add a measure.",
        repair_shape=RepairShape.AGGREGATION,
        patch_type=PatchType.ADD_SQL_SNIPPET_MEASURE,
        rationale="Cluster lacks aggregation.",
        confidence=0.8,
        patch_body={
            "name": "total_revenue",
            "sql_expression": "SUM(orders.revenue)",
            "usage_guidance": "Total revenue.",
        },
        blame_set=("main.sales.orders",),
        target_objects=(
            TargetObject(
                asset_kind=AssetKind.TABLE,
                identifier="main.sales.orders",
                columns=("revenue",),
            ),
        ),
        required_constructs=(),
    )


def _make_metadata_snapshot() -> dict:
    return {
        "data_sources": {
            "tables": [
                {
                    "identifier": "main.sales.orders",
                    "columns": [{"name": "revenue", "type": "double"}],
                }
            ],
        },
        "sql_snippets": {"measures": []},
    }


def test_l6_dispatch_finalizer_produces_nested_sql_snippet(monkeypatch):
    """When to_proposal_dict() succeeds AND finalizer succeeds,
    the dispatcher returns a proposal dict with sql_snippet nested
    and provenance plan9_materialization_source=plan9_direct."""
    proposal = _make_proposal()

    # Stub the synthesizer to return our typed proposal.
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.repair_intent_synthesizer."
        "synthesize_repair_intent_for_cluster",
        lambda **_: proposal,
    )

    out = dispatch_lever_6_with_intent(
        cluster={
            "cluster_id": "C1",
            "root_cause": "missing_measure",
            "question_ids": ["q1", "q2"],
            "question_traces": [{"qid": "q1"}],
        },
        llm_cluster=_StubCluster(),
        rca_evidence=None,
        ag_id="ag1",
        metadata_snapshot=_make_metadata_snapshot(),
        strategist_hints=None,
        w=None, spark=None,
        catalog="main", gold_schema="sales", warehouse_id="",
        benchmarks=None, raw_evidence=(),
    )

    assert out is not None
    assert "sql_snippet" in out
    assert out["sql_snippet"]["name"] == "total_revenue"
    assert out["sql_snippet"]["id"]
    assert "validation_passed" in out  # stamped (False here — no backend)
    assert out["provenance"]["plan9_materialization_source"] == "plan9_direct"


def test_l6_dispatch_finalizer_rejects_returns_legacy_fallback(monkeypatch):
    """When to_proposal_dict() succeeds BUT finalizer rejects (e.g.,
    invalid identifier), dispatcher must fall through to the
    safety-net legacy generator instead of returning the broken dict."""
    proposal = _make_proposal()
    proposal.patch_body["sql_expression"] = "SUM(nonexistent.col)"  # invalid

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.repair_intent_synthesizer."
        "synthesize_repair_intent_for_cluster",
        lambda **_: proposal,
    )

    # Stub the legacy generator so we can detect it ran.
    legacy_called = {"n": 0}
    def _fake_legacy(**_):
        legacy_called["n"] += 1
        return {
            "patch_type": "add_sql_snippet_measure",
            "lever": 6,
            "sql_snippet": {"id": "legacy-1", "name": "legacy"},
            "validation_passed": True,
        }
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.lever6_intent_dispatch."
        "_generate_lever6_proposal_legacy",
        _fake_legacy,
    )

    out = dispatch_lever_6_with_intent(
        cluster={
            "cluster_id": "C1",
            "root_cause": "missing_measure",
            "question_ids": ["q1"],
            "question_traces": [{"qid": "q1"}],
        },
        llm_cluster=_StubCluster(),
        rca_evidence=None,
        ag_id="ag1",
        metadata_snapshot=_make_metadata_snapshot(),
        strategist_hints=None,
        w=None, spark=None,
        catalog="main", gold_schema="sales", warehouse_id="",
        benchmarks=None, raw_evidence=(),
    )

    assert out is not None
    assert legacy_called["n"] == 1
    assert out["sql_snippet"]["id"] == "legacy-1"
    assert out["provenance"]["plan9_materialization_source"] == "plan9_legacy_fallback"
```

- [ ] **Step 6: Run the integration test — expect FAIL**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_lever6_intent_dispatch_finalizer_integration.py -v`
Expected: FAIL — the dispatcher currently returns the flat dict from `to_proposal_dict()` without calling the finalizer.

- [ ] **Step 7: Wire the finalizer into `dispatch_lever_6_with_intent`**

Modify `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever6_intent_dispatch.py:107-160`. Replace the block beginning with `proposal_dict: dict | None = None` through `materialization_source = "plan9_direct"` and the `try`/`except` around `to_proposal_dict()` with this expanded version (keep the rest of the function unchanged):

```python
    # Plan 9 Task 6 — materialize RepairProposal.patch_body directly
    # via to_proposal_dict() instead of having the legacy generator
    # re-do the SQL synthesis.
    # Plan 9 Task 6.1 — for ADD_SQL_SNIPPET_* patches, the flat
    # output must be finalized into the applier-expected nested
    # sql_snippet shape AND have validation_passed stamped after a
    # clean validate_sql_snippet result. Finalizer returns None on
    # validator failure → falls through to legacy safety net.
    from genie_space_optimizer.optimization.repair_intent import PatchType
    from genie_space_optimizer.optimization.sql_snippet_finalizer import (
        finalize_sql_snippet_proposal_dict,
    )

    _SQL_SNIPPET_TYPES = {
        PatchType.ADD_SQL_SNIPPET_MEASURE,
        PatchType.ADD_SQL_SNIPPET_FILTER,
        PatchType.ADD_SQL_SNIPPET_EXPRESSION,
    }

    proposal_dict: dict | None = None
    materialization_source = "plan9_direct"
    try:
        base_dict = proposal.to_proposal_dict()
        if proposal.patch_type in _SQL_SNIPPET_TYPES:
            proposal_dict = finalize_sql_snippet_proposal_dict(
                proposal,
                base_dict,
                cluster=cluster,
                metadata_snapshot=metadata_snapshot,
                w=w, spark=spark,
                catalog=catalog, gold_schema=gold_schema,
                warehouse_id=warehouse_id,
            )
            if proposal_dict is None:
                # Finalizer rejected — treat as decline and fall
                # through to the safety-net legacy generator.
                raise RuntimeError(
                    "plan9.finalizer_declined "
                    "intent_id=" + proposal.intent_id
                )
        else:
            proposal_dict = base_dict
    except Exception as exc:
        logger.warning(
            "plan9.l6_direct_materialization_failed intent_id=%s err=%s "
            "— falling back to legacy generator (safety net).",
            proposal.intent_id, exc,
        )
        materialization_source = "plan9_legacy_fallback"
        proposal_dict = _generate_lever6_proposal_legacy(
            cluster=cluster,
            metadata_snapshot=metadata_snapshot,
            strategist_hints=strategist_hints,
            w=w,
            spark=spark,
            catalog=catalog,
            gold_schema=gold_schema,
            warehouse_id=warehouse_id,
            benchmarks=benchmarks,
            raw_evidence=raw_evidence,
        )
        if proposal_dict is None:
            return None
```

- [ ] **Step 8: Run the integration test — expect PASS**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_lever6_intent_dispatch_finalizer_integration.py -v`
Expected: PASS (2 tests)

- [ ] **Step 9: Wire the finalizer into `_dispatch_lever_5b_for_cluster`**

Modify `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:10486-10555`. Replace the block beginning `proposal_dict: dict | None = None` and `materialization_source = "plan9_direct"` and the `try`/`except` around `to_proposal_dict()` with the same finalizer logic, only the `except`-branch fallback target is the per-lever generator (not the L6 legacy body). Concretely:

```python
            # Plan 9 Task 6 — materialize RepairProposal.patch_body
            # directly via to_proposal_dict().
            # Plan 9 Task 6.1 — finalize ADD_SQL_SNIPPET_* patches.
            from genie_space_optimizer.optimization.repair_intent import PatchType
            from genie_space_optimizer.optimization.sql_snippet_finalizer import (
                finalize_sql_snippet_proposal_dict,
            )
            _SQL_SNIPPET_TYPES = {
                PatchType.ADD_SQL_SNIPPET_MEASURE,
                PatchType.ADD_SQL_SNIPPET_FILTER,
                PatchType.ADD_SQL_SNIPPET_EXPRESSION,
            }
            proposal_dict: dict | None = None
            materialization_source = "plan9_direct"
            try:
                base_dict = proposal.to_proposal_dict()
                if proposal.patch_type in _SQL_SNIPPET_TYPES:
                    proposal_dict = finalize_sql_snippet_proposal_dict(
                        proposal,
                        base_dict,
                        cluster=cluster,
                        metadata_snapshot=metadata_snapshot,
                        w=w, spark=spark,
                        catalog=catalog, gold_schema=gold_schema,
                        warehouse_id=warehouse_id,
                    )
                    if proposal_dict is None:
                        raise RuntimeError(
                            "plan9.finalizer_declined "
                            "intent_id=" + proposal.intent_id
                        )
                else:
                    proposal_dict = base_dict
            except Exception as exc:
                logger.warning(
                    "plan9.l5b_direct_materialization_failed "
                    "intent_id=%s err=%s — falling back to per-lever "
                    "generator (safety net).",
                    proposal.intent_id, exc,
                )
                materialization_source = "plan9_legacy_fallback"
                proposal_dict = None
```

(The block below — `routed = route_to_per_lever_generator(...)` etc. — stays as in T6.)

- [ ] **Step 10: Run the full regression — L5b, L6, finalizer, and applier integration tests**

Run:
```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit/test_sql_snippet_finalizer.py \
  tests/unit/test_lever6_intent_dispatch_finalizer_integration.py \
  tests/unit/test_lever6_intent_dispatch_direct_materialization.py \
  tests/unit/test_l5b_dispatch_direct_materialization.py \
  tests/unit/test_applier_proposal_metadata.py \
  -v
```
Expected: ALL PASS.

- [ ] **Step 11: Run the broader Plan-5/L5/L6/applier regression sweep**

Run:
```bash
cd packages/genie-space-optimizer && uv run pytest \
  tests/unit -k "lever5 or lever6 or l5b or l6 or applier or proposal or finalizer" \
  -v --tb=short
```
Expected: ALL PASS — no regressions from the finalizer wire-in.

- [ ] **Step 12: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/sql_snippet_finalizer.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever6_intent_dispatch.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py \
        packages/genie-space-optimizer/tests/unit/test_sql_snippet_finalizer.py \
        packages/genie-space-optimizer/tests/unit/test_lever6_intent_dispatch_finalizer_integration.py \
        packages/genie-space-optimizer/docs/llmdrivenarchitecture/2026-05-19-plan-9-catalog-removal-and-llm-driven-structural-realization.md

git commit -m "$(cat <<'EOF'
plan9(t6.1): finalize ADD_SQL_SNIPPET_* patches with validators + applier-shape

T6 shipped direct materialization via RepairProposal.to_proposal_dict()
but did not run validate_sql_snippet or wrap the flat patch_body into
the nested sql_snippet shape applier.py:3162-3181 requires. Without
this fix, every Plan-9 direct L6 patch is dropped by the applier's
Tier-2.8 hard assertion (validation_passed=True is required for every
add_sql_snippet_* patch) and the missing sql_snippet object means
snippet_id=empty even for patches that would otherwise pass.

T6.1 adds sql_snippet_finalizer.finalize_sql_snippet_proposal_dict
which lifts the validator + proposal-dict builder block verbatim from
_generate_lever6_proposal_legacy_body (optimizer.py:14323-14468). The
finalizer:

  * Runs _validate_sql_identifiers against the metadata allowlist;
    returns None on failure (caller falls through to safety net).
  * Runs validate_sql_snippet against the warehouse when (w,
    warehouse_id) are provided; stamps validation_passed
    accordingly. No-backend dev paths get validation_passed=False
    (safe default — applier-gate drops the patch).
  * Fabricates the applier-expected fields from RepairProposal
    fields and target_objects: snippet_type from patch_type,
    display_name from intent_name, target_table from first TABLE
    in target_objects, rationale from RepairProposal.rationale,
    snippet_id deterministically hashed from (intent_id, sql).
  * Returns the nested sql_snippet object the applier reads at
    applier.py:3162.

Both _dispatch_lever_5b_for_cluster and dispatch_lever_6_with_intent
now call the finalizer for ADD_SQL_SNIPPET_* patch types after
to_proposal_dict() succeeds. Finalizer-decline routes through the
existing safety-net path (legacy per-lever generator) which already
stamps validation_passed via its own validate_sql_snippet call.
ADD_EXAMPLE_SQL, ADD_INSTRUCTION, UPDATE_INSTRUCTION, ADD_JOIN_SPEC,
ADD_COLUMN_DESCRIPTION are unchanged — the applier has no equivalent
hard assertion for those.

Closes the binary deploy-blocker introduced by T6.

Co-authored-by: Isaac
EOF
)"
```

---

### Task 7: Harden `enforce_structural_repair_shape` to reject `ABSENT` + 0.0 repairability

**Rationale:** Reviewer's claim #5 fix. `structural_repair_gate.py:68-72` admits any patch when `intended_patch_shape != "structural"` literally — including `ABSENT` patches with `repairability=0.0`. The 7Now postmortem confirms this: `emitted_patch_shape=absent`, `repairability_score=0.0`, `rca_root_cause=UNKNOWN`, yet the patch was admitted. After Plan 9, the gate also rejects when emitted shape is `ABSENT` AND repairability is 0.0, regardless of the intent string. Legacy RCA cards without Phase-2.3 metadata still fail open when emitted shape is non-`ABSENT`.

**Files:**

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/structural_repair_gate.py:48-81`
- Test: `packages/genie-space-optimizer/tests/unit/test_structural_repair_gate_rejects_absent_zero_repairability.py`

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_structural_repair_gate_rejects_absent_zero_repairability.py`:

```python
"""Plan 9 Task 7 — structural_repair_gate rejects ABSENT + 0.0
repairability regardless of intended_patch_shape string.

Closes the 7Now fail-open bug where emitted_patch_shape=absent,
repairability_score=0.0, rca_root_cause=UNKNOWN was admitted
because intended_patch_shape was not literally 'structural'.
"""
from genie_space_optimizer.optimization.structural_repair_gate import (
    enforce_structural_repair_shape,
)
from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
)


def test_rejects_absent_with_zero_repairability_when_intent_is_empty():
    """The 7Now bug: empty intent + ABSENT + 0.0 was admitted."""
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="",
        emitted_patch_shape=EmittedPatchShape.ABSENT,
        narrow_replacement_available=False,
    )
    assert verdict.outcome == "rejected"
    assert verdict.repairability is not None
    assert verdict.repairability.repairability_score == 0.0


def test_rejects_absent_with_zero_repairability_when_intent_is_instruction():
    """Same bug shape with intent=instruction."""
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="instruction",
        emitted_patch_shape=EmittedPatchShape.ABSENT,
        narrow_replacement_available=False,
    )
    assert verdict.outcome == "rejected"


def test_rejects_absent_with_zero_repairability_when_intent_is_structural():
    """Pre-Plan-9 rejection still fires."""
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.ABSENT,
        narrow_replacement_available=False,
    )
    assert verdict.outcome == "rejected"


def test_admits_non_absent_emitted_with_empty_intent_backward_compat():
    """Legacy RCA cards without Phase-2.3 metadata still fail open
    when emitted shape is non-ABSENT — Plan 9 only tightens the
    ABSENT + 0.0 combo."""
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="",  # legacy
        emitted_patch_shape=EmittedPatchShape.INSTRUCTION_ONLY,
        narrow_replacement_available=False,
    )
    assert verdict.outcome == "admitted"


def test_admits_structural_emitted_with_structural_intent():
    """Happy path unchanged."""
    verdict = enforce_structural_repair_shape(
        intended_patch_shape="structural",
        emitted_patch_shape=EmittedPatchShape.STRUCTURAL,
        narrow_replacement_available=True,
    )
    assert verdict.outcome == "admitted"
```

- [ ] **Step 2: Run the failing tests**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_structural_repair_gate_rejects_absent_zero_repairability.py -v`
Expected: FAIL on the first two tests (gate currently admits).

- [ ] **Step 3: Tighten the gate**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/structural_repair_gate.py:48-81`. Replace the function:

Current:
```python
def enforce_structural_repair_shape(
    *,
    intended_patch_shape: str,
    emitted_patch_shape: EmittedPatchShape,
    narrow_replacement_available: bool = False,
) -> StructuralRepairGateVerdict:
    """…"""
    score = compute_repairability(
        intended_patch_shape=intended_patch_shape,
        emitted_patch_shape=emitted_patch_shape,
        narrow_replacement_available=narrow_replacement_available,
    )
    intent = str(intended_patch_shape or "").strip().lower()
    if intent != "structural":
        return StructuralRepairGateVerdict(
            outcome="admitted", terminal_reason="", repairability=score,
        )
    if emitted_patch_shape == EmittedPatchShape.STRUCTURAL:
        return StructuralRepairGateVerdict(
            outcome="admitted", terminal_reason="", repairability=score,
        )
    return StructuralRepairGateVerdict(
        outcome="rejected",
        terminal_reason=TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY.value,
        repairability=score,
    )
```

Replacement:
```python
def enforce_structural_repair_shape(
    *,
    intended_patch_shape: str,
    emitted_patch_shape: EmittedPatchShape,
    narrow_replacement_available: bool = False,
) -> StructuralRepairGateVerdict:
    """Plan 9 Task 7 — rejection priority:

      1. ABSENT emitted + 0.0 repairability → REJECT regardless of
         intent (closes the 7Now fail-open bug).
      2. intent == 'structural' AND emitted != STRUCTURAL → REJECT
         (legacy rule).
      3. Otherwise → ADMIT (legacy fail-open for non-structural intent
         or for legacy RCA cards without Phase-2.3 metadata, IFF
         emitted shape is non-ABSENT).
    """
    score = compute_repairability(
        intended_patch_shape=intended_patch_shape,
        emitted_patch_shape=emitted_patch_shape,
        narrow_replacement_available=narrow_replacement_available,
    )
    intent = str(intended_patch_shape or "").strip().lower()

    # Plan 9 — ABSENT + 0.0 is degenerate regardless of intent.
    if (
        emitted_patch_shape == EmittedPatchShape.ABSENT
        and score.repairability_score == 0.0
    ):
        return StructuralRepairGateVerdict(
            outcome="rejected",
            terminal_reason=(
                TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY.value
            ),
            repairability=score,
        )

    # Pre-Plan-9 — structural intent must match structural emitted.
    if intent == "structural" and emitted_patch_shape != EmittedPatchShape.STRUCTURAL:
        return StructuralRepairGateVerdict(
            outcome="rejected",
            terminal_reason=(
                TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY.value
            ),
            repairability=score,
        )

    return StructuralRepairGateVerdict(
        outcome="admitted",
        terminal_reason="",
        repairability=score,
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_structural_repair_gate_rejects_absent_zero_repairability.py -v`
Expected: 5 passed.

- [ ] **Step 5: Run the existing structural gate tests to confirm no regression**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_structural_repair_gate.py tests/unit/test_repairability_score.py -v`
Expected: all passed (some tests may need updating if they relied on the fail-open behaviour for `ABSENT` + 0.0 + non-structural intent; update by adding an `EmittedPatchShape.INSTRUCTION_ONLY` shim where they previously used `ABSENT`).

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/structural_repair_gate.py \
        packages/genie-space-optimizer/tests/unit/test_structural_repair_gate_rejects_absent_zero_repairability.py
git commit -m "$(cat <<'EOF'
plan9(t7): structural gate rejects ABSENT + 0.0 repairability

Closes the 7Now fail-open bug. The gate previously admitted any
patch when intended_patch_shape != 'structural' literally,
including degenerate ABSENT patches with 0.0 repairability.

New rule order:
1. ABSENT + 0.0 repairability → REJECT regardless of intent.
2. intent=='structural' + emitted!=STRUCTURAL → REJECT (legacy).
3. Otherwise → ADMIT (legacy fail-open kept for non-ABSENT shapes).
EOF
)"
```

---

### Task 8: Emit `PLAN5_ANCHOR_ACTIVATION_V1` marker for every anchor

**Rationale:** Reviewer's "add activation markers" recommendation. Postmortem currently cannot tell whether Plan 5 fired on an anchor, declined, was validator-rejected, was routed to another lever, or materialized successfully. After Plan 9, every anchor records exactly one `PLAN5_ANCHOR_ACTIVATION_V1` marker with a typed status from `{plan5_intent_invoked, plan5_intent_declined, plan5_intent_validator_rejected, plan5_intent_routed, plan5_intent_materialized}` and a typed reason field. This marker is the postmortem source of truth.

**Files:**

- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/plan9_activation_markers.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever6_intent_dispatch.py` (emit at 3 points)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:10422+` (emit at 4 points inside `_dispatch_lever_5b_for_cluster`)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py` (emit at the "anchor decided" point in the per-AG loop)
- Test: `packages/genie-space-optimizer/tests/unit/test_plan9_activation_markers_emit.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_plan9_activation_markers_all_anchors_covered.py`

- [ ] **Step 1: Write the failing test for the marker emitter**

Create `packages/genie-space-optimizer/tests/unit/test_plan9_activation_markers_emit.py`:

```python
"""Plan 9 Task 8 — PLAN5_ANCHOR_ACTIVATION_V1 marker emitter.

Verifies the ActivationStatus enum has the five required values
and that emit_plan5_activation produces the expected stdout line.
"""
import io
from contextlib import redirect_stdout

from genie_space_optimizer.optimization.plan9_activation_markers import (
    ActivationStatus,
    emit_plan5_activation,
)


def test_activation_status_has_five_values():
    assert set(ActivationStatus) == {
        ActivationStatus.PLAN5_INTENT_INVOKED,
        ActivationStatus.PLAN5_INTENT_DECLINED,
        ActivationStatus.PLAN5_INTENT_VALIDATOR_REJECTED,
        ActivationStatus.PLAN5_INTENT_ROUTED,
        ActivationStatus.PLAN5_INTENT_MATERIALIZED,
    }


def test_emit_plan5_activation_writes_marker_line():
    buf = io.StringIO()
    with redirect_stdout(buf):
        emit_plan5_activation(
            run_id="run_test",
            iteration=2,
            ag_id="AG_H001",
            cluster_id="c_h001",
            status=ActivationStatus.PLAN5_INTENT_MATERIALIZED,
            reason="patch_body materialized to add_example_sql",
            patch_type="add_example_sql",
            intent_id="intent_h001_001",
        )
    output = buf.getvalue()
    assert "PLAN5_ANCHOR_ACTIVATION_V1" in output
    assert "AG_H001" in output
    assert "plan5_intent_materialized" in output
    assert "intent_h001_001" in output


def test_emit_plan5_activation_decline_includes_reason():
    buf = io.StringIO()
    with redirect_stdout(buf):
        emit_plan5_activation(
            run_id="run_test",
            iteration=1,
            ag_id="AG_H002",
            cluster_id="c_h002",
            status=ActivationStatus.PLAN5_INTENT_DECLINED,
            reason="llm_returned_abstain",
        )
    output = buf.getvalue()
    assert "plan5_intent_declined" in output
    assert "llm_returned_abstain" in output
```

- [ ] **Step 2: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_plan9_activation_markers_emit.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Create the marker emitter module**

Create `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/plan9_activation_markers.py`:

```python
"""Plan 9 Task 8 — PLAN5_ANCHOR_ACTIVATION_V1 marker.

One marker per anchor in every iteration of every lever-loop run.
Status enum:

  * PLAN5_INTENT_INVOKED — Plan 5 synthesizer dispatched; LLM
    call made.
  * PLAN5_INTENT_DECLINED — LLM returned abstain or empty
    RepairProposal (no synthesis).
  * PLAN5_INTENT_VALIDATOR_REJECTED — synthesizer returned a
    RepairProposal but a deterministic validator (patch_body
    shape, blame_set allowlist, leakage firewall) rejected it.
  * PLAN5_INTENT_ROUTED — cross-lever router redirected the
    intent (e.g. L5b intent routed to L6 generator); the routed
    proposal still produces a candidate.
  * PLAN5_INTENT_MATERIALIZED — proposal_dict produced and added
    to all_proposals.

Postmortem invariant: every anchor in every iteration MUST
produce exactly ONE marker. Anchors with no marker are bugs
(test_plan9_activation_markers_all_anchors_covered pins this).
"""
from __future__ import annotations

from enum import StrEnum

from genie_space_optimizer.optimization.run_analysis_contract import (
    marker_line,
)


class ActivationStatus(StrEnum):
    PLAN5_INTENT_INVOKED = "plan5_intent_invoked"
    PLAN5_INTENT_DECLINED = "plan5_intent_declined"
    PLAN5_INTENT_VALIDATOR_REJECTED = "plan5_intent_validator_rejected"
    PLAN5_INTENT_ROUTED = "plan5_intent_routed"
    PLAN5_INTENT_MATERIALIZED = "plan5_intent_materialized"


def emit_plan5_activation(
    *,
    run_id: str,
    iteration: int,
    ag_id: str,
    cluster_id: str,
    status: ActivationStatus,
    reason: str = "",
    patch_type: str = "",
    intent_id: str = "",
) -> None:
    """Emit one PLAN5_ANCHOR_ACTIVATION_V1 marker line to stdout."""
    payload = {
        "optimization_run_id": str(run_id),
        "iteration": int(iteration),
        "ag_id": str(ag_id),
        "cluster_id": str(cluster_id),
        "status": str(status.value),
        "reason": str(reason),
        "patch_type": str(patch_type),
        "intent_id": str(intent_id),
    }
    print(marker_line("PLAN5_ANCHOR_ACTIVATION_V1", payload), flush=True)
```

- [ ] **Step 4: Run the emitter test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_plan9_activation_markers_emit.py -v`
Expected: 3 passed.

- [ ] **Step 5: Emit at each L5b decision point in `_dispatch_lever_5b_for_cluster`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:10422+`. Inside `_dispatch_lever_5b_for_cluster`, add `emit_plan5_activation` calls at four decision points:

After Plan 5 short-circuit prelude (the `if plan5_lever_5b_llm_intent_enabled() and rca_evidence_typed and llm_cluster is not None and ag_id:` check):

```python
        from genie_space_optimizer.optimization.plan9_activation_markers import (
            ActivationStatus,
            emit_plan5_activation,
        )
        # Plan 9 Task 8 — Anchor invocation marker.
        emit_plan5_activation(
            run_id="",  # Filled by the caller via context; see harness wiring.
            iteration=int(iteration or 0),
            ag_id=str(ag_id),
            cluster_id=str(cluster.get("cluster_id") or ""),
            status=ActivationStatus.PLAN5_INTENT_INVOKED,
        )
```

Right after `proposal = synthesize_repair_intent_for_cluster(...)`:

```python
        if proposal is None:
            emit_plan5_activation(
                run_id="",
                iteration=int(iteration or 0),
                ag_id=str(ag_id),
                cluster_id=str(cluster.get("cluster_id") or ""),
                status=ActivationStatus.PLAN5_INTENT_DECLINED,
                reason="synthesizer_returned_none",
            )
            # Fall through to legacy path…
```

Right before the cross-lever router check (`routed = route_to_per_lever_generator(proposal)`):

```python
        if routed is not None and routed[1] is not None:
            emit_plan5_activation(
                run_id="",
                iteration=int(iteration or 0),
                ag_id=str(ag_id),
                cluster_id=str(cluster.get("cluster_id") or ""),
                status=ActivationStatus.PLAN5_INTENT_ROUTED,
                reason=f"routed_from_l5b_to_{routed[1].to_lever}",
                patch_type=proposal.patch_type.value,
                intent_id=proposal.intent_id,
            )
```

After successful materialization (where Plan 9 Task 6 added `return [proposal_dict]`):

```python
        emit_plan5_activation(
            run_id="",
            iteration=int(iteration or 0),
            ag_id=str(ag_id),
            cluster_id=str(cluster.get("cluster_id") or ""),
            status=ActivationStatus.PLAN5_INTENT_MATERIALIZED,
            reason="patch_body materialized via to_proposal_dict",
            patch_type=proposal.patch_type.value,
            intent_id=proposal.intent_id,
        )
        return [proposal_dict]
```

When `to_proposal_dict()` raises (the safety net path):

```python
        except Exception as exc:
            emit_plan5_activation(
                run_id="",
                iteration=int(iteration or 0),
                ag_id=str(ag_id),
                cluster_id=str(cluster.get("cluster_id") or ""),
                status=ActivationStatus.PLAN5_INTENT_VALIDATOR_REJECTED,
                reason=f"to_proposal_dict_raised:{type(exc).__name__}",
                patch_type=proposal.patch_type.value if proposal else "",
                intent_id=proposal.intent_id if proposal else "",
            )
```

- [ ] **Step 6: Thread `run_id` to `_dispatch_lever_5b_for_cluster`**

Add `run_id: str | None = None` to `_dispatch_lever_5b_for_cluster` signature; pass it from `_dispatch_lever_5_split` (which already receives it from upstream via the harness). Replace empty `""` placeholders in T8 Step 5 above with `str(run_id or "")`.

Use `rg "_dispatch_lever_5b_for_cluster\(" packages/genie-space-optimizer/src` to find every call site and add `run_id=run_id` to each.

- [ ] **Step 7: Emit at each L6 decision point in `dispatch_lever_6_with_intent`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever6_intent_dispatch.py`. Add four `emit_plan5_activation` calls mirroring T8 Step 5 (INVOKED at entry, DECLINED when `synthesize_repair_intent_for_cluster` returns None, MATERIALIZED on success, VALIDATOR_REJECTED on `to_proposal_dict` failure or validator failure).

Thread `run_id` parameter through `dispatch_lever_6_with_intent` signature; pass from `_generate_lever6_proposal` which receives it via Plan 9 Task 4's signature expansion.

- [ ] **Step 8: Write the failing test for anchor coverage**

Create `packages/genie-space-optimizer/tests/unit/test_plan9_activation_markers_all_anchors_covered.py`:

```python
"""Plan 9 Task 8 — every anchor in every iteration emits exactly
one PLAN5_ANCHOR_ACTIVATION_V1 marker.

Integration smoke test against a synthetic 2-AG iteration. After
the iteration, parse stdout for markers; assert one marker per
(ag_id, iteration) pair.
"""
import io
import json
import re
from contextlib import redirect_stdout

import pytest

# Test uses harness internals; mark for the integration runner.
pytestmark = pytest.mark.integration


def test_each_anchor_emits_exactly_one_activation_marker():
    """Run a 2-AG iteration through a stub harness path and verify
    each AG produces exactly one PLAN5_ANCHOR_ACTIVATION_V1 marker."""
    # … harness stub setup using the test fixtures from
    # tests/fixtures/two_ag_synthetic_iter1/ …
    # … invoke _run_one_iteration(stub_state) …
    # … capture stdout via redirect_stdout …

    buf = io.StringIO()
    with redirect_stdout(buf):
        # _run_one_iteration_stub(...)  # actual call here
        pass  # placeholder — fill with concrete stub when fixtures land
    output = buf.getvalue()

    markers = []
    for line in output.splitlines():
        m = re.search(
            r"PLAN5_ANCHOR_ACTIVATION_V1\s+(\{.*\})", line,
        )
        if m:
            markers.append(json.loads(m.group(1)))

    # Two AGs → exactly two markers.
    by_ag = {}
    for marker in markers:
        by_ag.setdefault(marker["ag_id"], []).append(marker)
    assert set(by_ag.keys()) == {"AG_H001", "AG_H002"}
    for ag_id, ag_markers in by_ag.items():
        assert len(ag_markers) == 1, (
            f"AG {ag_id} produced {len(ag_markers)} markers; "
            f"Plan 9 invariant requires exactly 1."
        )
```

(This integration test is a placeholder — fill in the harness stub call when the executor lands the actual test fixture in `tests/fixtures/two_ag_synthetic_iter1/`. The pytest will skip until then. The point of writing it now is to encode the Plan 9 invariant as an executable spec.)

- [ ] **Step 9: Run the L5b + L6 tests to confirm no regression**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_dispatch_lever_5b_for_cluster.py tests/unit/test_lever6_intent_dispatch.py tests/unit/test_lever6_intent_dispatch_direct_materialization.py tests/unit/test_l5b_dispatch_direct_materialization.py tests/unit/test_plan9_activation_markers_emit.py -v`
Expected: all passed.

- [ ] **Step 10: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/plan9_activation_markers.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py \
        packages/genie-space-optimizer/src/genie_space_optimizer/optimization/lever6_intent_dispatch.py \
        packages/genie-space-optimizer/tests/unit/test_plan9_activation_markers_emit.py \
        packages/genie-space-optimizer/tests/unit/test_plan9_activation_markers_all_anchors_covered.py
git commit -m "$(cat <<'EOF'
plan9(t8): emit PLAN5_ANCHOR_ACTIVATION_V1 marker per anchor

Five typed activation statuses (invoked / declined /
validator_rejected / routed / materialized) emitted at each
decision point in _dispatch_lever_5b_for_cluster and
dispatch_lever_6_with_intent.

Postmortem invariant: every anchor in every iteration produces
exactly one marker. Replaces the ambiguous lever6_force_llm_declined.
EOF
)"
```

---

### Task 9: Move forbidden-set filtering ahead of proposal generation

**Rationale:** Reviewer's "move forbidden filtering earlier" recommendation. The harness today computes `_forbidden_pair` at `harness.py:22528` (per-AG, before proposal generation) but only consults it as a collision check AFTER generation. Both postmortems show iterations 3-4 wasting cycles on retired signatures that the forbidden set already knows about. After Plan 9 Task 9, the harness applies the forbidden filter to the AG list before `generate_proposals_from_strategy` runs, so retired signatures never trigger an LLM call.

**Files:**

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py:22500-22600` (insert pre-generation filter)
- Test: `packages/genie-space-optimizer/tests/unit/test_harness_forbidden_set_pre_generation.py`

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_harness_forbidden_set_pre_generation.py`:

```python
"""Plan 9 Task 9 — forbidden-set filtering runs before
generate_proposals_from_strategy, not after.

Verifies that AGs whose source_cluster_signatures match the
forbidden_pair are skipped before any LLM call fires. Saves
iterations on retired signatures.
"""
import pytest

from genie_space_optimizer.optimization.harness import (
    filter_ags_by_forbidden_set_pre_generation,
)


def _make_ag(ag_id, signatures):
    return {
        "id": ag_id,
        "source_cluster_signatures": list(signatures),
        "target_qids": [f"q_{ag_id}"],
    }


def _make_forbidden_pair(*signatures, root_causes=()):
    from types import SimpleNamespace
    return SimpleNamespace(
        by_signature=frozenset(signatures),
        by_root_cause=frozenset(root_causes),
        by_terminal_signature=frozenset(),
    )


def test_filter_drops_ag_with_forbidden_signature():
    ags = [
        _make_ag("AG_001", ["sig_alpha"]),
        _make_ag("AG_002", ["sig_beta"]),
    ]
    forbidden = _make_forbidden_pair("sig_alpha")
    surviving, dropped = filter_ags_by_forbidden_set_pre_generation(
        ags, forbidden,
    )
    assert [ag["id"] for ag in surviving] == ["AG_002"]
    assert [ag["id"] for ag in dropped] == ["AG_001"]


def test_filter_keeps_all_when_no_forbidden_signatures():
    ags = [
        _make_ag("AG_001", ["sig_alpha"]),
        _make_ag("AG_002", ["sig_beta"]),
    ]
    forbidden = _make_forbidden_pair()
    surviving, dropped = filter_ags_by_forbidden_set_pre_generation(
        ags, forbidden,
    )
    assert len(surviving) == 2
    assert len(dropped) == 0


def test_filter_handles_ags_with_no_signatures():
    ags = [_make_ag("AG_001", [])]
    forbidden = _make_forbidden_pair("sig_alpha")
    surviving, dropped = filter_ags_by_forbidden_set_pre_generation(
        ags, forbidden,
    )
    assert [ag["id"] for ag in surviving] == ["AG_001"]
```

- [ ] **Step 2: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_harness_forbidden_set_pre_generation.py -v`
Expected: FAIL — `filter_ags_by_forbidden_set_pre_generation` does not exist.

- [ ] **Step 3: Add the pre-generation filter**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py`. After the existing `_compute_forbidden_ag_set_pair` call site (around `harness.py:22528`), add the helper near the top of the module (~line 4800-5000 where other AG helpers live):

```python
def filter_ags_by_forbidden_set_pre_generation(
    ags: list[dict],
    forbidden_pair,
) -> tuple[list[dict], list[dict]]:
    """Plan 9 Task 9 — drop AGs whose source_cluster_signatures
    match the forbidden set BEFORE generate_proposals_from_strategy
    fires. Pre-Plan-9 the harness only consulted the forbidden set
    as a post-generation collision check, wasting LLM calls on AGs
    that would have been rejected anyway.

    Returns (surviving_ags, dropped_ags). Dropped AGs get a
    typed AG_PREFILTERED_BY_FORBIDDEN_SET decision record at the
    caller; this helper is pure data manipulation.
    """
    by_signature = getattr(forbidden_pair, "by_signature", frozenset())
    if not by_signature:
        return (list(ags), [])

    surviving: list[dict] = []
    dropped: list[dict] = []
    for ag in ags:
        sigs = set(str(s) for s in (ag.get("source_cluster_signatures") or []))
        if sigs and sigs.intersection(by_signature):
            dropped.append(ag)
        else:
            surviving.append(ag)
    return (surviving, dropped)
```

- [ ] **Step 4: Wire the helper into the per-iteration loop**

At `harness.py:22500-22600`, just after `_forbidden_pair = _compute_forbidden_ag_set_pair(reflection_buffer)`, insert:

```python
            # Plan 9 Task 9 — drop forbidden AGs BEFORE generation.
            _surviving_ags, _prefiltered_ags = (
                filter_ags_by_forbidden_set_pre_generation(
                    ags_for_this_iteration, _forbidden_pair,
                )
            )
            if _prefiltered_ags:
                logger.info(
                    "plan9.prefiltered_ags count=%d ag_ids=%s",
                    len(_prefiltered_ags),
                    [ag.get("id", "?") for ag in _prefiltered_ags],
                )
                # Emit one decision record per dropped AG.
                for _dropped_ag in _prefiltered_ags:
                    _current_iter_inputs.setdefault(
                        "decision_records", [],
                    ).append({
                        "decision_type": "AG_PREFILTERED_BY_FORBIDDEN_SET",
                        "ag_id": str(_dropped_ag.get("id", "")),
                        "signatures": list(
                            _dropped_ag.get("source_cluster_signatures") or []
                        ),
                        "reason": "matches_forbidden_signature",
                    })
            ags_for_this_iteration = _surviving_ags
```

(The exact name of the AG list local — `ags_for_this_iteration` above — may be different in current code. Use `rg "for ag in " harness.py | grep -A1 "iteration_counter"` to find it.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_harness_forbidden_set_pre_generation.py -v`
Expected: 3 passed.

- [ ] **Step 6: Run the existing forbidden-set + collision-guard tests to confirm no regression**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_forbidden_set.py tests/unit/test_forbidden_ag_collision.py tests/unit/test_reflection_admitted_to_forbidden_set.py -v`
Expected: all passed.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/harness.py \
        packages/genie-space-optimizer/tests/unit/test_harness_forbidden_set_pre_generation.py
git commit -m "$(cat <<'EOF'
plan9(t9): apply forbidden-set filter before proposal generation

AGs whose source_cluster_signatures match the forbidden set are
dropped before generate_proposals_from_strategy fires.
Saves LLM calls on retired signatures.

Postmortem signal: AG_PREFILTERED_BY_FORBIDDEN_SET decision
record per dropped AG.
EOF
)"
```

---

### Task 10: Delete `pick_archetype`, `_derive_asset_slice_from_afs`, `ARCHETYPES`, `Archetype`

**Rationale:** Reviewer's "delete pick_archetype" recommendation. After T1–T6, every L5b/L6 path can build its slice from `RepairProposal.target_objects` (via `llm_direct_slice_resolver`), its prompt fragment from `RepairShape` (via `_repair_shape_fragments`), and its output validator contract from `RepairProposal.required_constructs`. The catalog is dead code. T10 deletes it cleanly. Keep `_ARCHETYPE_NAME_TO_SHAPE` as a deprecated compatibility mapping in `repair_intent.py` for postmortem readers of pre-Plan-9 traces.

**THIS TASK ONLY LANDS IN PR2, AFTER PR1 (T1–T9) HAS DEPLOYED AND TELEMETRY SHOWS ≥80% OF ANCHORS IN `plan5_intent_materialized` STATUS.**

**Files:**

- Delete: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/archetypes.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent.py` (move `_ARCHETYPE_NAME_TO_SHAPE` to deprecated section; remove `intent_from_archetype`)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/cluster_driven_synthesis.py:637-720` (remove `pick_archetype` branch entirely; signature drops `archetype` return value)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/synthesis.py` (delete archetype-driven branches)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/preflight_synthesis.py` (delete archetype-driven branches)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent_synthesizer.py` (tighten Pydantic schema: `target_objects` becomes required for non-OTHER shapes)
- Delete: archetype-only tests under `tests/unit/test_archetypes.py`, `tests/unit/test_pick_archetype.py`, `tests/unit/test_archetype_catalog.py`
- Test: `packages/genie-space-optimizer/tests/unit/test_archetype_catalog_deleted.py`

- [ ] **Step 1: Write the failing test for catalog deletion**

Create `packages/genie-space-optimizer/tests/unit/test_archetype_catalog_deleted.py`:

```python
"""Plan 9 Task 10 — archetype catalog is deleted.

Verifies that pick_archetype, ARCHETYPES, and the Archetype class
no longer exist; that _ARCHETYPE_NAME_TO_SHAPE is the only
catalog artifact retained (deprecated for postmortem readers
of pre-Plan-9 traces).
"""
import pytest


def test_archetype_module_does_not_exist():
    with pytest.raises(ModuleNotFoundError):
        import genie_space_optimizer.optimization.archetypes  # noqa: F401


def test_pick_archetype_does_not_exist_in_repair_intent():
    from genie_space_optimizer.optimization import repair_intent
    assert not hasattr(repair_intent, "pick_archetype")
    assert not hasattr(repair_intent, "intent_from_archetype")
    assert not hasattr(repair_intent, "Archetype")


def test_pick_archetype_does_not_exist_in_cluster_driven_synthesis():
    from genie_space_optimizer.optimization import cluster_driven_synthesis
    assert not hasattr(cluster_driven_synthesis, "pick_archetype")
    assert not hasattr(cluster_driven_synthesis, "_derive_asset_slice_from_afs")


def test_repair_shape_name_compat_map_retained_for_postmortem_replay():
    """Pre-Plan-9 serialized intents (with archetype names) must
    still be readable by postmortem tools."""
    from genie_space_optimizer.optimization.repair_intent import (
        _ARCHETYPE_NAME_TO_SHAPE,
        RepairShape,
    )
    assert _ARCHETYPE_NAME_TO_SHAPE.get("top_n_by_metric") == (
        RepairShape.TOP_N_BY_METRIC
    )
    assert _ARCHETYPE_NAME_TO_SHAPE.get("ordered_list_by_metric") == (
        RepairShape.ORDERED_LIST_BY_METRIC
    )
```

- [ ] **Step 2: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_archetype_catalog_deleted.py -v`
Expected: 3 FAILs (the module / functions still exist).

- [ ] **Step 3: Delete `archetypes.py`**

```bash
rm packages/genie-space-optimizer/src/genie_space_optimizer/optimization/archetypes.py
```

- [ ] **Step 4: Remove `intent_from_archetype` and `Archetype` from `repair_intent.py`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent.py`. Find the `intent_from_archetype` function (around line 262) and the `Archetype` TYPE_CHECKING import (line 230). Delete both. Move `_ARCHETYPE_NAME_TO_SHAPE` into a `# Deprecated — Plan 9 postmortem compat only` section with a docstring:

```python
# ─── Deprecated — Plan 9 postmortem compat only ────────────────────────
#
# Plan 9 deleted the archetype catalog. This mapping is retained ONLY so
# postmortem tools can read pre-Plan-9 traces (where archetype.name
# appeared in serialized intents). New code MUST NOT use this mapping;
# the LLM emits RepairShape directly via RepairProposal.repair_shape.
#
# Removed entirely once postmortem tools migrate to repair_shape-only
# readers (tracked in roadmap.md "Post-Plan-9 telemetry checklist").
_ARCHETYPE_NAME_TO_SHAPE: dict[str, RepairShape] = {
    "simple_enumerate": RepairShape.OTHER,
    "ordered_list_by_metric": RepairShape.ORDERED_LIST_BY_METRIC,
    "top_n_by_metric": RepairShape.TOP_N_BY_METRIC,
    "group_by_all_projected_keys": RepairShape.OTHER,
    "period_over_period": RepairShape.PERIOD_OVER_PERIOD,
    "correct_join_spec": RepairShape.JOIN_DISCOVERY,
    "cohort_retention": RepairShape.OTHER,
    "funnel_conversion": RepairShape.OTHER,
    "ratio_by_dimension": RepairShape.OTHER,
    "running_total": RepairShape.OTHER,
    "rank_within_group": RepairShape.RANK_WITHIN_GROUP,
    "pct_change": RepairShape.PERIOD_OVER_PERIOD,
    "filter_compose": RepairShape.FILTER_COMPOSE,
    "segment_compare": RepairShape.OTHER,
    "disambiguate_column": RepairShape.COLUMN_DESCRIPTION,
    "time_window_aggregate": RepairShape.PERIOD_OVER_PERIOD,
    "self_join_hierarchy": RepairShape.JOIN_DISCOVERY,
    "event_sequence": RepairShape.OTHER,
    "distinct_count_by_dim": RepairShape.OTHER,
    "pivot_wide": RepairShape.OTHER,
}
# ───────────────────────────────────────────────────────────────────────
```

- [ ] **Step 5: Delete `pick_archetype` + `_derive_asset_slice_from_afs` archetype branches in `cluster_driven_synthesis.py`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/cluster_driven_synthesis.py`. Delete the `pick_archetype` function definition (search for `def pick_archetype`). Delete the `_derive_asset_slice_from_afs` archetype branch — keep only the Plan-9 LLM-direct branch added in T6:

```python
def _derive_asset_slice_from_afs(
    afs: dict,
    metadata_snapshot: dict,
    *,
    column_k: int = PREFLIGHT_COLUMN_COVERAGE_K,
    repair_proposal: "RepairProposal | None" = None,
) -> tuple[AssetSlice, None] | None:
    """Plan 9 — slice derivation comes from RepairProposal.target_objects.
    Returns (slice, None) on success; None when target_objects cannot
    be resolved (the caller emits PLAN5_ANCHOR_ACTIVATION_V1 with
    status=plan5_intent_validator_rejected).
    """
    if repair_proposal is None or not repair_proposal.target_objects:
        return None
    from genie_space_optimizer.optimization.llm_direct_slice_resolver import (
        resolve_target_objects_to_asset_slice,
        UnknownTargetObjectError,
    )
    try:
        slice_ = resolve_target_objects_to_asset_slice(
            repair_proposal.target_objects, metadata_snapshot,
        )
        return (slice_, None)
    except UnknownTargetObjectError:
        return None
```

Delete every reference to `archetype.prompt_template`, `archetype.output_shape`, `archetype.name`, and `archetype.applicable_root_causes` in `cluster_driven_synthesis.py`. Replace with `repair_proposal.repair_shape` (via `fragment_for`) and `repair_proposal.required_constructs`.

- [ ] **Step 6: Delete archetype branches in `synthesis.py`**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/synthesis.py`. Remove the `archetype` parameter from `synthesize_example_sqls`, `check_output_shape`, and any other function that previously took it. Every reference to `archetype.prompt_template` / `archetype.output_shape` / `archetype.name` becomes a `RepairProposal.repair_shape` / `RepairProposal.required_constructs` / `RepairProposal.intent_name` reference.

Use `rg "archetype\." packages/genie-space-optimizer/src/genie_space_optimizer/optimization/synthesis.py` to enumerate every remaining call site.

- [ ] **Step 7: Delete archetype branches in `preflight_synthesis.py`**

Same shape as Step 6 for `preflight_synthesis.py`. Delete `archetype_name`, `archetype_prompt_template`, `archetype_output_shape` references from the prompt context dict. Plan 9 preflight uses `RepairProposal.intent_name` / `fragment_for(repair_shape)` / `RepairProposal.required_constructs`.

- [ ] **Step 8: Tighten `LlmRepairProposalOutput` Pydantic schema**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/repair_intent_synthesizer.py`. Add a Pydantic validator on `LlmRepairProposalOutput` requiring `target_objects` non-empty when `repair_shape != "other"` and `patch_type` is structural:

```python
    from pydantic import model_validator

    @model_validator(mode="after")
    def _require_target_objects_for_structural_shapes(self):
        structural_patch_types = {
            "add_example_sql",
            "add_sql_snippet_filter",
            "add_sql_snippet_expression",
            "add_sql_snippet_measure",
            "add_join_spec",
            "update_join_spec",
            "add_column_description",
            "update_column_description",
        }
        if (
            self.patch_type in structural_patch_types
            and self.repair_shape != "other"
            and not self.target_objects
        ):
            raise ValueError(
                f"Plan 9 — target_objects required for patch_type={self.patch_type!r} "
                f"and repair_shape={self.repair_shape!r} (only 'other' may omit)."
            )
        return self
```

- [ ] **Step 9: Delete archetype-only tests**

```bash
git rm packages/genie-space-optimizer/tests/unit/test_archetypes.py
git rm packages/genie-space-optimizer/tests/unit/test_pick_archetype.py
git rm packages/genie-space-optimizer/tests/unit/test_archetype_catalog.py
# Verify no other test imports from archetypes:
rg "from genie_space_optimizer.optimization.archetypes" packages/genie-space-optimizer/tests
```

If any other test imports from `archetypes`, update it to use `repair_shape` / `RepairProposal` typed contracts.

- [ ] **Step 10: Run the catalog-deletion test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_archetype_catalog_deleted.py -v`
Expected: 4 passed.

- [ ] **Step 11: Run the full GSO test suite to confirm no regression**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit tests/integration -v --ignore=tests/integration/test_e2e_intentionally_xfail.py`
Expected: all passed. Snapshot tests may need regeneration (`--snapshot-update`).

- [ ] **Step 12: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
plan9(t10): delete archetype catalog

After T1-T6, every L5b/L6 path uses RepairProposal.target_objects
(slice), RepairShape (prompt fragment), and required_constructs
(validator contract) — all LLM-emitted. The 21-shape catalog is
dead code; deleted.

Kept: _ARCHETYPE_NAME_TO_SHAPE in repair_intent.py as deprecated
postmortem-compat mapping for pre-Plan-9 trace readers.

Deleted:
  - archetypes.py (entire file)
  - Archetype dataclass, ARCHETYPES list, pick_archetype()
  - intent_from_archetype() adapter
  - _derive_asset_slice_from_afs archetype branch
  - archetype_* references in synthesis.py + preflight_synthesis.py
  - test_archetypes.py, test_pick_archetype.py, test_archetype_catalog.py

Tightened: LlmRepairProposalOutput requires target_objects non-empty
for structural patch_types when repair_shape != 'other'.

Closes Plan 8's deferred catalog removal.
EOF
)"
```

---

### Task 11: Flip `plan7_rollback_learning_enabled()` default to ON

**Rationale:** Reviewer's claim #4 fix. `plan7_rollback_learning_enabled()` at `config.py:8184-8198` currently defaults to `"false"`, whereas Plans 3/4/5/6 default-on via `_flag_default_on(...)`. Plan 7 is the cross-iteration learning loop — rolled-back hypotheses from iteration N ground iteration N+1's typed synthesis. With Plan 9's wire-in + materialization fixes, proposals now reliably carry `repair_intent` stamps, so `hypothesize_next_attempts_for_iteration` finally receives the typed inputs it needs.

**Files:**

- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py:8184-8198`
- Test: `packages/genie-space-optimizer/tests/unit/test_plan7_rollback_learning_default_on.py`

- [ ] **Step 1: Write the failing test**

Create `packages/genie-space-optimizer/tests/unit/test_plan7_rollback_learning_default_on.py`:

```python
"""Plan 9 Task 11 — plan7_rollback_learning_enabled defaults ON."""
import os

from genie_space_optimizer.common.config import (
    plan7_rollback_learning_enabled,
)


def test_plan7_rollback_learning_default_on(monkeypatch):
    monkeypatch.delenv("GSO_PLAN7_ROLLBACK_LEARNING", raising=False)
    assert plan7_rollback_learning_enabled() is True


def test_plan7_rollback_learning_explicit_off_disables(monkeypatch):
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "0")
    assert plan7_rollback_learning_enabled() is False


def test_plan7_rollback_learning_explicit_on_enables(monkeypatch):
    monkeypatch.setenv("GSO_PLAN7_ROLLBACK_LEARNING", "1")
    assert plan7_rollback_learning_enabled() is True
```

- [ ] **Step 2: Run the failing test**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_plan7_rollback_learning_default_on.py -v`
Expected: FAIL on `test_plan7_rollback_learning_default_on` (currently defaults to False).

- [ ] **Step 3: Flip the default**

Edit `packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py:8184-8198`. Replace the function:

Current:
```python
def plan7_rollback_learning_enabled() -> bool:
    """…
    False (default): no hypothesis call; existing learning behaviour
    is byte-stable.
    """
    raw = (
        os.environ.get("GSO_PLAN7_ROLLBACK_LEARNING") or "false"
    ).strip().lower()
    return raw in _TRUTHY_VALUES
```

Replacement:
```python
def plan7_rollback_learning_enabled() -> bool:
    """Whether the Plan-7 rollback-learning helper runs.

    Plan 9 — default flipped to ON. With Plan 9's wire-in fixes,
    proposals reliably carry repair_intent stamps so the
    hypothesizer receives the typed inputs it needs.

    True (default): when stages.acceptance returns a rolled_back
    outcome, the harness dispatches one LLM hypothesis call per
    rolled-back cluster and stamps surviving hypotheses onto
    metadata_snapshot["_last_attempt_hypothesis_by_cluster"].

    Set GSO_PLAN7_ROLLBACK_LEARNING=0 to force off (escape hatch
    for emergency rollback).
    """
    return _flag_default_on("GSO_PLAN7_ROLLBACK_LEARNING")
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_plan7_rollback_learning_default_on.py -v`
Expected: 3 passed.

- [ ] **Step 5: Run Plan 7 integration tests to confirm no regression**

Run: `cd packages/genie-space-optimizer && uv run pytest tests/unit/test_plan7_rollback_learning_end_to_end.py tests/unit/test_rollback_learning.py tests/unit/test_hypothesize_next_attempts_for_iteration.py -v`
Expected: all passed.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py \
        packages/genie-space-optimizer/tests/unit/test_plan7_rollback_learning_default_on.py
git commit -m "$(cat <<'EOF'
plan9(t11): default plan7_rollback_learning to ON

Plan 9's wire-in + materialization fixes (T1-T6) ensure proposals
reliably carry repair_intent stamps, so Plan 7's hypothesizer
finally receives the typed inputs it needs.

Closes reviewer claim #4. GSO_PLAN7_ROLLBACK_LEARNING=0 remains
as escape hatch.
EOF
)"
```

---

### Task 12: Roadmap closure + Plan 8 supersession banner

**Rationale:** Plan 9 closes the last remaining open questions on `roadmap.md`. T12 marks Plans 1–9 as historical, lists the four Plan 9 deferred items (above) for post-deployment review, and adds a "Post-Plan-9 telemetry checklist" listing the markers operators monitor for the first 5 deploys. T12 also adds a short banner to Plan 8 noting that the deferred catalog removal is now superseded by Plan 9.

**Files:**

- Modify: `packages/genie-space-optimizer/docs/llmdrivenarchitecture/roadmap.md`
- Modify: `packages/genie-space-optimizer/docs/llmdrivenarchitecture/2026-05-19-plan-8-deployment-readiness-and-cleanup.md` (add supersession banner at the top)

- [ ] **Step 1: Add supersession banner to Plan 8**

Edit `packages/genie-space-optimizer/docs/llmdrivenarchitecture/2026-05-19-plan-8-deployment-readiness-and-cleanup.md`. After line 5 (the `---` after the writing-plans header banner), insert a new banner:

```markdown
## SUPERSEDED IN PART BY PLAN 9 (2026-05-19)

Plan 8 v1's "Out of scope" list (above) included the deterministic-fallback retirements: inlining the heuristic body of `cluster_failures` (Plan 4 fallback removal) and deleting the closed-vocab `archetypes.py` catalog (Plan 5 fallback removal). The user's "keep deterministic fallbacks live as safety net" instruction explicitly deferred these.

After Plan 8 deployed, two postmortems (airline `59a173d3`, 7now `ab65fefe`) showed 0.0pp accuracy delta on both spaces with the architectural cause traced to `pick_archetype` doing three load-bearing jobs (slice + prompt + validator) that gate every structural repair regardless of whether Plan 5 fired. Plan 9 (`2026-05-19-plan-9-catalog-removal-and-llm-driven-structural-realization.md`) inverts those three jobs into LLM-emitted typed contracts and deletes the catalog. Plan 9 supersedes Plan 8's deferred archetype removal; Plan 9 also closes the wire-in gap that Plan 8 left at `harness.py:23302+`, `harness.py:23410+`, and `harness.py:23923+`.

Plan 8's typed-contract foundation (`RepairProposal`, `RepairIntent`, `CritiqueOutcome`, `NextAttemptHypothesis`) remains the bedrock Plan 9 builds on.

---
```

- [ ] **Step 2: Update roadmap.md**

Edit `packages/genie-space-optimizer/docs/llmdrivenarchitecture/roadmap.md`. Find the open-questions section and close each one with a resolved decision:

```markdown
## Resolved decisions (post-Plan-9)

| # | Question | Plan 9 decision |
|---|----------|-----------------|
| 1 | Should we retire the archetype catalog? | YES — deleted in Plan 9 Task 10. Replaced by RepairProposal.target_objects (slice), RepairShape-keyed prompt fragments (prompt), RepairProposal.required_constructs (validator contract). |
| 2 | Should we retire stage-1-discovery? | NO — Plan 5's intent synthesizer subsumes lever picking only AFTER stage-1 narrows the AG to a target_objects slice. Retiring stage-1 would force Plan 5 to ingest full schema per AG, breaking OTPM budget. (Same decision as Plan 8.) |
| 3 | Model overrides per skill or single global LLM_MODEL? | Single LLM_MODEL env var. (Closed in Plan 8 Task 11.) |
| 4 | Promote _last_attempt_hypothesis_by_cluster to typed field? | YES — promoted to FailureCluster.last_attempt_hypothesis in Plan 8 Task 8. |
| 5 | Default flag flip for Plan 7? | YES — flipped to ON in Plan 9 Task 11. |
| 6 | Close RepairShape vocabulary? | DEFERRED — Plan 9 keeps RepairShape.OTHER as the catalog-deletion safety net. Re-evaluate once telemetry shows OTHER usage drops below 5% for 5+ deploys. |
| 7 | Close NextAttemptHypothesis.failure_mode vocabulary? | DEFERRED — same as #6. Open-vocab annotations are postmortem-readable; typed fields (revised_repair_shape, revised_patch_type) carry actionable signal. |
| 8 | Close LlmCluster.semantic_theme vocabulary? | DEFERRED — same as #6. |
| 9 | Promote forbidden_signatures to ForbiddenPatternDescriptor? | DEFERRED — deterministic patch_retry_signature filter already enforces LLM-cannot-invent invariant. Re-evaluate if validated-subset filter is over-pruning. |
| 10 | Add F9 learning bug fix? | YES — fixed in Plan 8 Task 10. |
| 11 | Per-rolled-back-intent vs per-cluster Plan 7 hypothesis? | DEFERRED — Plan 7 emits one hypothesis per cluster covering every rolled-back intent. Per-intent split is a future plan if telemetry shows per-cluster aggregation loses signal. |
| 12 | Wire Plan 1-7 into production? | YES — Plan 8 v1 wired contracts; Plan 9 closed the remaining wire-in gap (harness.py:23302+, 23410+, 23923+). |
| 13 | Cross-cluster hypothesis correlation? | DEFERRED — Plan 7 sees one cluster at a time. Cross-AG correlation is a future plan. |
| 14 | Default critique gate enforcing? | YES — flipped to ON in Plan 8 Task 4. |

## Post-Plan-9 telemetry checklist (first 5 deploys)

Monitor these markers per lever-loop run:

1. **`PLAN5_ANCHOR_ACTIVATION_V1` distribution** — at least 80% of anchors should be in `plan5_intent_materialized` status. Anchors in `plan5_intent_declined` or `plan5_intent_validator_rejected` should each be <10%. Anchors with no marker are bugs; file an issue.
2. **`no_archetype_or_slice` decline reason** — should be ZERO occurrences (catalog deleted; this reason is no longer emittable). If it appears, a deletion in T10 was incomplete.
3. **`lever6_force_llm_declined`** — should be REPLACED by `PLAN5_ANCHOR_ACTIVATION_V1` with status `plan5_intent_validator_rejected` and a typed reason. If the legacy marker appears, the L6 forcing path wasn't wired to the activation markers.
4. **`plan7_rollback_learning=true`** in the per-iteration flag dump.
5. **Structural gate ABSENT + 0.0 rejections** — at least one rejection over the first 5 deploys confirms T7 is working; if zero rejections fire, no degenerate patches are being produced (good) OR the gate isn't running (bad — investigate).
6. **Headline accuracy** — at least one of airline / 7now should show non-zero accuracy delta on the first run. If both show zero, postmortem will cite the specific typed-decline markers; that's actionable signal for the next plan.

## Open vocabularies — re-evaluation triggers

The four deferred decisions (#6, #7, #8, #9) all hinge on telemetry. Re-evaluation trigger: when the most-common open-vocab string label recurs 3+ times across runs OR when the open-vocab field's usage in postmortem queries crosses a threshold (TBD by operator). At that point, a follow-up plan can promote the open vocab to a closed enum without touching Plan 9's structural changes.
```

- [ ] **Step 3: Commit**

```bash
git add packages/genie-space-optimizer/docs/llmdrivenarchitecture/roadmap.md \
        packages/genie-space-optimizer/docs/llmdrivenarchitecture/2026-05-19-plan-8-deployment-readiness-and-cleanup.md
git commit -m "$(cat <<'EOF'
plan9(t12): close roadmap; mark Plan 8 catalog removal superseded

All 14 roadmap open questions resolved. Plan 8 gets a supersession
banner noting Plan 9 closes its deferred archetype removal and
wire-in gap.

Post-Plan-9 telemetry checklist documents the markers operators
should monitor for the first 5 deploys.
EOF
)"
```

---

## Acceptance & Cutover

### PR1 acceptance (Streams A + B + C: T1–T9)

- All new unit tests added in T1–T9 pass.
- Full GSO test suite passes (`uv run pytest tests/unit tests/integration --ignore=tests/integration/test_e2e_intentionally_xfail.py`).
- One validation deploy of PR1 to a real workspace.
- Postmortem from the validation deploy shows ≥80% of anchors in `plan5_intent_materialized` status; zero `no_archetype_or_slice` markers in the new typed activation markers; `lever6_force_llm_declined` either disappears or is replaced by typed `plan5_intent_validator_rejected` markers with cited reasons; `plan7_rollback_learning=true` in flag dump (since T11 lands in PR2, this checkmark applies to PR2's validation deploy).

### PR2 acceptance (Streams D + E: T10–T12)

- All Plan 9 unit tests still pass after catalog deletion.
- `test_archetype_catalog_deleted.py` passes (Plan 9 Task 10 invariant).
- Full GSO test suite passes (snapshot tests regenerated as needed).
- One validation deploy of PR2 to a real workspace.
- Plan 7 rollback hypotheses appear in the new iteration's RCA evidence stamps.

### Headline acceptance (post-PR2 lever-loop run on airline + 7now)

- Every anchor records exactly one `PLAN5_ANCHOR_ACTIVATION_V1` marker.
- Zero `no_archetype_or_slice` / `no_top_n_archetype` markers (catalog deleted).
- `lever6_force_llm_declined` does not appear (replaced by typed decline markers).
- Structural gate rejects at least one patch with `emitted_patch_shape=ABSENT` + `repairability=0.0`.
- At least one of airline / 7now shows non-zero accuracy delta on this run. If both show zero, postmortem will cite the specific typed-decline markers (not "no archetype") and we have actionable signal for the next plan.

**Plan 9 does not promise accuracy gain by itself.** It promises the system stops being silently inert: every anchor records typed status, every decline has a typed reason, every materialization is direct from the LLM-emitted patch_body. The architecture becomes diagnosable.

---

## Self-Review

**Spec coverage:**

- ✅ Reviewer's claim #1 (wire-in gap for `generate_proposals_from_strategy` and `_select_lever_5_holistic_path`): T4.
- ✅ Reviewer's claim #2 (wire-in gap for `_force_lever6_proposal_for_ag`): T5.
- ✅ Reviewer's claim #3 (candidate critique runs too late): Partially addressed — T6 ensures proposals carry `repair_intent` stamps so the critique stage at `harness.py:24984` finally has typed inputs to ground its verdict. Moving critique earlier in the loop is deferred; T8's `PLAN5_ANCHOR_ACTIVATION_V1` marker is the postmortem source of truth for anchors that never reach critique.
- ✅ Reviewer's claim #4 (Plan 7 default off): T11.
- ✅ Reviewer's claim #5 (structural gate fail-open): T7.
- ✅ Reviewer's three-job inversion #1 (slice): T1 (TargetObject + target_objects field) + T6 (llm_direct_slice_resolver).
- ✅ Reviewer's three-job inversion #2 (prompt): T2 (RepairShape-keyed fragments).
- ✅ Reviewer's three-job inversion #3 (validator): T3 (required_constructs).
- ✅ Reviewer's "delete pick_archetype": T10 (in PR2).
- ✅ Reviewer's "one free-form OTHER fragment safety net": T2 (OTHER fragment with target_objects + rationale + no-invented-identifiers constraints; pinned by `test_repair_shape_fragments_other_safety_net`).
- ✅ My own "L6 materialization decoration": T6 (replaces legacy body call with `to_proposal_dict()`).
- ✅ My own "PLAN5_ANCHOR_ACTIVATION_V1 markers": T8.
- ✅ My own "forbidden-set filtering earlier": T9.
- ✅ My own "Plan 7 default-on with the wire-in landed": T11.
- ✅ Roadmap closure + Plan 8 supersession: T12.

**Placeholder scan:** Searched plan for "TBD" / "TODO" / "implement later" / "add appropriate" / "similar to Task N" / "fill in details" — only intentional `# placeholder` in T8 Step 8 (`test_plan9_activation_markers_all_anchors_covered.py` placeholder body, called out explicitly as "fill when fixtures land"). All other steps have concrete code blocks.

**Type consistency:**

- ✅ `TargetObject` (T1) used consistently in `RepairIntent.target_objects` (T1), `RepairProposal.target_objects` (T1), `LlmRepairProposalOutput.target_objects` (T1), `llm_direct_slice_resolver` (T6), `_derive_asset_slice_from_afs` (T6, T10).
- ✅ `RepairShape` (existing) used consistently in `REPAIR_SHAPE_FRAGMENTS` (T2), `fragment_for` (T2), `_derive_asset_slice_from_afs` (T6, T10), the L5b/L6 fallback branches (T6).
- ✅ `RepairProposal.required_constructs` (T3) referenced consistently in `check_output_shape` (T3) and the post-T10 archetype-deleted validator (T10).
- ✅ `ActivationStatus` (T8) referenced consistently in `_dispatch_lever_5b_for_cluster` (T8), `dispatch_lever_6_with_intent` (T8), and the postmortem checklist (T12).
- ✅ `to_proposal_dict()` referenced consistently in T6 L6 path, T6 L5b path, and `RepairProposal` (existing).
- ✅ `_ARCHETYPE_NAME_TO_SHAPE` (T10) explicitly kept in `repair_intent.py` for postmortem compat; the function `intent_from_archetype` is deleted (T10).

**Coverage gap check:** None — every reviewer claim, every three-job inversion job, and every independent finding has at least one task that implements it.

---

## Execution Handoff

Plan complete and saved to `packages/genie-space-optimizer/docs/llmdrivenarchitecture/2026-05-19-plan-9-catalog-removal-and-llm-driven-structural-realization.md`.

Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks. Fast iteration; clean isolation of each task's commit.

**2. Inline Execution** — Execute tasks in this session using executing-plans; batch execution with checkpoints for your review.

**Note on PR structure:** T1–T9 land together as PR1. T10–T12 land together as PR2 AFTER one validation deploy of PR1 shows healthy `PLAN5_ANCHOR_ACTIVATION_V1` markers (≥80% materialized; zero `no_archetype_or_slice`). Both PRs include their tests and commit per task; PR1 ~9 commits, PR2 ~3 commits.

Which approach?
