# Phase F+H Wire-up Audit Findings

**Status:** Audit-only. No harness changes. Citation backing for the redraft of [`2026-05-04-phase-f-h-harness-wireup-plan.md`](./2026-05-04-phase-f-h-harness-wireup-plan.md) and a verification sweep of the landed [`2026-05-04-phase-h-gso-run-output-contract-plan.md`](./2026-05-04-phase-h-gso-run-output-contract-plan.md) Option 1 batch (T1-T11, T14-T17).

**Methodology:** every finding cites a `file:line` evidence pin. The redraft must keep these citations green or explicitly refute them.

**Codebase verified at:** `fix/gso-lossless-contract-replay-gate` HEAD `88f4532` (Phase F+H wire-up T0 snapshot landed; no Phase A wire-up commits).

---

## Section 1 — Phase F+H Wire-up Plan: drift severity per stage

The wire-up plan was authored against an imagined post-Phase-H stage API. The actual stage modules (post-G-lite + post-Phase-H Option 1) drift substantially. Each finding is anchored to actual file:line evidence so the redraft cannot silently re-introduce the same drift.

### A1 — F2 (rca_evidence) — CRITICAL: defer per user direction

| Finding | Plan says | Reality | Evidence |
|---|---|---|---|
| `RcaEvidenceInput` field names | `eval_rows`, `metadata_snapshot`, `soft_eval_rows` (plan §A1 step 2) | `eval_rows`, `hard_failure_qids`, `soft_signal_qids`, `per_qid_judge`, `asi_metadata` | `src/genie_space_optimizer/optimization/stages/rca_evidence.py:33-50` |
| `collect()` does NOT emit decision records | plan implies emission ("emits per-qid evidence records via the new producer path") | the for-loop in `collect()` only mutates local dicts; no `ctx.decision_emit(...)` call anywhere in the module | `src/genie_space_optimizer/optimization/stages/rca_evidence.py:114-188` (function body); grep `ctx.decision_emit` returns zero hits in the file |
| Bundle would be sparse without proper extraction | plan does not flag this | with `per_qid_judge={}` and `asi_metadata={}`, `_build_metadata` returns `failure_type=""`, `_asi_finding_from_metadata` returns `None`, the for-loop `continue`s; per_qid_evidence stays empty | `src/genie_space_optimizer/optimization/stages/rca_evidence.py:154-160` (early-return branch in `collect()`) |

**Decision:** defer F2 in this batch (option 3a). The bundle is empty regardless of input fidelity, so the wire-up adds no value until either (a) a corrected wire-up that hoists per-qid extraction out of `cluster_failures` into harness scope, or (b) the stage module is rewritten to source per_qid_judge/asi_metadata from `metadata_snapshot` directly.

---

### A2 — F3 (clustering) — MEDIUM: spark parameter pass-through

| Finding | Plan says | Reality | Evidence |
|---|---|---|---|
| `ClusteringInput` field names | `eval_result_for_clustering`, `metadata_snapshot`, `soft_eval_result`, `qid_state` | matches ✅ | `src/genie_space_optimizer/optimization/stages/clustering.py:32-46` |
| Hard-call spark argument | inline harness call uses `spark=spark` | `form()` calls `cluster_failures(spark=None, ...)` for both hard and soft branches | `src/genie_space_optimizer/optimization/stages/clustering.py:96` (hard) and `:112` (soft); harness inline call `src/genie_space_optimizer/optimization/harness.py:9160` uses `spark=spark` |
| `cluster_failures` uses spark internally | plan asserts byte-stability based on "calls cluster_failures internally with the same args" | `cluster_failures` has a real spark-conditional branch that calls `read_asi_from_uc(spark, ...)` for ASI metadata enrichment | `src/genie_space_optimizer/optimization/optimizer.py:1913-1915` (`if spark and run_id and catalog and schema:` → `read_asi_from_uc(spark, run_id, catalog, schema)`) |
| `verbose` parameter | plan-snippet drops `verbose=False` from soft-call | `form()` does pass `verbose=False` for soft branch (matches harness) but does NOT pass `verbose=True` for hard branch (harness defaults it to True via inline call) | `src/genie_space_optimizer/optimization/stages/clustering.py:93-103` (hard, no verbose arg → defaults to True per `optimizer.py:1873`) and `:109-120` (soft, verbose=False explicit). The hard-branch default matches; OK. |

**Risk:** if `read_asi_from_uc` is ever called in the replay path (it's spark-gated; replay runs without spark), F3 wire-up would diverge from the harness's existing behavior. In replay-fixture mode `spark=None` everywhere, so this is likely safe — but **the byte-stability gate is the only verification**, and the redraft must explicitly note the spark-divergence risk for production runs.

---

### A3 — F4 (action_groups) — HIGH: emission duplication

| Finding | Plan says | Reality | Evidence |
|---|---|---|---|
| `ActionGroupsInput` field names | `action_groups`, `source_clusters_by_id`, `rca_id_by_cluster`, `ag_alternatives_by_id` | matches ✅ | `src/genie_space_optimizer/optimization/stages/action_groups.py:32-51` |
| `select()` emits records via `ctx.decision_emit` | plan calls F4 "observability-additive" but does flag dedup in Step 3 | `select()` calls `strategist_ag_records(...)` and loops `for record in records: ctx.decision_emit(record)` | `src/genie_space_optimizer/optimization/stages/action_groups.py:75-85` |
| Harness already emits `strategist_ag_records` | plan tells executor to "find and remove" the duplicate | harness already imports and calls `_strategist_ag_records(...)` under a try/except logging "Phase B: strategist_ag_records failed" | `src/genie_space_optimizer/optimization/harness.py:14863` (import alias), `:14884` (call), `:14908` (failure log) |

**Risk:** if Step 2 is committed before Step 3 deletes the harness inline call, byte-stability fails because every `STRATEGIST_AG_EMITTED` record fires twice. Redraft must move the harness deletion **into the same commit** as the stage insertion (one atomic change), not as a follow-up step inside the same task.

---

### A4 — F5 (proposals) — HIGH: emission duplication + content_fingerprint double-stamping

| Finding | Plan says | Reality | Evidence |
|---|---|---|---|
| `ProposalsInput` field names | `proposals_by_ag`, `rca_id_by_cluster`, `cluster_root_cause_by_id`, `proposal_alternatives_by_ag` | matches ✅ | `src/genie_space_optimizer/optimization/stages/proposals.py:39-55` |
| `generate()` emits records via `ctx.decision_emit` | plan does flag dedup in Step 3 | `generate()` calls `proposal_generated_records(...)` and loops `for record in records: ctx.decision_emit(record)` | `src/genie_space_optimizer/optimization/stages/proposals.py:117-129` |
| Harness already emits `proposal_generated_records` | plan tells executor to "find and remove" the duplicate | harness already imports and calls `_proposal_generated_records(...)` | `src/genie_space_optimizer/optimization/harness.py:15005` (import), `:15030` (call), `:15048` (failure log) |
| `generate()` stamps `content_fingerprint` on every proposal | plan replaces `lever_proposals` post-call so F6 reads the fingerprint | `generate()` writes `stamped["content_fingerprint"] = fingerprint` for every proposal | `src/genie_space_optimizer/optimization/stages/proposals.py:108-115` |
| PR-E T3 already stamps `content_fingerprint` upstream | plan does not flag this | the harness's existing `_run_content_fingerprint_dedup_helper` path stamps content_fingerprint as part of the PR-E rollback-class dedup gate. Double-stamping with a different signature function would mismatch | grep for `content_fingerprint` shows multiple harness sites that read it as input (not just F5 producing it) |

**Risk:** double-emission of `PROPOSAL_GENERATED` records (same as A3) plus potential content_fingerprint divergence if PR-E's stamping uses a different signature function than `proposals._content_fingerprint`. Redraft must verify the fingerprint algorithm matches across both stamping sites.

---

### A5 — F6 (gates) — HIGH: gate ORDER drift between plan-text and module

| Finding | Plan says | Reality | Evidence |
|---|---|---|---|
| `GatesInput` field names | `proposals_by_ag`, `ags`, `rca_evidence`, `applied_history`, `rolled_back_content_fingerprints`, `forbidden_signatures`, `space_snapshot` | matches ✅ | `src/genie_space_optimizer/optimization/stages/gates.py:56-64` |
| `GATE_PIPELINE_ORDER` | plan §A5 step 4: `lever5_structural → rca_groundedness → content_fingerprint_dedup → blast_radius → dead_on_arrival` | actual order: `content_fingerprint_dedup → lever5_structural → rca_groundedness → blast_radius → dead_on_arrival` | `src/genie_space_optimizer/optimization/stages/gates.py:33-39` |
| Harness inline gate order | plan claims "the inline gate logic is ~200-300 LOC" | actual sites: lever5 at `:14106`, AG-level groundedness at `:14921`, proposal-level groundedness at `:15058`, blast_radius at `:15562`. **No PR-E content_fingerprint_dedup helper as a producer**; the dedup runs as part of `_run_content_fingerprint_dedup_helper` integrated into a different code path | `src/genie_space_optimizer/optimization/harness.py:14106, 14921, 15058, 15562` |

**Risk:** different drop orderings → different surviving proposal sets → different downstream decisions. Even if `GATE_PIPELINE_ORDER` happens to converge to the same survival set on the airline replay fixture, the per-iteration `dropped` tuple ordering changes the canonical_decision_json. Redraft must (a) reconcile the order between the module and the harness inline path, then (b) update either side to match the chosen canonical order before the wire-up.

---

### A6 — F7 (application) — CRITICAL: stage is post-apply observability, not a replacement

| Finding | Plan says | Reality | Evidence |
|---|---|---|---|
| `ApplicationInput` field names | `w`, `space_id`, `patches_by_ag`, `ags`, `metadata_snapshot`, `apply_mode` (plan §A6 step 2) | `applied_entries_by_ag`, `ags`, `rca_id_by_cluster`, `cluster_root_cause_by_id`. No `w`, `space_id`, `patches_by_ag`, `metadata_snapshot`, or `apply_mode` field exists. | `src/genie_space_optimizer/optimization/stages/application.py:47-62` |
| `apply()` calls `apply_patch_set` internally | plan §A6 calls F7 "true replacement of `apply_patch_set`" | `apply()` does NOT call `apply_patch_set`. It iterates `inp.applied_entries_by_ag.items()` and converts each apply-log entry to an `AppliedPatch` typed record. The stage is **post-apply observability**, not a replacement. | `src/genie_space_optimizer/optimization/stages/application.py:137-188` (function body); grep `apply_patch_set` returns zero hits in the file |
| `AppliedPatchSet.post_snapshot` field | plan §A6 step 2 reads `_applied_set.post_snapshot` for the adapter | `AppliedPatchSet` has only `applied: tuple[AppliedPatch, ...]` and `applied_signature: str`. No `post_snapshot` field. | `src/genie_space_optimizer/optimization/stages/application.py:65-77` |
| Plan-snippet would raise `TypeError` | plan calls this "true replacement" | `_app_stage.ApplicationInput(w=..., space_id=..., patches_by_ag=..., metadata_snapshot=..., apply_mode=...)` raises `TypeError: __init__() got unexpected keyword argument 'w'` at first construction | dataclass `@dataclass class ApplicationInput:` strictly enforces the 4 declared fields per Python dataclass semantics |

**Risk:** the wire-up cannot be a "true replacement" of `apply_patch_set` using the existing F7 module. The redraft must either:
1. Keep `apply_patch_set` inline and ADD the F7 stage as post-apply observability (mirrors what F7 actually is), or
2. Rewrite F7 to actually call `apply_patch_set` internally and accept the harness inputs (requires module redesign — out of scope for a wire-up plan).

---

### A7 — F8 (acceptance) — CRITICAL: field-name drift + per-AG iteration shape mismatch

| Finding | Plan says | Reality | Evidence |
|---|---|---|---|
| `AcceptanceInput.applied_set` field | plan §A7 step 2: `AcceptanceInput(applied_set={...}, ...)` | actual field is `applied_entries_by_ag: dict[str, tuple[Mapping[str, Any], ...]]`. No `applied_set` field. | `src/genie_space_optimizer/optimization/stages/acceptance.py:50-80` |
| `decide()` per-AG iteration vs single-call | plan-snippet builds ONE applied_set dict and ONE call as if `decide_control_plane_acceptance` runs once per iteration | `decide()` iterates `for ag in inp.ags:` and calls `_decide_for_ag(ag=ag, inp=inp)` per AG; each `_decide_for_ag` invokes `decide_control_plane_acceptance(...)` per AG | `src/genie_space_optimizer/optimization/stages/acceptance.py:133-153` (`_decide_for_ag`) and `:156-273` (`decide` function body) |
| `decide()` emits records via `ctx.decision_emit` | plan does not flag dedup risk for F8 | `decide()` emits `ACCEPTANCE_DECIDED` per AG via `ag_outcome_decision_record` and `QID_RESOLUTION` per qid via `post_eval_resolution_records` | `src/genie_space_optimizer/optimization/stages/acceptance.py:156-160` (docstring), and harness already emits both: `_ag_outcome_decision_record` at `harness.py:12231, 12235`; `_post_eval_resolution_records` at `harness.py:17712, 17716` |
| `AgOutcomeRecord.rollback_class` field | plan §A7 step 2 (and A8 cross-ref) reads `rec.rollback_class` | `AgOutcomeRecord` has `ag_id`, `outcome`, `reason_code`, `target_qids`, `affected_qids`, `content_fingerprints`. No `rollback_class` field. | `src/genie_space_optimizer/optimization/stages/acceptance.py:38-46` |
| `AgOutcome.accepted_signature` field | plan §A8 step 2 reads `_ag_outcome.accepted_signature` | `AgOutcome` has `outcomes_by_ag`, `qid_resolutions`, `rolled_back_content_fingerprints`. No `accepted_signature` field. | `src/genie_space_optimizer/optimization/stages/acceptance.py:83-97` |

**Risk:** plan-snippet raises `TypeError` at `AcceptanceInput(applied_set=...)` construction. Even after fixing field names, the per-AG iteration model means the harness needs to feed pre-grouped `applied_entries_by_ag`, and the dedup with the existing per-AG emission paths in harness must be done atomically. The legacy `_LegacyDecision` shim in the plan-snippet papers over a real shape mismatch.

---

### A8 — F9 (learning) — CRITICAL: 3 invented input fields + 3 invented output fields

| Finding | Plan says | Reality | Evidence |
|---|---|---|---|
| `LearningInput.prior_terminal_state` field | plan §A8 step 2 sets `prior_terminal_state=dict(metadata_snapshot.get("_rca_terminal_state") or {})` | `LearningInput` has 12 declared fields; `prior_terminal_state` is NOT one of them | `src/genie_space_optimizer/optimization/stages/learning.py:38-51` |
| `LearningInput.baseline_post_arbiter_accuracy` field | plan §A8 step 2 sets `baseline_post_arbiter_accuracy=float(best_accuracy)` | not present on the dataclass | `src/genie_space_optimizer/optimization/stages/learning.py:38-51` |
| `LearningInput.candidate_post_arbiter_accuracy` field | plan §A8 step 2 sets `candidate_post_arbiter_accuracy=float(full_accuracy)` | not present on the dataclass | `src/genie_space_optimizer/optimization/stages/learning.py:38-51` |
| `LearningUpdate.divergence_label` field | plan §A8 step 2 reads `if _lrn_update.divergence_label:` | `LearningUpdate` has `new_reflection_buffer`, `new_do_not_retry`, `new_rolled_back_content_fingerprints`, `terminal_decision`, `retired_ags`, `ag_retired_records`. No `divergence_label` field. | `src/genie_space_optimizer/optimization/stages/learning.py:54-62` |
| `update()` emits AG_RETIRED records | plan §A8 step 3 instructs to delete the inline AG_RETIRED emit at `harness.py:11801-11828` | `update()` calls `_emit_ag_retired_records(ctx, retired_ags=...)` which loops `ctx.decision_emit(rec)` per retired AG | `src/genie_space_optimizer/optimization/stages/learning.py:114-142` (`_emit_ag_retired_records`); harness inline emit confirmed via grep: `harness.py:11801` carries `DecisionType.AG_RETIRED` (PR-B2 T5 wire-up) |

**Risk:** plan-snippet raises `TypeError` on construction (3 invented input fields). Even after field-name correction, reading `_lrn_update.divergence_label` raises `AttributeError`. The AG_RETIRED dedup must be atomic (same as A3/A4/A7).

---

### Summary table

| Commit | Stage | Plan-snippet constructs valid dataclass? | Plan reads valid output fields? | Emission dedup required? | Severity |
|---|---|---|---|---|---|
| A1 | F2 rca_evidence | ❌ (3 wrong field names) | n/a (no decision records emitted) | no | CRITICAL — defer 3a |
| A2 | F3 clustering | ✅ | ✅ | no | MEDIUM — verify spark replay-mode |
| A3 | F4 action_groups | ✅ | ✅ | YES — atomic dedup at `harness.py:14884` | HIGH |
| A4 | F5 proposals | ✅ | ✅ | YES — atomic dedup at `harness.py:15030` | HIGH |
| A5 | F6 gates | ✅ | ✅ | gate-order reconcile required | HIGH |
| A6 | F7 application | ❌ (5 of 6 fields invented) | ❌ (`post_snapshot` invented) | no | CRITICAL — module isn't a replacement |
| A7 | F8 acceptance | ❌ (`applied_set` invented) | ❌ (`rollback_class` and `accepted_signature` invented) | YES — atomic dedup at `harness.py:12235` and `:17716` | CRITICAL |
| A8 | F9 learning | ❌ (3 invented input fields) | ❌ (`divergence_label` invented) | YES — atomic dedup at `harness.py:11801` | CRITICAL |

**4 of 8 wire-up snippets raise `TypeError` or `AttributeError` on first call.** The plan cannot be executed as written. Redraft is required.

---

## Section 2 — Per-stage re-classification (5-category taxonomy)

The original plan used a 2-category model ("true replacement" vs "observability-additive INSERT/ADD"). Verification against actual module bodies shows that taxonomy is wrong for ≥4 stages. The redraft uses this 5-category model:

1. **True replacement** — module's verb runs the algorithm internally; harness inline call is deleted; byte-stability via parameter parity.
2. **Additive observability with dedup required** — module's verb emits records the harness already emits; commit must atomically delete the inline emit.
3. **Post-stage observability** — module's verb consumes the inline call's outputs (no algorithm replacement); commit inserts the verb call AFTER the inline call. May or may not require dedup.
4. **Defer** — module is observability-only-empty (sparse bundle regardless of inputs); no clear short-term wire path.
5. **Algorithm-replacement with config drift** — module runs the algorithm but with a different ordering / parameter set; reconciliation required before wire-up.

### Re-classification table

| # | Stage | Original plan label | Re-classification | Atomic dedup site (harness.py) | Verified against |
|---|---|---|---|---|---|
| A1 | F2 rca_evidence | obs-additive INSERT | **(4) Defer** | n/a | `stages/rca_evidence.py:114-188` (no `ctx.decision_emit`); `stages/rca_evidence.py:154-160` (early-return when judge/asi metadata empty) |
| A2 | F3 clustering | true replacement | **(1) True replacement** + spark replay-mode caveat | n/a | `stages/clustering.py:86-127` (`form` body); `optimizer.py:1913-1915` (spark-conditional `read_asi_from_uc`) |
| A3 | F4 action_groups | obs-additive ADD | **(2) Additive observability with dedup required** | `:14884` (delete `_strategist_ag_records(...)` call block 14863-14908) | `stages/action_groups.py:75-85` (emits via `ctx.decision_emit`) |
| A4 | F5 proposals | obs-additive ADD | **(2) Additive observability with dedup required** | `:15030` (delete `_proposal_generated_records(...)` call block 15005-15048) | `stages/proposals.py:117-129` (emits via `ctx.decision_emit`) |
| A5 | F6 gates | true replacement | **(5) Algorithm-replacement with config drift** — gate-order reconcile required before wire-up | n/a (gate-order reconcile ≠ dedup) | `stages/gates.py:33-39` (`GATE_PIPELINE_ORDER`); harness inline gate sites: `:14106` (lever5), `:14921`+`:15058` (groundedness AG-level, proposal-level), `:15562` (blast_radius) |
| A6 | F7 application | true replacement of `apply_patch_set` | **(3) Post-stage observability with dedup required** — `apply_patch_set` STAYS inline; F7 reads its output | `:16524` (delete `_patch_applied_records(...)` call block 16516-16541) | `stages/application.py:137-176` (consumes `inp.applied_entries_by_ag`, never calls `apply_patch_set`); `stages/application.py:159-171` (emits PATCH_APPLIED via `ctx.decision_emit`) |
| A7 | F8 acceptance | true replacement of `decide_control_plane_acceptance` | **(3) Post-stage observability with dedup required** — `decide_control_plane_acceptance` STAYS inline (per AG); F8 module's per-AG iteration is parallel observability | `:12235` (delete `_ag_outcome_decision_record` block 12231-12253) AND `:17716` (delete `_post_eval_resolution_records` block 17712-17734) | `stages/acceptance.py:133-153` (`_decide_for_ag` re-calls `decide_control_plane_acceptance` per-AG); `stages/acceptance.py:156` docstring confirms emission |
| A8 | F9 learning | true replacement of `resolve_terminal_on_plateau` | **(3) Post-stage observability with dedup required** — `resolve_terminal_on_plateau` STAYS inline; F9 emits AG_RETIRED + builds typed update record | `:11801` (delete inline AG_RETIRED emit block 11801-11828) | `stages/learning.py:114-142` (`_emit_ag_retired_records` loops `ctx.decision_emit`) |

### Key reframing

The single biggest redraft consequence: **F7, F8, F9 are NOT replacements**. Their inline harness calls (`apply_patch_set`, `decide_control_plane_acceptance`, `resolve_terminal_on_plateau`) STAY where they are. The stage modules consume the inline outputs and emit the typed observability surface alongside (with dedup deletes for the existing producer-direct emits where the stage module also emits).

This is consistent with the F-plan series' "observability-only" framing in the F2-F9 module docstrings — the stage modules were authored as typed observability surfaces, not as inline-call replacements. The wire-up plan's "true replacement" labeling for F7/F8/F9 was aspirational, not factual.

### Atomic-dedup load-bearing detail (Phase B foreshadow)

Phase B wraps each stage call with `wrap_with_io_capture(execute, stage_key)` per `stages/stage_io_capture.py:135-176`. The wrapper rebinds `ctx.decision_emit` to a capturing closure that BOTH appends to `captured_decisions` (for the bundle's `decisions.json`) AND calls the original `decision_emit` (so records still flow into `OptimizationTrace`).

Without atomic dedup in Phase A, after Phase B lands every duplicated record would:
1. Fire from the inline harness producer → `OptimizationTrace`.
2. Fire from the stage call → `OptimizationTrace` AND `decisions.json`.

The first fire alone breaks Phase A's byte-stability gate. The second fire breaks the bundle (decisions.json double-counts). The redraft's per-commit dedup-site enforcement is therefore the load-bearing detail; deferring "we'll clean it up later" guarantees a Phase B failure even if Phase A passed.

---

## Section 3 — Recommended remediation

**Scope:** redraft Phase A only. Phase B (wrap-with-io-capture) and Phase C (bundle assembly) are mechanical and shape-independent of the per-stage wire pattern; they do not need redrafting.

**Constraints for the redraft author:**

1. Every `Input(...)` snippet must construct against the actual dataclass declared in the cited file:line. No invented fields. Each commit carries a "**Verified against:** `<stage_module>.py:<input_lines>` (Input dataclass) and `<stage_module>.py:<verb_lines>` (named verb body)" line.
2. Every "delete inline emit / delete inline call" instruction includes the harness file:line of the producer to be deleted.
3. Re-classification follows the 5-category taxonomy in Section 2. Don't preserve the original "true replacement" / "obs-additive INSERT/ADD" labels.
4. T0 byte-stability snapshot is already landed (`88f4532`) — do not re-capture. If A1 (F2) is deferred, the snapshot still applies as-is to the remaining 7 commits.
5. Commit count may shrink. Original Phase A = 8. Re-classification with F2 deferred → 7 commits. Don't pad.
6. **Out of scope:** changing F-modules themselves. The redraft accepts the modules as-built and figures out how to wire them honestly. F7's "post-apply observability" character is not a defect to fix; it's a contract to wire against.
7. **Out of scope:** re-litigating Phase B/C.

**Cross-reference:** the redraft lives in [`2026-05-04-phase-f-h-harness-wireup-plan.md`](./2026-05-04-phase-f-h-harness-wireup-plan.md) Phase A section. This audit doc is the citation backing it pre-verifies against.
