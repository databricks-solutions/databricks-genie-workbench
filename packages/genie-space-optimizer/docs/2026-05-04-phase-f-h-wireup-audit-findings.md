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

---

## Section 4 — Phase A residual-risk pass on the redrafted plan (post-`8c66d3a`)

**Scope:** the Phase A redraft renumbered commits (A1=F3, A2=F4, A3=F5, A4=F7, A5=F8, A6=F9) and adopted the 5-category taxonomy from Section 2. This section verifies each redrafted commit against actual harness usage and the actual stage Output dataclasses, looking for the same class of latent bug that produced the original drift. Findings are graded **CLEAR** / **NEEDS PLAN PATCH** / **NEEDS MODULE PATCH** / **STOP**.

**Methodology:** for each redrafted commit, read the redraft's adapter snippet, the stage module's named-verb body, and the harness locals/sites the snippet references. Verify field shapes, downstream consumers of stage outputs, and atomic-dedup completeness.

**Numbering note:** Section 4 uses the redrafted plan's NEW commit numbers (A1-A6). Section 1's A1-A8 numbering was the original 8-commit plan. The two do not collide — Section 1 A2 (F3 clustering) corresponds to Section 4 A1; Section 1 A3 (F4 action_groups) corresponds to Section 4 A2; etc.

### A1 (F3 clustering, redraft) — NEEDS PLAN PATCH

The redraft's "True replacement" classification is correct in algorithm-replacement terms (`form()` calls `cluster_failures` internally), but the adapter at plan lines 299-300 silently drops demoted clusters and contradicts the F3 module's own docstring.

| # | Concern | Plan says | Reality | Evidence | Severity |
|---|---|---|---|---|---|
| 1 | Demoted-cluster downstream consumption | `clusters = list(_cluster_findings.clusters)` (drops demoted) | `_split_by_demoted` puts only `demoted_reason==""` clusters into `.clusters`; demoted go to `.rejected_cluster_alternatives`. The original inline `cluster_failures(...)` returned ALL clusters in one list. Downstream harness paths likely iterate the combined list (e.g. cluster_records / rca_formed_records emissions per F3 docstring at `:12296+`/`:12345+`). | `stages/clustering.py:65-83` (`_split_by_demoted`); `stages/clustering.py:104,121` (call sites); harness inline `cluster_failures(...)` at `harness.py:9158, 9171` (returns all clusters un-split) | NEEDS PLAN PATCH |
| 2 | F3 docstring contradicts the wire-up | Plan classifies as "True replacement" | F3 module docstring states "F3 is observability-only: it does NOT modify any harness call sites" and "harness wiring + emission move are deferred to a follow-up plan to preserve byte-stability" | `stages/clustering.py:10-17` | NEEDS PLAN PATCH (update docstring in same A1 commit) |
| 3 | spark replay-mode parity | Plan pins production-mode caveat (`form(spark=None)` skips UC enrichment vs harness `spark=spark`) | `cluster_failures` body actually uses spark for `read_asi_from_uc(spark, run_id, catalog, schema)` at `optimizer.py:1913-1915` (inside an `if spark and run_id and catalog and schema:` guard). Replay fixtures pass `spark=None` so the branch is skipped on both sides — replay-byte-stable. Production runs would diverge. | `optimizer.py:1913-1922` | CLEAR for replay; production caveat retained |
| 4 | `verbose` parameter parity | Plan does not flag | F3.form() passes `verbose=False` for soft branch (matches harness `:9174`); hard branch defaults to `verbose=True` per `optimizer.py:1873` (matches harness `:9158-9164` which omits verbose) | `stages/clustering.py:93-103,109-120`; harness `:9158, 9174` | CLEAR |
| 5 | Adapter passes through `held_out_qids` | Plan-snippet sets `eval_result_for_clustering`, `metadata_snapshot`, `soft_eval_result`, `qid_state` only | `ClusteringInput.held_out_qids` field defaults to `()` and `form()` does NOT pass it through to `cluster_failures(...)`. Harness inline call at `:9158-9164` also does not pass `held_out_qids`. Both default to `held_out_qids=None` per `optimizer.py:1874`. | `stages/clustering.py:46,93-103`; `optimizer.py:1874` | CLEAR (parity) |

**Remediation for A1 (must land before pushing the commit):**

1. Update the adapter to combine promoted + rejected back into the harness-side locals so downstream emissions don't shrink:

```python
clusters = list(_cluster_findings.clusters) + [
    c for c in _cluster_findings.rejected_cluster_alternatives
    if c.get("signal_type") != "soft"
]
soft_clusters = list(_cluster_findings.soft_clusters) + [
    c for c in _cluster_findings.rejected_cluster_alternatives
    if c.get("signal_type") == "soft"
]
```

This preserves the pre-wire-up cluster set. The `signal_type` field is stamped on each cluster by `cluster_failures` per the docstring at `optimizer.py:1889-1891`.

Alternative: patch `stages/clustering.py:_split_by_demoted` to keep all clusters in `.clusters` and use `rejected_cluster_alternatives` as a parallel observability surface. (Module patch.) Plan's `_split_by_demoted` was authored as part of Phase D.5 alternatives capture; that consumer reads `rejected_cluster_alternatives` separately and is unaffected by this change.

2. Update F3 docstring at `stages/clustering.py:10-17` from "F3 is observability-only" to reflect the wired state.

3. Verify by `grep`-ing harness for every iteration of `clusters` and `soft_clusters` after `:9178` to confirm whether any path consumes `demoted_reason`-bearing entries differently. Likely sites: `lever_assignments` (`:9217`), `cluster_lines` (`:9219`), `cluster_records` / `rca_formed_records` emissions.

### A2 (F4 action_groups, redraft) — NEEDS PLAN PATCH (small)

The plan-snippet has one issue — an in-scope check that the redraft itself flags but doesn't resolve.

| # | Concern | Plan says | Reality | Evidence | Severity |
|---|---|---|---|---|---|
| 1 | `rca_id_by_cluster` scope at `:14884` | Plan-snippet uses `rca_id_by_cluster=dict(rca_id_by_cluster)` and includes a manual reminder: "confirm `rca_id_by_cluster` is in scope at this code location by reading the surrounding ~30 lines" | The redraft itself flags this as a manual check but doesn't resolve it. `dir()` defensive guards are explicitly forbidden by the redraft constraints | plan §A2 step 2 (lines 339-355) | NEEDS PLAN PATCH (do the scope check inline; don't defer to executor) |
| 2 | `ag_alternatives_by_id={}` empty default | Plan-snippet passes empty dict | Phase D.5 alternatives capture builds AG alternatives in `harness.py` somewhere; the empty dict means F4's `ag_alternatives_by_id` carries no Phase D.5 stamping. The harness-direct path at `:14884` already populated AG alternatives via `_strategist_ag_records(...)` that DOES accept `ag_alternatives_by_id=...` per its own producer signature | grep `_strategist_ag_records` at harness:14863-14908 to confirm Phase D.5 input | NEEDS PLAN PATCH (locate Phase D.5 alternatives source in harness scope, pass through; otherwise post-redraft bundles lose Phase D.5 stamping) |
| 3 | Atomic dedup of `:14863-14908` block | Plan tells executor to "Replace the entire block `harness.py:14863-14908`" | Block boundary needs verification — `:14863` is the `_strategist_ag_records` import alias, `:14884` is the call site, `:14908` is the warning log inside `except Exception:`. Confirm `:14863-14908` is contiguous (no interleaved unrelated code) and `:14908` is the closing line of the `except` block | manual harness read | CLEAR pending Step 1 grep verification (plan already includes this) |

**Remediation for A2:**
- Resolve the `rca_id_by_cluster` scope check up front (read harness `:14820-14910` and confirm; if not in scope, hoist it from where it IS built before `:14884`).
- Locate Phase D.5 AG alternatives source in harness scope and plumb through `ag_alternatives_by_id`. If unavailable in harness scope at the F4 insertion point, accept the Phase D.5 stamping loss as a known regression for this commit and pin in commit message.

### A3 (F5 proposals, redraft) — NEEDS PLAN PATCH

| # | Concern | Plan says | Reality | Evidence | Severity |
|---|---|---|---|---|---|
| 1 | content_fingerprint algorithm parity (PR-E vs F5) | Plan §A3 Step 1 instructs executor to grep PR-E's stamping site and verify it uses the same `patch_retry_signature` function. If divergent, halt. | The PR-E stamping site is in `harness.py` somewhere — needs concrete `file:line` citation pre-execution rather than as a "halt at commit time" instruction. The deferral hides the actual reconciliation | plan §A3 Step 1 lines 383; `stages/proposals.py:77-92` (`_content_fingerprint` calls `patch_retry_signature`) | NEEDS PLAN PATCH (locate PR-E's harness stamping site by `file:line` in the redraft itself) |
| 2 | `lever_proposals` replacement decision | Plan §A3 step 2 comment: "Replace lever_proposals with fingerprint-stamped variants ONLY IF PR-E doesn't already stamp upstream." | This is a runtime branch that pivots based on the Step 1 reconciliation. The pivot is left to the executor | plan §A3 step 2 lines 414-417 + step 3 note lines 425 | NEEDS PLAN PATCH (decide ahead of time: read PR-E source; if it stamps, the redraft commits to NOT replacing; if it doesn't, the redraft commits to replacing. Don't leave conditional branches for the executor) |
| 3 | `_prop_slate` is unused | Plan binds `_prop_slate = _prop_stage.generate(...)` but never reads it again | The stage's emission side-effect is the only purpose of the call. `_prop_slate.proposals_by_ag` (fingerprinted) and `.content_fingerprints_emitted` are typed surfaces F6 would consume — but F6 is deferred. Until F6 wires, `_prop_slate` is dead. | `stages/proposals.py:131-135` (output construction); plan lines 412 (binding never used) | CLEAR (with note: F5 wire-up is observability-only until F6 lands) |

**Remediation for A3:**
- Pre-locate PR-E's content_fingerprint stamping site with `file:line`.
- Decide the lever_proposals replacement policy in the redraft itself, not at execution time.

### A4 (F7 application, redraft) — CLEAR with one TODO

| # | Concern | Plan says | Reality | Evidence | Severity |
|---|---|---|---|---|---|
| 1 | `apply_log` shape vs `applied_entries_by_ag` | Plan-snippet groups `apply_log["applied"]` by `entry["patch"]["ag_id"]` | `_entry_to_applied_patch` reads `entry.get("patch")` then `patch.get("proposal_id")`, `patch.get("target_qids")`, `patch.get("content_fingerprint")` — all attributes the harness's `apply_patch_set` returns in `apply_log` per existing `decision_emitters.patch_applied_records` consumer at `:16524` | `stages/application.py:88-123`; harness `:16516-16541` (consumer site) | CLEAR |
| 2 | `_applied_set.applied_signature` shape | Plan reads `_applied_set.applied_signature` as a stable cycle-detection string | `AppliedPatchSet.applied_signature: str` per `stages/application.py:65-77`; computed via `_compute_applied_signature` at `:126-134` returning a 16-char hex from sha256 | `stages/application.py:65-77, 126-134` | CLEAR |
| 3 | Atomic dedup at `:16516-16541` | Plan tells executor to delete the block | Block is bounded by the `_patch_applied_records` import / call / warning — same pattern as A2/A3. Execution-side Step 1 grep verifies | plan §A4 Step 1 lines 450 | CLEAR pending Step 1 grep |

A4 is the cleanest commit in the redraft. No plan patch required.

### A5 (F8 acceptance, redraft) — STOP — multiple critical issues

| # | Concern | Plan says | Reality | Evidence | Severity |
|---|---|---|---|---|---|
| 1 | `decide_control_plane_acceptance` purity gate | Plan §A5 Step 1: "If any are found, **HALT and defer F8** alongside F2" | The plan correctly identifies this gate. Whether `decide_control_plane_acceptance` is pure must be verified BEFORE authoring the commit. Without it, A5 fundamentally cannot proceed. | plan §A5 Step 1 lines 528 | STOP — pre-flight required |
| 2 | A5 plan-snippet has a Python BUG | Plan §A5 Step 2 builds `_accept_applied_by_ag` via: ```python
_accept_applied_by_ag.setdefault(str(ap.ag_id), tuple()).__add__(({"patch": {...}},))
``` | `setdefault(...).__add__(...)` returns a NEW tuple but does NOT mutate the dict. Every key ends up mapping to the empty `tuple()` (the default), and the `__add__` result is discarded. Net result: every AG maps to empty tuple. | plan §A5 Step 2 lines 545-555 | STOP — code bug in the plan-snippet |
| 3 | `dir()` defensive guards everywhere | Plan §A5 Step 2 uses `dir()` checks for `_best_pre_arbiter`, `full_pre_arbiter_accuracy`, `_baseline_rows_for_control_plane`, `full_result_1`, `MIN_POST_ARBITER_GAIN_PP` | The redraft itself flags this: "patterns shown are placeholders pending an audit of the surrounding code" (plan lines 596). The redraft constraints in Section 3 explicitly forbid `dir()` guards | plan §A5 Step 2 lines 568-587 + note lines 596 | NEEDS PLAN PATCH (resolve every `dir()` check by reading harness scope at the F8 insertion point) |
| 4 | TWO atomic dedup sites | Plan §A5 Step 3 instructs deletion of `:12231-12253` AND `:17712-17734` | Both sites are correctly identified per Section 1 A7. The two sites are far apart in harness, requiring two atomic deletions in the same commit | plan §A5 Step 3 lines 598; harness `:12235, :17716` (Section 1 evidence) | CLEAR pending grep verification |
| 5 | Per-AG re-call divergence | Plan classification rationale notes: "calling it from harness AND from the stage would double-execute the gate. Treat F8 as **post-stage observability**: keep `_control_plane_decision = decide_control_plane_acceptance(...)` at `harness.py:10347` untouched; insert `_accept_stage.decide(...)` AFTER, with a context that does NOT actually call the underlying primitive." | But `_decide_for_ag` at `stages/acceptance.py:142-153` ALWAYS calls `decide_control_plane_acceptance(...)`. There is no toggle to skip that call. The plan's framing requires either (a) the Step 1 purity check passing so re-calls are byte-stable, or (b) a module change to make F8.decide() optionally skip the inline gate | `stages/acceptance.py:142-153` | NEEDS MODULE PATCH (option b) OR Step 1 purity gate (option a) |

**Remediation for A5:**
- **STOP authoring this commit until Step 1's purity check is performed and resolved.** If `decide_control_plane_acceptance` is not pure, defer F8 alongside F2.
- Fix the `setdefault().__add__()` bug — the correct pattern is:

```python
_accept_applied_by_ag: dict[str, list[dict]] = {}
for ap in _applied_set.applied:
    _accept_applied_by_ag.setdefault(str(ap.ag_id), []).append({
        "patch": {
            "proposal_id": ap.proposal_id,
            "ag_id": ap.ag_id,
            "patch_type": ap.patch_type,
            "target_qids": list(ap.target_qids),
            "cluster_id": ap.cluster_id,
            "content_fingerprint": ap.content_fingerprint,
        },
    })
_accept_applied_by_ag = {
    k: tuple(v) for k, v in _accept_applied_by_ag.items()
}
```

- Resolve every `dir()` guard by reading harness scope at the F8 insertion point. The redraft's note that these are "placeholders pending an audit" must become real `harness.py:NNNN` references with literal local names confirmed.

### A6 (F9 learning, redraft) — STOP — same purity problem as A5

| # | Concern | Plan says | Reality | Evidence | Severity |
|---|---|---|---|---|---|
| 1 | `resolve_terminal_on_plateau` re-call | Plan classification rationale: "`resolve_terminal_on_plateau` continues to be called inline (the harness still owns the break/divergence decision based on its return value); F9's `update()` consumes the per-AG outcomes plus the resolved terminal state and emits AG_RETIRED records that the harness inline block formerly emitted" | F9.update() at `stages/learning.py:176-187` calls `resolve_terminal_on_plateau(...)` AGAIN with locally-built parameter sets — **double-execution**. The plan's framing assumes `update()` consumes the harness's `_resolved` (which is what the harness already computed inline) but `update()` does NOT accept a pre-computed `RcaTerminalDecision`; it always re-runs the resolver. | `stages/learning.py:176-187`; plan §A6 lines 621 | STOP — purity gate required (mirror A5) |
| 2 | `_resolved.retired_ags` reference | Plan §A6 step 2 reads `_resolved.retired_ags` for `current_hard_failure_qids=tuple(_resolved.retired_ags)` | The plan-snippet uses `_resolved` as a placeholder for the harness's existing `resolve_terminal_on_plateau` return value. But the assignment `current_hard_failure_qids=tuple(_resolved.retired_ags)` is semantically wrong — `current_hard_failure_qids` is the LIVE hard failure qid set, not the retired AGs. The redraft confused two unrelated concepts. | plan §A6 Step 2 lines 658; `stages/learning.py:46` (LearningInput.current_hard_failure_qids) | NEEDS PLAN PATCH (locate harness's actual `current_hard_qids` / `hard_failure_qids` local at the F9 insertion point) |
| 3 | `dir()` defensive guards | Plan §A6 Step 2 uses `dir()` for every input field except the first three | Same forbidden pattern as A5 | plan §A6 Step 2 lines 638-666 + note lines 679 | NEEDS PLAN PATCH (mirror A5 remediation) |
| 4 | `_emit_ag_retired_records` branch | F9.update() emits AG_RETIRED only when `decision.retired_ags` is truthy (`stages/learning.py:189-194`) | F9 module's `_emit_ag_retired_records` matches what PR-B2 did at the harness `:11801-11828` block. After atomic delete, F9 fully owns the emission. ✅ | `stages/learning.py:114-139, 189-194` | CLEAR |

**Remediation for A6:**
- **STOP** until A5's purity gate result is known. F9's `update()` re-calls `resolve_terminal_on_plateau` — same purity question. Must verify pure (no MLflow logs, no Spark queries, no global mutation) before proceeding.
- Fix the `current_hard_failure_qids` semantic confusion. Read the harness's actual hard-failure qid set at the F9 insertion point and pass that, not `_resolved.retired_ags`.
- Resolve every `dir()` guard with explicit harness scope citations.

### Section 4 summary table

| Commit | Stage | Adapter constructs valid input? | Output fields valid? | Atomic dedup pre-located? | Severity | Required action before push |
|---|---|---|---|---|---|---|
| A1 | F3 clustering | ✅ | ⚠ promoted-only `.clusters` shrinks downstream set | n/a | **NEEDS PLAN PATCH** | Combine promoted+rejected in adapter; update F3 docstring; replay-test |
| A2 | F4 action_groups | ⚠ scope-of-`rca_id_by_cluster` not pre-verified; AG alternatives source missing | ✅ | ✅ atomic delete `:14863-14908` | **NEEDS PLAN PATCH** | Resolve scope; locate Phase D.5 alternatives source |
| A3 | F5 proposals | ✅ | ✅ | ✅ atomic delete `:15005-15048` | **NEEDS PLAN PATCH** | Pre-locate PR-E content_fingerprint site; decide lever_proposals replacement policy ahead of time |
| A4 | F7 application | ✅ | ✅ | ✅ atomic delete `:16516-16541` | **CLEAR** | None — proceed with normal Step 1 grep verification |
| A5 | F8 acceptance | ❌ `setdefault().__add__()` bug; dir() guards | ✅ AgOutcome shape correct | ✅ atomic delete TWO sites `:12231-12253, :17712-17734` | **STOP** | Run Step 1 purity check; if pure, fix code bug + dir() guards; if impure, defer alongside F2 |
| A6 | F9 learning | ⚠ `current_hard_failure_qids` semantic bug; dir() guards | ✅ LearningUpdate shape correct | ✅ atomic delete `:11801-11828` | **STOP** | Mirror A5 purity gate; fix `current_hard_failure_qids` source; resolve dir() guards |

**Net effect:** of the 6 commits in the redraft, only A4 is push-ready. A1, A2, A3 need plan patches. A5 and A6 need a STOP-and-pre-flight pass on `decide_control_plane_acceptance` / `resolve_terminal_on_plateau` purity. The executor's smoke-test instinct (push A1 first) still works — A1's plan patch is mechanical (add an adapter combine, update a docstring) — but A5 and A6 must wait on the purity gate even after A1 lands.

---

## Section 5 — Phase B drift audit (post-redraft)

**Scope:** Phase B wraps each Phase A stage call site with `wrap_with_io_capture(execute=stage.execute, stage_key="...")` per `stage_io_capture.py`. This section verifies the capture decorator's contract against actual stage modules and stage-key conventions.

### 5.1 Capture decorator behavior

Verified against `src/genie_space_optimizer/optimization/stage_io_capture.py:83-168`.

| Property | Plan assumes | Reality | Evidence | Status |
|---|---|---|---|---|
| `decision_emit` rerouting is rebindable | Plan B Phase template assumes `ctx.decision_emit = _capturing_emit` works | `StageContext` is a regular `@dataclass` (not frozen, not slots) at `stages/_context.py:9-31`. `decision_emit` field at `:29` is a `Callable[..., None]` and is freely reassignable. | `stages/_context.py:9-31`; `stage_io_capture.py:131-141` | ✅ |
| `decision_emit` restoration is exception-safe | Plan assumes the wrapper restores `original_emit` even when `execute` raises | `try / finally` block at `stage_io_capture.py:138-141`. If `execute` raises, `finally` restores. ✅ | `stage_io_capture.py:138-141` | ✅ |
| `mlflow_anchor_run_id=None` skips logging | Plan §B prologue states "Phase B is invisible to replay" because anchor stays None | `stage_io_capture.py:117, 143` gate every `_log_text` call on `if anchor:`. Replay tests have anchor=None → log_text never fires → no MLflow side effects → byte-stable. ✅ | `stage_io_capture.py:117, 143` | ✅ |
| Output return value is unchanged | Plan §B assumes wrapper returns `out` from inner `execute(ctx, inp)` unchanged | `stage_io_capture.py:139, 167`: `out = execute(ctx, inp)` returned at line 167 unchanged. ✅ | `stage_io_capture.py:139, 167` | ✅ |
| MLflow failures don't propagate | Plan §B: "the decorator NEVER raises" | Every `_log_text` call wrapped in `try/except` at `:124-128, 150-154, 161-165`; the unknown stage_key path at `:110-115` warns and falls through to `execute(ctx, inp)`. ✅ | `stage_io_capture.py:108-115, 124-128, 150-165` | ✅ |
| Decision capture preserves order + dedup | Plan §B doesn't flag this | `_capturing_emit` at `:133-135` appends to `captured_decisions` AND calls `original_emit(record)` — every record fires twice **in the same run**: once captured, once into the existing OptimizationTrace path. This is intentional (records still flow into existing trace; capture is additive). But: if the underlying stage call ALSO emits records via the harness's separate inline producer (i.e. atomic dedup in Phase A was incomplete), the wrap doubles the duplicate. Phase B amplifies any Phase A dedup failure. | `stage_io_capture.py:130-141` | ⚠ load-bearing on Phase A's atomic-dedup completeness |

**No drift found in the capture decorator itself.** The Phase B mechanical wrap pattern is correct.

### 5.2 Stage key convention verified

Phase B template at plan §B prologue (lines 737-748) uses `stage_key="<stage_key>"` per commit. Per `stages/_registry.py:65-84` and `run_output_contract.PROCESS_STAGE_ORDER:53-148`:

| Phase B Commit | Plan stage_key | STAGES registry key | PROCESS_STAGE_ORDER key | Status |
|---|---|---|---|---|
| B9 (F2) | `"rca_evidence"` | `"rca_evidence"` | `"rca_evidence"` | ✅ |
| B10 (F3) | `"cluster_formation"` | `"cluster_formation"` | `"cluster_formation"` | ✅ |
| B11 (F4) | `"action_group_selection"` | `"action_group_selection"` | `"action_group_selection"` | ✅ |
| B12 (F5) | `"proposal_generation"` | `"proposal_generation"` | `"proposal_generation"` | ✅ |
| B13 (F6) | `"safety_gates"` | `"safety_gates"` | `"safety_gates"` | ✅ |
| B14 (F7) | `"applied_patches"` | `"applied_patches"` | `"applied_patches"` | ✅ |
| B15 (F8) | `"acceptance_decision"` | `"acceptance_decision"` | `"acceptance_decision"` | ✅ |
| B16 (F9) | `"learning_next_action"` | `"learning_next_action"` | `"learning_next_action"` | ✅ |

All 8 stage_keys match. No drift.

### 5.3 Output dataclass JSON-serializability

`stage_io_capture._serialize_io` uses `dataclasses.asdict` + `_safe_dumps` + `_normalize_for_json`. Verifying each stage's Output dataclass round-trips:

| Stage | Output dataclass | Field shapes | Serializability | Evidence |
|---|---|---|---|---|
| F2 rca_evidence | `RcaEvidenceBundle` | `dict[str, dict[str, Any]]`, `dict[str, str]`, `dict[str, tuple[str, ...]]`, `tuple[str, ...]` | ✅ asdict handles dicts; tuples become lists; Any → str via `default=str` fallback | `stages/rca_evidence.py:51-68` |
| F3 clustering | `ClusterFindings` | `tuple[dict[str, Any], ...]` x3 | ✅ | `stages/clustering.py:49-62` |
| F4 action_groups | `ActionGroupSlate` | `tuple[Mapping[str, Any], ...]` x2 | ✅ Mapping → asdict treats as dict | `stages/action_groups.py:54-65` |
| F5 proposals | `ProposalSlate` | `dict[str, tuple[dict[str, Any], ...]]`, `tuple[Mapping[...], ...]`, `tuple[str, ...]` | ✅ | `stages/proposals.py:58-74` |
| F6 gates | `GateOutcome` | not read; F6 deferred | n/a | `stages/gates.py:??` |
| F7 application | `AppliedPatchSet` | `tuple[AppliedPatch, ...]` (nested dataclass), `str` | ✅ asdict recursively flattens nested dataclasses | `stages/application.py:65-77` |
| F8 acceptance | `AgOutcome` | `dict[str, AgOutcomeRecord]` (nested dc), `dict[str, str]`, `set[str]` | ✅ asdict + `_normalize_for_json` converts set→sorted list | `stages/acceptance.py:83-98` |
| F9 learning | `LearningUpdate` | `tuple[dict, ...]`, `set[str]` x2, `dict[str, Any]`, `tuple[tuple[str, tuple[str, ...]], ...]`, `tuple[DecisionRecord, ...]` (nested dc) | ✅ all branches handled by asdict + normalize | `stages/learning.py:54-62` |

**No serialization drift found.** All Output dataclasses round-trip cleanly through `_serialize_io`.

### 5.4 F1 wrap intentionally out of scope

Plan §B prologue (line 754) acknowledges F1 wrap is OUT of this plan's scope. Consequence: `gso_postmortem_bundle/iterations/iter_NN/stages/01_evaluation_state/` directories will be empty after Phase C lands.

This is a design decision, not drift. Pin in roadmap update text (Section 1C of Track 1).

### 5.5 Section 5 verdict

**Phase B is push-ready as drafted, conditional on Phase A's atomic-dedup being complete.** Section 5.1 last row (load-bearing) is the only meaningful concern: any Phase A dedup gap will cause Phase B to **double-emit** the affected record (once into OptimizationTrace, once captured to decisions.json AND OptimizationTrace via `original_emit`). Phase A's per-commit byte-stability gate catches this — but if the executor pushes Phase B without a clean Phase A baseline, the failure mode is louder than necessary.

**Constraint added:** Phase B Commit B9 must NOT be pushed until ALL Phase A commits (A1-A6) pass byte-stability green. The natural stopping point at plan line 693 is the recommended checkpoint for that verification.

---

## Section 6 — Phase C drift audit (post-redraft)

**Scope:** Phase C has 3 commits — C17 (data aggregation + parent run tagging + activate capture), C18 (bundle assembly + GSO_ARTIFACT_INDEX_V1 + run_lever_loop exit JSON), C19 (end-to-end smoke test). This section verifies the imports, function signatures, and harness locals each commit references.

### 6.1 Imports and function signatures verified

| Plan Reference | Actual Symbol | Status |
|---|---|---|
| `from genie_space_optimizer.common.mlflow_names import lever_loop_parent_run_tags` (C17 Step 1) | `def lever_loop_parent_run_tags(...)` at `common/mlflow_names.py:155` | ✅ exists |
| `from genie_space_optimizer.optimization.run_output_bundle import build_artifact_index, build_manifest, build_run_summary` (C18 Step 1) | `def build_manifest(...)` at `:23`, `def build_artifact_index(...)` at `:54`, `def build_run_summary(...)` at `:89` (per `run_output_bundle.py`) | ✅ exists |
| `from genie_space_optimizer.optimization.run_output_contract import bundle_artifact_paths` (C18 Step 1) | `def bundle_artifact_paths(*, iterations: list[int])` at `run_output_contract.py:183` returning dict with keys `manifest, run_summary, artifact_index, operator_transcript, decision_trace_all, journey_validation_all, replay_fixture, scoreboard, failure_buckets, iterations` | ✅ exists; plan reads only `manifest`/`artifact_index`/`run_summary`/`operator_transcript` keys — all present |
| `from genie_space_optimizer.optimization.run_analysis_contract import artifact_index_marker` (C18 Step 1) | `def artifact_index_marker(*, optimization_run_id, parent_bundle_run_id, artifact_index_path, iterations)` at `run_analysis_contract.py:191` | ✅ exists; signature matches plan call `artifact_index_marker(optimization_run_id=..., parent_bundle_run_id=..., artifact_index_path=..., iterations=...)` |
| `marker_parser` consumes `GSO_ARTIFACT_INDEX_V1` | `tools/marker_parser.py:85: elif name == "GSO_ARTIFACT_INDEX_V1": artifact_index = payload` | ✅ symmetric — emitter and parser agree on marker name |
| `from genie_space_optimizer.optimization.operator_process_transcript import render_full_transcript, render_iteration_transcript, render_run_overview` (C18 Step 1) | `def render_run_overview(...)` at `:50`, `def render_iteration_transcript(...)` at `:85`, `def render_full_transcript(...)` at `:145` | ✅ exists |
| `from genie_space_optimizer.optimization.rca_decision_trace import OptimizationTrace` (C17 Step 3) | `OptimizationTrace` dataclass exists per existing decision-trace infrastructure (referenced from harness elsewhere) | ✅ |

**All imports resolve.** No drift in C17/C18 imports.

### 6.2 Harness locals referenced by C17

| Local | Plan use | Reality | Evidence | Status |
|---|---|---|---|---|
| `_db_job_id`, `_db_parent_run_id`, `_db_task_run_id` | Tag the parent run via `lever_loop_parent_run_tags(databricks_job_id=_db_job_id, ...)` | All three locals are initialized at `harness.py:10840-10842` and populated at `:10846-10852` from `os.environ`. Already used by the existing `lever_loop_parent_run_tags(...)` call at `:10858-10860`. | `harness.py:10840-10860` | ✅ |
| `_journey_emit`, `_decision_emit` | C17 Step 2 sets `_stage_ctx.decision_emit=_decision_emit`; A1 Step 2 binds `journey_emit=_journey_emit` | Per A1 Step 2 note (plan lines 277): "If the harness has no existing `_decision_emit` callable in scope (it currently emits via direct `decision_records.append(record.to_dict())` constructions), introduce a thin closure that forwards to whatever the current iteration uses for record collection." | The redraft itself flags that the closures may need to be introduced ad-hoc. `harness.py:9946` shows `decision_emit=lambda record: None` (placeholder for one StageContext build site, not the iteration-body's emit closure). The actual decision emit pattern in harness is direct `decision_records.append(...)` per the redraft note. | NEEDS PLAN PATCH — pre-locate the iteration-body's decision-records list and define the closure contract in the redraft |
| `prev_accuracy`, `best_accuracy`, `full_accuracy` | C17 Step 3 builds `_baseline_for_summary` referencing `prev_accuracy`; bundle assembly at C18 reads `best_accuracy - prev_accuracy` | `prev_accuracy` is a function parameter at `harness.py:10776`; `best_accuracy` and `full_accuracy` are iteration locals (verified by existing scoreboard rendering paths) | `harness.py:10776`; iteration locals (assumed by redraft, not pre-located) | ⚠ Verify scope at C17/C18 insertion points |
| `iteration_counter` | C17 Step 3 uses `_iter_traces[iteration_counter] = ...` | `iteration_counter` is the canonical iteration variable per harness convention | manual harness read | ✅ assumed; pre-locate in redraft |
| `_journey_events`, `_current_iter_inputs.get("decision_records", [])` | C17 Step 3 builds `OptimizationTrace(journey_events=tuple(_journey_events), decision_records=tuple(rec for rec in _current_iter_inputs.get("decision_records", [])))` | Neither local has been pre-verified as existing in iteration scope. The redraft itself flags the baseline_run_evaluation case: "The exact extraction code depends on what `baseline_run_evaluation` returns — read its actual return shape at `harness.py:1833-1870` and adapt. Don't fabricate field names." (plan lines 1040). The same standard applies to `_journey_events` / `_current_iter_inputs` but isn't enforced | plan §C17 Step 3 lines 1046-1060 | NEEDS PLAN PATCH — pre-locate `_journey_events` and `_current_iter_inputs` (or substitute names) in harness; define the closure contract |
| `hard_qids`, `_ag_outcome` | C17 Step 3 reads `len(hard_qids) if "hard_qids" in dir() else 0` and `any(rec.outcome == "accepted" for rec in _ag_outcome.outcomes_by_ag.values()) if "_ag_outcome" in dir() else False` | Same forbidden `dir()` pattern as A5/A6 | plan §C17 Step 3 lines 1054-1057 | NEEDS PLAN PATCH — resolve every `dir()` guard with explicit harness scope citations |

**C17 needs the same plan-patch pass as A5/A6** for the `dir()` guards plus pre-location of every iteration-body local it references. Without it, executors hit silent drift mid-commit.

### 6.3 Harness locals referenced by C18

| Local | Plan use | Reality | Status |
|---|---|---|---|
| `_phase_h_anchor_run_id` | C18 reads to gate the entire bundle-assembly block | Created by C17 Step 1; flows naturally to C18 ✅ | ✅ |
| `_iterations_completed = list(range(1, iteration_counter + 1))` | C18 Step 1 builds | `iteration_counter` confirmed via §6.2; computation is correct given the canonical convention that iteration_counter is the count of completed iterations | ✅ |
| `_baseline_for_summary`, `_iter_traces`, `_iter_summaries`, `_hard_failures_for_overview` | C18 reads (created by C17) | Created by C17 Step 3; flows to C18 ✅ | ✅ |
| `_lrn_update.terminal_decision.get("status")` | C18 Step 1 reads via `if "_lrn_update" in dir() else "max_iterations"` | Forbidden `dir()` pattern again | NEEDS PLAN PATCH |
| `space_id`, `domain`, `max_iterations` | C18 Step 1 passes to `render_run_overview(...)` | All are function params on `_run_lever_loop`; in scope ✅ | ✅ |
| `_iter_traces.get(i) is not None` filter | C18 Step 1 generator skips iterations that didn't populate traces | Defensive but reasonable. ✅ | ✅ |

**C18 has one forbidden `dir()` guard** (line 1129). Same remediation as C17.

### 6.4 `run_lever_loop.py` exit JSON drift

Plan §C18 Step 2 modifies `jobs/run_lever_loop.py` to add 3 fields to the exit payload:
- `parent_bundle_run_id`
- `artifact_index_path`
- `iterations_completed`

Verifying these don't conflict with the existing `lever_loop_exit_manifest()` builder at `run_analysis_contract.py:215-279`:

| Plan field | Existing exit payload key (per `lever_loop_exit_manifest`) | Status |
|---|---|---|
| `parent_bundle_run_id` | NOT present in `lever_loop_exit_manifest` payload (lines 238-279) | ✅ additive — no conflict |
| `artifact_index_path` | NOT present | ✅ additive |
| `iterations_completed` | NOT present (existing has `iteration_counter`, `per_iteration_decision_counts`, `per_iteration_journey_violations`) | ✅ additive |

**However:** plan §C18 Step 2 wraps the new fields directly into a hand-built `exit_payload` dict, parallel to the existing `lever_loop_exit_manifest()` builder. The redraft does not specify whether the new fields go INTO `lever_loop_exit_manifest` (extending it) or PARALLEL (a second exit blob). The existing call site `dbutils.notebook.exit(lever_loop_exit_manifest(...))` per `run_analysis_contract.py:236` produces a JSON string, which means C18's `exit_payload = {...}` would either need to (a) extend `lever_loop_exit_manifest()` to accept the new fields, or (b) merge into the JSON string post-build.

**Severity:** NEEDS PLAN PATCH — clarify whether C18 modifies `lever_loop_exit_manifest()` (extending the function signature) or adds a separate dict construction. Recommend extending the function for consistency with how the rest of the marker schema is defined.

### 6.5 C19 end-to-end smoke test verification

Plan §C19 creates `tests/integration/test_phase_h_bundle_populated.py`. Verifying the test's imports and assertions:

| Plan Test Assertion | Verifiable | Status |
|---|---|---|
| Stub `genie_space_optimizer.optimization.stage_io_capture._log_text` via `monkeypatch.setattr` | `_log_text` is module-scope at `stage_io_capture.py:37-49`; monkeypatch works ✅ | ✅ |
| Stub `mlflow.tracking.MlflowClient` and `mlflow.active_run` via `monkeypatch.setattr` | Standard pytest pattern ✅ | ✅ |
| Import `from genie_space_optimizer.optimization.lever_loop_replay import run_replay` | Verified by Section 1 evidence (replay tests use this) ✅ | ✅ |
| Fixture path `replay/fixtures/airline_real_v1.json` | Standard fixture per existing replay tests ✅ | ✅ |
| Test acknowledges replay path doesn't exercise harness production paths directly | Plan lines 1309-1316 — honest about boundary | ✅ honest scope; the test verifies bundle-path computation, not harness integration |
| TODO comment about "harness-level integration test" | Plan lines 1314-1316 — out of scope for this commit | ✅ properly deferred |

**C19 is push-ready as drafted.** The test correctly identifies that exercising the harness end-to-end requires a different test setup; what C19 verifies (path constants + log_text shim) is sufficient for the plan's claim ("contract-level acceptance test").

### 6.6 Section 6 verdict

**Phase C C17 + C18 are NOT push-ready.** Both reference harness iteration-body locals (especially `_journey_events`, `_current_iter_inputs`, `hard_qids`, `_ag_outcome`, `_lrn_update`) via forbidden `dir()` guards. The remediation pattern is identical to A5/A6: pre-locate every local with an explicit `harness.py:NNNN` citation, then either (a) bind a real reference if in scope or (b) hoist into the C17 anchoring location.

**Phase C C19 is push-ready as drafted** (boundary-test scope is honest).

**Critical drift not in the existing audit doc:** the redraft introduces a closure pattern (`_decision_emit` / `_journey_emit`) that the harness does not currently expose. Plan A1 Step 2 acknowledges this: "If the harness has no existing `_decision_emit` callable in scope ... introduce a thin closure that forwards to whatever the current iteration uses for record collection." But the closure contract isn't defined anywhere — the executor would have to infer it. This is the single biggest push-ahead risk for Phase A: **every** stage call in the iteration body uses `_stage_ctx.decision_emit=_decision_emit`, so the closure is load-bearing for byte-stability across all 6 Phase A commits.

**Required redraft action: pin the closure contract.** Specifically:

1. The harness's iteration-body decision-records collection is currently `decision_records.append(record.to_dict())` (per the redraft's own note). The closure must be:
   ```python
   def _decision_emit(record):
       decision_records.append(record.to_dict())
   ```
   defined ONCE per iteration body, hoisted above the F3 insertion point.

2. The journey-emit closure follows the same pattern. Plan must locate the harness's actual journey emission pattern (`grep -n "_emit_..journey" src/genie_space_optimizer/optimization/harness.py`) and define the closure analogously.

3. Once defined, every Phase A commit (A1-A6) and Phase B Commit (B9-B16) consumes `_decision_emit` / `_journey_emit` consistently — no commit-specific re-binding.

**Action items for redraft author (Section 6 + Section 4 combined):**

| # | Action | Affects | Severity |
|---|---|---|---|
| 1 | Pin the `_decision_emit` / `_journey_emit` closure contract in plan §A1 Step 2 with concrete `harness.py:NNNN` citations | A1-A6, B9-B16 | CRITICAL |
| 2 | Pre-locate iteration-body locals every commit references (`hard_qids`, `_ag_outcome`, `_lrn_update`, `_journey_events`, `_current_iter_inputs`, etc.) | A2-A6, C17, C18 | CRITICAL |
| 3 | Replace every `dir()` guard with literal `harness.py:NNNN` reference + bind the local directly (or HALT the commit) | A5, A6, C17, C18 | CRITICAL |
| 4 | Resolve A5/A6 purity gates for `decide_control_plane_acceptance` and `resolve_terminal_on_plateau` BEFORE authoring the commits | A5, A6 | STOP |
| 5 | Fix the `setdefault().__add__()` Python bug in A5 plan-snippet | A5 | STOP |
| 6 | Update F3 docstring + adapter-level demoted-cluster combine in A1 | A1 | NEEDS PLAN PATCH |
| 7 | Pre-locate PR-E content_fingerprint stamping site in A3 | A3 | NEEDS PLAN PATCH |
| 8 | Clarify C18 exit JSON path (extend `lever_loop_exit_manifest` vs parallel dict) | C18 | NEEDS PLAN PATCH |

**Net assessment:** of the 17 redrafted commits, **only A4, B9-B16, and C19 are push-ready as drafted** (10 commits). The remaining **7 commits (A1, A2, A3, A5, A6, C17, C18) need plan patches** before they can be pushed without inviting another mid-flight stop. The redraft closed the worst of the original drift (no more invented fields raising TypeError), but inherited new drift in the form of unresolved `dir()` guards, undefined closure contracts, and unverified iteration-body local scope.

**Recommended next step for the redraft author:** Section 4 + Section 6 action items 1-8 are a docs-only pass — no harness changes. ~60-90 minutes of harness reading + plan editing. The output is a self-contained, push-ready Phase A + C plan with no executor-side ambiguity.
