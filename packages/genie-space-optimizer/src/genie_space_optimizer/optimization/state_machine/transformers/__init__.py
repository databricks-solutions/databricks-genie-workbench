"""State machine transformers — typed-contract catalog.

The production state machine is the only path from a hard QID to an
APPLIED patch. Every stage in the funnel is governed by exactly one
transformer (or one transformer + one ValidationGate pair). This module
docstring is the **single source of truth** for what each transformer
takes in, what it puts out, and which terminal reasons it can emit.

Production wiring lives in ``state_machine/registry.py`` (PHASE3_REGISTRY).

═══════════════════════════════════════════════════════════════════════
Transformer contracts (post-Phase-3 deletion of routing_gate /
escalation_ladder; pre-Phase 4 harness cutover)
═══════════════════════════════════════════════════════════════════════

dispatch_input.build_initial_states_from_eval_rows  [ADMIT — not a
    funnel transformer, runs once per iteration before the SM]
    INPUT:    raw MLflow eval rows + optional quarantine / exclude
    OUTPUT:   tuple[QuestionStateInIteration] at stage HARD_QID_SEEN
    TERMINAL: (none — terminal happens inside the SM)

diagnose_llm.plan11_stage1_diagnosis  [HARD_QID_SEEN → DIAGNOSED]
    INPUT:    QuestionStateInIteration at HARD_QID_SEEN; ctx must
              carry baseline_eval_rows for the matched qid
    OUTPUT:   QuestionStateInIteration at DIAGNOSED with DiagnosedRecord
    TERMINAL: OPTIMIZER_NO_CANDIDATES (on Stage 1 abstain or LLM error)

cluster_batch.plan11_stage2_clustering  [DIAGNOSED → CLUSTERED]
    INPUT:    QuestionStateInIteration at DIAGNOSED
    OUTPUT:   QuestionStateInIteration at CLUSTERED with ClusteredRecord
    TERMINAL: PLAN11_CLUSTER_DECLINED (LLM declined or returned empty)

synthesize_llm.plan11_stage3_synthesis  [CLUSTERED → PROPOSED]
    INPUT:    QuestionStateInIteration at CLUSTERED
    OUTPUT:   QuestionStateInIteration at PROPOSED with a typed
              RepairProposal stored in ctx.proposal_store
    TERMINAL: PLAN11_SYNTHESIZE_DECLINED, PLAN11_SYNTHESIZE_CONTRACT_VIOLATION

structural_repair_gate  [PROPOSED → NORMALIZED]
    INPUT:    QuestionStateInIteration at PROPOSED
    OUTPUT:   QuestionStateInIteration at NORMALIZED with NormalizedRecord
    TERMINAL: STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY,
              STRUCTURAL_GATE_REJECTED

blast_radius_batch  [NORMALIZED → APPLYABLE]
    INPUT:    QuestionStateInIteration at NORMALIZED
    OUTPUT:   QuestionStateInIteration at APPLYABLE
    TERMINAL: BLAST_RADIUS_REJECTED, BLAST_RADIUS_DROPPED_TO_NARROW

narrow_replacement_gate  [NORMALIZED → APPLYABLE — runs after blast]
    INPUT:    QuestionStateInIteration at NORMALIZED dropped by blast
    OUTPUT:   QuestionStateInIteration at APPLYABLE with scoped artifact
    TERMINAL: NARROW_REPLACEMENT_DECLINED

applier_gate  [APPLYABLE → APPLIED]
    INPUT:    QuestionStateInIteration at APPLYABLE; ctx.w + ctx.space_id
              for the live Genie API; ctx.proposal_store for the typed
              RepairProposal corresponding to the latest attempt
    OUTPUT:   QuestionStateInIteration at APPLIED with AppliedRecord
    TERMINAL: applyability_rejected (recycles to PROPOSED for retry until
              Phase 3 deletes the routing_gate / escalation_ladder lane;
              after Phase 3, applyability_rejected is a final terminal
              for the QID this iteration)

evaluated_gate  [APPLIED → EVALUATED]
    INPUT:    QuestionStateInIteration at APPLIED
    OUTPUT:   QuestionStateInIteration at EVALUATED with eval scores
    TERMINAL: EVAL_GATE_INFRASTRUCTURE_FAILURE

acceptance_gate  [EVALUATED → ACCEPTED | ROLLED_BACK]
    INPUT:    QuestionStateInIteration at EVALUATED; baseline + post-apply
              accuracy from ctx
    OUTPUT:   QuestionStateInIteration at ACCEPTED (gain >= threshold) or
              ROLLED_BACK (regression or no gain)
    TERMINAL: ACCEPTANCE_REJECTED_REGRESSION,
              ACCEPTANCE_REJECTED_TARGET_UNCHANGED

═══════════════════════════════════════════════════════════════════════
Removed in SM Cutover Phase 3 (2026-05-23)
═══════════════════════════════════════════════════════════════════════

routing_gate         — replaced by deterministic CLUSTERED → synthesize
                       wiring; no in-SM ladder branching.
escalation_ladder    — replaced by single linear sequence; a declined
                       stage terminates the QID for this iteration.

See ``docs/llmdrivenarchitecture/v3/2026-05-21-optimizer-state-machine-design.md``
for the rationale; see ``state_machine/registry.py`` for the wiring.
"""
