# M05 — Drift detection and reconciliation

## 1. Purpose and scope

Classify live-observed, approved and deployed state separately, expose cheap status projections, and orchestrate preserve-before-choice reconciliation. Detect sanctioned bundle dual ownership and scheduled external changes without turning a fresh observed head into an approval. Reconciliation commands request approval/dispatch; this module never sends Genie PATCHes, edits legacy routers or reimplements the observer.

## 2. Exclusive ownership

| Own | Read only |
|---|---|
| New `backend/services/version_control/drift/` | M01 canonicalizer, M02 versions/binding views |
| New `backend/routers/vc_reconcile.py`, `backend/jobs/vc_reconcile.py` | M03 heads, M04 observer/gate and existing `auto_optimize.py` |
| `backend/tests/{test_vc_drift,test_vc_reconcile,test_vc_dual_authority}.py` | M06 approvals/facts, M08 bundle inventory/Job provisioning |

DDL/Volume ownership: **none**. Operation acknowledgements/reconcile facts go through M06. M04 owns edits to existing current-version/drift callers; M08 owns deploy-time guard plumbing.

## 3. Public contract

`DriftService.classify(heads, versions, reachability) -> DriftResult`, `reconcile(binding, action, actor) -> OperationHandle`, `scan(workspace_id, cursor) -> ReconcileBatch`. New overview/status/reconcile routes are in `contracts.md` §7. `DriftResult` carries state, common-base reference, policy target, reasons, observation freshness and allowed_actions. Unresolved/quarantine flags are separate from ordinary drift labels.

Read schema: coordination's three head IDs; version fingerprints/parents; registry governed_by and validated binding; M06 approved rendered target/policy evidence. Normative definitions: `contracts.md` §2/6. Promotion approval comparisons use **target-rendered** fingerprints, not untransformed source hashes. No additional mutable head or drift table. A cached projection has `projection_as_of` and can always be rebuilt.

## 4. Dependencies

Upstream: M04 `Observer.capture` for fresh durable state, M02 lineage/current-binding reader, M01 version-aware comparison, M03 current heads, M06 adoption/reapply approval requests and acknowledgement facts, M08 sanctioned-bundle inventory and local Job dispatcher. Downstream: M04 legacy drift adapters, M07 target drift preflight, M09 badges/actions. M04's fundamental fresh-base check is independent of M05 classification, avoiding a runtime cycle.

## 5. Ordered red → green tasks

| # | RED: first test and assertion | GREEN: minimal implementation |
|---|---|---|
| 1 | `test_three_heads_classify_clean_external_desired_diverged` in `test_vc_drift.py` | Pure fingerprint/lineage truth table with common-base reasoning |
| 2 | `test_missing_history_or_incompatible_canonicalizer_is_unknown` in same file | Preserve existing conservative behavior; no inferred clean state |
| 3 | `test_unreachable_and_unverified_override_ordinary_badges` in same file | Reachability/unresolved precedence with reason fields |
| 4 | `test_capture_advances_observed_but_drift_remains_until_policy_resolution` in `test_vc_reconcile.py` | Observer-first orchestration; approved/deployed unchanged |
| 5 | `test_adopt_creates_approval_request_not_automatic_approved_head` in same file | New M06 request bound to captured base and observed source |
| 6 | `test_reapply_requires_fresh_approval_for_new_base` in same file | Invalidate stale approval reference, issue new request and Job command |
| 7 | `test_acknowledge_records_scoped_divergence_without_fabricating_clean_heads` in same file | Append policy-checked acknowledgement; explicit badge/reason |
| 8 | `test_manual_merge_sql_changes_require_reviewed_new_payload` in same file | Compare/manual workflow only, reject automatic semantic merge |
| 9 | `test_reconcile_scan_paginates_isolates_failures_and_periodically_full_fetches` in same file | Bounded per-binding observation, update_time only optimization |
| 10 | `test_governed_bundle_content_is_flagged_within_one_cycle` in `test_vc_dual_authority.py` | Join sanctioned bundle inventory to registry and preserve/attribute observation |
| 11 | `test_duplicate_binding_inventory_quarantines_not_name_rebinds` in same file | Signal M03 quarantine and audited manual identity resolution |
| 12 | `test_overview_uses_projection_without_per_row_genie_calls` in `test_vc_drift.py` | Paged projection API with freshness; refresh uses observer endpoint |
| 13 | `test_target_approval_uses_rendered_fingerprint_not_source_environment_hash` in same file | Resolve approved head through target-local approved payload evidence |
| 14 | `test_reconcile_coordination_failure_disables_actions_but_keeps_available_history` in `test_vc_reconcile.py` | Propagate typed stale/fail-closed errors; no fallback writer |

## 6. Acceptance criteria

- [ ] FM-DRIFT: external state preserved before adopt/reapply choices; drift compares policy state.
- [ ] FM-DUAL-AUTHORITY: governed bundle content detected/flagged within one reconcile cycle.
- [ ] FM-OUTAGE and F-OPENS: failed capture cannot enable reconciliation.
- [ ] F-FRESH-APPROVAL/F-CANONICALIZER: adoption/reapply cannot recycle invalid approval or fabricate equality.
- [ ] Scheduled scan is bounded and per-space failures do not stop other bindings.

## 7. Parallel mocks and seam version

**VC/1.0**: mock M04 Observer, M02 lineage, M03 heads, M06 approval facts and M08 inventory/dispatcher. Pure classifier tests use three-head fixtures. Export expected legacy-current-version behavior to M04 rather than touching its test files. M09 uses status JSON fixtures.

## 8. Risks / verify at implementation time

Verify discoverable bundle state/inventory APIs and attribution reliability; absence of audit actor evidence must be labeled unknown, not invented. Sanctioned deploy prevention belongs to M08; admin/uncontrolled deploys remain detect/preserve/recover, not guaranteed prevention. Decide reconciliation cadence/lag SLO and stale threshold per environment. Snapshot history is not a complete external event stream. Keep backend watch modules untouched unless an explicitly reassigned call site is required.
