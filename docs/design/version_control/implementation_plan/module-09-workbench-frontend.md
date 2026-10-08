# M09 — Workbench history, reconciliation and promotion UI

## 1. Purpose and scope

Deliver the Workbench-native overview version indicators/drift badges and a dedicated agent Version Control and Promotion tab. The UI consumes durable projections and explicit operation states, shows captured external history before reconciliation choices, and never treats a success toast, capture or confirmation as approval. Build against frozen API fixtures while backend modules proceed in parallel.

## 2. Exclusive ownership

Own **all touched `frontend/**` paths**, including `frontend/src/App.tsx`, actual overview/agent pages discovered under `pages/`, new `components/version-control/`, `hooks/use-version-control.ts`, `lib/version-control-api.ts`, `types/version-control.ts`, colocated `*.test.tsx`/`*.test.ts`, and frontend dependency/lock/config files if necessary. Read only: backend contracts/routers, shared M02 JSON fixtures, current frontend routing/component conventions. Do not rename existing pages or rewrite unrelated app UI. No other module edits frontend files.

DDL/Volume ownership: **none**. No direct Delta/Genie/Volume client in the browser; authorized APIs supply read models and commands.

## 3. Public contract

Implement `VersionControlApi` typed methods matching `contracts.md` §7–8, `useVersionControl(bindingId)` and `VersionControlTab({bindingId})`. Overview consumes paged `BindingStatus`/version summaries; M02 fixtures define serialization and nullability. Full typed approval/receipt DTOs follow §6 without `any`. Browser sends idempotency key, reviewed base and binding revision; it never sends an approver identity as proof, service credentials, or direct Genie PATCH.

API status rules: 409 → show preserved conflict; 423 → unresolved/quarantined, no mutation retry button; 503 → stale/fail-closed, history remains usable if available. Async commands return operation_id; poll GET for status rather than replay POST. Data schema dependencies are read-only projections, not duplicated frontend authority.

## 4. Dependencies

M02 history/diff; M04 observe/restore; M05 status/reconcile/overview; M06 approvals/audit; M07 validate/promote/receipts. M08 mounts backend routes; M09 owns frontend app mounting. All backend dependencies mockable with fetch mocks or the repo's existing request-test utilities; do not add a new framework unnecessarily.

## 5. Ordered red → green tasks

Each named test belongs in the indicated new frontend file; use the existing vitest/React test conventions.

| # | RED: first test and assertion | GREEN: minimal implementation |
|---|---|---|
| 1 | `loads_vc_contract_fixtures_without_type_or_nullability_drift` in `lib/version-control-api.test.ts` | Typed API client/DTOs and golden fixture consumption |
| 2 | `overview_badge_uses_approved_deployed_not_observed_equality` in `components/version-control/overview-badge.test.tsx` | Drift-state mapping plus version short ID and freshness |
| 3 | `overview_does_not_fetch_live_genie_per_row` in same file | One projection query, explicit refresh action |
| 4 | `open_waits_for_capture_before_showing_reconcile_choices` in `components/version-control/version-control-tab.test.tsx` | Call observe on agent/history open and return from Genie; gate choices on result |
| 5 | `capture_failure_shows_stale_history_and_disables_mutating_actions` in same file | Loading/empty/error/stale/busy states with available history |
| 6 | `history_shows_origin_actor_lineage_and_optimizer_drillthrough` in `components/version-control/history.test.tsx` | Timeline, pagination, semantic diff selection and links |
| 7 | `semantic_diff_groups_changes_and_flags_manual_sql_review` in `components/version-control/diff.test.tsx` | Categorized diff component; unknown comparison state |
| 8 | `restore_and_forward_submit_reviewed_base_and_poll_operation` in `components/version-control/restore.test.tsx` | Historical selection + confirmation/approval reference + 202 handle |
| 9 | `adopt_requests_approval_and_reapply_does_not_reuse_invalid_approval` in `components/version-control/reconcile.test.tsx` | Adopt/compare/reapply/acknowledge flows reflecting server policy |
| 10 | `quarantine_and_partial_outcome_never_offer_blind_retry` in same file | Read-only verification/status view with operator evidence explanation |
| 11 | `approval_ui_shows_bound_digests_distinct_approvers_and_expiry` in `components/version-control/approvals.test.tsx` | Request/vote/audit rendering; server actions authoritative |
| 12 | `promotion_requires_explicit_target_mapping_preflight_and_approval` in `components/version-control/promotion.test.tsx` | Version→mapping→validate→approve→promote flow inside Workbench |
| 13 | `receipt_shows_intended_rendered_observed_and_compensation_status` in same file | Durable receipt display/polling, no false deploy success |
| 14 | `double_click_or_network_loss_does_not_generate_new_mutation_key` in `lib/version-control-api.test.ts` | Stable per-intent idempotency and operation polling; changed intent gets new key |
| 15 | `late_response_for_previous_binding_cannot_replace_current_heads` in `hooks/use-version-control.test.ts` | Abort/generation-aware query handling; invalidate after operations |
| 16 | `version_control_actions_are_keyboard_accessible_and_errors_announced` in tab test | Existing semantic components, focus/labels/live status; no UI-only authorization |

## 6. Acceptance criteria

- [ ] FM-HISTORY/FM-RESTORE/FM-DRIFT visible end-to-end in local milestone UI.
- [ ] F-OPENS: capture-on-open cannot clear drift or present successful alignment on persistence failure.
- [ ] FM-OUTAGE/FM-RECOVERY: stale/unreachable/partial/quarantine states distinguishable; no blind retries.
- [ ] F-FRESH-APPROVAL/F-PROMOTION: users can approve/reconcile/promote entirely inside Workbench.
- [ ] Overview is projection-based and paginated; all async surfaces have loading/empty/error states.

## 7. Parallel mocks and seam version

**VC/1.0**. Backend response fixtures cover three distinct heads, external origin, invalidated approval, partial description write, ambiguous create, unreachable history and target receipt. M09 may request M02 fixture additions but does not edit M02's folder. Vitest fakes verify exact URLs and idempotency/base fields before real API integration.

## 8. Risks / verify at implementation time

Verify actual existing overview/agent page and routing names before assigning changes; new suggested filenames are not claims those files already exist. Use current app state/query conventions and approved UI dependencies. Client auth checks are convenience only. Do not label missing approved/deployed history “In sync,” invent numeric version ordering from UUIDs, or expose raw snapshots to unauthorized viewers. Profile selection must not reveal credentials; multi-workspace views still dispatch target-locally.
