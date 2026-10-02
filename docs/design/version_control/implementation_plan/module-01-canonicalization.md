# M01 — Canonicalization and semantic comparison

## 1. Purpose and scope

Extend the existing fingerprint canonicalizer rather than replacing it. Separate exact API envelope/restorable state from stable config, benchmark and governed-metadata comparison; expose semantic diffs and version-aware equality without destroying existing Genie normalization behavior. This module is pure: no API calls, SQL, approval decisions or head updates.

## 2. Exclusive ownership

| Own | Read only |
|---|---|
| `backend/services/config_fingerprint.py` | `backend/services/genie_client.py` |
| New `backend/services/version_control/canonical_diff.py` | Shared M02 `contracts.py`, existing optimizer fingerprint consumers |
| Extend `backend/tests/test_config_fingerprint.py` | `backend/routers/auto_optimize.py`, design inputs |
| New `backend/tests/test_vc_semantic_diff.py`, `backend/tests/fixtures/vc_canonicalization/` | M02 contract fixtures |

DDL/Volume ownership: **none**. M02 stores canonical and raw observation data. Do not modify legacy caller files; deliver compatibility expectations to M04/M10.

## 3. Public contract

VC/1.0 `Canonicalizer.observe(envelope: dict, version='vc-c14n/1') -> Snapshot`, `compare(left, right) -> Comparison`, `semantic_diff(left, right) -> list[DiffItem]`; keep existing exported fingerprint signatures working. Shared types are owned by M02. `Fingerprints` contains config/benchmark/metadata/canonicalizer_version; aggregate state identity uses the domain-separated contract hash. SQL-bearing diff items set `review_required=true`.

Persisted schema consumed: `genie_space_versions.{serialized_space_json,restorable_metadata_json,canonical_state_json,raw_state_digest,config_fingerprint,benchmark_fingerprint,metadata_fingerprint,state_digest,canonicalizer_version}` as specified in `contracts.md` §2. M01 never authors its DDL.

## 4. Dependencies

Upstream: frozen M02 DTOs (develop against fixtures before M02 runtime exists). Downstream: M02 history/diff, M04 desired/live comparison, M05 drift, M07 rendering verification, M10 champion comparison. M01 depends on none of their implementations.

## 5. Ordered red → green tasks

Each row is a separate TDD cycle: add test, run and record expected failure, implement only enough to pass, refactor with green tests.

| # | RED: first test and assertion | GREEN: minimal implementation |
|---|---|---|
| 1 | `test_existing_fingerprint_api_remains_compatible` in `test_config_fingerprint.py`: current exports and known fixture hashes remain valid | Wrap/extend existing implementation; do not rename public functions |
| 2 | `test_observe_preserves_exact_restorable_state_and_envelope` in same file: normalized comparison does not mutate input/raw state | Separate copy-based extraction from canonicalization |
| 3 | `test_normalized_metric_views_quotes_fragments_and_wrappers_match` in same file: historical wrappers, metric-view flattening, boundary quotes and fragment order preserve equality | Reuse existing normalization paths and add only missing cases |
| 4 | `test_unordered_ids_sort_but_meaningful_order_survives` in same file: ID-addressed reordering is equal; instruction/SQL meaningful sequence change is not | Field-specific collection rules, not blanket array sorting |
| 5 | `test_description_changes_only_metadata_fingerprint` in same file: description drift leaves config/benchmark hashes stable | Add allowlisted governed metadata extraction/hash |
| 6 | `test_benchmark_change_does_not_change_config_identity` in same file: benchmark independence remains true | Preserve benchmark separation in aggregate identity |
| 7 | `test_malformed_serialized_space_is_rejected_not_empty` in same file: invalid JSON/missing required payload does not masquerade as blank config | Explicit parse/validation error |
| 8 | `test_cross_canonicalizer_comparison_is_unknown_without_dual_compute` in same file | Version registry and supported dual-compute path using retained raw state |
| 9 | `test_semantic_diff_categorizes_by_id_and_flags_sql_review` in `test_vc_semantic_diff.py`: sources/columns/instructions/joins/filters/parameters/SQL/benchmarks/questions/metadata/bindings categorized | Deterministic category and ID-indexed comparison |
| 10 | `test_canonicalizer_is_idempotent_and_hash_is_domain_separated` in `test_config_fingerprint.py`: repeated canonicalization stable, structured hash fields cannot collide by concatenation | Central deterministic encoding; reuse contract fixtures |

## 6. Acceptance criteria

- [ ] Existing fingerprint suite stays green; API normalization does not trigger false drift.
- [ ] FM-HISTORY/FM-RESTORE: exact restorable state and metadata survive capture and comparison.
- [ ] FM-DRIFT and F-DESCRIPTION: metadata-only/partial changes remain diagnosable.
- [ ] F-CANONICALIZER: incompatible versions produce unknown, never a false clean badge.
- [ ] No duplicate canonicalizer implementation in optimizer or promotion.

## 7. Parallel mocks and seam version

**VC/1.0**: mock nothing platform-related; use deterministic Snapshot fixtures from M02. Consumers may fake `Canonicalizer`, but must later run M01's normalization golden cases against the real adapter. New golden fixtures are M01-owned, shared wire fixtures M02-owned.

## 8. Risks / verify at implementation time

Verify actual existing function names and read-back normalization cases before extending; characterize current hashes first. Confirm the governed metadata allowlist (description first, title only if consistently updateable). API preview ETags do not change this module's contract. A canonicalizer migration needs dual-compute policy and retained raw data, not backfilled historical hashes. Do not silently strip unknown executable fields or secrets from otherwise “restorable” data; flag unsafe capture/schema issues for policy handling.
