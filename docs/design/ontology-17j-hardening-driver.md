# Ontology — Phase 5 / 17j: hardening + undo + E2E · Goal-Mode driver

> **One stage per run, on the `ontology` branch.** Spec of record:
> `ontology-17j-hardening-build.md` (§1–§11). Prereq: 17i apply is BUILT + deploy-verified
> (`cf92ef58`). This stage makes an applied membership **reversible** and closes the Stage-2
> hardening gaps, all additive. **STOP before the live write** — the human deploy-verify (§10)
> exercises the real UC undo.

## Why now
Apply (17i) is live but **one-way**: a curator who applies a domain has no in-product way back,
and the `genie_ont_applied.prev_value` column exists precisely `'for a future undo (17j)'`
(ddl.py:260) but nothing reads it. Two gaps also block a trustworthy undo: `_reassign_items`
records `unset_tag` with `current_value=None` (apply.py:382) so an unset can't be reversed, and
there is no reader for applied rows.

## Grounded facts (code, 2026-09-18)
- Audit table `genie_ont_applied` (ddl.py:249-266): `shape` ∈ create_tag|set_tag|unset_tag,
  `prev_value` captured for undo, CDF enabled. PK `(metastore_id, apply_id)`.
- `services/apply.py` is the single writer: `_set_tag_statement`/`_unset_tag_statement`/`_bt*`/
  `_probe_write`/`_write_audit_row`/`_flip_consents`/`_plan_hash`/`_probe_client` all reusable.
- `mirror.py` has `read_approved_consents`/`read_domain_members`/`read_tag_members` (the read
  pattern to mirror for applied rows).
- Firewall: `_APPLY_ALLOWED=["set tag","unset tag","create governed tag"]`; `"drop/alter
  governed tag"` are ALWAYS-forbidden (test_ontology_firewall.py:25-35) → **create_tag is NOT
  undoable**. Router-verbs test keys on file names (`apply.py/drafts.py/refresh.py`,
  test_ontology_firewall.py:139) → undo routes go IN `apply.py`.

## Testability seam
Additive to `services/apply.py` + `services/mirror.py` (read only) + `routers/apply.py` +
`models.py` + `frontend/`. No new dep, no new router file. Undo reuses apply's guardrails
(dry-run, grant probe → copy-ready, plan_hash+confirm gate, per-statement fail-soft, SP
bookkeeping). Degrade-clean per MV-D43.

---

## GOAL PROMPT (paste verbatim into Goal Mode)

Implement **Phase 5 / 17j (hardening + undo + E2E)** of `ontology-17j-hardening-build.md` on the
`ontology` branch. Additive, offline-green, no new dep, no new router file. STOP before the live
write.

BUILD A — unset pre-value capture (`services/apply.py`): in `_reassign_items`, probe
`grants.current_tag_value(client, asset_fqn, conflict_tag, asset_type=asset_type)` for the
`unset_tag` item (exactly as the sibling `set_tag` already does at apply.py:394) and pass it as
`current_value` so the audit row's `prev_value` is persisted. Absent ⇒ None (byte-identical).

BUILD B — read applied rows (`services/mirror.py`): add read-only
`read_applied_memberships(metastore_id, proposal_ids)` returning `state='applied'` rows
(metastore-scoped, MV-D49), same shape/pattern as `read_domain_members`. No write.

BUILD C — undo plan + execute (`services/apply.py`, the single writer): add
`build_undo_plan(metastore_id, proposal_ids) -> models.ApplyPlan` (inverse of each applied row
per the build §2 table: set_tag+prev=None → UNSET; set_tag+prev=<v> → SET =`<v>`; unset_tag+
prev=<v> → SET =`<v>`; create_tag → EXCLUDE + count into `ApplyPlan.notes`), reusing
`_set_tag_statement`/`_unset_tag_statement`/`_probe_write`/`current_tag_value`/`_plan_hash`. Add
`execute_undo_plan(...)` mirroring `execute_apply_plan`: OBO write, current-value no-op guard
(record state='noop'), per-statement fail-soft, `_write_audit_row` (SP) writing a NEW row for the
inverse shape with the reverted-from value as its `prev_value`, then a parameterized SP consent
flip **applied→approved**. Idempotent (colliding `apply_id`).

BUILD D — routes (`routers/apply.py`, same file): `POST /apply/undo-preview` → `build_undo_plan`
(writes nothing); `POST /apply/undo` (`models.ApplyUndoRequest`: plan_hash, confirm,
proposal_ids) → confirm 400 + plan_hash 409 gates (reuse the execute pattern) →
`execute_undo_plan`; OBO email via existing `_obo_email`.

BUILD E — models + frontend: add `ApplyPlan.notes: list[str] = []` and `ApplyUndoRequest`
(mirror in `frontend/src/ontology/types`). On a successful apply result render an **Undo**
affordance → `undo-preview` → inverse diff via the SAME `ApplyDiff.tsx` → confirm → `undo`.
Undo initializes ONLY from a persisted applied row (terminal-state rule); `notes` render as an
informational line (governed tag left in place), never an error; keep the error boundary.

GUARDRAILS (hard): additive only; single-writer carve intact — inverse uses `set tag`/`unset
tag` in `apply.py` ONLY, NEVER `drop`/`alter governed tag` (`test_no_write_path_in_ontology_
module` stays green); no new router file (`test_backend_router_verbs…` file list unchanged);
undo-preview writes NOTHING; undo gated confirm=true + plan_hash (409); per-statement fail-soft
(never 500); idempotent; OBO write / SP bookkeeping split preserved (undo audit + consent re-flip
are SP — `test_phase5_apply_identity_split…` extended, not weakened); DEFAULT-OFF; no new dep.

TESTS (`packages/.../tests/unit/` + `backend/tests/test_ontology_apply.py` +
`test_ontology_firewall.py` + `frontend/`): inverse-statement table incl. escaping + create_tag
excluded/noted; unset pre-value captured; no-op guard; consent re-flip parameterized; undo audit
is a new append (original untouched); undo-preview writes nothing; 400/409 gates; blocked→copy-
ready not executed; carve + POST allowlist + identity-split green; Undo renders only from a
persisted row; inverse diff + notes render. Add offline E2E `test_ontology_apply_e2e`: approve →
preview → execute → assert audit row → undo-preview → undo → asset back to pre-state + undo audit
row + consent back to approved.

ACCEPTANCE (offline): `./scripts/test.sh` green; `cd frontend && npx tsc -b && npm run lint &&
npm run test`; `git status -- uv.lock` clean; lockfiles untouched. Then STOP for deploy-verify.

---

## Deploy-verify gate (human, after offline-green — the STOP checkpoint)
`./scripts/deploy.sh --update` — **`deploy.sh` reads `GENIE_DEPLOY_PROFILE` from `.env.deploy`
(today `fevm-serverless` → workspace 6t92c3) and IGNORES a `DATABRICKS_CONFIG_PROFILE=…` prefix;
to target another workspace edit `.env.deploy`, do not prefix the command** (verified 2026-09-18).
Then in the LIVE app on a real estate:
1. Apply one small domain (17i) → confirm the member tags land in UC attributed to YOU.
2. **Undo** → preview shows the inverse diff → confirm → the member tags are removed in UC
   (attributed to YOU via OBO), an undo `genie_ont_applied` row exists, the (now empty) governed
   tag REMAINS with the informational note, and the consent is back to `approved`.
3. Re-preview apply: the proposal is actionable again (idempotent, no dupes).
4. An account WITHOUT the grant sees copy-ready on undo, never a hang.
Review undo-safety (dry-run purity, no auto-drop of the tag, OBO attribution, SP bookkeeping,
degrade) with a human before marking P5 BUILT.
