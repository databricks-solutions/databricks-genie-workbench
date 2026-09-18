# Ontology — Phase 5 / 17j: hardening + undo + E2E · build spec of record

**Track:** P5 (backlog). **Branch:** `ontology`. **Prereq:** 17i apply is BUILT +
deploy-verified (`cf92ef58`; §P2). This spec turns the live-but-one-way apply into a
**reversible** one, closes the hardening gaps the Stage-2 deploy-verify surfaced, and adds the
E2E coverage the write path never had. Additive, no new dep, no new router file.

> Companion driver: `ontology-17j-hardening-driver.md` (the Goal-Mode prompt). This file is the
> **content** source of truth; `backlog.md` is the sequencing source; `mv-advisor-playbook.md`
> is the MV-D register.

## §1 Goal

An applied governed-tag membership can be **undone by the same consenting human, under OBO**,
from the audit trail alone — with the same guardrails as apply (dry-run preview, grant probe →
copy-ready, per-statement fail-soft, plan_hash + confirm gate, SP bookkeeping). Plus: the
pre-state capture that makes an unset undoable, an E2E test of the full apply→undo loop, and
docs/changelog.

## §2 Undo semantics (inverse of a recorded `genie_ont_applied` row)

The inverse is computed from the audit row's `shape` + `prev_value` (ddl.py:249-266). Only the
two membership shapes are reversible; the create is not (§3).

| Applied row | `prev_value` | Undo statement | Meaning |
|---|---|---|---|
| `set_tag` | `NULL` (an *add*) | `UNSET TAG …` | remove the tag we added |
| `set_tag` | `<old>` (a *move*) | `SET TAG … = \`<old>\`` | restore the prior value |
| `unset_tag` | `<old>` (captured in §3) | `SET TAG … = \`<old>\`` | re-add the removed value |
| `create_tag` | — | **none** | see §3 — never auto-dropped |

- Only rows with `state='applied'` are undoable. `failed`/`blocked` rows never wrote UC — skip.
- Idempotency: undo probes `grants.current_tag_value` first; if the asset is already at the
  post-undo value, it's a no-op (records `state='noop'`), so a double-undo is safe.

## §3 Two hardening carves (both in `services/apply.py`)

1. **`create_tag` is NOT undoable.** `"drop governed tag"` / `"alter governed tag"` are in
   `_FORBIDDEN` and absent from `_APPLY_ALLOWED` (test_ontology_firewall.py:25-35) — even the
   single-writer module may not emit them, and MV policy says "never auto-drop the created UC
   object outside sandbox; drop is an explicit OBO endpoint." Undo of a domain therefore reverts
   its member `set_tag`s and **leaves the empty governed tag in place**, surfaced in the result
   as an informational note (not a failure).
2. **Capture the unset pre-value.** Today `_reassign_items` builds `unset_tag` items with
   `current_value=None` (apply.py:382), so the audit row's `prev_value` is `NULL` and the unset
   can't be reversed. Fix: probe `grants.current_tag_value(client, asset_fqn, conflict_tag, …)`
   for the unset item exactly as the set item already does (apply.py:394), so `prev_value` is
   persisted. Absent ⇒ `None` (byte-identical; that unset simply stays non-undoable, honestly).

## §4 Undo service (`services/apply.py` — the single writer)

Mirror the apply pair, reusing every helper (`_bt`, `_bt_fqn`, `_set_tag_statement`,
`_unset_tag_statement`, `_probe_write`, `_write_audit_row`, `_plan_hash`, `_probe_client`):

- `build_undo_plan(metastore_id, proposal_ids: list[str]) -> models.ApplyPlan` — reads
  `state='applied'` rows via a new read-only `mirror.read_applied_memberships(metastore_id,
  proposal_ids)` (same shape as `mirror.read_domain_members`), computes the inverse statement +
  diff + grant probe per row, fingerprints with the existing `_plan_hash`. `create_tag` rows are
  dropped from the item list and counted into a new `notes` field (§6). No UC write.
- `execute_undo_plan(plan, metastore_id, workspace_id, applied_by) -> models.ApplyResult` —
  identical control flow to `execute_apply_plan`: OBO write, per-statement fail-soft, current-
  value no-op guard, `_write_audit_row` (SP) with the inverse `shape` + the value being reverted
  *from* captured as the new row's `prev_value`, then a consent flip **applied → approved** via a
  parameterized SP update (re-surfaces the proposal as actionable). Idempotent.

**Audit model (append-only, CDF-friendly):** an undo writes a NEW `genie_ont_applied` row for the
inverse action (never mutates the original); `apply_id` = `_apply_id(proposal_id, inverse_shape,
target_fqn, reverted_value)` so a repeated undo collides idempotently.

## §5 Route (added to the existing `routers/apply.py` — no new file)

- `POST /api/ontology/apply/undo-preview` → `build_undo_plan` (dry-run; writes nothing).
- `POST /api/ontology/apply/undo` (`models.ApplyUndoRequest`: `plan_hash`, `confirm`,
  `proposal_ids`) → `plan_hash` 409 gate + `confirm` 400 gate (reuse the execute pattern) →
  `execute_undo_plan`. OBO email via the existing `_obo_email(request)`.

Router-verbs firewall (test_ontology_firewall.py:123-141) keys on **file names**; both routes
live in `apply.py`, so `post_files == ["apply.py","drafts.py","refresh.py"]` stays green.

## §6 Models (`backend/ontology/models.py`, mirrored in `frontend/src/ontology/types`)

- Reuse `ApplyPlan` / `ApplyItem` / `ApplyOutcome` / `ApplyResult` as-is (the undo plan is an
  apply plan of inverse statements). Add `ApplyPlan.notes: list[str] = []` for the "N governed
  tags left in place (undo never drops a tag)" line (default `[]` ⇒ byte-identical).
- Add `ApplyUndoRequest` (same fields as `ApplyExecuteRequest`).

## §7 Frontend (`ApplyResult` surface + `ApplyDiff.tsx`)

- On a successful apply result, render an **Undo** affordance that calls `undo-preview` →
  shows the inverse diff through the SAME `ApplyDiff` component → confirm → `undo`. Terminal-state
  rule (playbook): the Undo control initializes only from a persisted applied row, never an
  ambient default. The "tag left in place" `notes` render as an informational line, not an error.
- Error boundary already mandatory on these surfaces — keep it.

## §8 Guardrails (hard)

Additive only; single-writer carve intact (inverse statements use the already-allowed `set tag`
/ `unset tag` tokens in `apply.py` ONLY; **no** `drop`/`alter governed tag`); no new router file;
undo-preview writes nothing; undo gated on `confirm=true` + `plan_hash` (409); per-statement
fail-soft; idempotent (current-value no-op guard + colliding `apply_id`); OBO write / SP
bookkeeping split preserved (undo audit + consent re-flip are SP); DEFAULT-OFF (no undo without
confirm); no new dep (`uv.lock`/`package-lock.json` untouched).

## §9 Tests

- Wheel/unit: inverse-statement table (§2) incl. escaping; `create_tag` excluded + noted;
  unset pre-value now captured; current-value no-op guard; consent re-flip parameterized;
  undo audit row is a new append (original untouched).
- Backend: `undo-preview` writes nothing; `undo` 400/409 gates; blocked item → copy-ready, not
  executed; firewall carve + POST allowlist + identity-split tests still green.
- Frontend: Undo affordance renders only from a persisted applied row; inverse diff renders;
  `notes` line renders; error boundary holds.
- **E2E** (`test_ontology_apply_e2e` — offline, mocked SDK): approve → preview → execute →
  audit row asserted → undo-preview → undo → asset back to pre-state, undo audit row present,
  consent back to `approved`.

## §10 Deploy-verify gate (human STOP — after offline-green)

`./scripts/deploy.sh --update` — **note:** `deploy.sh` reads `GENIE_DEPLOY_PROFILE` from
`.env.deploy` (today `fevm-serverless` → workspace 6t92c3) and **ignores** any
`DATABRICKS_CONFIG_PROFILE=…` prefix; to target another workspace edit `.env.deploy`, don't
prefix the command. Then in the live app: apply one small domain → Undo → confirm the member
tags are removed in UC (attributed to you), an undo audit row exists, the empty governed tag
remains with the informational note, the consent is back to `approved`, and re-preview is clean.
An account without the grant sees copy-ready on undo, never a hang.

## §11 Docs

Update `docs/docs/` (auth/optimization pages as touched) + changelog in the same commit; refresh
`backlog.md` P5 → BUILT and any `mv-advisor-playbook.md` MV-D register line in the same commit
(MV-D9). New MV-D for the undo semantics + the "never auto-drop a governed tag" invariant.
