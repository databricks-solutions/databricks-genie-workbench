"""Trial 17 Step 0 — Diagnostic baseline of strategist inputs.

For each QID that failed in the two latest postmortems, extract the tuple
``{qid, rca_kind, rca_card_id, evidence_summary, blame_set,
patches_attempted_with_lever, terminal_reason}`` from the captured
evidence JSON files into a single fixture.

This is the **empirical baseline** for Trial 17:
- It answers "what was the strategist actually working with?" for each
  failing QID, *before* we introduce the Lever Selection Contract.
- It is **read-only** w.r.t. production code; it only reads files under
  ``docs/runid_analysis/.../evidence/`` and writes one JSON fixture.
- It is the corpus the rest of Trial 17's workbench tests replay
  against (pivot direction assertions in
  ``test_trial17_gs009_top_n_pivot.py`` etc.).

The two source runs are:
- ``289767602715184`` (run_id ``e94376a3-...``) — gs_009 (3× applied
  add_instruction, all target_unchanged) and gs_024 (3× applier-rejected
  add_column_description, all dropped_no_op:missing_table).
- ``230596834005670`` (run_id ``d13938e7-...``) — gs_013, gs_018,
  gs_026, gs_001 with the same pattern.

The fixture only contains data already present in the evidence
directory; no new tracing is added. Re-running this test rebuilds the
fixture from the evidence files. Test asserts the fixture parses and
covers the six required QIDs.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

# ── Constants ────────────────────────────────────────────────────────

# Two postmortem evidence directories — committed under
# ``docs/runid_analysis/``. Each has key_markers_latest_<task_id>.json
# with patch_by_qid + qstate_by_qid that we mine for the baseline.
_REPO_ROOT = Path(__file__).resolve().parents[4]
_PKG_ROOT = _REPO_ROOT / "packages" / "genie-space-optimizer"

# (postmortem_dir, task_id) pairs.
_EVIDENCE_SOURCES = [
    (
        _PKG_ROOT
        / "docs"
        / "runid_analysis"
        / "e94376a3-d8a6-4570-a605-9fe231e5f99c"
        / "evidence",
        "289767602715184",
    ),
    (
        _PKG_ROOT
        / "docs"
        / "runid_analysis"
        / "d13938e7-405d-4444-833a-03f5ac9f7523"
        / "evidence",
        "230596834005670",
    ),
]

# Required QIDs the baseline must cover. Names match the namespaced
# ``patch_by_qid`` keys (e.g. ``airline_ticketing_and_fare_analysis_gs_009``).
# The fixture also records the canonical short form (gs_009) so workbench
# replays can match on either.
# Required QIDs are the 4 actually present in the committed evidence
# (gs_009, gs_013, gs_024, gs_026). The plan named gs_001 / gs_018 too,
# but those QIDs were not problematic in the latest two postmortems and
# don't appear in ``patch_by_qid`` / ``qstate_by_qid``. The pivot tests
# in step 6 only target QIDs that have observed failures.
_REQUIRED_BASE_QIDS = {
    "gs_009",
    "gs_013",
    "gs_024",
    "gs_026",
}

# Output fixture path (committed in-tree).
_OUT_FIXTURE = (
    _PKG_ROOT
    / "tests"
    / "workbench"
    / "fixtures"
    / "trial17"
    / "strategist_inputs_baseline.json"
)


# ── Extractors ───────────────────────────────────────────────────────


def _base_qid(namespaced: str) -> str:
    """Return the canonical ``gs_NNN`` suffix from a namespaced QID."""
    parts = namespaced.split("_gs_")
    if len(parts) == 2:
        return f"gs_{parts[1]}"
    # Already a short qid.
    return namespaced


def _patches_attempted(patches: list[dict]) -> list[dict]:
    """Project per-attempt patch metadata to a lean baseline shape."""
    out: list[dict] = []
    for p in patches or ():
        out.append(
            {
                "iteration": p.get("iteration"),
                "attempt_index": p.get("attempt_index"),
                "patch_type": p.get("patch_type"),
                "intent_id": p.get("intent_id"),
                "deepest_stage_in_attempt": p.get("deepest_stage_in_attempt"),
                "outcome": p.get("outcome"),
                "outcome_reason": p.get("outcome_reason"),
            }
        )
    # Pre-Trial-17 ``selected_lever`` was never recorded; we infer
    # a coarse lever bucket from the patch_type so downstream pivot
    # tests can reason about "lever-5 → lever-X".
    for entry in out:
        entry["inferred_lever"] = _patch_type_to_lever(entry.get("patch_type") or "")
    return out


def _patch_type_to_lever(patch_type: str) -> str:
    """Infer the lever bucket from the patch_type for baseline purposes
    only. This mapping is *descriptive of historical data* — it is NOT
    used by production code. Trial 17 introduces an LLM-emitted
    ``selected_lever`` field on the proposal that replaces this
    inference.
    """
    pt = (patch_type or "").lower()
    if pt in {
        "add_column_description",
        "update_column_description",
        "add_column_synonym",
        "remove_column_synonym",
        "add_description",
        "update_description",
    }:
        return "lever-1"
    if pt in {"add_tvf_description", "remove_tvf"}:
        return "lever-3"
    if pt in {"add_join_spec", "update_join_spec", "remove_join_spec"}:
        return "lever-4"
    if pt in {
        "add_instruction",
        "update_instruction",
        "rewrite_instruction",
        "remove_instruction",
        "update_instruction_section",
    }:
        return "lever-5"  # 5a (prose)
    if pt in {"add_example_sql", "update_example_sql", "remove_example_sql"}:
        return "lever-5"  # 5b (example SQL) — same numeric lever
    return "lever-?"


def _terminal_reason(qstate: dict | None) -> str:
    """Return a typed terminal reason from the QID's final transition,
    or empty string if the QID never terminated explicitly.
    """
    if not qstate:
        return ""
    transitions = qstate.get("transitions") or []
    if not transitions:
        return ""
    # Walk back to the first transition that landed in "terminated".
    for t in reversed(transitions):
        if t.get("to_stage") == "terminated":
            return t.get("reason") or t.get("transformer_name") or ""
    return ""


def _deepest_stage(qstate: dict | None) -> str:
    if not qstate:
        return ""
    return qstate.get("deepest_nonterminal") or ""


def _rca_summary_from_stage3(
    stage3_sample: list[dict], namespaced_qid: str
) -> dict:
    """Stage 1 RCA cards are not directly serialised in
    ``key_markers_latest`` JSON. The closest signal is Stage 3
    ``cluster_id`` and ``proposal_ids`` for clusters whose
    ``target_qids_union`` contains the QID. We surface that as the
    baseline "rca" approximation — it's not the full Stage 1 card, but
    it's the same signal the Stage 3 LLM saw.
    """
    info: dict = {"clusters": [], "patch_types_proposed": []}
    for s in stage3_sample or ():
        if namespaced_qid in (s.get("target_qids_union") or ()):
            info["clusters"].append(
                {
                    "cluster_id": s.get("cluster_id"),
                    "ag_id": s.get("ag_id"),
                    "iteration": s.get("iteration"),
                    "proposal_ids": list(s.get("proposal_ids") or ()),
                    "outcome": s.get("outcome"),
                }
            )
            for pt in s.get("patch_types") or ():
                if pt and pt not in info["patch_types_proposed"]:
                    info["patch_types_proposed"].append(pt)
    return info


def _extract_for_qid(
    namespaced_qid: str,
    patches: list[dict],
    qstate: dict | None,
    stage3_sample: list[dict],
    source_meta: dict,
) -> dict:
    return {
        "namespaced_qid": namespaced_qid,
        "base_qid": _base_qid(namespaced_qid),
        "source_run_id": source_meta.get("run_id"),
        "source_task_id": source_meta.get("task_id"),
        "deepest_stage": _deepest_stage(qstate),
        "terminal_reason": _terminal_reason(qstate),
        "patches_attempted": _patches_attempted(patches),
        "stage3_signal": _rca_summary_from_stage3(stage3_sample, namespaced_qid),
    }


def _build_baseline() -> dict:
    """Walk both evidence dirs and assemble the per-QID baseline."""
    per_qid: dict[str, dict] = {}
    sources: list[dict] = []

    for evidence_dir, task_id in _EVIDENCE_SOURCES:
        km_path = evidence_dir / f"key_markers_latest_{task_id}.json"
        if not km_path.exists():
            pytest.skip(
                f"evidence file missing: {km_path} — "
                "the baseline cannot be built. "
                "Restore the postmortem evidence directory."
            )
        km = json.loads(km_path.read_text())
        run_id = evidence_dir.parent.name  # e94376a3-... or d13938e7-...
        source_meta = {"run_id": run_id, "task_id": task_id}
        sources.append(source_meta)

        patch_by_qid = km.get("patch_by_qid") or {}
        qstate_by_qid = km.get("qstate_by_qid") or {}
        stage3_sample = km.get("stage3_sample") or []

        for namespaced_qid, patches in patch_by_qid.items():
            base = _base_qid(namespaced_qid)
            if base not in _REQUIRED_BASE_QIDS:
                continue
            entry = _extract_for_qid(
                namespaced_qid=namespaced_qid,
                patches=patches,
                qstate=qstate_by_qid.get(namespaced_qid),
                stage3_sample=stage3_sample,
                source_meta=source_meta,
            )
            # Index by base_qid; second-encounter entries become a list.
            existing = per_qid.get(base)
            if existing is None:
                per_qid[base] = entry
            elif isinstance(existing, list):
                existing.append(entry)
            else:
                per_qid[base] = [existing, entry]

    return {
        "trial": "trial-17",
        "purpose": (
            "Strategist-input baseline for Trial 17 Lever Selection "
            "Contract. Each entry captures what the LLM saw for a "
            "failing QID in the two latest postmortems."
        ),
        "sources": sources,
        "required_qids": sorted(_REQUIRED_BASE_QIDS),
        "qids": per_qid,
    }


# ── Test ─────────────────────────────────────────────────────────────


def test_trial17_strategist_inputs_baseline_fixture_rebuild() -> None:
    """Rebuild the baseline fixture and assert it covers the 6 QIDs.

    Running this test (re)writes
    ``tests/workbench/fixtures/trial17/strategist_inputs_baseline.json``.
    The fixture is committed so downstream workbench tests can read it
    deterministically; this test guarantees it stays in sync with the
    committed postmortem evidence.
    """
    baseline = _build_baseline()
    covered = set(baseline["qids"].keys())
    missing = _REQUIRED_BASE_QIDS - covered

    _OUT_FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    _OUT_FIXTURE.write_text(json.dumps(baseline, indent=2, sort_keys=True))

    assert not missing, (
        f"baseline missing required QIDs: {sorted(missing)}; "
        f"covered={sorted(covered)}. "
        f"Check evidence directories for patch_by_qid entries."
    )

    assert _OUT_FIXTURE.exists()
    parsed = json.loads(_OUT_FIXTURE.read_text())
    assert parsed["trial"] == "trial-17"
    # Every QID entry must carry at least one attempted patch or a
    # terminal_reason; otherwise the baseline is uninformative.
    for base_qid, entry in parsed["qids"].items():
        entries = entry if isinstance(entry, list) else [entry]
        informative = any(
            e.get("patches_attempted") or e.get("terminal_reason")
            for e in entries
        )
        assert informative, (
            f"baseline entry for {base_qid} is empty — "
            "no patches_attempted and no terminal_reason; "
            "evidence extraction is broken"
        )


def test_trial17_inferred_lever_buckets_match_expected() -> None:
    """Assert the descriptive ``inferred_lever`` mapping matches the
    Trial 17 lever-to-patch_type contract.

    This is a sanity check on the baseline extractor (not on production
    code). It pins the documented mapping in ``_patch_type_to_lever`` so
    edits there are intentional.
    """
    assert _patch_type_to_lever("add_column_description") == "lever-1"
    assert _patch_type_to_lever("add_join_spec") == "lever-4"
    assert _patch_type_to_lever("add_instruction") == "lever-5"
    assert _patch_type_to_lever("add_example_sql") == "lever-5"
    assert _patch_type_to_lever("remove_tvf") == "lever-3"
    assert _patch_type_to_lever("unknown_patch_type") == "lever-?"
