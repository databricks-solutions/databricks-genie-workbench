"""Plan 11 — Stage 3: LLM-driven per-cluster patch synthesis.

Replaces the archetype catalog
(``cluster_driven_synthesis.py:pick_archetype``). Returns the same
:class:`ClusterSynthesisResult` envelope the legacy synthesizer does so
``optimizer.py`` callsites in PR 2 are drop-in replacements.

Entry point: :func:`run_plan11_synthesis_for_single_cluster`. The handler
is dormant during PR 1 (flag-off); PR 2 wires it in.
"""
from __future__ import annotations

import json
import time
from typing import Any, Mapping

from genie_space_optimizer.optimization.cluster_driven_synthesis import (
    ClusterSynthesisResult,
)
from genie_space_optimizer.optimization.llm_reasoning_call import LlmReasoningCall
from genie_space_optimizer.optimization.llm_reasoning_io import (
    LlmReasoningRequest,
    LlmReasoningResponse,
)
from genie_space_optimizer.optimization.repair_intent import PatchType, RepairShape
from genie_space_optimizer.optimization.repair_proposal_typed import RepairProposal
from genie_space_optimizer.optimization.run_analysis_contract import (
    insufficient_signatures_in_context_marker,
    plan11_stage3_synthesis_marker,
)
from genie_space_optimizer.optimization.stages.plan11_types import FailureCluster
from genie_space_optimizer.skills._loader import _SKILL_LOADER
from genie_space_optimizer.skills.plan11_synthesize.output_schema import (
    Plan11SynthesizeOutput,
)


_SKILL_ID = "plan11_synthesize"
_PROMPT_CONST = "PLAN11_SYNTHESIZE_PROMPT"


# Trial 18 step 4 — Stage 3 resolved-table preflight scope.
# Mirrors the applier_gate ``_PREFLIGHT_METADATA_PATCH_TYPES`` set so
# the upstream check has the same blast radius as the downstream one.
# Pushing it earlier saves a Stage 3 LLM turn whenever the LLM names
# a table the schema_slice doesn't resolve.
_TRIAL18_PREFLIGHT_METADATA_PATCH_TYPES: frozenset[str] = frozenset(
    {
        "add_column_description",
        "update_column_description",
        "add_column_synonym",
        "rename_column_alias",
        "hide_column",
        "unhide_column",
        "add_description",
        "update_description",
        "add_join_spec",
        "update_join_spec",
        "remove_join_spec",
    }
)


def _trial20_resolve_canonical_target_table(
    *,
    raw_target: str,
    resolved_tables: frozenset[str],
) -> str:
    """Trial 20 Workstream F2 — canonical-target resolver.

    Postmortem 519131527536322 (airline) and 766686021706995 (7now)
    both showed metadata patches naming an unresolved table tail
    (``tkt_payment`` instead of ``main.7now.tkt_payment``, etc.) and
    failing the applier_gate preflight at apply-time. Trial 18 dropped
    such proposals at Stage 3 exit, but only when the exact identifier
    was missing — a partial name like ``tkt_payment`` would not match
    the fully-qualified entry ``main.7now.tkt_payment`` in
    ``resolved_tables`` and was therefore silently kept as
    ``preflight_target_missing`` at apply-time.

    Returns the canonical identifier when a unique tail-match exists,
    or ``""`` when the target cannot be resolved (caller drops the
    proposal). Exact matches return ``raw_target`` unchanged. Ambiguous
    tails (e.g. ``users`` matching multiple catalogs) return ``""`` so
    F2 conservatively drops rather than guess.
    """
    if not raw_target:
        return ""
    target = raw_target.strip()
    if not target:
        return ""
    if target in resolved_tables:
        return target
    # Try tail-match for partial identifiers (``tkt_payment`` ->
    # ``main.7now.tkt_payment``). The tail must be unique.
    tail = target.split(".")[-1] if "." in target else target
    matches = [t for t in resolved_tables if t.split(".")[-1] == tail]
    if len(matches) == 1:
        return matches[0]
    return ""


def _trial18_resolved_table_ids(schema_slice: dict[str, Any]) -> frozenset[str]:
    """Trial 18 step 4 — extract the set of resolved table identifiers
    from the schema slice the Stage 3 LLM already sees.

    The Stage 3 ``schema_slice`` is a Genie Space config dict whose
    ``data_sources`` section enumerates tables and metric views by
    ``identifier``. Returning ``frozenset()`` when the slice is empty
    (synthetic replays / pre-snapshot iterations) deliberately
    disables the preflight — see
    ``_trial18_target_table_unresolved``.
    """
    ds = (schema_slice or {}).get("data_sources") or {}
    resolved: set[str] = set()
    for key in ("tables", "metric_views"):
        for entry in ds.get(key, []) or []:
            ident = entry.get("identifier") if isinstance(entry, dict) else None
            if isinstance(ident, str) and ident.strip():
                resolved.add(ident.strip())
    return frozenset(resolved)


def _trial18_target_table_unresolved(
    *,
    patch_type_value: str,
    patch_body: dict[str, Any],
    resolved_tables: frozenset[str],
) -> bool:
    """Trial 18 step 4 — return True when the LLM-named target table
    is missing from the schema slice the Stage 3 prompt was built on.

    The check is **strictly additive**: it short-circuits to ``False``
    (proposal survives) when any of the following hold so the existing
    applier_gate late preflight remains the authoritative catch:

    * Patch type is not in ``_TRIAL18_PREFLIGHT_METADATA_PATCH_TYPES``
      (no notion of target_table for non-metadata levers).
    * ``patch_body.table`` (or ``patch_body.target``) is not a
      non-empty string — legacy ``object_id="t:c"`` encodings are
      opaque to a string-based check.
    * ``resolved_tables`` is empty — synthetic replays / pre-snapshot
      iterations cannot make a confident drop decision.
    """
    if patch_type_value not in _TRIAL18_PREFLIGHT_METADATA_PATCH_TYPES:
        return False
    if not resolved_tables:
        return False
    raw_table = patch_body.get("table") or patch_body.get("target")
    if not isinstance(raw_table, str):
        return False
    target_table = raw_table.strip()
    if not target_table:
        return False
    return target_table not in resolved_tables


def _classify_synthesis_empty_reason(
    *,
    raw_proposals: list,
    parsed_output: Any,
) -> str:
    """Map the parsed Stage 3 response shape to a typed empty-synthesis
    reason.

    Trial 13 Track 5 — the dc89d1a9 run emitted 6 ``empty_synthesis``
    markers with no diagnostic signal. The classifier walks the
    response in fail-loud order:

    * ``"all_candidates_unsafe"`` — every raw proposal carried a
      patch_type the survival contract rejected. (raw_proposals
      non-empty, all dropped during ``_safe_patch_type`` filtering.)
    * ``"no_applicable_archetype"`` — the LLM emitted a
      ``decline_reason`` of ``no_applicable_archetype``.
    * ``"prompt_constraint_collision"`` — the LLM emitted a
      ``decline_reason`` of ``prompt_constraint_collision``.
    * ``"parse_returned_zero"`` — none of the above; the LLM returned
      a parseable response whose ``proposals`` list was already empty.
    """
    decline_reason = ""
    if parsed_output is not None:
        try:
            decline_reason = str(
                parsed_output.get("decline_reason") or ""
            ).strip().lower()
        except (AttributeError, TypeError):
            decline_reason = ""
    if decline_reason == "no_applicable_archetype":
        return "no_applicable_archetype"
    if decline_reason == "prompt_constraint_collision":
        return "prompt_constraint_collision"
    if raw_proposals:
        return "all_candidates_unsafe"
    return "parse_returned_zero"


def _load_prior_iteration_drops_from_ledger(
    *, iteration: int,
) -> dict[str, Any] | None:
    """Trial 22 W3 — read the most recent prior iteration's
    ``compiler_drop_summary`` from the durable
    ``iteration_candidate_ledger.jsonl``.

    Returns the summary dict for the highest iteration strictly less
    than ``iteration`` that carries a non-empty drop summary, or
    ``None`` when the ledger is absent/empty/parse-fails. Best-effort
    and side-effect-free — never raises into the synthesis path.
    """
    try:
        import os as _os

        from genie_space_optimizer.optimization.candidate_ledger import (
            read_ledger,
        )

        root = _os.environ.get("GSO_RUN_ARTIFACT_ROOT") or "/tmp"
        path = root + "/iteration_candidate_ledger.jsonl"
        if not _os.path.exists(path):
            return None
        rows = read_ledger(path)
    except Exception:
        return None

    best: dict[str, Any] | None = None
    best_iter = -1
    for row in rows:
        summary = getattr(row, "compiler_drop_summary", None)
        if not summary:
            continue
        row_iter = int(getattr(row, "iteration", -1))
        if row_iter < int(iteration) and row_iter > best_iter:
            best_iter = row_iter
            best = dict(summary)
    return best


def _build_request(
    *,
    cluster: FailureCluster,
    schema_slice: dict[str, Any],
    member_qid_evidence: list[dict[str, Any]],
    history: list[dict[str, Any]],
    iteration: int,
    forbidden_signatures: tuple[str, ...] = (),
    insufficient_repair_signatures: tuple[str, ...] = (),
    prior_iteration_drops: Mapping[str, Any] | None = None,
    asset_grounding: dict[str, Any] | None = None,
    pivot_directive: str = "",
) -> LlmReasoningRequest:
    rsm = _SKILL_LOADER.load_reasoning_metadata(_SKILL_ID)
    if rsm is None:
        raise RuntimeError(
            f"{_SKILL_ID!r} is not a reasoning skill — check SKILL.md "
            "frontmatter"
        )
    output_cls = _SKILL_LOADER.load_output_schema_class(_SKILL_ID)
    system_body = _SKILL_LOADER.load_prompt(
        _SKILL_ID, expected_constant_name=_PROMPT_CONST,
    )
    # Trial 16.3 — surface prior-iteration typed-rejection strings to
    # the lever LLM so it can avoid re-proposing patch_type / shape
    # combinations whose typed rejection already appears here. Without
    # this, postmortem 813949510175466 shows gs_013 re-proposing
    # ``update_column_description`` for the same missing_table cause
    # that just rejected ``add_column_description`` in the prior
    # iteration. Empty default keeps the prompt schema stable.
    # Trial 17 step 2 — Lever Selection Contract.
    # Emit the closed lever menu + plan-then-synthesize instructions
    # so the Stage 3 LLM reasons explicitly about which of the 6
    # levers it is operating on. The validator in this module drops
    # any proposal whose (selected_lever, patch_type) pair is
    # inconsistent with LEVER_TO_PATCH_TYPES.
    from genie_space_optimizer.optimization.levers_contract import (
        archetype_catalog_menu_for_prompt,
        lever_menu_for_prompt,
    )

    # Trial 20 Workstream D — multi-lever bundles as default strategy.
    # When the cluster has ``insufficient_repair_signatures`` (the
    # Trial 18 sibling channel from ``kept_insufficient`` acceptance),
    # a single-lever proposal is insufficient by construction: the
    # last attempt already produced a behaviour-unchanged candidate
    # under that lever family, so the strategist must reinforce with
    # a SECOND lever family in the same ``bundle_id``. The free-text
    # ``single_lever_justification`` field lets iteration 1 emit a
    # single-lever candidate when the diagnosis genuinely fits one
    # family; the LLM owns that argument.
    from genie_space_optimizer.optimization.trial20_flags import (
        trial20_multi_lever_bundle_default_enabled,
    )

    _t20_bundle_default = trial20_multi_lever_bundle_default_enabled()
    _t20_has_insufficient = bool(insufficient_repair_signatures)

    # Phase 2 P2.1 — ``single_lever_justification`` retired. The new
    # ``selected_levers`` list expresses kit cardinality directly
    # (len==1 ⇒ single lever, len>=2 ⇒ kit), and the KIT_FOR_RCA
    # validator (P2.2) hard-rejects single-element kits for diagnoses
    # that demand a companion. We no longer ask the LLM to write a
    # free-text rebuttal alongside its lever choice — the choice
    # itself is auditable.
    if _t20_bundle_default:
        if _t20_has_insufficient:
            _t20_bundle_clause = (
                "  - bundle_id: REQUIRED on this iteration. The cluster "
                "carries non-empty insufficient_repair_signatures, so a "
                "single-lever repair has already proven insufficient. "
                "Emit a multi-lever bundle of >=2 proposals sharing the "
                "same bundle_id and DIFFERENT lever families, and set "
                "``selected_levers`` on EACH proposal to the same kit. "
                "Single-lever proposals will be rejected at the "
                "validator.\n"
            )
        else:
            _t20_bundle_clause = (
                "  - bundle_id: STRONGLY PREFERRED. Multi-lever bundles "
                "are the default strategy when the diagnosis suggests "
                "a single lever cannot address the failure mode. Emit "
                ">=2 proposals sharing the same bundle_id and "
                "DIFFERENT lever families, and set ``selected_levers`` "
                "on EACH proposal to the same kit list.\n"
            )
    else:
        _t20_bundle_clause = (
            "  - bundle_id (optional): non-empty string when multiple "
            "proposals must be applied together.\n"
        )

    lever_contract_instructions = (
        "Trial 17 — Lever Selection Contract. For EACH proposal you "
        "emit, you MUST also set:\n"
        "  - selected_levers: a CLOSED list of lever_ids the proposal "
        "    recruits as a kit (e.g. ['lever-1', 'lever-6']). Each "
        "    entry MUST be drawn from 'lever-1' .. 'lever-6' (see "
        "    lever_menu). EMIT 2+ ENTRIES for grammar-pivot, "
        "    join-semantics, time-grain, value-mapping, and "
        "    column-disambiguation diagnoses (the KIT_FOR_RCA "
        "    contract — see Phase 2 P2.2). EMIT 1 ENTRY only for "
        "    genuinely single-lever diagnoses such as plain "
        "    add_instruction prose for soft policy.\n"
        "  - selected_lever: DEPRECATED. The deterministic validator "
        "    still reads this when ``selected_levers`` is empty; "
        "    prefer setting ``selected_levers`` directly.\n"
        "  - expected_behavioral_change: one or two sentences "
        "describing what the generated SQL grammar will do "
        "differently after this patch lands. Be concrete: name the "
        "shape (ORDER BY, LIMIT, RANK, etc.) and the column.\n"
        "  - fallback_lever: which lever to try next iteration if "
        "sliced eval shows target_unchanged. Same closed enum.\n"
        + _t20_bundle_clause +
        "The 'allowed_patch_types' in each lever_menu entry MUST be "
        "consistent with this proposal's patch_type. Inconsistent "
        "pairs (e.g. selected_lever='lever-1' with "
        "patch_type='add_instruction') will be rejected by the "
        "deterministic validator and surfaced as a forbidden_signature "
        "in the next iteration's prompt under "
        "'lever_plan_violation:plan=<X>,patch=<Y>'.\n"
        "\n"
        "Lever selection guidance (read BEFORE picking a lever, including "
        "on iteration 1 when no forbidden_signatures exist):\n"
        "  * Each lever_menu entry now carries 'description' and "
        "'prefer_when'. Match the diagnosis 'rca_kind_label' against "
        "the 'prefer_when' tokens to pick the right family.\n"
        "  * Grammar-pivot diagnoses — the rca_kind names a concrete "
        "SQL shape such as 'RANK() instead of LIMIT N', 'missing "
        "ORDER BY', 'missing GROUP BY', 'missing WHERE filter', "
        "'missing window' — are LEAST likely to be fixed by lever-5a "
        "(add_instruction prose alone). The Genie planner reads "
        "instructions as soft hints, not grammar constraints, so a "
        "prose-only patch on a grammar pivot is the highest-risk "
        "lever-5 choice. Prefer:\n"
        "      lever-6 (sql_snippet_filter / expression / measure) — "
        "       the most direct way to install a reusable grammar "
        "       shape the planner can compose; OR\n"
        "      lever-5b (add_example_sql) — anchor the shape by "
        "       demonstration when no compact snippet captures it; OR\n"
        "      lever-1 (add_column_description) with a worked example "
        "       in the description IF the underlying ambiguity is "
        "       column-meaning, not grammar.\n"
        "  * Reserve lever-5a (add_instruction prose) for "
        "non-grammar diagnoses: business terminology, soft policy, or "
        "conventions that don't change SQL shape.\n"
        "\n"
        "Consult the prior-iteration forbidden_signatures (below) to "
        "pivot away from levers that already failed for this QID + "
        "rca_kind. When an instruction-only patch returned "
        "target_unchanged, prefer a structural lever (lever-4 / "
        "lever-6) or a metadata lever (lever-1) — repeating the same "
        "lever for the same rca_kind is unlikely to change behavior.\n"
        "\n"
        "Phase 2 P2.1 — Lever Kit mandate. Emit ``selected_levers`` as "
        "a LIST of lever_ids drawn from the closed enum above; ONE "
        "entry only for genuinely single-lever fixes (plain prose "
        "for soft policy), TWO OR MORE entries for any diagnosis "
        "whose rca_kind matches KIT_FOR_RCA (value_mapping_missing, "
        "join_semantics_wrong, time_grain_wrong, column_disambiguation, "
        "table_routing_wrong) — the synthesizer will hard-reject "
        "single-lever proposals for those diagnoses."
    )

    # Trial 23 W4 — RCA-to-mechanism routing (correct at source). Name
    # the RCA kinds add_example_sql CANNOT fix and the mechanism that
    # can, so the producer stops reaching for the inert exemplar lever.
    try:
        from genie_space_optimizer.optimization.trial23_flags import (
            trial23_rca_mechanism_routing_enabled as _t23_w4_on,
        )
        _t23_w4_enabled = _t23_w4_on()
    except Exception:
        _t23_w4_enabled = False
    if _t23_w4_enabled:
        lever_contract_instructions += (
            "\n\n"
            "Trial 23 W4 — RCA-to-mechanism routing (MANDATORY for the "
            "RCA kinds below). add_example_sql (lever-5b) anchors a SQL "
            "SHAPE by demonstration but is BEHAVIORALLY INERT for these "
            "root causes; routing them to add_example_sql alone wastes "
            "the iteration. For each, prefer the named fixing "
            "mechanism (you MAY add an exemplar as a COMPANION, but it "
            "MUST be paired with the fixing lever, never used alone):\n"
            "  * extra_defensive_filter — the planner injects an "
            "unwanted predicate. Fix with lever-5a (add_instruction "
            "telling the planner not to add the filter) OR lever-6 "
            "(add_sql_snippet_filter overriding the WHERE clause). An "
            "exemplar alone does NOT suppress the filter.\n"
            "  * top_n_cardinality_collapse — a top-N/ranking query "
            "collapses cardinality at the wrong grain. Fix with lever-6 "
            "(add_sql_snippet_expression/measure) OR a measure "
            "description. Another exemplar repeats the collapse.\n"
            "  * canonical_dimension_missed — the planner never routes "
            "to the canonical dimension because it is undescribed. Fix "
            "with lever-1/lever-2 (add column/table description or "
            "synonym) OR a routing change exposing the dimension. An "
            "exemplar cannot conjure a dimension the planner does not "
            "know exists.\n"
            "Emitting add_example_sql as the SOLE mechanism for any of "
            "the above will be flagged "
            "(rca_mechanism_defaulted_to_example_sql) and fed back as a "
            "forbidden signature next iteration."
        )

    # Trial 24 — Kit at Source. Promote the W4 "prefer the named
    # mechanism" guidance to a HARD KIT MANDATE for the two example-SQL-
    # insufficient RCAs that died as lone single levers in the e943 /
    # d139 postmortems. When the Trial 24 flag is on, KIT_FOR_RCA carries
    # these RCAs (see action_groups._TRIAL24_KIT_FOR_RCA), so the P2.2
    # validator hard-rejects single-lever proposals; the prompt must tell
    # the producer to emit the >= 2-lever-family kit on the FIRST try so
    # it does not burn an iteration on the inevitable kit_for_rca
    # violation retry.
    try:
        from genie_space_optimizer.optimization.trial24_flags import (
            trial24_filter_removal_solo_enabled as _t24_solo_on,
            trial24_kit_at_source_enabled as _t24_on,
        )
        _t24_enabled = _t24_on()
        _t24_filter_solo = _t24_solo_on()
    except Exception:
        _t24_enabled = False
        _t24_filter_solo = False
    if _t24_enabled:
        # Trial 26 W26.2 — derive the kit-mandate enumeration from the
        # ACTIVE kit map instead of hard-coding RCA kinds. The validator
        # (action_groups.kit_for_rca_violation_reason) rejects single-
        # lever proposals for every RCA in the active map; before this
        # change the prompt only named the two original Trial 24 kinds,
        # so when W26.2 expanded the map to wrong_aggregation /
        # wrong_column the producer was never told to emit their kit and
        # the proposal died as kit_for_rca_violation:...:singleton. Reading
        # the same map the validator reads keeps producer and gate in
        # lock-step; a future map expansion needs no edit here. Scope is
        # the EXTENSION kinds only (we subtract the base KIT_FOR_RCA,
        # whose kits are covered by the general P2.1 lever-kit mandate
        # above) so base-kind prompt behaviour is byte-stable.
        from genie_space_optimizer.optimization.stages.action_groups import (
            KIT_FOR_RCA as _base_kit_for_rca,
            active_kit_for_rca_map as _active_kit_for_rca_map,
        )

        _extension_kit = {
            _rca: _levers
            for _rca, _levers in _active_kit_for_rca_map().items()
            if _rca not in _base_kit_for_rca
        }
        _kit_bullets: list[str] = []
        # Follow-on B — when filter-removal-solo is on, extra_defensive_filter
        # is a single-mechanism INSTRUCTION fix excluded from the kit map
        # (a filter REMOVAL cannot be expressed as a positive snippet — the
        # LLM emits a no-op 1=1 / TRUE that is rejected). Surface its
        # justified-solo instruction explicitly since it is absent from
        # ``_extension_kit``.
        if _t24_filter_solo:
            _kit_bullets.append(
                "  * extra_defensive_filter — this is a filter-REMOVAL "
                "fix: emit a SINGLE add_instruction (lever-5a) telling the "
                "planner NOT to inject the defensive predicate. Do NOT "
                "express removal as a positive SQL snippet — a "
                "``1=1`` / ``TRUE`` tautology is a behavioral no-op and is "
                "rejected. Ground the instruction with a concrete "
                "``expected_behavioral_change`` so it lands solo.\n"
            )
        for _rca in sorted(_extension_kit):
            _companions = sorted(_extension_kit[_rca])
            _kit_bullets.append(
                f"  * {_rca} — MUST emit a >= 2-lever-family kit recruiting "
                f"its companion lever families {_companions} (see "
                f"lever_menu above for what each lever_id does). Set "
                f"``selected_levers`` to this kit on EVERY member and share "
                f"a ``bundle_id``; a single-lever proposal is hard-rejected "
                f"as kit_for_rca_violation:rca={_rca}:singleton.\n"
            )
        lever_contract_instructions += (
            "\n\n"
            "Trial 24 — Kit at Source (MANDATORY KIT, not a preference). "
            "The RCA kinds below carry a KIT_FOR_RCA companion "
            "contract: a SINGLE-lever proposal for them is HARD-REJECTED "
            "(kit_for_rca_violation:...:singleton) and wastes the "
            "iteration. Emit a >= 2-lever-family kit on the FIRST try, "
            "with ``selected_levers`` set to the kit on EVERY member and "
            "a shared ``bundle_id``:\n"
            + "".join(_kit_bullets)
            + "The instruction/metadata member does NOT need a separate "
            "justification slot when it ships inside this kit — the "
            "companion structural lever IS the justification."
        )

    # Trial 20 Workstream D2 — curated example bundle patterns. Surfaced
    # as ILLUSTRATIVE EXAMPLES, NOT mandatory mappings; the LLM keeps
    # full lever-selection autonomy. Templates describe the kind of
    # composite reinforcement that has historically been needed for
    # certain failure modes — not a deterministic lookup.
    if _t20_bundle_default:
        lever_contract_instructions += (
            "\n\n"
            "Curated Example Bundle Patterns (ILLUSTRATIVE ONLY, not "
            "mandatory): when emitting a multi-lever bundle, the "
            "combinations below have historically been effective. "
            "You MAY pick a different combination when your reasoning "
            "suggests so — no template gates output.\n"
            "  * Canonical-dimension disambiguation (e.g. ambiguous "
            "category column): "
            "(lever-1: add_column_description) + (lever-6: "
            "add_sql_snippet_filter)\n"
            "  * Top-N / RANK rewrite (e.g. RANK<=10 instead of "
            "LIMIT 10): "
            "(lever-1: add_column_description) + (lever-5: "
            "add_example_sql), OR "
            "(lever-6: add_sql_snippet_expression) + (lever-5: "
            "add_example_sql)\n"
            "  * Missing WHERE filter (e.g. defensive filter Genie "
            "did not infer): "
            "(lever-1: add_column_description) + (lever-6: "
            "add_sql_snippet_filter)\n"
        )

    # Phase 0 P0.4 — LRU compaction. Stage 3's growth-prone slots
    # (``history``, ``forbidden_signatures``,
    # ``insufficient_repair_signatures``) are the dominant token
    # contributors after multiple iterations. We compact in place
    # ordered from "least valuable" → "most valuable" so the most
    # recent signal is preserved:
    #   1. ``history``                          — drop oldest first
    #   2. ``forbidden_signatures``             — drop oldest first
    #   3. ``insufficient_repair_signatures``   — drop oldest LAST
    # If even after compacting the budget is still too small, the
    # LlmReasoningCall pre-admission gate will emit a typed
    # PROMPT_TOO_LARGE abstain and the caller defers cleanly.
    from genie_space_optimizer.optimization.llm_prompt_compaction import (
        compact_history_slots_to_fit,
    )
    from genie_space_optimizer.optimization.llm_reasoning_call import (
        MAX_PROMPT_INPUT_TOKENS,
    )
    # Phase 1 P1.3 — fixed-window cap BEFORE the LRU compactor sees
    # the slots. The structural cap keeps the prompt size bounded
    # even when the run has accumulated 20+ iterations; the LRU
    # compactor remains the final fail-safe for the rare case where
    # the last-3-iterations payload itself overflows.
    from genie_space_optimizer.optimization.llm_history_window import (
        cap_iteration_bucketed_history,
        cap_signature_list,
    )

    history = cap_iteration_bucketed_history(
        history or [], current_iteration=iteration,
    )
    forbidden_list = cap_signature_list(forbidden_signatures or ())
    insufficient_list = cap_signature_list(
        insufficient_repair_signatures or ()
    )
    static_chars = (
        len(system_body)
        + len(json.dumps(
            {
                "iteration": iteration,
                "cluster": cluster.to_json(),
                "member_qid_evidence": member_qid_evidence,
                "schema_slice": schema_slice,
                "history": [],
                "forbidden_signatures": [],
                "insufficient_repair_signatures": [],
                "lever_menu": lever_menu_for_prompt(),
                "lever_contract_instructions": lever_contract_instructions,
                "archetype_catalog_menu": archetype_catalog_menu_for_prompt(),
            },
            default=str,
        ))
    )
    compact_history_slots_to_fit(
        static_chars=static_chars,
        history_slots=[
            ("history", history),
            ("forbidden_signatures", forbidden_list),
            ("insufficient_repair_signatures", insufficient_list),
        ],
        target_token_cap=MAX_PROMPT_INPUT_TOKENS,
    )

    # Phase 0 P0.5 — split static menus into cacheable blocks. The
    # lever_menu, lever_contract_instructions, and
    # archetype_catalog_menu are byte-identical across iterations
    # and clusters, so the Anthropic prompt cache can serve them
    # at 0.1x cost after the first call warms the cache. Schema
    # slice is stable per cluster but not across clusters; we keep
    # it in the dynamic payload to avoid invalidating the cache on
    # every cluster switch.
    lever_menu_json = json.dumps(
        {"lever_menu": lever_menu_for_prompt()}, default=str,
    )
    lever_contract_block = (
        "lever_contract_instructions:\n"
        + lever_contract_instructions
    )
    archetype_menu_json = json.dumps(
        {"archetype_catalog_menu": archetype_catalog_menu_for_prompt()},
        default=str,
    )

    # Trial 22 W3 — durable retry feedback. When the prior iteration's
    # slate compiler dropped every proposal, surface the exact drop
    # reasons / bundle violations so the LLM does not re-emit the same
    # shapes. Sourced from the durable iteration terminal-state ledger
    # (NOT a transient cluster result) — see harness wiring.
    from genie_space_optimizer.optimization.proposal_slate_compiler import (
        render_prior_iteration_drops,
    )

    _t22_prior_drops_section = render_prior_iteration_drops(
        prior_iteration_drops
    )

    _user_payload: dict[str, Any] = {
            "iteration": iteration,
            "cluster": cluster.to_json(),
            "member_qid_evidence": member_qid_evidence,
            "schema_slice": schema_slice,
            "history": history,
            # Trial 22 W3 — rendered prior-iteration compiler drops.
            # Empty string when the prior iteration had no full drop.
            "prior_iteration_drops": _t22_prior_drops_section,
            "forbidden_signatures": list(forbidden_list),
            # Trial 19 Workstream A (A2) — sibling channel for
            # ``insufficient_repair_signatures``. Distinct from
            # ``forbidden_signatures`` (hard rejections): signatures
            # here name (lever, patch_type, rca_kind, behavior)
            # quadruples that landed via the Trial 18 acceptance
            # gate's ``kept_insufficient`` lane — they applied
            # cleanly and did not regress, but did NOT move
            # accuracy. The Stage 3 prompt instructs the LLM to
            # treat each entry as forbidden as a sole primary
            # repair for the same QID/RCA family; it must add a
            # concrete reinforcement (bundle a second lever or
            # patch_type) or pivot to a different lever.
            #
            # Plumbed by ``run_plan11_synthesis_for_single_cluster``
            # from the per-iteration
            # ``TransformerContext.insufficient_repair_signatures``,
            # which the harness accumulates across iterations via
            # ``harvest_sm_insufficient_repair_signatures`` /
            # ``extend_sm_insufficient_repair_signatures``.
            "insufficient_repair_signatures": list(insufficient_list),
            # NOTE Phase 0 P0.5 — lever_menu / lever_contract_instructions /
            # archetype_catalog_menu were moved out of this JSON
            # payload into cacheable_user_blocks below so the
            # Anthropic prompt cache can serve them at 0.1x cost
            # after the first warm-up. They are still SENT to the
            # LLM, just as separate user content blocks.
    }
    # Trial 23 W5 — pre-generation asset grounding. Only present when the
    # caller resolved a grounding block (diagnosis lacked implicated
    # assets AND blame_set resolved against the slice), so the prompt is
    # byte-stable for clusters that do not need grounding.
    if asset_grounding:
        _user_payload["asset_grounding"] = asset_grounding
    # Trial 23 W8 — pivot destination. Only present on the single
    # pivot re-prompt issued when a sole-lever-in-rejected-family drop
    # would otherwise empty the slate, so the prompt is byte-stable for
    # every normal synthesis call.
    if pivot_directive:
        _user_payload["pivot_directive"] = pivot_directive
    user_prompt = json.dumps(_user_payload, default=str)
    return LlmReasoningRequest(
        call_id=(
            f"plan11_stage3_synthesize.{cluster.cluster_id}"
            f".iter_{int(iteration)}"
        ),
        skill_id=_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens,
        cacheable_user_blocks=(
            lever_menu_json,
            lever_contract_block,
            archetype_menu_json,
        ),
    )


def _target_objects_from_blame_set(
    blame_set: list[str] | tuple[str, ...],
) -> tuple:
    """Plan 12 — derive RepairProposal.target_objects from blame_set
    entries when the Stage 3 LLM did not emit a target_objects field
    (the current Plan 11 ``Plan11SynthesizeOutput`` schema doesn't).

    Identifier shapes accepted (Trial 13g — defense-in-depth alongside
    the Stage 3 prompt's FQN guidance):

      * ``catalog.schema.table.column`` (4+ parts) — fully qualified;
        grouped by the 3-part table prefix with the column appended.
      * ``catalog.schema.table`` (3 parts) — TABLE TargetObject with
        empty ``columns``.
      * ``table.column`` (2 parts) — TABLE TargetObject using the
        ``table`` portion as the identifier with the column appended.
        Tolerated because Stage 1 RCA evidence and Genie Space
        configs frequently surface 2-part identifiers when the
        catalog/schema is implicit; ``TargetObject.identifier`` only
        enforces non-empty.
      * ``table`` (1 part, no dot) — TABLE TargetObject with empty
        ``columns``.

    Entries are deduplicated by identifier in arrival order;
    whitespace-only entries are skipped.
    """
    from genie_space_optimizer.optimization.target_object_typed import (
        AssetKind,
        TargetObject,
    )

    by_table: dict[str, list[str]] = {}
    table_order: list[str] = []
    for raw in blame_set or ():
        s = str(raw).strip()
        if not s:
            continue
        parts = s.split(".")
        if len(parts) >= 4:
            table_id = ".".join(parts[:3])
            column = parts[3]
        elif len(parts) == 3:
            table_id = s
            column = ""
        elif len(parts) == 2:
            # Trial 13g — accept 2-part ``table.column``. The LLM (and
            # the Stage 1 evidence it grounds in) often elides the
            # catalog/schema prefix; dropping these entries left
            # ``target_objects`` empty and tripped the Plan 12 survival
            # contract on otherwise-valid proposals.
            table_id = parts[0].strip()
            column = parts[1].strip()
        else:
            # Single bare identifier — treat as a TABLE with no
            # explicit column. Non-empty was guaranteed above.
            table_id = s
            column = ""
        if not table_id:
            continue
        if table_id not in by_table:
            by_table[table_id] = []
            table_order.append(table_id)
        if column and column not in by_table[table_id]:
            by_table[table_id].append(column)

    out: list[TargetObject] = []
    for table_id in table_order:
        out.append(
            TargetObject(
                asset_kind=AssetKind.TABLE,
                identifier=table_id,
                columns=tuple(by_table[table_id]),
            )
        )
    return tuple(out)


def _union_member_blame_sets(
    member_qid_evidence: list[dict[str, Any]] | None,
) -> list[str]:
    """Trial 13g — final fallback in the blame derivation chain.

    Iterates ``member_qid_evidence`` (the per-QID Stage 1 diagnoses
    threaded into Stage 3 by the SM transformer or batch caller) and
    returns the deduplicated union of every entry's ``blame_set`` in
    arrival order. An entry may surface ``blame_set`` at the top
    level (the shape the SM transformer emits) or nested under a
    ``diagnosis`` key (matching the prompt's
    ``<context_inputs>`` ``diagnosis (PerQidDiagnosis)`` convention).

    Returns ``[]`` when ``member_qid_evidence`` is empty or no entry
    carries a blame_set — the caller treats that as the final
    ``"empty"`` source bucket in :data:`PROPOSALS_BLAME_SET_SOURCES`.
    """
    if not member_qid_evidence:
        return []
    seen: set[str] = set()
    unioned: list[str] = []
    for entry in member_qid_evidence:
        if not isinstance(entry, dict):
            continue
        blame_raw = entry.get("blame_set")
        if not blame_raw:
            diagnosis = entry.get("diagnosis") or {}
            if isinstance(diagnosis, dict):
                blame_raw = diagnosis.get("blame_set")
        for blame in (blame_raw or ()):
            s = str(blame).strip()
            if s and s not in seen:
                seen.add(s)
                unioned.append(s)
    return unioned


def _safe_patch_type(raw: str) -> PatchType | None:
    """Resolve a free-form patch_type string to the closed enum.

    Returns ``None`` if the LLM emitted a value that does not match
    any ``PatchType`` member after case-folding (Step 0 of
    ``validate_patch.py`` would reject the proposal anyway; we drop it
    here so the repair loop in Task 9 has a typed surface).

    Trial 13e — delegates to :func:`coerce_patch_type` so an LLM that
    emits UPPER_SNAKE (``"ADD_INSTRUCTION"``) is tolerated instead of
    silently dropping every proposal.
    """
    from genie_space_optimizer.optimization.repair_intent import (
        coerce_patch_type,
    )
    return coerce_patch_type(raw)


def run_plan11_synthesis_for_single_cluster(
    cluster: FailureCluster,
    schema_slice: dict[str, Any],
    history: list[dict[str, Any]],
    *,
    member_qid_evidence: list[dict[str, Any]] | None = None,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    w: Any,
    forbidden_signatures: tuple[str, ...] = (),
    insufficient_repair_signatures: tuple[str, ...] = (),
    llm_response_override: Any = None,
    # P4 producer-side hooks. Defaults keep legacy callers (tests,
    # batched fan-out, replay harnesses) byte-stable; only the live SM
    # transformer in ``state_machine/transformers/synthesize_llm.py``
    # passes the new kwargs through.
    metadata_snapshot: dict[str, Any] | None = None,
    space_id: str = "",
    prior_mechanism_attempts: tuple[Any, ...] = (),
    # P4 C8 — gold/warehouse plumbing for the snippet validator. Empty
    # values short-circuit to the metadata-only validation path.
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
    # Trial 22 W3 — the prior iteration's durable compiler_drop_summary
    # (read from the iteration terminal-state ledger by the harness).
    # Threaded into the Stage 3 prompt so the LLM avoids re-emitting
    # shapes the compiler just dropped. None on iteration 1.
    prior_iteration_drops: Mapping[str, Any] | None = None,
    # Trial 23 W8 — recursion guard for the pivot-destination re-prompt.
    # Set True only on the single re-entry the D3 sole-lever drop issues
    # so a replacement bundle that also empties cannot recurse forever.
    _pivot_attempt: bool = False,
) -> ClusterSynthesisResult:
    """Plan 11 Stage 3 — synthesize patches for one cluster via LLM.

    Returns :class:`ClusterSynthesisResult` (same type as the legacy
    ``run_cluster_driven_synthesis_for_single_cluster`` so PR 2 wiring is
    a drop-in replacement).

    ``proposal`` carries the first RepairProposal's ``to_json()`` dict
    (legacy contract). ``skipped_reason`` uses an ``exception:…`` colon
    prefix when the LLM declines, so ``ClusterSynthesisResult`` accepts
    it under the closed :class:`SkippedReason` invariant.

    Phase 1 P1.1 — when ``llm_response_override`` is provided the
    function skips the live ``LlmReasoningCall.invoke`` and uses that
    response directly. This is the seam ``run_plan11_synthesis_for_all_clusters``
    uses to fan out a single batched LLM call's proposals into per-
    cluster ``ClusterSynthesisResult`` envelopes while reusing all of
    the survival-contract validators, marker emission, blame-set
    fallbacks, and lever-plan checks that live in this function.
    """
    # Trial 22 W3 — durable read path. When the caller did not hand us
    # the prior iteration's compiler_drop_summary, recover it directly
    # from the durable iteration_candidate_ledger.jsonl (the SAME
    # artifact the harness persists it to). This keeps the retry-feedback
    # round-trip self-contained and ledger-backed rather than depending
    # on fragile cross-iteration in-memory threading through the SM.
    if prior_iteration_drops is None and int(iteration) > 1:
        prior_iteration_drops = _load_prior_iteration_drops_from_ledger(
            iteration=int(iteration)
        )

    # Trial 21 W3+C8 — verdict pre-declared so both the live-LLM-call
    # branch and the batched-override branch can hand the same value
    # to the Actuator wire-in below.
    _stage3_prompt_size_verdict_local: dict[str, int | bool] = {}
    if llm_response_override is None:
        # Trial 23 W5 — pre-generation asset grounding. When the cluster's
        # repair diagnosis lacks resolved implicated_assets but the
        # blame_set resolves against the schema slice, inject the resolved
        # catalog.schema.table[.column] references so the LLM anchors
        # SQL-shape repairs to assets that exist (instead of emitting
        # ungrounded snippets the applier cannot land). Observe-and-ground,
        # NOT a block — the hard-block promotion stays behind
        # trial23_asset_grounding_blocking_enabled (default OFF) until the
        # W7-W9 repair paths exist (plan "central design tension").
        _w5_grounding: dict[str, Any] | None = None
        try:
            from genie_space_optimizer.optimization.trial23_flags import (
                trial23_asset_grounding_enabled,
            )
            if trial23_asset_grounding_enabled():
                from genie_space_optimizer.optimization.asset_grounding import (
                    asset_grounding_injected_marker as _w5_marker,
                    build_asset_grounding as _w5_build,
                )
                _w5_implicated: list[str] = []
                _w5_shape_parts: list[str] = []
                for _ev in member_qid_evidence or []:
                    _diag = _ev.get("diagnosis") if isinstance(_ev, dict) else None
                    if _diag is None:
                        continue
                    for _a in (
                        getattr(_diag, "implicated_assets", None)
                        or (_diag.get("implicated_assets")
                            if isinstance(_diag, dict) else ())
                        or ()
                    ):
                        if str(_a or "").strip():
                            _w5_implicated.append(str(_a))
                    _exp = (
                        getattr(_diag, "expected_sql_shape", None)
                        or (_diag.get("expected_sql_shape")
                            if isinstance(_diag, dict) else "")
                        or ""
                    )
                    if str(_exp or "").strip():
                        _w5_shape_parts.append(str(_exp))
                _w5_grounding = _w5_build(
                    schema_slice=schema_slice,
                    blame_set=tuple(cluster.primary_blame_set or ()),
                    implicated_assets=tuple(_w5_implicated),
                    root_cause=str(getattr(cluster, "root_cause", "") or ""),
                    sql_shape_delta="; ".join(_w5_shape_parts),
                )
                if _w5_grounding:
                    print(
                        _w5_marker(
                            optimization_run_id=optimization_run_id,
                            iteration=iteration,
                            cluster_id=cluster.cluster_id,
                            root_cause=str(
                                getattr(cluster, "root_cause", "") or ""
                            ),
                            resolved_assets=tuple(
                                _w5_grounding.get("resolved_assets", ())
                            ),
                        ),
                        flush=True,
                    )
        except Exception:
            _w5_grounding = None

        request = _build_request(
            cluster=cluster,
            schema_slice=schema_slice,
            member_qid_evidence=member_qid_evidence or [],
            history=history,
            iteration=iteration,
            forbidden_signatures=forbidden_signatures,
            insufficient_repair_signatures=insufficient_repair_signatures,
            prior_iteration_drops=prior_iteration_drops,
            asset_grounding=_w5_grounding,
        )

        # Trial 19 A5 — emit the audit marker IFF a non-empty insufficient
        # signature tuple reached this LLM call. Lets postmortem grading
        # join harness ``_sm_insufficient_repair_signatures`` against the
        # actual consumer-side counts (G2 invariant).
        if insufficient_repair_signatures:
            _pairs_preview = tuple(insufficient_repair_signatures[:20])
            print(
                insufficient_signatures_in_context_marker(
                    optimization_run_id=optimization_run_id,
                    iteration=iteration,
                    stage="plan11_synthesize",
                    count=len(insufficient_repair_signatures),
                    qid_rca_pairs=_pairs_preview,
                )
            )

        # Trial 21 W3+C8 — emit a per-cluster prompt-size breakdown
        # marker BEFORE the LLM call so postmortems can audit
        # segment-level token attribution and the 40k cap, AND stash
        # the verdict on the request so the Evidence Actuator can
        # drop downstream proposals with ``PROMPT_SPLIT_REQUIRED``.
        # Flipped to enforce-mode: ``observe_only=False``.
        _stage3_size_verdict: dict[str, int | bool] = {}
        try:
            from genie_space_optimizer.optimization.stage3_prompt_sizer import (
                STAGE3_PROMPT_TOTAL_CAP as _C8_TOTAL_CAP,
                estimate_tokens as _c8_estimate_tokens,
                slice_segments as _c8_slice_segments,
            )
            _user_prompt_tokens = _c8_estimate_tokens(
                str(getattr(request, "user_prompt", "") or "")
            )
            _cacheable_blocks = tuple(
                getattr(request, "cacheable_user_blocks", ()) or ()
            )
            _cacheable_tokens = sum(
                _c8_estimate_tokens(str(b or "")) for b in _cacheable_blocks
            )
            _system_tokens = _c8_estimate_tokens(
                str(getattr(request, "system_msg", "") or "")
            )
            _stage3_size_verdict = _c8_slice_segments(
                system_msg_tokens=int(_system_tokens),
                user_prompt_tokens=int(_user_prompt_tokens),
                cacheable_block_tokens=int(_cacheable_tokens),
                cap=int(_C8_TOTAL_CAP),
            )
            import json as _c8_json
            _marker_payload = dict(_stage3_size_verdict)
            _marker_payload["optimization_run_id"] = str(optimization_run_id)
            _marker_payload["iteration"] = int(iteration)
            _marker_payload["cluster_id"] = str(cluster.cluster_id)
            # Echo the pre-slice user_prompt_tokens so postmortems can
            # see the original overage that drove sub_cluster_split.
            _marker_payload["user_prompt_tokens_pre_slice"] = int(
                _user_prompt_tokens
            )
            print(
                "GSO_STAGE3_PROMPT_SIZE_BREAKDOWN_V1 "
                + _c8_json.dumps(
                    _marker_payload, sort_keys=True, default=str
                ),
                flush=True,
            )
        except Exception:
            pass
        # Stash on the cluster's local state so the Actuator wire-in at
        # the function tail can read it without restructuring the
        # control flow.
        _stage3_prompt_size_verdict_local = _stage3_size_verdict

        # Trial 22 W7 — when the RCA-subcluster Stage 3 request overflows
        # the cap (``sub_cluster_split_needed``), compute and emit the
        # deterministic token-budget-aware partition so postmortems can
        # see the slice the runtime must honor. Scoped to the
        # RCA-subcluster builder so the (already-working) H001 cluster
        # path is never split (bright-line #5). Default-ON sub-flag
        # ``GSO_TRIAL22_SUBCLUSTER_SLICE`` is the rollback switch.
        try:
            import os as _w7_os

            _w7_flag = (
                _w7_os.environ.get("GSO_TRIAL22_SUBCLUSTER_SLICE") or ""
            ).strip().lower()
            _w7_enabled = _w7_flag not in ("0", "false", "no", "off")
            _w7_split = bool(
                _stage3_size_verdict.get("sub_cluster_split_needed")
            )
            _w7_builder = (
                "rca_subcluster"
                if "subcluster" in str(cluster.cluster_id or "").lower()
                else "cluster"
            )
            if (
                _w7_enabled
                and _w7_split
                and _w7_builder == "rca_subcluster"
            ):
                from genie_space_optimizer.optimization.stage3_prompt_sizer import (  # noqa: E501
                    STAGE3_PROMPT_TOTAL_CAP as _W7_CAP,
                    partition_rca_subcluster_by_token_budget as _w7_partition,
                    stage3_subcluster_split_marker as _w7_marker,
                )

                _w7_qids = [str(q) for q in (cluster.member_qids or ())]
                _w7_user_pre = int(
                    _marker_payload.get("user_prompt_tokens_pre_slice", 0)
                ) if isinstance(_marker_payload, dict) else 0
                _w7_sys = int(_system_tokens)
                _w7_cache = int(_cacheable_tokens)
                _w7_parts = _w7_partition(
                    qids=_w7_qids,
                    user_prompt_tokens=_w7_user_pre,
                    system_msg_tokens=_w7_sys,
                    cacheable_block_tokens=_w7_cache,
                    cap=int(_W7_CAP),
                )
                _w7_budget = int(_W7_CAP) - (_w7_sys + _w7_cache)
                if len(_w7_parts) > 1:
                    print(
                        _w7_marker(
                            optimization_run_id=str(optimization_run_id),
                            iteration=int(iteration),
                            cluster_id=str(cluster.cluster_id),
                            builder=_w7_builder,
                            partitions=_w7_parts,
                            user_prompt_tokens=_w7_user_pre,
                            user_budget=_w7_budget,
                            cap=int(_W7_CAP),
                        ),
                        flush=True,
                    )
        except Exception:
            pass

        # Trial 23 W6 — REAL partitioned re-dispatch. Trial 22 W7 above
        # only emits the planned-partition marker and then issues one
        # oversized call (declined as prompt_too_large). W6 makes N
        # smaller Stage 3 calls (one per QID partition) and merges the
        # proposals so the corrective mechanism family is synthesized
        # instead of declined.
        #
        # Pre-Trial-27 the dispatch was scoped to the RCA-subcluster
        # builder so the working H001 cluster path was never split
        # (bright-line #5). Trial 27 W27.1 relaxes that scope — when
        # ``trial27_w6_extend_nonsubcluster_enabled`` is True (default
        # ON when master ``GSO_TRIAL27_STAGE3_DESTARVE`` is ON), the
        # partition fires on ANY cluster with ``sub_cluster_split_needed
        # = True``. Bright-line #5 stays preserved structurally: when
        # the partition cannot actually split further (``len(_w6_parts)
        # == 1``), the existing guard below falls through to the
        # single-call branch byte-stably.
        _w6_did_dispatch = False
        try:
            from genie_space_optimizer.optimization.trial23_flags import (
                trial23_subcluster_real_slice_enabled,
            )
            from genie_space_optimizer.optimization.trial27_flags import (
                trial27_w6_extend_nonsubcluster_enabled,
            )
            _w6_is_subcluster = (
                "subcluster" in str(cluster.cluster_id or "").lower()
            )
            _w6_split_needed = bool(
                _stage3_prompt_size_verdict_local.get(
                    "sub_cluster_split_needed"
                )
            )
            _w27_extend = trial27_w6_extend_nonsubcluster_enabled()
            _w6_eligible_builder = (
                _w6_is_subcluster or _w27_extend
            )
            if (
                trial23_subcluster_real_slice_enabled()
                and _w6_eligible_builder
                and _w6_split_needed
            ):
                import dataclasses as _w6_dc

                from genie_space_optimizer.optimization.stage3_prompt_sizer import (  # noqa: E501
                    STAGE3_PROMPT_TOTAL_CAP as _W6_CAP,
                    estimate_tokens as _w6_estimate,
                    partition_rca_subcluster_by_token_budget as _w6_partition,
                )
                from genie_space_optimizer.optimization.subcluster_redispatch import (  # noqa: E501
                    merge_subcluster_responses as _w6_merge,
                    slice_member_evidence as _w6_slice_ev,
                    subcluster_real_slice_marker as _w6_marker,
                    trial27_w6_extended_marker as _w27_marker,
                )

                _w6_user_t = _w6_estimate(
                    str(getattr(request, "user_prompt", "") or "")
                )
                _w6_sys_t = _w6_estimate(
                    str(getattr(request, "system_msg", "") or "")
                )
                _w6_cache_t = sum(
                    _w6_estimate(str(b or ""))
                    for b in (
                        getattr(request, "cacheable_user_blocks", ()) or ()
                    )
                )
                _w6_parts = _w6_partition(
                    qids=[str(q) for q in (cluster.member_qids or ())],
                    user_prompt_tokens=int(_w6_user_t),
                    system_msg_tokens=int(_w6_sys_t),
                    cacheable_block_tokens=int(_w6_cache_t),
                    cap=int(_W6_CAP),
                )
                if len(_w6_parts) > 1:
                    _w6_responses = []
                    _t0 = time.monotonic()
                    for _w6_part in _w6_parts:
                        _w6_sub_cluster = _w6_dc.replace(
                            cluster, member_qids=tuple(_w6_part)
                        )
                        _w6_sub_request = _build_request(
                            cluster=_w6_sub_cluster,
                            schema_slice=schema_slice,
                            member_qid_evidence=_w6_slice_ev(
                                member_qid_evidence or [], _w6_part
                            ),
                            history=history,
                            iteration=iteration,
                            forbidden_signatures=forbidden_signatures,
                            insufficient_repair_signatures=(
                                insufficient_repair_signatures
                            ),
                            prior_iteration_drops=prior_iteration_drops,
                            asset_grounding=_w5_grounding,
                        )
                        _w6_responses.append(
                            LlmReasoningCall().invoke(
                                w=w, request=_w6_sub_request
                            )
                        )
                    duration_ms = int((time.monotonic() - _t0) * 1000)
                    resp = _w6_merge(
                        _w6_responses,
                        call_id=(
                            f"plan11_stage3_synthesize.{cluster.cluster_id}"
                            f".iter_{int(iteration)}"
                        ),
                        skill_id=_SKILL_ID,
                    )
                    _w6_merged_count = len(
                        (resp.parsed_output or {}).get("proposals", [])
                        if resp.parsed_output
                        else []
                    )
                    print(
                        _w6_marker(
                            optimization_run_id=optimization_run_id,
                            iteration=iteration,
                            cluster_id=cluster.cluster_id,
                            batch_count=len(_w6_parts),
                            batch_sizes=[len(p) for p in _w6_parts],
                            proposals_merged=_w6_merged_count,
                        ),
                        flush=True,
                    )
                    # Trial 27 W27.1 — additionally emit the extension
                    # marker when the dispatch engaged on a non-
                    # subcluster cluster (i.e., the W27.1 relaxation
                    # is what allowed this path to fire). Isolates the
                    # W27.1-attributable population for postmortems and
                    # rollback measurement; the subcluster-only path
                    # stays byte-stable (no W27 marker).
                    if _w27_extend and not _w6_is_subcluster:
                        print(
                            _w27_marker(
                                optimization_run_id=str(
                                    optimization_run_id
                                ),
                                iteration=int(iteration),
                                cluster_id=str(cluster.cluster_id),
                                member_qids_count=len(
                                    cluster.member_qids or ()
                                ),
                                partition_count=len(_w6_parts),
                                partition_sizes=[
                                    len(p) for p in _w6_parts
                                ],
                            ),
                            flush=True,
                        )
                    _w6_did_dispatch = True
        except Exception:
            _w6_did_dispatch = False

        if not _w6_did_dispatch:
            t0 = time.monotonic()
            resp = LlmReasoningCall().invoke(w=w, request=request)
            duration_ms = int((time.monotonic() - t0) * 1000)
    else:
        # Phase 1 P1.1 — reuse a pre-computed response from the batched
        # path. The override carries this cluster's slice of proposals
        # already partitioned by cluster_id by the batcher.
        resp = llm_response_override
        duration_ms = int(getattr(resp, "duration_ms", 0) or 0)

    tokens_in = int(getattr(resp, "tokens_input", 0) or 0)
    tokens_out = int(getattr(resp, "tokens_output", 0) or 0)

    if not resp.succeeded or resp.parsed_output is None:
        abstain_reason = ""
        abstain_explanation = ""
        if resp.declined is not None:
            abstain_reason = str(getattr(resp.declined, "reason", ""))
            abstain_explanation = str(getattr(resp.declined, "explanation", ""))
        outcome = "declined" if resp.declined is not None else "llm_error"
        print(
            plan11_stage3_synthesis_marker(
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                ag_id=ag_id,
                cluster_id=cluster.cluster_id,
                outcome=outcome,
                abstain_reason=abstain_reason,
                abstain_explanation=abstain_explanation,
                duration_ms=duration_ms,
                tokens_input=tokens_in,
                tokens_output=tokens_out,
            )
        )
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=(),
            skipped_reason=f"exception:plan11_stage3_{outcome}",
        )

    raw_proposals = resp.parsed_output.get("proposals", []) or []
    proposals: list[RepairProposal] = []
    # Trial 18 step 4 — compute resolved tables once per Stage 3
    # invocation so the per-proposal preflight is O(1). The set is
    # derived from the same ``schema_slice`` the LLM saw, so any drop
    # decision matches the prompt context the LLM had at decode time.
    from genie_space_optimizer.optimization.trial18_flags import (
        trial18_acceptance_overhaul_enabled,
    )

    _trial18_enabled = trial18_acceptance_overhaul_enabled()
    _trial18_resolved = (
        _trial18_resolved_table_ids(schema_slice) if _trial18_enabled
        else frozenset()
    )
    # Trial 13e — track the *raw* ``patch_type`` string for every
    # proposal that fails coercion to PatchType so the
    # ``empty_synthesis`` marker can surface them. Empty / whitespace
    # raws are tracked under a ``"<empty>"`` sentinel so the
    # ``all_candidates_unsafe`` guardrail in the marker always sees a
    # non-empty map (an LLM emitting proposals with missing patch_type
    # is its own vocabulary defect — visible, not silent).
    rejected_patch_types_raw: dict[str, int] = {}
    # Trial 13g — track which source supplied each surviving
    # proposal's ``blame_set`` so the Stage 3 marker exposes whether
    # the upstream evidence chain bottomed out. Closed vocabulary:
    # ``llm`` / ``cluster`` / ``member_union`` / ``empty``.
    proposals_blame_set_source: dict[str, int] = {}
    member_union_blame_set = _union_member_blame_sets(member_qid_evidence)
    # Trial 17 — Lever Selection Contract validator. Imported once
    # outside the loop so each iteration does not pay the lookup cost.
    from genie_space_optimizer.optimization.levers_contract import (
        validate_plan_vs_proposal_consistency,
    )
    # Phase 2 P2.2 — KIT_FOR_RCA mandatory-companion validator.
    from genie_space_optimizer.optimization.stages.action_groups import (
        kit_for_rca_violation_reason,
    )
    # Resolve the cluster's rca_kind once — every proposal under the
    # same cluster inherits it. The FailureCluster's ``root_cause``
    # is the closed-enum RCA label Stage 1 emitted.
    _cluster_rca_kind = str(getattr(cluster, "root_cause", "") or "")

    # Trial 21 W4+C3 — collect per-proposal snippet validator verdicts
    # so the Evidence Actuator can re-check on its end. Keyed by
    # ``intent_id`` (the framework-stamped per-proposal identifier).
    _c3_actuator_verdicts_by_intent_id: dict[str, dict[str, str]] = {}

    # Trial 22 W6 — collect per-proposal implicated-asset and
    # justification evidence keyed by intent_id so the Evidence
    # Actuator's required-assets gate runs on real diagnosis evidence.
    _t22_assets_by_intent_id: dict[str, list[str]] = {}
    _t22_justification_by_intent_id: dict[str, str] = {}

    # Mechanism-binding (e943 Phase 2 #10) — capture each proposal's C5
    # mechanism-coverage verdict keyed by intent_id so the post-loop
    # binding filter can DROP an ``uncovered`` proposal when the binding
    # flag is on (observe-only otherwise).
    _c5_outcome_by_intent_id: dict[str, str] = {}

    # Trial 24 Follow-on B — bundle_ids whose no-op suppression snippet
    # was dropped at the producer for a filter-removal RCA. After the
    # synthesis loop the corrective instruction sibling(s) in these
    # bundles are degraded to SOLO (bundle_id cleared) so they reach the
    # slate as standalone proposals and never hit the Phase-1.5 cohesion
    # cascade that would otherwise drop them alongside the dead snippet.
    _t24_filter_removal_solo_bundle_ids: set[str] = set()

    for idx, item in enumerate(raw_proposals):
        raw_pt = str(item.get("patch_type", ""))
        pt = _safe_patch_type(raw_pt)
        if pt is None:
            # Unknown patch_type — skip (the repair loop will pick it up
            # when validate_patch returns patch_type_unknown). Capture
            # the raw string for the Stage 3 marker.
            key = raw_pt.strip() or "<empty>"
            rejected_patch_types_raw[key] = (
                rejected_patch_types_raw.get(key, 0) + 1
            )
            continue
        # Trial 17 step 2 — deterministic plan-vs-proposal consistency
        # validator. When the LLM declared ``selected_lever``, the
        # (lever, patch_type) pair must satisfy
        # ``LEVER_TO_PATCH_TYPES``. Inconsistent proposals are dropped
        # here with a typed bucket so the Stage 3 marker surfaces the
        # violation, AND a record is logged so the next iteration's
        # forbidden_signature channel sees the lever_plan_violation.
        selected_lever_raw = str(item.get("selected_lever", "") or "")
        # Phase 2 P2.1 — primary lever-kit channel. ``selected_levers``
        # is the new list-of-lever_ids field; we accept the legacy
        # single-string form as a 1-element kit.
        selected_levers_raw = item.get("selected_levers") or []
        if not isinstance(selected_levers_raw, list):
            selected_levers_raw = []
        selected_levers_list: list[str] = [
            str(s) for s in selected_levers_raw if s
        ]
        if not selected_levers_list and selected_lever_raw:
            selected_levers_list = [selected_lever_raw]
        # For the legacy Trial 17 validator we still consult the
        # single-string field — derive it from the kit's first entry
        # when only the new list shape was emitted so downstream
        # readers see a consistent view.
        if not selected_lever_raw and selected_levers_list:
            selected_lever_raw = selected_levers_list[0]
        violation = validate_plan_vs_proposal_consistency(
            selected_lever=selected_lever_raw,
            patch_type=pt,
        )
        if violation is not None:
            key = (
                f"{selected_lever_raw or '?'}::{raw_pt or '<empty>'}"
                f"::{violation}"
            )
            rejected_patch_types_raw[key] = (
                rejected_patch_types_raw.get(key, 0) + 1
            )
            # Emit a CONTRACT_FAILED marker so postmortem tooling can
            # see the violation alongside other Stage 3 dropouts. The
            # idempotent emitter keys on intent_id which we synthesize
            # from cluster_id + idx (same shape as the surviving
            # proposals).
            print(
                json.dumps(
                    {
                        "trial17_lever_plan_violation": violation,
                        "cluster_id": cluster.cluster_id,
                        "intent_id_seq": f"{cluster.cluster_id}_{idx:03d}",
                        "iteration": iteration,
                        "patch_type": pt.value,
                        "selected_lever": selected_lever_raw,
                    },
                    default=str,
                ),
                flush=True,
            )
            continue
        # Phase 2 P2.2 — KIT_FOR_RCA mandatory-companion gate. When
        # the cluster's rca_kind demands a kit (value_mapping_missing,
        # join_semantics_wrong, time_grain_wrong, column_disambiguation,
        # table_routing_wrong), reject any single-lever proposal and
        # any proposal whose kit misses the companion set. Emits a
        # typed forbidden_signature so the NEXT iteration's LLM sees
        # why its singleton was rejected.
        kit_violation = kit_for_rca_violation_reason(
            _cluster_rca_kind, selected_levers_list
        )
        if kit_violation:
            key = (
                f"{_cluster_rca_kind or '?'}::"
                f"kit={','.join(selected_levers_list) or '<empty>'}::"
                f"{kit_violation}"
            )
            rejected_patch_types_raw[key] = (
                rejected_patch_types_raw.get(key, 0) + 1
            )
            print(
                json.dumps(
                    {
                        "phase2_kit_for_rca_violation": kit_violation,
                        "cluster_id": cluster.cluster_id,
                        "intent_id_seq": (
                            f"{cluster.cluster_id}_{idx:03d}"
                        ),
                        "iteration": iteration,
                        "rca_kind": _cluster_rca_kind,
                        "selected_levers": selected_levers_list,
                    },
                    default=str,
                ),
                flush=True,
            )
            continue
        # Trial 18 step 4 — resolved-table preflight for metadata
        # patch types. Drops proposals whose ``patch_body.table`` is
        # not in the schema slice the prompt was built on, so the LLM
        # is told (via ``synthesis_rejected_patch_types`` on the next
        # iteration's marker) that the previous turn picked an
        # unresolvable target. Strictly additive — see
        # ``_trial18_target_table_unresolved`` for skip conditions.
        if _trial18_enabled:
            _patch_body_check = dict(item.get("patch_body") or {})
            # Trial 20 Workstream F2 — attempt canonical-target
            # resolution BEFORE Trial 18's strict identity check.
            # When a unique tail-match exists, rewrite the patch_body
            # so downstream apply uses the resolved identifier and
            # the proposal is kept. Only the exact-miss-after-resolve
            # case falls through to the Trial 18 drop path.
            from genie_space_optimizer.optimization.trial20_flags import (
                trial20_enforce_enabled as _t20_enforce,
            )
            if (
                _t20_enforce()
                and pt.value in _TRIAL18_PREFLIGHT_METADATA_PATCH_TYPES
                and _trial18_resolved
            ):
                _raw_t = (
                    _patch_body_check.get("table")
                    or _patch_body_check.get("target")
                    or ""
                )
                if isinstance(_raw_t, str) and _raw_t.strip():
                    _canonical = _trial20_resolve_canonical_target_table(
                        raw_target=_raw_t,
                        resolved_tables=_trial18_resolved,
                    )
                    if _canonical and _canonical != _raw_t.strip():
                        # Rewrite the live ``item`` patch_body so the
                        # resolved identifier flows into the typed
                        # RepairProposal below.
                        _existing_body = dict(item.get("patch_body") or {})
                        if _existing_body.get("table") == _raw_t:
                            _existing_body["table"] = _canonical
                        elif _existing_body.get("target") == _raw_t:
                            _existing_body["target"] = _canonical
                        item["patch_body"] = _existing_body
                        _patch_body_check = _existing_body
                        try:
                            print(
                                json.dumps(
                                    {
                                        "marker": (
                                            "GSO_TRIAL20_CANONICAL_TARGET_RESOLVED_V1"
                                        ),
                                        "cluster_id": cluster.cluster_id,
                                        "iteration": iteration,
                                        "patch_type": pt.value,
                                        "raw_target": _raw_t,
                                        "canonical_target": _canonical,
                                    },
                                    default=str,
                                    sort_keys=True,
                                ),
                                flush=True,
                            )
                        except Exception:
                            pass
            if _trial18_target_table_unresolved(
                patch_type_value=pt.value,
                patch_body=_patch_body_check,
                resolved_tables=_trial18_resolved,
            ):
                key = f"{pt.value}::target_table_unresolved"
                rejected_patch_types_raw[key] = (
                    rejected_patch_types_raw.get(key, 0) + 1
                )
                print(
                    json.dumps(
                        {
                            "trial18_synthesis_target_unresolved": True,
                            "cluster_id": cluster.cluster_id,
                            "intent_id_seq": f"{cluster.cluster_id}_{idx:03d}",
                            "iteration": iteration,
                            "patch_type": pt.value,
                            "selected_lever": selected_lever_raw,
                            "unresolved_table": str(
                                _patch_body_check.get("table")
                                or _patch_body_check.get("target")
                                or ""
                            ),
                        },
                        default=str,
                    ),
                    flush=True,
                )
                continue
        item_blame_set = [str(b) for b in (item.get("blame_set") or [])]
        # Plan 12 — derive target_objects from blame_set so the Plan 11
        # synthesize output passes the survival contract until the
        # ``Plan11SynthesizeOutput`` schema is extended to emit
        # target_objects directly. Trial 13g — 3-step fallback chain:
        # LLM blame_set -> cluster.primary_blame_set -> union of
        # member_qid_evidence blame_sets. The latter is the Stage 1
        # per-QID seed which is always populated by the diagnose
        # contract; reaching it means the Stage 3 LLM AND the Stage 2
        # clustering both dropped their blame seed. Each branch
        # records the source for the Stage 3 marker so postmortems
        # can pinpoint where the chain bottomed out.
        if item_blame_set:
            effective_blame_set = item_blame_set
            source = "llm"
        elif cluster.primary_blame_set:
            effective_blame_set = [str(b) for b in cluster.primary_blame_set]
            source = "cluster"
        elif member_union_blame_set:
            effective_blame_set = list(member_union_blame_set)
            source = "member_union"
        else:
            effective_blame_set = []
            source = "empty"
        proposals_blame_set_source[source] = (
            proposals_blame_set_source.get(source, 0) + 1
        )

        # P4 producer-side validation. Runs C4 (metadata target
        # resolution), C3 (snippet validation), and C5 (mechanism
        # coverage) on the LLM-emitted patch_body BEFORE the proposal
        # is appended to the cluster's bundle. Unresolvable / invalid
        # bodies are dropped here so the downstream applier-side gates
        # do not waste cycles on them and the strategist sees the
        # mechanism-repeat signal on the NEXT iteration. C5 is
        # observe-first — it emits the marker but does not drop.
        _patch_body_p4 = dict(item.get("patch_body") or {})
        _patch_type_wire_p4 = str(pt.value or "").lower()
        _intent_id_p4 = f"{cluster.cluster_id}_{idx:03d}"
        _qid_p4_primary = ""
        _target_qids_p4 = tuple(
            str(q) for q in (item.get("target_qids") or [])
        )
        if _target_qids_p4:
            _qid_p4_primary = _target_qids_p4[0]
        elif cluster.member_qids:
            _qid_p4_primary = str(cluster.member_qids[0])

        # ── C4: metadata target resolution ────────────────────────
        _c4_dropped = False
        try:
            from genie_space_optimizer.optimization.metadata_target_resolver import (
                METADATA_PATCH_TYPES_WITH_TARGETS as _C4_PATCH_TYPES,
                validate_and_stamp_metadata_patch_target as _c4_validate,
            )
            if _patch_type_wire_p4 in _C4_PATCH_TYPES:
                _c4_snapshot = dict(metadata_snapshot or {})
                _c4_verdict = _c4_validate(
                    _patch_body_p4,
                    patch_type_wire=_patch_type_wire_p4,
                    metadata_snapshot=_c4_snapshot,
                    space_id=str(space_id or ""),
                )
                try:
                    import json as _c4_json
                    print(
                        "GSO_TARGET_RESOLVER_V1 "
                        + _c4_json.dumps(
                            {
                                "optimization_run_id": str(
                                    optimization_run_id
                                ),
                                "iteration": int(iteration),
                                "qid": _qid_p4_primary,
                                "intent_id": _intent_id_p4,
                                "patch_type": _patch_type_wire_p4,
                                "outcome": _c4_verdict.outcome,
                                "abstain_reason": (
                                    _c4_verdict.abstain_reason.value
                                    if _c4_verdict.abstain_reason is not None
                                    else ""
                                ),
                                "resolved_table": _c4_verdict.resolved_table,
                                "resolved_column": (
                                    _c4_verdict.resolved_column
                                ),
                                "error_message": (
                                    _c4_verdict.error_message[:200]
                                ),
                            },
                            sort_keys=True,
                            default=str,
                        ),
                        flush=True,
                    )
                except Exception:
                    pass
                if _c4_verdict.outcome == "unresolvable":
                    # Drop the proposal. The strategist re-asks Stage 3
                    # next iteration with the typed feedback string.
                    _c4_dropped = True
        except Exception:
            pass
        if _c4_dropped:
            continue

        # ── C3: snippet validator ─────────────────────────────────
        _c3_dropped = False
        try:
            from genie_space_optimizer.optimization.producer_snippet_validator import (
                validate_and_stamp_snippet_patch_body as _c3_validate,
            )
            # Only snippet patch_types go through the validator. The
            # wrapper short-circuits non-snippet types to "stamped"
            # (no-op), so calling unconditionally is safe.
            _c3_verdict = _c3_validate(
                _patch_body_p4,
                intent_id=_intent_id_p4,
                patch_type_wire=_patch_type_wire_p4,
                metadata_snapshot=dict(metadata_snapshot or {}),
                spark=spark,
                catalog=str(catalog or ""),
                gold_schema=str(gold_schema or ""),
                w=w,
                warehouse_id=str(warehouse_id or ""),
            )
            # Only emit the marker for actual snippet patches so the
            # log is not flooded with no-op events on every metadata
            # / structural proposal.
            if _patch_type_wire_p4.startswith("add_sql_snippet_") or (
                _patch_type_wire_p4 in {
                    "add_example_sql",
                    "update_example_sql",
                }
            ):
                try:
                    import json as _c3_json
                    print(
                        "GSO_SNIPPET_VALIDATOR_V1 "
                        + _c3_json.dumps(
                            {
                                "optimization_run_id": str(
                                    optimization_run_id
                                ),
                                "iteration": int(iteration),
                                "qid": _qid_p4_primary,
                                "intent_id": _intent_id_p4,
                                "patch_type": _patch_type_wire_p4,
                                "outcome": _c3_verdict.outcome,
                                "abstain_reason": (
                                    _c3_verdict.abstain_reason.value
                                    if _c3_verdict.abstain_reason is not None
                                    else ""
                                ),
                                "error_message": (
                                    _c3_verdict.error_message[:200]
                                ),
                            },
                            sort_keys=True,
                            default=str,
                        ),
                        flush=True,
                    )
                except Exception:
                    pass
            # Trial 21 W4+C3 — enforce: declined snippets are dropped
            # at the producer (before the Actuator sees them). The
            # validator runs first to fail-fast; the Actuator carries a
            # defensive duplicate of the same check via
            # ``snippet_validator_verdict_by_proposal_id`` so that any
            # snippet validator outcome that slips past this loop
            # still gets dropped before the applier sees it.
            if str(_c3_verdict.outcome or "").lower() == "declined":
                _c3_dropped = True
                # Trial 23 W7 — snippet repair loop (repair, not drop).
                # Re-prompt ONCE with the EXACT canonical validator error
                # + resolved schema, re-validate, and drop only if the
                # repair also fails. Gated by
                # ``trial23_snippet_repair_enabled`` for one-flag rollback
                # to the immediate drop. On success the repaired body
                # replaces the invalid one and the verdict is upgraded to
                # ``stamped`` so the downstream Actuator stash is honest.
                try:
                    from genie_space_optimizer.optimization.trial23_flags import (  # noqa: E501
                        trial23_snippet_repair_enabled,
                    )
                    if trial23_snippet_repair_enabled():
                        from genie_space_optimizer.optimization.snippet_repair import (  # noqa: E501
                            apply_repaired_sql as _w7_apply,
                            build_snippet_repair_payload as _w7_payload,
                            extract_repaired_sql as _w7_extract,
                            snippet_repair_marker as _w7_marker,
                        )
                        _w7_rsm = _SKILL_LOADER.load_reasoning_metadata(
                            _SKILL_ID
                        )
                        _w7_out_cls = _SKILL_LOADER.load_output_schema_class(
                            _SKILL_ID
                        )
                        _w7_sys = _SKILL_LOADER.load_prompt(
                            _SKILL_ID, expected_constant_name=_PROMPT_CONST
                        )
                        _w7_req = LlmReasoningRequest(
                            call_id=(
                                f"plan11_snippet_repair.{cluster.cluster_id}"
                                f".{_intent_id_p4}.iter_{int(iteration)}"
                            ),
                            skill_id=_SKILL_ID,
                            system_msg=_w7_sys,
                            user_prompt=json.dumps(
                                _w7_payload(
                                    patch_type_wire=_patch_type_wire_p4,
                                    patch_body=_patch_body_p4,
                                    validator_error=(
                                        _c3_verdict.error_message
                                    ),
                                    schema_slice=schema_slice,
                                ),
                                default=str,
                            ),
                            result_cls=_w7_out_cls,
                            max_tokens=_w7_rsm.max_tokens,
                        )
                        _w7_resp = LlmReasoningCall().invoke(
                            w=w, request=_w7_req
                        )
                        _w7_sql = (
                            _w7_extract(_w7_resp.parsed_output)
                            if getattr(_w7_resp, "succeeded", False)
                            else ""
                        )
                        if _w7_sql:
                            _w7_body = _w7_apply(
                                _patch_body_p4,
                                _w7_sql,
                                patch_type_wire=_patch_type_wire_p4,
                            )
                            _w7_reverdict = _c3_validate(
                                _w7_body,
                                intent_id=_intent_id_p4,
                                patch_type_wire=_patch_type_wire_p4,
                                metadata_snapshot=dict(
                                    metadata_snapshot or {}
                                ),
                                spark=spark,
                                catalog=str(catalog or ""),
                                gold_schema=str(gold_schema or ""),
                                w=w,
                                warehouse_id=str(warehouse_id or ""),
                            )
                            if (
                                str(_w7_reverdict.outcome or "").lower()
                                != "declined"
                            ):
                                # Repair succeeded — adopt the repaired,
                                # now-stamped body and upgrade the verdict.
                                _patch_body_p4.clear()
                                _patch_body_p4.update(_w7_body)
                                _c3_verdict = _w7_reverdict
                                _c3_dropped = False
                                print(
                                    _w7_marker(
                                        optimization_run_id=(
                                            optimization_run_id
                                        ),
                                        iteration=iteration,
                                        cluster_id=cluster.cluster_id,
                                        intent_id=_intent_id_p4,
                                        patch_type=_patch_type_wire_p4,
                                        outcome="repaired",
                                    ),
                                    flush=True,
                                )
                            else:
                                print(
                                    _w7_marker(
                                        optimization_run_id=(
                                            optimization_run_id
                                        ),
                                        iteration=iteration,
                                        cluster_id=cluster.cluster_id,
                                        intent_id=_intent_id_p4,
                                        patch_type=_patch_type_wire_p4,
                                        outcome="repair_failed",
                                        validator_error=(
                                            _w7_reverdict.error_message
                                        ),
                                    ),
                                    flush=True,
                                )
                        else:
                            print(
                                _w7_marker(
                                    optimization_run_id=optimization_run_id,
                                    iteration=iteration,
                                    cluster_id=cluster.cluster_id,
                                    intent_id=_intent_id_p4,
                                    patch_type=_patch_type_wire_p4,
                                    outcome="repair_no_sql",
                                    validator_error=_c3_verdict.error_message,
                                ),
                                flush=True,
                            )
                except Exception:
                    pass
            # Stash the verdict so the Trial 21 Actuator wire-in below
            # can re-check the same proposal against the same verdict
            # (defense in depth for paths that bypass the producer
            # drop, e.g. batched-override).
            _c3_actuator_verdicts_by_intent_id[_intent_id_p4] = {
                "outcome": str(_c3_verdict.outcome or ""),
                "abstain_reason": (
                    _c3_verdict.abstain_reason.value
                    if _c3_verdict.abstain_reason is not None
                    else ""
                ),
                "error_message": _c3_verdict.error_message[:200],
            }
        except Exception:
            pass
        if _c3_dropped:
            # Trial 24 Follow-on B — no-op suppression snippet degrade.
            # When the snippet was declined as a TAUTOLOGY for a
            # filter-removal RCA, record its bundle_id so the corrective
            # instruction sibling is emitted SOLO (post-loop) instead of
            # cascading. Best-effort; never blocks the producer drop.
            _t24_drop_reason = "snippet_invalid"
            try:
                from genie_space_optimizer.optimization.trial24_flags import (  # noqa: E501
                    trial24_filter_removal_solo_enabled as _t24_solo_on,
                )
                from genie_space_optimizer.optimization.llm_abstain import (
                    AbstainReason as _t24_abstain,
                )
                from genie_space_optimizer.optimization.stages.action_groups import (  # noqa: E501
                    _TRIAL24_KIT_FOR_RCA as _t24_rca_set,
                    _normalize_rca_kind as _t24_norm_rca,
                )
                _is_noop = (
                    _c3_verdict.abstain_reason
                    is _t24_abstain.SNIPPET_NOOP_SUPPRESSION
                )
                if (
                    _is_noop
                    and _t24_solo_on()
                    and _t24_norm_rca(_cluster_rca_kind) in _t24_rca_set
                ):
                    _t24_drop_reason = "snippet_noop_suppression"
                    _t24_bid = str(item.get("bundle_id", "") or "")
                    if _t24_bid:
                        _t24_filter_removal_solo_bundle_ids.add(_t24_bid)
                    try:
                        import json as _t24_noop_json
                        print(
                            "GSO_TRIAL24_NOOP_SNIPPET_DEGRADE_V1 "
                            + _t24_noop_json.dumps(
                                {
                                    "optimization_run_id": str(
                                        optimization_run_id
                                    ),
                                    "iteration": int(iteration),
                                    "cluster_id": str(cluster.cluster_id),
                                    "intent_id": _intent_id_p4,
                                    "rca_kind": _t24_norm_rca(
                                        _cluster_rca_kind
                                    ),
                                    "bundle_id": _t24_bid,
                                    "patch_type": _patch_type_wire_p4,
                                },
                                sort_keys=True,
                                default=str,
                            ),
                            flush=True,
                        )
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                import json as _c3_drop_json
                print(
                    "GSO_TRIAL21_PRODUCER_DROP_V1 "
                    + _c3_drop_json.dumps(
                        {
                            "optimization_run_id": str(optimization_run_id),
                            "iteration": int(iteration),
                            "cluster_id": str(cluster.cluster_id),
                            "intent_id": _intent_id_p4,
                            "patch_type": _patch_type_wire_p4,
                            "drop_reason": _t24_drop_reason,
                            "gate": "producer_snippet_validator",
                        },
                        sort_keys=True,
                        default=str,
                    ),
                    flush=True,
                )
            except Exception:
                pass
            continue

        # ── C5: mechanism coverage (observe-first) ────────────────
        try:
            from genie_space_optimizer.optimization.mechanism_coverage import (
                check_mechanism_coverage as _c5_check,
            )
            from genie_space_optimizer.optimization.patch_mechanism import (
                mechanism_for_patch_type as _c5_mech_for,
            )
            _c5_mech = _c5_mech_for(_patch_type_wire_p4)
            if _c5_mech is not None:
                # behavior_delta is best-effort threaded from the
                # proposal's expected_behavioral_change. C1 will
                # surface the typed RepairDiagnosis once wired; until
                # then the proposal-level expected_behavioral_change
                # is the closest available signal.
                _c5_behavior_delta = str(
                    item.get("expected_behavioral_change", "") or ""
                )
                _c5_override = str(
                    item.get(
                        "mechanism_coverage_override_justification",
                        "",
                    )
                    or ""
                )
                _c5_verdict = _c5_check(
                    behavior_delta=_c5_behavior_delta,
                    proposed_mechanisms=(_c5_mech,),
                    mechanism_coverage_override_justification=_c5_override,
                )
                # Mechanism-binding: stash the verdict so the post-loop
                # filter can drop ``uncovered`` proposals when binding.
                _c5_outcome_by_intent_id[_intent_id_p4] = _c5_verdict.outcome
                try:
                    import json as _c5_json
                    print(
                        "GSO_MECHANISM_COVERAGE_V1 "
                        + _c5_json.dumps(
                            {
                                "optimization_run_id": str(
                                    optimization_run_id
                                ),
                                "iteration": int(iteration),
                                "qid": _qid_p4_primary,
                                "intent_id": _intent_id_p4,
                                "patch_type": _patch_type_wire_p4,
                                "outcome": _c5_verdict.outcome,
                                "inferred_category": (
                                    _c5_verdict.inferred_category.value
                                ),
                                "proposed_mechanisms": [
                                    m.value
                                    for m in _c5_verdict.proposed_mechanisms
                                ],
                                "adequate_mechanisms": [
                                    m.value
                                    for m in _c5_verdict.adequate_mechanisms
                                ],
                                "override_present": bool(
                                    _c5_verdict.override_justification
                                ),
                                "observe_only": True,
                            },
                            sort_keys=True,
                            default=str,
                        ),
                        flush=True,
                    )
                except Exception:
                    pass
        except Exception:
            pass

        proposals.append(
            RepairProposal(
                intent_id=_intent_id_p4,
                intent_name=str(item.get("intent_name", ""))[:80],
                intent_description=str(item.get("intent_description", "")),
                repair_shape=RepairShape.OTHER,  # legacy field; new code reads repair_hypothesis
                patch_type=pt,
                rationale=str(item.get("rationale", "")),
                confidence=item.get("confidence", "low"),  # type: ignore[arg-type]
                patch_body=_patch_body_p4,
                blame_set=tuple(effective_blame_set),
                target_objects=_target_objects_from_blame_set(
                    effective_blame_set,
                ),
                repair_hypothesis=str(item.get("repair_hypothesis", "")),
                target_qids=tuple(
                    str(q) for q in (item.get("target_qids") or [])
                ),
                # Trial 17 — Lever Selection Contract fields. The
                # validator above already enforced consistency between
                # ``selected_lever`` and ``patch_type``; here we just
                # stamp them onto the typed proposal so downstream
                # gates (acceptance_gate, applier_gate) can read them.
                selected_lever=selected_lever_raw,
                expected_behavioral_change=str(
                    item.get("expected_behavioral_change", "") or ""
                ),
                fallback_lever=str(item.get("fallback_lever", "") or ""),
                bundle_id=str(item.get("bundle_id", "") or ""),
                # Trial 20 D1 — thread the LLM's free-text single-
                # lever justification onto the typed proposal so the
                # downstream D4 marker emission carries it through
                # to ``GSO_TRIAL20_SINGLE_LEVER_JUSTIFIED_V1`` for
                # postmortem audit.
                single_lever_justification=str(
                    item.get("single_lever_justification", "") or ""
                ),
            )
        )

        # Trial 22 W6 — accumulate the per-proposal asset / justification
        # evidence keyed by intent_id so the Evidence Actuator's
        # ``_check_required_assets`` runs on REAL diagnosis evidence
        # instead of short-circuiting. The implicated assets are the
        # proposal's effective blame_set (the columns/tables Stage 1's
        # RCA named); the justification is the LLM's free-text
        # single-lever rebuttal. Both default to empty when absent, and
        # ``_asset_gate_enabled()`` is the emergency rollback switch.
        _t22_assets_by_intent_id[_intent_id_p4] = [
            str(a) for a in (effective_blame_set or ())
        ]
        _t24_justification = str(
            item.get("single_lever_justification", "") or ""
        )
        # Follow-on B (FB2) — ground a solo corrective instruction's
        # justification. ``extra_defensive_filter`` is reclassified as a
        # single-mechanism instruction fix (FB1), so the corrective
        # ``add_instruction`` ships SOLO with no kit waiver. The live e943
        # run carried an EMPTY ``single_lever_justification`` and died at
        # ``_check_required_assets`` as ``unjustified_single_lever``. Fall
        # back to the proposal's ``expected_behavioral_change`` then
        # ``rationale`` so the required-assets gate sees grounded
        # evidence.
        #
        # Two scopes, both INSTRUCTION_TEXT-only and both never
        # fabricating (empty sources still drop):
        #   * NARROW (FB2): cluster ``rca_kind`` in the Trial 24 forced-
        #     kit map. Byte-stable behaviour shipped with Follow-on B.
        #   * GENERAL (replay-readiness): any ``rca_kind`` when
        #     ``trial24_general_instruction_grounding_enabled()`` — so a
        #     grounded solo corrective instruction lands across a broader
        #     multi-RCA replay, not only the two forced-kit RCAs.
        if not _t24_justification:
            try:
                from genie_space_optimizer.optimization.trial24_flags import (
                    trial24_filter_removal_solo_enabled,
                    trial24_general_instruction_grounding_enabled,
                )
                from genie_space_optimizer.optimization.patch_mechanism import (  # noqa: E501
                    PatchMechanism,
                    mechanism_for_patch_type,
                )
                from genie_space_optimizer.optimization.stages.action_groups import (  # noqa: E501
                    _TRIAL24_KIT_FOR_RCA as _t24_rca_set,
                    _normalize_rca_kind as _t24_norm_rca,
                )

                _t24_is_instruction = (
                    mechanism_for_patch_type(str(pt.value))
                    is PatchMechanism.INSTRUCTION_TEXT
                )
                _t24_narrow_scope = (
                    trial24_filter_removal_solo_enabled()
                    and _t24_norm_rca(_cluster_rca_kind) in _t24_rca_set
                )
                _t24_general_scope = (
                    trial24_general_instruction_grounding_enabled()
                )
                if _t24_is_instruction and (
                    _t24_narrow_scope or _t24_general_scope
                ):
                    _t24_justification = str(
                        item.get("expected_behavioral_change", "") or ""
                    ) or str(item.get("rationale", "") or "")
            except Exception:
                pass
        _t22_justification_by_intent_id[_intent_id_p4] = _t24_justification

    # Trial 24 Follow-on B — degrade-to-solo. For any bundle whose no-op
    # suppression snippet was dropped above, clear the bundle_id on the
    # surviving instruction-family member(s) so they reach the slate as
    # standalone proposals (never the Phase-1.5 cohesion cascade).
    if _t24_filter_removal_solo_bundle_ids and proposals:
        try:
            from dataclasses import replace as _t24_replace
            from genie_space_optimizer.optimization.patch_mechanism import (
                PatchMechanism as _t24_mech_enum,
                mechanism_for_patch_type as _t24_mech_for,
            )
            _t24_new_proposals: list[RepairProposal] = []
            for _t24_p in proposals:
                _t24_pb = str(getattr(_t24_p, "bundle_id", "") or "")
                if (
                    _t24_pb in _t24_filter_removal_solo_bundle_ids
                    and _t24_mech_for(str(_t24_p.patch_type.value))
                    is _t24_mech_enum.INSTRUCTION_TEXT
                ):
                    _t24_new_proposals.append(
                        _t24_replace(_t24_p, bundle_id="")
                    )
                else:
                    _t24_new_proposals.append(_t24_p)
            proposals = _t24_new_proposals
        except Exception:
            pass

    # Trial 24 — GSO_TRIAL24_KIT_FORCED_V1 marker. When the Trial 24 flag
    # is on AND the cluster's rca_kind carries a Trial 24 kit contract,
    # emit one audit marker recording the RCA, its companion set, and the
    # union of lever-ids the synthesizer actually emitted. Postmortems
    # use this to confirm the corrective kit was BORN at source (not
    # repaired downstream). Best-effort; never blocks synthesis.
    try:
        from genie_space_optimizer.optimization.trial24_flags import (
            trial24_kit_at_source_enabled as _t24_marker_on,
        )
        if _t24_marker_on() and proposals:
            from genie_space_optimizer.optimization.stages.action_groups import (  # noqa: E501
                _kit_for_rca_companions as _t24_companions_for,
                _normalize_rca_kind as _t24_norm,
            )
            _t24_key = _t24_norm(_cluster_rca_kind)
            # Follow-on B — read the FLAG-AWARE companion lookup so the
            # KIT_FORCED marker does NOT fire for extra_defensive_filter
            # once it is reclassified as an instruction solo
            # (filter-removal-solo on); it still fires for RCAs that remain
            # forced kits (e.g. top_n_cardinality_collapse).
            _t24_companions = _t24_companions_for(_t24_key)
            if _t24_companions is not None:
                _t24_emitted_levers: set[str] = set()
                for _t24_p in proposals:
                    for _lev in _t24_p.effective_selected_levers():
                        if _lev:
                            _t24_emitted_levers.add(str(_lev))
                print(
                    "GSO_TRIAL24_KIT_FORCED_V1 "
                    + json.dumps(
                        {
                            "optimization_run_id": optimization_run_id,
                            "iteration": iteration,
                            "cluster_id": cluster.cluster_id,
                            "rca_kind": _t24_key,
                            "companion_set": sorted(_t24_companions),
                            "emitted_levers": sorted(_t24_emitted_levers),
                            "kit_satisfied": (
                                len(_t24_emitted_levers) >= 2
                                and bool(
                                    _t24_emitted_levers & set(_t24_companions)
                                )
                            ),
                        },
                        sort_keys=True,
                        default=str,
                    ),
                    flush=True,
                )
    except Exception:
        pass

    # Trial 13 Track 5 — typed ``synthesis_empty_reason`` + populated
    # ``target_qids_union`` on ``empty_synthesis``. The dc89d1a9 run
    # emitted 6 markers with no reason and no target QIDs, leaving the
    # postmortem with no signal. We compute the union from the input
    # cluster's member QIDs (the bail-out attributes back to the
    # QIDs whose evidence we tried to repair) regardless of whether
    # any proposal survived parse.
    if proposals:
        outcome_label = "synthesized"
        target_qids_union = sorted(
            {q for p in proposals for q in p.target_qids}
        )
        synthesis_empty_reason = ""
    else:
        outcome_label = "empty_synthesis"
        target_qids_union = sorted(
            {str(q) for q in (cluster.member_qids or ())}
        )
        synthesis_empty_reason = _classify_synthesis_empty_reason(
            raw_proposals=raw_proposals,
            parsed_output=resp.parsed_output,
        )
    print(
        plan11_stage3_synthesis_marker(
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            ag_id=ag_id,
            cluster_id=cluster.cluster_id,
            outcome=outcome_label,
            proposals_count=len(proposals),
            proposal_ids=[p.intent_id for p in proposals],
            patch_types=[p.patch_type.value for p in proposals],
            target_qids_union=target_qids_union,
            duration_ms=duration_ms,
            tokens_input=tokens_in,
            tokens_output=tokens_out,
            synthesis_empty_reason=synthesis_empty_reason,
            synthesis_rejected_patch_types=(
                dict(rejected_patch_types_raw)
                if outcome_label == "empty_synthesis"
                else None
            ),
            proposals_blame_set_source=(
                dict(proposals_blame_set_source)
                if proposals_blame_set_source
                else None
            ),
            # Trial 17.1 — index-parallel to proposal_ids / patch_types.
            # Empty strings for proposals where the LLM omitted the
            # field (legacy or older models) so the arrays stay aligned.
            selected_levers=[
                str(getattr(p, "selected_lever", "") or "") for p in proposals
            ],
            expected_behavioral_changes=[
                str(getattr(p, "expected_behavioral_change", "") or "")
                for p in proposals
            ],
            fallback_levers=[
                str(getattr(p, "fallback_lever", "") or "") for p in proposals
            ],
            bundle_ids=[
                str(getattr(p, "bundle_id", "") or "") for p in proposals
            ],
        )
    )

    # Plan 12 — survival-contract validation at Stage 3 exit. Proposals
    # that fail the contract get a CONTRACT_FAILED GSO_PATCH_OUTCOME_V1
    # marker (idempotent per intent_id) and are dropped from the
    # returned list. The Stage 3 marker above carries the pre-filter
    # proposal_ids so I22's coverage check sees a matching outcome for
    # every declared intent_id (CONTRACT_FAILED for the dropped ones,
    # APPLIED / VALIDATOR_REJECTED / BLAST_RADIUS_REJECTED for the
    # survivors once downstream stages run).
    from genie_space_optimizer.optimization.patch_outcome import (
        PatchOutcomeKind,
    )
    from genie_space_optimizer.optimization.patch_survival_emitter import (
        emit_patch_outcome,
    )
    from genie_space_optimizer.optimization.repair_proposal_typed import (
        validate_survival_contract,
    )

    surviving: list[RepairProposal] = []
    for p in proposals:
        result = validate_survival_contract(p)
        if result.is_valid:
            surviving.append(p)
            continue
        reason = (
            "missing_required_fields_" + "_".join(result.missing_fields)
        )
        emit_patch_outcome(
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            ag_id=ag_id,
            cluster_id=cluster.cluster_id,
            intent_id=p.intent_id or "<empty>",
            outcome_kind=PatchOutcomeKind.CONTRACT_FAILED,
            terminal_reason=reason,
        )
    proposals = surviving

    # Mechanism-coverage binding (e943 Phase 2 #10) — promote the C5
    # check from observe-only to binding behind a flag (default ON since
    # promotion; disable with GSO_MECHANISM_COVERAGE_BINDING=0). An
    # ``uncovered`` proposal (its mechanism cannot address the
    # behavior_delta, no override supplied) is DROPPED from the slate so
    # the optimizer stops shipping behaviorally-inert patches. The pure
    # selector never empties the slate (slate-safety: all-uncovered →
    # keep all and fall back to observe-only).
    try:
        from genie_space_optimizer.optimization.mechanism_binding import (
            coverage_survivor_indices,
        )
        from genie_space_optimizer.optimization.mechanism_binding_flags import (
            mechanism_coverage_binding_enabled,
        )

        if mechanism_coverage_binding_enabled() and proposals:
            _cov_outcomes = [
                _c5_outcome_by_intent_id.get(p.intent_id or "")
                for p in proposals
            ]
            _cov_survivors, _cov_dropped = coverage_survivor_indices(
                _cov_outcomes,
                binding_enabled=True,
            )
            if _cov_dropped:
                for _drop_i in _cov_dropped:
                    _dropped_p = proposals[_drop_i]
                    emit_patch_outcome(
                        optimization_run_id=optimization_run_id,
                        iteration=iteration,
                        ag_id=ag_id,
                        cluster_id=cluster.cluster_id,
                        intent_id=_dropped_p.intent_id or "<empty>",
                        outcome_kind=PatchOutcomeKind.CONTRACT_FAILED,
                        terminal_reason=(
                            "mechanism_does_not_cover_behavior_delta"
                        ),
                    )
                proposals = [proposals[i] for i in _cov_survivors]
    except Exception:
        pass

    # Trial 23 W4 — RCA-to-mechanism routing (correct at source). When
    # the cluster's RCA kind is one that ``add_example_sql`` cannot fix
    # (extra_defensive_filter, top_n_cardinality_collapse,
    # canonical_dimension_missed) and the surviving slate defaulted to
    # example_sql with no fixing mechanism paired in, emit the
    # ``rca_mechanism_defaulted_to_example_sql`` anti-success marker.
    # The synthesis prompt carries the matching routing guidance, so a
    # firing marker means the LLM ignored it; the marker feeds the next
    # iteration's forbidden-signature channel. This is observe-and-route
    # (not a drop) on purpose — the repair/redirect paths land in W7-W9,
    # and dropping the sole surviving proposal here would re-create the
    # all-dropped flatline (see plan "central design tension").
    try:
        from genie_space_optimizer.optimization.trial23_flags import (
            trial23_rca_mechanism_routing_enabled,
        )
        if (
            trial23_rca_mechanism_routing_enabled()
            and proposals
            and _cluster_rca_kind
        ):
            from genie_space_optimizer.optimization.patch_mechanism import (
                mechanism_for_patch_type,
            )
            from genie_space_optimizer.optimization.rca_mechanism_routing import (
                rca_mechanism_default_reason,
                rca_mechanism_defaulted_marker,
            )
            _w4_mechs = set()
            for _w4_p in proposals:
                _w4_m = mechanism_for_patch_type(
                    str(getattr(_w4_p.patch_type, "value", _w4_p.patch_type))
                )
                if _w4_m is not None:
                    _w4_mechs.add(_w4_m)
            _w4_reason = rca_mechanism_default_reason(
                _cluster_rca_kind, _w4_mechs
            )
            if _w4_reason:
                print(
                    rca_mechanism_defaulted_marker(
                        optimization_run_id=optimization_run_id,
                        iteration=iteration,
                        cluster_id=cluster.cluster_id,
                        rca_kind=_cluster_rca_kind,
                        mechanisms=_w4_mechs,
                    ),
                    flush=True,
                )
                # RCA-route binding (e943 Phase 2 #10) — promote the
                # observe-and-route marker to binding behind a default-
                # OFF flag. A proposal that defaulted to example_sql for
                # an example-SQL-insufficient RCA (no fixing mechanism
                # paired in) is BLOCKED from the slate. The pure selector
                # never empties the slate, so the documented "central
                # design tension" (dropping the sole survivor flatlines)
                # is preserved — all-defaulted → keep all, observe-only.
                from genie_space_optimizer.optimization.mechanism_binding import (
                    rca_route_survivor_indices,
                )
                from genie_space_optimizer.optimization.mechanism_binding_flags import (
                    rca_mechanism_route_binding_enabled,
                )

                if rca_mechanism_route_binding_enabled():
                    _w4_defaulted = []
                    for _w4_p in proposals:
                        _w4_pm = mechanism_for_patch_type(
                            str(
                                getattr(
                                    _w4_p.patch_type,
                                    "value",
                                    _w4_p.patch_type,
                                )
                            )
                        )
                        _w4_pm_set = {_w4_pm} if _w4_pm is not None else set()
                        _w4_defaulted.append(
                            bool(
                                rca_mechanism_default_reason(
                                    _cluster_rca_kind, _w4_pm_set
                                )
                            )
                        )
                    _w4_survivors, _w4_dropped = rca_route_survivor_indices(
                        _w4_defaulted,
                        binding_enabled=True,
                    )
                    if _w4_dropped:
                        for _w4_drop_i in _w4_dropped:
                            _w4_drop_p = proposals[_w4_drop_i]
                            emit_patch_outcome(
                                optimization_run_id=optimization_run_id,
                                iteration=iteration,
                                ag_id=ag_id,
                                cluster_id=cluster.cluster_id,
                                intent_id=_w4_drop_p.intent_id or "<empty>",
                                outcome_kind=PatchOutcomeKind.CONTRACT_FAILED,
                                terminal_reason=_w4_reason,
                            )
                        proposals = [proposals[i] for i in _w4_survivors]
    except Exception:
        pass

    # Track B / B1 — lone-instruction route binding. The example_sql
    # block above catches the exemplar false-fix; this catches the prose
    # false-fix. For a SQL-shape RCA where INSTRUCTION_TEXT is NOT a
    # fixing mechanism (top_n_cardinality_collapse,
    # canonical_dimension_missed) a lone ``add_instruction`` cannot change
    # the generated SQL shape, so it is behaviorally inert — the d139 /
    # e943 phantom accept. Emit the
    # ``..._DEFAULTED_TO_INSTRUCTION_TEXT_V1`` anti-success marker and,
    # when the binding flag is ON (default), DROP the inert lone-
    # instruction proposals provided a non-defaulted proposal survives
    # (slate-safety preserved by ``rca_route_survivor_indices``).
    # ``extra_defensive_filter`` is intentionally exempt: the routing
    # brain deems an instruction a valid fix there.
    try:
        from genie_space_optimizer.optimization.trial23_flags import (
            trial23_rca_mechanism_routing_enabled,
        )
        if (
            trial23_rca_mechanism_routing_enabled()
            and proposals
            and _cluster_rca_kind
        ):
            from genie_space_optimizer.optimization.patch_mechanism import (
                mechanism_for_patch_type,
            )
            from genie_space_optimizer.optimization.rca_mechanism_routing import (  # noqa: E501
                rca_instruction_default_reason,
                rca_instruction_defaulted_marker,
            )
            _instr_mechs = set()
            for _instr_p in proposals:
                _instr_m = mechanism_for_patch_type(
                    str(
                        getattr(
                            _instr_p.patch_type, "value", _instr_p.patch_type
                        )
                    )
                )
                if _instr_m is not None:
                    _instr_mechs.add(_instr_m)
            _instr_reason = rca_instruction_default_reason(
                _cluster_rca_kind, _instr_mechs
            )
            if _instr_reason:
                print(
                    rca_instruction_defaulted_marker(
                        optimization_run_id=optimization_run_id,
                        iteration=iteration,
                        cluster_id=cluster.cluster_id,
                        rca_kind=_cluster_rca_kind,
                        mechanisms=_instr_mechs,
                    ),
                    flush=True,
                )
                from genie_space_optimizer.optimization.mechanism_binding import (  # noqa: E501
                    rca_route_survivor_indices,
                )
                from genie_space_optimizer.optimization.mechanism_binding_flags import (  # noqa: E501
                    instruction_route_binding_enabled,
                )

                if instruction_route_binding_enabled():
                    _instr_defaulted = []
                    for _instr_p in proposals:
                        _instr_pm = mechanism_for_patch_type(
                            str(
                                getattr(
                                    _instr_p.patch_type,
                                    "value",
                                    _instr_p.patch_type,
                                )
                            )
                        )
                        _instr_pm_set = (
                            {_instr_pm} if _instr_pm is not None else set()
                        )
                        _instr_defaulted.append(
                            bool(
                                rca_instruction_default_reason(
                                    _cluster_rca_kind, _instr_pm_set
                                )
                            )
                        )
                    _instr_survivors, _instr_dropped = (
                        rca_route_survivor_indices(
                            _instr_defaulted,
                            binding_enabled=True,
                        )
                    )
                    if _instr_dropped:
                        for _instr_drop_i in _instr_dropped:
                            _instr_drop_p = proposals[_instr_drop_i]
                            emit_patch_outcome(
                                optimization_run_id=optimization_run_id,
                                iteration=iteration,
                                ag_id=ag_id,
                                cluster_id=cluster.cluster_id,
                                intent_id=(
                                    _instr_drop_p.intent_id or "<empty>"
                                ),
                                outcome_kind=PatchOutcomeKind.CONTRACT_FAILED,
                                terminal_reason=_instr_reason,
                            )
                        proposals = [
                            proposals[i] for i in _instr_survivors
                        ]
    except Exception:
        pass

    # Trial 20 Workstream D3 + D4 — bundle-default validator and
    # marker emission. When ``trial20_multi_lever_bundle_default_enabled``
    # is ON:
    #
    # * D3 (hard rule): refuse sole-lever proposals that reuse the
    #   same lever family as any ``rejected_insufficient_repeat``
    #   signature for this cluster.
    # * D4 (markers): emit ``GSO_TRIAL20_BUNDLE_EMITTED_V1`` on every
    #   multi-lever bundle and ``GSO_TRIAL20_SINGLE_LEVER_JUSTIFIED_V1``
    #   on single-lever proposals carrying the LLM's free-text
    #   ``single_lever_justification``.
    from genie_space_optimizer.optimization.trial20_flags import (
        trial20_multi_lever_bundle_default_enabled,
    )
    _t20_bundle_default = trial20_multi_lever_bundle_default_enabled()
    _t20_has_insufficient = bool(insufficient_repair_signatures)
    if _t20_bundle_default and proposals:
        rejected_lever_families: set[str] = set()
        for _sig in (insufficient_repair_signatures or ()):
            _sig_s = str(_sig or "")
            if not _sig_s:
                continue
            first = _sig_s.split(":", 1)[0].strip()
            if first and first != "?":
                rejected_lever_families.add(first)

        bundle_groups: dict[str, list] = {}
        for _p in proposals:
            _bid = str(getattr(_p, "bundle_id", "") or "").strip()
            if _bid:
                bundle_groups.setdefault(_bid, []).append(_p)

        # D3: when prior insufficient signatures exist, require a
        # multi-lever bundle with proposals from DIFFERENT lever
        # families. Drop sole-lever proposals from rejected families.
        if _t20_has_insufficient and rejected_lever_families:
            _d3_before = len(proposals)
            kept: list = []
            for _p in proposals:
                _lever = str(getattr(_p, "selected_lever", "") or "")
                _bid = str(getattr(_p, "bundle_id", "") or "").strip()
                _shares_bundle = bool(
                    _bid and len(bundle_groups.get(_bid, [])) >= 2
                )
                if (
                    _lever
                    and _lever in rejected_lever_families
                    and not _shares_bundle
                ):
                    try:
                        import json as _json_t20d3
                        print(
                            "GSO_TRIAL20_STRATEGIST_GATE_REJECTED_V1 "
                            + _json_t20d3.dumps(
                                {
                                    "iteration": iteration,
                                    "cluster_id": cluster.cluster_id,
                                    "intent_id": _p.intent_id or "<empty>",
                                    "selected_lever": _lever,
                                    "patch_type": str(
                                        getattr(_p, "patch_type", "")
                                    ),
                                    "rejected_lever_families": sorted(
                                        rejected_lever_families
                                    ),
                                    "reason": "sole_lever_in_rejected_family",
                                },
                                sort_keys=True,
                                default=str,
                            ),
                            flush=True,
                        )
                    except Exception:
                        pass
                    continue
                kept.append(_p)
            proposals = kept

            # Trial 23 W8 — pivot with a destination. When the D3 drop
            # emptied the slate (the only proposal was the rejected sole
            # lever), issue ONE replacement re-prompt that demands a
            # multi-lever bundle pairing the rejected family with a
            # DIFFERENT companion lever, and route the result back
            # through the full normalization + gate pipeline via
            # ``llm_response_override``. ``_pivot_attempt`` guards
            # against infinite recursion; the flag gates one-flag
            # rollback to the immediate empty-slate behaviour.
            try:
                from genie_space_optimizer.optimization.trial23_flags import (  # noqa: E501
                    trial23_pivot_destination_enabled,
                )
                if (
                    trial23_pivot_destination_enabled()
                    and not _pivot_attempt
                ):
                    from genie_space_optimizer.optimization.pivot_destination import (  # noqa: E501
                        build_pivot_directive as _w8_directive,
                        pivot_destination_marker as _w8_marker,
                        slate_emptied_by_sole_lever as _w8_emptied,
                    )
                    if _w8_emptied(
                        proposals_before=_d3_before,
                        proposals_after=len(proposals),
                        rejected_families=rejected_lever_families,
                    ):
                        _w8_root = str(
                            getattr(cluster, "root_cause", "") or ""
                        )
                        print(
                            _w8_marker(
                                optimization_run_id=optimization_run_id,
                                iteration=iteration,
                                cluster_id=cluster.cluster_id,
                                rejected_families=rejected_lever_families,
                                outcome="pivot_attempted",
                            ),
                            flush=True,
                        )
                        _w8_req = _build_request(
                            cluster=cluster,
                            schema_slice=schema_slice,
                            member_qid_evidence=member_qid_evidence or [],
                            history=history,
                            iteration=iteration,
                            forbidden_signatures=forbidden_signatures,
                            insufficient_repair_signatures=(
                                insufficient_repair_signatures
                            ),
                            prior_iteration_drops=prior_iteration_drops,
                            pivot_directive=_w8_directive(
                                rejected_families=rejected_lever_families,
                                cluster_id=cluster.cluster_id,
                                root_cause=_w8_root,
                            ),
                        )
                        _w8_resp = LlmReasoningCall().invoke(
                            w=w, request=_w8_req
                        )
                        _w8_result = run_plan11_synthesis_for_single_cluster(
                            cluster,
                            schema_slice,
                            history,
                            member_qid_evidence=member_qid_evidence,
                            optimization_run_id=optimization_run_id,
                            iteration=iteration,
                            ag_id=ag_id,
                            w=w,
                            forbidden_signatures=forbidden_signatures,
                            insufficient_repair_signatures=(
                                insufficient_repair_signatures
                            ),
                            llm_response_override=_w8_resp,
                            metadata_snapshot=metadata_snapshot,
                            space_id=space_id,
                            prior_mechanism_attempts=prior_mechanism_attempts,
                            spark=spark,
                            catalog=catalog,
                            gold_schema=gold_schema,
                            warehouse_id=warehouse_id,
                            prior_iteration_drops=prior_iteration_drops,
                            _pivot_attempt=True,
                        )
                        _w8_landed = _w8_result.proposal is not None
                        print(
                            _w8_marker(
                                optimization_run_id=optimization_run_id,
                                iteration=iteration,
                                cluster_id=cluster.cluster_id,
                                rejected_families=rejected_lever_families,
                                outcome=(
                                    "pivot_landed"
                                    if _w8_landed
                                    else "pivot_emptied_slate"
                                ),
                                replacement_proposals=(
                                    1 if _w8_landed else 0
                                ),
                            ),
                            flush=True,
                        )
                        return _w8_result
            except Exception:
                pass

        # D4: emit markers describing the bundle / single-lever shape
        # of what survived. P4 C2 — fold the mechanism-repeat guard
        # alongside the bundle-emission marker so postmortems can
        # cross-reference the two. Observe-first: we do not block on
        # ``"blocked"`` yet (the bundle still emits), but the marker
        # flags repeat-after-unproductive mechanism choices for the
        # current iteration's bundle.
        try:
            import json as _json_t20d4
            try:
                from genie_space_optimizer.optimization.patch_mechanism import (
                    MechanismAttempt as _C2_Attempt,
                    behavior_delta_hash as _c2_bdh,
                    check_mechanism_repeat_guard as _c2_check,
                    mechanism_for_patch_type as _c2_mech_for,
                    mechanism_repeat_guard_marker as _c2_marker,
                )
            except Exception:
                _C2_Attempt = None
                _c2_check = None
                _c2_marker = None
                _c2_mech_for = None
                _c2_bdh = None
            _prior_attempts_c2 = tuple(prior_mechanism_attempts or ())

            for _bid, _members in bundle_groups.items():
                if len(_members) < 2:
                    continue
                lever_keys = sorted(
                    {
                        str(getattr(m, "selected_lever", "") or "")
                        for m in _members
                    } - {""}
                )
                patch_types = sorted(
                    {str(getattr(m, "patch_type", "")) for m in _members}
                )
                print(
                    "GSO_TRIAL20_BUNDLE_EMITTED_V1 "
                    + _json_t20d4.dumps(
                        {
                            "iteration": iteration,
                            "cluster_id": cluster.cluster_id,
                            "bundle_id": _bid,
                            "lever_keys": lever_keys,
                            "patch_types": patch_types,
                            "size": len(_members),
                        },
                        sort_keys=True,
                        default=str,
                    ),
                    flush=True,
                )

                # P4 C2: mechanism repeat guard per bundle. The
                # ``proposed_mechanisms`` set is the union of the
                # mechanism each bundle member's patch_type maps to.
                if _c2_check is None or _c2_mech_for is None:
                    continue
                try:
                    _bundle_mechs = tuple(
                        m for m in (
                            _c2_mech_for(
                                str(
                                    getattr(
                                        mb.patch_type,
                                        "value",
                                        mb.patch_type,
                                    )
                                    or ""
                                ).lower()
                            )
                            for mb in _members
                        )
                        if m is not None
                    )
                    if not _bundle_mechs:
                        continue
                    _bundle_qid = str(
                        (
                            tuple(getattr(_members[0], "target_qids", ()) or ())
                            or (cluster.member_qids or ("",))
                        )[0]
                    )
                    _bundle_behavior = str(
                        getattr(_members[0], "expected_behavioral_change", "")
                        or ""
                    )
                    _bundle_just = str(
                        getattr(_members[0], "single_lever_justification", "")
                        or ""
                    )
                    _c2_verdict = _c2_check(
                        qid=_bundle_qid,
                        behavior_delta=_bundle_behavior,
                        proposed_mechanisms=_bundle_mechs,
                        mechanism_change_justification=_bundle_just,
                        prior_attempts=_prior_attempts_c2,
                    )
                    if _c2_marker is not None:
                        print(
                            _c2_marker(
                                optimization_run_id=optimization_run_id,
                                iteration=iteration,
                                qid=_bundle_qid,
                                behavior_delta=_bundle_behavior,
                                proposed_mechanisms=_bundle_mechs,
                                verdict=_c2_verdict,
                                mechanism_change_justification=_bundle_just,
                            ),
                            flush=True,
                        )
                except Exception:
                    pass
            for _p in proposals:
                _bid = str(getattr(_p, "bundle_id", "") or "").strip()
                _is_solo = (not _bid) or len(bundle_groups.get(_bid, [])) < 2
                if not _is_solo:
                    continue
                _justification = str(
                    getattr(_p, "single_lever_justification", "") or ""
                )
                print(
                    "GSO_TRIAL20_SINGLE_LEVER_JUSTIFIED_V1 "
                    + _json_t20d4.dumps(
                        {
                            "iteration": iteration,
                            "cluster_id": cluster.cluster_id,
                            "intent_id": _p.intent_id or "<empty>",
                            "selected_lever": str(
                                getattr(_p, "selected_lever", "") or ""
                            ),
                            "patch_type": str(getattr(_p, "patch_type", "")),
                            "single_lever_justification": _justification,
                            "justification_present": bool(
                                _justification.strip()
                            ),
                        },
                        sort_keys=True,
                        default=str,
                    ),
                    flush=True,
                )
        except Exception:
            pass

    # Trial 21 W2 — Evidence Actuator. ``compile_slate`` runs the
    # seven-check pipeline (prompt budget, metadata target, snippet
    # validator, required assets, mechanism coverage, mechanism repeat,
    # bundle invariants) and drops proposals with a typed DropReason
    # instead of the old observe-only P4 marker path. When the flag is
    # OFF, every proposal passes through unchanged (the compiler
    # context is empty so the seven checks no-op — emergency-rollback
    # behavior). As W3-W9 land they plumb their respective fields into
    # ``runtime_ctx`` so the corresponding check graduates from no-op
    # to active drop.
    from genie_space_optimizer.optimization.trial21_flags import (
        trial21_actuator_enabled,
    )
    if trial21_actuator_enabled() and proposals:
        from genie_space_optimizer.optimization.proposal_slate_compiler import (
            SlateCompilerContext,
            compile_slate,
        )
        _t21_cluster_id = str(getattr(cluster, "cluster_id", "") or "")
        _t21_prompt_size_by_cluster: dict[str, dict[str, int | bool]] = {}
        if _stage3_prompt_size_verdict_local:
            _t21_prompt_size_by_cluster[_t21_cluster_id] = dict(
                _stage3_prompt_size_verdict_local
            )
        # Trial 21 W5+C2 — project prior_mechanism_attempts into the
        # plain-dict shape SlateCompilerContext expects. The harness
        # threads :class:`MechanismAttempt` instances; the Actuator
        # only needs (qid, patch_type, selected_lever, rca_kind,
        # behavior_delta) so we project here without changing the
        # downstream P4 C2 marker pipeline.
        _t21_prior_attempts: list[dict[str, str]] = []
        for _attempt in (prior_mechanism_attempts or ()):
            try:
                _t21_prior_attempts.append(
                    {
                        "qid": str(getattr(_attempt, "qid", "") or ""),
                        "patch_type": str(
                            getattr(_attempt, "patch_type", "") or ""
                        ).lower(),
                        "selected_lever": str(
                            getattr(_attempt, "selected_lever", "") or ""
                        ),
                        "rca_kind": str(
                            getattr(_attempt, "rca_kind", "") or ""
                        ),
                        "behavioral_diff": str(
                            getattr(_attempt, "behavioral_diff", "") or ""
                        ),
                    }
                )
            except Exception:
                continue
        _t21_ctx = SlateCompilerContext(
            optimization_run_id=str(optimization_run_id or ""),
            iteration=int(iteration),
            cluster_id=_t21_cluster_id,
            prompt_size_verdict_by_cluster=_t21_prompt_size_by_cluster,
            snippet_validator_verdict_by_proposal_id=dict(
                _c3_actuator_verdicts_by_intent_id
            ),
            prior_mechanism_attempts=tuple(_t21_prior_attempts),
            # Trial 22 W6 — wire the per-proposal asset / justification
            # evidence so ``_check_required_assets`` stops short-
            # circuiting. ``_asset_gate_enabled()`` (default ON) is the
            # surgical rollback switch.
            implicated_assets_by_proposal_id=dict(
                _t22_assets_by_intent_id
            ),
            justification_by_proposal_id=dict(
                _t22_justification_by_intent_id
            ),
        )
        _t21_result = compile_slate(proposals, _t21_ctx)
        # Emit each Actuator marker. The markers feed the postmortem
        # replay regression suite (Trial 21 W1) and the per-iteration
        # auditing pipeline.
        try:
            import json as _json_t21
            for _marker in _t21_result.actuator_markers:
                _payload = {
                    k: v for k, v in _marker.items() if k != "marker"
                }
                print(
                    f"{_marker.get('marker', 'GSO_SLATE_COMPILER_DECISION_V1')} "
                    + _json_t21.dumps(
                        _payload, sort_keys=True, default=str
                    ),
                    flush=True,
                )
        except Exception:
            pass
        # Trial 22 W3 — build the durable drop summary BEFORE we
        # discard the compiler result. The harness copies this onto
        # the iteration terminal-state ledger so iteration N+1's
        # Stage 3 prompt can warn the LLM about the exact drops.
        try:
            from genie_space_optimizer.optimization.proposal_slate_compiler import (
                build_compiler_drop_summary,
                build_retry_feedback_marker,
            )
            _t22_drop_summary = build_compiler_drop_summary(_t21_result)
            if _t22_drop_summary.get("drop_reason_counts"):
                import json as _json_t22
                _t22_marker = build_retry_feedback_marker(
                    iteration=int(iteration),
                    summary=_t22_drop_summary,
                )
                print(
                    "GSO_TRIAL22_RETRY_FEEDBACK_V1 "
                    + _json_t22.dumps(
                        {
                            k: v
                            for k, v in _t22_marker.items()
                            if k != "marker"
                        },
                        sort_keys=True,
                        default=str,
                    ),
                    flush=True,
                )
        except Exception:
            _t22_drop_summary = None
        proposals = list(_t21_result.surviving_proposals)

    if not proposals:
        return ClusterSynthesisResult(
            proposal=None,
            attempted_archetypes=(),
            skipped_reason="synth_none",
            compiler_drop_summary=locals().get("_t22_drop_summary"),
        )

    # ClusterSynthesisResult.proposal is the legacy dict shape; surface
    # the first proposal as a dict so PR 2 wiring slots into the same
    # downstream pipeline as the archetype path.
    return ClusterSynthesisResult(
        proposal=proposals[0].to_json(),
        attempted_archetypes=(),
        skipped_reason="",
        compiler_drop_summary=locals().get("_t22_drop_summary"),
    )


# ─── Phase 1 P1.1 — Stage 3 batched synthesis across all clusters ────────


# Switch criteria for batched mode. The pacer (P0.2) tracks a 120k
# input-token-per-minute ceiling; a batched call still has to fit the
# per-prompt 40k MAX_PROMPT_INPUT_TOKENS gate (P0.4). We pick a 35k
# total-input budget for the batched user_prompt so that the cacheable
# system + lever-menu blocks (which add ~3–4k effective tokens on a
# cold call) still keep the total under MAX_PROMPT_INPUT_TOKENS once
# they are appended back at LLM-call time.
BATCH_STAGE3_MIN_CLUSTERS: int = 3
BATCH_STAGE3_MAX_INPUT_TOKENS: int = 35_000


def estimate_batched_stage3_input_tokens(
    cluster_inputs: list[dict[str, Any]],
    *,
    history: list[dict[str, Any]],
    forbidden_signatures: tuple[str, ...] = (),
    insufficient_repair_signatures: tuple[str, ...] = (),
) -> int:
    """Estimate the input token count of the batched user_prompt body.

    Returns a conservative upper-bound number of tokens at 4 chars per
    token (matches the heuristic in ``llm_token_budget.py``). The
    estimate covers only the dynamic per-cluster payload and the
    shared history/forbidden/insufficient slots — the static skill
    body and cacheable lever / archetype menus are not counted because
    they are sent as separate ``cacheable_user_blocks`` and charged
    at 0.1x cost after the cache warms.
    """
    dynamic = {
        "history": list(history or []),
        "forbidden_signatures": list(forbidden_signatures or ()),
        "insufficient_repair_signatures": list(
            insufficient_repair_signatures or ()
        ),
        "clusters": [
            {
                "cluster_id": str(ci.get("cluster_id", "")),
                "ag_id": str(ci.get("ag_id", "")),
                "cluster": ci.get("cluster_json") or {},
                "member_qid_evidence": ci.get("member_qid_evidence") or [],
                "schema_slice": ci.get("schema_slice") or {},
            }
            for ci in cluster_inputs
        ],
    }
    payload_chars = len(json.dumps(dynamic, default=str))
    return (payload_chars + 3) // 4


def should_batch_stage3_synthesis(
    cluster_inputs: list[dict[str, Any]],
    *,
    history: list[dict[str, Any]],
    forbidden_signatures: tuple[str, ...] = (),
    insufficient_repair_signatures: tuple[str, ...] = (),
) -> bool:
    """Pure predicate that decides whether the batched Stage 3 path
    should run.

    True iff at least :data:`BATCH_STAGE3_MIN_CLUSTERS` clusters are
    pending Stage 3 synthesis AND the estimated dynamic input token
    count fits within :data:`BATCH_STAGE3_MAX_INPUT_TOKENS`.

    The caller (``_invoke_stage3_llm``) flips back to the per-cluster
    path when this returns False so single-cluster iterations and
    over-large prompts still run safely. Splitting the predicate out
    keeps the decision deterministic and unit-testable in isolation.
    """
    if len(cluster_inputs) < BATCH_STAGE3_MIN_CLUSTERS:
        return False
    estimate = estimate_batched_stage3_input_tokens(
        cluster_inputs,
        history=history,
        forbidden_signatures=forbidden_signatures,
        insufficient_repair_signatures=insufficient_repair_signatures,
    )
    return estimate <= BATCH_STAGE3_MAX_INPUT_TOKENS


def _build_batched_request(
    cluster_inputs: list[dict[str, Any]],
    *,
    history: list[dict[str, Any]],
    iteration: int,
    forbidden_signatures: tuple[str, ...] = (),
    insufficient_repair_signatures: tuple[str, ...] = (),
) -> LlmReasoningRequest:
    """Build a single ``LlmReasoningRequest`` that asks the LLM to emit
    proposals for ALL ``cluster_inputs`` in one call.

    ``cluster_inputs`` shape (each entry):
      * ``cluster_id``: str — must be unique within the batch
      * ``ag_id``: str — surfaced to the LLM but not split-keyed
      * ``cluster_json``: dict — output of ``FailureCluster.to_json``
      * ``schema_slice``: dict — schema slice for this cluster
      * ``member_qid_evidence``: list[dict] — per-QID Stage 1 evidence

    Each proposal in the response MUST set ``cluster_id`` to one of
    the input cluster ids so :func:`run_plan11_synthesis_for_all_clusters`
    can split the result back into per-cluster envelopes. We instruct
    the LLM about this explicitly at the top of ``user_prompt`` and
    re-state the contract right above the cluster list.

    The static skill system body, lever menu, lever contract
    instructions, and archetype catalog menu are reused unchanged from
    ``_build_request`` so the Anthropic prompt cache stays warm
    between batched and single-cluster calls.
    """
    rsm = _SKILL_LOADER.load_reasoning_metadata(_SKILL_ID)
    if rsm is None:
        raise RuntimeError(
            f"{_SKILL_ID!r} is not a reasoning skill — check SKILL.md "
            "frontmatter"
        )
    output_cls = _SKILL_LOADER.load_output_schema_class(_SKILL_ID)
    system_body = _SKILL_LOADER.load_prompt(
        _SKILL_ID, expected_constant_name=_PROMPT_CONST,
    )

    from genie_space_optimizer.optimization.levers_contract import (
        archetype_catalog_menu_for_prompt,
        lever_menu_for_prompt,
    )

    # Reuse the same lever-contract instructions block from the
    # per-cluster builder. The block is byte-identical so the prompt
    # cache can serve it across both paths.
    _t20_has_insufficient = bool(insufficient_repair_signatures)
    from genie_space_optimizer.optimization.trial20_flags import (
        trial20_multi_lever_bundle_default_enabled,
    )

    _t20_bundle_default = trial20_multi_lever_bundle_default_enabled()
    if _t20_bundle_default:
        if _t20_has_insufficient:
            _t20_bundle_clause = (
                "  - bundle_id: REQUIRED on this iteration. The clusters "
                "carry non-empty insufficient_repair_signatures, so a "
                "single-lever repair has already proven insufficient. "
                "Emit a multi-lever bundle of >=2 proposals per cluster "
                "sharing the same bundle_id and DIFFERENT lever "
                "families. Single-lever proposals will be rejected at "
                "the validator.\n"
            )
        else:
            _t20_bundle_clause = (
                "  - bundle_id: STRONGLY PREFERRED. Multi-lever bundles "
                "are the default strategy when the diagnosis suggests "
                "a single lever cannot address the failure mode. Emit "
                ">=2 proposals (per cluster) sharing the same "
                "bundle_id and DIFFERENT lever families, and set "
                "``selected_levers`` on EACH proposal to the same "
                "kit list.\n"
            )
    else:
        _t20_bundle_clause = (
            "  - bundle_id (optional): non-empty string when multiple "
            "proposals must be applied together.\n"
        )

    lever_contract_instructions = (
        "Trial 17 — Lever Selection Contract. For EACH proposal you "
        "emit, you MUST also set:\n"
        "  - selected_levers: a CLOSED list of lever_ids drawn from "
        "    'lever-1' .. 'lever-6' (see lever_menu). EMIT 2+ "
        "    ENTRIES for grammar-pivot, join-semantics, time-grain, "
        "    value-mapping, and column-disambiguation diagnoses "
        "    (KIT_FOR_RCA — P2.2). EMIT 1 ENTRY only for genuinely "
        "    single-lever diagnoses such as plain add_instruction "
        "    prose for soft policy.\n"
        "  - selected_lever (DEPRECATED): the deterministic validator "
        "    falls back to this single-string field only when "
        "    ``selected_levers`` is empty; prefer setting "
        "    ``selected_levers`` directly.\n"
        "  - expected_behavioral_change: one or two sentences.\n"
        "  - fallback_lever: which lever to try next iteration.\n"
        + _t20_bundle_clause
    )

    # Trial 24 — Kit at Source (batched builder). Same mandatory-kit
    # clause as the single-cluster builder so the batched synthesis path
    # also emits the >= 2-lever-family kit for the example-SQL-
    # insufficient RCAs on the first try.
    try:
        from genie_space_optimizer.optimization.trial24_flags import (
            trial24_filter_removal_solo_enabled as _t24_batched_solo_on,
            trial24_kit_at_source_enabled as _t24_batched_on,
        )
        _t24_batched_enabled = _t24_batched_on()
        _t24_batched_filter_solo = _t24_batched_solo_on()
    except Exception:
        _t24_batched_enabled = False
        _t24_batched_filter_solo = False
    if _t24_batched_enabled:
        if _t24_batched_filter_solo:
            _t24_batched_filter_clause = (
                "For extra_defensive_filter (a filter-REMOVAL RCA) emit a "
                "SINGLE add_instruction (lever-5a) telling the planner not "
                "to inject the predicate — never a positive ``1=1`` / "
                "``TRUE`` snippet (a no-op that is rejected); ground it "
                "with a concrete ``expected_behavioral_change``. "
            )
        else:
            _t24_batched_filter_clause = (
                "For extra_defensive_filter emit {lever-5a + lever-6}. "
            )
        lever_contract_instructions += (
            "\n\n"
            "Trial 24 — Kit at Source (MANDATORY KIT). "
            + _t24_batched_filter_clause
            + "For top_n_cardinality_collapse emit {lever-6 + lever-1}. A "
            "single-lever proposal for the KIT RCAs is hard-rejected "
            "(kit_for_rca_violation:...:singleton). Set "
            "``selected_levers`` to the kit on every member and share a "
            "``bundle_id``; the instruction/metadata member is justified "
            "by its structural companion lever (no separate "
            "justification slot needed)."
        )

    # Phase 0 P0.4 — LRU compaction on the growth-prone shared slots
    # before we estimate sizes.
    from genie_space_optimizer.optimization.llm_prompt_compaction import (
        compact_history_slots_to_fit,
    )
    from genie_space_optimizer.optimization.llm_reasoning_call import (
        MAX_PROMPT_INPUT_TOKENS,
    )
    # Phase 1 P1.3 — fixed-window cap (last 3 iterations) before the
    # LRU compactor sees the slots. Same structural bound as the
    # single-cluster build_request; the batched path consumes the
    # SAME shared signature/history slots so it must apply the same
    # cap or the slots will leak through unbounded.
    from genie_space_optimizer.optimization.llm_history_window import (
        cap_iteration_bucketed_history,
        cap_signature_list,
    )

    history_list = cap_iteration_bucketed_history(
        history or [], current_iteration=iteration,
    )
    forbidden_list = cap_signature_list(forbidden_signatures or ())
    insufficient_list = cap_signature_list(
        insufficient_repair_signatures or ()
    )
    static_chars = (
        len(system_body)
        + len(json.dumps(
            {
                "iteration": iteration,
                "clusters": [
                    {
                        "cluster_id": ci.get("cluster_id", ""),
                        "ag_id": ci.get("ag_id", ""),
                        "cluster": ci.get("cluster_json") or {},
                        "member_qid_evidence": ci.get(
                            "member_qid_evidence"
                        ) or [],
                        "schema_slice": ci.get("schema_slice") or {},
                    }
                    for ci in cluster_inputs
                ],
                "history": [],
                "forbidden_signatures": [],
                "insufficient_repair_signatures": [],
                "lever_menu": lever_menu_for_prompt(),
                "lever_contract_instructions": lever_contract_instructions,
                "archetype_catalog_menu": archetype_catalog_menu_for_prompt(),
            },
            default=str,
        ))
    )
    compact_history_slots_to_fit(
        static_chars=static_chars,
        history_slots=[
            ("history", history_list),
            ("forbidden_signatures", forbidden_list),
            ("insufficient_repair_signatures", insufficient_list),
        ],
        target_token_cap=MAX_PROMPT_INPUT_TOKENS,
    )

    lever_menu_json = json.dumps(
        {"lever_menu": lever_menu_for_prompt()}, default=str,
    )
    lever_contract_block = (
        "lever_contract_instructions:\n"
        + lever_contract_instructions
    )
    archetype_menu_json = json.dumps(
        {"archetype_catalog_menu": archetype_catalog_menu_for_prompt()},
        default=str,
    )

    # Explicit batched directive prefix. We keep this as a small
    # plaintext block so the LLM sees the contract BEFORE it reads the
    # JSON body. The cluster_id field tag is the critical batched
    # contract — without it the splitter cannot route proposals.
    batch_directive = (
        "Phase 1 P1.1 — Batched Stage 3 synthesis. You are receiving "
        f"{len(cluster_inputs)} failure clusters in ONE call. For "
        "EACH proposal you emit, set the `cluster_id` field to the "
        "id of the cluster the proposal targets (one of: "
        + ", ".join(repr(str(ci.get("cluster_id", ""))) for ci in cluster_inputs)
        + "). Proposals whose `cluster_id` is empty or does not match "
        "an input cluster will be dropped by the splitter. Emit zero "
        "or more proposals per cluster; group multi-lever bundles "
        "under the same `bundle_id` PER CLUSTER (bundle_id is scoped "
        "to a cluster).\n"
    )

    user_prompt = batch_directive + json.dumps(
        {
            "iteration": iteration,
            "history": history_list,
            "forbidden_signatures": list(forbidden_list),
            "insufficient_repair_signatures": list(insufficient_list),
            "clusters": [
                {
                    "cluster_id": str(ci.get("cluster_id", "")),
                    "ag_id": str(ci.get("ag_id", "")),
                    "cluster": ci.get("cluster_json") or {},
                    "member_qid_evidence": ci.get(
                        "member_qid_evidence"
                    ) or [],
                    "schema_slice": ci.get("schema_slice") or {},
                }
                for ci in cluster_inputs
            ],
        },
        default=str,
    )
    return LlmReasoningRequest(
        call_id=(
            "plan11_stage3_synthesize.BATCHED"
            f".n{len(cluster_inputs)}.iter_{int(iteration)}"
        ),
        skill_id=_SKILL_ID,
        system_msg=system_body,
        user_prompt=user_prompt,
        result_cls=output_cls,
        max_tokens=rsm.max_tokens * max(1, len(cluster_inputs)),
        cacheable_user_blocks=(
            lever_menu_json,
            lever_contract_block,
            archetype_menu_json,
        ),
    )


def _split_batched_proposals_by_cluster(
    parsed_output: dict[str, Any] | None,
    cluster_ids: list[str],
    cluster_member_qids: dict[str, set[str]],
) -> dict[str, list[dict[str, Any]]]:
    """Partition the LLM's batched ``proposals`` list into a
    ``cluster_id`` → ``[proposals]`` map.

    Routing precedence:
      1. Explicit ``proposal["cluster_id"]`` if it matches an input
         cluster id.
      2. Fallback: ``proposal["target_qids"]`` overlap with a unique
         cluster's ``member_qids``. If exactly one cluster's member
         set contains any of the target qids, route there.

    Proposals that cannot be routed are silently dropped — the
    splitter does NOT raise so a malformed LLM response degrades to
    "no proposals for this cluster" rather than blowing up the whole
    iteration.
    """
    by_cluster: dict[str, list[dict[str, Any]]] = {
        cid: [] for cid in cluster_ids
    }
    if not parsed_output:
        return by_cluster
    raw = parsed_output.get("proposals") or []
    valid_ids = set(cluster_ids)
    for item in raw:
        if not isinstance(item, dict):
            continue
        tagged = str(item.get("cluster_id", "") or "").strip()
        if tagged and tagged in valid_ids:
            by_cluster[tagged].append(item)
            continue
        # Fallback: route by target_qids overlap.
        target_qids = {
            str(q) for q in (item.get("target_qids") or [])
        }
        if not target_qids:
            continue
        matched: list[str] = []
        for cid in cluster_ids:
            members = cluster_member_qids.get(cid) or set()
            if target_qids & members:
                matched.append(cid)
        if len(matched) == 1:
            by_cluster[matched[0]].append(item)
    return by_cluster


def run_plan11_synthesis_for_all_clusters(
    cluster_inputs: list[dict[str, Any]],
    history: list[dict[str, Any]],
    *,
    optimization_run_id: str,
    iteration: int,
    w: Any,
    forbidden_signatures: tuple[str, ...] = (),
    insufficient_repair_signatures: tuple[str, ...] = (),
    # P4 producer-side hooks — forwarded to the per-cluster path so
    # C3/C4/C5/C8 run on every proposal in the batched fan-out.
    metadata_snapshot: dict[str, Any] | None = None,
    space_id: str = "",
    spark: Any = None,
    catalog: str = "",
    gold_schema: str = "",
    warehouse_id: str = "",
) -> dict[str, ClusterSynthesisResult]:
    """Phase 1 P1.1 — batched Stage 3 synthesis across all clusters.

    Runs ONE ``LlmReasoningCall`` carrying the full list of failure
    clusters, then splits the response by ``cluster_id`` and dispatches
    each cluster's slice through
    :func:`run_plan11_synthesis_for_single_cluster` via its
    ``llm_response_override`` seam. This reuses all of the per-cluster
    post-processing (survival contract, blame-set fallback, lever
    plan validator, Trial 20 D3/D4 markers) without duplicating it.

    Token economics:
      * One static system + cacheable_user_blocks payload (lever menu,
        lever contract, archetype menu) — paid once per call instead
        of N times.
      * One dynamic JSON body that lists all clusters — paid once.
      * Output: one decode pass instead of N (proportional savings on
        output tokens when the LLM batches its reasoning).

    Returns ``{cluster_id: ClusterSynthesisResult}``. Cluster ids
    missing from the LLM's response receive a
    ``ClusterSynthesisResult(proposal=None, attempted_archetypes=(), skipped_reason="synth_none")``
    so callers always see one envelope per requested cluster.

    Caller contract — ``cluster_inputs`` is a list of dicts with:
      * ``cluster``: ``FailureCluster`` instance (typed)
      * ``cluster_json``: ``cluster.to_json()`` precomputed (or
        recomputed here for safety)
      * ``schema_slice``: dict
      * ``member_qid_evidence``: list[dict]
      * ``ag_id``: str
    """
    if not cluster_inputs:
        return {}

    # Normalize: pull typed clusters and precompute cluster_json /
    # member-qid sets.
    typed_clusters: dict[str, FailureCluster] = {}
    schema_slices: dict[str, dict[str, Any]] = {}
    member_evidence_by_cid: dict[str, list[dict[str, Any]]] = {}
    ag_id_by_cid: dict[str, str] = {}
    cluster_ids: list[str] = []
    cluster_member_qids: dict[str, set[str]] = {}
    normalized_inputs: list[dict[str, Any]] = []
    for ci in cluster_inputs:
        cluster = ci.get("cluster")
        if cluster is None:
            continue
        cid = str(getattr(cluster, "cluster_id", "") or "")
        if not cid or cid in typed_clusters:
            continue
        typed_clusters[cid] = cluster
        schema_slices[cid] = dict(ci.get("schema_slice") or {})
        member_evidence_by_cid[cid] = list(ci.get("member_qid_evidence") or [])
        ag_id_by_cid[cid] = str(ci.get("ag_id", "") or "")
        cluster_ids.append(cid)
        cluster_member_qids[cid] = {
            str(q) for q in (getattr(cluster, "member_qids", ()) or ())
        }
        normalized_inputs.append(
            {
                "cluster_id": cid,
                "ag_id": ag_id_by_cid[cid],
                "cluster_json": cluster.to_json(),
                "schema_slice": schema_slices[cid],
                "member_qid_evidence": member_evidence_by_cid[cid],
            }
        )

    if not normalized_inputs:
        return {}

    # Build + dispatch the single batched LLM call.
    request = _build_batched_request(
        normalized_inputs,
        history=history,
        iteration=iteration,
        forbidden_signatures=forbidden_signatures,
        insufficient_repair_signatures=insufficient_repair_signatures,
    )

    if insufficient_repair_signatures:
        _pairs_preview = tuple(insufficient_repair_signatures[:20])
        print(
            insufficient_signatures_in_context_marker(
                optimization_run_id=optimization_run_id,
                iteration=iteration,
                stage="plan11_synthesize_batched",
                count=len(insufficient_repair_signatures),
                qid_rca_pairs=_pairs_preview,
            )
        )

    t0 = time.monotonic()
    resp = LlmReasoningCall().invoke(w=w, request=request)
    duration_ms = int((time.monotonic() - t0) * 1000)

    # If the batched call failed (declined / error), each cluster gets
    # the same declined-style envelope so the caller can decide whether
    # to fall back to per-cluster calls. We mirror the per-cluster
    # "exception:plan11_stage3_<outcome>" skipped_reason so postmortem
    # tooling sees the same shape.
    if not resp.succeeded or resp.parsed_output is None:
        outcome = "declined" if resp.declined is not None else "llm_error"
        out: dict[str, ClusterSynthesisResult] = {}
        for cid in cluster_ids:
            print(
                plan11_stage3_synthesis_marker(
                    optimization_run_id=optimization_run_id,
                    iteration=iteration,
                    ag_id=ag_id_by_cid.get(cid, ""),
                    cluster_id=cid,
                    outcome=outcome,
                    abstain_reason=(
                        str(getattr(resp.declined, "reason", ""))
                        if resp.declined is not None
                        else ""
                    ),
                    abstain_explanation=(
                        str(getattr(resp.declined, "explanation", ""))
                        if resp.declined is not None
                        else ""
                    ),
                    duration_ms=duration_ms,
                    tokens_input=int(getattr(resp, "tokens_input", 0) or 0),
                    tokens_output=int(getattr(resp, "tokens_output", 0) or 0),
                )
            )
            out[cid] = ClusterSynthesisResult(
                proposal=None,
                attempted_archetypes=(),
                skipped_reason=f"exception:plan11_stage3_{outcome}",
            )
        return out

    # Split the batched proposals back into per-cluster slices, then
    # dispatch each slice through the standard single-cluster
    # post-processing via ``llm_response_override``.
    proposals_by_cid = _split_batched_proposals_by_cluster(
        resp.parsed_output, cluster_ids, cluster_member_qids,
    )
    tokens_in_total = int(getattr(resp, "tokens_input", 0) or 0)
    tokens_out_total = int(getattr(resp, "tokens_output", 0) or 0)
    # Apportion the input tokens to the first cluster only to avoid
    # double-counting the shared payload in the per-cluster markers.
    # Output tokens we apportion proportionally to proposal counts
    # so each cluster's marker reflects roughly its share of the
    # decode pass.
    out_results: dict[str, ClusterSynthesisResult] = {}
    total_proposal_count = sum(
        len(v) for v in proposals_by_cid.values()
    ) or 1
    first = True
    for cid in cluster_ids:
        slice_proposals = proposals_by_cid.get(cid) or []
        slice_tokens_in = tokens_in_total if first else 0
        first = False
        slice_tokens_out = int(
            tokens_out_total * len(slice_proposals) / total_proposal_count
        )
        slice_resp = LlmReasoningResponse(
            call_id=f"{resp.call_id}.cluster_{cid}",
            skill_id=resp.skill_id,
            succeeded=True,
            parsed_output={"proposals": slice_proposals},
            declined=None,
            raw_text=resp.raw_text,
            tokens_input=slice_tokens_in,
            tokens_output=slice_tokens_out,
            duration_ms=duration_ms,
            error=None,
        )
        out_results[cid] = run_plan11_synthesis_for_single_cluster(
            typed_clusters[cid],
            schema_slices[cid],
            list(history or []),
            member_qid_evidence=member_evidence_by_cid[cid] or None,
            optimization_run_id=optimization_run_id,
            iteration=iteration,
            ag_id=ag_id_by_cid[cid],
            w=w,
            forbidden_signatures=forbidden_signatures,
            insufficient_repair_signatures=insufficient_repair_signatures,
            llm_response_override=slice_resp,
            metadata_snapshot=dict(metadata_snapshot or {}),
            space_id=str(space_id or ""),
            spark=spark,
            catalog=str(catalog or ""),
            gold_schema=str(gold_schema or ""),
            warehouse_id=str(warehouse_id or ""),
        )
    return out_results


# ─── Plan v3 state-machine integration ─────────────────────────────────


class StageThreeContractError(ValueError):
    """Raised when Stage 3 synthesis output is missing fields required by the state machine."""


_STAGE3_REQUIRED_FIELDS: tuple[str, ...] = (
    "intent_id",
    "patch_type",
    "target_objects",
    "target_qids",
    "rca_card_id",
    "causal_target",
    "original_patch_body",
)


def validate_synthesis_output_for_state_machine(proposal_payload: dict) -> None:
    """Raise StageThreeContractError if any state-machine-required field is missing or empty.

    Called at Stage 3 exit. The L6 lane writes a ProposalAttempt to the
    per-QID state ONLY when this validation passes; otherwise the
    transformer emits a contract_failed ProposalAttempt and the run-level
    invariant SM7 catches it.
    """
    missing: list[str] = []
    for field in _STAGE3_REQUIRED_FIELDS:
        value = proposal_payload.get(field, None)
        if value in (None, "", (), [], {}):
            missing.append(field)
    if missing:
        raise StageThreeContractError(
            f"Stage 3 RepairProposal missing required field(s): {missing}. "
            f"State machine requires {_STAGE3_REQUIRED_FIELDS}."
        )


# ─── Escalation rung dispatcher (Plan v3 step §M) ──────────────────────


from enum import StrEnum  # noqa: E402 — placed near consumers


class EscalationRungHint(StrEnum):
    """Closed set of escalation rung shapes the unified dispatcher
    emits. Rungs 1, 3, 4 of the v3 escalation ladder route through
    ``synthesize_escalation_for_state``; rung 2 uses the existing
    ``narrow_replacement_with_llm``."""

    SCOPED_L6 = "scoped_l6"                  # rung 1 — narrower structural fix
    ADD_EXAMPLE_SQL = "add_example_sql"      # rung 3 — teaching artifact
    NARROWED_EXAMPLE_SQL = "narrowed_example_sql"  # rung 4 — single-QID example


def synthesize_escalation_for_state(
    *,
    rung_hint: EscalationRungHint,
    failed_proposal,
    failure_reason: str,
    cluster,
    schema_slice: dict,
    history: list,
    optimization_run_id: str,
    iteration: int,
    ag_id: str,
    w,
):
    """Unified LLM dispatch for escalation rungs 1, 3, 4.

    Reuses ``run_plan11_synthesis_for_single_cluster``'s prompt-building
    + LLM-call machinery by appending a rung-conditional hint marker
    to the cluster's ``repair_hypothesis``. The LLM picks up the hint
    in the prompt and shapes its proposal accordingly:

      * ``SCOPED_L6`` — ask for a narrower structural patch than the
        rejected one.
      * ``ADD_EXAMPLE_SQL`` — ask for a question-scoped teaching
        artifact in place of a structural fix.
      * ``NARROWED_EXAMPLE_SQL`` — ask for an example SQL targeting
        ONLY the failed QID (single-QID member projection).

    Returns the same ``ClusterSynthesisResult`` shape as
    ``run_plan11_synthesis_for_single_cluster`` so the adapter
    callers in ``escalation_ladder.py`` consume one type.

    Parameters
    ----------
    failed_proposal
        The ``RepairProposal`` that the gate just rejected — used to
        derive context for the hint (its patch_type, target_objects,
        blame_set inform what "narrower" means).
    failure_reason
        The rejection reason string. Surfaced into the
        ``repair_hypothesis`` so the LLM sees *why* the prior attempt
        failed and avoids re-emitting the same shape.
    """
    from genie_space_optimizer.optimization.stages.plan11_types import (
        FailureCluster,
    )

    hint_marker = f"escalation_rung:{rung_hint.value}"
    reason_marker = f"prior_failure:{failure_reason}"
    enriched_hypothesis = (
        f"{cluster.repair_hypothesis} | {hint_marker} | {reason_marker}"
    ).strip(" |")

    # NARROWED_EXAMPLE_SQL projects the cluster down to the single
    # failed QID — the rung's whole point is to avoid collateral
    # exposure to co-members.
    if rung_hint == EscalationRungHint.NARROWED_EXAMPLE_SQL:
        member_qids = tuple(failed_proposal.target_qids[:1]) or (
            cluster.member_qids[:1]
        )
    else:
        member_qids = cluster.member_qids

    escalation_cluster = FailureCluster(
        cluster_id=cluster.cluster_id,
        semantic_theme=cluster.semantic_theme,
        member_qids=member_qids,
        unifying_evidence=cluster.unifying_evidence,
        repair_hypothesis=enriched_hypothesis,
        primary_blame_set=cluster.primary_blame_set,
        confidence=cluster.confidence,
    )

    return run_plan11_synthesis_for_single_cluster(
        escalation_cluster,
        dict(schema_slice),
        list(history),
        member_qid_evidence=None,
        optimization_run_id=optimization_run_id,
        iteration=iteration,
        ag_id=ag_id,
        w=w,
    )
