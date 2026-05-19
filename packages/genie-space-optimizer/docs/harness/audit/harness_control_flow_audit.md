# Harness Control-Flow Audit

Function: `_run_lever_loop`  
Line range: 17543–33769  
Total branch points: 1420  

## Reachability summary

* `airline_run_59a173d3`: 4349 lines executed
* `seven_now_run_ab65fefe`: 4406 lines executed

## Branch points

| lineno | type | depth | parent | detail | reached:airline_run_59a173d3 | reached:seven_now_run_ab65fefe | snippet |
|---|---|---|---|---|---|---|---|
| 17629 | try | 0 | module |  | YES | YES | `try:` |
| 17643 | if | 1 | try |  | YES | YES | `if _chunk_d_enabled_rm():` |
| 17651 | try | 2 | if |  | no | no | `try:` |
| 17655 | try | 3 | try |  | no | no | `try:` |
| 17659 | for | 4 | try |  | no | no | `for _rm_tag_key in ("jobId", "multitaskParentRunId", "jobRunId", "runId"):` |
| 17660 | try | 5 | for |  | no | no | `try:` |
| 17662 | if | 6 | try |  | no | no | `if _rm_val.isDefined():` |
| 17664 | except_handler | 5 | for |  | no | no | `except Exception:` |
| 17666 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 17668 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 17702 | try | 2 | if |  | no | no | `try:` |
| 17704 | if | 3 | try |  | no | no | `if _rm_active_run is not None:` |
| 17706 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 17741 | try | 2 | if |  | no | no | `try:` |
| 17763 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 17767 | try | 2 | if |  | YES | YES | `try:` |
| 17769 | if | 3 | try |  | no | no | `if _legacy_active_run is not None:` |
| 17771 | except_handler | 2 | if |  | YES | YES | `except Exception:` |
| 17781 | try | 1 | try |  | YES | YES | `try:` |
| 17783 | except_handler | 1 | try |  | YES | YES | `except NameError:` |
| 17794 | if | 1 | try |  | YES | YES | `if gso_run_manifest_v2_enabled():` |
| 17795 | try | 2 | if |  | YES | YES | `try:` |
| 17805 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 17807 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 17809 | try | 0 | module |  | YES | YES | `try:` |
| 17811 | if | 1 | try |  | YES | YES | `if _mlflow_run_analysis.active_run() is not None:` |
| 17818 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 17835 | try | 0 | module |  | YES | YES | `try:` |
| 17846 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 17851 | if | 0 | module |  | YES | YES | `if not _phase_h_anchor_run_id:` |
| 17852 | try | 1 | if |  | YES | YES | `try:` |
| 17855 | if | 2 | try |  | YES | YES | `if _active_phase_h is not None:` |
| 17857 | except_handler | 1 | if |  | no | no | `except Exception:` |
| 17970 | if | 0 | module |  | YES | YES | `if resume_state.get("prev_scores"):` |
| 17972 | if | 0 | module |  | YES | YES | `if resume_state.get("prev_model_id"):` |
| 17974 | if | 0 | module |  | YES | YES | `if resume_state.get("prev_accuracy"):` |
| 17985 | if | 0 | module |  | YES | YES | `if "_pre_arbiter/overall_accuracy" not in best_scores:` |
| 17986 | if | 1 | if |  | YES | YES | `if "_pre_arbiter/result_correctness" in best_scores:` |
| 18017 | try | 0 | module |  | YES | YES | `try:` |
| 18038 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18063 | try | 0 | module |  | YES | YES | `try:` |
| 18065 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18067 | if | 0 | module |  | YES | YES | `if not _run_row.get("config_snapshot"):` |
| 18076 | try | 1 | if |  | YES | YES | `try:` |
| 18083 | except_handler | 1 | if |  | no | no | `except Exception:` |
| 18176 | try | 0 | module |  | YES | YES | `try:` |
| 18178 | if | 1 | try |  | YES | YES | `if _mlflow_phase_b_init.active_run() is not None:` |
| 18182 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18195 | for | 0 | module |  | YES | YES | `for ov in _judge_overrides:` |
| 18196 | try | 1 | for |  | no | no | `try:` |
| 18198 | if | 2 | try |  | no | no | `if "Genie answer is actually fine" in feedback or "Correct" in feedback:` |
| 18200 | if | 3 | if |  | no | no | `if genie_sql:` |
| 18206 | if | 3 | if |  | no | no | `elif "both answers are wrong" in feedback or "Both Wrong" in feedback:` |
| 18213 | if | 4 | if |  | no | no | `elif "Ambiguous" in feedback:` |
| 18220 | except_handler | 1 | for |  | no | no | `except Exception:` |
| 18223 | if | 0 | module |  | YES | YES | `if _human_sql_fixes:` |
| 18224 | try | 1 | if |  | no | no | `try:` |
| 18231 | except_handler | 1 | if |  | no | no | `except Exception:` |
| 18247 | try | 0 | module |  | YES | YES | `try:` |
| 18255 | if | 1 | try |  | YES | YES | `if _snippet_repair_result.get("rewritten", 0) > 0:` |
| 18258 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18290 | if | 0 | module |  | YES | YES | `if uc_columns:` |
| 18298 | if | 0 | module |  | YES | YES | `if not enrichment_done:` |
| 18308 | with | 1 | if |  | YES | YES | `with _mlflow_legacy_enr.start_run(run_name=_legacy_enr_run_name, nested=True):` |
| 18324 | if | 2 | with |  | YES | YES | `if enrichment_result.get("total_enriched", 0) > 0 or enrichment_result.get("tabl` |
| 18330 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18336 | if | 2 | with |  | YES | YES | `if join_result.get("total_applied", 0) > 0:` |
| 18342 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18348 | if | 2 | with |  | YES | YES | `if meta_result.get("description_generated") or meta_result.get("questions_genera` |
| 18354 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18368 | if | 2 | with |  | YES | YES | `if _legacy_miner_out["total_applied"] or _legacy_miner_out["keep_in_prose_count"` |
| 18374 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18380 | if | 2 | with |  | YES | YES | `if (` |
| 18389 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18394 | if | 2 | with |  | YES | YES | `if ENABLE_PREFLIGHT_EXAMPLE_SQL_SYNTHESIS:` |
| 18395 | try | 3 | if |  | YES | YES | `try:` |
| 18406 | if | 4 | try |  | YES | YES | `if legacy_preflight_result.get("applied", 0) > 0:` |
| 18412 | if | 5 | if |  | no | no | `if uc_columns:` |
| 18414 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 18440 | if | 2 | with |  | no | no | `if (` |
| 18449 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18484 | if | 1 | if |  | no | no | `if uc_columns:` |
| 18486 | if | 1 | if |  | no | no | `if enrichment_model_id:` |
| 18500 | try | 0 | module |  | YES | YES | `try:` |
| 18512 | if | 1 | try |  | YES | YES | `if _current_instr and _current_instr.strip() and _is_unstructured(_current_instr` |
| 18521 | if | 2 | if |  | no | no | `if _restructured_secs:` |
| 18524 | for | 3 | if |  | no | no | `for _sec in INSTRUCTION_SECTION_ORDER:` |
| 18526 | if | 4 | for |  | no | no | `if not _lines_list:` |
| 18527 | continue | 5 | if |  | no | no | `continue` |
| 18529 | for | 4 | for |  | no | no | `for _ln in _lines_list:` |
| 18531 | if | 5 | for |  | no | no | `if not _s:` |
| 18532 | continue | 6 | if |  | no | no | `continue` |
| 18533 | if | 5 | for |  | no | no | `if not _s.startswith("- "):` |
| 18539 | if | 3 | if |  | no | no | `if len(_restructured_text.strip()) >= len(_current_instr.strip()) * 0.5:` |
| 18541 | try | 4 | if |  | no | no | `try:` |
| 18555 | if | 5 | try |  | no | no | `if uc_columns:` |
| 18575 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 18595 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18603 | try | 0 | module |  | YES | YES | `try:` |
| 18607 | if | 1 | try |  | YES | YES | `if _pre_loop_instr and _pre_loop_instr.strip():` |
| 18617 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18628 | try | 0 | module |  | YES | YES | `try:` |
| 18641 | except_handler | 0 | module |  | no | no | `except Exception:  # noqa: BLE001 — never block job startup` |
| 18650 | if | 0 | module |  | YES | YES | `if baseline_iter:` |
| 18652 | if | 1 | if |  | YES | YES | `if isinstance(rows_json, list):` |
| 18656 | if | 2 | if |  | no | no | `elif isinstance(rows_json, str):` |
| 18657 | try | 3 | if |  | no | no | `try:` |
| 18661 | except_handler | 3 | if |  | no | no | `except (json.JSONDecodeError, TypeError):` |
| 18678 | try | 0 | module |  | YES | YES | `try:` |
| 18683 | if | 1 | try |  | YES | YES | `if baseline_iter:` |
| 18686 | if | 2 | if |  | YES | YES | `if isinstance(_rj, list):` |
| 18688 | if | 3 | if |  | no | no | `elif isinstance(_rj, str):` |
| 18689 | try | 4 | if |  | no | no | `try:` |
| 18691 | except_handler | 4 | if |  | no | no | `except (json.JSONDecodeError, TypeError):` |
| 18693 | for | 2 | if |  | YES | YES | `for _row in _rj_rows or []:` |
| 18694 | if | 3 | for |  | YES | YES | `if not isinstance(_row, dict):` |
| 18695 | continue | 4 | if |  | no | no | `continue` |
| 18698 | if | 3 | for |  | YES | YES | `if _qid and _sql:` |
| 18700 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18875 | try | 0 | module |  | YES | YES | `try:` |
| 18893 | if | 1 | try |  | YES | YES | `if (` |
| 18897 | try | 2 | if |  | YES | YES | `try:` |
| 18918 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 18924 | if | 1 | try |  | YES | YES | `if not _baseline_rows_seed:` |
| 18932 | if | 1 | try |  | YES | YES | `if _seeded:` |
| 18940 | if | 2 | if |  | no | no | `elif _baseline_rows_seed:` |
| 18957 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 19011 | if | 0 | module |  | YES | YES | `if baseline_iter:` |
| 19012 | try | 1 | if |  | YES | YES | `try:` |
| 19016 | except_handler | 1 | if |  | no | no | `except Exception:` |
| 19094 | for | 0 | module |  | YES | YES | `for _iter_num in range(1, max_iterations + 1):` |
| 19109 | if | 1 | for |  | YES | YES | `if not _was_collision_skip_this_iter:` |
| 19148 | try | 1 | for |  | YES | YES | `try:` |
| 19163 | if | 2 | try |  | YES | YES | `if reserved_recovery_budget_enabled():` |
| 19192 | if | 3 | if |  | YES | YES | `if _budget_action == RecoveryBudgetAction.SKIP_EARLY_TERMINATE:` |
| 19195 | if | 4 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 19196 | try | 5 | if |  | no | no | `try:` |
| 19206 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 19212 | break | 4 | if |  | no | no | `break` |
| 19219 | if | 2 | try |  | YES | YES | `if arbiter_objective_complete_from_counts(` |
| 19231 | break | 3 | if |  | no | no | `break` |
| 19232 | if | 2 | try |  | YES | YES | `if all_thresholds_met(best_scores, thresholds):` |
| 19246 | if | 2 | try |  | YES | YES | `if _prev_terminal_state:` |
| 19247 | try | 3 | if |  | YES | YES | `try:` |
| 19258 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 19263 | if | 2 | try |  | YES | YES | `if legacy_plateau_allows_stop(` |
| 19289 | try | 3 | if |  | no | no | `try:` |
| 19291 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 19331 | try | 3 | if |  | no | no | `try:` |
| 19343 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 19353 | for | 3 | if |  | no | no | `for _rb in reflection_buffer:` |
| 19354 | for | 4 | for |  | no | no | `for _delta in _rb.get("sql_shape_deltas", []) or []:` |
| 19356 | if | 5 | for |  | no | no | `if _qid and (_delta.get("remaining") or _delta.get("improved")):` |
| 19379 | if | 3 | if |  | no | no | `if _resolved.should_continue:` |
| 19386 | continue | 4 | if |  | no | no | `continue` |
| 19429 | try | 3 | if |  | no | no | `try:` |
| 19468 | try | 4 | try |  | no | no | `try:` |
| 19480 | except_handler | 4 | try |  | no | no | `except NameError:` |
| 19528 | except_handler | 3 | if |  | no | no | `except Exception as _learning_stage_exc:` |
| 19529 | try | 4 | except_handler |  | no | no | `try:` |
| 19533 | if | 5 | try |  | no | no | `if _typed_on():` |
| 19547 | except_handler | 4 | except_handler |  | no | no | `except Exception:` |
| 19561 | break | 3 | if |  | no | no | `break` |
| 19562 | if | 2 | try |  | YES | YES | `if (` |
| 19580 | if | 2 | try |  | YES | YES | `if _diverging:` |
| 19594 | break | 3 | if |  | no | no | `break` |
| 19603 | for | 2 | try |  | YES | YES | `for _rb_entry in reversed(reflection_buffer):` |
| 19604 | if | 3 | for |  | YES | YES | `if _rb_entry.get("escalation_handled"):` |
| 19605 | continue | 4 | if |  | no | no | `continue` |
| 19606 | if | 3 | for |  | YES | YES | `if _rb_entry.get("accepted"):` |
| 19607 | break | 4 | if |  | no | no | `break` |
| 19608 | if | 3 | for |  | YES | YES | `if _rb_entry.get("rollback_class") == _RC.CONTENT_REGRESSION.value:` |
| 19614 | continue | 4 | if |  | YES | YES | `continue` |
| 19615 | if | 2 | try |  | YES | YES | `if _consecutive_rb >= CONSECUTIVE_ROLLBACK_LIMIT:` |
| 19620 | break | 3 | if |  | no | no | `break` |
| 19624 | for | 2 | try |  | YES | YES | `for _esc_entry in reversed(reflection_buffer):` |
| 19625 | if | 3 | for |  | YES | YES | `if not _esc_entry.get("escalation_handled"):` |
| 19626 | break | 4 | if |  | YES | YES | `break` |
| 19628 | if | 3 | for |  | no | no | `if _last_esc_type is None:` |
| 19630 | if | 3 | for |  | no | no | `if _esc_reason == _last_esc_type:` |
| 19633 | break | 4 | if |  | no | no | `break` |
| 19634 | if | 2 | try |  | YES | YES | `if _consecutive_esc >= CONSECUTIVE_ESCALATION_LIMIT:` |
| 19649 | break | 3 | if |  | no | no | `break` |
| 19743 | if | 2 | try |  | YES | YES | `if _forced_synthesis_proposals_carryover:` |
| 19839 | try | 2 | try |  | YES | YES | `try:` |
| 19840 | if | 3 | try |  | YES | YES | `if reflection_buffer:` |
| 19853 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19868 | try | 2 | try |  | YES | YES | `try:` |
| 19870 | if | 3 | try |  | YES | YES | `if _rot_records:` |
| 19874 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19886 | try | 2 | try |  | YES | YES | `try:` |
| 19887 | for | 3 | try |  | YES | YES | `for _sc in soft_signal_clusters or []:` |
| 19889 | if | 4 | for |  | no | no | `if _scid and _scid not in _soft_clusters_seen_run:` |
| 19891 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19898 | try | 2 | try |  | YES | YES | `try:` |
| 19900 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19926 | if | 2 | try |  | YES | YES | `if not (_latest_eval_result or {}).get("question_ids"):` |
| 19927 | try | 3 | if |  | no | no | `try:` |
| 19931 | if | 4 | try |  | no | no | `if _lazy_seed:` |
| 19943 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 19955 | try | 2 | try |  | YES | YES | `try:` |
| 19989 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20009 | for | 2 | try |  | YES | YES | `for _q in _already_passing_set:` |
| 20011 | for | 2 | try |  | YES | YES | `for _q in _hard_qid_set:` |
| 20013 | for | 2 | try |  | YES | YES | `for _q in _soft_qid_set:` |
| 20015 | for | 2 | try |  | YES | YES | `for _q in _gt_corr_qid_set:` |
| 20019 | for | 2 | try |  | YES | YES | `for _c in (clusters or []):` |
| 20021 | if | 3 | for |  | YES | YES | `if _cid:` |
| 20023 | for | 3 | for |  | YES | YES | `for _q in (_c.get("question_ids") or []):` |
| 20025 | if | 4 | for |  | YES | YES | `if _qstr and _cid:` |
| 20036 | try | 2 | try |  | YES | YES | `try:` |
| 20049 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20119 | try | 2 | try |  | YES | YES | `try:` |
| 20135 | except_handler | 2 | try |  | no | no | `except Exception as _exc_eval:` |
| 20136 | try | 3 | except_handler |  | no | no | `try:` |
| 20140 | if | 4 | try |  | no | no | `if _typed_on():` |
| 20154 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 20167 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 20168 | raise | 4 | if |  | no | no | `raise` |
| 20174 | try | 2 | try |  | YES | YES | `try:` |
| 20207 | except_handler | 2 | try |  | no | no | `except Exception as _cluster_exc:` |
| 20208 | try | 3 | except_handler |  | no | no | `try:` |
| 20212 | if | 4 | try |  | no | no | `if _typed_on():` |
| 20226 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 20239 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 20240 | raise | 4 | if |  | no | no | `raise` |
| 20246 | try | 2 | try |  | YES | YES | `try:` |
| 20260 | except_handler | 2 | try |  | no | no | `except Exception as _rca_formed_exc:` |
| 20261 | try | 3 | except_handler |  | no | no | `try:` |
| 20265 | if | 4 | try |  | no | no | `if _typed_on():` |
| 20279 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 20292 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 20293 | raise | 4 | if |  | no | no | `raise` |
| 20297 | try | 2 | try |  | YES | YES | `try:` |
| 20311 | except_handler | 2 | try |  | no | no | `except Exception as _unresolved_rca_exc:` |
| 20312 | try | 3 | except_handler |  | no | no | `try:` |
| 20316 | if | 4 | try |  | no | no | `if _typed_on():` |
| 20330 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 20342 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 20343 | raise | 4 | if |  | no | no | `raise` |
| 20347 | try | 2 | try |  | YES | YES | `try:` |
| 20353 | for | 3 | try |  | YES | YES | `for _qid in (_eval_qids_for_entry or []):` |
| 20356 | if | 4 | for |  | YES | YES | `if isinstance(_scores, dict) and _qstr in _scores:` |
| 20362 | if | 4 | for |  | YES | YES | `if isinstance(_arbiter_map, dict) and _qstr in _arbiter_map:` |
| 20384 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20393 | if | 2 | try |  | YES | YES | `if _iter_num == 1:` |
| 20398 | if | 3 | if |  | YES | YES | `if _scaled_max_iterations != max_iterations:` |
| 20421 | try | 2 | try |  | YES | YES | `try:` |
| 20430 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20436 | try | 2 | try |  | YES | YES | `try:` |
| 20447 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20457 | try | 2 | try |  | YES | YES | `try:` |
| 20465 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20472 | try | 2 | try |  | YES | YES | `try:` |
| 20506 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20509 | try | 2 | try |  | YES | YES | `try:` |
| 20511 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20513 | try | 2 | try |  | YES | YES | `try:` |
| 20515 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20517 | try | 2 | try |  | YES | YES | `try:` |
| 20526 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20534 | try | 2 | try |  | YES | YES | `try:` |
| 20545 | if | 3 | try |  | YES | YES | `if _t8_cases:` |
| 20547 | try | 4 | if |  | no | no | `try:` |
| 20559 | for | 5 | try |  | no | no | `for _idx, _c in enumerate(_t8_cases, start=1):` |
| 20585 | if | 5 | try |  | no | no | `if _t8_audit_rows:` |
| 20590 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 20602 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20611 | if | 2 | try |  | YES | YES | `if human_required_signatures:` |
| 20630 | if | 3 | if |  | no | no | `if _dropped_hard or _dropped_soft:` |
| 20638 | if | 2 | try |  | YES | YES | `if not clusters and not soft_signal_clusters:` |
| 20640 | try | 3 | if |  | no | no | `try:` |
| 20662 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 20677 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 20678 | try | 4 | if |  | no | no | `try:` |
| 20690 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 20696 | break | 3 | if |  | no | no | `break` |
| 20747 | if | 2 | try |  | YES | YES | `if metadata_snapshot.get("_regression_mining_hints"):` |
| 20763 | for | 2 | try |  | YES | YES | `for _sc in soft_signal_clusters or []:` |
| 20764 | if | 3 | for |  | no | no | `if isinstance(_sc, dict):` |
| 20798 | try | 2 | try |  | YES | YES | `try:` |
| 20810 | if | 3 | try |  | YES | YES | `if (` |
| 20821 | for | 4 | if |  | no | no | `for _cid, _drifted in (` |
| 20833 | if | 5 | for |  | no | no | `if _t5_key in _iter_emitted_keys:` |
| 20834 | continue | 6 | if |  | no | no | `continue` |
| 20840 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20876 | if | 2 | try |  | YES | YES | `if reflection_buffer:` |
| 20880 | if | 3 | if |  | YES | YES | `if not _rollback_state_trusted_for_quarantine:` |
| 20896 | for | 3 | if |  | YES | YES | `for _pq_id, _pq_info in _persist_data.items():` |
| 20900 | if | 4 | for |  | YES | YES | `if _pq_class == "ADDITIVE_LEVERS_EXHAUSTED" or (` |
| 20904 | if | 5 | if |  | no | no | `elif _pq_conv in ("stuck", "worsening") and _pq_consec >= 2:` |
| 20913 | if | 3 | if |  | YES | YES | `if _soft_skip_qids:` |
| 20955 | if | 3 | if |  | YES | YES | `if _quarantine_qids:` |
| 20957 | if | 4 | if |  | YES | YES | `if _newly_quarantined:` |
| 20963 | try | 5 | if |  | YES | YES | `try:` |
| 20966 | for | 6 | try |  | YES | YES | `for _hq_id in sorted(_newly_quarantined):` |
| 20982 | if | 6 | try |  | YES | YES | `if _flag_items:` |
| 20992 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 21001 | for | 4 | if |  | YES | YES | `for c in list(clusters) + list(soft_signal_clusters or []):` |
| 21016 | try | 4 | if |  | YES | YES | `try:` |
| 21033 | if | 5 | try |  | YES | YES | `if _q_decision["action"] == "stop_for_human_review":` |
| 21044 | break | 6 | if |  | no | no | `break` |
| 21045 | if | 5 | try |  | YES | YES | `if _q_decision["action"] == "diagnostic_lane":` |
| 21050 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 21055 | if | 4 | if |  | YES | YES | `if not clusters and not soft_signal_clusters:` |
| 21057 | break | 5 | if |  | YES | YES | `break` |
| 21087 | try | 2 | try |  | YES | YES | `try:` |
| 21089 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 21094 | try | 2 | try |  | YES | YES | `try:` |
| 21113 | if | 3 | try |  | YES | YES | `if _process_all_ags and pending_action_groups:` |
| 21127 | while | 4 | if |  | no | no | `while pending_action_groups:` |
| 21133 | if | 5 | while |  | no | no | `if not _candidate_sig_set:` |
| 21146 | if | 6 | if |  | no | no | `if not _src_ids or (_src_ids & _live_cluster_ids):` |
| 21148 | break | 7 | if |  | no | no | `break` |
| 21149 | continue | 6 | if |  | no | no | `continue` |
| 21150 | if | 5 | while |  | no | no | `if _candidate_sig_set & _live_cluster_signatures:` |
| 21155 | break | 6 | if |  | no | no | `break` |
| 21158 | if | 4 | if |  | no | no | `if _dropped_for_drift:` |
| 21159 | for | 5 | if |  | no | no | `for _drop in _dropped_for_drift:` |
| 21177 | if | 4 | if |  | no | no | `if ag is not None:` |
| 21191 | if | 5 | if |  | no | no | `if _regression_debt_qids_for_next_iteration:` |
| 21198 | if | 6 | if |  | no | no | `if not (_debt_set & _ag_qids):` |
| 21207 | if | 3 | try |  | YES | YES | `if ag is None:` |
| 21210 | if | 4 | if |  | YES | YES | `if _regression_debt_qids_for_next_iteration:` |
| 21218 | if | 4 | if |  | YES | YES | `if _unresolved_target_debt_qids_for_next_iteration:` |
| 21248 | while | 4 | if |  | YES | YES | `while diagnostic_action_queue and _diag_preempt is None:` |
| 21265 | if | 5 | while |  | no | no | `if _candidate_sig_set:` |
| 21271 | if | 5 | while |  | no | no | `if not _matches_live:` |
| 21284 | continue | 6 | if |  | no | no | `continue` |
| 21312 | if | 4 | if |  | YES | YES | `if _intent_collisions:` |
| 21326 | try | 5 | if |  | no | no | `try:` |
| 21327 | for | 6 | try |  | no | no | `for _coll in _intent_collisions:` |
| 21331 | for | 7 | for |  | no | no | `for _qids_list in _qbycol.values():` |
| 21335 | if | 7 | for |  | no | no | `if _all_qids:` |
| 21341 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 21346 | if | 4 | if |  | YES | YES | `if _diag_preempt is not None:` |
| 21352 | if | 5 | if |  | YES | YES | `elif _memo_key in strategist_memo_cache:` |
| 21360 | if | 6 | if |  | YES | YES | `if _strategist_constraints.to_strategist_context():` |
| 21386 | try | 6 | if |  | YES | YES | `try:` |
| 21390 | if | 7 | try |  | YES | YES | `if _iter_fb_enabled():` |
| 21392 | if | 8 | if |  | YES | YES | `if _prior_iter >= 0:` |
| 21399 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 21455 | if | 6 | if |  | YES | YES | `if _selector_out["source"] == "three_stage_pipeline":` |
| 21485 | if | 4 | if |  | YES | YES | `if _l3_diagnostics:` |
| 21529 | try | 4 | if |  | YES | YES | `try:` |
| 21534 | if | 5 | try |  | YES | YES | `if _nm_enabled() and action_groups:` |
| 21546 | for | 6 | if |  | no | no | `for _c in clusters or []:` |
| 21549 | if | 7 | for |  | no | no | `if _cid and isinstance(_kit, dict):` |
| 21555 | for | 6 | if |  | no | no | `for _i in sorted(_iter_summaries.keys()):` |
| 21556 | if | 7 | for |  | no | no | `if int(_i) >= int(iteration_counter or 0):` |
| 21557 | continue | 8 | if |  | no | no | `continue` |
| 21561 | if | 7 | for |  | no | no | `if _prior_fb is None:` |
| 21562 | continue | 8 | if |  | no | no | `continue` |
| 21563 | for | 7 | for |  | no | no | `for _key, _shapes in (` |
| 21571 | for | 6 | if |  | no | no | `for _ag in action_groups:` |
| 21606 | if | 7 | for |  | no | no | `if _result.differs or not _strict:` |
| 21623 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 21637 | if | 4 | if |  | YES | YES | `if int(iteration_counter or 0) >= 2:` |
| 21638 | try | 5 | if |  | YES | YES | `try:` |
| 21642 | if | 6 | try |  | YES | YES | `if _al_enabled2():` |
| 21657 | for | 7 | if |  | YES | YES | `for _c in _cands:` |
| 21667 | for | 7 | if |  | YES | YES | `for _pa in _new_pas:` |
| 21682 | for | 7 | if |  | YES | YES | `for _c in _cands:` |
| 21683 | if | 8 | for |  | no | no | `if _c.signature_hash in _synthesised_sigs:` |
| 21684 | continue | 9 | if |  | no | no | `continue` |
| 21700 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 21711 | try | 4 | if |  | YES | YES | `try:` |
| 21716 | if | 5 | try |  | YES | YES | `if _repair_planner_enabled():` |
| 21735 | for | 6 | if |  | YES | YES | `for _c in clusters or []:` |
| 21737 | if | 7 | for |  | YES | YES | `if _kit is None:` |
| 21739 | if | 8 | if |  | YES | YES | `if _card is None:` |
| 21740 | continue | 9 | if |  | YES | YES | `continue` |
| 21751 | continue | 8 | if |  | no | no | `continue` |
| 21762 | if | 7 | for |  | no | no | `if _propagation in (` |
| 21797 | try | 6 | if |  | YES | YES | `try:` |
| 21801 | if | 7 | try |  | YES | YES | `if _al_enabled():` |
| 21811 | for | 8 | if |  | YES | YES | `for _r in _tier1_records:` |
| 21821 | if | 8 | if |  | YES | YES | `if _tier1_records:` |
| 21828 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 21833 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 21843 | try | 4 | if |  | YES | YES | `try:` |
| 21853 | if | 5 | try |  | YES | YES | `if _uncovered:` |
| 21858 | try | 6 | if |  | YES | YES | `try:` |
| 21889 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 21899 | try | 6 | if |  | YES | YES | `try:` |
| 21903 | if | 7 | try |  | YES | YES | `if _recall_enabled():` |
| 21926 | if | 8 | if |  | YES | YES | `if _eligible_ids:` |
| 21935 | try | 9 | if |  | no | no | `try:` |
| 21946 | except_handler | 9 | if |  | no | no | `except Exception:` |
| 21960 | if | 9 | if |  | no | no | `if _recall_succeeded:` |
| 21968 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 21973 | if | 6 | if |  | YES | YES | `if _uncovered:` |
| 21984 | for | 6 | if |  | YES | YES | `for _c in _uncovered:` |
| 22004 | try | 7 | for |  | YES | YES | `try:` |
| 22008 | if | 8 | try |  | YES | YES | `if (` |
| 22046 | if | 9 | if |  | YES | YES | `if _t3_trig_key not in _iter_emitted_keys:` |
| 22064 | if | 9 | if |  | YES | YES | `if _t3_exh_key not in _iter_emitted_keys:` |
| 22075 | for | 10 | if |  | YES | YES | `for _q in _t3_target_qids:` |
| 22076 | try | 11 | for |  | YES | YES | `try:` |
| 22082 | except_handler | 11 | for |  | no | no | `except Exception:` |
| 22092 | continue | 9 | if |  | YES | YES | `continue` |
| 22093 | except_handler | 7 | for |  | no | no | `except Exception:` |
| 22104 | try | 7 | for |  | no | no | `try:` |
| 22115 | if | 8 | try |  | no | no | `if _diag_qids:` |
| 22129 | except_handler | 7 | for |  | no | no | `except Exception:` |
| 22134 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 22170 | for | 4 | if |  | YES | YES | `for _ag_in in action_groups:` |
| 22176 | if | 4 | if |  | YES | YES | `if len(_decomposed_action_groups) != len(action_groups):` |
| 22202 | try | 4 | if |  | YES | YES | `try:` |
| 22203 | for | 5 | try |  | YES | YES | `for _ag_w8 in (action_groups or []):` |
| 22213 | if | 6 | for |  | no | no | `if _before_w8 and set(_before_w8) < set(_after_w8):` |
| 22225 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 22231 | if | 4 | if |  | YES | YES | `if _process_all_ags and len(action_groups) > 1:` |
| 22250 | for | 5 | if |  | no | no | `for _buffered_ag in pending_action_groups:` |
| 22273 | if | 3 | try |  | YES | YES | `if isinstance(_global_rewrite, dict):` |
| 22275 | if | 4 | if |  | no | no | `if non_empty and ag is not None:` |
| 22279 | if | 4 | if |  | YES | YES | `elif isinstance(_global_rewrite, str) and _global_rewrite.strip():` |
| 22280 | if | 5 | if |  | no | no | `if ag is not None:` |
| 22285 | if | 3 | try |  | YES | YES | `if ag is None and _iter_num == 1:` |
| 22295 | if | 4 | if |  | YES | YES | `if _fb_ags:` |
| 22299 | finally | 2 | try |  | YES | YES | `_mlflow.end_run()` |
| 22301 | if | 2 | try |  | YES | YES | `if ag is None and clusters:` |
| 22303 | for | 3 | if |  | YES | YES | `for c in clusters:` |
| 22305 | if | 3 | if |  | YES | YES | `if _remaining_qids and _iter_num <= max_iterations - 1:` |
| 22338 | if | 2 | try |  | YES | YES | `if ag is None:` |
| 22344 | try | 3 | if |  | no | no | `try:` |
| 22366 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 22380 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 22381 | try | 4 | if |  | no | no | `try:` |
| 22399 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 22405 | break | 3 | if |  | no | no | `break` |
| 22438 | try | 2 | try |  | YES | YES | `try:` |
| 22446 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 22487 | for | 2 | try |  | YES | YES | `for _rc_idx, _rc in enumerate(ranked):` |
| 22489 | if | 3 | for |  | YES | YES | `if _ag_source_cids and _rc_cid not in set(_ag_source_cids):` |
| 22490 | continue | 4 | if |  | YES | YES | `continue` |
| 22492 | if | 3 | for |  | YES | YES | `if _rc_sig and _rc_sig not in _ag_source_signatures:` |
| 22494 | if | 3 | for |  | YES | YES | `if not _ag_cluster_info:` |
| 22542 | for | 2 | try |  | YES | YES | `for _scid in (ag.get("source_cluster_ids") or []):` |
| 22548 | if | 3 | for |  | YES | YES | `if isinstance(_candidate_cluster, _Mapping):` |
| 22550 | break | 4 | if |  | YES | YES | `break` |
| 22551 | try | 2 | try |  | YES | YES | `try:` |
| 22559 | except_handler | 2 | try |  | no | no | `except _FailureClusterIdentityError as _identity_err:` |
| 22566 | if | 2 | try |  | YES | YES | `if _failure_cluster_for_collision is not None:` |
| 22578 | if | 2 | try |  | YES | YES | `if _collision_pair_matches(_collision_pair, _forbidden_pair):` |
| 22583 | if | 3 | if |  | no | no | `if (` |
| 22590 | if | 4 | if |  | no | no | `elif (` |
| 22655 | try | 3 | if |  | no | no | `try:` |
| 22677 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 22693 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 22694 | try | 4 | if |  | no | no | `try:` |
| 22713 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 22725 | if | 3 | if |  | no | no | `if _should_terminate_on_collision_saturation(` |
| 22750 | break | 4 | if |  | no | no | `break` |
| 22751 | continue | 3 | if |  | no | no | `continue` |
| 22767 | if | 2 | try |  | YES | YES | `if _ag_proposals and isinstance(_ag_proposals, list):` |
| 22768 | for | 3 | if |  | no | no | `for _prop in _ag_proposals:` |
| 22769 | if | 4 | for |  | no | no | `if not isinstance(_prop, dict):` |
| 22770 | continue | 5 | if |  | no | no | `continue` |
| 22771 | try | 4 | for |  | no | no | `try:` |
| 22784 | except_handler | 4 | for |  | no | no | `except Exception:` |
| 22786 | if | 3 | if |  | no | no | `if _ag_proposals:` |
| 22791 | if | 2 | try |  | YES | YES | `if _escalation:` |
| 22824 | if | 3 | if |  | no | no | `if _escalation == "flag_for_review" or (` |
| 22843 | continue | 4 | if |  | no | no | `continue` |
| 22845 | if | 3 | if |  | no | no | `if _escalation == "gt_repair":` |
| 22847 | if | 4 | if |  | no | no | `if _gt_repair_corrections > 0:` |
| 22884 | continue | 4 | if |  | no | no | `continue` |
| 22886 | if | 3 | if |  | no | no | `if _escalation == "remove_tvf" and _esc_tier in ("auto_apply", "apply_and_flag")` |
| 22889 | if | 4 | if |  | no | no | `if _tvf_id:` |
| 22918 | for | 5 | if |  | no | no | `for idx, entry in enumerate(_tvf_apply_log.get("applied", [])):` |
| 22924 | if | 5 | if |  | no | no | `if _tvf_apply_log.get("patch_deployed", False):` |
| 22927 | if | 6 | if |  | no | no | `if _original_instruction_sections:` |
| 22940 | try | 2 | try |  | YES | YES | `try:` |
| 22978 | if | 3 | try |  | YES | YES | `if _all_required_rca_levers:` |
| 23000 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 23003 | if | 2 | try |  | YES | YES | `if "6" in lever_keys:` |
| 23004 | try | 3 | if |  | YES | YES | `try:` |
| 23025 | for | 4 | try |  | YES | YES | `for _row in _structural_rows:` |
| 23026 | for | 5 | for |  | YES | YES | `for _candidate in extract_failed_row_sql_expression_candidates(_row):` |
| 23028 | if | 4 | try |  | YES | YES | `if _structural_candidates:` |
| 23052 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 23071 | try | 2 | try |  | YES | YES | `try:` |
| 23081 | if | 3 | try |  | YES | YES | `if early_rca_preflight_enabled():` |
| 23094 | if | 4 | if |  | YES | YES | `if _preflight_records:` |
| 23098 | if | 4 | if |  | YES | YES | `if _preflight_decision.action == _WU3_SlateAction.SKIP_AG:` |
| 23121 | continue | 5 | if |  | no | no | `continue` |
| 23122 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 23147 | try | 2 | try |  | YES | YES | `try:` |
| 23151 | if | 3 | try |  | YES | YES | `if _doc_enabled():` |
| 23167 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 23172 | for | 2 | try |  | YES | YES | `for lever_key in lever_keys:` |
| 23184 | try | 3 | for |  | YES | YES | `try:` |
| 23189 | except_handler | 3 | for |  | no | no | `except Exception:` |
| 23213 | try | 3 | for |  | YES | YES | `try:` |
| 23227 | try | 4 | try |  | YES | YES | `try:` |
| 23233 | if | 5 | try |  | YES | YES | `if _bon_first_cid:` |
| 23234 | for | 6 | if |  | YES | YES | `for _bon_c in (clusters or []):` |
| 23235 | if | 7 | for |  | YES | YES | `if str(_bon_c.get("cluster_id") or "") == _bon_first_cid:` |
| 23239 | if | 8 | if |  | YES | YES | `if _bon_card is not None:` |
| 23254 | break | 8 | if |  | YES | YES | `break` |
| 23255 | except_handler | 4 | try |  | no | no | `except Exception:` |
| 23265 | try | 4 | try |  | YES | YES | `try:` |
| 23279 | except_handler | 4 | try |  | no | no | `except Exception:` |
| 23288 | except_handler | 3 | for |  | no | no | `except Exception:` |
| 23296 | if | 3 | for |  | YES | YES | `if _use_best_of_n:` |
| 23300 | for | 4 | if |  | no | no | `for _bon_idx in range(3):` |
| 23301 | try | 5 | for |  | no | no | `try:` |
| 23317 | if | 6 | try |  | no | no | `if _sample:` |
| 23322 | except_handler | 5 | for |  | no | no | `except Exception:` |
| 23330 | if | 4 | if |  | no | no | `if _bon_candidates:` |
| 23331 | try | 5 | if |  | no | no | `try:` |
| 23340 | if | 6 | try |  | no | no | `if _bon_top is not None:` |
| 23354 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 23368 | try | 4 | if |  | no | no | `try:` |
| 23373 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23378 | try | 4 | if |  | no | no | `try:` |
| 23403 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23435 | if | 3 | for |  | YES | YES | `if _directive_outcome_ledger is not None and (` |
| 23438 | try | 4 | if |  | YES | YES | `try:` |
| 23448 | try | 5 | try |  | YES | YES | `try:` |
| 23453 | except_handler | 5 | try |  | no | no | `except Exception:` |
| 23469 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23481 | if | 2 | try |  | YES | YES | `if not all_proposals:` |
| 23482 | try | 3 | if |  | YES | YES | `try:` |
| 23494 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 23508 | if | 2 | try |  | YES | YES | `if _directive_outcome_ledger is not None:` |
| 23509 | try | 3 | if |  | YES | YES | `try:` |
| 23516 | for | 4 | try |  | YES | YES | `for _lever_int, _outcome in list(` |
| 23526 | if | 5 | for |  | YES | YES | `if _refined != _outcome:` |
| 23530 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 23540 | if | 2 | try |  | YES | YES | `if _directive_outcome_ledger is not None:` |
| 23541 | try | 3 | if |  | YES | YES | `try:` |
| 23553 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 23567 | try | 2 | try |  | YES | YES | `try:` |
| 23593 | if | 3 | try |  | YES | YES | `if (` |
| 23599 | for | 4 | if |  | YES | YES | `for _cid in (ag.get("source_cluster_ids") or []):` |
| 23604 | if | 5 | for |  | YES | YES | `if isinstance(_src_cluster, dict) and not _l5_ag_root_cause:` |
| 23608 | if | 5 | for |  | YES | YES | `if _l5_ag_rca_id and _l5_ag_root_cause:` |
| 23609 | break | 6 | if |  | YES | no | `break` |
| 23628 | try | 4 | if |  | YES | YES | `try:` |
| 23633 | for | 5 | try |  | YES | YES | `for _md in _l5_ag_drops:` |
| 23634 | for | 6 | for |  | YES | YES | `for _rc in (_md.get("root_causes") or ()):` |
| 23636 | if | 7 | for |  | YES | YES | `if _rc_s and _rc_s not in _l5_marker_root_causes:` |
| 23647 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23659 | try | 4 | if |  | YES | YES | `try:` |
| 23731 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23748 | try | 4 | if |  | YES | YES | `try:` |
| 23780 | for | 5 | try |  | YES | YES | `for _forced_proposal in _dispatch_result.appended_proposals:` |
| 23788 | for | 5 | try |  | YES | YES | `for _nsc_dict in _dispatch_result.emitted_decision_records:` |
| 23794 | try | 6 | for |  | YES | YES | `try:` |
| 23817 | except_handler | 6 | for |  | no | no | `except Exception:` |
| 23823 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23836 | if | 5 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 23837 | raise | 6 | if |  | no | no | `raise` |
| 23838 | except_handler | 2 | try |  | no | no | `except Exception as _lever5_structural_gate_exc:` |
| 23839 | try | 3 | except_handler |  | no | no | `try:` |
| 23843 | if | 4 | try |  | no | no | `if _typed_on():` |
| 23857 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 23869 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 23870 | raise | 4 | if |  | no | no | `raise` |
| 23880 | try | 2 | try |  | YES | YES | `try:` |
| 23884 | for | 3 | try |  | YES | YES | `for _force_cid in (ag.get("source_cluster_ids") or ()):` |
| 23888 | if | 4 | for |  | YES | YES | `if not isinstance(_force_cluster, dict):` |
| 23889 | continue | 5 | if |  | no | no | `continue` |
| 23904 | if | 4 | for |  | YES | YES | `if (` |
| 23961 | if | 4 | for |  | YES | YES | `if _ag_sigs:` |
| 23964 | try | 4 | for |  | YES | YES | `try:` |
| 23989 | if | 5 | try |  | YES | YES | `if _forced_l6 is None:` |
| 23991 | except_handler | 4 | for |  | no | no | `except Exception as _force_exc:` |
| 23994 | try | 5 | except_handler |  | no | no | `try:` |
| 23998 | if | 6 | try |  | no | no | `if _typed_on():` |
| 24012 | except_handler | 5 | except_handler |  | no | no | `except Exception:` |
| 24026 | if | 5 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 24027 | raise | 6 | if |  | no | no | `raise` |
| 24029 | if | 4 | for |  | YES | YES | `if _forced_l6 is not None:` |
| 24074 | if | 5 | if |  | YES | YES | `if _force_outcome == "raised":` |
| 24102 | except_handler | 2 | try |  | no | no | `except Exception as _forced_lever6_n3_exc:` |
| 24103 | try | 3 | except_handler |  | no | no | `try:` |
| 24107 | if | 4 | try |  | no | no | `if _typed_on():` |
| 24121 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 24135 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 24136 | raise | 4 | if |  | no | no | `raise` |
| 24172 | for | 2 | try |  | YES | YES | `for _rb in reflection_buffer:` |
| 24173 | if | 3 | for |  | no | no | `if _rb.get("accepted"):` |
| 24174 | continue | 4 | if |  | no | no | `continue` |
| 24182 | if | 3 | for |  | no | no | `if _rb.get("rollback_class") != _RC.CONTENT_REGRESSION.value:` |
| 24183 | continue | 4 | if |  | no | no | `continue` |
| 24185 | for | 3 | for |  | no | no | `for _dnr in _rb.get("do_not_retry", []):` |
| 24187 | if | 4 | for |  | no | no | `if " on " not in _s:` |
| 24188 | continue | 5 | if |  | no | no | `continue` |
| 24193 | for | 3 | for |  | no | no | `for _rb_patch in _rb.get("do_not_retry_patches", []) or []:` |
| 24194 | if | 4 | for |  | no | no | `if isinstance(_rb_patch, dict):` |
| 24236 | for | 2 | try |  | YES | YES | `for _rb in reflection_buffer:` |
| 24237 | if | 3 | for |  | no | no | `if _rb.get("accepted"):` |
| 24238 | continue | 4 | if |  | no | no | `continue` |
| 24239 | for | 3 | for |  | no | no | `for _rb_patch in _rb.get("do_not_retry_patches", []) or []:` |
| 24240 | if | 4 | for |  | no | no | `if isinstance(_rb_patch, dict):` |
| 24248 | if | 2 | try |  | YES | YES | `if _content_dedup_dropped:` |
| 24254 | if | 2 | try |  | YES | YES | `if _patch_forbidden:` |
| 24264 | for | 3 | if |  | no | no | `for _rb in reflection_buffer:` |
| 24265 | if | 4 | for |  | no | no | `if _rb.get("accepted"):` |
| 24266 | continue | 5 | if |  | no | no | `continue` |
| 24267 | for | 4 | for |  | no | no | `for _entry in _rb.get("do_not_retry", []) or []:` |
| 24269 | if | 5 | for |  | no | no | `if " on " in _es:` |
| 24275 | for | 3 | if |  | no | no | `for _p in all_proposals:` |
| 24296 | if | 4 | for |  | no | no | `if (` |
| 24305 | if | 5 | if |  | no | no | `if _retry_decision.allowed:` |
| 24312 | continue | 6 | if |  | no | no | `continue` |
| 24313 | if | 4 | for |  | no | no | `if _key in _patch_forbidden:` |
| 24314 | if | 5 | if |  | no | no | `if not _justification:` |
| 24317 | continue | 6 | if |  | no | no | `continue` |
| 24318 | if | 5 | if |  | no | no | `if (` |
| 24327 | continue | 6 | if |  | no | no | `continue` |
| 24353 | if | 3 | if |  | no | no | `if _dropped:` |
| 24360 | for | 4 | if |  | no | no | `for _ptype, _target, _reason in _dropped:` |
| 24369 | for | 4 | if |  | no | no | `for _ptype, _target, _reason in _dropped:` |
| 24377 | if | 3 | if |  | no | no | `if _reflection_rewrites:` |
| 24378 | try | 4 | if |  | no | no | `try:` |
| 24383 | for | 5 | try |  | no | no | `for _idx, _rw in enumerate(_reflection_rewrites, start=1):` |
| 24407 | if | 5 | try |  | no | no | `if _t10_rows:` |
| 24412 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 24431 | try | 3 | if |  | no | no | `try:` |
| 24432 | if | 4 | try |  | no | no | `if _dropped:` |
| 24450 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 24502 | if | 2 | try |  | YES | YES | `if _collateral_details:` |
| 24503 | for | 3 | if |  | no | no | `for _ptype, _target, _deps in _collateral_details:` |
| 24516 | for | 2 | try |  | YES | YES | `for pi, p in enumerate(all_proposals, 1):` |
| 24524 | if | 3 | for |  | no | no | `if status == "FAILED (non-JSON)":` |
| 24526 | if | 4 | if |  | no | no | `elif status == "INVALID_TARGET":` |
| 24534 | if | 3 | for |  | no | no | `if table:` |
| 24536 | if | 3 | for |  | no | no | `if column:` |
| 24541 | if | 3 | for |  | no | no | `if isinstance(_p_col_sect, dict) and _p_col_sect:` |
| 24543 | for | 4 | if |  | no | no | `for _sk, _sv in _p_col_sect.items():` |
| 24546 | if | 4 | if |  | no | no | `elif isinstance(_p_tbl_sect, dict) and _p_tbl_sect:` |
| 24548 | for | 5 | if |  | no | no | `for _sk, _sv in _p_tbl_sect.items():` |
| 24551 | if | 5 | if |  | no | no | `elif proposed_value:` |
| 24559 | if | 2 | try |  | YES | YES | `if _n_failed:` |
| 24567 | for | 2 | try |  | YES | YES | `for pi, p in enumerate(all_proposals, 1):` |
| 24569 | if | 3 | for |  | no | no | `if not prov:` |
| 24570 | continue | 4 | if |  | no | no | `continue` |
| 24584 | try | 2 | try |  | YES | YES | `try:` |
| 24586 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24589 | if | 2 | try |  | YES | YES | `if not all_proposals:` |
| 24621 | try | 3 | if |  | YES | YES | `try:` |
| 24625 | if | 4 | try |  | YES | YES | `if forbidden_ag_admits_no_action_enabled():` |
| 24657 | try | 5 | if |  | YES | YES | `try:` |
| 24726 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 24732 | try | 5 | if |  | YES | YES | `try:` |
| 24733 | for | 6 | try |  | YES | YES | `for _lk in lever_keys:` |
| 24739 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 24745 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 24762 | try | 3 | if |  | YES | YES | `try:` |
| 24784 | except_handler | 3 | if |  | YES | YES | `except Exception:` |
| 24798 | if | 3 | if |  | YES | YES | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 24799 | try | 4 | if |  | YES | YES | `try:` |
| 24818 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 24824 | continue | 3 | if |  | YES | YES | `continue` |
| 24829 | try | 2 | try |  | no | no | `try:` |
| 24836 | for | 3 | try |  | no | no | `for _p in all_proposals:` |
| 24838 | if | 4 | for |  | no | no | `if _decision["compatible"]:` |
| 24847 | if | 3 | try |  | no | no | `if _incompatible_proposals:` |
| 24864 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24877 | try | 2 | try |  | no | no | `try:` |
| 24912 | for | 3 | try |  | no | no | `for _prop in (all_proposals or []):` |
| 24914 | if | 4 | for |  | no | no | `if not _pid:` |
| 24915 | continue | 5 | if |  | no | no | `continue` |
| 24921 | try | 4 | for |  | no | no | `try:` |
| 24923 | except_handler | 4 | for |  | no | no | `except Exception:` |
| 24925 | if | 4 | for |  | no | no | `if _ri_obj is not None:` |
| 24937 | for | 3 | try |  | no | no | `for _c in (clusters or []):` |
| 24939 | if | 4 | for |  | no | no | `if _cid_crit:` |
| 24951 | for | 3 | try |  | no | no | `for _qid, _ev in (_rca_evidence_typed_flat or {}).items():` |
| 24955 | if | 4 | for |  | no | no | `if not _cid_for_qid:` |
| 24956 | continue | 5 | if |  | no | no | `continue` |
| 24986 | if | 3 | try |  | no | no | `if _critique_gate_on() and critique_outcome.dropped_by_critique:` |
| 24994 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 25004 | try | 2 | try |  | no | no | `try:` |
| 25021 | if | 3 | try |  | no | no | `if _shape_decisions:` |
| 25043 | if | 4 | if |  | no | no | `if _rca_shape_drops:` |
| 25051 | for | 5 | if |  | no | no | `for _drop in _rca_shape_drops:` |
| 25076 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 25084 | try | 2 | try |  | no | no | `try:` |
| 25090 | if | 3 | try |  | no | no | `if _shape_dropped_ids:` |
| 25129 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 25140 | try | 2 | try |  | no | no | `try:` |
| 25144 | for | 3 | try |  | no | no | `for _snap in reversed(_ag_snapshots):` |
| 25145 | if | 4 | for |  | no | no | `if str(_snap.get("id")) == str(ag_id):` |
| 25160 | break | 5 | if |  | no | no | `break` |
| 25161 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 25170 | try | 2 | try |  | no | no | `try:` |
| 25181 | if | 3 | try |  | no | no | `if not _ag_assigned_qids:` |
| 25190 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 25218 | try | 2 | try |  | no | no | `try:` |
| 25261 | if | 3 | try |  | no | no | `if _chunk_b_on():` |
| 25312 | try | 3 | try |  | no | no | `try:` |
| 25313 | if | 4 | try |  | no | no | `if isinstance(metadata_snapshot, dict):` |
| 25317 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25323 | try | 3 | try |  | no | no | `try:` |
| 25334 | if | 4 | try |  | no | no | `if _pd_records:` |
| 25338 | for | 5 | if |  | no | no | `for _pd_rec in _pd_records:` |
| 25339 | try | 6 | for |  | no | no | `try:` |
| 25341 | if | 7 | try |  | no | no | `if _pd_key in _iter_emitted_keys:` |
| 25342 | continue | 8 | if |  | no | no | `continue` |
| 25344 | except_handler | 6 | for |  | no | no | `except Exception:` |
| 25350 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25377 | try | 3 | try |  | no | no | `try:` |
| 25384 | if | 4 | try |  | no | no | `if (` |
| 25394 | for | 5 | if |  | no | no | `for _sc in soft_signal_clusters or []:` |
| 25396 | if | 6 | for |  | no | no | `if not _sc_cid:` |
| 25397 | continue | 7 | if |  | no | no | `continue` |
| 25408 | for | 5 | if |  | no | no | `for _cand in clusters or []:` |
| 25409 | if | 6 | for |  | no | no | `if not isinstance(_cand, dict):` |
| 25410 | continue | 7 | if |  | no | no | `continue` |
| 25411 | if | 6 | for |  | no | no | `if bool(_cand.get("rca_card")):` |
| 25412 | continue | 7 | if |  | no | no | `continue` |
| 25414 | if | 6 | for |  | no | no | `if not _cand_cid:` |
| 25415 | continue | 7 | if |  | no | no | `continue` |
| 25417 | if | 6 | for |  | no | no | `if not _soft_entry:` |
| 25418 | continue | 7 | if |  | no | no | `continue` |
| 25428 | try | 6 | for |  | no | no | `try:` |
| 25433 | except_handler | 6 | for |  | no | no | `except Exception:` |
| 25441 | if | 6 | for |  | no | no | `if _prov_card is None:` |
| 25442 | continue | 7 | if |  | no | no | `continue` |
| 25448 | try | 6 | for |  | no | no | `try:` |
| 25469 | except_handler | 6 | for |  | no | no | `except Exception:` |
| 25475 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25491 | try | 3 | try |  | no | no | `try:` |
| 25495 | if | 4 | try |  | no | no | `if ag_emit_grounding_gate_enabled():` |
| 25496 | checkpoint_call | 5 | if | collect_blocked_clusters | no | no | `_grounding_result = collect_blocked_clusters(` |
| 25504 | if | 5 | if |  | no | no | `if _grounding_result.records_payload:` |
| 25508 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25538 | try | 3 | try |  | no | no | `try:` |
| 25545 | if | 4 | try |  | no | no | `if strategist_recovery_pivot_enabled() and reflection_buffer:` |
| 25560 | for | 5 | if |  | no | no | `for _c in (clusters or []):` |
| 25561 | if | 6 | for |  | no | no | `if not isinstance(_c, dict):` |
| 25562 | continue | 7 | if |  | no | no | `continue` |
| 25564 | if | 6 | for |  | no | no | `if not _cid:` |
| 25565 | continue | 7 | if |  | no | no | `continue` |
| 25566 | for | 6 | for |  | no | no | `for _q in (_c.get("question_ids") or ()):` |
| 25568 | if | 7 | for |  | no | no | `if _qs:` |
| 25588 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25648 | if | 3 | try |  | no | no | `if _admission_on():` |
| 25682 | except_handler | 2 | try |  | no | no | `except Exception as _strategist_ag_exc:` |
| 25683 | try | 3 | except_handler |  | no | no | `try:` |
| 25687 | if | 4 | try |  | no | no | `if _typed_on():` |
| 25701 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 25714 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 25715 | raise | 4 | if |  | no | no | `raise` |
| 25722 | try | 2 | try |  | no | no | `try:` |
| 25737 | if | 3 | try |  | no | no | `if not _ag_verdict.accepted:` |
| 25740 | for | 4 | if |  | no | no | `for _cid in (ag.get("source_cluster_ids") or []):` |
| 25744 | if | 5 | for |  | no | no | `if _ag_rca_id_c5:` |
| 25745 | break | 6 | if |  | no | no | `break` |
| 25762 | except_handler | 2 | try |  | no | no | `except Exception as _groundedness_ag_exc:` |
| 25763 | try | 3 | except_handler |  | no | no | `try:` |
| 25767 | if | 4 | try |  | no | no | `if _typed_on():` |
| 25781 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 25793 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 25794 | raise | 4 | if |  | no | no | `raise` |
| 25800 | try | 2 | try |  | no | no | `try:` |
| 25801 | for | 3 | try |  | no | no | `for _p in (all_proposals or []):` |
| 25803 | if | 4 | for |  | no | no | `if not _ptids:` |
| 25806 | if | 4 | for |  | no | no | `if not _ptids:` |
| 25807 | continue | 5 | if |  | no | no | `continue` |
| 25831 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 25866 | try | 2 | try |  | no | no | `try:` |
| 25873 | if | 3 | try |  | no | no | `if _chunk_c_on_f5():` |
| 25927 | except_handler | 2 | try |  | no | no | `except Exception as _proposal_generated_exc:` |
| 25928 | try | 3 | except_handler |  | no | no | `try:` |
| 25932 | if | 4 | try |  | no | no | `if _typed_on():` |
| 25946 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 25959 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 25960 | raise | 4 | if |  | no | no | `raise` |
| 25964 | try | 2 | try |  | no | no | `try:` |
| 25977 | for | 3 | try |  | no | no | `for _prop in (all_proposals or []):` |
| 25979 | if | 4 | for |  | no | no | `if not _prop_id:` |
| 25980 | continue | 5 | if |  | no | no | `continue` |
| 25984 | if | 4 | for |  | no | no | `if _verdict_p.accepted:` |
| 25985 | continue | 5 | if |  | no | no | `continue` |
| 25995 | if | 3 | try |  | no | no | `if _proposal_drops_c5:` |
| 26004 | except_handler | 2 | try |  | no | no | `except Exception as _groundedness_proposal_exc:` |
| 26005 | try | 3 | except_handler |  | no | no | `try:` |
| 26009 | if | 4 | try |  | no | no | `if _typed_on():` |
| 26023 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 26035 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 26036 | raise | 4 | if |  | no | no | `raise` |
| 26046 | try | 2 | try |  | no | no | `try:` |
| 26052 | if | 3 | try |  | no | no | `if len(patches) > _pre_split_count:` |
| 26058 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 26071 | try | 2 | try |  | no | no | `try:` |
| 26099 | for | 3 | try |  | no | no | `for _patch in patches:` |
| 26100 | try | 4 | for |  | no | no | `try:` |
| 26102 | if | 5 | try |  | no | no | `if isinstance(_rca_exec, dict):` |
| 26107 | except_handler | 4 | for |  | no | no | `except Exception:` |
| 26119 | if | 4 | for |  | no | no | `if _score >= MIN_PROPOSAL_RELEVANCE:` |
| 26155 | if | 3 | try |  | no | no | `if _dropped:` |
| 26192 | try | 3 | try |  | no | no | `try:` |
| 26210 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 26216 | try | 3 | try |  | no | no | `try:` |
| 26228 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 26235 | try | 3 | try |  | no | no | `try:` |
| 26240 | for | 4 | try |  | no | no | `for _idx, (_patch, _score, _dec) in enumerate(` |
| 26295 | if | 4 | try |  | no | no | `if _grounding_rows:` |
| 26299 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 26304 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 26316 | if | 2 | try |  | no | no | `if _grounding_skip.skip:` |
| 26369 | try | 3 | if |  | no | no | `try:` |
| 26391 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 26406 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 26407 | try | 4 | if |  | no | no | `try:` |
| 26426 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26432 | continue | 3 | if |  | no | no | `continue` |
| 26439 | try | 2 | try |  | no | no | `try:` |
| 26471 | if | 3 | try |  | no | no | `if _stage6_br_pure_on():` |
| 26488 | for | 4 | if |  | no | no | `for _candidate in patches:` |
| 26495 | if | 5 | for |  | no | no | `if not _decision["safe"]:` |
| 26524 | continue | 6 | if |  | no | no | `continue` |
| 26531 | if | 5 | for |  | no | no | `if not _scope_decision["safe"]:` |
| 26553 | continue | 6 | if |  | no | no | `continue` |
| 26586 | if | 3 | try |  | no | no | `if _narrow_kept:` |
| 26602 | try | 3 | try |  | no | no | `try:` |
| 26606 | if | 4 | try |  | no | no | `if (` |
| 26705 | if | 5 | if |  | no | no | `if _p24_outside_target:` |
| 26707 | if | 5 | if |  | no | no | `if (` |
| 26715 | try | 6 | if |  | no | no | `try:` |
| 26722 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 26724 | if | 6 | if |  | no | no | `if _p24_retest.get("safe") is True:` |
| 26730 | try | 5 | if |  | no | no | `try:` |
| 26746 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 26751 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 26771 | if | 3 | try |  | no | no | `if _stage6_nr_pure_on():` |
| 26772 | try | 4 | if |  | no | no | `try:` |
| 26795 | if | 5 | try |  | no | no | `if _rco4_nr_outcome.halt_no_structural_alternative:` |
| 26850 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26859 | try | 4 | if |  | no | no | `try:` |
| 26869 | if | 5 | try |  | no | no | `if _structural_drops:` |
| 26915 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26922 | if | 3 | try |  | no | no | `if _blast_dropped:` |
| 26948 | try | 4 | if |  | no | no | `try:` |
| 26957 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26966 | try | 4 | if |  | no | no | `try:` |
| 26973 | for | 5 | try |  | no | no | `for _cid in (ag.get("source_cluster_ids") or []):` |
| 26977 | if | 6 | for |  | no | no | `if not _br_root_cause:` |
| 26981 | if | 6 | for |  | no | no | `if not _br_rca_id:` |
| 26985 | if | 6 | for |  | no | no | `if _br_root_cause and _br_rca_id:` |
| 26986 | break | 7 | if |  | no | no | `break` |
| 27004 | except_handler | 4 | if |  | no | no | `except Exception as _blast_radius_exc:` |
| 27005 | try | 5 | except_handler |  | no | no | `try:` |
| 27009 | if | 6 | try |  | no | no | `if _typed_on():` |
| 27023 | except_handler | 5 | except_handler |  | no | no | `except Exception:` |
| 27037 | if | 5 | except_handler |  | no | no | `if is_strict_mode():` |
| 27038 | raise | 6 | if |  | no | no | `raise` |
| 27048 | try | 4 | if |  | no | no | `try:` |
| 27055 | if | 5 | try |  | no | no | `if _t2_target_qids:` |
| 27056 | for | 6 | if |  | no | no | `for _drop in _blast_dropped or ():` |
| 27075 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 27082 | except_handler | 2 | try |  | no | no | `except ImportError:` |
| 27107 | for | 3 | except_handler |  | no | no | `for _candidate in patches:` |
| 27114 | if | 4 | for |  | no | no | `if _decision["safe"]:` |
| 27152 | if | 3 | except_handler |  | no | no | `if _narrow_kept:` |
| 27160 | try | 3 | except_handler |  | no | no | `try:` |
| 27164 | if | 4 | try |  | no | no | `if (` |
| 27241 | if | 5 | if |  | no | no | `if _p24b_outside_target:` |
| 27243 | if | 5 | if |  | no | no | `if (` |
| 27248 | try | 6 | if |  | no | no | `try:` |
| 27255 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 27257 | if | 6 | if |  | no | no | `if _p24b_retest.get("safe") is True:` |
| 27260 | try | 5 | if |  | no | no | `try:` |
| 27277 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 27282 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 27289 | if | 3 | except_handler |  | no | no | `if _blast_dropped:` |
| 27308 | try | 2 | try |  | no | no | `try:` |
| 27312 | if | 3 | try |  | no | no | `if _structural_repair_on():` |
| 27340 | for | 4 | if |  | no | no | `for _sr_cid in (ag.get("source_cluster_ids") or []):` |
| 27349 | if | 5 | for |  | no | no | `if _sr_full_card is not None or _sr_rca_card is not None:` |
| 27350 | break | 6 | if |  | no | no | `break` |
| 27409 | try | 4 | if |  | no | no | `try:` |
| 27425 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 27431 | try | 4 | if |  | no | no | `try:` |
| 27450 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 27461 | if | 4 | if |  | no | no | `if _sr_verdict.outcome == "rejected":` |
| 27462 | if | 5 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 27463 | try | 6 | if |  | no | no | `try:` |
| 27480 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 27521 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27539 | try | 2 | try |  | no | no | `try:` |
| 27543 | if | 3 | try |  | no | no | `if _chunk_c_on_f6():` |
| 27586 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27605 | try | 2 | try |  | no | no | `try:` |
| 27612 | if | 3 | try |  | no | no | `if _stage6_app_pure_on():` |
| 27646 | if | 3 | try |  | no | no | `if _non_applyable_decisions:` |
| 27667 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27678 | try | 2 | try |  | no | no | `try:` |
| 27680 | if | 3 | try |  | no | no | `if _non_applyable:` |
| 27699 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27726 | for | 2 | try |  | no | no | `for _p in patches:` |
| 27727 | if | 3 | for |  | no | no | `if not l5_l6_patch_requires_asset_alignment(_p):` |
| 27729 | continue | 4 | if |  | no | no | `continue` |
| 27739 | if | 3 | for |  | no | no | `if _decision.get("aligned"):` |
| 27741 | continue | 4 | if |  | no | no | `continue` |
| 27749 | if | 2 | try |  | no | no | `if _alignment_drops:` |
| 27759 | try | 2 | try |  | no | no | `try:` |
| 27760 | if | 3 | try |  | no | no | `if _alignment_drops:` |
| 27808 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27819 | if | 2 | try |  | no | no | `if len(patches) > MAX_AG_PATCHES:` |
| 27852 | if | 3 | if |  | no | no | `if _no_causal_halt():` |
| 27858 | if | 4 | if |  | no | no | `if (` |
| 27899 | try | 5 | if |  | no | no | `try:` |
| 27964 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 27970 | try | 5 | if |  | no | no | `try:` |
| 27971 | for | 6 | try |  | no | no | `for _lk in lever_keys:` |
| 27977 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 28001 | try | 4 | if |  | no | no | `try:` |
| 28006 | if | 5 | try |  | no | no | `if _hub_scoped_enabled():` |
| 28022 | for | 6 | if |  | no | no | `for _p in _expanded:` |
| 28024 | if | 7 | for |  | no | no | `if not _scoped_from:` |
| 28025 | continue | 8 | if |  | no | no | `continue` |
| 28037 | for | 6 | if |  | no | no | `for _p in _before_cap:` |
| 28038 | if | 7 | for |  | no | no | `if not _is_hub_patch(_p, threshold=_hub_th_val):` |
| 28039 | continue | 8 | if |  | no | no | `continue` |
| 28047 | if | 7 | for |  | no | no | `if _has_sibling:` |
| 28048 | continue | 8 | if |  | no | no | `continue` |
| 28066 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 28081 | if | 4 | if |  | no | no | `if _kit_aware_enabled():` |
| 28106 | try | 5 | if |  | no | no | `try:` |
| 28110 | if | 6 | try |  | no | no | `if _soft_ev_enabled():` |
| 28118 | if | 7 | if |  | no | no | `if _soft_lookup:` |
| 28136 | try | 8 | if |  | no | no | `try:` |
| 28137 | for | 9 | try |  | no | no | `for _qids in _soft_lookup.values():` |
| 28138 | for | 10 | for |  | no | no | `for _q in _qids:` |
| 28140 | except_handler | 8 | if |  | no | no | `except Exception:` |
| 28146 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 28153 | try | 5 | if |  | no | no | `try:` |
| 28166 | for | 6 | try |  | no | no | `for _ko in _kit_outcomes:` |
| 28179 | if | 7 | for |  | no | no | `if _ko.get("accepted"):` |
| 28180 | if | 8 | if |  | no | no | `if _ko.get("risk_downgraded_from_high_to_medium"):` |
| 28193 | continue | 8 | if |  | no | no | `continue` |
| 28195 | if | 7 | for |  | no | no | `if _reason == "kit_atomicity_violation":` |
| 28217 | if | 6 | try |  | no | no | `if not patches:` |
| 28229 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 28262 | for | 3 | if |  | no | no | `for _d in _dropped_decisions:` |
| 28279 | try | 3 | if |  | no | no | `try:` |
| 28281 | for | 4 | try |  | no | no | `for _bp in (_before_cap or []):` |
| 28285 | if | 5 | for |  | no | no | `if _bpid:` |
| 28287 | for | 4 | try |  | no | no | `for _d in _dropped_decisions:` |
| 28293 | if | 5 | for |  | no | no | `if not _dt_qids:` |
| 28301 | if | 5 | for |  | no | no | `if _dt_qids:` |
| 28320 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28356 | try | 3 | if |  | no | no | `try:` |
| 28393 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28419 | if | 2 | try |  | no | no | `if (` |
| 28446 | try | 3 | if |  | no | no | `try:` |
| 28450 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28464 | if | 3 | if |  | no | no | `if not pending_action_groups:` |
| 28466 | try | 3 | if |  | no | no | `try:` |
| 28488 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28504 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 28505 | try | 4 | if |  | no | no | `try:` |
| 28524 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 28562 | continue | 3 | if |  | no | no | `continue` |
| 28571 | if | 2 | try |  | no | no | `if SHADOW_APPLY:` |
| 28602 | if | 2 | try |  | no | no | `if not _pre_ag_snapshot_capture.get("captured"):` |
| 28622 | try | 3 | if |  | no | no | `try:` |
| 28626 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28641 | if | 3 | if |  | no | no | `if not pending_action_groups:` |
| 28643 | try | 3 | if |  | no | no | `try:` |
| 28665 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28680 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 28681 | try | 4 | if |  | no | no | `try:` |
| 28700 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 28738 | continue | 3 | if |  | no | no | `continue` |
| 28756 | try | 2 | try |  | no | no | `try:` |
| 28758 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 28768 | try | 2 | try |  | no | no | `try:` |
| 28788 | if | 3 | try |  | no | no | `if _survival_table:` |
| 28791 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 28799 | try | 2 | try |  | no | no | `try:` |
| 28840 | if | 3 | try |  | no | no | `if not _recon.in_agreement:` |
| 28847 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 28858 | if | 2 | try |  | no | no | `if _apply_skip.skip:` |
| 28859 | if | 3 | if |  | no | no | `if _apply_skip.reason_code == "no_applied_patches":` |
| 28891 | try | 4 | if |  | no | no | `try:` |
| 28898 | for | 5 | try |  | no | no | `for _cid in (ag.get("source_cluster_ids") or []):` |
| 28902 | if | 6 | for |  | no | no | `if not _doa_root_cause:` |
| 28906 | if | 6 | for |  | no | no | `if not _doa_rca_id:` |
| 28910 | if | 6 | for |  | no | no | `if _doa_root_cause and _doa_rca_id:` |
| 28911 | break | 7 | if |  | no | no | `break` |
| 28930 | except_handler | 4 | if |  | no | no | `except Exception as _dead_on_arrival_exc:` |
| 28931 | try | 5 | except_handler |  | no | no | `try:` |
| 28935 | if | 6 | try |  | no | no | `if _typed_on():` |
| 28949 | except_handler | 5 | except_handler |  | no | no | `except Exception:` |
| 28965 | if | 5 | except_handler |  | no | no | `if is_strict_mode():` |
| 28966 | raise | 6 | if |  | no | no | `raise` |
| 28988 | if | 4 | if |  | no | no | `if not pending_action_groups:` |
| 28992 | try | 4 | if |  | no | no | `try:` |
| 29056 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 29062 | try | 4 | if |  | no | no | `try:` |
| 29063 | for | 5 | try |  | no | no | `for _lk in lever_keys:` |
| 29069 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 29086 | try | 3 | if |  | no | no | `try:` |
| 29093 | if | 4 | try |  | no | no | `if _decision_counts:` |
| 29108 | try | 4 | try |  | no | no | `try:` |
| 29110 | if | 5 | try |  | no | no | `if _decision_counts and _mlflow_apl.active_run() is not None:` |
| 29130 | except_handler | 4 | try |  | no | no | `except Exception:` |
| 29136 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29192 | try | 3 | if |  | no | no | `try:` |
| 29196 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29204 | try | 3 | if |  | no | no | `try:` |
| 29266 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29272 | try | 3 | if |  | no | no | `try:` |
| 29273 | for | 4 | try |  | no | no | `for _lk in lever_keys:` |
| 29279 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29296 | try | 3 | if |  | no | no | `try:` |
| 29318 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29333 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 29334 | try | 4 | if |  | no | no | `try:` |
| 29353 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 29360 | continue | 3 | if |  | no | no | `continue` |
| 29363 | for | 2 | try |  | no | no | `for idx, entry in enumerate(apply_log.get("applied", [])):` |
| 29377 | try | 3 | for |  | no | no | `try:` |
| 29400 | if | 4 | try |  | no | no | `if not _ap_target_qids:` |
| 29415 | if | 4 | try |  | no | no | `if _ap_target_qid_set:` |
| 29423 | if | 4 | try |  | no | no | `if _ap_broad_qid_set:` |
| 29431 | except_handler | 3 | for |  | no | no | `except Exception:` |
| 29456 | try | 2 | try |  | no | no | `try:` |
| 29463 | if | 3 | try |  | no | no | `if _chunk_c_on_f7():` |
| 29518 | except_handler | 2 | try |  | no | no | `except Exception as _patch_applied_exc:` |
| 29519 | try | 3 | except_handler |  | no | no | `try:` |
| 29523 | if | 4 | try |  | no | no | `if _typed_on():` |
| 29537 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 29550 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 29551 | raise | 4 | if |  | no | no | `raise` |
| 29554 | if | 2 | try |  | no | no | `if _queued:` |
| 29557 | for | 3 | if |  | no | no | `for qentry in _queued:` |
| 29582 | for | 3 | if |  | no | no | `for qi, qe in enumerate(_queued, 1):` |
| 29590 | if | 2 | try |  | no | no | `if not apply_log.get("patch_deployed", False) and apply_log.get("applied"):` |
| 29633 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 29634 | try | 4 | if |  | no | no | `try:` |
| 29653 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 29659 | if | 3 | if |  | no | no | `if _pe_class == RollbackClass.SCHEMA_FAILURE:` |
| 29674 | break | 4 | if |  | no | no | `break` |
| 29682 | if | 3 | if |  | no | no | `if _pe_class == RollbackClass.INFRA_FAILURE:` |
| 29684 | for | 4 | if |  | no | no | `for _rb_entry in reversed(reflection_buffer):` |
| 29685 | if | 5 | for |  | no | no | `if _rb_entry.get("rollback_class") == RollbackClass.INFRA_FAILURE.value:` |
| 29688 | break | 6 | if |  | no | no | `break` |
| 29689 | if | 4 | if |  | no | no | `if _consecutive_infra >= INFRA_RETRY_BUDGET:` |
| 29707 | break | 5 | if |  | no | no | `break` |
| 29709 | try | 3 | if |  | no | no | `try:` |
| 29731 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29741 | continue | 3 | if |  | no | no | `continue` |
| 29745 | if | 2 | try |  | no | no | `if _applied:` |
| 29747 | for | 3 | if |  | no | no | `for ai, aentry in enumerate(_applied, 1):` |
| 29755 | if | 2 | try |  | no | no | `if _dropped:` |
| 29757 | for | 3 | if |  | no | no | `for di, dp in enumerate(_dropped, 1):` |
| 29834 | try | 2 | try |  | no | no | `try:` |
| 29836 | if | 3 | try |  | no | no | `if bool(_gr.get("passed")) or str(` |
| 29844 | if | 4 | if |  | no | no | `if bool(_gr.get("passed")):` |
| 29848 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 29859 | try | 2 | try |  | no | no | `try:` |
| 29861 | if | 3 | try |  | no | no | `if (` |
| 29868 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 29886 | if | 2 | try |  | no | no | `if _gate_eval:` |
| 29896 | try | 3 | if |  | no | no | `try:` |
| 29898 | if | 4 | try |  | no | no | `if _backfill_rows and not _current_iter_inputs.get("eval_rows"):` |
| 29900 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29913 | try | 2 | try |  | no | no | `try:` |
| 29916 | if | 3 | try |  | no | no | `if _t4_verdict_for_persist is not None:` |
| 29921 | for | 4 | if |  | no | no | `for _c in (strategy.get("_source_clusters") or []) if strategy else []:` |
| 29923 | if | 5 | for |  | no | no | `if not _cid:` |
| 29924 | continue | 6 | if |  | no | no | `continue` |
| 29925 | for | 5 | for |  | no | no | `for _q in _c.get("question_ids") or []:` |
| 29928 | for | 4 | if |  | no | no | `for _p in (all_proposals or []):` |
| 29930 | if | 5 | for |  | no | no | `if not _pid:` |
| 29931 | continue | 6 | if |  | no | no | `continue` |
| 29932 | for | 5 | for |  | no | no | `for _q in _p.get("target_qids") or []:` |
| 29936 | for | 4 | if |  | no | no | `for _entry in _applied_patch_entries:` |
| 29944 | if | 5 | for |  | no | no | `if _ap_pid:` |
| 29956 | if | 4 | if |  | no | no | `if _t4_rows:` |
| 29963 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30006 | try | 2 | try |  | no | no | `try:` |
| 30036 | for | 3 | try |  | no | no | `for _entry in (apply_log.get("applied") or []):` |
| 30039 | if | 4 | for |  | no | no | `if _entry_ag:` |
| 30084 | try | 3 | try |  | no | no | `try:` |
| 30093 | if | 4 | try |  | no | no | `if _ag_id_for_canonical and _typed_canonical is not None:` |
| 30097 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 30104 | try | 3 | try |  | no | no | `try:` |
| 30132 | except_handler | 3 | try |  | no | no | `except Exception as _accept_inp_exc:` |
| 30137 | try | 4 | except_handler |  | no | no | `try:` |
| 30146 | except_handler | 4 | except_handler |  | no | no | `except Exception:` |
| 30152 | raise | 4 | except_handler |  | no | no | `raise` |
| 30175 | try | 3 | try |  | no | no | `try:` |
| 30208 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 30225 | try | 3 | try |  | no | no | `try:` |
| 30315 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 30321 | except_handler | 2 | try |  | no | no | `except Exception as _accept_stage_exc:` |
| 30322 | try | 3 | except_handler |  | no | no | `try:` |
| 30326 | if | 4 | try |  | no | no | `if _typed_on():` |
| 30340 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 30355 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 30356 | raise | 4 | if |  | no | no | `raise` |
| 30363 | if | 2 | try |  | no | no | `if not gate_result.get("passed"):` |
| 30367 | try | 3 | if |  | no | no | `try:` |
| 30375 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30386 | try | 3 | if |  | no | no | `try:` |
| 30396 | if | 4 | try |  | no | no | `if not _restore_decision.get("verified", True):` |
| 30425 | raise | 5 | if |  | no | no | `raise FailedRollbackVerification(` |
| 30428 | except_handler | 3 | if |  | no | no | `except FailedRollbackVerification:` |
| 30429 | raise | 4 | except_handler |  | no | no | `raise` |
| 30430 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30446 | try | 3 | if |  | no | no | `try:` |
| 30464 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30475 | if | 3 | if |  | no | no | `if pending_action_groups:` |
| 30482 | for | 3 | if |  | no | no | `for lk in lever_keys:` |
| 30504 | for | 3 | if |  | no | no | `for qid, tid in _fail_tmap.items():` |
| 30506 | if | 4 | for |  | no | no | `if qid in _fail_qids:` |
| 30508 | if | 5 | if |  | no | no | `elif "regressions" in gate_result:` |
| 30511 | if | 3 | if |  | no | no | `if _fail_run_id:` |
| 30531 | for | 3 | if |  | no | no | `for _r in _regressions:` |
| 30532 | if | 4 | for |  | no | no | `if _r.get("judge") == "control_plane_acceptance":` |
| 30536 | break | 5 | if |  | no | no | `break` |
| 30590 | try | 3 | if |  | no | no | `try:` |
| 30592 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30595 | for | 3 | if |  | no | no | `for _qid, _cand_row in _candidate_by_qid_for_delta.items():` |
| 30597 | if | 4 | for |  | no | no | `if not _gt_sql:` |
| 30598 | continue | 5 | if |  | no | no | `continue` |
| 30600 | try | 4 | for |  | no | no | `try:` |
| 30609 | except_handler | 4 | for |  | no | no | `except Exception:` |
| 30610 | continue | 5 | except_handler |  | no | no | `continue` |
| 30611 | if | 4 | for |  | no | no | `if _delta.get("improved") or _delta.get("remaining"):` |
| 30654 | try | 3 | if |  | no | no | `try:` |
| 30660 | for | 4 | try |  | no | no | `for _r in gate_result.get("regressions") or []:` |
| 30661 | for | 5 | for |  | no | no | `for _q in _r.get("blocking_qids") or []:` |
| 30662 | if | 6 | for |  | no | no | `if _q:` |
| 30667 | if | 4 | try |  | no | no | `if not _regressed_qids and prev_failure_qids is not None:` |
| 30681 | if | 4 | try |  | no | no | `if _mined_insights:` |
| 30692 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30699 | try | 3 | if |  | no | no | `try:` |
| 30704 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30712 | if | 3 | if |  | no | no | `if _mined_insights:` |
| 30713 | try | 4 | if |  | no | no | `try:` |
| 30729 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 30735 | for | 3 | if |  | no | no | `for p in patches:` |
| 30745 | if | 4 | for |  | no | no | `if ft and tgt:` |
| 30763 | for | 3 | if |  | no | no | `for c in clusters:` |
| 30765 | if | 4 | for |  | no | no | `if source_cids and cid not in source_cids:` |
| 30766 | continue | 5 | if |  | no | no | `continue` |
| 30769 | if | 4 | for |  | no | no | `if not rc_ft or not _should_mark_tried_lever_aware:` |
| 30770 | continue | 5 | if |  | no | no | `continue` |
| 30788 | if | 4 | for |  | no | no | `if len(_distinct_lever_sets) >= 2:` |
| 30790 | if | 3 | if |  | no | no | `if not _should_mark_tried_lever_aware:` |
| 30795 | try | 3 | if |  | no | no | `try:` |
| 30817 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30840 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 30841 | try | 4 | if |  | no | no | `try:` |
| 30860 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 30900 | continue | 3 | if |  | no | no | `continue` |
| 30904 | for | 2 | try |  | no | no | `for lk in lever_keys:` |
| 30910 | try | 2 | try |  | no | no | `try:` |
| 30924 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30929 | try | 2 | try |  | no | no | `try:` |
| 30931 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30948 | try | 2 | try |  | no | no | `try:` |
| 30982 | except_handler | 2 | try |  | no | no | `except Exception as _observed_effect_exc:` |
| 30983 | try | 3 | except_handler |  | no | no | `try:` |
| 30987 | if | 4 | try |  | no | no | `if _typed_on():` |
| 31001 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 31013 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 31014 | raise | 4 | if |  | no | no | `raise` |
| 31031 | for | 2 | try |  | no | no | `for qid, tid in _full_trace_map.items():` |
| 31033 | if | 3 | for |  | no | no | `if qid in _full_failures:` |
| 31036 | if | 2 | try |  | no | no | `if _full_run_id:` |
| 31039 | try | 2 | try |  | no | no | `try:` |
| 31042 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31045 | try | 2 | try |  | no | no | `try:` |
| 31047 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31050 | try | 2 | try |  | no | no | `try:` |
| 31054 | if | 3 | try |  | no | no | `if _persist_data:` |
| 31056 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31086 | try | 2 | try |  | no | no | `try:` |
| 31088 | if | 3 | try |  | no | no | `if (` |
| 31097 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31132 | try | 2 | try |  | no | no | `try:` |
| 31137 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31141 | if | 2 | try |  | no | no | `if _acc_delta >= 1.0:` |
| 31182 | if | 2 | try |  | no | no | `if _regression_debt_qids_for_next_iteration:` |
| 31198 | if | 2 | try |  | no | no | `if new_refs:` |
| 31201 | if | 2 | try |  | no | no | `if new_hashes:` |
| 31214 | if | 2 | try |  | no | no | `if post_instructions:` |
| 31239 | if | 2 | try |  | no | no | `if _original_instruction_sections:` |
| 31247 | try | 2 | try |  | no | no | `try:` |
| 31259 | if | 3 | try |  | no | no | `if not _diag_rows and isinstance(full_result, dict):` |
| 31261 | if | 4 | if |  | no | no | `if isinstance(_rows_json, list):` |
| 31263 | if | 5 | if |  | no | no | `elif isinstance(_rows_json, str):` |
| 31264 | try | 6 | if |  | no | no | `try:` |
| 31267 | except_handler | 6 | if |  | no | no | `except (ValueError, TypeError):` |
| 31269 | for | 3 | try |  | no | no | `for _r in _diag_rows:` |
| 31270 | if | 4 | for |  | no | no | `if not isinstance(_r, dict):` |
| 31271 | continue | 5 | if |  | no | no | `continue` |
| 31272 | for | 4 | for |  | no | no | `for _log in (_r.get("_asi_extraction_log") or []):` |
| 31273 | if | 5 | for |  | no | no | `if not isinstance(_log, dict):` |
| 31274 | continue | 6 | if |  | no | no | `continue` |
| 31278 | if | 3 | try |  | no | no | `if _asi_total:` |
| 31289 | if | 4 | if |  | no | no | `if _none_pct > 50.0:` |
| 31325 | if | 3 | try |  | no | no | `if _pre_arb is not None:` |
| 31329 | if | 3 | try |  | no | no | `if _adj is not None:` |
| 31337 | if | 3 | try |  | no | no | `if isinstance(_bcr, (int, float)) and _bcr is not None:` |
| 31350 | if | 4 | if |  | no | no | `if _rescue > 0.30:` |
| 31359 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31366 | try | 2 | try |  | no | no | `try:` |
| 31368 | if | 3 | try |  | no | no | `if not _eval_rows:` |
| 31370 | if | 4 | if |  | no | no | `if isinstance(_eval_rows_json, str):` |
| 31372 | try | 5 | if |  | no | no | `try:` |
| 31374 | except_handler | 5 | if |  | no | no | `except (ValueError, TypeError):` |
| 31376 | if | 5 | if |  | no | no | `elif isinstance(_eval_rows_json, list):` |
| 31378 | if | 3 | try |  | no | no | `if _eval_rows:` |
| 31383 | if | 4 | if |  | no | no | `if _mine_result.get("total_applied", 0) > 0:` |
| 31387 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31397 | try | 2 | try |  | no | no | `try:` |
| 31421 | try | 3 | try |  | no | no | `try:` |
| 31425 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 31430 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31454 | try | 2 | try |  | no | no | `try:` |
| 31464 | if | 3 | try |  | no | no | `if _journey_report is not None:` |
| 31468 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31478 | if | 2 | try |  | no | no | `if _journey_report is not None:` |
| 31487 | if | 3 | if |  | no | no | `if not _phase_a_result.success:` |
| 31508 | try | 2 | try |  | no | no | `try:` |
| 31521 | if | 3 | try |  | no | no | `if _decision_records:` |
| 31539 | try | 4 | if |  | no | no | `try:` |
| 31548 | for | 5 | try |  | no | no | `for _r in _decision_records:` |
| 31550 | if | 6 | for |  | no | no | `if not _qid or _qid in _qids_seen:` |
| 31551 | continue | 7 | if |  | no | no | `continue` |
| 31552 | if | 6 | for |  | no | no | `if getattr(_r, "outcome", None) != _DecisionOutcome.UNRESOLVED:` |
| 31553 | continue | 7 | if |  | no | no | `continue` |
| 31558 | if | 6 | for |  | no | no | `if _classification.bucket is not None:` |
| 31561 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31572 | try | 4 | if |  | no | no | `try:` |
| 31576 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31615 | if | 4 | if |  | no | no | `if not _phase_b_result.success:` |
| 31632 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31637 | try | 3 | except_handler |  | no | no | `try:` |
| 31639 | if | 4 | try |  | no | no | `if _mlflow_phase_b_partial.active_run() is not None:` |
| 31643 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 31652 | try | 2 | try |  | no | no | `try:` |
| 31671 | if | 3 | try |  | no | no | `if _iter_record_count == 0:` |
| 31688 | try | 4 | if |  | no | no | `try:` |
| 31690 | if | 5 | try |  | no | no | `if _mlflow_no_rec.active_run() is not None:` |
| 31700 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31711 | try | 4 | if |  | no | no | `try:` |
| 31721 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31723 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31733 | try | 2 | try |  | no | no | `try:` |
| 31756 | except_handler | 2 | try |  | no | no | `except Exception as _orphan_rca_exc:` |
| 31757 | try | 3 | except_handler |  | no | no | `try:` |
| 31761 | if | 4 | try |  | no | no | `if _typed_on():` |
| 31775 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 31787 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 31788 | raise | 4 | if |  | no | no | `raise` |
| 31794 | try | 2 | try |  | no | no | `try:` |
| 31816 | if | 3 | try |  | no | no | `if _w1_count:` |
| 31821 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31838 | try | 2 | try |  | no | no | `try:` |
| 31842 | if | 3 | try |  | no | no | `if productive_iteration_budget_enabled():` |
| 31849 | if | 4 | if |  | no | no | `if _iter_applied_count == 0:` |
| 31857 | if | 5 | if |  | no | no | `if _iter_no_op_cause:` |
| 31872 | if | 4 | if |  | no | no | `if _iter_budget_key not in _iter_emitted_keys:` |
| 31891 | if | 5 | if |  | no | no | `if not _iter_consumed:` |
| 31893 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31902 | try | 2 | try |  | no | no | `try:` |
| 31921 | try | 3 | try |  | no | no | `try:` |
| 31940 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 31959 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31967 | try | 2 | try |  | no | no | `try:` |
| 31969 | try | 3 | try |  | no | no | `try:` |
| 31975 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 32000 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 32012 | try | 2 | try |  | no | no | `try:` |
| 32018 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 32046 | finally | 1 | for |  | YES | YES | `_f_cur = locals().get("_current_iter_inputs")` |
| 32047 | if | 2 | finally |  | YES | YES | `if isinstance(_f_cur, dict) and not _f_cur.get(` |
| 32050 | try | 3 | if |  | YES | YES | `try:` |
| 32080 | except_handler | 3 | if |  | YES | YES | `except Exception:` |
| 32098 | if | 2 | finally |  | YES | YES | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 32099 | try | 3 | if |  | YES | YES | `try:` |
| 32103 | if | 4 | try |  | YES | YES | `if _exc_val is not None:` |
| 32123 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 32144 | try | 2 | finally |  | YES | YES | `try:` |
| 32161 | if | 3 | try |  | YES | YES | `if iteration_terminal_policy_enabled():` |
| 32162 | try | 4 | if |  | YES | YES | `try:` |
| 32166 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 32173 | if | 4 | if |  | YES | YES | `if str(_iter_terminal_reason or "") != "accepted":` |
| 32205 | if | 5 | if |  | YES | YES | `if _router_action.add_to_forbidden_set:` |
| 32234 | if | 5 | if |  | YES | YES | `if _abort_break:` |
| 32248 | except_handler | 2 | finally |  | no | no | `except Exception:` |
| 32265 | try | 2 | finally |  | YES | YES | `try:` |
| 32274 | if | 3 | try |  | YES | YES | `if candidate_ledger_enabled():` |
| 32322 | try | 4 | if |  | YES | YES | `try:` |
| 32326 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 32336 | except_handler | 2 | finally |  | no | no | `except Exception:` |
| 32350 | if | 2 | finally |  | YES | YES | `if _loop_should_abort:` |
| 32363 | break | 3 | if |  | no | no | `break` |
| 32374 | try | 0 | module |  | YES | YES | `try:` |
| 32376 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 32382 | if | 0 | module |  | YES | YES | `if _phase35_drained:` |
| 32384 | for | 1 | if |  | no | no | `for _snap in _replay_fixture_iterations:` |
| 32385 | try | 2 | for |  | no | no | `try:` |
| 32387 | except_handler | 2 | for |  | no | no | `except Exception:` |
| 32388 | continue | 3 | except_handler |  | no | no | `continue` |
| 32389 | for | 1 | if |  | no | no | `for _call in _phase35_drained:` |
| 32390 | try | 2 | for |  | no | no | `try:` |
| 32392 | except_handler | 2 | for |  | no | no | `except Exception:` |
| 32395 | if | 2 | for |  | no | no | `if _snap is not None:` |
| 32397 | try | 0 | module |  | YES | YES | `try:` |
| 32399 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 32457 | for | 0 | module |  | YES | YES | `for _rb_entry in reflection_buffer:` |
| 32458 | if | 1 | for |  | YES | YES | `if _rb_entry.get("accepted"):` |
| 32459 | continue | 2 | if |  | no | no | `continue` |
| 32460 | if | 1 | for |  | YES | YES | `if _rb_entry.get("escalation_handled"):` |
| 32461 | continue | 2 | if |  | no | no | `continue` |
| 32463 | if | 0 | module |  | YES | YES | `if len(ags_rolled_back) and _rb_class_counter:` |
| 32474 | if | 0 | module |  | YES | YES | `if lever_changes:` |
| 32476 | for | 1 | if |  | no | no | `for lc in lever_changes:` |
| 32480 | for | 2 | for |  | no | no | `for p in lc.get("patches", []):` |
| 32484 | if | 1 | if |  | YES | YES | `elif not ags_accepted:` |
| 32489 | for | 0 | module |  | YES | YES | `for sname, sval in sorted(best_scores.items()):` |
| 32499 | try | 0 | module |  | YES | YES | `try:` |
| 32523 | try | 1 | try |  | YES | YES | `try:` |
| 32542 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32554 | try | 1 | try |  | YES | YES | `try:` |
| 32565 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32579 | try | 1 | try |  | YES | YES | `try:` |
| 32586 | if | 2 | try |  | YES | YES | `if _replay_fixture_summary is not None:` |
| 32590 | for | 3 | if |  | YES | YES | `for _per in (` |
| 32593 | if | 4 | for |  | YES | YES | `if int(_per.get("eval_rows") or 0) == 0:` |
| 32604 | if | 2 | try |  | YES | YES | `if _is_empty:` |
| 32611 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32621 | if | 1 | try |  | YES | YES | `if _dual_emit_on():` |
| 32625 | try | 2 | if |  | YES | YES | `try:` |
| 32638 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 32671 | try | 1 | try |  | YES | YES | `try:` |
| 32674 | if | 2 | try |  | YES | YES | `if mlflow.active_run() is not None:` |
| 32683 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32688 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 32700 | try | 0 | module |  | YES | YES | `try:` |
| 32727 | if | 1 | try |  | YES | YES | `if gso_run_manifest_v2_enabled():` |
| 32728 | try | 2 | if |  | YES | YES | `try:` |
| 32738 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 32740 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 32759 | try | 0 | module |  | YES | YES | `try:` |
| 32771 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 32797 | try | 0 | module |  | YES | YES | `try:` |
| 32861 | try | 1 | try |  | YES | YES | `try:` |
| 32863 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32883 | try | 1 | try |  | YES | YES | `try:` |
| 32888 | except_handler | 1 | try |  | YES | YES | `except (NameError, AttributeError):` |
| 32926 | for | 1 | try |  | YES | YES | `for _i in _phase_h_iterations_completed:` |
| 32929 | if | 2 | for |  | YES | YES | `if _trace is None:` |
| 32934 | if | 3 | if |  | YES | YES | `if not _summary:` |
| 32964 | try | 1 | try |  | YES | YES | `try:` |
| 32968 | if | 2 | try |  | YES | YES | `if _trend_enabled():` |
| 32994 | try | 3 | if |  | YES | YES | `try:` |
| 32997 | if | 4 | try |  | YES | YES | `if _by_root:` |
| 33016 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 33021 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 33043 | if | 1 | try |  | YES | YES | `if _phase_h_anchor_run_id:` |
| 33044 | try | 2 | if |  | no | no | `try:` |
| 33151 | try | 3 | try |  | no | no | `try:` |
| 33170 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 33188 | try | 3 | try |  | no | no | `try:` |
| 33192 | if | 4 | try |  | no | no | `if _ledger_enabled_phase_h():` |
| 33200 | if | 5 | if |  | no | no | `if _os_for_ledger_copy.path.exists(_ledger_src):` |
| 33206 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 33225 | try | 3 | try |  | no | no | `try:` |
| 33229 | if | 4 | try |  | no | no | `if _phase_h_totality_enabled():` |
| 33267 | if | 5 | if |  | no | no | `if _totality_violation is not None:` |
| 33280 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 33341 | try | 3 | try |  | no | no | `try:` |
| 33375 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 33399 | for | 3 | try |  | no | no | `for _k, _v in _paths.items():` |
| 33400 | if | 4 | for |  | no | no | `if _k == "iterations":` |
| 33401 | for | 5 | if |  | no | no | `for _iter_paths in (_v or {}).values():` |
| 33402 | for | 6 | for |  | no | no | `for _path in (_iter_paths or {}).values():` |
| 33403 | if | 7 | for |  | no | no | `if isinstance(_path, str):` |
| 33405 | if | 5 | if |  | no | no | `elif isinstance(_v, str):` |
| 33410 | try | 3 | try |  | no | no | `try:` |
| 33420 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 33430 | if | 3 | try |  | no | no | `if not _completeness["complete"]:` |
| 33431 | try | 4 | if |  | no | no | `try:` |
| 33449 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 33456 | except_handler | 2 | if |  | no | no | `except Exception as _phase_h_upload_exc:` |
| 33480 | except_handler | 0 | module |  | no | no | `except Exception as _phase_h_render_exc:` |
| 33498 | if | 1 | except_handler |  | no | no | `if "_bundle_assembly_failed_payloads" not in locals():` |
| 33501 | if | 1 | except_handler |  | no | no | `if "_bundle_assembly_incomplete_payloads" not in locals():` |
| 33527 | try | 0 | module |  | YES | YES | `try:` |
| 33528 | for | 1 | try |  | YES | YES | `for _it_idx, _it_trace in (` |
| 33531 | for | 2 | for |  | YES | YES | `for _rec in getattr(_it_trace, "decision_records", ()) or ():` |
| 33537 | if | 3 | for |  | no | no | `if _rec_dict is None:` |
| 33538 | continue | 4 | if |  | no | no | `continue` |
| 33539 | if | 3 | for |  | no | no | `if str(_rec_dict.get("reason_code") or "") == (` |
| 33543 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 33548 | try | 0 | module |  | YES | YES | `try:` |
| 33566 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 33631 | try | 0 | module |  | YES | YES | `try:` |
| 33634 | if | 1 | try |  | YES | YES | `if any(count > 0 for count in _narrowing_summary["hits"].values()):` |
| 33643 | except_handler | 0 | module |  | no | no | `except Exception as _narrowing_log_exc:` |
| 33651 | try | 0 | module |  | YES | YES | `try:` |
| 33654 | if | 1 | try |  | YES | YES | `if any(c > 0 for c in _l5_summary["hits"].values()) or _l5_summary["shadow_compa` |
| 33664 | except_handler | 0 | module |  | no | no | `except Exception as _l5_log_exc:` |
| 33670 | try | 0 | module |  | YES | YES | `try:` |
| 33673 | if | 1 | try |  | YES | YES | `if (_ts_summary["discovery_calls"] > 0` |
| 33686 | except_handler | 0 | module |  | no | no | `except Exception as _ts_log_exc:` |
| 33692 | try | 0 | module |  | YES | YES | `try:` |
| 33695 | if | 1 | try |  | YES | YES | `if (_re_summary["shadow_comparisons"] > 0` |
| 33706 | except_handler | 0 | module |  | no | no | `except Exception as _re_log_exc:` |
| 33730 | try | 0 | module |  | YES | YES | `try:` |
| 33751 | except_handler | 0 | module |  | no | no | `except Exception as _upload_exc:` |
| 33752 | try | 1 | except_handler |  | no | no | `try:` |
| 33758 | except_handler | 1 | except_handler |  | no | no | `except Exception:` |
| 33765 | return | 0 | module |  | YES | YES | `return _build_loop_out_with_pretty_print(` |
