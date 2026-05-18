# Harness Control-Flow Audit

Function: `_run_lever_loop`  
Line range: 17528–33369  
Total branch points: 1397  

## Reachability summary

* `airline_run_59a173d3`: 4316 lines executed
* `seven_now_run_ab65fefe`: 4373 lines executed

## Branch points

| lineno | type | depth | parent | detail | reached:airline_run_59a173d3 | reached:seven_now_run_ab65fefe | snippet |
|---|---|---|---|---|---|---|---|
| 17614 | try | 0 | module |  | YES | YES | `try:` |
| 17628 | if | 1 | try |  | YES | YES | `if _chunk_d_enabled_rm():` |
| 17636 | try | 2 | if |  | no | no | `try:` |
| 17640 | try | 3 | try |  | no | no | `try:` |
| 17644 | for | 4 | try |  | no | no | `for _rm_tag_key in ("jobId", "multitaskParentRunId", "jobRunId", "runId"):` |
| 17645 | try | 5 | for |  | no | no | `try:` |
| 17647 | if | 6 | try |  | no | no | `if _rm_val.isDefined():` |
| 17649 | except_handler | 5 | for |  | no | no | `except Exception:` |
| 17651 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 17653 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 17687 | try | 2 | if |  | no | no | `try:` |
| 17689 | if | 3 | try |  | no | no | `if _rm_active_run is not None:` |
| 17691 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 17726 | try | 2 | if |  | no | no | `try:` |
| 17748 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 17752 | try | 2 | if |  | YES | YES | `try:` |
| 17754 | if | 3 | try |  | no | no | `if _legacy_active_run is not None:` |
| 17756 | except_handler | 2 | if |  | YES | YES | `except Exception:` |
| 17766 | try | 1 | try |  | YES | YES | `try:` |
| 17768 | except_handler | 1 | try |  | YES | YES | `except NameError:` |
| 17779 | if | 1 | try |  | YES | YES | `if gso_run_manifest_v2_enabled():` |
| 17780 | try | 2 | if |  | YES | YES | `try:` |
| 17790 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 17792 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 17794 | try | 0 | module |  | YES | YES | `try:` |
| 17796 | if | 1 | try |  | YES | YES | `if _mlflow_run_analysis.active_run() is not None:` |
| 17803 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 17820 | try | 0 | module |  | YES | YES | `try:` |
| 17831 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 17836 | if | 0 | module |  | YES | YES | `if not _phase_h_anchor_run_id:` |
| 17837 | try | 1 | if |  | YES | YES | `try:` |
| 17840 | if | 2 | try |  | YES | YES | `if _active_phase_h is not None:` |
| 17842 | except_handler | 1 | if |  | no | no | `except Exception:` |
| 17955 | if | 0 | module |  | YES | YES | `if resume_state.get("prev_scores"):` |
| 17957 | if | 0 | module |  | YES | YES | `if resume_state.get("prev_model_id"):` |
| 17959 | if | 0 | module |  | YES | YES | `if resume_state.get("prev_accuracy"):` |
| 17970 | if | 0 | module |  | YES | YES | `if "_pre_arbiter/overall_accuracy" not in best_scores:` |
| 17971 | if | 1 | if |  | YES | YES | `if "_pre_arbiter/result_correctness" in best_scores:` |
| 18002 | try | 0 | module |  | YES | YES | `try:` |
| 18023 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18048 | try | 0 | module |  | YES | YES | `try:` |
| 18050 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18052 | if | 0 | module |  | YES | YES | `if not _run_row.get("config_snapshot"):` |
| 18061 | try | 1 | if |  | YES | YES | `try:` |
| 18068 | except_handler | 1 | if |  | no | no | `except Exception:` |
| 18161 | try | 0 | module |  | YES | YES | `try:` |
| 18163 | if | 1 | try |  | YES | YES | `if _mlflow_phase_b_init.active_run() is not None:` |
| 18167 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18180 | for | 0 | module |  | YES | YES | `for ov in _judge_overrides:` |
| 18181 | try | 1 | for |  | no | no | `try:` |
| 18183 | if | 2 | try |  | no | no | `if "Genie answer is actually fine" in feedback or "Correct" in feedback:` |
| 18185 | if | 3 | if |  | no | no | `if genie_sql:` |
| 18191 | if | 3 | if |  | no | no | `elif "both answers are wrong" in feedback or "Both Wrong" in feedback:` |
| 18198 | if | 4 | if |  | no | no | `elif "Ambiguous" in feedback:` |
| 18205 | except_handler | 1 | for |  | no | no | `except Exception:` |
| 18208 | if | 0 | module |  | YES | YES | `if _human_sql_fixes:` |
| 18209 | try | 1 | if |  | no | no | `try:` |
| 18216 | except_handler | 1 | if |  | no | no | `except Exception:` |
| 18232 | try | 0 | module |  | YES | YES | `try:` |
| 18240 | if | 1 | try |  | YES | YES | `if _snippet_repair_result.get("rewritten", 0) > 0:` |
| 18243 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18275 | if | 0 | module |  | YES | YES | `if uc_columns:` |
| 18283 | if | 0 | module |  | YES | YES | `if not enrichment_done:` |
| 18293 | with | 1 | if |  | YES | YES | `with _mlflow_legacy_enr.start_run(run_name=_legacy_enr_run_name, nested=True):` |
| 18309 | if | 2 | with |  | YES | YES | `if enrichment_result.get("total_enriched", 0) > 0 or enrichment_result.get("tabl` |
| 18315 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18321 | if | 2 | with |  | YES | YES | `if join_result.get("total_applied", 0) > 0:` |
| 18327 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18333 | if | 2 | with |  | YES | YES | `if meta_result.get("description_generated") or meta_result.get("questions_genera` |
| 18339 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18353 | if | 2 | with |  | YES | YES | `if _legacy_miner_out["total_applied"] or _legacy_miner_out["keep_in_prose_count"` |
| 18359 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18365 | if | 2 | with |  | YES | YES | `if (` |
| 18374 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18379 | if | 2 | with |  | YES | YES | `if ENABLE_PREFLIGHT_EXAMPLE_SQL_SYNTHESIS:` |
| 18380 | try | 3 | if |  | YES | YES | `try:` |
| 18391 | if | 4 | try |  | YES | YES | `if legacy_preflight_result.get("applied", 0) > 0:` |
| 18397 | if | 5 | if |  | no | no | `if uc_columns:` |
| 18399 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 18425 | if | 2 | with |  | no | no | `if (` |
| 18434 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18469 | if | 1 | if |  | no | no | `if uc_columns:` |
| 18471 | if | 1 | if |  | no | no | `if enrichment_model_id:` |
| 18485 | try | 0 | module |  | YES | YES | `try:` |
| 18497 | if | 1 | try |  | YES | YES | `if _current_instr and _current_instr.strip() and _is_unstructured(_current_instr` |
| 18506 | if | 2 | if |  | no | no | `if _restructured_secs:` |
| 18509 | for | 3 | if |  | no | no | `for _sec in INSTRUCTION_SECTION_ORDER:` |
| 18511 | if | 4 | for |  | no | no | `if not _lines_list:` |
| 18512 | continue | 5 | if |  | no | no | `continue` |
| 18514 | for | 4 | for |  | no | no | `for _ln in _lines_list:` |
| 18516 | if | 5 | for |  | no | no | `if not _s:` |
| 18517 | continue | 6 | if |  | no | no | `continue` |
| 18518 | if | 5 | for |  | no | no | `if not _s.startswith("- "):` |
| 18524 | if | 3 | if |  | no | no | `if len(_restructured_text.strip()) >= len(_current_instr.strip()) * 0.5:` |
| 18526 | try | 4 | if |  | no | no | `try:` |
| 18540 | if | 5 | try |  | no | no | `if uc_columns:` |
| 18560 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 18580 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18588 | try | 0 | module |  | YES | YES | `try:` |
| 18592 | if | 1 | try |  | YES | YES | `if _pre_loop_instr and _pre_loop_instr.strip():` |
| 18602 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18612 | if | 0 | module |  | YES | YES | `if baseline_iter:` |
| 18614 | if | 1 | if |  | YES | YES | `if isinstance(rows_json, list):` |
| 18618 | if | 2 | if |  | no | no | `elif isinstance(rows_json, str):` |
| 18619 | try | 3 | if |  | no | no | `try:` |
| 18623 | except_handler | 3 | if |  | no | no | `except (json.JSONDecodeError, TypeError):` |
| 18640 | try | 0 | module |  | YES | YES | `try:` |
| 18645 | if | 1 | try |  | YES | YES | `if baseline_iter:` |
| 18648 | if | 2 | if |  | YES | YES | `if isinstance(_rj, list):` |
| 18650 | if | 3 | if |  | no | no | `elif isinstance(_rj, str):` |
| 18651 | try | 4 | if |  | no | no | `try:` |
| 18653 | except_handler | 4 | if |  | no | no | `except (json.JSONDecodeError, TypeError):` |
| 18655 | for | 2 | if |  | YES | YES | `for _row in _rj_rows or []:` |
| 18656 | if | 3 | for |  | YES | YES | `if not isinstance(_row, dict):` |
| 18657 | continue | 4 | if |  | no | no | `continue` |
| 18660 | if | 3 | for |  | YES | YES | `if _qid and _sql:` |
| 18662 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18837 | try | 0 | module |  | YES | YES | `try:` |
| 18855 | if | 1 | try |  | YES | YES | `if (` |
| 18859 | try | 2 | if |  | YES | YES | `try:` |
| 18880 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 18886 | if | 1 | try |  | YES | YES | `if not _baseline_rows_seed:` |
| 18894 | if | 1 | try |  | YES | YES | `if _seeded:` |
| 18902 | if | 2 | if |  | no | no | `elif _baseline_rows_seed:` |
| 18919 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18973 | if | 0 | module |  | YES | YES | `if baseline_iter:` |
| 18974 | try | 1 | if |  | YES | YES | `try:` |
| 18978 | except_handler | 1 | if |  | no | no | `except Exception:` |
| 19056 | for | 0 | module |  | YES | YES | `for _iter_num in range(1, max_iterations + 1):` |
| 19071 | if | 1 | for |  | YES | YES | `if not _was_collision_skip_this_iter:` |
| 19110 | try | 1 | for |  | YES | YES | `try:` |
| 19125 | if | 2 | try |  | YES | YES | `if reserved_recovery_budget_enabled():` |
| 19154 | if | 3 | if |  | YES | YES | `if _budget_action == RecoveryBudgetAction.SKIP_EARLY_TERMINATE:` |
| 19157 | if | 4 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 19158 | try | 5 | if |  | no | no | `try:` |
| 19168 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 19174 | break | 4 | if |  | no | no | `break` |
| 19181 | if | 2 | try |  | YES | YES | `if arbiter_objective_complete_from_counts(` |
| 19193 | break | 3 | if |  | no | no | `break` |
| 19194 | if | 2 | try |  | YES | YES | `if all_thresholds_met(best_scores, thresholds):` |
| 19208 | if | 2 | try |  | YES | YES | `if _prev_terminal_state:` |
| 19209 | try | 3 | if |  | YES | YES | `try:` |
| 19220 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 19225 | if | 2 | try |  | YES | YES | `if legacy_plateau_allows_stop(` |
| 19251 | try | 3 | if |  | no | no | `try:` |
| 19253 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 19293 | try | 3 | if |  | no | no | `try:` |
| 19305 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 19315 | for | 3 | if |  | no | no | `for _rb in reflection_buffer:` |
| 19316 | for | 4 | for |  | no | no | `for _delta in _rb.get("sql_shape_deltas", []) or []:` |
| 19318 | if | 5 | for |  | no | no | `if _qid and (_delta.get("remaining") or _delta.get("improved")):` |
| 19341 | if | 3 | if |  | no | no | `if _resolved.should_continue:` |
| 19348 | continue | 4 | if |  | no | no | `continue` |
| 19391 | try | 3 | if |  | no | no | `try:` |
| 19430 | try | 4 | try |  | no | no | `try:` |
| 19442 | except_handler | 4 | try |  | no | no | `except NameError:` |
| 19490 | except_handler | 3 | if |  | no | no | `except Exception as _learning_stage_exc:` |
| 19491 | try | 4 | except_handler |  | no | no | `try:` |
| 19495 | if | 5 | try |  | no | no | `if _typed_on():` |
| 19509 | except_handler | 4 | except_handler |  | no | no | `except Exception:` |
| 19523 | break | 3 | if |  | no | no | `break` |
| 19524 | if | 2 | try |  | YES | YES | `if (` |
| 19542 | if | 2 | try |  | YES | YES | `if _diverging:` |
| 19556 | break | 3 | if |  | no | no | `break` |
| 19565 | for | 2 | try |  | YES | YES | `for _rb_entry in reversed(reflection_buffer):` |
| 19566 | if | 3 | for |  | YES | YES | `if _rb_entry.get("escalation_handled"):` |
| 19567 | continue | 4 | if |  | no | no | `continue` |
| 19568 | if | 3 | for |  | YES | YES | `if _rb_entry.get("accepted"):` |
| 19569 | break | 4 | if |  | no | no | `break` |
| 19570 | if | 3 | for |  | YES | YES | `if _rb_entry.get("rollback_class") == _RC.CONTENT_REGRESSION.value:` |
| 19576 | continue | 4 | if |  | YES | YES | `continue` |
| 19577 | if | 2 | try |  | YES | YES | `if _consecutive_rb >= CONSECUTIVE_ROLLBACK_LIMIT:` |
| 19582 | break | 3 | if |  | no | no | `break` |
| 19586 | for | 2 | try |  | YES | YES | `for _esc_entry in reversed(reflection_buffer):` |
| 19587 | if | 3 | for |  | YES | YES | `if not _esc_entry.get("escalation_handled"):` |
| 19588 | break | 4 | if |  | YES | YES | `break` |
| 19590 | if | 3 | for |  | no | no | `if _last_esc_type is None:` |
| 19592 | if | 3 | for |  | no | no | `if _esc_reason == _last_esc_type:` |
| 19595 | break | 4 | if |  | no | no | `break` |
| 19596 | if | 2 | try |  | YES | YES | `if _consecutive_esc >= CONSECUTIVE_ESCALATION_LIMIT:` |
| 19611 | break | 3 | if |  | no | no | `break` |
| 19705 | if | 2 | try |  | YES | YES | `if _forced_synthesis_proposals_carryover:` |
| 19801 | try | 2 | try |  | YES | YES | `try:` |
| 19802 | if | 3 | try |  | YES | YES | `if reflection_buffer:` |
| 19815 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19830 | try | 2 | try |  | YES | YES | `try:` |
| 19832 | if | 3 | try |  | YES | YES | `if _rot_records:` |
| 19836 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19848 | try | 2 | try |  | YES | YES | `try:` |
| 19849 | for | 3 | try |  | YES | YES | `for _sc in soft_signal_clusters or []:` |
| 19851 | if | 4 | for |  | no | no | `if _scid and _scid not in _soft_clusters_seen_run:` |
| 19853 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19860 | try | 2 | try |  | YES | YES | `try:` |
| 19862 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19888 | if | 2 | try |  | YES | YES | `if not (_latest_eval_result or {}).get("question_ids"):` |
| 19889 | try | 3 | if |  | no | no | `try:` |
| 19893 | if | 4 | try |  | no | no | `if _lazy_seed:` |
| 19905 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 19917 | try | 2 | try |  | YES | YES | `try:` |
| 19951 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19971 | for | 2 | try |  | YES | YES | `for _q in _already_passing_set:` |
| 19973 | for | 2 | try |  | YES | YES | `for _q in _hard_qid_set:` |
| 19975 | for | 2 | try |  | YES | YES | `for _q in _soft_qid_set:` |
| 19977 | for | 2 | try |  | YES | YES | `for _q in _gt_corr_qid_set:` |
| 19981 | for | 2 | try |  | YES | YES | `for _c in (clusters or []):` |
| 19983 | if | 3 | for |  | YES | YES | `if _cid:` |
| 19985 | for | 3 | for |  | YES | YES | `for _q in (_c.get("question_ids") or []):` |
| 19987 | if | 4 | for |  | YES | YES | `if _qstr and _cid:` |
| 19998 | try | 2 | try |  | YES | YES | `try:` |
| 20011 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20081 | try | 2 | try |  | YES | YES | `try:` |
| 20097 | except_handler | 2 | try |  | no | no | `except Exception as _exc_eval:` |
| 20098 | try | 3 | except_handler |  | no | no | `try:` |
| 20102 | if | 4 | try |  | no | no | `if _typed_on():` |
| 20116 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 20129 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 20130 | raise | 4 | if |  | no | no | `raise` |
| 20136 | try | 2 | try |  | YES | YES | `try:` |
| 20169 | except_handler | 2 | try |  | no | no | `except Exception as _cluster_exc:` |
| 20170 | try | 3 | except_handler |  | no | no | `try:` |
| 20174 | if | 4 | try |  | no | no | `if _typed_on():` |
| 20188 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 20201 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 20202 | raise | 4 | if |  | no | no | `raise` |
| 20208 | try | 2 | try |  | YES | YES | `try:` |
| 20222 | except_handler | 2 | try |  | no | no | `except Exception as _rca_formed_exc:` |
| 20223 | try | 3 | except_handler |  | no | no | `try:` |
| 20227 | if | 4 | try |  | no | no | `if _typed_on():` |
| 20241 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 20254 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 20255 | raise | 4 | if |  | no | no | `raise` |
| 20259 | try | 2 | try |  | YES | YES | `try:` |
| 20273 | except_handler | 2 | try |  | no | no | `except Exception as _unresolved_rca_exc:` |
| 20274 | try | 3 | except_handler |  | no | no | `try:` |
| 20278 | if | 4 | try |  | no | no | `if _typed_on():` |
| 20292 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 20304 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 20305 | raise | 4 | if |  | no | no | `raise` |
| 20309 | try | 2 | try |  | YES | YES | `try:` |
| 20315 | for | 3 | try |  | YES | YES | `for _qid in (_eval_qids_for_entry or []):` |
| 20318 | if | 4 | for |  | YES | YES | `if isinstance(_scores, dict) and _qstr in _scores:` |
| 20324 | if | 4 | for |  | YES | YES | `if isinstance(_arbiter_map, dict) and _qstr in _arbiter_map:` |
| 20346 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20355 | if | 2 | try |  | YES | YES | `if _iter_num == 1:` |
| 20360 | if | 3 | if |  | YES | YES | `if _scaled_max_iterations != max_iterations:` |
| 20383 | try | 2 | try |  | YES | YES | `try:` |
| 20392 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20398 | try | 2 | try |  | YES | YES | `try:` |
| 20409 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20419 | try | 2 | try |  | YES | YES | `try:` |
| 20427 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20434 | try | 2 | try |  | YES | YES | `try:` |
| 20468 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20471 | try | 2 | try |  | YES | YES | `try:` |
| 20473 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20475 | try | 2 | try |  | YES | YES | `try:` |
| 20477 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20479 | try | 2 | try |  | YES | YES | `try:` |
| 20488 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20496 | try | 2 | try |  | YES | YES | `try:` |
| 20507 | if | 3 | try |  | YES | YES | `if _t8_cases:` |
| 20509 | try | 4 | if |  | no | no | `try:` |
| 20521 | for | 5 | try |  | no | no | `for _idx, _c in enumerate(_t8_cases, start=1):` |
| 20547 | if | 5 | try |  | no | no | `if _t8_audit_rows:` |
| 20552 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 20564 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20573 | if | 2 | try |  | YES | YES | `if human_required_signatures:` |
| 20592 | if | 3 | if |  | no | no | `if _dropped_hard or _dropped_soft:` |
| 20600 | if | 2 | try |  | YES | YES | `if not clusters and not soft_signal_clusters:` |
| 20602 | try | 3 | if |  | no | no | `try:` |
| 20624 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 20639 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 20640 | try | 4 | if |  | no | no | `try:` |
| 20652 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 20658 | break | 3 | if |  | no | no | `break` |
| 20709 | if | 2 | try |  | YES | YES | `if metadata_snapshot.get("_regression_mining_hints"):` |
| 20725 | for | 2 | try |  | YES | YES | `for _sc in soft_signal_clusters or []:` |
| 20726 | if | 3 | for |  | no | no | `if isinstance(_sc, dict):` |
| 20760 | try | 2 | try |  | YES | YES | `try:` |
| 20772 | if | 3 | try |  | YES | YES | `if (` |
| 20783 | for | 4 | if |  | no | no | `for _cid, _drifted in (` |
| 20795 | if | 5 | for |  | no | no | `if _t5_key in _iter_emitted_keys:` |
| 20796 | continue | 6 | if |  | no | no | `continue` |
| 20802 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20838 | if | 2 | try |  | YES | YES | `if reflection_buffer:` |
| 20842 | if | 3 | if |  | YES | YES | `if not _rollback_state_trusted_for_quarantine:` |
| 20858 | for | 3 | if |  | YES | YES | `for _pq_id, _pq_info in _persist_data.items():` |
| 20862 | if | 4 | for |  | YES | YES | `if _pq_class == "ADDITIVE_LEVERS_EXHAUSTED" or (` |
| 20866 | if | 5 | if |  | no | no | `elif _pq_conv in ("stuck", "worsening") and _pq_consec >= 2:` |
| 20875 | if | 3 | if |  | YES | YES | `if _soft_skip_qids:` |
| 20917 | if | 3 | if |  | YES | YES | `if _quarantine_qids:` |
| 20919 | if | 4 | if |  | YES | YES | `if _newly_quarantined:` |
| 20925 | try | 5 | if |  | YES | YES | `try:` |
| 20928 | for | 6 | try |  | YES | YES | `for _hq_id in sorted(_newly_quarantined):` |
| 20944 | if | 6 | try |  | YES | YES | `if _flag_items:` |
| 20954 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 20963 | for | 4 | if |  | YES | YES | `for c in list(clusters) + list(soft_signal_clusters or []):` |
| 20978 | try | 4 | if |  | YES | YES | `try:` |
| 20995 | if | 5 | try |  | YES | YES | `if _q_decision["action"] == "stop_for_human_review":` |
| 21006 | break | 6 | if |  | no | no | `break` |
| 21007 | if | 5 | try |  | YES | YES | `if _q_decision["action"] == "diagnostic_lane":` |
| 21012 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 21017 | if | 4 | if |  | YES | YES | `if not clusters and not soft_signal_clusters:` |
| 21019 | break | 5 | if |  | YES | YES | `break` |
| 21049 | try | 2 | try |  | YES | YES | `try:` |
| 21051 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 21056 | try | 2 | try |  | YES | YES | `try:` |
| 21075 | if | 3 | try |  | YES | YES | `if _process_all_ags and pending_action_groups:` |
| 21089 | while | 4 | if |  | no | no | `while pending_action_groups:` |
| 21095 | if | 5 | while |  | no | no | `if not _candidate_sig_set:` |
| 21108 | if | 6 | if |  | no | no | `if not _src_ids or (_src_ids & _live_cluster_ids):` |
| 21110 | break | 7 | if |  | no | no | `break` |
| 21111 | continue | 6 | if |  | no | no | `continue` |
| 21112 | if | 5 | while |  | no | no | `if _candidate_sig_set & _live_cluster_signatures:` |
| 21117 | break | 6 | if |  | no | no | `break` |
| 21120 | if | 4 | if |  | no | no | `if _dropped_for_drift:` |
| 21121 | for | 5 | if |  | no | no | `for _drop in _dropped_for_drift:` |
| 21139 | if | 4 | if |  | no | no | `if ag is not None:` |
| 21153 | if | 5 | if |  | no | no | `if _regression_debt_qids_for_next_iteration:` |
| 21160 | if | 6 | if |  | no | no | `if not (_debt_set & _ag_qids):` |
| 21169 | if | 3 | try |  | YES | YES | `if ag is None:` |
| 21172 | if | 4 | if |  | YES | YES | `if _regression_debt_qids_for_next_iteration:` |
| 21180 | if | 4 | if |  | YES | YES | `if _unresolved_target_debt_qids_for_next_iteration:` |
| 21210 | while | 4 | if |  | YES | YES | `while diagnostic_action_queue and _diag_preempt is None:` |
| 21227 | if | 5 | while |  | no | no | `if _candidate_sig_set:` |
| 21233 | if | 5 | while |  | no | no | `if not _matches_live:` |
| 21246 | continue | 6 | if |  | no | no | `continue` |
| 21274 | if | 4 | if |  | YES | YES | `if _intent_collisions:` |
| 21288 | try | 5 | if |  | no | no | `try:` |
| 21289 | for | 6 | try |  | no | no | `for _coll in _intent_collisions:` |
| 21293 | for | 7 | for |  | no | no | `for _qids_list in _qbycol.values():` |
| 21297 | if | 7 | for |  | no | no | `if _all_qids:` |
| 21303 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 21308 | if | 4 | if |  | YES | YES | `if _diag_preempt is not None:` |
| 21314 | if | 5 | if |  | YES | YES | `elif _memo_key in strategist_memo_cache:` |
| 21322 | if | 6 | if |  | YES | YES | `if _strategist_constraints.to_strategist_context():` |
| 21348 | try | 6 | if |  | YES | YES | `try:` |
| 21352 | if | 7 | try |  | YES | YES | `if _iter_fb_enabled():` |
| 21354 | if | 8 | if |  | YES | YES | `if _prior_iter >= 0:` |
| 21361 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 21417 | if | 6 | if |  | YES | YES | `if _selector_out["source"] == "three_stage_pipeline":` |
| 21447 | if | 4 | if |  | YES | YES | `if _l3_diagnostics:` |
| 21491 | try | 4 | if |  | YES | YES | `try:` |
| 21496 | if | 5 | try |  | YES | YES | `if _nm_enabled() and action_groups:` |
| 21508 | for | 6 | if |  | no | no | `for _c in clusters or []:` |
| 21511 | if | 7 | for |  | no | no | `if _cid and isinstance(_kit, dict):` |
| 21517 | for | 6 | if |  | no | no | `for _i in sorted(_iter_summaries.keys()):` |
| 21518 | if | 7 | for |  | no | no | `if int(_i) >= int(iteration_counter or 0):` |
| 21519 | continue | 8 | if |  | no | no | `continue` |
| 21523 | if | 7 | for |  | no | no | `if _prior_fb is None:` |
| 21524 | continue | 8 | if |  | no | no | `continue` |
| 21525 | for | 7 | for |  | no | no | `for _key, _shapes in (` |
| 21533 | for | 6 | if |  | no | no | `for _ag in action_groups:` |
| 21568 | if | 7 | for |  | no | no | `if _result.differs or not _strict:` |
| 21585 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 21599 | if | 4 | if |  | YES | YES | `if int(iteration_counter or 0) >= 2:` |
| 21600 | try | 5 | if |  | YES | YES | `try:` |
| 21604 | if | 6 | try |  | YES | YES | `if _al_enabled2():` |
| 21619 | for | 7 | if |  | YES | YES | `for _c in _cands:` |
| 21629 | for | 7 | if |  | YES | YES | `for _pa in _new_pas:` |
| 21644 | for | 7 | if |  | YES | YES | `for _c in _cands:` |
| 21645 | if | 8 | for |  | no | no | `if _c.signature_hash in _synthesised_sigs:` |
| 21646 | continue | 9 | if |  | no | no | `continue` |
| 21662 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 21673 | try | 4 | if |  | YES | YES | `try:` |
| 21678 | if | 5 | try |  | YES | YES | `if _repair_planner_enabled():` |
| 21697 | for | 6 | if |  | YES | YES | `for _c in clusters or []:` |
| 21699 | if | 7 | for |  | YES | YES | `if _kit is None:` |
| 21701 | if | 8 | if |  | YES | YES | `if _card is None:` |
| 21702 | continue | 9 | if |  | YES | YES | `continue` |
| 21713 | continue | 8 | if |  | no | no | `continue` |
| 21724 | if | 7 | for |  | no | no | `if _propagation in (` |
| 21759 | try | 6 | if |  | YES | YES | `try:` |
| 21763 | if | 7 | try |  | YES | YES | `if _al_enabled():` |
| 21773 | for | 8 | if |  | YES | YES | `for _r in _tier1_records:` |
| 21783 | if | 8 | if |  | YES | YES | `if _tier1_records:` |
| 21790 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 21795 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 21805 | try | 4 | if |  | YES | YES | `try:` |
| 21815 | if | 5 | try |  | YES | YES | `if _uncovered:` |
| 21820 | try | 6 | if |  | YES | YES | `try:` |
| 21851 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 21861 | try | 6 | if |  | YES | YES | `try:` |
| 21865 | if | 7 | try |  | YES | YES | `if _recall_enabled():` |
| 21888 | if | 8 | if |  | YES | YES | `if _eligible_ids:` |
| 21897 | try | 9 | if |  | no | no | `try:` |
| 21908 | except_handler | 9 | if |  | no | no | `except Exception:` |
| 21922 | if | 9 | if |  | no | no | `if _recall_succeeded:` |
| 21930 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 21935 | if | 6 | if |  | YES | YES | `if _uncovered:` |
| 21946 | for | 6 | if |  | YES | YES | `for _c in _uncovered:` |
| 21966 | try | 7 | for |  | YES | YES | `try:` |
| 21970 | if | 8 | try |  | YES | YES | `if (` |
| 22008 | if | 9 | if |  | YES | YES | `if _t3_trig_key not in _iter_emitted_keys:` |
| 22026 | if | 9 | if |  | YES | YES | `if _t3_exh_key not in _iter_emitted_keys:` |
| 22037 | for | 10 | if |  | YES | YES | `for _q in _t3_target_qids:` |
| 22038 | try | 11 | for |  | YES | YES | `try:` |
| 22044 | except_handler | 11 | for |  | no | no | `except Exception:` |
| 22054 | continue | 9 | if |  | YES | YES | `continue` |
| 22055 | except_handler | 7 | for |  | no | no | `except Exception:` |
| 22066 | try | 7 | for |  | no | no | `try:` |
| 22077 | if | 8 | try |  | no | no | `if _diag_qids:` |
| 22091 | except_handler | 7 | for |  | no | no | `except Exception:` |
| 22096 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 22132 | for | 4 | if |  | YES | YES | `for _ag_in in action_groups:` |
| 22138 | if | 4 | if |  | YES | YES | `if len(_decomposed_action_groups) != len(action_groups):` |
| 22164 | try | 4 | if |  | YES | YES | `try:` |
| 22165 | for | 5 | try |  | YES | YES | `for _ag_w8 in (action_groups or []):` |
| 22175 | if | 6 | for |  | no | no | `if _before_w8 and set(_before_w8) < set(_after_w8):` |
| 22187 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 22193 | if | 4 | if |  | YES | YES | `if _process_all_ags and len(action_groups) > 1:` |
| 22212 | for | 5 | if |  | no | no | `for _buffered_ag in pending_action_groups:` |
| 22235 | if | 3 | try |  | YES | YES | `if isinstance(_global_rewrite, dict):` |
| 22237 | if | 4 | if |  | no | no | `if non_empty and ag is not None:` |
| 22241 | if | 4 | if |  | YES | YES | `elif isinstance(_global_rewrite, str) and _global_rewrite.strip():` |
| 22242 | if | 5 | if |  | no | no | `if ag is not None:` |
| 22247 | if | 3 | try |  | YES | YES | `if ag is None and _iter_num == 1:` |
| 22257 | if | 4 | if |  | YES | YES | `if _fb_ags:` |
| 22261 | finally | 2 | try |  | YES | YES | `_mlflow.end_run()` |
| 22263 | if | 2 | try |  | YES | YES | `if ag is None and clusters:` |
| 22265 | for | 3 | if |  | YES | YES | `for c in clusters:` |
| 22267 | if | 3 | if |  | YES | YES | `if _remaining_qids and _iter_num <= max_iterations - 1:` |
| 22300 | if | 2 | try |  | YES | YES | `if ag is None:` |
| 22306 | try | 3 | if |  | no | no | `try:` |
| 22328 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 22342 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 22343 | try | 4 | if |  | no | no | `try:` |
| 22361 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 22367 | break | 3 | if |  | no | no | `break` |
| 22400 | try | 2 | try |  | YES | YES | `try:` |
| 22408 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 22449 | for | 2 | try |  | YES | YES | `for _rc_idx, _rc in enumerate(ranked):` |
| 22451 | if | 3 | for |  | YES | YES | `if _ag_source_cids and _rc_cid not in set(_ag_source_cids):` |
| 22452 | continue | 4 | if |  | YES | YES | `continue` |
| 22454 | if | 3 | for |  | YES | YES | `if _rc_sig and _rc_sig not in _ag_source_signatures:` |
| 22456 | if | 3 | for |  | YES | YES | `if not _ag_cluster_info:` |
| 22504 | for | 2 | try |  | YES | YES | `for _scid in (ag.get("source_cluster_ids") or []):` |
| 22510 | if | 3 | for |  | YES | YES | `if isinstance(_candidate_cluster, _Mapping):` |
| 22512 | break | 4 | if |  | YES | YES | `break` |
| 22513 | try | 2 | try |  | YES | YES | `try:` |
| 22521 | except_handler | 2 | try |  | no | no | `except _FailureClusterIdentityError as _identity_err:` |
| 22528 | if | 2 | try |  | YES | YES | `if _failure_cluster_for_collision is not None:` |
| 22540 | if | 2 | try |  | YES | YES | `if _collision_pair_matches(_collision_pair, _forbidden_pair):` |
| 22545 | if | 3 | if |  | no | no | `if (` |
| 22552 | if | 4 | if |  | no | no | `elif (` |
| 22617 | try | 3 | if |  | no | no | `try:` |
| 22639 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 22655 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 22656 | try | 4 | if |  | no | no | `try:` |
| 22675 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 22687 | if | 3 | if |  | no | no | `if _should_terminate_on_collision_saturation(` |
| 22712 | break | 4 | if |  | no | no | `break` |
| 22713 | continue | 3 | if |  | no | no | `continue` |
| 22729 | if | 2 | try |  | YES | YES | `if _ag_proposals and isinstance(_ag_proposals, list):` |
| 22730 | for | 3 | if |  | no | no | `for _prop in _ag_proposals:` |
| 22731 | if | 4 | for |  | no | no | `if not isinstance(_prop, dict):` |
| 22732 | continue | 5 | if |  | no | no | `continue` |
| 22733 | try | 4 | for |  | no | no | `try:` |
| 22746 | except_handler | 4 | for |  | no | no | `except Exception:` |
| 22748 | if | 3 | if |  | no | no | `if _ag_proposals:` |
| 22753 | if | 2 | try |  | YES | YES | `if _escalation:` |
| 22786 | if | 3 | if |  | no | no | `if _escalation == "flag_for_review" or (` |
| 22805 | continue | 4 | if |  | no | no | `continue` |
| 22807 | if | 3 | if |  | no | no | `if _escalation == "gt_repair":` |
| 22809 | if | 4 | if |  | no | no | `if _gt_repair_corrections > 0:` |
| 22846 | continue | 4 | if |  | no | no | `continue` |
| 22848 | if | 3 | if |  | no | no | `if _escalation == "remove_tvf" and _esc_tier in ("auto_apply", "apply_and_flag")` |
| 22851 | if | 4 | if |  | no | no | `if _tvf_id:` |
| 22880 | for | 5 | if |  | no | no | `for idx, entry in enumerate(_tvf_apply_log.get("applied", [])):` |
| 22886 | if | 5 | if |  | no | no | `if _tvf_apply_log.get("patch_deployed", False):` |
| 22889 | if | 6 | if |  | no | no | `if _original_instruction_sections:` |
| 22902 | try | 2 | try |  | YES | YES | `try:` |
| 22940 | if | 3 | try |  | YES | YES | `if _all_required_rca_levers:` |
| 22962 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 22965 | if | 2 | try |  | YES | YES | `if "6" in lever_keys:` |
| 22966 | try | 3 | if |  | YES | YES | `try:` |
| 22987 | for | 4 | try |  | YES | YES | `for _row in _structural_rows:` |
| 22988 | for | 5 | for |  | YES | YES | `for _candidate in extract_failed_row_sql_expression_candidates(_row):` |
| 22990 | if | 4 | try |  | YES | YES | `if _structural_candidates:` |
| 23014 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 23037 | try | 2 | try |  | YES | YES | `try:` |
| 23041 | if | 3 | try |  | YES | YES | `if _doc_enabled():` |
| 23057 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 23062 | for | 2 | try |  | YES | YES | `for lever_key in lever_keys:` |
| 23074 | try | 3 | for |  | YES | YES | `try:` |
| 23079 | except_handler | 3 | for |  | no | no | `except Exception:` |
| 23103 | try | 3 | for |  | YES | YES | `try:` |
| 23115 | try | 4 | try |  | YES | YES | `try:` |
| 23117 | if | 5 | try |  | YES | YES | `if _bon_cids:` |
| 23119 | if | 5 | try |  | YES | YES | `if _bon_first_cid:` |
| 23120 | for | 6 | if |  | YES | YES | `for _bon_c in (clusters or []):` |
| 23121 | if | 7 | for |  | YES | YES | `if str(_bon_c.get("cluster_id") or "") == _bon_first_cid:` |
| 23123 | if | 8 | if |  | YES | YES | `if _bon_card is not None:` |
| 23128 | break | 8 | if |  | YES | YES | `break` |
| 23129 | except_handler | 4 | try |  | no | no | `except Exception:` |
| 23134 | try | 4 | try |  | YES | YES | `try:` |
| 23148 | except_handler | 4 | try |  | no | no | `except Exception:` |
| 23157 | except_handler | 3 | for |  | no | no | `except Exception:` |
| 23165 | if | 3 | for |  | YES | YES | `if _use_best_of_n:` |
| 23169 | for | 4 | if |  | no | no | `for _bon_idx in range(3):` |
| 23170 | try | 5 | for |  | no | no | `try:` |
| 23186 | if | 6 | try |  | no | no | `if _sample:` |
| 23191 | except_handler | 5 | for |  | no | no | `except Exception:` |
| 23199 | if | 4 | if |  | no | no | `if _bon_candidates:` |
| 23200 | try | 5 | if |  | no | no | `try:` |
| 23209 | if | 6 | try |  | no | no | `if _bon_top is not None:` |
| 23223 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 23237 | try | 4 | if |  | no | no | `try:` |
| 23242 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23247 | try | 4 | if |  | no | no | `try:` |
| 23272 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23304 | if | 3 | for |  | YES | YES | `if _directive_outcome_ledger is not None and (` |
| 23307 | try | 4 | if |  | YES | YES | `try:` |
| 23317 | try | 5 | try |  | YES | YES | `try:` |
| 23322 | except_handler | 5 | try |  | no | no | `except Exception:` |
| 23338 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23350 | if | 2 | try |  | YES | YES | `if not all_proposals:` |
| 23351 | try | 3 | if |  | YES | YES | `try:` |
| 23363 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 23377 | if | 2 | try |  | YES | YES | `if _directive_outcome_ledger is not None:` |
| 23378 | try | 3 | if |  | YES | YES | `try:` |
| 23385 | for | 4 | try |  | YES | YES | `for _lever_int, _outcome in list(` |
| 23395 | if | 5 | for |  | YES | YES | `if _refined != _outcome:` |
| 23399 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 23409 | if | 2 | try |  | YES | YES | `if _directive_outcome_ledger is not None:` |
| 23410 | try | 3 | if |  | YES | YES | `try:` |
| 23422 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 23436 | try | 2 | try |  | YES | YES | `try:` |
| 23462 | if | 3 | try |  | YES | YES | `if (` |
| 23468 | for | 4 | if |  | YES | YES | `for _cid in (ag.get("source_cluster_ids") or []):` |
| 23473 | if | 5 | for |  | YES | YES | `if isinstance(_src_cluster, dict) and not _l5_ag_root_cause:` |
| 23477 | if | 5 | for |  | YES | YES | `if _l5_ag_rca_id and _l5_ag_root_cause:` |
| 23478 | break | 6 | if |  | YES | no | `break` |
| 23497 | try | 4 | if |  | YES | YES | `try:` |
| 23502 | for | 5 | try |  | YES | YES | `for _md in _l5_ag_drops:` |
| 23503 | for | 6 | for |  | YES | YES | `for _rc in (_md.get("root_causes") or ()):` |
| 23505 | if | 7 | for |  | YES | YES | `if _rc_s and _rc_s not in _l5_marker_root_causes:` |
| 23516 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23528 | try | 4 | if |  | YES | YES | `try:` |
| 23600 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23617 | try | 4 | if |  | YES | YES | `try:` |
| 23649 | for | 5 | try |  | YES | YES | `for _forced_proposal in _dispatch_result.appended_proposals:` |
| 23657 | for | 5 | try |  | YES | YES | `for _nsc_dict in _dispatch_result.emitted_decision_records:` |
| 23663 | try | 6 | for |  | YES | YES | `try:` |
| 23686 | except_handler | 6 | for |  | no | no | `except Exception:` |
| 23692 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23705 | if | 5 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 23706 | raise | 6 | if |  | no | no | `raise` |
| 23707 | except_handler | 2 | try |  | no | no | `except Exception as _lever5_structural_gate_exc:` |
| 23708 | try | 3 | except_handler |  | no | no | `try:` |
| 23712 | if | 4 | try |  | no | no | `if _typed_on():` |
| 23726 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 23738 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 23739 | raise | 4 | if |  | no | no | `raise` |
| 23749 | try | 2 | try |  | YES | YES | `try:` |
| 23753 | for | 3 | try |  | YES | YES | `for _force_cid in (ag.get("source_cluster_ids") or ()):` |
| 23757 | if | 4 | for |  | YES | YES | `if not isinstance(_force_cluster, dict):` |
| 23758 | continue | 5 | if |  | no | no | `continue` |
| 23773 | if | 4 | for |  | YES | YES | `if (` |
| 23830 | if | 4 | for |  | YES | YES | `if _ag_sigs:` |
| 23833 | try | 4 | for |  | YES | YES | `try:` |
| 23858 | if | 5 | try |  | YES | YES | `if _forced_l6 is None:` |
| 23860 | except_handler | 4 | for |  | no | no | `except Exception as _force_exc:` |
| 23863 | try | 5 | except_handler |  | no | no | `try:` |
| 23867 | if | 6 | try |  | no | no | `if _typed_on():` |
| 23881 | except_handler | 5 | except_handler |  | no | no | `except Exception:` |
| 23895 | if | 5 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 23896 | raise | 6 | if |  | no | no | `raise` |
| 23898 | if | 4 | for |  | YES | YES | `if _forced_l6 is not None:` |
| 23943 | if | 5 | if |  | YES | YES | `if _force_outcome == "raised":` |
| 23971 | except_handler | 2 | try |  | no | no | `except Exception as _forced_lever6_n3_exc:` |
| 23972 | try | 3 | except_handler |  | no | no | `try:` |
| 23976 | if | 4 | try |  | no | no | `if _typed_on():` |
| 23990 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 24004 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 24005 | raise | 4 | if |  | no | no | `raise` |
| 24041 | for | 2 | try |  | YES | YES | `for _rb in reflection_buffer:` |
| 24042 | if | 3 | for |  | no | no | `if _rb.get("accepted"):` |
| 24043 | continue | 4 | if |  | no | no | `continue` |
| 24051 | if | 3 | for |  | no | no | `if _rb.get("rollback_class") != _RC.CONTENT_REGRESSION.value:` |
| 24052 | continue | 4 | if |  | no | no | `continue` |
| 24054 | for | 3 | for |  | no | no | `for _dnr in _rb.get("do_not_retry", []):` |
| 24056 | if | 4 | for |  | no | no | `if " on " not in _s:` |
| 24057 | continue | 5 | if |  | no | no | `continue` |
| 24062 | for | 3 | for |  | no | no | `for _rb_patch in _rb.get("do_not_retry_patches", []) or []:` |
| 24063 | if | 4 | for |  | no | no | `if isinstance(_rb_patch, dict):` |
| 24105 | for | 2 | try |  | YES | YES | `for _rb in reflection_buffer:` |
| 24106 | if | 3 | for |  | no | no | `if _rb.get("accepted"):` |
| 24107 | continue | 4 | if |  | no | no | `continue` |
| 24108 | for | 3 | for |  | no | no | `for _rb_patch in _rb.get("do_not_retry_patches", []) or []:` |
| 24109 | if | 4 | for |  | no | no | `if isinstance(_rb_patch, dict):` |
| 24117 | if | 2 | try |  | YES | YES | `if _content_dedup_dropped:` |
| 24123 | if | 2 | try |  | YES | YES | `if _patch_forbidden:` |
| 24133 | for | 3 | if |  | no | no | `for _rb in reflection_buffer:` |
| 24134 | if | 4 | for |  | no | no | `if _rb.get("accepted"):` |
| 24135 | continue | 5 | if |  | no | no | `continue` |
| 24136 | for | 4 | for |  | no | no | `for _entry in _rb.get("do_not_retry", []) or []:` |
| 24138 | if | 5 | for |  | no | no | `if " on " in _es:` |
| 24144 | for | 3 | if |  | no | no | `for _p in all_proposals:` |
| 24165 | if | 4 | for |  | no | no | `if (` |
| 24174 | if | 5 | if |  | no | no | `if _retry_decision.allowed:` |
| 24181 | continue | 6 | if |  | no | no | `continue` |
| 24182 | if | 4 | for |  | no | no | `if _key in _patch_forbidden:` |
| 24183 | if | 5 | if |  | no | no | `if not _justification:` |
| 24186 | continue | 6 | if |  | no | no | `continue` |
| 24187 | if | 5 | if |  | no | no | `if (` |
| 24196 | continue | 6 | if |  | no | no | `continue` |
| 24222 | if | 3 | if |  | no | no | `if _dropped:` |
| 24229 | for | 4 | if |  | no | no | `for _ptype, _target, _reason in _dropped:` |
| 24238 | for | 4 | if |  | no | no | `for _ptype, _target, _reason in _dropped:` |
| 24246 | if | 3 | if |  | no | no | `if _reflection_rewrites:` |
| 24247 | try | 4 | if |  | no | no | `try:` |
| 24252 | for | 5 | try |  | no | no | `for _idx, _rw in enumerate(_reflection_rewrites, start=1):` |
| 24276 | if | 5 | try |  | no | no | `if _t10_rows:` |
| 24281 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 24300 | try | 3 | if |  | no | no | `try:` |
| 24301 | if | 4 | try |  | no | no | `if _dropped:` |
| 24319 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 24371 | if | 2 | try |  | YES | YES | `if _collateral_details:` |
| 24372 | for | 3 | if |  | no | no | `for _ptype, _target, _deps in _collateral_details:` |
| 24385 | for | 2 | try |  | YES | YES | `for pi, p in enumerate(all_proposals, 1):` |
| 24393 | if | 3 | for |  | no | no | `if status == "FAILED (non-JSON)":` |
| 24395 | if | 4 | if |  | no | no | `elif status == "INVALID_TARGET":` |
| 24403 | if | 3 | for |  | no | no | `if table:` |
| 24405 | if | 3 | for |  | no | no | `if column:` |
| 24410 | if | 3 | for |  | no | no | `if isinstance(_p_col_sect, dict) and _p_col_sect:` |
| 24412 | for | 4 | if |  | no | no | `for _sk, _sv in _p_col_sect.items():` |
| 24415 | if | 4 | if |  | no | no | `elif isinstance(_p_tbl_sect, dict) and _p_tbl_sect:` |
| 24417 | for | 5 | if |  | no | no | `for _sk, _sv in _p_tbl_sect.items():` |
| 24420 | if | 5 | if |  | no | no | `elif proposed_value:` |
| 24428 | if | 2 | try |  | YES | YES | `if _n_failed:` |
| 24436 | for | 2 | try |  | YES | YES | `for pi, p in enumerate(all_proposals, 1):` |
| 24438 | if | 3 | for |  | no | no | `if not prov:` |
| 24439 | continue | 4 | if |  | no | no | `continue` |
| 24453 | try | 2 | try |  | YES | YES | `try:` |
| 24455 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24458 | if | 2 | try |  | YES | YES | `if not all_proposals:` |
| 24490 | try | 3 | if |  | YES | YES | `try:` |
| 24494 | if | 4 | try |  | YES | YES | `if forbidden_ag_admits_no_action_enabled():` |
| 24526 | try | 5 | if |  | YES | YES | `try:` |
| 24595 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 24601 | try | 5 | if |  | YES | YES | `try:` |
| 24602 | for | 6 | try |  | YES | YES | `for _lk in lever_keys:` |
| 24608 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 24614 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 24631 | try | 3 | if |  | YES | YES | `try:` |
| 24653 | except_handler | 3 | if |  | YES | YES | `except Exception:` |
| 24667 | if | 3 | if |  | YES | YES | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 24668 | try | 4 | if |  | YES | YES | `try:` |
| 24687 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 24693 | continue | 3 | if |  | YES | YES | `continue` |
| 24698 | try | 2 | try |  | no | no | `try:` |
| 24705 | for | 3 | try |  | no | no | `for _p in all_proposals:` |
| 24707 | if | 4 | for |  | no | no | `if _decision["compatible"]:` |
| 24716 | if | 3 | try |  | no | no | `if _incompatible_proposals:` |
| 24733 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24740 | try | 2 | try |  | no | no | `try:` |
| 24757 | if | 3 | try |  | no | no | `if _shape_decisions:` |
| 24779 | if | 4 | if |  | no | no | `if _rca_shape_drops:` |
| 24787 | for | 5 | if |  | no | no | `for _drop in _rca_shape_drops:` |
| 24812 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24820 | try | 2 | try |  | no | no | `try:` |
| 24826 | if | 3 | try |  | no | no | `if _shape_dropped_ids:` |
| 24865 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24876 | try | 2 | try |  | no | no | `try:` |
| 24880 | for | 3 | try |  | no | no | `for _snap in reversed(_ag_snapshots):` |
| 24881 | if | 4 | for |  | no | no | `if str(_snap.get("id")) == str(ag_id):` |
| 24896 | break | 5 | if |  | no | no | `break` |
| 24897 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24906 | try | 2 | try |  | no | no | `try:` |
| 24917 | if | 3 | try |  | no | no | `if not _ag_assigned_qids:` |
| 24926 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24954 | try | 2 | try |  | no | no | `try:` |
| 24997 | if | 3 | try |  | no | no | `if _chunk_b_on():` |
| 25048 | try | 3 | try |  | no | no | `try:` |
| 25049 | if | 4 | try |  | no | no | `if isinstance(metadata_snapshot, dict):` |
| 25053 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25059 | try | 3 | try |  | no | no | `try:` |
| 25070 | if | 4 | try |  | no | no | `if _pd_records:` |
| 25074 | for | 5 | if |  | no | no | `for _pd_rec in _pd_records:` |
| 25075 | try | 6 | for |  | no | no | `try:` |
| 25077 | if | 7 | try |  | no | no | `if _pd_key in _iter_emitted_keys:` |
| 25078 | continue | 8 | if |  | no | no | `continue` |
| 25080 | except_handler | 6 | for |  | no | no | `except Exception:` |
| 25086 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25113 | try | 3 | try |  | no | no | `try:` |
| 25120 | if | 4 | try |  | no | no | `if (` |
| 25130 | for | 5 | if |  | no | no | `for _sc in soft_signal_clusters or []:` |
| 25132 | if | 6 | for |  | no | no | `if not _sc_cid:` |
| 25133 | continue | 7 | if |  | no | no | `continue` |
| 25144 | for | 5 | if |  | no | no | `for _cand in clusters or []:` |
| 25145 | if | 6 | for |  | no | no | `if not isinstance(_cand, dict):` |
| 25146 | continue | 7 | if |  | no | no | `continue` |
| 25147 | if | 6 | for |  | no | no | `if bool(_cand.get("rca_card")):` |
| 25148 | continue | 7 | if |  | no | no | `continue` |
| 25150 | if | 6 | for |  | no | no | `if not _cand_cid:` |
| 25151 | continue | 7 | if |  | no | no | `continue` |
| 25153 | if | 6 | for |  | no | no | `if not _soft_entry:` |
| 25154 | continue | 7 | if |  | no | no | `continue` |
| 25164 | try | 6 | for |  | no | no | `try:` |
| 25169 | except_handler | 6 | for |  | no | no | `except Exception:` |
| 25177 | if | 6 | for |  | no | no | `if _prov_card is None:` |
| 25178 | continue | 7 | if |  | no | no | `continue` |
| 25184 | try | 6 | for |  | no | no | `try:` |
| 25205 | except_handler | 6 | for |  | no | no | `except Exception:` |
| 25211 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25227 | try | 3 | try |  | no | no | `try:` |
| 25231 | if | 4 | try |  | no | no | `if ag_emit_grounding_gate_enabled():` |
| 25232 | checkpoint_call | 5 | if | collect_blocked_clusters | no | no | `_grounding_result = collect_blocked_clusters(` |
| 25240 | if | 5 | if |  | no | no | `if _grounding_result.records_payload:` |
| 25244 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25274 | try | 3 | try |  | no | no | `try:` |
| 25281 | if | 4 | try |  | no | no | `if strategist_recovery_pivot_enabled() and reflection_buffer:` |
| 25296 | for | 5 | if |  | no | no | `for _c in (clusters or []):` |
| 25297 | if | 6 | for |  | no | no | `if not isinstance(_c, dict):` |
| 25298 | continue | 7 | if |  | no | no | `continue` |
| 25300 | if | 6 | for |  | no | no | `if not _cid:` |
| 25301 | continue | 7 | if |  | no | no | `continue` |
| 25302 | for | 6 | for |  | no | no | `for _q in (_c.get("question_ids") or ()):` |
| 25304 | if | 7 | for |  | no | no | `if _qs:` |
| 25324 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25384 | if | 3 | try |  | no | no | `if _admission_on():` |
| 25418 | except_handler | 2 | try |  | no | no | `except Exception as _strategist_ag_exc:` |
| 25419 | try | 3 | except_handler |  | no | no | `try:` |
| 25423 | if | 4 | try |  | no | no | `if _typed_on():` |
| 25437 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 25450 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 25451 | raise | 4 | if |  | no | no | `raise` |
| 25458 | try | 2 | try |  | no | no | `try:` |
| 25473 | if | 3 | try |  | no | no | `if not _ag_verdict.accepted:` |
| 25476 | for | 4 | if |  | no | no | `for _cid in (ag.get("source_cluster_ids") or []):` |
| 25480 | if | 5 | for |  | no | no | `if _ag_rca_id_c5:` |
| 25481 | break | 6 | if |  | no | no | `break` |
| 25498 | except_handler | 2 | try |  | no | no | `except Exception as _groundedness_ag_exc:` |
| 25499 | try | 3 | except_handler |  | no | no | `try:` |
| 25503 | if | 4 | try |  | no | no | `if _typed_on():` |
| 25517 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 25529 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 25530 | raise | 4 | if |  | no | no | `raise` |
| 25536 | try | 2 | try |  | no | no | `try:` |
| 25537 | for | 3 | try |  | no | no | `for _p in (all_proposals or []):` |
| 25539 | if | 4 | for |  | no | no | `if not _ptids:` |
| 25542 | if | 4 | for |  | no | no | `if not _ptids:` |
| 25543 | continue | 5 | if |  | no | no | `continue` |
| 25567 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 25602 | try | 2 | try |  | no | no | `try:` |
| 25609 | if | 3 | try |  | no | no | `if _chunk_c_on_f5():` |
| 25663 | except_handler | 2 | try |  | no | no | `except Exception as _proposal_generated_exc:` |
| 25664 | try | 3 | except_handler |  | no | no | `try:` |
| 25668 | if | 4 | try |  | no | no | `if _typed_on():` |
| 25682 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 25695 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 25696 | raise | 4 | if |  | no | no | `raise` |
| 25700 | try | 2 | try |  | no | no | `try:` |
| 25713 | for | 3 | try |  | no | no | `for _prop in (all_proposals or []):` |
| 25715 | if | 4 | for |  | no | no | `if not _prop_id:` |
| 25716 | continue | 5 | if |  | no | no | `continue` |
| 25720 | if | 4 | for |  | no | no | `if _verdict_p.accepted:` |
| 25721 | continue | 5 | if |  | no | no | `continue` |
| 25731 | if | 3 | try |  | no | no | `if _proposal_drops_c5:` |
| 25740 | except_handler | 2 | try |  | no | no | `except Exception as _groundedness_proposal_exc:` |
| 25741 | try | 3 | except_handler |  | no | no | `try:` |
| 25745 | if | 4 | try |  | no | no | `if _typed_on():` |
| 25759 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 25771 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 25772 | raise | 4 | if |  | no | no | `raise` |
| 25782 | try | 2 | try |  | no | no | `try:` |
| 25788 | if | 3 | try |  | no | no | `if len(patches) > _pre_split_count:` |
| 25794 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 25807 | try | 2 | try |  | no | no | `try:` |
| 25835 | for | 3 | try |  | no | no | `for _patch in patches:` |
| 25836 | try | 4 | for |  | no | no | `try:` |
| 25838 | if | 5 | try |  | no | no | `if isinstance(_rca_exec, dict):` |
| 25843 | except_handler | 4 | for |  | no | no | `except Exception:` |
| 25855 | if | 4 | for |  | no | no | `if _score >= MIN_PROPOSAL_RELEVANCE:` |
| 25891 | if | 3 | try |  | no | no | `if _dropped:` |
| 25928 | try | 3 | try |  | no | no | `try:` |
| 25946 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25952 | try | 3 | try |  | no | no | `try:` |
| 25964 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25971 | try | 3 | try |  | no | no | `try:` |
| 25976 | for | 4 | try |  | no | no | `for _idx, (_patch, _score, _dec) in enumerate(` |
| 26031 | if | 4 | try |  | no | no | `if _grounding_rows:` |
| 26035 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 26040 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 26052 | if | 2 | try |  | no | no | `if _grounding_skip.skip:` |
| 26105 | try | 3 | if |  | no | no | `try:` |
| 26127 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 26142 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 26143 | try | 4 | if |  | no | no | `try:` |
| 26162 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26168 | continue | 3 | if |  | no | no | `continue` |
| 26175 | try | 2 | try |  | no | no | `try:` |
| 26207 | if | 3 | try |  | no | no | `if _stage6_br_pure_on():` |
| 26224 | for | 4 | if |  | no | no | `for _candidate in patches:` |
| 26231 | if | 5 | for |  | no | no | `if not _decision["safe"]:` |
| 26260 | continue | 6 | if |  | no | no | `continue` |
| 26267 | if | 5 | for |  | no | no | `if not _scope_decision["safe"]:` |
| 26289 | continue | 6 | if |  | no | no | `continue` |
| 26322 | if | 3 | try |  | no | no | `if _narrow_kept:` |
| 26338 | try | 3 | try |  | no | no | `try:` |
| 26342 | if | 4 | try |  | no | no | `if (` |
| 26441 | if | 5 | if |  | no | no | `if _p24_outside_target:` |
| 26443 | if | 5 | if |  | no | no | `if (` |
| 26451 | try | 6 | if |  | no | no | `try:` |
| 26458 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 26460 | if | 6 | if |  | no | no | `if _p24_retest.get("safe") is True:` |
| 26466 | try | 5 | if |  | no | no | `try:` |
| 26482 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 26487 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 26507 | if | 3 | try |  | no | no | `if _stage6_nr_pure_on():` |
| 26508 | try | 4 | if |  | no | no | `try:` |
| 26531 | if | 5 | try |  | no | no | `if _rco4_nr_outcome.halt_no_structural_alternative:` |
| 26586 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26595 | try | 4 | if |  | no | no | `try:` |
| 26605 | if | 5 | try |  | no | no | `if _structural_drops:` |
| 26651 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26658 | if | 3 | try |  | no | no | `if _blast_dropped:` |
| 26684 | try | 4 | if |  | no | no | `try:` |
| 26693 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26702 | try | 4 | if |  | no | no | `try:` |
| 26709 | for | 5 | try |  | no | no | `for _cid in (ag.get("source_cluster_ids") or []):` |
| 26713 | if | 6 | for |  | no | no | `if not _br_root_cause:` |
| 26717 | if | 6 | for |  | no | no | `if not _br_rca_id:` |
| 26721 | if | 6 | for |  | no | no | `if _br_root_cause and _br_rca_id:` |
| 26722 | break | 7 | if |  | no | no | `break` |
| 26740 | except_handler | 4 | if |  | no | no | `except Exception as _blast_radius_exc:` |
| 26741 | try | 5 | except_handler |  | no | no | `try:` |
| 26745 | if | 6 | try |  | no | no | `if _typed_on():` |
| 26759 | except_handler | 5 | except_handler |  | no | no | `except Exception:` |
| 26773 | if | 5 | except_handler |  | no | no | `if is_strict_mode():` |
| 26774 | raise | 6 | if |  | no | no | `raise` |
| 26784 | try | 4 | if |  | no | no | `try:` |
| 26791 | if | 5 | try |  | no | no | `if _t2_target_qids:` |
| 26792 | for | 6 | if |  | no | no | `for _drop in _blast_dropped or ():` |
| 26811 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26818 | except_handler | 2 | try |  | no | no | `except ImportError:` |
| 26843 | for | 3 | except_handler |  | no | no | `for _candidate in patches:` |
| 26850 | if | 4 | for |  | no | no | `if _decision["safe"]:` |
| 26888 | if | 3 | except_handler |  | no | no | `if _narrow_kept:` |
| 26896 | try | 3 | except_handler |  | no | no | `try:` |
| 26900 | if | 4 | try |  | no | no | `if (` |
| 26977 | if | 5 | if |  | no | no | `if _p24b_outside_target:` |
| 26979 | if | 5 | if |  | no | no | `if (` |
| 26984 | try | 6 | if |  | no | no | `try:` |
| 26991 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 26993 | if | 6 | if |  | no | no | `if _p24b_retest.get("safe") is True:` |
| 26996 | try | 5 | if |  | no | no | `try:` |
| 27013 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 27018 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 27025 | if | 3 | except_handler |  | no | no | `if _blast_dropped:` |
| 27044 | try | 2 | try |  | no | no | `try:` |
| 27048 | if | 3 | try |  | no | no | `if _structural_repair_on():` |
| 27070 | for | 4 | if |  | no | no | `for _sr_cid in (ag.get("source_cluster_ids") or []):` |
| 27076 | if | 5 | for |  | no | no | `if _sr_rca_card is not None:` |
| 27077 | break | 6 | if |  | no | no | `break` |
| 27116 | try | 4 | if |  | no | no | `try:` |
| 27132 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 27138 | try | 4 | if |  | no | no | `try:` |
| 27157 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 27168 | if | 4 | if |  | no | no | `if _sr_verdict.outcome == "rejected":` |
| 27169 | if | 5 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 27170 | try | 6 | if |  | no | no | `try:` |
| 27187 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 27228 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27246 | try | 2 | try |  | no | no | `try:` |
| 27250 | if | 3 | try |  | no | no | `if _chunk_c_on_f6():` |
| 27293 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27312 | try | 2 | try |  | no | no | `try:` |
| 27319 | if | 3 | try |  | no | no | `if _stage6_app_pure_on():` |
| 27353 | if | 3 | try |  | no | no | `if _non_applyable_decisions:` |
| 27374 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27385 | try | 2 | try |  | no | no | `try:` |
| 27387 | if | 3 | try |  | no | no | `if _non_applyable:` |
| 27406 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27433 | for | 2 | try |  | no | no | `for _p in patches:` |
| 27434 | if | 3 | for |  | no | no | `if not l5_l6_patch_requires_asset_alignment(_p):` |
| 27436 | continue | 4 | if |  | no | no | `continue` |
| 27446 | if | 3 | for |  | no | no | `if _decision.get("aligned"):` |
| 27448 | continue | 4 | if |  | no | no | `continue` |
| 27456 | if | 2 | try |  | no | no | `if _alignment_drops:` |
| 27466 | try | 2 | try |  | no | no | `try:` |
| 27467 | if | 3 | try |  | no | no | `if _alignment_drops:` |
| 27515 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27526 | if | 2 | try |  | no | no | `if len(patches) > MAX_AG_PATCHES:` |
| 27559 | if | 3 | if |  | no | no | `if _no_causal_halt():` |
| 27565 | if | 4 | if |  | no | no | `if (` |
| 27606 | try | 5 | if |  | no | no | `try:` |
| 27671 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 27677 | try | 5 | if |  | no | no | `try:` |
| 27678 | for | 6 | try |  | no | no | `for _lk in lever_keys:` |
| 27684 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 27708 | try | 4 | if |  | no | no | `try:` |
| 27713 | if | 5 | try |  | no | no | `if _hub_scoped_enabled():` |
| 27729 | for | 6 | if |  | no | no | `for _p in _expanded:` |
| 27731 | if | 7 | for |  | no | no | `if not _scoped_from:` |
| 27732 | continue | 8 | if |  | no | no | `continue` |
| 27744 | for | 6 | if |  | no | no | `for _p in _before_cap:` |
| 27745 | if | 7 | for |  | no | no | `if not _is_hub_patch(_p, threshold=_hub_th_val):` |
| 27746 | continue | 8 | if |  | no | no | `continue` |
| 27754 | if | 7 | for |  | no | no | `if _has_sibling:` |
| 27755 | continue | 8 | if |  | no | no | `continue` |
| 27773 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 27788 | if | 4 | if |  | no | no | `if _kit_aware_enabled():` |
| 27813 | try | 5 | if |  | no | no | `try:` |
| 27817 | if | 6 | try |  | no | no | `if _soft_ev_enabled():` |
| 27825 | if | 7 | if |  | no | no | `if _soft_lookup:` |
| 27843 | try | 8 | if |  | no | no | `try:` |
| 27844 | for | 9 | try |  | no | no | `for _qids in _soft_lookup.values():` |
| 27845 | for | 10 | for |  | no | no | `for _q in _qids:` |
| 27847 | except_handler | 8 | if |  | no | no | `except Exception:` |
| 27853 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 27860 | try | 5 | if |  | no | no | `try:` |
| 27873 | for | 6 | try |  | no | no | `for _ko in _kit_outcomes:` |
| 27886 | if | 7 | for |  | no | no | `if _ko.get("accepted"):` |
| 27887 | if | 8 | if |  | no | no | `if _ko.get("risk_downgraded_from_high_to_medium"):` |
| 27900 | continue | 8 | if |  | no | no | `continue` |
| 27902 | if | 7 | for |  | no | no | `if _reason == "kit_atomicity_violation":` |
| 27924 | if | 6 | try |  | no | no | `if not patches:` |
| 27936 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 27969 | for | 3 | if |  | no | no | `for _d in _dropped_decisions:` |
| 27986 | try | 3 | if |  | no | no | `try:` |
| 27988 | for | 4 | try |  | no | no | `for _bp in (_before_cap or []):` |
| 27992 | if | 5 | for |  | no | no | `if _bpid:` |
| 27994 | for | 4 | try |  | no | no | `for _d in _dropped_decisions:` |
| 28000 | if | 5 | for |  | no | no | `if not _dt_qids:` |
| 28008 | if | 5 | for |  | no | no | `if _dt_qids:` |
| 28027 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28063 | try | 3 | if |  | no | no | `try:` |
| 28100 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28126 | if | 2 | try |  | no | no | `if (` |
| 28153 | try | 3 | if |  | no | no | `try:` |
| 28157 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28171 | if | 3 | if |  | no | no | `if not pending_action_groups:` |
| 28173 | try | 3 | if |  | no | no | `try:` |
| 28195 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28211 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 28212 | try | 4 | if |  | no | no | `try:` |
| 28231 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 28269 | continue | 3 | if |  | no | no | `continue` |
| 28278 | if | 2 | try |  | no | no | `if SHADOW_APPLY:` |
| 28309 | if | 2 | try |  | no | no | `if not _pre_ag_snapshot_capture.get("captured"):` |
| 28329 | try | 3 | if |  | no | no | `try:` |
| 28333 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28348 | if | 3 | if |  | no | no | `if not pending_action_groups:` |
| 28350 | try | 3 | if |  | no | no | `try:` |
| 28372 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28387 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 28388 | try | 4 | if |  | no | no | `try:` |
| 28407 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 28445 | continue | 3 | if |  | no | no | `continue` |
| 28463 | try | 2 | try |  | no | no | `try:` |
| 28465 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 28475 | try | 2 | try |  | no | no | `try:` |
| 28495 | if | 3 | try |  | no | no | `if _survival_table:` |
| 28498 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 28506 | try | 2 | try |  | no | no | `try:` |
| 28547 | if | 3 | try |  | no | no | `if not _recon.in_agreement:` |
| 28554 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 28565 | if | 2 | try |  | no | no | `if _apply_skip.skip:` |
| 28566 | if | 3 | if |  | no | no | `if _apply_skip.reason_code == "no_applied_patches":` |
| 28598 | try | 4 | if |  | no | no | `try:` |
| 28605 | for | 5 | try |  | no | no | `for _cid in (ag.get("source_cluster_ids") or []):` |
| 28609 | if | 6 | for |  | no | no | `if not _doa_root_cause:` |
| 28613 | if | 6 | for |  | no | no | `if not _doa_rca_id:` |
| 28617 | if | 6 | for |  | no | no | `if _doa_root_cause and _doa_rca_id:` |
| 28618 | break | 7 | if |  | no | no | `break` |
| 28637 | except_handler | 4 | if |  | no | no | `except Exception as _dead_on_arrival_exc:` |
| 28638 | try | 5 | except_handler |  | no | no | `try:` |
| 28642 | if | 6 | try |  | no | no | `if _typed_on():` |
| 28656 | except_handler | 5 | except_handler |  | no | no | `except Exception:` |
| 28672 | if | 5 | except_handler |  | no | no | `if is_strict_mode():` |
| 28673 | raise | 6 | if |  | no | no | `raise` |
| 28695 | if | 4 | if |  | no | no | `if not pending_action_groups:` |
| 28699 | try | 4 | if |  | no | no | `try:` |
| 28763 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 28769 | try | 4 | if |  | no | no | `try:` |
| 28770 | for | 5 | try |  | no | no | `for _lk in lever_keys:` |
| 28776 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 28793 | try | 3 | if |  | no | no | `try:` |
| 28800 | if | 4 | try |  | no | no | `if _decision_counts:` |
| 28815 | try | 4 | try |  | no | no | `try:` |
| 28817 | if | 5 | try |  | no | no | `if _decision_counts and _mlflow_apl.active_run() is not None:` |
| 28837 | except_handler | 4 | try |  | no | no | `except Exception:` |
| 28843 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28899 | try | 3 | if |  | no | no | `try:` |
| 28903 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28911 | try | 3 | if |  | no | no | `try:` |
| 28973 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28979 | try | 3 | if |  | no | no | `try:` |
| 28980 | for | 4 | try |  | no | no | `for _lk in lever_keys:` |
| 28986 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29003 | try | 3 | if |  | no | no | `try:` |
| 29025 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29040 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 29041 | try | 4 | if |  | no | no | `try:` |
| 29060 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 29067 | continue | 3 | if |  | no | no | `continue` |
| 29070 | for | 2 | try |  | no | no | `for idx, entry in enumerate(apply_log.get("applied", [])):` |
| 29084 | try | 3 | for |  | no | no | `try:` |
| 29107 | if | 4 | try |  | no | no | `if not _ap_target_qids:` |
| 29122 | if | 4 | try |  | no | no | `if _ap_target_qid_set:` |
| 29130 | if | 4 | try |  | no | no | `if _ap_broad_qid_set:` |
| 29138 | except_handler | 3 | for |  | no | no | `except Exception:` |
| 29163 | try | 2 | try |  | no | no | `try:` |
| 29170 | if | 3 | try |  | no | no | `if _chunk_c_on_f7():` |
| 29225 | except_handler | 2 | try |  | no | no | `except Exception as _patch_applied_exc:` |
| 29226 | try | 3 | except_handler |  | no | no | `try:` |
| 29230 | if | 4 | try |  | no | no | `if _typed_on():` |
| 29244 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 29257 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 29258 | raise | 4 | if |  | no | no | `raise` |
| 29261 | if | 2 | try |  | no | no | `if _queued:` |
| 29264 | for | 3 | if |  | no | no | `for qentry in _queued:` |
| 29289 | for | 3 | if |  | no | no | `for qi, qe in enumerate(_queued, 1):` |
| 29297 | if | 2 | try |  | no | no | `if not apply_log.get("patch_deployed", False) and apply_log.get("applied"):` |
| 29340 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 29341 | try | 4 | if |  | no | no | `try:` |
| 29360 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 29366 | if | 3 | if |  | no | no | `if _pe_class == RollbackClass.SCHEMA_FAILURE:` |
| 29381 | break | 4 | if |  | no | no | `break` |
| 29389 | if | 3 | if |  | no | no | `if _pe_class == RollbackClass.INFRA_FAILURE:` |
| 29391 | for | 4 | if |  | no | no | `for _rb_entry in reversed(reflection_buffer):` |
| 29392 | if | 5 | for |  | no | no | `if _rb_entry.get("rollback_class") == RollbackClass.INFRA_FAILURE.value:` |
| 29395 | break | 6 | if |  | no | no | `break` |
| 29396 | if | 4 | if |  | no | no | `if _consecutive_infra >= INFRA_RETRY_BUDGET:` |
| 29414 | break | 5 | if |  | no | no | `break` |
| 29416 | try | 3 | if |  | no | no | `try:` |
| 29438 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29448 | continue | 3 | if |  | no | no | `continue` |
| 29452 | if | 2 | try |  | no | no | `if _applied:` |
| 29454 | for | 3 | if |  | no | no | `for ai, aentry in enumerate(_applied, 1):` |
| 29462 | if | 2 | try |  | no | no | `if _dropped:` |
| 29464 | for | 3 | if |  | no | no | `for di, dp in enumerate(_dropped, 1):` |
| 29541 | try | 2 | try |  | no | no | `try:` |
| 29543 | if | 3 | try |  | no | no | `if bool(_gr.get("passed")) or str(` |
| 29551 | if | 4 | if |  | no | no | `if bool(_gr.get("passed")):` |
| 29555 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 29566 | try | 2 | try |  | no | no | `try:` |
| 29568 | if | 3 | try |  | no | no | `if (` |
| 29575 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 29593 | if | 2 | try |  | no | no | `if _gate_eval:` |
| 29603 | try | 3 | if |  | no | no | `try:` |
| 29605 | if | 4 | try |  | no | no | `if _backfill_rows and not _current_iter_inputs.get("eval_rows"):` |
| 29607 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29620 | try | 2 | try |  | no | no | `try:` |
| 29623 | if | 3 | try |  | no | no | `if _t4_verdict_for_persist is not None:` |
| 29628 | for | 4 | if |  | no | no | `for _c in (strategy.get("_source_clusters") or []) if strategy else []:` |
| 29630 | if | 5 | for |  | no | no | `if not _cid:` |
| 29631 | continue | 6 | if |  | no | no | `continue` |
| 29632 | for | 5 | for |  | no | no | `for _q in _c.get("question_ids") or []:` |
| 29635 | for | 4 | if |  | no | no | `for _p in (all_proposals or []):` |
| 29637 | if | 5 | for |  | no | no | `if not _pid:` |
| 29638 | continue | 6 | if |  | no | no | `continue` |
| 29639 | for | 5 | for |  | no | no | `for _q in _p.get("target_qids") or []:` |
| 29643 | for | 4 | if |  | no | no | `for _entry in _applied_patch_entries:` |
| 29651 | if | 5 | for |  | no | no | `if _ap_pid:` |
| 29663 | if | 4 | if |  | no | no | `if _t4_rows:` |
| 29670 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 29713 | try | 2 | try |  | no | no | `try:` |
| 29743 | for | 3 | try |  | no | no | `for _entry in (apply_log.get("applied") or []):` |
| 29746 | if | 4 | for |  | no | no | `if _entry_ag:` |
| 29791 | try | 3 | try |  | no | no | `try:` |
| 29800 | if | 4 | try |  | no | no | `if _ag_id_for_canonical and _typed_canonical is not None:` |
| 29804 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 29811 | try | 3 | try |  | no | no | `try:` |
| 29839 | except_handler | 3 | try |  | no | no | `except Exception as _accept_inp_exc:` |
| 29844 | try | 4 | except_handler |  | no | no | `try:` |
| 29853 | except_handler | 4 | except_handler |  | no | no | `except Exception:` |
| 29859 | raise | 4 | except_handler |  | no | no | `raise` |
| 29882 | try | 3 | try |  | no | no | `try:` |
| 29915 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 29921 | except_handler | 2 | try |  | no | no | `except Exception as _accept_stage_exc:` |
| 29922 | try | 3 | except_handler |  | no | no | `try:` |
| 29926 | if | 4 | try |  | no | no | `if _typed_on():` |
| 29940 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 29955 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 29956 | raise | 4 | if |  | no | no | `raise` |
| 29963 | if | 2 | try |  | no | no | `if not gate_result.get("passed"):` |
| 29967 | try | 3 | if |  | no | no | `try:` |
| 29975 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29986 | try | 3 | if |  | no | no | `try:` |
| 29996 | if | 4 | try |  | no | no | `if not _restore_decision.get("verified", True):` |
| 30025 | raise | 5 | if |  | no | no | `raise FailedRollbackVerification(` |
| 30028 | except_handler | 3 | if |  | no | no | `except FailedRollbackVerification:` |
| 30029 | raise | 4 | except_handler |  | no | no | `raise` |
| 30030 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30046 | try | 3 | if |  | no | no | `try:` |
| 30064 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30075 | if | 3 | if |  | no | no | `if pending_action_groups:` |
| 30082 | for | 3 | if |  | no | no | `for lk in lever_keys:` |
| 30104 | for | 3 | if |  | no | no | `for qid, tid in _fail_tmap.items():` |
| 30106 | if | 4 | for |  | no | no | `if qid in _fail_qids:` |
| 30108 | if | 5 | if |  | no | no | `elif "regressions" in gate_result:` |
| 30111 | if | 3 | if |  | no | no | `if _fail_run_id:` |
| 30131 | for | 3 | if |  | no | no | `for _r in _regressions:` |
| 30132 | if | 4 | for |  | no | no | `if _r.get("judge") == "control_plane_acceptance":` |
| 30136 | break | 5 | if |  | no | no | `break` |
| 30190 | try | 3 | if |  | no | no | `try:` |
| 30192 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30195 | for | 3 | if |  | no | no | `for _qid, _cand_row in _candidate_by_qid_for_delta.items():` |
| 30197 | if | 4 | for |  | no | no | `if not _gt_sql:` |
| 30198 | continue | 5 | if |  | no | no | `continue` |
| 30200 | try | 4 | for |  | no | no | `try:` |
| 30209 | except_handler | 4 | for |  | no | no | `except Exception:` |
| 30210 | continue | 5 | except_handler |  | no | no | `continue` |
| 30211 | if | 4 | for |  | no | no | `if _delta.get("improved") or _delta.get("remaining"):` |
| 30254 | try | 3 | if |  | no | no | `try:` |
| 30260 | for | 4 | try |  | no | no | `for _r in gate_result.get("regressions") or []:` |
| 30261 | for | 5 | for |  | no | no | `for _q in _r.get("blocking_qids") or []:` |
| 30262 | if | 6 | for |  | no | no | `if _q:` |
| 30267 | if | 4 | try |  | no | no | `if not _regressed_qids and prev_failure_qids is not None:` |
| 30281 | if | 4 | try |  | no | no | `if _mined_insights:` |
| 30292 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30299 | try | 3 | if |  | no | no | `try:` |
| 30304 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30312 | if | 3 | if |  | no | no | `if _mined_insights:` |
| 30313 | try | 4 | if |  | no | no | `try:` |
| 30329 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 30335 | for | 3 | if |  | no | no | `for p in patches:` |
| 30345 | if | 4 | for |  | no | no | `if ft and tgt:` |
| 30363 | for | 3 | if |  | no | no | `for c in clusters:` |
| 30365 | if | 4 | for |  | no | no | `if source_cids and cid not in source_cids:` |
| 30366 | continue | 5 | if |  | no | no | `continue` |
| 30369 | if | 4 | for |  | no | no | `if not rc_ft or not _should_mark_tried_lever_aware:` |
| 30370 | continue | 5 | if |  | no | no | `continue` |
| 30388 | if | 4 | for |  | no | no | `if len(_distinct_lever_sets) >= 2:` |
| 30390 | if | 3 | if |  | no | no | `if not _should_mark_tried_lever_aware:` |
| 30395 | try | 3 | if |  | no | no | `try:` |
| 30417 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30440 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 30441 | try | 4 | if |  | no | no | `try:` |
| 30460 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 30500 | continue | 3 | if |  | no | no | `continue` |
| 30504 | for | 2 | try |  | no | no | `for lk in lever_keys:` |
| 30510 | try | 2 | try |  | no | no | `try:` |
| 30524 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30529 | try | 2 | try |  | no | no | `try:` |
| 30531 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30548 | try | 2 | try |  | no | no | `try:` |
| 30582 | except_handler | 2 | try |  | no | no | `except Exception as _observed_effect_exc:` |
| 30583 | try | 3 | except_handler |  | no | no | `try:` |
| 30587 | if | 4 | try |  | no | no | `if _typed_on():` |
| 30601 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 30613 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 30614 | raise | 4 | if |  | no | no | `raise` |
| 30631 | for | 2 | try |  | no | no | `for qid, tid in _full_trace_map.items():` |
| 30633 | if | 3 | for |  | no | no | `if qid in _full_failures:` |
| 30636 | if | 2 | try |  | no | no | `if _full_run_id:` |
| 30639 | try | 2 | try |  | no | no | `try:` |
| 30642 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30645 | try | 2 | try |  | no | no | `try:` |
| 30647 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30650 | try | 2 | try |  | no | no | `try:` |
| 30654 | if | 3 | try |  | no | no | `if _persist_data:` |
| 30656 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30686 | try | 2 | try |  | no | no | `try:` |
| 30688 | if | 3 | try |  | no | no | `if (` |
| 30697 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30732 | try | 2 | try |  | no | no | `try:` |
| 30737 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30741 | if | 2 | try |  | no | no | `if _acc_delta >= 1.0:` |
| 30782 | if | 2 | try |  | no | no | `if _regression_debt_qids_for_next_iteration:` |
| 30798 | if | 2 | try |  | no | no | `if new_refs:` |
| 30801 | if | 2 | try |  | no | no | `if new_hashes:` |
| 30814 | if | 2 | try |  | no | no | `if post_instructions:` |
| 30839 | if | 2 | try |  | no | no | `if _original_instruction_sections:` |
| 30847 | try | 2 | try |  | no | no | `try:` |
| 30859 | if | 3 | try |  | no | no | `if not _diag_rows and isinstance(full_result, dict):` |
| 30861 | if | 4 | if |  | no | no | `if isinstance(_rows_json, list):` |
| 30863 | if | 5 | if |  | no | no | `elif isinstance(_rows_json, str):` |
| 30864 | try | 6 | if |  | no | no | `try:` |
| 30867 | except_handler | 6 | if |  | no | no | `except (ValueError, TypeError):` |
| 30869 | for | 3 | try |  | no | no | `for _r in _diag_rows:` |
| 30870 | if | 4 | for |  | no | no | `if not isinstance(_r, dict):` |
| 30871 | continue | 5 | if |  | no | no | `continue` |
| 30872 | for | 4 | for |  | no | no | `for _log in (_r.get("_asi_extraction_log") or []):` |
| 30873 | if | 5 | for |  | no | no | `if not isinstance(_log, dict):` |
| 30874 | continue | 6 | if |  | no | no | `continue` |
| 30878 | if | 3 | try |  | no | no | `if _asi_total:` |
| 30889 | if | 4 | if |  | no | no | `if _none_pct > 50.0:` |
| 30925 | if | 3 | try |  | no | no | `if _pre_arb is not None:` |
| 30929 | if | 3 | try |  | no | no | `if _adj is not None:` |
| 30937 | if | 3 | try |  | no | no | `if isinstance(_bcr, (int, float)) and _bcr is not None:` |
| 30950 | if | 4 | if |  | no | no | `if _rescue > 0.30:` |
| 30959 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30966 | try | 2 | try |  | no | no | `try:` |
| 30968 | if | 3 | try |  | no | no | `if not _eval_rows:` |
| 30970 | if | 4 | if |  | no | no | `if isinstance(_eval_rows_json, str):` |
| 30972 | try | 5 | if |  | no | no | `try:` |
| 30974 | except_handler | 5 | if |  | no | no | `except (ValueError, TypeError):` |
| 30976 | if | 5 | if |  | no | no | `elif isinstance(_eval_rows_json, list):` |
| 30978 | if | 3 | try |  | no | no | `if _eval_rows:` |
| 30983 | if | 4 | if |  | no | no | `if _mine_result.get("total_applied", 0) > 0:` |
| 30987 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30997 | try | 2 | try |  | no | no | `try:` |
| 31021 | try | 3 | try |  | no | no | `try:` |
| 31025 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 31030 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31054 | try | 2 | try |  | no | no | `try:` |
| 31064 | if | 3 | try |  | no | no | `if _journey_report is not None:` |
| 31068 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31078 | if | 2 | try |  | no | no | `if _journey_report is not None:` |
| 31087 | if | 3 | if |  | no | no | `if not _phase_a_result.success:` |
| 31108 | try | 2 | try |  | no | no | `try:` |
| 31121 | if | 3 | try |  | no | no | `if _decision_records:` |
| 31139 | try | 4 | if |  | no | no | `try:` |
| 31148 | for | 5 | try |  | no | no | `for _r in _decision_records:` |
| 31150 | if | 6 | for |  | no | no | `if not _qid or _qid in _qids_seen:` |
| 31151 | continue | 7 | if |  | no | no | `continue` |
| 31152 | if | 6 | for |  | no | no | `if getattr(_r, "outcome", None) != _DecisionOutcome.UNRESOLVED:` |
| 31153 | continue | 7 | if |  | no | no | `continue` |
| 31158 | if | 6 | for |  | no | no | `if _classification.bucket is not None:` |
| 31161 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31172 | try | 4 | if |  | no | no | `try:` |
| 31176 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31215 | if | 4 | if |  | no | no | `if not _phase_b_result.success:` |
| 31232 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31237 | try | 3 | except_handler |  | no | no | `try:` |
| 31239 | if | 4 | try |  | no | no | `if _mlflow_phase_b_partial.active_run() is not None:` |
| 31243 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 31252 | try | 2 | try |  | no | no | `try:` |
| 31271 | if | 3 | try |  | no | no | `if _iter_record_count == 0:` |
| 31288 | try | 4 | if |  | no | no | `try:` |
| 31290 | if | 5 | try |  | no | no | `if _mlflow_no_rec.active_run() is not None:` |
| 31300 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31311 | try | 4 | if |  | no | no | `try:` |
| 31321 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31323 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31333 | try | 2 | try |  | no | no | `try:` |
| 31356 | except_handler | 2 | try |  | no | no | `except Exception as _orphan_rca_exc:` |
| 31357 | try | 3 | except_handler |  | no | no | `try:` |
| 31361 | if | 4 | try |  | no | no | `if _typed_on():` |
| 31375 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 31387 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 31388 | raise | 4 | if |  | no | no | `raise` |
| 31394 | try | 2 | try |  | no | no | `try:` |
| 31416 | if | 3 | try |  | no | no | `if _w1_count:` |
| 31421 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31438 | try | 2 | try |  | no | no | `try:` |
| 31442 | if | 3 | try |  | no | no | `if productive_iteration_budget_enabled():` |
| 31449 | if | 4 | if |  | no | no | `if _iter_applied_count == 0:` |
| 31457 | if | 5 | if |  | no | no | `if _iter_no_op_cause:` |
| 31472 | if | 4 | if |  | no | no | `if _iter_budget_key not in _iter_emitted_keys:` |
| 31491 | if | 5 | if |  | no | no | `if not _iter_consumed:` |
| 31493 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31502 | try | 2 | try |  | no | no | `try:` |
| 31521 | try | 3 | try |  | no | no | `try:` |
| 31540 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 31559 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31567 | try | 2 | try |  | no | no | `try:` |
| 31569 | try | 3 | try |  | no | no | `try:` |
| 31575 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 31600 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31612 | try | 2 | try |  | no | no | `try:` |
| 31618 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31646 | finally | 1 | for |  | YES | YES | `_f_cur = locals().get("_current_iter_inputs")` |
| 31647 | if | 2 | finally |  | YES | YES | `if isinstance(_f_cur, dict) and not _f_cur.get(` |
| 31650 | try | 3 | if |  | YES | YES | `try:` |
| 31680 | except_handler | 3 | if |  | YES | YES | `except Exception:` |
| 31698 | if | 2 | finally |  | YES | YES | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 31699 | try | 3 | if |  | YES | YES | `try:` |
| 31703 | if | 4 | try |  | YES | YES | `if _exc_val is not None:` |
| 31723 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 31744 | try | 2 | finally |  | YES | YES | `try:` |
| 31761 | if | 3 | try |  | YES | YES | `if iteration_terminal_policy_enabled():` |
| 31762 | try | 4 | if |  | YES | YES | `try:` |
| 31766 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31773 | if | 4 | if |  | YES | YES | `if str(_iter_terminal_reason or "") != "accepted":` |
| 31805 | if | 5 | if |  | YES | YES | `if _router_action.add_to_forbidden_set:` |
| 31834 | if | 5 | if |  | YES | YES | `if _abort_break:` |
| 31848 | except_handler | 2 | finally |  | no | no | `except Exception:` |
| 31865 | try | 2 | finally |  | YES | YES | `try:` |
| 31874 | if | 3 | try |  | YES | YES | `if candidate_ledger_enabled():` |
| 31922 | try | 4 | if |  | YES | YES | `try:` |
| 31926 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31936 | except_handler | 2 | finally |  | no | no | `except Exception:` |
| 31950 | if | 2 | finally |  | YES | YES | `if _loop_should_abort:` |
| 31963 | break | 3 | if |  | no | no | `break` |
| 31974 | try | 0 | module |  | YES | YES | `try:` |
| 31976 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 31982 | if | 0 | module |  | YES | YES | `if _phase35_drained:` |
| 31984 | for | 1 | if |  | no | no | `for _snap in _replay_fixture_iterations:` |
| 31985 | try | 2 | for |  | no | no | `try:` |
| 31987 | except_handler | 2 | for |  | no | no | `except Exception:` |
| 31988 | continue | 3 | except_handler |  | no | no | `continue` |
| 31989 | for | 1 | if |  | no | no | `for _call in _phase35_drained:` |
| 31990 | try | 2 | for |  | no | no | `try:` |
| 31992 | except_handler | 2 | for |  | no | no | `except Exception:` |
| 31995 | if | 2 | for |  | no | no | `if _snap is not None:` |
| 31997 | try | 0 | module |  | YES | YES | `try:` |
| 31999 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 32057 | for | 0 | module |  | YES | YES | `for _rb_entry in reflection_buffer:` |
| 32058 | if | 1 | for |  | YES | YES | `if _rb_entry.get("accepted"):` |
| 32059 | continue | 2 | if |  | no | no | `continue` |
| 32060 | if | 1 | for |  | YES | YES | `if _rb_entry.get("escalation_handled"):` |
| 32061 | continue | 2 | if |  | no | no | `continue` |
| 32063 | if | 0 | module |  | YES | YES | `if len(ags_rolled_back) and _rb_class_counter:` |
| 32074 | if | 0 | module |  | YES | YES | `if lever_changes:` |
| 32076 | for | 1 | if |  | no | no | `for lc in lever_changes:` |
| 32080 | for | 2 | for |  | no | no | `for p in lc.get("patches", []):` |
| 32084 | if | 1 | if |  | YES | YES | `elif not ags_accepted:` |
| 32089 | for | 0 | module |  | YES | YES | `for sname, sval in sorted(best_scores.items()):` |
| 32099 | try | 0 | module |  | YES | YES | `try:` |
| 32123 | try | 1 | try |  | YES | YES | `try:` |
| 32142 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32154 | try | 1 | try |  | YES | YES | `try:` |
| 32165 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32179 | try | 1 | try |  | YES | YES | `try:` |
| 32186 | if | 2 | try |  | YES | YES | `if _replay_fixture_summary is not None:` |
| 32190 | for | 3 | if |  | YES | YES | `for _per in (` |
| 32193 | if | 4 | for |  | YES | YES | `if int(_per.get("eval_rows") or 0) == 0:` |
| 32204 | if | 2 | try |  | YES | YES | `if _is_empty:` |
| 32211 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32221 | if | 1 | try |  | YES | YES | `if _dual_emit_on():` |
| 32225 | try | 2 | if |  | YES | YES | `try:` |
| 32238 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 32271 | try | 1 | try |  | YES | YES | `try:` |
| 32274 | if | 2 | try |  | YES | YES | `if mlflow.active_run() is not None:` |
| 32283 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32288 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 32300 | try | 0 | module |  | YES | YES | `try:` |
| 32327 | if | 1 | try |  | YES | YES | `if gso_run_manifest_v2_enabled():` |
| 32328 | try | 2 | if |  | YES | YES | `try:` |
| 32338 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 32340 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 32359 | try | 0 | module |  | YES | YES | `try:` |
| 32371 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 32397 | try | 0 | module |  | YES | YES | `try:` |
| 32461 | try | 1 | try |  | YES | YES | `try:` |
| 32463 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32483 | try | 1 | try |  | YES | YES | `try:` |
| 32488 | except_handler | 1 | try |  | YES | YES | `except (NameError, AttributeError):` |
| 32526 | for | 1 | try |  | YES | YES | `for _i in _phase_h_iterations_completed:` |
| 32529 | if | 2 | for |  | YES | YES | `if _trace is None:` |
| 32534 | if | 3 | if |  | YES | YES | `if not _summary:` |
| 32564 | try | 1 | try |  | YES | YES | `try:` |
| 32568 | if | 2 | try |  | YES | YES | `if _trend_enabled():` |
| 32594 | try | 3 | if |  | YES | YES | `try:` |
| 32597 | if | 4 | try |  | YES | YES | `if _by_root:` |
| 32616 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 32621 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32643 | if | 1 | try |  | YES | YES | `if _phase_h_anchor_run_id:` |
| 32644 | try | 2 | if |  | no | no | `try:` |
| 32751 | try | 3 | try |  | no | no | `try:` |
| 32770 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 32788 | try | 3 | try |  | no | no | `try:` |
| 32792 | if | 4 | try |  | no | no | `if _ledger_enabled_phase_h():` |
| 32800 | if | 5 | if |  | no | no | `if _os_for_ledger_copy.path.exists(_ledger_src):` |
| 32806 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 32825 | try | 3 | try |  | no | no | `try:` |
| 32829 | if | 4 | try |  | no | no | `if _phase_h_totality_enabled():` |
| 32867 | if | 5 | if |  | no | no | `if _totality_violation is not None:` |
| 32880 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 32941 | try | 3 | try |  | no | no | `try:` |
| 32975 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 32999 | for | 3 | try |  | no | no | `for _k, _v in _paths.items():` |
| 33000 | if | 4 | for |  | no | no | `if _k == "iterations":` |
| 33001 | for | 5 | if |  | no | no | `for _iter_paths in (_v or {}).values():` |
| 33002 | for | 6 | for |  | no | no | `for _path in (_iter_paths or {}).values():` |
| 33003 | if | 7 | for |  | no | no | `if isinstance(_path, str):` |
| 33005 | if | 5 | if |  | no | no | `elif isinstance(_v, str):` |
| 33010 | try | 3 | try |  | no | no | `try:` |
| 33020 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 33030 | if | 3 | try |  | no | no | `if not _completeness["complete"]:` |
| 33031 | try | 4 | if |  | no | no | `try:` |
| 33049 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 33056 | except_handler | 2 | if |  | no | no | `except Exception as _phase_h_upload_exc:` |
| 33080 | except_handler | 0 | module |  | no | no | `except Exception as _phase_h_render_exc:` |
| 33098 | if | 1 | except_handler |  | no | no | `if "_bundle_assembly_failed_payloads" not in locals():` |
| 33101 | if | 1 | except_handler |  | no | no | `if "_bundle_assembly_incomplete_payloads" not in locals():` |
| 33127 | try | 0 | module |  | YES | YES | `try:` |
| 33128 | for | 1 | try |  | YES | YES | `for _it_idx, _it_trace in (` |
| 33131 | for | 2 | for |  | YES | YES | `for _rec in getattr(_it_trace, "decision_records", ()) or ():` |
| 33137 | if | 3 | for |  | no | no | `if _rec_dict is None:` |
| 33138 | continue | 4 | if |  | no | no | `continue` |
| 33139 | if | 3 | for |  | no | no | `if str(_rec_dict.get("reason_code") or "") == (` |
| 33143 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 33148 | try | 0 | module |  | YES | YES | `try:` |
| 33166 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 33231 | try | 0 | module |  | YES | YES | `try:` |
| 33234 | if | 1 | try |  | YES | YES | `if any(count > 0 for count in _narrowing_summary["hits"].values()):` |
| 33243 | except_handler | 0 | module |  | no | no | `except Exception as _narrowing_log_exc:` |
| 33251 | try | 0 | module |  | YES | YES | `try:` |
| 33254 | if | 1 | try |  | YES | YES | `if any(c > 0 for c in _l5_summary["hits"].values()) or _l5_summary["shadow_compa` |
| 33264 | except_handler | 0 | module |  | no | no | `except Exception as _l5_log_exc:` |
| 33270 | try | 0 | module |  | YES | YES | `try:` |
| 33273 | if | 1 | try |  | YES | YES | `if (_ts_summary["discovery_calls"] > 0` |
| 33286 | except_handler | 0 | module |  | no | no | `except Exception as _ts_log_exc:` |
| 33292 | try | 0 | module |  | YES | YES | `try:` |
| 33295 | if | 1 | try |  | YES | YES | `if (_re_summary["shadow_comparisons"] > 0` |
| 33306 | except_handler | 0 | module |  | no | no | `except Exception as _re_log_exc:` |
| 33330 | try | 0 | module |  | YES | YES | `try:` |
| 33351 | except_handler | 0 | module |  | no | no | `except Exception as _upload_exc:` |
| 33352 | try | 1 | except_handler |  | no | no | `try:` |
| 33358 | except_handler | 1 | except_handler |  | no | no | `except Exception:` |
| 33365 | return | 0 | module |  | YES | YES | `return _build_loop_out_with_pretty_print(` |
