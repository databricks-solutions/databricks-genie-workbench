# Harness Control-Flow Audit

Function: `_run_lever_loop`  
Line range: 17458–33256  
Total branch points: 1386  

## Reachability summary

* `airline_run_59a173d3`: 4276 lines executed
* `seven_now_run_ab65fefe`: 4333 lines executed

## Branch points

| lineno | type | depth | parent | detail | reached:airline_run_59a173d3 | reached:seven_now_run_ab65fefe | snippet |
|---|---|---|---|---|---|---|---|
| 17544 | try | 0 | module |  | YES | YES | `try:` |
| 17558 | if | 1 | try |  | YES | YES | `if _chunk_d_enabled_rm():` |
| 17566 | try | 2 | if |  | no | no | `try:` |
| 17570 | try | 3 | try |  | no | no | `try:` |
| 17574 | for | 4 | try |  | no | no | `for _rm_tag_key in ("jobId", "multitaskParentRunId", "jobRunId", "runId"):` |
| 17575 | try | 5 | for |  | no | no | `try:` |
| 17577 | if | 6 | try |  | no | no | `if _rm_val.isDefined():` |
| 17579 | except_handler | 5 | for |  | no | no | `except Exception:` |
| 17581 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 17583 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 17617 | try | 2 | if |  | no | no | `try:` |
| 17619 | if | 3 | try |  | no | no | `if _rm_active_run is not None:` |
| 17621 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 17656 | try | 2 | if |  | no | no | `try:` |
| 17678 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 17682 | try | 2 | if |  | YES | YES | `try:` |
| 17684 | if | 3 | try |  | no | no | `if _legacy_active_run is not None:` |
| 17686 | except_handler | 2 | if |  | YES | YES | `except Exception:` |
| 17696 | try | 1 | try |  | YES | YES | `try:` |
| 17698 | except_handler | 1 | try |  | YES | YES | `except NameError:` |
| 17709 | if | 1 | try |  | YES | YES | `if gso_run_manifest_v2_enabled():` |
| 17710 | try | 2 | if |  | YES | YES | `try:` |
| 17720 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 17722 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 17724 | try | 0 | module |  | YES | YES | `try:` |
| 17726 | if | 1 | try |  | YES | YES | `if _mlflow_run_analysis.active_run() is not None:` |
| 17733 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 17750 | try | 0 | module |  | YES | YES | `try:` |
| 17761 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 17766 | if | 0 | module |  | YES | YES | `if not _phase_h_anchor_run_id:` |
| 17767 | try | 1 | if |  | YES | YES | `try:` |
| 17770 | if | 2 | try |  | YES | YES | `if _active_phase_h is not None:` |
| 17772 | except_handler | 1 | if |  | no | no | `except Exception:` |
| 17885 | if | 0 | module |  | YES | YES | `if resume_state.get("prev_scores"):` |
| 17887 | if | 0 | module |  | YES | YES | `if resume_state.get("prev_model_id"):` |
| 17889 | if | 0 | module |  | YES | YES | `if resume_state.get("prev_accuracy"):` |
| 17900 | if | 0 | module |  | YES | YES | `if "_pre_arbiter/overall_accuracy" not in best_scores:` |
| 17901 | if | 1 | if |  | YES | YES | `if "_pre_arbiter/result_correctness" in best_scores:` |
| 17932 | try | 0 | module |  | YES | YES | `try:` |
| 17953 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 17978 | try | 0 | module |  | YES | YES | `try:` |
| 17980 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 17982 | if | 0 | module |  | YES | YES | `if not _run_row.get("config_snapshot"):` |
| 17991 | try | 1 | if |  | YES | YES | `try:` |
| 17998 | except_handler | 1 | if |  | no | no | `except Exception:` |
| 18091 | try | 0 | module |  | YES | YES | `try:` |
| 18093 | if | 1 | try |  | YES | YES | `if _mlflow_phase_b_init.active_run() is not None:` |
| 18097 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18110 | for | 0 | module |  | YES | YES | `for ov in _judge_overrides:` |
| 18111 | try | 1 | for |  | no | no | `try:` |
| 18113 | if | 2 | try |  | no | no | `if "Genie answer is actually fine" in feedback or "Correct" in feedback:` |
| 18115 | if | 3 | if |  | no | no | `if genie_sql:` |
| 18121 | if | 3 | if |  | no | no | `elif "both answers are wrong" in feedback or "Both Wrong" in feedback:` |
| 18128 | if | 4 | if |  | no | no | `elif "Ambiguous" in feedback:` |
| 18135 | except_handler | 1 | for |  | no | no | `except Exception:` |
| 18138 | if | 0 | module |  | YES | YES | `if _human_sql_fixes:` |
| 18139 | try | 1 | if |  | no | no | `try:` |
| 18146 | except_handler | 1 | if |  | no | no | `except Exception:` |
| 18162 | try | 0 | module |  | YES | YES | `try:` |
| 18170 | if | 1 | try |  | YES | YES | `if _snippet_repair_result.get("rewritten", 0) > 0:` |
| 18173 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18205 | if | 0 | module |  | YES | YES | `if uc_columns:` |
| 18213 | if | 0 | module |  | YES | YES | `if not enrichment_done:` |
| 18223 | with | 1 | if |  | YES | YES | `with _mlflow_legacy_enr.start_run(run_name=_legacy_enr_run_name, nested=True):` |
| 18239 | if | 2 | with |  | YES | YES | `if enrichment_result.get("total_enriched", 0) > 0 or enrichment_result.get("tabl` |
| 18245 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18251 | if | 2 | with |  | YES | YES | `if join_result.get("total_applied", 0) > 0:` |
| 18257 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18263 | if | 2 | with |  | YES | YES | `if meta_result.get("description_generated") or meta_result.get("questions_genera` |
| 18269 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18283 | if | 2 | with |  | YES | YES | `if _legacy_miner_out["total_applied"] or _legacy_miner_out["keep_in_prose_count"` |
| 18289 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18295 | if | 2 | with |  | YES | YES | `if (` |
| 18304 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18309 | if | 2 | with |  | YES | YES | `if ENABLE_PREFLIGHT_EXAMPLE_SQL_SYNTHESIS:` |
| 18310 | try | 3 | if |  | YES | YES | `try:` |
| 18321 | if | 4 | try |  | YES | YES | `if legacy_preflight_result.get("applied", 0) > 0:` |
| 18327 | if | 5 | if |  | no | no | `if uc_columns:` |
| 18329 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 18355 | if | 2 | with |  | no | no | `if (` |
| 18364 | if | 3 | if |  | no | no | `if uc_columns:` |
| 18399 | if | 1 | if |  | no | no | `if uc_columns:` |
| 18401 | if | 1 | if |  | no | no | `if enrichment_model_id:` |
| 18415 | try | 0 | module |  | YES | YES | `try:` |
| 18427 | if | 1 | try |  | YES | YES | `if _current_instr and _current_instr.strip() and _is_unstructured(_current_instr` |
| 18436 | if | 2 | if |  | no | no | `if _restructured_secs:` |
| 18439 | for | 3 | if |  | no | no | `for _sec in INSTRUCTION_SECTION_ORDER:` |
| 18441 | if | 4 | for |  | no | no | `if not _lines_list:` |
| 18442 | continue | 5 | if |  | no | no | `continue` |
| 18444 | for | 4 | for |  | no | no | `for _ln in _lines_list:` |
| 18446 | if | 5 | for |  | no | no | `if not _s:` |
| 18447 | continue | 6 | if |  | no | no | `continue` |
| 18448 | if | 5 | for |  | no | no | `if not _s.startswith("- "):` |
| 18454 | if | 3 | if |  | no | no | `if len(_restructured_text.strip()) >= len(_current_instr.strip()) * 0.5:` |
| 18456 | try | 4 | if |  | no | no | `try:` |
| 18470 | if | 5 | try |  | no | no | `if uc_columns:` |
| 18490 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 18510 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18518 | try | 0 | module |  | YES | YES | `try:` |
| 18522 | if | 1 | try |  | YES | YES | `if _pre_loop_instr and _pre_loop_instr.strip():` |
| 18532 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18542 | if | 0 | module |  | YES | YES | `if baseline_iter:` |
| 18544 | if | 1 | if |  | YES | YES | `if isinstance(rows_json, list):` |
| 18548 | if | 2 | if |  | no | no | `elif isinstance(rows_json, str):` |
| 18549 | try | 3 | if |  | no | no | `try:` |
| 18553 | except_handler | 3 | if |  | no | no | `except (json.JSONDecodeError, TypeError):` |
| 18724 | try | 0 | module |  | YES | YES | `try:` |
| 18742 | if | 1 | try |  | YES | YES | `if (` |
| 18746 | try | 2 | if |  | YES | YES | `try:` |
| 18767 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 18773 | if | 1 | try |  | YES | YES | `if not _baseline_rows_seed:` |
| 18781 | if | 1 | try |  | YES | YES | `if _seeded:` |
| 18789 | if | 2 | if |  | no | no | `elif _baseline_rows_seed:` |
| 18806 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 18860 | if | 0 | module |  | YES | YES | `if baseline_iter:` |
| 18861 | try | 1 | if |  | YES | YES | `try:` |
| 18865 | except_handler | 1 | if |  | no | no | `except Exception:` |
| 18943 | for | 0 | module |  | YES | YES | `for _iter_num in range(1, max_iterations + 1):` |
| 18958 | if | 1 | for |  | YES | YES | `if not _was_collision_skip_this_iter:` |
| 18997 | try | 1 | for |  | YES | YES | `try:` |
| 19012 | if | 2 | try |  | YES | YES | `if reserved_recovery_budget_enabled():` |
| 19041 | if | 3 | if |  | YES | YES | `if _budget_action == RecoveryBudgetAction.SKIP_EARLY_TERMINATE:` |
| 19044 | if | 4 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 19045 | try | 5 | if |  | no | no | `try:` |
| 19055 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 19061 | break | 4 | if |  | no | no | `break` |
| 19068 | if | 2 | try |  | YES | YES | `if arbiter_objective_complete_from_counts(` |
| 19080 | break | 3 | if |  | no | no | `break` |
| 19081 | if | 2 | try |  | YES | YES | `if all_thresholds_met(best_scores, thresholds):` |
| 19095 | if | 2 | try |  | YES | YES | `if _prev_terminal_state:` |
| 19096 | try | 3 | if |  | YES | YES | `try:` |
| 19107 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 19112 | if | 2 | try |  | YES | YES | `if legacy_plateau_allows_stop(` |
| 19138 | try | 3 | if |  | no | no | `try:` |
| 19140 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 19180 | try | 3 | if |  | no | no | `try:` |
| 19192 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 19202 | for | 3 | if |  | no | no | `for _rb in reflection_buffer:` |
| 19203 | for | 4 | for |  | no | no | `for _delta in _rb.get("sql_shape_deltas", []) or []:` |
| 19205 | if | 5 | for |  | no | no | `if _qid and (_delta.get("remaining") or _delta.get("improved")):` |
| 19228 | if | 3 | if |  | no | no | `if _resolved.should_continue:` |
| 19235 | continue | 4 | if |  | no | no | `continue` |
| 19278 | try | 3 | if |  | no | no | `try:` |
| 19317 | try | 4 | try |  | no | no | `try:` |
| 19329 | except_handler | 4 | try |  | no | no | `except NameError:` |
| 19377 | except_handler | 3 | if |  | no | no | `except Exception as _learning_stage_exc:` |
| 19378 | try | 4 | except_handler |  | no | no | `try:` |
| 19382 | if | 5 | try |  | no | no | `if _typed_on():` |
| 19396 | except_handler | 4 | except_handler |  | no | no | `except Exception:` |
| 19410 | break | 3 | if |  | no | no | `break` |
| 19411 | if | 2 | try |  | YES | YES | `if (` |
| 19429 | if | 2 | try |  | YES | YES | `if _diverging:` |
| 19443 | break | 3 | if |  | no | no | `break` |
| 19452 | for | 2 | try |  | YES | YES | `for _rb_entry in reversed(reflection_buffer):` |
| 19453 | if | 3 | for |  | YES | YES | `if _rb_entry.get("escalation_handled"):` |
| 19454 | continue | 4 | if |  | no | no | `continue` |
| 19455 | if | 3 | for |  | YES | YES | `if _rb_entry.get("accepted"):` |
| 19456 | break | 4 | if |  | no | no | `break` |
| 19457 | if | 3 | for |  | YES | YES | `if _rb_entry.get("rollback_class") == _RC.CONTENT_REGRESSION.value:` |
| 19463 | continue | 4 | if |  | YES | YES | `continue` |
| 19464 | if | 2 | try |  | YES | YES | `if _consecutive_rb >= CONSECUTIVE_ROLLBACK_LIMIT:` |
| 19469 | break | 3 | if |  | no | no | `break` |
| 19473 | for | 2 | try |  | YES | YES | `for _esc_entry in reversed(reflection_buffer):` |
| 19474 | if | 3 | for |  | YES | YES | `if not _esc_entry.get("escalation_handled"):` |
| 19475 | break | 4 | if |  | YES | YES | `break` |
| 19477 | if | 3 | for |  | no | no | `if _last_esc_type is None:` |
| 19479 | if | 3 | for |  | no | no | `if _esc_reason == _last_esc_type:` |
| 19482 | break | 4 | if |  | no | no | `break` |
| 19483 | if | 2 | try |  | YES | YES | `if _consecutive_esc >= CONSECUTIVE_ESCALATION_LIMIT:` |
| 19498 | break | 3 | if |  | no | no | `break` |
| 19592 | if | 2 | try |  | YES | YES | `if _forced_synthesis_proposals_carryover:` |
| 19688 | try | 2 | try |  | YES | YES | `try:` |
| 19689 | if | 3 | try |  | YES | YES | `if reflection_buffer:` |
| 19702 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19717 | try | 2 | try |  | YES | YES | `try:` |
| 19719 | if | 3 | try |  | YES | YES | `if _rot_records:` |
| 19723 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19735 | try | 2 | try |  | YES | YES | `try:` |
| 19736 | for | 3 | try |  | YES | YES | `for _sc in soft_signal_clusters or []:` |
| 19738 | if | 4 | for |  | no | no | `if _scid and _scid not in _soft_clusters_seen_run:` |
| 19740 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19747 | try | 2 | try |  | YES | YES | `try:` |
| 19749 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19775 | if | 2 | try |  | YES | YES | `if not (_latest_eval_result or {}).get("question_ids"):` |
| 19776 | try | 3 | if |  | no | no | `try:` |
| 19780 | if | 4 | try |  | no | no | `if _lazy_seed:` |
| 19792 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 19804 | try | 2 | try |  | YES | YES | `try:` |
| 19838 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19858 | for | 2 | try |  | YES | YES | `for _q in _already_passing_set:` |
| 19860 | for | 2 | try |  | YES | YES | `for _q in _hard_qid_set:` |
| 19862 | for | 2 | try |  | YES | YES | `for _q in _soft_qid_set:` |
| 19864 | for | 2 | try |  | YES | YES | `for _q in _gt_corr_qid_set:` |
| 19868 | for | 2 | try |  | YES | YES | `for _c in (clusters or []):` |
| 19870 | if | 3 | for |  | YES | YES | `if _cid:` |
| 19872 | for | 3 | for |  | YES | YES | `for _q in (_c.get("question_ids") or []):` |
| 19874 | if | 4 | for |  | YES | YES | `if _qstr and _cid:` |
| 19885 | try | 2 | try |  | YES | YES | `try:` |
| 19898 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 19968 | try | 2 | try |  | YES | YES | `try:` |
| 19984 | except_handler | 2 | try |  | no | no | `except Exception as _exc_eval:` |
| 19985 | try | 3 | except_handler |  | no | no | `try:` |
| 19989 | if | 4 | try |  | no | no | `if _typed_on():` |
| 20003 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 20016 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 20017 | raise | 4 | if |  | no | no | `raise` |
| 20023 | try | 2 | try |  | YES | YES | `try:` |
| 20056 | except_handler | 2 | try |  | no | no | `except Exception as _cluster_exc:` |
| 20057 | try | 3 | except_handler |  | no | no | `try:` |
| 20061 | if | 4 | try |  | no | no | `if _typed_on():` |
| 20075 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 20088 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 20089 | raise | 4 | if |  | no | no | `raise` |
| 20095 | try | 2 | try |  | YES | YES | `try:` |
| 20109 | except_handler | 2 | try |  | no | no | `except Exception as _rca_formed_exc:` |
| 20110 | try | 3 | except_handler |  | no | no | `try:` |
| 20114 | if | 4 | try |  | no | no | `if _typed_on():` |
| 20128 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 20141 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 20142 | raise | 4 | if |  | no | no | `raise` |
| 20146 | try | 2 | try |  | YES | YES | `try:` |
| 20160 | except_handler | 2 | try |  | no | no | `except Exception as _unresolved_rca_exc:` |
| 20161 | try | 3 | except_handler |  | no | no | `try:` |
| 20165 | if | 4 | try |  | no | no | `if _typed_on():` |
| 20179 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 20191 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 20192 | raise | 4 | if |  | no | no | `raise` |
| 20196 | try | 2 | try |  | YES | YES | `try:` |
| 20202 | for | 3 | try |  | YES | YES | `for _qid in (_eval_qids_for_entry or []):` |
| 20205 | if | 4 | for |  | YES | YES | `if isinstance(_scores, dict) and _qstr in _scores:` |
| 20211 | if | 4 | for |  | YES | YES | `if isinstance(_arbiter_map, dict) and _qstr in _arbiter_map:` |
| 20233 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20242 | if | 2 | try |  | YES | YES | `if _iter_num == 1:` |
| 20247 | if | 3 | if |  | YES | YES | `if _scaled_max_iterations != max_iterations:` |
| 20270 | try | 2 | try |  | YES | YES | `try:` |
| 20279 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20285 | try | 2 | try |  | YES | YES | `try:` |
| 20296 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20306 | try | 2 | try |  | YES | YES | `try:` |
| 20314 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20321 | try | 2 | try |  | YES | YES | `try:` |
| 20355 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20358 | try | 2 | try |  | YES | YES | `try:` |
| 20360 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20362 | try | 2 | try |  | YES | YES | `try:` |
| 20364 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20366 | try | 2 | try |  | YES | YES | `try:` |
| 20375 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20383 | try | 2 | try |  | YES | YES | `try:` |
| 20394 | if | 3 | try |  | YES | YES | `if _t8_cases:` |
| 20396 | try | 4 | if |  | no | no | `try:` |
| 20408 | for | 5 | try |  | no | no | `for _idx, _c in enumerate(_t8_cases, start=1):` |
| 20434 | if | 5 | try |  | no | no | `if _t8_audit_rows:` |
| 20439 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 20451 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20460 | if | 2 | try |  | YES | YES | `if human_required_signatures:` |
| 20479 | if | 3 | if |  | no | no | `if _dropped_hard or _dropped_soft:` |
| 20487 | if | 2 | try |  | YES | YES | `if not clusters and not soft_signal_clusters:` |
| 20489 | try | 3 | if |  | no | no | `try:` |
| 20511 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 20526 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 20527 | try | 4 | if |  | no | no | `try:` |
| 20539 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 20545 | break | 3 | if |  | no | no | `break` |
| 20596 | if | 2 | try |  | YES | YES | `if metadata_snapshot.get("_regression_mining_hints"):` |
| 20612 | for | 2 | try |  | YES | YES | `for _sc in soft_signal_clusters or []:` |
| 20613 | if | 3 | for |  | no | no | `if isinstance(_sc, dict):` |
| 20647 | try | 2 | try |  | YES | YES | `try:` |
| 20659 | if | 3 | try |  | YES | YES | `if (` |
| 20670 | for | 4 | if |  | no | no | `for _cid, _drifted in (` |
| 20682 | if | 5 | for |  | no | no | `if _t5_key in _iter_emitted_keys:` |
| 20683 | continue | 6 | if |  | no | no | `continue` |
| 20689 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20725 | if | 2 | try |  | YES | YES | `if reflection_buffer:` |
| 20729 | if | 3 | if |  | YES | YES | `if not _rollback_state_trusted_for_quarantine:` |
| 20745 | for | 3 | if |  | YES | YES | `for _pq_id, _pq_info in _persist_data.items():` |
| 20749 | if | 4 | for |  | YES | YES | `if _pq_class == "ADDITIVE_LEVERS_EXHAUSTED" or (` |
| 20753 | if | 5 | if |  | no | no | `elif _pq_conv in ("stuck", "worsening") and _pq_consec >= 2:` |
| 20762 | if | 3 | if |  | YES | YES | `if _soft_skip_qids:` |
| 20804 | if | 3 | if |  | YES | YES | `if _quarantine_qids:` |
| 20806 | if | 4 | if |  | YES | YES | `if _newly_quarantined:` |
| 20812 | try | 5 | if |  | YES | YES | `try:` |
| 20815 | for | 6 | try |  | YES | YES | `for _hq_id in sorted(_newly_quarantined):` |
| 20831 | if | 6 | try |  | YES | YES | `if _flag_items:` |
| 20841 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 20850 | for | 4 | if |  | YES | YES | `for c in list(clusters) + list(soft_signal_clusters or []):` |
| 20865 | try | 4 | if |  | YES | YES | `try:` |
| 20882 | if | 5 | try |  | YES | YES | `if _q_decision["action"] == "stop_for_human_review":` |
| 20893 | break | 6 | if |  | no | no | `break` |
| 20894 | if | 5 | try |  | YES | YES | `if _q_decision["action"] == "diagnostic_lane":` |
| 20899 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 20904 | if | 4 | if |  | YES | YES | `if not clusters and not soft_signal_clusters:` |
| 20906 | break | 5 | if |  | YES | YES | `break` |
| 20936 | try | 2 | try |  | YES | YES | `try:` |
| 20938 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 20943 | try | 2 | try |  | YES | YES | `try:` |
| 20962 | if | 3 | try |  | YES | YES | `if _process_all_ags and pending_action_groups:` |
| 20976 | while | 4 | if |  | no | no | `while pending_action_groups:` |
| 20982 | if | 5 | while |  | no | no | `if not _candidate_sig_set:` |
| 20995 | if | 6 | if |  | no | no | `if not _src_ids or (_src_ids & _live_cluster_ids):` |
| 20997 | break | 7 | if |  | no | no | `break` |
| 20998 | continue | 6 | if |  | no | no | `continue` |
| 20999 | if | 5 | while |  | no | no | `if _candidate_sig_set & _live_cluster_signatures:` |
| 21004 | break | 6 | if |  | no | no | `break` |
| 21007 | if | 4 | if |  | no | no | `if _dropped_for_drift:` |
| 21008 | for | 5 | if |  | no | no | `for _drop in _dropped_for_drift:` |
| 21026 | if | 4 | if |  | no | no | `if ag is not None:` |
| 21040 | if | 5 | if |  | no | no | `if _regression_debt_qids_for_next_iteration:` |
| 21047 | if | 6 | if |  | no | no | `if not (_debt_set & _ag_qids):` |
| 21056 | if | 3 | try |  | YES | YES | `if ag is None:` |
| 21059 | if | 4 | if |  | YES | YES | `if _regression_debt_qids_for_next_iteration:` |
| 21067 | if | 4 | if |  | YES | YES | `if _unresolved_target_debt_qids_for_next_iteration:` |
| 21097 | while | 4 | if |  | YES | YES | `while diagnostic_action_queue and _diag_preempt is None:` |
| 21114 | if | 5 | while |  | no | no | `if _candidate_sig_set:` |
| 21120 | if | 5 | while |  | no | no | `if not _matches_live:` |
| 21133 | continue | 6 | if |  | no | no | `continue` |
| 21161 | if | 4 | if |  | YES | YES | `if _intent_collisions:` |
| 21175 | try | 5 | if |  | no | no | `try:` |
| 21176 | for | 6 | try |  | no | no | `for _coll in _intent_collisions:` |
| 21180 | for | 7 | for |  | no | no | `for _qids_list in _qbycol.values():` |
| 21184 | if | 7 | for |  | no | no | `if _all_qids:` |
| 21190 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 21195 | if | 4 | if |  | YES | YES | `if _diag_preempt is not None:` |
| 21201 | if | 5 | if |  | YES | YES | `elif _memo_key in strategist_memo_cache:` |
| 21209 | if | 6 | if |  | YES | YES | `if _strategist_constraints.to_strategist_context():` |
| 21235 | try | 6 | if |  | YES | YES | `try:` |
| 21239 | if | 7 | try |  | YES | YES | `if _iter_fb_enabled():` |
| 21241 | if | 8 | if |  | YES | YES | `if _prior_iter >= 0:` |
| 21248 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 21304 | if | 6 | if |  | YES | YES | `if _selector_out["source"] == "three_stage_pipeline":` |
| 21334 | if | 4 | if |  | YES | YES | `if _l3_diagnostics:` |
| 21378 | try | 4 | if |  | YES | YES | `try:` |
| 21383 | if | 5 | try |  | YES | YES | `if _nm_enabled() and action_groups:` |
| 21395 | for | 6 | if |  | no | no | `for _c in clusters or []:` |
| 21398 | if | 7 | for |  | no | no | `if _cid and isinstance(_kit, dict):` |
| 21404 | for | 6 | if |  | no | no | `for _i in sorted(_iter_summaries.keys()):` |
| 21405 | if | 7 | for |  | no | no | `if int(_i) >= int(iteration_counter or 0):` |
| 21406 | continue | 8 | if |  | no | no | `continue` |
| 21410 | if | 7 | for |  | no | no | `if _prior_fb is None:` |
| 21411 | continue | 8 | if |  | no | no | `continue` |
| 21412 | for | 7 | for |  | no | no | `for _key, _shapes in (` |
| 21420 | for | 6 | if |  | no | no | `for _ag in action_groups:` |
| 21455 | if | 7 | for |  | no | no | `if _result.differs or not _strict:` |
| 21472 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 21486 | if | 4 | if |  | YES | YES | `if int(iteration_counter or 0) >= 2:` |
| 21487 | try | 5 | if |  | YES | YES | `try:` |
| 21491 | if | 6 | try |  | YES | YES | `if _al_enabled2():` |
| 21506 | for | 7 | if |  | YES | YES | `for _c in _cands:` |
| 21516 | for | 7 | if |  | YES | YES | `for _pa in _new_pas:` |
| 21531 | for | 7 | if |  | YES | YES | `for _c in _cands:` |
| 21532 | if | 8 | for |  | no | no | `if _c.signature_hash in _synthesised_sigs:` |
| 21533 | continue | 9 | if |  | no | no | `continue` |
| 21549 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 21560 | try | 4 | if |  | YES | YES | `try:` |
| 21565 | if | 5 | try |  | YES | YES | `if _repair_planner_enabled():` |
| 21584 | for | 6 | if |  | YES | YES | `for _c in clusters or []:` |
| 21586 | if | 7 | for |  | YES | YES | `if _kit is None:` |
| 21588 | if | 8 | if |  | YES | YES | `if _card is None:` |
| 21589 | continue | 9 | if |  | YES | YES | `continue` |
| 21600 | continue | 8 | if |  | no | no | `continue` |
| 21611 | if | 7 | for |  | no | no | `if _propagation in (` |
| 21646 | try | 6 | if |  | YES | YES | `try:` |
| 21650 | if | 7 | try |  | YES | YES | `if _al_enabled():` |
| 21660 | for | 8 | if |  | YES | YES | `for _r in _tier1_records:` |
| 21670 | if | 8 | if |  | YES | YES | `if _tier1_records:` |
| 21677 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 21682 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 21692 | try | 4 | if |  | YES | YES | `try:` |
| 21702 | if | 5 | try |  | YES | YES | `if _uncovered:` |
| 21707 | try | 6 | if |  | YES | YES | `try:` |
| 21738 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 21748 | try | 6 | if |  | YES | YES | `try:` |
| 21752 | if | 7 | try |  | YES | YES | `if _recall_enabled():` |
| 21775 | if | 8 | if |  | YES | YES | `if _eligible_ids:` |
| 21784 | try | 9 | if |  | no | no | `try:` |
| 21795 | except_handler | 9 | if |  | no | no | `except Exception:` |
| 21809 | if | 9 | if |  | no | no | `if _recall_succeeded:` |
| 21817 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 21822 | if | 6 | if |  | YES | YES | `if _uncovered:` |
| 21833 | for | 6 | if |  | YES | YES | `for _c in _uncovered:` |
| 21853 | try | 7 | for |  | YES | YES | `try:` |
| 21857 | if | 8 | try |  | YES | YES | `if (` |
| 21895 | if | 9 | if |  | YES | YES | `if _t3_trig_key not in _iter_emitted_keys:` |
| 21913 | if | 9 | if |  | YES | YES | `if _t3_exh_key not in _iter_emitted_keys:` |
| 21924 | for | 10 | if |  | YES | YES | `for _q in _t3_target_qids:` |
| 21925 | try | 11 | for |  | YES | YES | `try:` |
| 21931 | except_handler | 11 | for |  | no | no | `except Exception:` |
| 21941 | continue | 9 | if |  | YES | YES | `continue` |
| 21942 | except_handler | 7 | for |  | no | no | `except Exception:` |
| 21953 | try | 7 | for |  | no | no | `try:` |
| 21964 | if | 8 | try |  | no | no | `if _diag_qids:` |
| 21978 | except_handler | 7 | for |  | no | no | `except Exception:` |
| 21983 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 22019 | for | 4 | if |  | YES | YES | `for _ag_in in action_groups:` |
| 22025 | if | 4 | if |  | YES | YES | `if len(_decomposed_action_groups) != len(action_groups):` |
| 22051 | try | 4 | if |  | YES | YES | `try:` |
| 22052 | for | 5 | try |  | YES | YES | `for _ag_w8 in (action_groups or []):` |
| 22062 | if | 6 | for |  | no | no | `if _before_w8 and set(_before_w8) < set(_after_w8):` |
| 22074 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 22080 | if | 4 | if |  | YES | YES | `if _process_all_ags and len(action_groups) > 1:` |
| 22099 | for | 5 | if |  | no | no | `for _buffered_ag in pending_action_groups:` |
| 22122 | if | 3 | try |  | YES | YES | `if isinstance(_global_rewrite, dict):` |
| 22124 | if | 4 | if |  | no | no | `if non_empty and ag is not None:` |
| 22128 | if | 4 | if |  | YES | YES | `elif isinstance(_global_rewrite, str) and _global_rewrite.strip():` |
| 22129 | if | 5 | if |  | no | no | `if ag is not None:` |
| 22134 | if | 3 | try |  | YES | YES | `if ag is None and _iter_num == 1:` |
| 22144 | if | 4 | if |  | YES | YES | `if _fb_ags:` |
| 22148 | finally | 2 | try |  | YES | YES | `_mlflow.end_run()` |
| 22150 | if | 2 | try |  | YES | YES | `if ag is None and clusters:` |
| 22152 | for | 3 | if |  | YES | YES | `for c in clusters:` |
| 22154 | if | 3 | if |  | YES | YES | `if _remaining_qids and _iter_num <= max_iterations - 1:` |
| 22187 | if | 2 | try |  | YES | YES | `if ag is None:` |
| 22193 | try | 3 | if |  | no | no | `try:` |
| 22215 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 22229 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 22230 | try | 4 | if |  | no | no | `try:` |
| 22248 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 22254 | break | 3 | if |  | no | no | `break` |
| 22287 | try | 2 | try |  | YES | YES | `try:` |
| 22295 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 22336 | for | 2 | try |  | YES | YES | `for _rc_idx, _rc in enumerate(ranked):` |
| 22338 | if | 3 | for |  | YES | YES | `if _ag_source_cids and _rc_cid not in set(_ag_source_cids):` |
| 22339 | continue | 4 | if |  | YES | YES | `continue` |
| 22341 | if | 3 | for |  | YES | YES | `if _rc_sig and _rc_sig not in _ag_source_signatures:` |
| 22343 | if | 3 | for |  | YES | YES | `if not _ag_cluster_info:` |
| 22391 | for | 2 | try |  | YES | YES | `for _scid in (ag.get("source_cluster_ids") or []):` |
| 22397 | if | 3 | for |  | YES | YES | `if isinstance(_candidate_cluster, _Mapping):` |
| 22399 | break | 4 | if |  | YES | YES | `break` |
| 22400 | try | 2 | try |  | YES | YES | `try:` |
| 22408 | except_handler | 2 | try |  | no | no | `except _FailureClusterIdentityError as _identity_err:` |
| 22415 | if | 2 | try |  | YES | YES | `if _failure_cluster_for_collision is not None:` |
| 22427 | if | 2 | try |  | YES | YES | `if _collision_pair_matches(_collision_pair, _forbidden_pair):` |
| 22432 | if | 3 | if |  | no | no | `if (` |
| 22439 | if | 4 | if |  | no | no | `elif (` |
| 22504 | try | 3 | if |  | no | no | `try:` |
| 22526 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 22542 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 22543 | try | 4 | if |  | no | no | `try:` |
| 22562 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 22574 | if | 3 | if |  | no | no | `if _should_terminate_on_collision_saturation(` |
| 22599 | break | 4 | if |  | no | no | `break` |
| 22600 | continue | 3 | if |  | no | no | `continue` |
| 22616 | if | 2 | try |  | YES | YES | `if _ag_proposals and isinstance(_ag_proposals, list):` |
| 22617 | for | 3 | if |  | no | no | `for _prop in _ag_proposals:` |
| 22618 | if | 4 | for |  | no | no | `if not isinstance(_prop, dict):` |
| 22619 | continue | 5 | if |  | no | no | `continue` |
| 22620 | try | 4 | for |  | no | no | `try:` |
| 22633 | except_handler | 4 | for |  | no | no | `except Exception:` |
| 22635 | if | 3 | if |  | no | no | `if _ag_proposals:` |
| 22640 | if | 2 | try |  | YES | YES | `if _escalation:` |
| 22673 | if | 3 | if |  | no | no | `if _escalation == "flag_for_review" or (` |
| 22692 | continue | 4 | if |  | no | no | `continue` |
| 22694 | if | 3 | if |  | no | no | `if _escalation == "gt_repair":` |
| 22696 | if | 4 | if |  | no | no | `if _gt_repair_corrections > 0:` |
| 22733 | continue | 4 | if |  | no | no | `continue` |
| 22735 | if | 3 | if |  | no | no | `if _escalation == "remove_tvf" and _esc_tier in ("auto_apply", "apply_and_flag")` |
| 22738 | if | 4 | if |  | no | no | `if _tvf_id:` |
| 22767 | for | 5 | if |  | no | no | `for idx, entry in enumerate(_tvf_apply_log.get("applied", [])):` |
| 22773 | if | 5 | if |  | no | no | `if _tvf_apply_log.get("patch_deployed", False):` |
| 22776 | if | 6 | if |  | no | no | `if _original_instruction_sections:` |
| 22789 | try | 2 | try |  | YES | YES | `try:` |
| 22827 | if | 3 | try |  | YES | YES | `if _all_required_rca_levers:` |
| 22849 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 22852 | if | 2 | try |  | YES | YES | `if "6" in lever_keys:` |
| 22853 | try | 3 | if |  | YES | YES | `try:` |
| 22874 | for | 4 | try |  | YES | YES | `for _row in _structural_rows:` |
| 22875 | for | 5 | for |  | YES | YES | `for _candidate in extract_failed_row_sql_expression_candidates(_row):` |
| 22877 | if | 4 | try |  | YES | YES | `if _structural_candidates:` |
| 22901 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 22924 | try | 2 | try |  | YES | YES | `try:` |
| 22928 | if | 3 | try |  | YES | YES | `if _doc_enabled():` |
| 22944 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 22949 | for | 2 | try |  | YES | YES | `for lever_key in lever_keys:` |
| 22961 | try | 3 | for |  | YES | YES | `try:` |
| 22966 | except_handler | 3 | for |  | no | no | `except Exception:` |
| 22990 | try | 3 | for |  | YES | YES | `try:` |
| 23002 | try | 4 | try |  | YES | YES | `try:` |
| 23004 | if | 5 | try |  | YES | YES | `if _bon_cids:` |
| 23006 | if | 5 | try |  | YES | YES | `if _bon_first_cid:` |
| 23007 | for | 6 | if |  | YES | YES | `for _bon_c in (clusters or []):` |
| 23008 | if | 7 | for |  | YES | YES | `if str(_bon_c.get("cluster_id") or "") == _bon_first_cid:` |
| 23010 | if | 8 | if |  | YES | YES | `if _bon_card is not None:` |
| 23015 | break | 8 | if |  | YES | YES | `break` |
| 23016 | except_handler | 4 | try |  | no | no | `except Exception:` |
| 23021 | try | 4 | try |  | YES | YES | `try:` |
| 23035 | except_handler | 4 | try |  | no | no | `except Exception:` |
| 23044 | except_handler | 3 | for |  | no | no | `except Exception:` |
| 23052 | if | 3 | for |  | YES | YES | `if _use_best_of_n:` |
| 23056 | for | 4 | if |  | no | no | `for _bon_idx in range(3):` |
| 23057 | try | 5 | for |  | no | no | `try:` |
| 23073 | if | 6 | try |  | no | no | `if _sample:` |
| 23078 | except_handler | 5 | for |  | no | no | `except Exception:` |
| 23086 | if | 4 | if |  | no | no | `if _bon_candidates:` |
| 23087 | try | 5 | if |  | no | no | `try:` |
| 23096 | if | 6 | try |  | no | no | `if _bon_top is not None:` |
| 23110 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 23124 | try | 4 | if |  | no | no | `try:` |
| 23129 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23134 | try | 4 | if |  | no | no | `try:` |
| 23159 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23191 | if | 3 | for |  | YES | YES | `if _directive_outcome_ledger is not None and (` |
| 23194 | try | 4 | if |  | YES | YES | `try:` |
| 23204 | try | 5 | try |  | YES | YES | `try:` |
| 23209 | except_handler | 5 | try |  | no | no | `except Exception:` |
| 23225 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23237 | if | 2 | try |  | YES | YES | `if not all_proposals:` |
| 23238 | try | 3 | if |  | YES | YES | `try:` |
| 23250 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 23264 | if | 2 | try |  | YES | YES | `if _directive_outcome_ledger is not None:` |
| 23265 | try | 3 | if |  | YES | YES | `try:` |
| 23272 | for | 4 | try |  | YES | YES | `for _lever_int, _outcome in list(` |
| 23282 | if | 5 | for |  | YES | YES | `if _refined != _outcome:` |
| 23286 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 23296 | if | 2 | try |  | YES | YES | `if _directive_outcome_ledger is not None:` |
| 23297 | try | 3 | if |  | YES | YES | `try:` |
| 23309 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 23323 | try | 2 | try |  | YES | YES | `try:` |
| 23349 | if | 3 | try |  | YES | YES | `if (` |
| 23355 | for | 4 | if |  | YES | YES | `for _cid in (ag.get("source_cluster_ids") or []):` |
| 23360 | if | 5 | for |  | YES | YES | `if isinstance(_src_cluster, dict) and not _l5_ag_root_cause:` |
| 23364 | if | 5 | for |  | YES | YES | `if _l5_ag_rca_id and _l5_ag_root_cause:` |
| 23365 | break | 6 | if |  | YES | no | `break` |
| 23384 | try | 4 | if |  | YES | YES | `try:` |
| 23389 | for | 5 | try |  | YES | YES | `for _md in _l5_ag_drops:` |
| 23390 | for | 6 | for |  | YES | YES | `for _rc in (_md.get("root_causes") or ()):` |
| 23392 | if | 7 | for |  | YES | YES | `if _rc_s and _rc_s not in _l5_marker_root_causes:` |
| 23403 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23415 | try | 4 | if |  | YES | YES | `try:` |
| 23487 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23504 | try | 4 | if |  | YES | YES | `try:` |
| 23536 | for | 5 | try |  | YES | YES | `for _forced_proposal in _dispatch_result.appended_proposals:` |
| 23544 | for | 5 | try |  | YES | YES | `for _nsc_dict in _dispatch_result.emitted_decision_records:` |
| 23550 | try | 6 | for |  | YES | YES | `try:` |
| 23573 | except_handler | 6 | for |  | no | no | `except Exception:` |
| 23579 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 23592 | if | 5 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 23593 | raise | 6 | if |  | no | no | `raise` |
| 23594 | except_handler | 2 | try |  | no | no | `except Exception as _lever5_structural_gate_exc:` |
| 23595 | try | 3 | except_handler |  | no | no | `try:` |
| 23599 | if | 4 | try |  | no | no | `if _typed_on():` |
| 23613 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 23625 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 23626 | raise | 4 | if |  | no | no | `raise` |
| 23636 | try | 2 | try |  | YES | YES | `try:` |
| 23640 | for | 3 | try |  | YES | YES | `for _force_cid in (ag.get("source_cluster_ids") or ()):` |
| 23644 | if | 4 | for |  | YES | YES | `if not isinstance(_force_cluster, dict):` |
| 23645 | continue | 5 | if |  | no | no | `continue` |
| 23660 | if | 4 | for |  | YES | YES | `if (` |
| 23717 | if | 4 | for |  | YES | YES | `if _ag_sigs:` |
| 23720 | try | 4 | for |  | YES | YES | `try:` |
| 23745 | if | 5 | try |  | YES | YES | `if _forced_l6 is None:` |
| 23747 | except_handler | 4 | for |  | no | no | `except Exception as _force_exc:` |
| 23750 | try | 5 | except_handler |  | no | no | `try:` |
| 23754 | if | 6 | try |  | no | no | `if _typed_on():` |
| 23768 | except_handler | 5 | except_handler |  | no | no | `except Exception:` |
| 23782 | if | 5 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 23783 | raise | 6 | if |  | no | no | `raise` |
| 23785 | if | 4 | for |  | YES | YES | `if _forced_l6 is not None:` |
| 23830 | if | 5 | if |  | YES | YES | `if _force_outcome == "raised":` |
| 23858 | except_handler | 2 | try |  | no | no | `except Exception as _forced_lever6_n3_exc:` |
| 23859 | try | 3 | except_handler |  | no | no | `try:` |
| 23863 | if | 4 | try |  | no | no | `if _typed_on():` |
| 23877 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 23891 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 23892 | raise | 4 | if |  | no | no | `raise` |
| 23928 | for | 2 | try |  | YES | YES | `for _rb in reflection_buffer:` |
| 23929 | if | 3 | for |  | no | no | `if _rb.get("accepted"):` |
| 23930 | continue | 4 | if |  | no | no | `continue` |
| 23938 | if | 3 | for |  | no | no | `if _rb.get("rollback_class") != _RC.CONTENT_REGRESSION.value:` |
| 23939 | continue | 4 | if |  | no | no | `continue` |
| 23941 | for | 3 | for |  | no | no | `for _dnr in _rb.get("do_not_retry", []):` |
| 23943 | if | 4 | for |  | no | no | `if " on " not in _s:` |
| 23944 | continue | 5 | if |  | no | no | `continue` |
| 23949 | for | 3 | for |  | no | no | `for _rb_patch in _rb.get("do_not_retry_patches", []) or []:` |
| 23950 | if | 4 | for |  | no | no | `if isinstance(_rb_patch, dict):` |
| 23992 | for | 2 | try |  | YES | YES | `for _rb in reflection_buffer:` |
| 23993 | if | 3 | for |  | no | no | `if _rb.get("accepted"):` |
| 23994 | continue | 4 | if |  | no | no | `continue` |
| 23995 | for | 3 | for |  | no | no | `for _rb_patch in _rb.get("do_not_retry_patches", []) or []:` |
| 23996 | if | 4 | for |  | no | no | `if isinstance(_rb_patch, dict):` |
| 24004 | if | 2 | try |  | YES | YES | `if _content_dedup_dropped:` |
| 24010 | if | 2 | try |  | YES | YES | `if _patch_forbidden:` |
| 24020 | for | 3 | if |  | no | no | `for _rb in reflection_buffer:` |
| 24021 | if | 4 | for |  | no | no | `if _rb.get("accepted"):` |
| 24022 | continue | 5 | if |  | no | no | `continue` |
| 24023 | for | 4 | for |  | no | no | `for _entry in _rb.get("do_not_retry", []) or []:` |
| 24025 | if | 5 | for |  | no | no | `if " on " in _es:` |
| 24031 | for | 3 | if |  | no | no | `for _p in all_proposals:` |
| 24052 | if | 4 | for |  | no | no | `if (` |
| 24061 | if | 5 | if |  | no | no | `if _retry_decision.allowed:` |
| 24068 | continue | 6 | if |  | no | no | `continue` |
| 24069 | if | 4 | for |  | no | no | `if _key in _patch_forbidden:` |
| 24070 | if | 5 | if |  | no | no | `if not _justification:` |
| 24073 | continue | 6 | if |  | no | no | `continue` |
| 24074 | if | 5 | if |  | no | no | `if (` |
| 24083 | continue | 6 | if |  | no | no | `continue` |
| 24109 | if | 3 | if |  | no | no | `if _dropped:` |
| 24116 | for | 4 | if |  | no | no | `for _ptype, _target, _reason in _dropped:` |
| 24125 | for | 4 | if |  | no | no | `for _ptype, _target, _reason in _dropped:` |
| 24133 | if | 3 | if |  | no | no | `if _reflection_rewrites:` |
| 24134 | try | 4 | if |  | no | no | `try:` |
| 24139 | for | 5 | try |  | no | no | `for _idx, _rw in enumerate(_reflection_rewrites, start=1):` |
| 24163 | if | 5 | try |  | no | no | `if _t10_rows:` |
| 24168 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 24187 | try | 3 | if |  | no | no | `try:` |
| 24188 | if | 4 | try |  | no | no | `if _dropped:` |
| 24206 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 24258 | if | 2 | try |  | YES | YES | `if _collateral_details:` |
| 24259 | for | 3 | if |  | no | no | `for _ptype, _target, _deps in _collateral_details:` |
| 24272 | for | 2 | try |  | YES | YES | `for pi, p in enumerate(all_proposals, 1):` |
| 24280 | if | 3 | for |  | no | no | `if status == "FAILED (non-JSON)":` |
| 24282 | if | 4 | if |  | no | no | `elif status == "INVALID_TARGET":` |
| 24290 | if | 3 | for |  | no | no | `if table:` |
| 24292 | if | 3 | for |  | no | no | `if column:` |
| 24297 | if | 3 | for |  | no | no | `if isinstance(_p_col_sect, dict) and _p_col_sect:` |
| 24299 | for | 4 | if |  | no | no | `for _sk, _sv in _p_col_sect.items():` |
| 24302 | if | 4 | if |  | no | no | `elif isinstance(_p_tbl_sect, dict) and _p_tbl_sect:` |
| 24304 | for | 5 | if |  | no | no | `for _sk, _sv in _p_tbl_sect.items():` |
| 24307 | if | 5 | if |  | no | no | `elif proposed_value:` |
| 24315 | if | 2 | try |  | YES | YES | `if _n_failed:` |
| 24323 | for | 2 | try |  | YES | YES | `for pi, p in enumerate(all_proposals, 1):` |
| 24325 | if | 3 | for |  | no | no | `if not prov:` |
| 24326 | continue | 4 | if |  | no | no | `continue` |
| 24340 | try | 2 | try |  | YES | YES | `try:` |
| 24342 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24345 | if | 2 | try |  | YES | YES | `if not all_proposals:` |
| 24377 | try | 3 | if |  | YES | YES | `try:` |
| 24381 | if | 4 | try |  | YES | YES | `if forbidden_ag_admits_no_action_enabled():` |
| 24413 | try | 5 | if |  | YES | YES | `try:` |
| 24482 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 24488 | try | 5 | if |  | YES | YES | `try:` |
| 24489 | for | 6 | try |  | YES | YES | `for _lk in lever_keys:` |
| 24495 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 24501 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 24518 | try | 3 | if |  | YES | YES | `try:` |
| 24540 | except_handler | 3 | if |  | YES | YES | `except Exception:` |
| 24554 | if | 3 | if |  | YES | YES | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 24555 | try | 4 | if |  | YES | YES | `try:` |
| 24574 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 24580 | continue | 3 | if |  | YES | YES | `continue` |
| 24585 | try | 2 | try |  | no | no | `try:` |
| 24592 | for | 3 | try |  | no | no | `for _p in all_proposals:` |
| 24594 | if | 4 | for |  | no | no | `if _decision["compatible"]:` |
| 24603 | if | 3 | try |  | no | no | `if _incompatible_proposals:` |
| 24620 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24627 | try | 2 | try |  | no | no | `try:` |
| 24644 | if | 3 | try |  | no | no | `if _shape_decisions:` |
| 24666 | if | 4 | if |  | no | no | `if _rca_shape_drops:` |
| 24674 | for | 5 | if |  | no | no | `for _drop in _rca_shape_drops:` |
| 24699 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24707 | try | 2 | try |  | no | no | `try:` |
| 24713 | if | 3 | try |  | no | no | `if _shape_dropped_ids:` |
| 24752 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24763 | try | 2 | try |  | no | no | `try:` |
| 24767 | for | 3 | try |  | no | no | `for _snap in reversed(_ag_snapshots):` |
| 24768 | if | 4 | for |  | no | no | `if str(_snap.get("id")) == str(ag_id):` |
| 24783 | break | 5 | if |  | no | no | `break` |
| 24784 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24793 | try | 2 | try |  | no | no | `try:` |
| 24804 | if | 3 | try |  | no | no | `if not _ag_assigned_qids:` |
| 24813 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 24841 | try | 2 | try |  | no | no | `try:` |
| 24884 | if | 3 | try |  | no | no | `if _chunk_b_on():` |
| 24935 | try | 3 | try |  | no | no | `try:` |
| 24936 | if | 4 | try |  | no | no | `if isinstance(metadata_snapshot, dict):` |
| 24940 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 24946 | try | 3 | try |  | no | no | `try:` |
| 24957 | if | 4 | try |  | no | no | `if _pd_records:` |
| 24961 | for | 5 | if |  | no | no | `for _pd_rec in _pd_records:` |
| 24962 | try | 6 | for |  | no | no | `try:` |
| 24964 | if | 7 | try |  | no | no | `if _pd_key in _iter_emitted_keys:` |
| 24965 | continue | 8 | if |  | no | no | `continue` |
| 24967 | except_handler | 6 | for |  | no | no | `except Exception:` |
| 24973 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25000 | try | 3 | try |  | no | no | `try:` |
| 25007 | if | 4 | try |  | no | no | `if (` |
| 25017 | for | 5 | if |  | no | no | `for _sc in soft_signal_clusters or []:` |
| 25019 | if | 6 | for |  | no | no | `if not _sc_cid:` |
| 25020 | continue | 7 | if |  | no | no | `continue` |
| 25031 | for | 5 | if |  | no | no | `for _cand in clusters or []:` |
| 25032 | if | 6 | for |  | no | no | `if not isinstance(_cand, dict):` |
| 25033 | continue | 7 | if |  | no | no | `continue` |
| 25034 | if | 6 | for |  | no | no | `if bool(_cand.get("rca_card")):` |
| 25035 | continue | 7 | if |  | no | no | `continue` |
| 25037 | if | 6 | for |  | no | no | `if not _cand_cid:` |
| 25038 | continue | 7 | if |  | no | no | `continue` |
| 25040 | if | 6 | for |  | no | no | `if not _soft_entry:` |
| 25041 | continue | 7 | if |  | no | no | `continue` |
| 25051 | try | 6 | for |  | no | no | `try:` |
| 25056 | except_handler | 6 | for |  | no | no | `except Exception:` |
| 25064 | if | 6 | for |  | no | no | `if _prov_card is None:` |
| 25065 | continue | 7 | if |  | no | no | `continue` |
| 25071 | try | 6 | for |  | no | no | `try:` |
| 25092 | except_handler | 6 | for |  | no | no | `except Exception:` |
| 25098 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25114 | try | 3 | try |  | no | no | `try:` |
| 25118 | if | 4 | try |  | no | no | `if ag_emit_grounding_gate_enabled():` |
| 25119 | checkpoint_call | 5 | if | collect_blocked_clusters | no | no | `_grounding_result = collect_blocked_clusters(` |
| 25127 | if | 5 | if |  | no | no | `if _grounding_result.records_payload:` |
| 25131 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25161 | try | 3 | try |  | no | no | `try:` |
| 25168 | if | 4 | try |  | no | no | `if strategist_recovery_pivot_enabled() and reflection_buffer:` |
| 25183 | for | 5 | if |  | no | no | `for _c in (clusters or []):` |
| 25184 | if | 6 | for |  | no | no | `if not isinstance(_c, dict):` |
| 25185 | continue | 7 | if |  | no | no | `continue` |
| 25187 | if | 6 | for |  | no | no | `if not _cid:` |
| 25188 | continue | 7 | if |  | no | no | `continue` |
| 25189 | for | 6 | for |  | no | no | `for _q in (_c.get("question_ids") or ()):` |
| 25191 | if | 7 | for |  | no | no | `if _qs:` |
| 25211 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25271 | if | 3 | try |  | no | no | `if _admission_on():` |
| 25305 | except_handler | 2 | try |  | no | no | `except Exception as _strategist_ag_exc:` |
| 25306 | try | 3 | except_handler |  | no | no | `try:` |
| 25310 | if | 4 | try |  | no | no | `if _typed_on():` |
| 25324 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 25337 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 25338 | raise | 4 | if |  | no | no | `raise` |
| 25345 | try | 2 | try |  | no | no | `try:` |
| 25360 | if | 3 | try |  | no | no | `if not _ag_verdict.accepted:` |
| 25363 | for | 4 | if |  | no | no | `for _cid in (ag.get("source_cluster_ids") or []):` |
| 25367 | if | 5 | for |  | no | no | `if _ag_rca_id_c5:` |
| 25368 | break | 6 | if |  | no | no | `break` |
| 25385 | except_handler | 2 | try |  | no | no | `except Exception as _groundedness_ag_exc:` |
| 25386 | try | 3 | except_handler |  | no | no | `try:` |
| 25390 | if | 4 | try |  | no | no | `if _typed_on():` |
| 25404 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 25416 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 25417 | raise | 4 | if |  | no | no | `raise` |
| 25423 | try | 2 | try |  | no | no | `try:` |
| 25424 | for | 3 | try |  | no | no | `for _p in (all_proposals or []):` |
| 25426 | if | 4 | for |  | no | no | `if not _ptids:` |
| 25429 | if | 4 | for |  | no | no | `if not _ptids:` |
| 25430 | continue | 5 | if |  | no | no | `continue` |
| 25454 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 25489 | try | 2 | try |  | no | no | `try:` |
| 25496 | if | 3 | try |  | no | no | `if _chunk_c_on_f5():` |
| 25550 | except_handler | 2 | try |  | no | no | `except Exception as _proposal_generated_exc:` |
| 25551 | try | 3 | except_handler |  | no | no | `try:` |
| 25555 | if | 4 | try |  | no | no | `if _typed_on():` |
| 25569 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 25582 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 25583 | raise | 4 | if |  | no | no | `raise` |
| 25587 | try | 2 | try |  | no | no | `try:` |
| 25600 | for | 3 | try |  | no | no | `for _prop in (all_proposals or []):` |
| 25602 | if | 4 | for |  | no | no | `if not _prop_id:` |
| 25603 | continue | 5 | if |  | no | no | `continue` |
| 25607 | if | 4 | for |  | no | no | `if _verdict_p.accepted:` |
| 25608 | continue | 5 | if |  | no | no | `continue` |
| 25618 | if | 3 | try |  | no | no | `if _proposal_drops_c5:` |
| 25627 | except_handler | 2 | try |  | no | no | `except Exception as _groundedness_proposal_exc:` |
| 25628 | try | 3 | except_handler |  | no | no | `try:` |
| 25632 | if | 4 | try |  | no | no | `if _typed_on():` |
| 25646 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 25658 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 25659 | raise | 4 | if |  | no | no | `raise` |
| 25669 | try | 2 | try |  | no | no | `try:` |
| 25675 | if | 3 | try |  | no | no | `if len(patches) > _pre_split_count:` |
| 25681 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 25694 | try | 2 | try |  | no | no | `try:` |
| 25722 | for | 3 | try |  | no | no | `for _patch in patches:` |
| 25723 | try | 4 | for |  | no | no | `try:` |
| 25725 | if | 5 | try |  | no | no | `if isinstance(_rca_exec, dict):` |
| 25730 | except_handler | 4 | for |  | no | no | `except Exception:` |
| 25742 | if | 4 | for |  | no | no | `if _score >= MIN_PROPOSAL_RELEVANCE:` |
| 25778 | if | 3 | try |  | no | no | `if _dropped:` |
| 25815 | try | 3 | try |  | no | no | `try:` |
| 25833 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25839 | try | 3 | try |  | no | no | `try:` |
| 25851 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25858 | try | 3 | try |  | no | no | `try:` |
| 25863 | for | 4 | try |  | no | no | `for _idx, (_patch, _score, _dec) in enumerate(` |
| 25918 | if | 4 | try |  | no | no | `if _grounding_rows:` |
| 25922 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 25927 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 25939 | if | 2 | try |  | no | no | `if _grounding_skip.skip:` |
| 25992 | try | 3 | if |  | no | no | `try:` |
| 26014 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 26029 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 26030 | try | 4 | if |  | no | no | `try:` |
| 26049 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26055 | continue | 3 | if |  | no | no | `continue` |
| 26062 | try | 2 | try |  | no | no | `try:` |
| 26094 | if | 3 | try |  | no | no | `if _stage6_br_pure_on():` |
| 26111 | for | 4 | if |  | no | no | `for _candidate in patches:` |
| 26118 | if | 5 | for |  | no | no | `if not _decision["safe"]:` |
| 26147 | continue | 6 | if |  | no | no | `continue` |
| 26154 | if | 5 | for |  | no | no | `if not _scope_decision["safe"]:` |
| 26176 | continue | 6 | if |  | no | no | `continue` |
| 26209 | if | 3 | try |  | no | no | `if _narrow_kept:` |
| 26225 | try | 3 | try |  | no | no | `try:` |
| 26229 | if | 4 | try |  | no | no | `if (` |
| 26328 | if | 5 | if |  | no | no | `if _p24_outside_target:` |
| 26330 | if | 5 | if |  | no | no | `if (` |
| 26338 | try | 6 | if |  | no | no | `try:` |
| 26345 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 26347 | if | 6 | if |  | no | no | `if _p24_retest.get("safe") is True:` |
| 26353 | try | 5 | if |  | no | no | `try:` |
| 26369 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 26374 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 26394 | if | 3 | try |  | no | no | `if _stage6_nr_pure_on():` |
| 26395 | try | 4 | if |  | no | no | `try:` |
| 26418 | if | 5 | try |  | no | no | `if _rco4_nr_outcome.halt_no_structural_alternative:` |
| 26473 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26482 | try | 4 | if |  | no | no | `try:` |
| 26492 | if | 5 | try |  | no | no | `if _structural_drops:` |
| 26538 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26545 | if | 3 | try |  | no | no | `if _blast_dropped:` |
| 26571 | try | 4 | if |  | no | no | `try:` |
| 26580 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26589 | try | 4 | if |  | no | no | `try:` |
| 26596 | for | 5 | try |  | no | no | `for _cid in (ag.get("source_cluster_ids") or []):` |
| 26600 | if | 6 | for |  | no | no | `if not _br_root_cause:` |
| 26604 | if | 6 | for |  | no | no | `if not _br_rca_id:` |
| 26608 | if | 6 | for |  | no | no | `if _br_root_cause and _br_rca_id:` |
| 26609 | break | 7 | if |  | no | no | `break` |
| 26627 | except_handler | 4 | if |  | no | no | `except Exception as _blast_radius_exc:` |
| 26628 | try | 5 | except_handler |  | no | no | `try:` |
| 26632 | if | 6 | try |  | no | no | `if _typed_on():` |
| 26646 | except_handler | 5 | except_handler |  | no | no | `except Exception:` |
| 26660 | if | 5 | except_handler |  | no | no | `if is_strict_mode():` |
| 26661 | raise | 6 | if |  | no | no | `raise` |
| 26671 | try | 4 | if |  | no | no | `try:` |
| 26678 | if | 5 | try |  | no | no | `if _t2_target_qids:` |
| 26679 | for | 6 | if |  | no | no | `for _drop in _blast_dropped or ():` |
| 26698 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 26705 | except_handler | 2 | try |  | no | no | `except ImportError:` |
| 26730 | for | 3 | except_handler |  | no | no | `for _candidate in patches:` |
| 26737 | if | 4 | for |  | no | no | `if _decision["safe"]:` |
| 26775 | if | 3 | except_handler |  | no | no | `if _narrow_kept:` |
| 26783 | try | 3 | except_handler |  | no | no | `try:` |
| 26787 | if | 4 | try |  | no | no | `if (` |
| 26864 | if | 5 | if |  | no | no | `if _p24b_outside_target:` |
| 26866 | if | 5 | if |  | no | no | `if (` |
| 26871 | try | 6 | if |  | no | no | `try:` |
| 26878 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 26880 | if | 6 | if |  | no | no | `if _p24b_retest.get("safe") is True:` |
| 26883 | try | 5 | if |  | no | no | `try:` |
| 26900 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 26905 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 26912 | if | 3 | except_handler |  | no | no | `if _blast_dropped:` |
| 26931 | try | 2 | try |  | no | no | `try:` |
| 26935 | if | 3 | try |  | no | no | `if _structural_repair_on():` |
| 26957 | for | 4 | if |  | no | no | `for _sr_cid in (ag.get("source_cluster_ids") or []):` |
| 26963 | if | 5 | for |  | no | no | `if _sr_rca_card is not None:` |
| 26964 | break | 6 | if |  | no | no | `break` |
| 27003 | try | 4 | if |  | no | no | `try:` |
| 27019 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 27025 | try | 4 | if |  | no | no | `try:` |
| 27044 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 27055 | if | 4 | if |  | no | no | `if _sr_verdict.outcome == "rejected":` |
| 27056 | if | 5 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 27057 | try | 6 | if |  | no | no | `try:` |
| 27074 | except_handler | 6 | if |  | no | no | `except Exception:` |
| 27115 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27133 | try | 2 | try |  | no | no | `try:` |
| 27137 | if | 3 | try |  | no | no | `if _chunk_c_on_f6():` |
| 27180 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27199 | try | 2 | try |  | no | no | `try:` |
| 27206 | if | 3 | try |  | no | no | `if _stage6_app_pure_on():` |
| 27240 | if | 3 | try |  | no | no | `if _non_applyable_decisions:` |
| 27261 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27272 | try | 2 | try |  | no | no | `try:` |
| 27274 | if | 3 | try |  | no | no | `if _non_applyable:` |
| 27293 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27320 | for | 2 | try |  | no | no | `for _p in patches:` |
| 27321 | if | 3 | for |  | no | no | `if not l5_l6_patch_requires_asset_alignment(_p):` |
| 27323 | continue | 4 | if |  | no | no | `continue` |
| 27333 | if | 3 | for |  | no | no | `if _decision.get("aligned"):` |
| 27335 | continue | 4 | if |  | no | no | `continue` |
| 27343 | if | 2 | try |  | no | no | `if _alignment_drops:` |
| 27353 | try | 2 | try |  | no | no | `try:` |
| 27354 | if | 3 | try |  | no | no | `if _alignment_drops:` |
| 27402 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 27413 | if | 2 | try |  | no | no | `if len(patches) > MAX_AG_PATCHES:` |
| 27446 | if | 3 | if |  | no | no | `if _no_causal_halt():` |
| 27452 | if | 4 | if |  | no | no | `if (` |
| 27493 | try | 5 | if |  | no | no | `try:` |
| 27558 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 27564 | try | 5 | if |  | no | no | `try:` |
| 27565 | for | 6 | try |  | no | no | `for _lk in lever_keys:` |
| 27571 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 27595 | try | 4 | if |  | no | no | `try:` |
| 27600 | if | 5 | try |  | no | no | `if _hub_scoped_enabled():` |
| 27616 | for | 6 | if |  | no | no | `for _p in _expanded:` |
| 27618 | if | 7 | for |  | no | no | `if not _scoped_from:` |
| 27619 | continue | 8 | if |  | no | no | `continue` |
| 27631 | for | 6 | if |  | no | no | `for _p in _before_cap:` |
| 27632 | if | 7 | for |  | no | no | `if not _is_hub_patch(_p, threshold=_hub_th_val):` |
| 27633 | continue | 8 | if |  | no | no | `continue` |
| 27641 | if | 7 | for |  | no | no | `if _has_sibling:` |
| 27642 | continue | 8 | if |  | no | no | `continue` |
| 27660 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 27675 | if | 4 | if |  | no | no | `if _kit_aware_enabled():` |
| 27700 | try | 5 | if |  | no | no | `try:` |
| 27704 | if | 6 | try |  | no | no | `if _soft_ev_enabled():` |
| 27712 | if | 7 | if |  | no | no | `if _soft_lookup:` |
| 27730 | try | 8 | if |  | no | no | `try:` |
| 27731 | for | 9 | try |  | no | no | `for _qids in _soft_lookup.values():` |
| 27732 | for | 10 | for |  | no | no | `for _q in _qids:` |
| 27734 | except_handler | 8 | if |  | no | no | `except Exception:` |
| 27740 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 27747 | try | 5 | if |  | no | no | `try:` |
| 27760 | for | 6 | try |  | no | no | `for _ko in _kit_outcomes:` |
| 27773 | if | 7 | for |  | no | no | `if _ko.get("accepted"):` |
| 27774 | if | 8 | if |  | no | no | `if _ko.get("risk_downgraded_from_high_to_medium"):` |
| 27787 | continue | 8 | if |  | no | no | `continue` |
| 27789 | if | 7 | for |  | no | no | `if _reason == "kit_atomicity_violation":` |
| 27811 | if | 6 | try |  | no | no | `if not patches:` |
| 27823 | except_handler | 5 | if |  | no | no | `except Exception:` |
| 27856 | for | 3 | if |  | no | no | `for _d in _dropped_decisions:` |
| 27873 | try | 3 | if |  | no | no | `try:` |
| 27875 | for | 4 | try |  | no | no | `for _bp in (_before_cap or []):` |
| 27879 | if | 5 | for |  | no | no | `if _bpid:` |
| 27881 | for | 4 | try |  | no | no | `for _d in _dropped_decisions:` |
| 27887 | if | 5 | for |  | no | no | `if not _dt_qids:` |
| 27895 | if | 5 | for |  | no | no | `if _dt_qids:` |
| 27914 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 27950 | try | 3 | if |  | no | no | `try:` |
| 27987 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28013 | if | 2 | try |  | no | no | `if (` |
| 28040 | try | 3 | if |  | no | no | `try:` |
| 28044 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28058 | if | 3 | if |  | no | no | `if not pending_action_groups:` |
| 28060 | try | 3 | if |  | no | no | `try:` |
| 28082 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28098 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 28099 | try | 4 | if |  | no | no | `try:` |
| 28118 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 28156 | continue | 3 | if |  | no | no | `continue` |
| 28165 | if | 2 | try |  | no | no | `if SHADOW_APPLY:` |
| 28196 | if | 2 | try |  | no | no | `if not _pre_ag_snapshot_capture.get("captured"):` |
| 28216 | try | 3 | if |  | no | no | `try:` |
| 28220 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28235 | if | 3 | if |  | no | no | `if not pending_action_groups:` |
| 28237 | try | 3 | if |  | no | no | `try:` |
| 28259 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28274 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 28275 | try | 4 | if |  | no | no | `try:` |
| 28294 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 28332 | continue | 3 | if |  | no | no | `continue` |
| 28350 | try | 2 | try |  | no | no | `try:` |
| 28352 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 28362 | try | 2 | try |  | no | no | `try:` |
| 28382 | if | 3 | try |  | no | no | `if _survival_table:` |
| 28385 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 28393 | try | 2 | try |  | no | no | `try:` |
| 28434 | if | 3 | try |  | no | no | `if not _recon.in_agreement:` |
| 28441 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 28452 | if | 2 | try |  | no | no | `if _apply_skip.skip:` |
| 28453 | if | 3 | if |  | no | no | `if _apply_skip.reason_code == "no_applied_patches":` |
| 28485 | try | 4 | if |  | no | no | `try:` |
| 28492 | for | 5 | try |  | no | no | `for _cid in (ag.get("source_cluster_ids") or []):` |
| 28496 | if | 6 | for |  | no | no | `if not _doa_root_cause:` |
| 28500 | if | 6 | for |  | no | no | `if not _doa_rca_id:` |
| 28504 | if | 6 | for |  | no | no | `if _doa_root_cause and _doa_rca_id:` |
| 28505 | break | 7 | if |  | no | no | `break` |
| 28524 | except_handler | 4 | if |  | no | no | `except Exception as _dead_on_arrival_exc:` |
| 28525 | try | 5 | except_handler |  | no | no | `try:` |
| 28529 | if | 6 | try |  | no | no | `if _typed_on():` |
| 28543 | except_handler | 5 | except_handler |  | no | no | `except Exception:` |
| 28559 | if | 5 | except_handler |  | no | no | `if is_strict_mode():` |
| 28560 | raise | 6 | if |  | no | no | `raise` |
| 28582 | if | 4 | if |  | no | no | `if not pending_action_groups:` |
| 28586 | try | 4 | if |  | no | no | `try:` |
| 28650 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 28656 | try | 4 | if |  | no | no | `try:` |
| 28657 | for | 5 | try |  | no | no | `for _lk in lever_keys:` |
| 28663 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 28680 | try | 3 | if |  | no | no | `try:` |
| 28687 | if | 4 | try |  | no | no | `if _decision_counts:` |
| 28702 | try | 4 | try |  | no | no | `try:` |
| 28704 | if | 5 | try |  | no | no | `if _decision_counts and _mlflow_apl.active_run() is not None:` |
| 28724 | except_handler | 4 | try |  | no | no | `except Exception:` |
| 28730 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28786 | try | 3 | if |  | no | no | `try:` |
| 28790 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28798 | try | 3 | if |  | no | no | `try:` |
| 28860 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28866 | try | 3 | if |  | no | no | `try:` |
| 28867 | for | 4 | try |  | no | no | `for _lk in lever_keys:` |
| 28873 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28890 | try | 3 | if |  | no | no | `try:` |
| 28912 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 28927 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 28928 | try | 4 | if |  | no | no | `try:` |
| 28947 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 28954 | continue | 3 | if |  | no | no | `continue` |
| 28957 | for | 2 | try |  | no | no | `for idx, entry in enumerate(apply_log.get("applied", [])):` |
| 28971 | try | 3 | for |  | no | no | `try:` |
| 28994 | if | 4 | try |  | no | no | `if not _ap_target_qids:` |
| 29009 | if | 4 | try |  | no | no | `if _ap_target_qid_set:` |
| 29017 | if | 4 | try |  | no | no | `if _ap_broad_qid_set:` |
| 29025 | except_handler | 3 | for |  | no | no | `except Exception:` |
| 29050 | try | 2 | try |  | no | no | `try:` |
| 29057 | if | 3 | try |  | no | no | `if _chunk_c_on_f7():` |
| 29112 | except_handler | 2 | try |  | no | no | `except Exception as _patch_applied_exc:` |
| 29113 | try | 3 | except_handler |  | no | no | `try:` |
| 29117 | if | 4 | try |  | no | no | `if _typed_on():` |
| 29131 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 29144 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 29145 | raise | 4 | if |  | no | no | `raise` |
| 29148 | if | 2 | try |  | no | no | `if _queued:` |
| 29151 | for | 3 | if |  | no | no | `for qentry in _queued:` |
| 29176 | for | 3 | if |  | no | no | `for qi, qe in enumerate(_queued, 1):` |
| 29184 | if | 2 | try |  | no | no | `if not apply_log.get("patch_deployed", False) and apply_log.get("applied"):` |
| 29227 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 29228 | try | 4 | if |  | no | no | `try:` |
| 29247 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 29253 | if | 3 | if |  | no | no | `if _pe_class == RollbackClass.SCHEMA_FAILURE:` |
| 29268 | break | 4 | if |  | no | no | `break` |
| 29276 | if | 3 | if |  | no | no | `if _pe_class == RollbackClass.INFRA_FAILURE:` |
| 29278 | for | 4 | if |  | no | no | `for _rb_entry in reversed(reflection_buffer):` |
| 29279 | if | 5 | for |  | no | no | `if _rb_entry.get("rollback_class") == RollbackClass.INFRA_FAILURE.value:` |
| 29282 | break | 6 | if |  | no | no | `break` |
| 29283 | if | 4 | if |  | no | no | `if _consecutive_infra >= INFRA_RETRY_BUDGET:` |
| 29301 | break | 5 | if |  | no | no | `break` |
| 29303 | try | 3 | if |  | no | no | `try:` |
| 29325 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29335 | continue | 3 | if |  | no | no | `continue` |
| 29339 | if | 2 | try |  | no | no | `if _applied:` |
| 29341 | for | 3 | if |  | no | no | `for ai, aentry in enumerate(_applied, 1):` |
| 29349 | if | 2 | try |  | no | no | `if _dropped:` |
| 29351 | for | 3 | if |  | no | no | `for di, dp in enumerate(_dropped, 1):` |
| 29428 | try | 2 | try |  | no | no | `try:` |
| 29430 | if | 3 | try |  | no | no | `if bool(_gr.get("passed")) or str(` |
| 29438 | if | 4 | if |  | no | no | `if bool(_gr.get("passed")):` |
| 29442 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 29453 | try | 2 | try |  | no | no | `try:` |
| 29455 | if | 3 | try |  | no | no | `if (` |
| 29462 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 29480 | if | 2 | try |  | no | no | `if _gate_eval:` |
| 29490 | try | 3 | if |  | no | no | `try:` |
| 29492 | if | 4 | try |  | no | no | `if _backfill_rows and not _current_iter_inputs.get("eval_rows"):` |
| 29494 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29507 | try | 2 | try |  | no | no | `try:` |
| 29510 | if | 3 | try |  | no | no | `if _t4_verdict_for_persist is not None:` |
| 29515 | for | 4 | if |  | no | no | `for _c in (strategy.get("_source_clusters") or []) if strategy else []:` |
| 29517 | if | 5 | for |  | no | no | `if not _cid:` |
| 29518 | continue | 6 | if |  | no | no | `continue` |
| 29519 | for | 5 | for |  | no | no | `for _q in _c.get("question_ids") or []:` |
| 29522 | for | 4 | if |  | no | no | `for _p in (all_proposals or []):` |
| 29524 | if | 5 | for |  | no | no | `if not _pid:` |
| 29525 | continue | 6 | if |  | no | no | `continue` |
| 29526 | for | 5 | for |  | no | no | `for _q in _p.get("target_qids") or []:` |
| 29530 | for | 4 | if |  | no | no | `for _entry in _applied_patch_entries:` |
| 29538 | if | 5 | for |  | no | no | `if _ap_pid:` |
| 29550 | if | 4 | if |  | no | no | `if _t4_rows:` |
| 29557 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 29600 | try | 2 | try |  | no | no | `try:` |
| 29630 | for | 3 | try |  | no | no | `for _entry in (apply_log.get("applied") or []):` |
| 29633 | if | 4 | for |  | no | no | `if _entry_ag:` |
| 29678 | try | 3 | try |  | no | no | `try:` |
| 29687 | if | 4 | try |  | no | no | `if _ag_id_for_canonical and _typed_canonical is not None:` |
| 29691 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 29698 | try | 3 | try |  | no | no | `try:` |
| 29726 | except_handler | 3 | try |  | no | no | `except Exception as _accept_inp_exc:` |
| 29731 | try | 4 | except_handler |  | no | no | `try:` |
| 29740 | except_handler | 4 | except_handler |  | no | no | `except Exception:` |
| 29746 | raise | 4 | except_handler |  | no | no | `raise` |
| 29769 | try | 3 | try |  | no | no | `try:` |
| 29802 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 29808 | except_handler | 2 | try |  | no | no | `except Exception as _accept_stage_exc:` |
| 29809 | try | 3 | except_handler |  | no | no | `try:` |
| 29813 | if | 4 | try |  | no | no | `if _typed_on():` |
| 29827 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 29842 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 29843 | raise | 4 | if |  | no | no | `raise` |
| 29850 | if | 2 | try |  | no | no | `if not gate_result.get("passed"):` |
| 29854 | try | 3 | if |  | no | no | `try:` |
| 29862 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29873 | try | 3 | if |  | no | no | `try:` |
| 29883 | if | 4 | try |  | no | no | `if not _restore_decision.get("verified", True):` |
| 29912 | raise | 5 | if |  | no | no | `raise FailedRollbackVerification(` |
| 29915 | except_handler | 3 | if |  | no | no | `except FailedRollbackVerification:` |
| 29916 | raise | 4 | except_handler |  | no | no | `raise` |
| 29917 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29933 | try | 3 | if |  | no | no | `try:` |
| 29951 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 29962 | if | 3 | if |  | no | no | `if pending_action_groups:` |
| 29969 | for | 3 | if |  | no | no | `for lk in lever_keys:` |
| 29991 | for | 3 | if |  | no | no | `for qid, tid in _fail_tmap.items():` |
| 29993 | if | 4 | for |  | no | no | `if qid in _fail_qids:` |
| 29995 | if | 5 | if |  | no | no | `elif "regressions" in gate_result:` |
| 29998 | if | 3 | if |  | no | no | `if _fail_run_id:` |
| 30018 | for | 3 | if |  | no | no | `for _r in _regressions:` |
| 30019 | if | 4 | for |  | no | no | `if _r.get("judge") == "control_plane_acceptance":` |
| 30023 | break | 5 | if |  | no | no | `break` |
| 30077 | try | 3 | if |  | no | no | `try:` |
| 30079 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30082 | for | 3 | if |  | no | no | `for _qid, _cand_row in _candidate_by_qid_for_delta.items():` |
| 30084 | if | 4 | for |  | no | no | `if not _gt_sql:` |
| 30085 | continue | 5 | if |  | no | no | `continue` |
| 30087 | try | 4 | for |  | no | no | `try:` |
| 30096 | except_handler | 4 | for |  | no | no | `except Exception:` |
| 30097 | continue | 5 | except_handler |  | no | no | `continue` |
| 30098 | if | 4 | for |  | no | no | `if _delta.get("improved") or _delta.get("remaining"):` |
| 30141 | try | 3 | if |  | no | no | `try:` |
| 30147 | for | 4 | try |  | no | no | `for _r in gate_result.get("regressions") or []:` |
| 30148 | for | 5 | for |  | no | no | `for _q in _r.get("blocking_qids") or []:` |
| 30149 | if | 6 | for |  | no | no | `if _q:` |
| 30154 | if | 4 | try |  | no | no | `if not _regressed_qids and prev_failure_qids is not None:` |
| 30168 | if | 4 | try |  | no | no | `if _mined_insights:` |
| 30179 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30186 | try | 3 | if |  | no | no | `try:` |
| 30191 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30199 | if | 3 | if |  | no | no | `if _mined_insights:` |
| 30200 | try | 4 | if |  | no | no | `try:` |
| 30216 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 30222 | for | 3 | if |  | no | no | `for p in patches:` |
| 30232 | if | 4 | for |  | no | no | `if ft and tgt:` |
| 30250 | for | 3 | if |  | no | no | `for c in clusters:` |
| 30252 | if | 4 | for |  | no | no | `if source_cids and cid not in source_cids:` |
| 30253 | continue | 5 | if |  | no | no | `continue` |
| 30256 | if | 4 | for |  | no | no | `if not rc_ft or not _should_mark_tried_lever_aware:` |
| 30257 | continue | 5 | if |  | no | no | `continue` |
| 30275 | if | 4 | for |  | no | no | `if len(_distinct_lever_sets) >= 2:` |
| 30277 | if | 3 | if |  | no | no | `if not _should_mark_tried_lever_aware:` |
| 30282 | try | 3 | if |  | no | no | `try:` |
| 30304 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 30327 | if | 3 | if |  | no | no | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 30328 | try | 4 | if |  | no | no | `try:` |
| 30347 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 30387 | continue | 3 | if |  | no | no | `continue` |
| 30391 | for | 2 | try |  | no | no | `for lk in lever_keys:` |
| 30397 | try | 2 | try |  | no | no | `try:` |
| 30411 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30416 | try | 2 | try |  | no | no | `try:` |
| 30418 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30435 | try | 2 | try |  | no | no | `try:` |
| 30469 | except_handler | 2 | try |  | no | no | `except Exception as _observed_effect_exc:` |
| 30470 | try | 3 | except_handler |  | no | no | `try:` |
| 30474 | if | 4 | try |  | no | no | `if _typed_on():` |
| 30488 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 30500 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 30501 | raise | 4 | if |  | no | no | `raise` |
| 30518 | for | 2 | try |  | no | no | `for qid, tid in _full_trace_map.items():` |
| 30520 | if | 3 | for |  | no | no | `if qid in _full_failures:` |
| 30523 | if | 2 | try |  | no | no | `if _full_run_id:` |
| 30526 | try | 2 | try |  | no | no | `try:` |
| 30529 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30532 | try | 2 | try |  | no | no | `try:` |
| 30534 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30537 | try | 2 | try |  | no | no | `try:` |
| 30541 | if | 3 | try |  | no | no | `if _persist_data:` |
| 30543 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30573 | try | 2 | try |  | no | no | `try:` |
| 30575 | if | 3 | try |  | no | no | `if (` |
| 30584 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30619 | try | 2 | try |  | no | no | `try:` |
| 30624 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30628 | if | 2 | try |  | no | no | `if _acc_delta >= 1.0:` |
| 30669 | if | 2 | try |  | no | no | `if _regression_debt_qids_for_next_iteration:` |
| 30685 | if | 2 | try |  | no | no | `if new_refs:` |
| 30688 | if | 2 | try |  | no | no | `if new_hashes:` |
| 30701 | if | 2 | try |  | no | no | `if post_instructions:` |
| 30726 | if | 2 | try |  | no | no | `if _original_instruction_sections:` |
| 30734 | try | 2 | try |  | no | no | `try:` |
| 30746 | if | 3 | try |  | no | no | `if not _diag_rows and isinstance(full_result, dict):` |
| 30748 | if | 4 | if |  | no | no | `if isinstance(_rows_json, list):` |
| 30750 | if | 5 | if |  | no | no | `elif isinstance(_rows_json, str):` |
| 30751 | try | 6 | if |  | no | no | `try:` |
| 30754 | except_handler | 6 | if |  | no | no | `except (ValueError, TypeError):` |
| 30756 | for | 3 | try |  | no | no | `for _r in _diag_rows:` |
| 30757 | if | 4 | for |  | no | no | `if not isinstance(_r, dict):` |
| 30758 | continue | 5 | if |  | no | no | `continue` |
| 30759 | for | 4 | for |  | no | no | `for _log in (_r.get("_asi_extraction_log") or []):` |
| 30760 | if | 5 | for |  | no | no | `if not isinstance(_log, dict):` |
| 30761 | continue | 6 | if |  | no | no | `continue` |
| 30765 | if | 3 | try |  | no | no | `if _asi_total:` |
| 30776 | if | 4 | if |  | no | no | `if _none_pct > 50.0:` |
| 30812 | if | 3 | try |  | no | no | `if _pre_arb is not None:` |
| 30816 | if | 3 | try |  | no | no | `if _adj is not None:` |
| 30824 | if | 3 | try |  | no | no | `if isinstance(_bcr, (int, float)) and _bcr is not None:` |
| 30837 | if | 4 | if |  | no | no | `if _rescue > 0.30:` |
| 30846 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30853 | try | 2 | try |  | no | no | `try:` |
| 30855 | if | 3 | try |  | no | no | `if not _eval_rows:` |
| 30857 | if | 4 | if |  | no | no | `if isinstance(_eval_rows_json, str):` |
| 30859 | try | 5 | if |  | no | no | `try:` |
| 30861 | except_handler | 5 | if |  | no | no | `except (ValueError, TypeError):` |
| 30863 | if | 5 | if |  | no | no | `elif isinstance(_eval_rows_json, list):` |
| 30865 | if | 3 | try |  | no | no | `if _eval_rows:` |
| 30870 | if | 4 | if |  | no | no | `if _mine_result.get("total_applied", 0) > 0:` |
| 30874 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30884 | try | 2 | try |  | no | no | `try:` |
| 30908 | try | 3 | try |  | no | no | `try:` |
| 30912 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 30917 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30941 | try | 2 | try |  | no | no | `try:` |
| 30951 | if | 3 | try |  | no | no | `if _journey_report is not None:` |
| 30955 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 30965 | if | 2 | try |  | no | no | `if _journey_report is not None:` |
| 30974 | if | 3 | if |  | no | no | `if not _phase_a_result.success:` |
| 30995 | try | 2 | try |  | no | no | `try:` |
| 31008 | if | 3 | try |  | no | no | `if _decision_records:` |
| 31026 | try | 4 | if |  | no | no | `try:` |
| 31035 | for | 5 | try |  | no | no | `for _r in _decision_records:` |
| 31037 | if | 6 | for |  | no | no | `if not _qid or _qid in _qids_seen:` |
| 31038 | continue | 7 | if |  | no | no | `continue` |
| 31039 | if | 6 | for |  | no | no | `if getattr(_r, "outcome", None) != _DecisionOutcome.UNRESOLVED:` |
| 31040 | continue | 7 | if |  | no | no | `continue` |
| 31045 | if | 6 | for |  | no | no | `if _classification.bucket is not None:` |
| 31048 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31059 | try | 4 | if |  | no | no | `try:` |
| 31063 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31102 | if | 4 | if |  | no | no | `if not _phase_b_result.success:` |
| 31119 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31124 | try | 3 | except_handler |  | no | no | `try:` |
| 31126 | if | 4 | try |  | no | no | `if _mlflow_phase_b_partial.active_run() is not None:` |
| 31130 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 31139 | try | 2 | try |  | no | no | `try:` |
| 31158 | if | 3 | try |  | no | no | `if _iter_record_count == 0:` |
| 31175 | try | 4 | if |  | no | no | `try:` |
| 31177 | if | 5 | try |  | no | no | `if _mlflow_no_rec.active_run() is not None:` |
| 31187 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31198 | try | 4 | if |  | no | no | `try:` |
| 31208 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31210 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31220 | try | 2 | try |  | no | no | `try:` |
| 31243 | except_handler | 2 | try |  | no | no | `except Exception as _orphan_rca_exc:` |
| 31244 | try | 3 | except_handler |  | no | no | `try:` |
| 31248 | if | 4 | try |  | no | no | `if _typed_on():` |
| 31262 | except_handler | 3 | except_handler |  | no | no | `except Exception:` |
| 31274 | if | 3 | except_handler |  | no | no | `if _phase_b_strict_mode():` |
| 31275 | raise | 4 | if |  | no | no | `raise` |
| 31281 | try | 2 | try |  | no | no | `try:` |
| 31303 | if | 3 | try |  | no | no | `if _w1_count:` |
| 31308 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31325 | try | 2 | try |  | no | no | `try:` |
| 31329 | if | 3 | try |  | no | no | `if productive_iteration_budget_enabled():` |
| 31336 | if | 4 | if |  | no | no | `if _iter_applied_count == 0:` |
| 31344 | if | 5 | if |  | no | no | `if _iter_no_op_cause:` |
| 31359 | if | 4 | if |  | no | no | `if _iter_budget_key not in _iter_emitted_keys:` |
| 31378 | if | 5 | if |  | no | no | `if not _iter_consumed:` |
| 31380 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31389 | try | 2 | try |  | no | no | `try:` |
| 31408 | try | 3 | try |  | no | no | `try:` |
| 31427 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 31446 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31454 | try | 2 | try |  | no | no | `try:` |
| 31456 | try | 3 | try |  | no | no | `try:` |
| 31462 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 31487 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31499 | try | 2 | try |  | no | no | `try:` |
| 31505 | except_handler | 2 | try |  | no | no | `except Exception:` |
| 31533 | finally | 1 | for |  | YES | YES | `_f_cur = locals().get("_current_iter_inputs")` |
| 31534 | if | 2 | finally |  | YES | YES | `if isinstance(_f_cur, dict) and not _f_cur.get(` |
| 31537 | try | 3 | if |  | YES | YES | `try:` |
| 31567 | except_handler | 3 | if |  | YES | YES | `except Exception:` |
| 31585 | if | 2 | finally |  | YES | YES | `if _iter_marker_active and not _iter_terminal_emitted:` |
| 31586 | try | 3 | if |  | YES | YES | `try:` |
| 31590 | if | 4 | try |  | YES | YES | `if _exc_val is not None:` |
| 31610 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 31631 | try | 2 | finally |  | YES | YES | `try:` |
| 31648 | if | 3 | try |  | YES | YES | `if iteration_terminal_policy_enabled():` |
| 31649 | try | 4 | if |  | YES | YES | `try:` |
| 31653 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31660 | if | 4 | if |  | YES | YES | `if str(_iter_terminal_reason or "") != "accepted":` |
| 31692 | if | 5 | if |  | YES | YES | `if _router_action.add_to_forbidden_set:` |
| 31721 | if | 5 | if |  | YES | YES | `if _abort_break:` |
| 31735 | except_handler | 2 | finally |  | no | no | `except Exception:` |
| 31752 | try | 2 | finally |  | YES | YES | `try:` |
| 31761 | if | 3 | try |  | YES | YES | `if candidate_ledger_enabled():` |
| 31809 | try | 4 | if |  | YES | YES | `try:` |
| 31813 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 31823 | except_handler | 2 | finally |  | no | no | `except Exception:` |
| 31837 | if | 2 | finally |  | YES | YES | `if _loop_should_abort:` |
| 31850 | break | 3 | if |  | no | no | `break` |
| 31861 | try | 0 | module |  | YES | YES | `try:` |
| 31863 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 31869 | if | 0 | module |  | YES | YES | `if _phase35_drained:` |
| 31871 | for | 1 | if |  | no | no | `for _snap in _replay_fixture_iterations:` |
| 31872 | try | 2 | for |  | no | no | `try:` |
| 31874 | except_handler | 2 | for |  | no | no | `except Exception:` |
| 31875 | continue | 3 | except_handler |  | no | no | `continue` |
| 31876 | for | 1 | if |  | no | no | `for _call in _phase35_drained:` |
| 31877 | try | 2 | for |  | no | no | `try:` |
| 31879 | except_handler | 2 | for |  | no | no | `except Exception:` |
| 31882 | if | 2 | for |  | no | no | `if _snap is not None:` |
| 31884 | try | 0 | module |  | YES | YES | `try:` |
| 31886 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 31944 | for | 0 | module |  | YES | YES | `for _rb_entry in reflection_buffer:` |
| 31945 | if | 1 | for |  | YES | YES | `if _rb_entry.get("accepted"):` |
| 31946 | continue | 2 | if |  | no | no | `continue` |
| 31947 | if | 1 | for |  | YES | YES | `if _rb_entry.get("escalation_handled"):` |
| 31948 | continue | 2 | if |  | no | no | `continue` |
| 31950 | if | 0 | module |  | YES | YES | `if len(ags_rolled_back) and _rb_class_counter:` |
| 31961 | if | 0 | module |  | YES | YES | `if lever_changes:` |
| 31963 | for | 1 | if |  | no | no | `for lc in lever_changes:` |
| 31967 | for | 2 | for |  | no | no | `for p in lc.get("patches", []):` |
| 31971 | if | 1 | if |  | YES | YES | `elif not ags_accepted:` |
| 31976 | for | 0 | module |  | YES | YES | `for sname, sval in sorted(best_scores.items()):` |
| 31986 | try | 0 | module |  | YES | YES | `try:` |
| 32010 | try | 1 | try |  | YES | YES | `try:` |
| 32029 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32041 | try | 1 | try |  | YES | YES | `try:` |
| 32052 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32066 | try | 1 | try |  | YES | YES | `try:` |
| 32073 | if | 2 | try |  | YES | YES | `if _replay_fixture_summary is not None:` |
| 32077 | for | 3 | if |  | YES | YES | `for _per in (` |
| 32080 | if | 4 | for |  | YES | YES | `if int(_per.get("eval_rows") or 0) == 0:` |
| 32091 | if | 2 | try |  | YES | YES | `if _is_empty:` |
| 32098 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32108 | if | 1 | try |  | YES | YES | `if _dual_emit_on():` |
| 32112 | try | 2 | if |  | YES | YES | `try:` |
| 32125 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 32158 | try | 1 | try |  | YES | YES | `try:` |
| 32161 | if | 2 | try |  | YES | YES | `if mlflow.active_run() is not None:` |
| 32170 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32175 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 32187 | try | 0 | module |  | YES | YES | `try:` |
| 32214 | if | 1 | try |  | YES | YES | `if gso_run_manifest_v2_enabled():` |
| 32215 | try | 2 | if |  | YES | YES | `try:` |
| 32225 | except_handler | 2 | if |  | no | no | `except Exception:` |
| 32227 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 32246 | try | 0 | module |  | YES | YES | `try:` |
| 32258 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 32284 | try | 0 | module |  | YES | YES | `try:` |
| 32348 | try | 1 | try |  | YES | YES | `try:` |
| 32350 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32370 | try | 1 | try |  | YES | YES | `try:` |
| 32375 | except_handler | 1 | try |  | YES | YES | `except (NameError, AttributeError):` |
| 32413 | for | 1 | try |  | YES | YES | `for _i in _phase_h_iterations_completed:` |
| 32416 | if | 2 | for |  | YES | YES | `if _trace is None:` |
| 32421 | if | 3 | if |  | YES | YES | `if not _summary:` |
| 32451 | try | 1 | try |  | YES | YES | `try:` |
| 32455 | if | 2 | try |  | YES | YES | `if _trend_enabled():` |
| 32481 | try | 3 | if |  | YES | YES | `try:` |
| 32484 | if | 4 | try |  | YES | YES | `if _by_root:` |
| 32503 | except_handler | 3 | if |  | no | no | `except Exception:` |
| 32508 | except_handler | 1 | try |  | no | no | `except Exception:` |
| 32530 | if | 1 | try |  | YES | YES | `if _phase_h_anchor_run_id:` |
| 32531 | try | 2 | if |  | no | no | `try:` |
| 32638 | try | 3 | try |  | no | no | `try:` |
| 32657 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 32675 | try | 3 | try |  | no | no | `try:` |
| 32679 | if | 4 | try |  | no | no | `if _ledger_enabled_phase_h():` |
| 32687 | if | 5 | if |  | no | no | `if _os_for_ledger_copy.path.exists(_ledger_src):` |
| 32693 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 32712 | try | 3 | try |  | no | no | `try:` |
| 32716 | if | 4 | try |  | no | no | `if _phase_h_totality_enabled():` |
| 32754 | if | 5 | if |  | no | no | `if _totality_violation is not None:` |
| 32767 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 32828 | try | 3 | try |  | no | no | `try:` |
| 32862 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 32886 | for | 3 | try |  | no | no | `for _k, _v in _paths.items():` |
| 32887 | if | 4 | for |  | no | no | `if _k == "iterations":` |
| 32888 | for | 5 | if |  | no | no | `for _iter_paths in (_v or {}).values():` |
| 32889 | for | 6 | for |  | no | no | `for _path in (_iter_paths or {}).values():` |
| 32890 | if | 7 | for |  | no | no | `if isinstance(_path, str):` |
| 32892 | if | 5 | if |  | no | no | `elif isinstance(_v, str):` |
| 32897 | try | 3 | try |  | no | no | `try:` |
| 32907 | except_handler | 3 | try |  | no | no | `except Exception:` |
| 32917 | if | 3 | try |  | no | no | `if not _completeness["complete"]:` |
| 32918 | try | 4 | if |  | no | no | `try:` |
| 32936 | except_handler | 4 | if |  | no | no | `except Exception:` |
| 32943 | except_handler | 2 | if |  | no | no | `except Exception as _phase_h_upload_exc:` |
| 32967 | except_handler | 0 | module |  | no | no | `except Exception as _phase_h_render_exc:` |
| 32985 | if | 1 | except_handler |  | no | no | `if "_bundle_assembly_failed_payloads" not in locals():` |
| 32988 | if | 1 | except_handler |  | no | no | `if "_bundle_assembly_incomplete_payloads" not in locals():` |
| 33014 | try | 0 | module |  | YES | YES | `try:` |
| 33015 | for | 1 | try |  | YES | YES | `for _it_idx, _it_trace in (` |
| 33018 | for | 2 | for |  | YES | YES | `for _rec in getattr(_it_trace, "decision_records", ()) or ():` |
| 33024 | if | 3 | for |  | no | no | `if _rec_dict is None:` |
| 33025 | continue | 4 | if |  | no | no | `continue` |
| 33026 | if | 3 | for |  | no | no | `if str(_rec_dict.get("reason_code") or "") == (` |
| 33030 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 33035 | try | 0 | module |  | YES | YES | `try:` |
| 33053 | except_handler | 0 | module |  | no | no | `except Exception:` |
| 33118 | try | 0 | module |  | YES | YES | `try:` |
| 33121 | if | 1 | try |  | YES | YES | `if any(count > 0 for count in _narrowing_summary["hits"].values()):` |
| 33130 | except_handler | 0 | module |  | no | no | `except Exception as _narrowing_log_exc:` |
| 33138 | try | 0 | module |  | YES | YES | `try:` |
| 33141 | if | 1 | try |  | YES | YES | `if any(c > 0 for c in _l5_summary["hits"].values()) or _l5_summary["shadow_compa` |
| 33151 | except_handler | 0 | module |  | no | no | `except Exception as _l5_log_exc:` |
| 33157 | try | 0 | module |  | YES | YES | `try:` |
| 33160 | if | 1 | try |  | YES | YES | `if (_ts_summary["discovery_calls"] > 0` |
| 33173 | except_handler | 0 | module |  | no | no | `except Exception as _ts_log_exc:` |
| 33179 | try | 0 | module |  | YES | YES | `try:` |
| 33182 | if | 1 | try |  | YES | YES | `if (_re_summary["shadow_comparisons"] > 0` |
| 33193 | except_handler | 0 | module |  | no | no | `except Exception as _re_log_exc:` |
| 33217 | try | 0 | module |  | YES | YES | `try:` |
| 33238 | except_handler | 0 | module |  | no | no | `except Exception as _upload_exc:` |
| 33239 | try | 1 | except_handler |  | no | no | `try:` |
| 33245 | except_handler | 1 | except_handler |  | no | no | `except Exception:` |
| 33252 | return | 0 | module |  | YES | YES | `return _build_loop_out_with_pretty_print(` |
