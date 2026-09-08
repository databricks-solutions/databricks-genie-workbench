"""On-demand metric-view advice for the IQ Scan surface (MV-D23).

``POST /spaces/{space_id}/mv/suggest`` runs the metric-view advisor *now*, with
no optimization run behind it. The advisor's orchestration is
SparkSession-free (``mv_advisor.advise_from_corpus`` — the pyspark-free
optimization-leaf seam, the ``mv_create``/``mv_yaml`` precedent), so the backend
drives it directly over a SQL warehouse rather than launching a job.

Identity (MV-D1): the space config is fetched under the caller's OBO client in
the route and handed in here; everything on the GSO side — table bootstrap, the
sentinel advice run, the embedding and signal reads, and the candidate
persistence — runs as the **service principal**, exactly as the in-job path
does. Nothing here creates or drops a UC object, so no OBO write is involved.

Two invariants this module keeps:

* **One migration applicator.** ``wh_ensure_optimization_tables`` (the warehouse
  bootstrapper) applies ``_ALL_DDL`` + ``ADDITIVE_COLUMN_MIGRATIONS`` and is
  called before the first advice INSERT, so ``run_kind`` / ``yaml_text`` /
  ``provenance`` exist before any write depends on them. This module does not
  apply a second migration.
* **One run-insert writer.** The sentinel advice run is written through
  ``wh_create_advice_run`` → ``wh_create_run`` (born terminal, MV-D23 guardrail
  i), never a parallel INSERT.
"""

from __future__ import annotations

import concurrent.futures
import logging
import uuid
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

# The suggest route is interactive, so a slow embedding endpoint must degrade,
# not stall (MV-D15). Past this wall-clock budget the embedding call is
# abandoned and S reports no vector — the same degraded signal an endpoint
# failure already produces — rather than holding the request open.
DEFAULT_EMBEDDING_TIMEOUT_S = 20.0


class _TimeoutEmbeddingClient:
    """Wrap an ``EmbeddingClient`` with a hard wall-clock timeout (MV-D15).

    On timeout the inner call is abandoned and one empty vector per input is
    returned — byte-identical to the endpoint-unreachable path the advisor
    already models, which degrades **S** to its unavailable status. The route
    proceeds; it never blocks on a slow endpoint. The abandoned inner thread is
    left to finish or die with the short-lived executor; it writes nothing.
    """

    def __init__(self, inner: Any, timeout_s: float) -> None:
        self._inner = inner
        self._timeout_s = timeout_s

    def embed(self, texts: Any) -> list[list[float]]:
        texts = list(texts)
        # A fresh single-worker executor, shut down WITHOUT waiting: on timeout
        # the abandoned inner thread must not join here, or the "hard timeout"
        # would still block the request on the slow endpoint via the pool's
        # shutdown. wait=False returns immediately; the orphan thread finishes on
        # its own and writes nothing.
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = pool.submit(self._inner.embed, texts)
        try:
            result = future.result(timeout=self._timeout_s)
            pool.shutdown(wait=False)
            return result
        except concurrent.futures.TimeoutError:
            logger.warning(
                "mv_suggest: embedding endpoint exceeded %ss; S degrades to "
                "unavailable for this request (MV-D15)",
                self._timeout_s,
            )
            pool.shutdown(wait=False, cancel_futures=True)
            return [[] for _ in texts]
        except Exception:
            logger.warning(
                "mv_suggest: embedding endpoint failed; S degrades to "
                "unavailable for this request", exc_info=True,
            )
            pool.shutdown(wait=False, cancel_futures=True)
            return [[] for _ in texts]


def suggest_for_space(
    *,
    sp_ws: Any,
    catalog: str,
    schema: str,
    warehouse_id: str,
    llm_model: str,
    space_id: str,
    applied_config: dict | None,
    triggered_by: str | None,
    embedding_timeout_s: float = DEFAULT_EMBEDDING_TIMEOUT_S,
    on_stage: Callable[[str], None] | None = None,
) -> tuple[Any, str]:
    """Run one advice request. Returns ``(AdvisorOutcome, run_id)``.

    Blocking by design (multiple minutes on a real space — the SCORING stage's
    per-candidate embedding + warehouse signal reads dominate): the caller
    offloads it to a worker thread so the event loop stays free. ``sp_ws`` is the
    service principal client; ``applied_config`` is the space's parsed
    serialized_space the route already fetched under OBO.

    ``on_stage`` (MV-D31) is an optional progress callback the streaming route
    binds; it is called with a :data:`MV_ADVISOR_STAGES` label ON ENTRY to each
    phase (``STAGE_READING`` here, the rest inside ``advise_from_corpus``). It
    defaults to ``None`` (no-op), so the blocking route and the in-job path are
    unchanged. Independent of ``on_stage``, this always persists ONE
    ``genie_opt_stages`` row per advice run — a ``STARTED`` then a terminal row
    carrying ``AdvisorOutcome.detail()`` and the real ``duration_seconds`` — so a
    later mount can hydrate "last scanned + N proposals" without re-running.
    """
    from genie_space_optimizer.common.warehouse import (
        wh_create_advice_run,
        wh_ensure_optimization_tables,
        wh_load_mv_suppressed_fingerprints,
        wh_supersede_legacy_mv_candidates,
        wh_upsert_mv_candidate,
        wh_write_stage,
    )
    from genie_space_optimizer.optimization.mv_advisor import (
        MV_ADVISOR_PHASE_NAME,
        STAGE_READING,
        advise_from_corpus,
        estate_metric_view_yamls,
        space_corpus_entries,
    )
    from genie_space_optimizer.optimization.mv_scoring import (
        FoundationModelEmbeddingClient,
        metric_view_fields,
    )
    from genie_space_optimizer.optimization.mv_signals import warehouse_reader

    # Invariant (one migration applicator): the shared bootstrapper ensures the
    # additive columns exist BEFORE the first advice INSERT / candidate upsert.
    wh_ensure_optimization_tables(sp_ws, warehouse_id, catalog, schema)

    run_id = str(uuid.uuid4())
    domain = ""

    # Guardrail (i): born-terminal sentinel run, written before any candidate so
    # the candidates' run_id resolves to a real row (MV-D23 severs the orphan-id
    # failure mode). config_snapshot is the audit home for who asked and against
    # what space config.
    wh_create_advice_run(
        sp_ws,
        warehouse_id,
        run_id=run_id,
        space_id=space_id,
        domain=domain,
        catalog=catalog,
        schema=schema,
        triggered_by=triggered_by,
        config_snapshot=applied_config,
        llm_model=llm_model,
    )

    # One persisted stage row per advice run (MV-D31 hydration source): STARTED
    # now, terminal below. The four interactive sub-stages ride ``on_stage`` and
    # are transient — they are never persisted here.
    wh_write_stage(
        sp_ws, warehouse_id, run_id=run_id, stage=MV_ADVISOR_PHASE_NAME,
        status="STARTED", catalog=catalog, schema=schema,
    )

    emit_stage = on_stage or (lambda _stage: None)

    # STAGE_READING is the caller's to emit — it owns the config read that the
    # corpus is assembled from; the remaining three fire inside advise_from_corpus.
    emit_stage(STAGE_READING)
    corpus = space_corpus_entries(applied_config)
    embedding_client = _TimeoutEmbeddingClient(
        FoundationModelEmbeddingClient(sp_ws), embedding_timeout_s
    )
    signal_reader = warehouse_reader(sp_ws, warehouse_id)

    def _reader(tables: set[str]) -> Any:
        # Warehouse-only estate read (spark=None; w + warehouse_id): the same
        # DESCRIBE ... AS JSON the job uses, driven through the SQL warehouse.
        yamls = estate_metric_view_yamls(
            None, tables, w=sp_ws, warehouse_id=warehouse_id
        )
        return metric_view_fields(yamls)

    def _persist(proposal: Any, rendered: Any) -> bool:
        # Mirrors mv_scoring.persist_proposal's ScoredProposal→row mapping (the
        # loop already dropped non-persistable proposals), plus the MV-D23
        # yaml_text: the rendered MV-D22 body rides on the candidate row so a
        # standalone candidate is replayable without a run-partitioned artifact.
        yaml_text = rendered.yaml_text if getattr(rendered, "ok", False) else None
        wh_upsert_mv_candidate(
            sp_ws,
            warehouse_id,
            catalog=catalog,
            schema=schema,
            run_id=run_id,
            target_space_id=proposal.target_space_id,
            suggestion_id=proposal.suggestion_id,
            dedup_fingerprint=proposal.dedup_fingerprint,
            candidate_type=proposal.candidate_type,
            confidence_score=proposal.confidence_score,
            tier=proposal.tier,
            # MV-D32 as-implemented (Prompt 15.7b): carry the score-only tier and
            # coverage-cap flag so the interactive-suggest surface persists the
            # same split inputs the in-job path does (both already on ScoredProposal).
            uncapped_tier=getattr(proposal, "uncapped_tier", None),
            tier_capped_by_coverage=getattr(proposal, "tier_capped_by_coverage", None),
            proposed_object=proposal.proposed_object,
            score_components=proposal.components.to_dict(),
            evidence=dict(proposal.evidence),
            provenance=dict(proposal.provenance),
            alternatives=[dict(entry) for entry in proposal.alternatives],
            conflicts=[dict(entry) for entry in proposal.conflicts],
            requested_mode="suggest_only",
            effective_mode="suggest_only",
            yaml_text=yaml_text,
        )
        # MV-D30 as-implemented (Prompt 15.6): when a view-grained bundle lands,
        # retire any legacy per-measure candidate it now covers so hydration and
        # a re-scan surface the bundle alone, never both grains mixed. The member
        # fingerprints ride in evidence["measures"][].dedup_fingerprint.
        evidence = getattr(proposal, "evidence", None) or {}
        if evidence.get("bundle"):
            member_fps = [
                str(m.get("dedup_fingerprint"))
                for m in evidence.get("measures", [])
                if isinstance(m, dict) and m.get("dedup_fingerprint")
            ]
            if member_fps:
                wh_supersede_legacy_mv_candidates(
                    sp_ws,
                    warehouse_id,
                    catalog=catalog,
                    schema=schema,
                    target_space_id=proposal.target_space_id,
                    member_fingerprints=member_fps,
                    superseded_by=proposal.dedup_fingerprint,
                )
        return True

    def _no_artifact(_proposal: Any, _rendered: Any) -> bool:
        # MV-D23: genie_opt_artifacts is run-partitioned; the standalone path
        # persists the replay body on the candidate row (yaml_text) instead, so
        # there is no run-keyed artifact to write here.
        return False

    try:
        outcome = advise_from_corpus(
            space_id=space_id,
            run_id=run_id,
            corpus_entries=corpus,
            applied_config=applied_config,
            benchmarks=(),
            wide_schema_inventory=None,
            metric_view_reader=_reader,
            embedding_client=embedding_client,
            signal_reader=signal_reader,
            intent_texts=(),
            domain=domain,
            max_candidates=None,
            persist_proposal=_persist,
            write_ddl_artifact=_no_artifact,
            # MV-D30: the interactive suggest path reads the SAME per-measure
            # suppression ledger the reject route writes (warehouse twin), so a
            # re-scan never resurfaces a measure the user rejected — and the in-job
            # advisor, which injects the Spark-side reader, agrees with it.
            read_suppressed_fingerprints=lambda: wh_load_mv_suppressed_fingerprints(
                sp_ws, warehouse_id, catalog=catalog, schema=schema,
                target_space_id=space_id,
            ),
            on_stage=emit_stage,
        )
    except Exception as exc:
        # The phase is isolated, but a swallowed exception must still leave a
        # terminal stage row, or hydration would read a stuck STARTED forever.
        wh_write_stage(
            sp_ws, warehouse_id, run_id=run_id, stage=MV_ADVISOR_PHASE_NAME,
            status="FAILED", catalog=catalog, schema=schema,
            error_message=type(exc).__name__,
        )
        raise

    # Terminal row: COMPLETE unless the outcome is a clean SKIP. detail() carries
    # skip_reason + measures_found (the note-2 hydration source); duration_seconds
    # is computed in-engine against the STARTED row above.
    terminal_status = "SKIPPED" if outcome.status == "SKIPPED" else outcome.status
    wh_write_stage(
        sp_ws, warehouse_id, run_id=run_id, stage=MV_ADVISOR_PHASE_NAME,
        status=terminal_status, catalog=catalog, schema=schema,
        detail=outcome.detail(),
    )
    return outcome, run_id
