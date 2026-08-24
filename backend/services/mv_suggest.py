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
) -> tuple[Any, str]:
    """Run one advice request. Returns ``(AdvisorOutcome, run_id)``.

    Blocking by design (seconds, not minutes): the caller offloads it to a
    worker thread so the event loop stays free. ``sp_ws`` is the service
    principal client; ``applied_config`` is the space's parsed serialized_space
    the route already fetched under OBO.
    """
    from genie_space_optimizer.common.warehouse import (
        wh_create_advice_run,
        wh_ensure_optimization_tables,
        wh_upsert_mv_candidate,
    )
    from genie_space_optimizer.optimization.mv_advisor import (
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
        return True

    def _no_artifact(_proposal: Any, _rendered: Any) -> bool:
        # MV-D23: genie_opt_artifacts is run-partitioned; the standalone path
        # persists the replay body on the candidate row (yaml_text) instead, so
        # there is no run-keyed artifact to write here.
        return False

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
    )
    return outcome, run_id
