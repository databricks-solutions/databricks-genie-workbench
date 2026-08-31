"""Ontology refresh: freshness/last-run status + the on-demand trigger (MV-D41).

`get_status()` reads the ``genie_ont_runs`` mirror header (cheap) and reports
freshness + last-run state for the freshness chip and the button label.
`trigger()` launches the materialize job via ``jobs.run_now`` (reuse of the GSO
launcher precedent, keyed on ``GSO_ONT_JOB_ID``); if a run is already in flight it
returns the in-flight status rather than launching a duplicate. The trigger does
NOT itself write UC or the snapshots — the job does.

Never blocks a request on the job (MV-D43): the reader swap in taxonomy/tags uses
:func:`mirror_is_fresh` to prefer a fresh mirror and otherwise degrade to the
Phase-1 live path.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from backend.ontology.models import OntologyRefreshStatus
from backend.ontology.services import mirror, ont_settings

logger = logging.getLogger(__name__)

FRESHNESS_WINDOW_HOURS = 24


def _parse_iso(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _age_hours(as_of, now: datetime) -> float | None:
    dt = _parse_iso(as_of)
    if dt is None:
        return None
    return (now - dt).total_seconds() / 3600.0


def _humanize(hours: float) -> str:
    if hours < 1:
        mins = max(1, int(round(hours * 60)))
        return f"{mins} minute{'s' if mins != 1 else ''} ago"
    if hours < 48:
        h = int(round(hours))
        return f"{h} hour{'s' if h != 1 else ''} ago"
    days = int(round(hours / 24))
    return f"{days} day{'s' if days != 1 else ''} ago"


def _map_last_run_state(state) -> str:
    s = str(state or "").lower()
    if s in {"succeeded", "failed", "running", "skipped"}:
        return s
    return "none"


def compute_status(head: dict | None, succeeded: dict | None, *, now: datetime | None = None) -> OntologyRefreshStatus:
    """Pure freshness/state resolution from the run headers (unit-testable)."""
    now = now or datetime.now(timezone.utc)
    window = FRESHNESS_WINDOW_HOURS
    last_run_state = _map_last_run_state(head.get("state") if head else None)

    succ_age = _age_hours(succeeded.get("as_of"), now) if succeeded else None
    mirror_fresh = succ_age is not None and succ_age <= window
    mirror_as_of = str(succeeded.get("as_of")) if succeeded and succeeded.get("as_of") is not None else None

    if head is None:
        state, message = "cold", "Not updated yet — showing the live view."
    elif last_run_state == "running":
        state = "running"
        message = "Refreshing…"
    elif last_run_state == "skipped":
        # The last refresh ran with no catalog scope, so it scanned nothing and
        # (by the materializer's empty-scope guard) preserved the prior snapshot
        # rather than clearing it. Surface that explicitly — even when a fresh prior
        # mirror still backs the reads — so a user who forgot to set an allowlist
        # gets feedback instead of a silent "Updated recently".
        state = "skipped"
        if mirror_fresh:
            message = (
                "Last refresh scanned nothing — set a catalog allowlist in Settings. "
                "Showing the last saved snapshot."
            )
        else:
            message = (
                "Last refresh scanned nothing — set a catalog allowlist in Settings "
                "to build the ontology."
            )
    elif succeeded is None:
        if last_run_state == "failed":
            state, message = "failed", "Last refresh didn't finish — showing the live view."
        else:
            state, message = "cold", "Not updated yet — showing the live view."
    elif mirror_fresh:
        state = "fresh"
        message = f"Updated {_humanize(succ_age)}"
    else:
        state = "stale"
        message = f"Showing the live view (last update {_humanize(succ_age)})"

    source = "mirror" if mirror_fresh else "live"
    return OntologyRefreshStatus(
        state=state,
        source=source,
        mirror_as_of=mirror_as_of,
        last_run_id=str(head.get("run_id")) if head and head.get("run_id") else None,
        last_run_state=last_run_state,
        freshness_window_hours=window,
        message=message,
    )


async def get_status() -> OntologyRefreshStatus:
    ms = ont_settings._metastore_id()
    head = await mirror.latest_run(ms)
    succeeded = await mirror.latest_succeeded_run(ms)
    return compute_status(head, succeeded)


async def mirror_is_fresh(metastore_id: str) -> bool:
    """Whether a fresh materialized mirror backs this metastore (reader-swap gate)."""
    succeeded = await mirror.latest_succeeded_run(metastore_id)
    if not succeeded:
        return False
    age = _age_hours(succeeded.get("as_of"), datetime.now(timezone.utc))
    return age is not None and age <= FRESHNESS_WINDOW_HOURS


def _launch(job_id: str, *, metastore_id: str, workspace_id: str, allowlist: list[str]) -> str | None:
    """Trigger the materialize job via run_now. Returns the job run id, or None.

    ``metastore_id`` is the run grain (MV-D49) — passed so the on-demand run scopes
    to the same metastore the app reads; ``workspace_id`` rides along as provenance.
    """
    import json

    from backend.services.auth import get_service_principal_client

    ws_client = get_service_principal_client()
    waiter = ws_client.jobs.run_now(
        job_id=int(job_id),
        job_parameters={
            "metastore_id": metastore_id,
            "workspace_id": workspace_id,
            "trigger": "on_demand",
            "catalog": os.environ.get("GSO_CATALOG", ""),
            "schema": os.environ.get("GSO_SCHEMA", "genie_space_optimizer"),
            "catalog_allowlist": json.dumps(allowlist),
        },
    )
    return str(getattr(waiter, "run_id", "")) or None


async def trigger() -> OntologyRefreshStatus:
    """Start an on-demand materialization; idempotent while one is running."""
    ms = ont_settings._metastore_id()
    ws = ont_settings._workspace_id()  # provenance only
    head = await mirror.latest_run(ms)
    if head and _map_last_run_state(head.get("state")) == "running":
        # A run is already in flight — return it, do not launch a duplicate.
        return await get_status()

    job_id = os.environ.get("GSO_ONT_JOB_ID", "").strip()
    if not job_id.isdigit():
        status = await get_status()
        status.message = "Refresh isn't set up yet — the nightly job hasn't been deployed."
        return status

    settings = await ont_settings.get_settings()
    try:
        _launch(job_id, metastore_id=ms, workspace_id=ws, allowlist=settings.catalog_allowlist)
    except Exception as e:  # noqa: BLE001 — surface plainly, never 500 the button
        logger.warning("ontology refresh launch failed: %s", e)
        status = await get_status()
        status.message = "Couldn't start a refresh just now — please try again."
        return status

    status = await get_status()
    status.state = "queued"
    status.last_run_state = "running"
    status.message = "Refresh started — this usually takes a moment."
    return status
