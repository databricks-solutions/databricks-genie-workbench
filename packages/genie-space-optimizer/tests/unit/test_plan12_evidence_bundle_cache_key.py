"""Plan 12 — evidence_bundle cache must key off latest_task_run_id, not
parent run id. Otherwise rerun attempts of the same parent return
stale evidence (the bug both 2026-05-20 postmortems flagged)."""


def test_cache_keyed_by_task_run_id(monkeypatch):
    from genie_space_optimizer.optimization.harness import (
        clear_evidence_bundle_cache,
        get_evidence_bundle_for_task_run,
    )

    clear_evidence_bundle_cache()
    fetch_calls: list[str] = []

    def _fake_fetch(task_run_id):
        fetch_calls.append(str(task_run_id))
        return {"task_run_id": task_run_id, "rows": [1, 2, 3]}

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness._fetch_evidence_bundle",
        _fake_fetch,
    )

    # First fetch for run A, task X — should call _fetch_evidence_bundle.
    b1 = get_evidence_bundle_for_task_run(parent_run_id="A", task_run_id="X")
    # Second fetch with same task_run_id — must hit cache.
    b2 = get_evidence_bundle_for_task_run(parent_run_id="A", task_run_id="X")
    assert b1 == b2
    assert fetch_calls == ["X"]

    # Same parent run, NEW task_run_id (rerun) — must re-fetch.
    b3 = get_evidence_bundle_for_task_run(parent_run_id="A", task_run_id="Y")
    assert b3 != b1
    assert fetch_calls == ["X", "Y"], (
        "cache must NOT return stale row when task_run_id changes"
    )


def test_clear_cache_forces_refetch(monkeypatch):
    from genie_space_optimizer.optimization.harness import (
        clear_evidence_bundle_cache,
        get_evidence_bundle_for_task_run,
    )

    clear_evidence_bundle_cache()
    fetch_calls: list[str] = []

    def _fake_fetch(task_run_id):
        fetch_calls.append(str(task_run_id))
        return {"task_run_id": task_run_id}

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.harness._fetch_evidence_bundle",
        _fake_fetch,
    )

    get_evidence_bundle_for_task_run(parent_run_id="A", task_run_id="X")
    clear_evidence_bundle_cache()
    get_evidence_bundle_for_task_run(parent_run_id="A", task_run_id="X")
    assert fetch_calls == ["X", "X"]
