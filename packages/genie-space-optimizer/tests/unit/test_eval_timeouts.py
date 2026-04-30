from __future__ import annotations

from types import SimpleNamespace
import pytest

from genie_space_optimizer.common import genie_client
from genie_space_optimizer.optimization import evaluation


class _FakeGenie:
    def start_conversation(self, space_id: str, content: str):
        return SimpleNamespace(conversation_id="c1", message_id="m1")

    def get_message(self, space_id: str, conversation_id: str, message_id: str):
        return SimpleNamespace(status="RUNNING", attachments=[])


def test_run_genie_query_returns_timeout_status(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    now_values = [0.0, 0.1, 2.1, 2.1]

    def fake_time() -> float:
        return now_values.pop(0) if now_values else 2.1

    monkeypatch.setattr(genie_client.time, "sleep", sleeps.append)
    monkeypatch.setattr(genie_client.time, "time", fake_time)

    result = genie_client.run_genie_query(
        SimpleNamespace(genie=_FakeGenie()),
        "space-1",
        "Question?",
        max_wait=1,
    )

    assert result["status"] == "TIMEOUT"
    assert result["conversation_id"] == "c1"
    assert result["message_id"] == "m1"
    assert "timed out" in result["error"]


def test_fetch_genie_result_df_returns_none_after_running_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(genie_client.time, "sleep", sleeps.append)

    class _Statements:
        def get_statement(self, statement_id: str):
            return SimpleNamespace(
                status=SimpleNamespace(state="RUNNING"),
                result=None,
                manifest=None,
            )

    result = genie_client.fetch_genie_result_df(
        SimpleNamespace(statement_execution=_Statements()),
        "stmt-1",
        max_retries=2,
        initial_delay=0.5,
    )

    assert result is None
    assert sleeps == [0.5, 1.0]


def test_execute_sql_via_warehouse_raises_running_timeout() -> None:
    class _StatementExecution:
        def execute_statement(self, **kwargs):
            return SimpleNamespace(
                status=SimpleNamespace(state="RUNNING", error=None),
                statement_id="stmt-warehouse-1",
                manifest=None,
                result=None,
            )

    with pytest.raises(RuntimeError, match="SQL warehouse query did not finish"):
        evaluation._execute_sql_via_warehouse(
            SimpleNamespace(statement_execution=_StatementExecution()),
            "warehouse-1",
            "SELECT 1",
            wait_timeout="1s",
        )
