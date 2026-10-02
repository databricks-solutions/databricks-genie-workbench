"""Positive termination only; request-lifetime/recovery policy remains M03's gate."""

from datetime import datetime, timezone

from ..contracts import ExecutionTerminalEvidence, TerminationEvidence


class PlatformTerminationEvidenceProvider:
    def __init__(self, attempt_inventory, job_reader, worker_supervisor_reader):
        self._attempt_inventory = attempt_inventory
        self._job_reader = job_reader
        self._worker_reader = worker_supervisor_reader

    @staticmethod
    def _past(timestamp):
        return (isinstance(timestamp, datetime) and timestamp.tzinfo is not None
                and timestamp <= datetime.now(timezone.utc))

    def for_attempt(self, execution_ref, attempt_id):
        try:
            if execution_ref.startswith("worker:"):
                proof = self._worker_reader(execution_ref, attempt_id)
                if (not proof or proof.get("positive_termination") is not True
                        or proof.get("attempt_id") != attempt_id
                        or proof.get("execution_ref") != execution_ref
                        or proof.get("source") != "platform_worker_supervisor"
                        or not self._past(proof.get("terminated_at"))):
                    return None
                return TerminationEvidence(attempt_id, execution_ref, proof["source"],
                                           proof["terminated_at"], (), proof["evidence_digest"])
            if not execution_ref.startswith("job:"):
                return None
            inventory = self._attempt_inventory(execution_ref, attempt_id)
            if (inventory.get("attempt_id") != attempt_id or inventory.get("complete") is not True
                    or inventory.get("execution_ref") != execution_ref
                    or inventory.get("source") != "databricks_jobs"):
                return None
            references = inventory["executions"]
            if execution_ref not in references or len(references) != len(set(references)):
                return None
            executions = []
            for reference in references:
                state = self._job_reader(reference)
                if (state.get("state") not in {"TERMINATED", "SKIPPED"}
                        or state.get("execution_ref") != reference
                        or state.get("attempt_id") != attempt_id
                        or not self._past(state.get("terminal_at"))):
                    return None
                executions.append(ExecutionTerminalEvidence(reference, state["terminal_at"], state["evidence_digest"]))
            return TerminationEvidence(attempt_id, execution_ref, "databricks_jobs",
                                       max(item.terminal_at for item in executions), tuple(executions), None)
        except Exception:
            return None
