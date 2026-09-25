"""Parameter-bound SQL storage; M08 supplies an explicitly target-authenticated runner.

The entire operations table and private Volume are target-owned securables.
JSON kinds, workspace predicates, and Volume prefixes are not ACL boundaries.
This adapter neither provisions grants nor treats a successful INSERT response
as sufficient durability: DurableOperationFacts performs committed read-back.
"""

import json
import re
from hashlib import sha256

from backend.services.version_control.contracts import OperationFact, from_wire, to_wire
from .facts import validate_evidence


class DeltaFactStore:
    def __init__(self, sql, catalog, control_schema, target_workspace_id, *, writes_enabled=False, read_evidence=None):
        if any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]*', name) for name in (catalog, control_schema)):
            raise ValueError('Invalid control catalog or schema identifier')
        self.sql = sql
        self.table = f'`{catalog}`.`{control_schema}`.`genie_space_operations`'
        self.target_workspace_id = target_workspace_id
        self.writes_enabled = writes_enabled
        self.read_evidence = read_evidence
        self.evidence_prefix = f'/Volumes/{catalog}/{control_schema}/vc_approval_evidence/'

    def _validate(self, payload):
        fact = from_wire(OperationFact, {key: value for key, value in payload.items()
                                        if key not in ('operation_request', 'admission_claim')})
        if fact.binding.workspace_id != self.target_workspace_id or fact.actor.workspace_id != self.target_workspace_id:
            raise PermissionError('Facts must belong to the trusted target workspace')
        validate_evidence(fact)
        if fact.evidence_uri is not None:
            if (not fact.evidence_uri.startswith(self.evidence_prefix)
                    or any(part in ('.', '..', '') for part in fact.evidence_uri[len(self.evidence_prefix):].split('/'))
                    or '%' in fact.evidence_uri or '\\' in fact.evidence_uri):
                raise PermissionError('Private evidence must remain in the target-owned Volume')
            if self.read_evidence is None:
                raise PermissionError('Private evidence reader unavailable')
            content = self.read_evidence(fact.evidence_uri)
            if not isinstance(content, bytes) or sha256(content).hexdigest() != fact.evidence_digest:
                raise ValueError('Private evidence integrity failure')
        return fact

    def read(self):
        rows = self.sql(f'SELECT evidence_json FROM {self.table} WHERE target_workspace = :target_workspace',
                        {'target_workspace': self.target_workspace_id})
        payloads = []
        for row in rows:
            envelope = json.loads(row['evidence_json'])
            if envelope.get('schema_version') != 'VC/1.0' or set(envelope) != {'schema_version', 'fact'}:
                raise ValueError('Unknown durable fact schema')
            self._validate(envelope['fact'])
            payloads.append(envelope['fact'])
        return tuple(payloads)

    def append(self, event_key, payload):
        if not self.writes_enabled:
            raise PermissionError('Delta fact writes disabled')
        fact = self._validate(payload)
        if fact.event_key != event_key:
            raise ValueError('Fact event key mismatch')
        wire = to_wire(fact)
        parameters = {key: value for key, value in wire.items()
                      if key not in ('binding', 'request', 'actor', 'evidence', 'expected_base_fingerprint')}
        parameters.update({
            'space_key': fact.binding.space_key, 'binding_id': fact.binding.binding_id,
            'binding_revision': fact.binding.binding_revision, 'target_workspace': self.target_workspace_id,
            'idempotency_key': fact.request.idempotency_key, 'request_digest': fact.request.request_digest,
            'actor_id': fact.actor.subject_id, 'actor_kind': fact.actor.actor_kind,
            'requested_base_fingerprint': fact.expected_base_fingerprint,
            'evidence_json': json.dumps({'schema_version': 'VC/1.0', 'fact': to_wire(payload)},
                                        sort_keys=True, separators=(',', ':'), allow_nan=False),
        })
        columns = ', '.join(parameters)
        values = ', '.join(f':{column}' for column in parameters)
        self.sql(f'INSERT INTO {self.table} ({columns}) VALUES ({values})', parameters)
