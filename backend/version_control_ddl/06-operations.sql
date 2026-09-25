CREATE TABLE IF NOT EXISTS ${catalog}.${control_schema}.genie_space_operations (
  event_id STRING NOT NULL,
  fact_kind STRING NOT NULL,
  operation_id STRING NOT NULL,
  transition_sequence BIGINT NOT NULL,
  event_key STRING NOT NULL,
  space_key STRING NOT NULL,
  binding_id STRING NOT NULL,
  binding_revision BIGINT NOT NULL,
  target_workspace STRING NOT NULL,
  idempotency_key STRING NOT NULL,
  request_digest STRING NOT NULL,
  operation_type STRING NOT NULL,
  attempt_id STRING,
  generation BIGINT,
  coordination_backend STRING NOT NULL,
  requested_base_fingerprint STRING,
  desired_artifact_fingerprint STRING,
  mapping_fingerprint STRING,
  transformer_version STRING,
  test_policy_fingerprint STRING,
  pre_version_id STRING,
  post_version_id STRING,
  approval_id STRING,
  approval_digest STRING,
  approval_reference STRING,
  release_id STRING,
  job_run_id STRING,
  requester_id STRING NOT NULL,
  actor_id STRING NOT NULL,
  actor_kind STRING NOT NULL,
  status STRING NOT NULL,
  drift_override BOOLEAN NOT NULL,
  evidence_json STRING NOT NULL,
  evidence_uri STRING,
  evidence_digest STRING,
  recorded_at TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES ('delta.appendOnly'='true');
-- CHECK constraints are attached via ALTER; inline table DDL supports only
-- PRIMARY KEY / FOREIGN KEY. DROP IF EXISTS + ADD keeps re-runs idempotent.
ALTER TABLE ${catalog}.${control_schema}.genie_space_operations DROP CONSTRAINT IF EXISTS operations_backend;
ALTER TABLE ${catalog}.${control_schema}.genie_space_operations ADD CONSTRAINT operations_backend CHECK (coordination_backend = 'delta');
ALTER TABLE ${catalog}.${control_schema}.genie_space_operations DROP CONSTRAINT IF EXISTS operation_sequence;
ALTER TABLE ${catalog}.${control_schema}.genie_space_operations ADD CONSTRAINT operation_sequence CHECK (transition_sequence >= 0);
ALTER TABLE ${catalog}.${control_schema}.genie_space_operations DROP CONSTRAINT IF EXISTS operation_binding_revision;
ALTER TABLE ${catalog}.${control_schema}.genie_space_operations ADD CONSTRAINT operation_binding_revision CHECK (binding_revision > 0);
ALTER TABLE ${catalog}.${control_schema}.genie_space_operations DROP CONSTRAINT IF EXISTS operation_fact;
ALTER TABLE ${catalog}.${control_schema}.genie_space_operations ADD CONSTRAINT operation_fact CHECK (fact_kind IN ('operation','create_intent','approval_request','approval_vote','approval_granted','approval_consumed','approval_invalidated','release','receipt','acknowledgement','break_glass','recovery'));
