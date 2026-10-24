CREATE TABLE IF NOT EXISTS ${catalog}.${control_schema}.genie_ops_coordination (
  binding_id STRING NOT NULL,
  binding_revision BIGINT NOT NULL,
  space_key STRING NOT NULL,
  workspace_id STRING NOT NULL,
  space_id STRING,
  generation BIGINT NOT NULL,
  row_version BIGINT NOT NULL,
  state STRING NOT NULL,
  holder STRING,
  attempt_id STRING,
  executor_kind STRING,
  executor_ref STRING,
  lease_expires_at TIMESTAMP,
  active_operation_id STRING,
  idempotency_key STRING,
  request_digest STRING,
  approval_id STRING,
  approval_digest STRING,
  approval_consumption_published BOOLEAN NOT NULL,
  pre_version_id STRING,
  preimage_digest STRING,
  create_intent_event_id STRING,
  expected_base_fingerprint STRING,
  admitted_at TIMESTAMP,
  mutation_stage STRING,
  checkpoint_json STRING,
  unresolved BOOLEAN NOT NULL,
  quarantine_reason STRING,
  termination_evidence_json STRING,
  observed_head_version_id STRING,
  approved_head_version_id STRING,
  deployed_head_version_id STRING,
  observed_sequence BIGINT NOT NULL,
  updated_at TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES ('delta.isolationLevel'='Serializable');
-- CHECK constraints are attached via ALTER; inline table DDL supports only
-- PRIMARY KEY / FOREIGN KEY. DROP IF EXISTS + ADD keeps re-runs idempotent.
ALTER TABLE ${catalog}.${control_schema}.genie_ops_coordination DROP CONSTRAINT IF EXISTS coordination_state;
ALTER TABLE ${catalog}.${control_schema}.genie_ops_coordination ADD CONSTRAINT coordination_state CHECK (state IN ('idle','observing','reserved','admitted','quarantined'));
ALTER TABLE ${catalog}.${control_schema}.genie_ops_coordination DROP CONSTRAINT IF EXISTS nonnegative_fence;
ALTER TABLE ${catalog}.${control_schema}.genie_ops_coordination ADD CONSTRAINT nonnegative_fence CHECK (generation >= 0 AND row_version >= 0);
