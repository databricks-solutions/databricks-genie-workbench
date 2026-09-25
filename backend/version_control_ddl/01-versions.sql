CREATE TABLE IF NOT EXISTS ${catalog}.${control_schema}.genie_space_versions (
  version_id STRING NOT NULL,
  binding_id STRING NOT NULL,
  binding_revision BIGINT NOT NULL,
  space_key STRING NOT NULL,
  workspace_id STRING NOT NULL,
  space_id STRING NOT NULL,
  environment STRING NOT NULL,
  observation_key STRING NOT NULL,
  observed_at TIMESTAMP NOT NULL,
  observation_reason STRING NOT NULL,
  observed_by STRING NOT NULL,
  actor_kind STRING NOT NULL,
  origin STRING NOT NULL,
  parent_version_id STRING,
  restored_from_version_id STRING,
  operation_id STRING,
  attempt_id STRING,
  generation BIGINT,
  api_update_time TIMESTAMP,
  response_envelope_json STRING,
  response_envelope_uri STRING,
  response_envelope_digest STRING NOT NULL,
  serialized_space_json STRING NOT NULL,
  restorable_metadata_json STRING NOT NULL,
  raw_state_digest STRING NOT NULL,
  canonical_state_json STRING NOT NULL,
  config_fingerprint STRING NOT NULL,
  benchmark_fingerprint STRING NOT NULL,
  metadata_fingerprint STRING NOT NULL,
  state_digest STRING NOT NULL,
  canonicalizer_version STRING NOT NULL,
  optimizer_run_id STRING,
  champion_id STRING,
  release_id STRING
) USING DELTA TBLPROPERTIES ('delta.appendOnly'='true');
-- CHECK constraints are attached via ALTER; inline table DDL supports only
-- PRIMARY KEY / FOREIGN KEY. DROP IF EXISTS + ADD keeps re-runs idempotent.
ALTER TABLE ${catalog}.${control_schema}.genie_space_versions DROP CONSTRAINT IF EXISTS valid_origin;
ALTER TABLE ${catalog}.${control_schema}.genie_space_versions ADD CONSTRAINT valid_origin CHECK (origin IN ('workbench','external','optimizer','restore','promotion','unknown','create'));
ALTER TABLE ${catalog}.${control_schema}.genie_space_versions DROP CONSTRAINT IF EXISTS valid_revision;
ALTER TABLE ${catalog}.${control_schema}.genie_space_versions ADD CONSTRAINT valid_revision CHECK (binding_revision > 0);
ALTER TABLE ${catalog}.${control_schema}.genie_space_versions DROP CONSTRAINT IF EXISTS envelope_present;
ALTER TABLE ${catalog}.${control_schema}.genie_space_versions ADD CONSTRAINT envelope_present CHECK ((response_envelope_json IS NOT NULL AND response_envelope_uri IS NULL) OR (response_envelope_json IS NULL AND response_envelope_uri IS NOT NULL));
