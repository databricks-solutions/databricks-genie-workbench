CREATE TABLE IF NOT EXISTS ${catalog}.${control_schema}.genie_space_registry (
  registry_event_id STRING NOT NULL,
  space_key STRING NOT NULL,
  binding_id STRING NOT NULL,
  binding_revision BIGINT NOT NULL,
  event_sequence BIGINT NOT NULL,
  event_type STRING NOT NULL,
  supersedes_event_id STRING,
  workspace_id STRING NOT NULL,
  space_id STRING,
  environment STRING NOT NULL,
  governed_by STRING NOT NULL,
  binding_reason STRING NOT NULL,
  bound_by STRING NOT NULL,
  bound_at TIMESTAMP NOT NULL,
  create_operation_id STRING,
  evidence_json STRING
) USING DELTA TBLPROPERTIES ('delta.appendOnly'='true');
-- CHECK constraints are attached via ALTER; inline table DDL supports only
-- PRIMARY KEY / FOREIGN KEY. DROP IF EXISTS + ADD keeps re-runs idempotent.
ALTER TABLE ${catalog}.${control_schema}.genie_space_registry DROP CONSTRAINT IF EXISTS registry_governance;
ALTER TABLE ${catalog}.${control_schema}.genie_space_registry ADD CONSTRAINT registry_governance CHECK (governed_by = 'workbench');
ALTER TABLE ${catalog}.${control_schema}.genie_space_registry DROP CONSTRAINT IF EXISTS registry_event;
ALTER TABLE ${catalog}.${control_schema}.genie_space_registry ADD CONSTRAINT registry_event CHECK (event_type IN ('provisional','bound','tombstoned','rebound'));
ALTER TABLE ${catalog}.${control_schema}.genie_space_registry DROP CONSTRAINT IF EXISTS registry_revision;
ALTER TABLE ${catalog}.${control_schema}.genie_space_registry ADD CONSTRAINT registry_revision CHECK (binding_revision > 0 AND event_sequence > 0);
