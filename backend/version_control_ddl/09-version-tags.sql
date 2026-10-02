CREATE TABLE IF NOT EXISTS ${catalog}.${control_schema}.genie_space_version_tags (
  version_id STRING NOT NULL PRIMARY KEY,
  space_id STRING NOT NULL,
  label STRING NOT NULL,
  note STRING,
  author STRING,
  updated_at TIMESTAMP NOT NULL
) USING DELTA TBLPROPERTIES ('delta.isolationLevel'='Serializable');
-- Mutable UX metadata (not a governed VC fact): a version carries AT MOST ONE tag+comment,
-- so version_id is the PRIMARY KEY (a 1:1/0..1 mapping to genie_space_versions). This table
-- is intentionally NOT appendOnly — the writer upserts (MERGE) and deletes tags in place,
-- exactly like the coordination table. CHECK constraints are attached via ALTER because
-- inline table DDL supports only PRIMARY KEY / FOREIGN KEY; DROP IF EXISTS + ADD keeps
-- re-runs idempotent.
ALTER TABLE ${catalog}.${control_schema}.genie_space_version_tags DROP CONSTRAINT IF EXISTS version_tags_label_len;
ALTER TABLE ${catalog}.${control_schema}.genie_space_version_tags ADD CONSTRAINT version_tags_label_len CHECK (length(label) BETWEEN 1 AND 60);
