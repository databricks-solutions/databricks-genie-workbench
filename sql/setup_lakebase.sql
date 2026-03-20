-- Genie Workbench Lakebase Schema
-- Run this once after creating your Lakebase PostgreSQL instance.
--
-- Usage:
--   psql -h <host> -U <user> -d <database> -f sql/setup_lakebase.sql
--
-- Or via the setup script:
--   ./scripts/setup_lakebase.sh

-- ---------------------------------------------------------------------------
-- Scan results (stores GenieIQ score history per space)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS scan_results (
    id          SERIAL PRIMARY KEY,
    space_id    VARCHAR(64) NOT NULL,
    score       INTEGER     NOT NULL CHECK (score >= 0 AND score <= 100),
    maturity    VARCHAR(32) NOT NULL,
    breakdown   JSONB       NOT NULL DEFAULT '{}',
    findings    JSONB       NOT NULL DEFAULT '[]',
    next_steps  JSONB       NOT NULL DEFAULT '[]',
    scanned_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (space_id, scanned_at)
);

CREATE INDEX IF NOT EXISTS idx_scan_results_space_id  ON scan_results(space_id);
CREATE INDEX IF NOT EXISTS idx_scan_results_scanned_at ON scan_results(scanned_at DESC);
CREATE INDEX IF NOT EXISTS idx_scan_results_score      ON scan_results(score);

-- ---------------------------------------------------------------------------
-- Starred spaces
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS starred_spaces (
    space_id   VARCHAR(64) PRIMARY KEY,
    starred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Seen spaces (tracks which spaces have been observed, for "new" badges)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS seen_spaces (
    space_id   VARCHAR(64) PRIMARY KEY,
    first_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Optimization runs (tracks benchmark accuracy from the optimization workflow)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS optimization_runs (
    id              SERIAL PRIMARY KEY,
    space_id        VARCHAR(64) NOT NULL,
    benchmark_total INTEGER NOT NULL,
    benchmark_correct INTEGER NOT NULL,
    accuracy        REAL NOT NULL,  -- benchmark_correct / benchmark_total
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_optimization_runs_space_id ON optimization_runs(space_id);

-- ---------------------------------------------------------------------------
-- Grant permissions (update with your app's DB user before running)
-- ---------------------------------------------------------------------------
-- GRANT ALL ON ALL TABLES IN SCHEMA public TO genie_workbench_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO genie_workbench_user;
