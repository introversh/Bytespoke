-- Database Schema for Resilient Pipeline and Operational Telemetry
-- Target RDBMS: SQLite 3 / Relational SQL

PRAGMA foreign_keys = ON;

-- 1. Ingested Batches Table
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    record_count INTEGER NOT NULL,
    payload_checksum TEXT NOT NULL,
    source_file TEXT,
    status TEXT NOT NULL CHECK(status IN ('INGESTED', 'VALIDATED', 'PROCESSED', 'FAILED')),
    ingested_at TEXT NOT NULL
);

-- 2. Transaction Records Table
CREATE TABLE IF NOT EXISTS transactions (
    txn_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    amount REAL NOT NULL,
    currency TEXT NOT NULL,
    fx_rate REAL DEFAULT 1.0,
    amount_usd REAL,
    is_high_risk BOOLEAN DEFAULT 0,
    status TEXT NOT NULL CHECK(status IN ('PENDING', 'VALIDATED', 'SETTLED', 'QUARANTINED', 'REJECTED')),
    validated_at TEXT,
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id) ON DELETE CASCADE
);

-- 3. Pipeline Runs Table (Operational Metadata)
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    overall_status TEXT NOT NULL CHECK(overall_status IN ('SUCCESS', 'DEGRADED_SUCCESS', 'FAILED')),
    start_time TEXT NOT NULL,
    end_time TEXT,
    total_duration_ms REAL,
    recovery_occurred BOOLEAN DEFAULT 0,
    recovered_step TEXT,
    host_name TEXT,
    os_name TEXT,
    python_version TEXT,
    process_id INTEGER,
    memory_rss_mb REAL,
    archive_seal TEXT
);

-- 4. Pipeline Step Executions Table
CREATE TABLE IF NOT EXISTS pipeline_steps (
    step_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    step_index INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('PENDING', 'RUNNING', 'SUCCESS', 'RECOVERED', 'FAILED', 'SKIPPED')),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 0,
    start_time TEXT,
    end_time TEXT,
    duration_ms REAL DEFAULT 0.0,
    recovery_notes TEXT,
    skip_reason TEXT,
    error_message TEXT,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);

-- 5. Step Retry Attempts Table
CREATE TABLE IF NOT EXISTS step_retries (
    retry_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    step_name TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    error_type TEXT NOT NULL,
    error_message TEXT NOT NULL,
    backoff_delay_sec REAL NOT NULL,
    stack_trace_snippet TEXT,
    timestamp TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);

-- 6. Pipeline Event Stream Table
CREATE TABLE IF NOT EXISTS pipeline_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    step_name TEXT,
    level TEXT NOT NULL CHECK(level IN ('INFO', 'WARN', 'RETRY', 'RECOVERY', 'ERROR')),
    message TEXT NOT NULL,
    details_json TEXT,
    timestamp TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);

-- 7. Root Cause Analysis Records Table
CREATE TABLE IF NOT EXISTS rca_records (
    rca_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    incident_ref TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL,
    executive_summary TEXT NOT NULL,
    root_cause_technical TEXT NOT NULL,
    five_whys_json TEXT NOT NULL,
    capa_actions_json TEXT NOT NULL,
    generated_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_txns_batch ON transactions(batch_id);
CREATE INDEX IF NOT EXISTS idx_steps_run ON pipeline_steps(run_id);
CREATE INDEX IF NOT EXISTS idx_retries_run ON step_retries(run_id);
CREATE INDEX IF NOT EXISTS idx_events_run ON pipeline_events(run_id);

-- Operational Reporting View
CREATE VIEW IF NOT EXISTS v_pipeline_run_summary AS
SELECT 
    r.run_id,
    r.pipeline_name,
    r.overall_status,
    r.total_duration_ms,
    r.recovery_occurred,
    r.recovered_step,
    COUNT(DISTINCT s.step_id) as total_steps,
    SUM(CASE WHEN s.status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_steps,
    SUM(CASE WHEN s.status = 'RECOVERED' THEN 1 ELSE 0 END) as recovered_steps,
    SUM(CASE WHEN s.status = 'SKIPPED' THEN 1 ELSE 0 END) as skipped_steps,
    COUNT(ret.retry_id) as total_retries_logged
FROM pipeline_runs r
LEFT JOIN pipeline_steps s ON r.run_id = s.run_id
LEFT JOIN step_retries ret ON r.run_id = ret.run_id
GROUP BY r.run_id;
