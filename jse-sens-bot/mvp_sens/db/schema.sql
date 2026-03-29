CREATE TABLE IF NOT EXISTS sens_financial_announcements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sens_id TEXT UNIQUE,
    company TEXT,
    title TEXT,
    announcement_date TEXT,
    pdf_url TEXT,
    local_pdf_path TEXT,
    first_seen_run_id TEXT,
    first_seen_at TEXT,
    category TEXT NOT NULL DEFAULT 'other',
    classification_reason TEXT,
    classification_version TEXT,
    classified_at TEXT,
    analyst_relevant INTEGER NOT NULL DEFAULT 0,
    relevance_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS release_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sens_id TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    signal_datetime TEXT NOT NULL,
    source_text TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'title',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (sens_id, signal_type, signal_datetime, source_text),
    FOREIGN KEY (sens_id) REFERENCES sens_financial_announcements(sens_id)
);

CREATE TABLE IF NOT EXISTS pipeline_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    scraped_count INTEGER NOT NULL DEFAULT 0,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    skipped_irrelevant_count INTEGER NOT NULL DEFAULT 0,
    skipped_existing_count INTEGER NOT NULL DEFAULT 0,
    skipped_failed_download_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ingest_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    event_type TEXT NOT NULL,
    sens_id TEXT,
    message TEXT NOT NULL,
    metadata_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES ingest_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_ingest_runs_started_at
ON ingest_runs(started_at);

CREATE INDEX IF NOT EXISTS idx_ingest_runs_status
ON ingest_runs(status);

CREATE INDEX IF NOT EXISTS idx_ingest_events_run_id
ON ingest_events(run_id);

CREATE INDEX IF NOT EXISTS idx_ingest_events_stage
ON ingest_events(stage);

CREATE INDEX IF NOT EXISTS idx_ingest_events_created_at
ON ingest_events(created_at);

CREATE INDEX IF NOT EXISTS idx_announcements_category
ON sens_financial_announcements(category);

CREATE INDEX IF NOT EXISTS idx_announcements_analyst_relevant
ON sens_financial_announcements(analyst_relevant);

CREATE INDEX IF NOT EXISTS idx_announcements_classified_at
ON sens_financial_announcements(classified_at);

CREATE INDEX IF NOT EXISTS idx_announcements_first_seen_run_id
ON sens_financial_announcements(first_seen_run_id);

CREATE INDEX IF NOT EXISTS idx_announcements_first_seen_at
ON sens_financial_announcements(first_seen_at);

CREATE INDEX IF NOT EXISTS idx_release_signals_sens_id
ON release_signals(sens_id);

CREATE INDEX IF NOT EXISTS idx_release_signals_signal_datetime
ON release_signals(signal_datetime);

CREATE INDEX IF NOT EXISTS idx_pipeline_state_updated_at
ON pipeline_state(updated_at);
