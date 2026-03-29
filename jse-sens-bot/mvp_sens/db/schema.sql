CREATE TABLE IF NOT EXISTS sens_financial_announcements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sens_id TEXT UNIQUE,
    company TEXT,
    title TEXT,
    announcement_date TEXT,
    pdf_url TEXT,
    local_pdf_path TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);