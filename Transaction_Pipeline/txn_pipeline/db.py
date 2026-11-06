import os
import sqlite3

DDL = """
CREATE TABLE IF NOT EXISTS dim_account (
    account_id TEXT PRIMARY KEY,
    account_currency TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ACTIVE','CLOSED')),
    opening_balance_minor INTEGER NOT NULL,
    run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_account_history (
    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,
    account_currency TEXT NOT NULL,
    status TEXT NOT NULL,
    opening_balance_minor INTEGER NOT NULL,
    superseded_by_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_txn (
    txn_id TEXT PRIMARY KEY,
    txn_type TEXT NOT NULL,
    currency TEXT NOT NULL,
    leg_count INTEGER NOT NULL,
    total_amount_minor INTEGER NOT NULL,      -- sum of debits (= sum of credits)
    first_event_ts TEXT NOT NULL,
    last_event_ts TEXT NOT NULL,
    is_late_arriving INTEGER NOT NULL,
    source_file TEXT NOT NULL,
    run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_txn_leg (
    leg_id TEXT PRIMARY KEY,                  -- sha1(txn_id|leg_no)[:16]
    txn_id TEXT NOT NULL REFERENCES fact_txn(txn_id),
    leg_no INTEGER NOT NULL,
    account_id TEXT NOT NULL REFERENCES dim_account(account_id),
    direction TEXT NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
    amount_minor INTEGER NOT NULL CHECK (amount_minor > 0),
    signed_amount_minor INTEGER NOT NULL,     -- CREDIT +, DEBIT -
    currency TEXT NOT NULL,
    event_ts TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    txn_type TEXT NOT NULL,
    is_late_arriving INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_no INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    UNIQUE (txn_id, leg_no)
);
CREATE TABLE IF NOT EXISTS rejected_record (
    reject_id TEXT PRIMARY KEY,               -- sha1(input_sha|row_no|reason)[:16]
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_no INTEGER NOT NULL,
    txn_id TEXT,
    leg_no TEXT,
    reason_code TEXT NOT NULL,
    reason_detail TEXT,
    raw_record TEXT NOT NULL,
    rejected_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    input_sha256 TEXT,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT NOT NULL,                     -- RUNNING | SUCCESS | EMPTY_INPUT | FAILED
    rows_read INTEGER, rows_accepted INTEGER, rows_inserted INTEGER,
    rows_already_loaded INTEGER, rows_rejected INTEGER, rejects_inserted INTEGER,
    txns_accepted INTEGER, txns_rejected INTEGER,
    late_arriving_rows INTEGER, out_of_order_rows INTEGER,
    duration_seconds REAL, warnings TEXT, error TEXT
);
CREATE VIEW IF NOT EXISTS vw_account_balance AS
SELECT a.account_id, a.account_currency, a.opening_balance_minor,
       COALESCE(SUM(l.signed_amount_minor), 0) AS net_movement_minor,
       a.opening_balance_minor + COALESCE(SUM(l.signed_amount_minor), 0) AS closing_balance_minor
FROM dim_account a LEFT JOIN fact_txn_leg l ON l.account_id = a.account_id
GROUP BY a.account_id;
"""


def connect(path):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)  # autocommit; we use explicit BEGIN
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(DDL)
    return conn
