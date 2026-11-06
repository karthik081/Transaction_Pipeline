# Transaction Leg Validation Pipeline

Validates debit/credit legs of financial transactions (balance, currency, reference keys,
event ordering), quarantines invalid records with a traceable reason, and publishes only
valid transactions to curated tables. Python 3.10+ **standard library only** (csv, sqlite3,
decimal, unittest) - no install step, no credentials.

## Quick start (clean environment)
```bash
python -m unittest discover -s tests -t . -v                      # 31 tests
python -m txn_pipeline generate --txns 150 --seed 42 --anomaly-rate 0.12   # data/sample/*
python -m txn_pipeline run                                        # ingest -> output/curated.db
python -m txn_pipeline run                                        # rerun: 0 inserts (idempotent)
python -m txn_pipeline report                                     # reconciliation + report + CSV export
python -m txn_pipeline generate --txns 40000 --out-dir data/large && \
python -m txn_pipeline run --input data/large/transactions.csv --accounts data/large/accounts.csv
python -m txn_pipeline benchmark --txns 40000                     # ~100k rows
```
Every path/threshold is a CLI flag or a field of `Config` (`txn_pipeline/config.py`).

## Architecture
```
transactions.csv ─► read_input ─► row validation ─► in-batch dedupe ─► whole-txn replay check
accounts.csv     ─► read_accounts      │ fail                │ fail            │ fail
                                       ▼                     ▼                 ▼
                                 rejected_record  (one row per rejected source row, raw JSON kept)
                                       ▲
              txn-level checks (legs>=2, one currency, debits=credits, leg time skew)
                                       │ pass
                                       ▼
        ONE sqlite transaction: dim_account upsert → fact_txn → fact_txn_leg → rejected_record
                                       ▼
                 pipeline_run (status, counts, timings, error)  →  report / CSV export
```
Modules: `validators.py` (pure rules) · `pipeline.py` (orchestration, load) · `generator.py` ·
`reports.py` (reconciliation) · `__main__.py` (CLI). See `docs/data_dictionary.md` for column-level
meanings and `docs/design_notes.md` for implementation trade-offs and operational behavior.

## Schemas
**Input `transactions.csv`** (one row = one leg): `txn_id, leg_no, account_id, direction (DEBIT|CREDIT),
amount (decimal string, major units), currency, event_ts, ingested_at (ISO-8601 with tz), txn_type`.
Column order is irrelevant; extra columns are ignored with a warning; missing columns fail the run.
**Input `accounts.csv`**: `account_id, account_currency, status (ACTIVE|CLOSED), opening_balance_minor`.

| Table | Grain / key | Notes |
|---|---|---|
| `dim_account` | `account_id` PK | upsert; old versions kept in `dim_account_history` |
| `fact_txn` | `txn_id` PK | derived: `leg_count`, `total_amount_minor`, first/last event ts, `is_late_arriving` |
| `fact_txn_leg` | `leg_id` PK, UNIQUE(`txn_id`,`leg_no`) | `leg_id = sha1(txn_id\|leg_no)[:16]`; `amount_minor` integer >0; `signed_amount_minor` CREDIT+ / DEBIT-; `content_hash`; `source_file`, `source_row_no`, `run_id` |
| `rejected_record` | `reject_id` PK = sha1(input_sha256\|row_no\|reason)[:16] | `reason_code`, `reason_detail`, `raw_record` (JSON), `source_file`, `source_row_no`, `run_id` |
| `pipeline_run` | `run_id` PK | status RUNNING/SUCCESS/EMPTY_INPUT/FAILED, rows_read/accepted/inserted/already_loaded/rejected, txns_*, late/out-of-order counts, duration, warnings, error |
| `vw_account_balance` | view | opening + net movement = closing balance |

Naming: snake_case tables/columns, `dim_`/`fact_`/`vw_` prefixes, timestamps ISO-8601 UTC (`...Z`),
money stored as **integer minor units** (no floats; JPY exponent 0, others 2), deterministic IDs.

## Validation rules (reason codes)
Row level (first failing check wins, one reject row per source row): `MALFORMED_ROW`,
`MISSING_REQUIRED_FIELD`, `INVALID_DIRECTION`, `INVALID_LEG_NO`, `UNSUPPORTED_CURRENCY`,
`UNKNOWN_ACCOUNT` (referential integrity), `ACCOUNT_CLOSED`, `CURRENCY_MISMATCH` (leg vs account),
`INVALID_AMOUNT` (non-numeric, <=0, NaN, too many decimals), `INVALID_TIMESTAMP`,
`INGESTED_BEFORE_EVENT` (ordering).
Duplicates: exact replay → first kept, rest `DUPLICATE_LEG`; same key, different content →
all rows `DUPLICATE_CONFLICT`; an existing curated transaction is accepted only as an exact whole-set
replay. Changed legs use `CONFLICTS_WITH_CURATED`; partial or extended incoming leg sets use
`INCOMPLETE_OR_CHANGED_CURATED_TXN`. Account master data rejects duplicate IDs, unsupported currencies,
invalid statuses and missing IDs before publishing.
Transaction level: `TXN_TOO_FEW_LEGS`, `TXN_MIXED_CURRENCY`, `TXN_IMBALANCE` (debits ≠ credits),
`TXN_LEG_TS_SKEW` (legs > 60 s apart). If any leg of a txn fails, sibling legs are quarantined as
`TXN_REJECTED_PEER_LEG` - **a transaction is published whole or not at all**.
Warnings (loaded, not rejected): late arrival (`ingested_at - event_ts > 24h` → `is_late_arriving=1`);
out-of-order rows (event_ts earlier than previous row) are counted; processing is order-independent.

## Idempotency, retry, restart, backfill
- Deterministic keys + `INSERT OR IGNORE`: rerunning the same file inserts 0 legs/rejects
  (`rows_already_loaded` shows what was skipped); tested.
- The whole publish is one DB transaction. Crash/exception → rollback, run marked `FAILED` with the error
  (tested with an injected failure). **Retry = run the same command again.**
- Schema break (missing required column / unreadable file) fails the run before any write.
- Backfill: feed older files in any order; ordering does not matter for keys or validation.
  Re-sent transactions must match the complete previously curated leg set and content; partial
  batches, added legs, or changed content for an existing transaction are quarantined as a whole.

## Evidence & results (seed 42, 150 txns)
`output/reports/validation_report.md` (12 reconciliation checks, all PASS), `output/curated/*.csv`,
`output/test_evidence.txt`, `data/sample/expected_labels.csv` (kept **separate** from input; the test
suite compares pipeline decisions with these labels for 3,000 txns × 13 anomaly types).
Sample run: 389 rows read → 335 accepted (130 txns) + 54 quarantined (20 txns); rerun → 0 inserted.
Reconciliation: `rows_read = accepted + rejected`, `accepted = inserted + already_loaded`,
curated txns balanced, debit/credit totals reconcile **per currency**, no orphan accounts, rejects
traceable to run + source row. The current suite has **31 passing tests**, including regression tests
for partial/changed replays of previously curated transactions and invalid account master data.

## Performance (`python -m txn_pipeline benchmark --txns 40000`, ~100k rows, laptop sandbox)
| mode | total | validate | load | rows/s |
|---|---|---|---|---|
| per-row insert | 5.56 s | 3.23 s | 1.72 s | ~18.0k |
| `executemany` | 4.07 s | 2.25 s | 1.49 s | ~24.7k |
Finding: batch insertion was ~27% faster in this execution. Both variants use one transaction; Python-side row validation remains a significant share of total time. Optimization applied: `executemany` plus a single atomic transaction. Next optimization: scope existing-key lookups to transaction IDs in the incoming batch and consider chunked validation only if profiling justifies the added complexity.

## Assumptions
CSV input; one currency per transaction (no FX); customer-style signs (CREDIT increases balance);
`opening_balance_minor` is a given reference; all rows of a txn arrive in the same file.

## Known limitations / next steps
- A txn split across files (legs in different batches) would be rejected as too-few-legs; would need a
  staging/watermark table to hold open transactions.
- Overdraft / running-balance rules are not enforced (balances are reported, not gated).
- SQLite is single-writer; swap to Postgres/Parquet+DuckDB for scale. Add Airflow/cron scheduling,
  metrics export (Prometheus) and structured JSON logs.
- Account master-data errors fail the batch before publication; a separate master-data quarantine
  workflow is not implemented.

## Design / trade-off note
- **Whole-transaction atomicity** over leg-by-leg acceptance: a half-posted transfer is worse than a delayed one.
- **One reject row per source row** keeps the identity `read = accepted + rejected` exact and auditable.
- **Integer minor units + Decimal parsing** avoid float drift and make balance checks exact.
- **Stdlib only** keeps setup to zero and the code readable; trade-off is single-process speed.
- **Deterministic reject_id/leg_id** make idempotency a property of the keys, not of run bookkeeping.


## Report behavior on reruns

The execution report shows the latest run's input, accepted/rejected decision counts,
and newly inserted/already-loaded counts. Rejection reason counts are recovered from
the persisted quarantine records for the same input file SHA-256. This is intentional:
deterministic reject IDs prevent duplicate quarantine rows on reruns, so an exact rerun
can correctly show `rejects_inserted = 0` while still reporting the original rejection
reasons for that input batch.
