# Design and operational notes

## Processing sequence

1. Hash and read the source file; validate the required header and account master data.
2. Parse each transaction row, validate required values, references, amount precision and timestamp ordering.
3. Deduplicate within the batch by deterministic (`txn_id`, `leg_no`) identity. Exact duplicate rows keep the first source row; conflicting duplicates reject the transaction.
4. Compare each incoming transaction with curated history. Exact whole-transaction replay is counted as already loaded. Any partial, extended or changed transaction is rejected rather than appended to an existing fact.
5. Validate transaction-level leg count, currency, debit/credit equality and event timestamp skew.
6. Publish account changes, complete transaction facts, legs, rejects and run metrics in one SQLite transaction.
7. Produce currency-aware reconciliation checks and CSV/report exports.

## Why these choices

- **Whole-transaction atomicity:** a partial transfer is not a valid analytical fact. If any leg fails, sibling legs are rejected.
- **Integer minor units:** exact monetary comparisons avoid floating-point rounding.
- **SQLite + standard library:** makes the project runnable locally with Python 3.10+ and no service credentials. It is intentionally a small, inspectable vertical slice rather than a distributed platform.
- **Deterministic IDs and content hashes:** reruns do not depend on mutable run metadata; a changed transaction cannot silently overwrite curated facts.
- **One reject row per source row:** supports source-to-outcome reconciliation and traceability.
- **Single transaction + `executemany`:** reduces SQLite write overhead while preserving atomic rollback.

## Retry and backfill

- If a run fails during publish, the data transaction is rolled back. Re-run the same CLI command after correcting the underlying issue.
- Replaying the same complete file inserts no additional curated legs or rejects.
- Older complete transactions can be backfilled later; event ordering is reported, not used as a global watermark.
- Existing transaction IDs are immutable at the curated grain. Any partial, extra-leg or changed replay is quarantined.
- Missing required columns or invalid account master data fail before curated writes.

## Known limitations

- No cross-file open-transaction staging or watermarking.
- No FX conversion, overdraft policy, running-balance rejection, or account-level daily balance snapshots.
- SQLite is single-writer; use a server database and an orchestrator for concurrent or production workloads.
- Operational logs are CLI output and `pipeline_run`; production deployment should add structured logs, alerting and metrics export.
