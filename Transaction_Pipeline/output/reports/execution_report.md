# Transaction Pipeline - Execution Report

## Execution Details

- Run ID: run_20261106T100114_285c5a
- Source file: transactions.csv
- Status: SUCCESS
- Execution time: 0.01 seconds

## Record Summary

- Total input records: 389
- Accepted records: 335
- Rejected records: 54
- Accepted percentage: 86.12%
- Rejected percentage: 13.88%

## Curated Database Summary

- Total curated transactions in database: 130
- Total curated transaction legs in database: 335
- Newly inserted records in latest run: 0
- Already loaded records in latest run: 335

## Rejected Records by Reason

Counts below are the persisted rejection decisions for this input file hash. Exact reruns reuse the original quarantine rows, so the latest run may insert zero new rejects.

| Rejection reason | Record count |
|---|---:|
| TXN_REJECTED_PEER_LEG | 27 |
| INVALID_AMOUNT | 5 |
| DUPLICATE_CONFLICT | 4 |
| TXN_LEG_TS_SKEW | 3 |
| TXN_IMBALANCE | 3 |
| MISSING_REQUIRED_FIELD | 2 |
| MALFORMED_ROW | 2 |
| INGESTED_BEFORE_EVENT | 2 |
| CURRENCY_MISMATCH | 2 |
| ACCOUNT_CLOSED | 2 |
| UNKNOWN_ACCOUNT | 1 |
| DUPLICATE_LEG | 1 |

## Reconciliation Checks

| Check | Result |
|---|---|
| Input = Accepted + Rejected | PASS |
| Accepted = Inserted + Already Loaded | PASS |

## Final Result

*PASS*
