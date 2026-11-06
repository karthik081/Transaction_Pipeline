# Data dictionary

All names use `snake_case`. Timestamps are normalized to UTC ISO-8601 strings. Monetary values in curated tables are integer minor units; the currency exponent is configured in `txn_pipeline/config.py` (USD/EUR/INR: 2, JPY: 0).

## Source files

### `transactions.csv` — one row per transaction leg

| Column | Type / format | Required | Meaning / rules |
|---|---|---:|---|
| `txn_id` | string | Yes | Stable source transaction identifier; groups all legs of one transaction. |
| `leg_no` | positive integer | Yes | Unique leg number within `txn_id`; combined key is deterministic. |
| `account_id` | string | Yes | Foreign key to `accounts.csv.account_id`. |
| `direction` | `DEBIT` / `CREDIT` | Yes | Debit reduces account balance; credit increases it. |
| `amount` | decimal string | Yes | Positive major-unit amount; precision must match currency exponent. |
| `currency` | ISO currency code | Yes | Must be configured and equal the referenced account currency. |
| `event_ts` | ISO-8601 with timezone | Yes | Business event time; normalized to UTC. |
| `ingested_at` | ISO-8601 with timezone | Yes | Source ingestion time; must not precede `event_ts`. |
| `txn_type` | string | Yes | Source transaction classification, retained in curated output. |

### `accounts.csv` — one row per account

| Column | Type / format | Required | Meaning / rules |
|---|---|---:|---|
| `account_id` | string | Yes | Unique account key; duplicates fail the source batch. |
| `account_currency` | ISO currency code | Yes | Must be configured. |
| `status` | `ACTIVE` / `CLOSED` | Yes | Only active accounts can be used by transaction legs. |
| `opening_balance_minor` | integer | Yes | Starting balance in the account currency's minor units. |

## Curated and operational tables

| Table | Grain / primary key | Important columns and meaning |
|---|---|---|
| `dim_account` | One current row per account; `account_id` | Currency, status, opening balance, and last modifying `run_id`. |
| `dim_account_history` | One prior account version per history row; `history_id` | Previous attributes retained when an account dimension changes. |
| `fact_txn` | One row per complete transaction; `txn_id` | `leg_count`, currency, debit total in minor units, first/last event timestamps, late-arrival flag, source file and run. |
| `fact_txn_leg` | One row per transaction leg; `leg_id`; unique (`txn_id`, `leg_no`) | Account, direction, amount, signed amount, currency, event/ingestion timestamps, content hash, source file/row and run. |
| `rejected_record` | One rejected source row per reason; `reject_id` | Reason code/detail, raw source record, transaction/leg keys, source file/row and run. |
| `pipeline_run` | One row per invocation; `run_id` | Status, start/end, row and transaction counts, timing, warnings and failure reason. |
| `vw_account_balance` | One row per account | Opening balance + signed curated movement = closing balance. |

## Derived fields and invariants

- `leg_id = SHA1(txn_id + '|' + leg_no)[:16]`.
- `amount_minor` is derived from a decimal string using `Decimal`, never binary floating point.
- `signed_amount_minor` is positive for credits and negative for debits.
- `content_hash` identifies business content; `ingested_at` is intentionally excluded so an exact replay with a later ingestion timestamp remains idempotent.
- `is_late_arriving` is true when `ingested_at - event_ts` exceeds the configured 24-hour threshold.
- A transaction is curated only as a complete set of at least two legs, in one currency, with equal debit and credit totals and event-time skew within the configured limit.
- A previously curated transaction can only be replayed if the complete incoming leg set and content hashes match the curated set. Partial, extended, or changed versions are quarantined.
- Every accepted leg references an account and a curated transaction. Rejected source rows retain raw data and source/run lineage.

## Assumptions and scope boundaries

- The sample represents transfers/payments where each transaction balances within one currency. FX conversion and cross-currency settlement are out of scope.
- Account balances are reported from the supplied opening balance and curated signed movements. Overdraft and running-balance gating are not enforced.
- All legs for a new transaction must arrive in the same input batch. A transaction split across files requires a staging/watermark design and is not supported in this vertical slice.
- Account master-data quality errors fail the run before any curated publish; there is no separate account-master quarantine table.
