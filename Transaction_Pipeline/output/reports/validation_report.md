# Validation report

Curated: **130 transactions, 335 legs**; quarantined rows: **54**.

## Reconciliation checks (0 violations = pass)

| check | violations | result |
|---|---|---|
| curated_txns_balanced | 0 | PASS |
| total_debits_equal_total_credits | 0 | PASS |
| net_movement_sums_to_zero | 0 | PASS |
| no_orphan_accounts | 0 | PASS |
| no_currency_mismatch_vs_account | 0 | PASS |
| fact_txn_leg_count_matches_legs | 0 | PASS |
| fact_txn_total_matches_debits | 0 | PASS |
| no_txn_without_legs | 0 | PASS |
| run_counts_reconcile (read = accepted + rejected) | 0 | PASS |
| run_counts_reconcile (accepted = inserted + already_loaded) | 0 | PASS |
| rejects_traceable_to_run_and_source | 0 | PASS |
| curated_legs_equal_sum_of_inserted_rows | 0 | PASS |

## Pipeline runs

| run_id | source | status | read | accepted | inserted | already_loaded | rejected | txns_ok | txns_rejected | late | out_of_order | secs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| run_20261105T052457_0d9177 | transactions.csv | SUCCESS | 389 | 335 | 335 | 0 | 54 | 130 | 20 | 3 | 25 | 0.0437 |
| run_20261105T052459_36937d | transactions.csv | SUCCESS | 389 | 335 | 0 | 335 | 54 | 130 | 20 | 3 | 25 | 0.0141 |
| run_20261105T071707_0d4c21 | transactions.csv | SUCCESS | 389 | 335 | 0 | 335 | 54 | 130 | 20 | 3 | 25 | 0.0131 |
| run_20261105T162439_88a1c6 | transactions.csv | SUCCESS | 389 | 335 | 0 | 335 | 54 | 130 | 20 | 3 | 25 | 0.032 |
| run_20261106T100114_285c5a | transactions.csv | SUCCESS | 389 | 335 | 0 | 335 | 54 | 130 | 20 | 3 | 25 | 0.0122 |

## Rejects by reason

| reason_code | rows |
|---|---|
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

## Sample query: daily volume (first 7 days, minor units)

| date | txns | total_amount_minor |
|---|---|---|
| 2026-01-02 | 6 | 202000624711 |
| 2026-01-03 | 5 | 1523686 |
| 2026-01-04 | 6 | 100001204456 |
| 2026-01-05 | 7 | 101001182171 |
| 2026-01-06 | 3 | 100000252221 |
| 2026-01-07 | 3 | 200000290772 |
| 2026-01-08 | 4 | 519045 |

## Sample query: top 5 closing balances

| account_id | currency | closing_balance_minor |
|---|---|---|
| acc_00034 | INR | 201036701299 |
| acc_00031 | JPY | 201029338316 |
| acc_00001 | EUR | 200015643054 |
| acc_00035 | JPY | 101001033191 |
| acc_00003 | JPY | 100099829613 |
