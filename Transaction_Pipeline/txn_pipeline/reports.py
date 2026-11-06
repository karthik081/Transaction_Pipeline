"""Reconciliation checks, validation report and CSV export of curated outputs."""
import csv
import json
import os

CHECKS = [
    ("curated_txns_balanced",
     "SELECT COUNT(*) FROM (SELECT txn_id FROM fact_txn_leg GROUP BY txn_id, currency "
     "HAVING SUM(CASE WHEN direction='DEBIT' THEN amount_minor ELSE 0 END) != "
     "SUM(CASE WHEN direction='CREDIT' THEN amount_minor ELSE 0 END))"),
    ("total_debits_equal_total_credits",
     "SELECT ABS(COALESCE(SUM(CASE WHEN direction='DEBIT' THEN amount_minor ELSE -amount_minor END),0)) "
     "FROM fact_txn_leg"),
    ("net_movement_sums_to_zero", "SELECT COALESCE(SUM(signed_amount_minor),0) FROM fact_txn_leg"),
    ("no_orphan_accounts",
     "SELECT COUNT(*) FROM fact_txn_leg l LEFT JOIN dim_account a USING(account_id) WHERE a.account_id IS NULL"),
    ("no_currency_mismatch_vs_account",
     "SELECT COUNT(*) FROM fact_txn_leg l JOIN dim_account a USING(account_id) "
     "WHERE a.account_currency != l.currency"),
    ("fact_txn_leg_count_matches_legs",
     "SELECT COUNT(*) FROM fact_txn t WHERE t.leg_count != "
     "(SELECT COUNT(*) FROM fact_txn_leg l WHERE l.txn_id = t.txn_id)"),
    ("fact_txn_total_matches_debits",
     "SELECT COUNT(*) FROM fact_txn t WHERE t.total_amount_minor != "
     "(SELECT SUM(amount_minor) FROM fact_txn_leg l WHERE l.txn_id=t.txn_id AND l.direction='DEBIT')"),
    ("no_txn_without_legs",
     "SELECT COUNT(*) FROM fact_txn t WHERE NOT EXISTS (SELECT 1 FROM fact_txn_leg l WHERE l.txn_id=t.txn_id)"),
    ("run_counts_reconcile (read = accepted + rejected)",
     "SELECT COUNT(*) FROM pipeline_run WHERE status IN ('SUCCESS','EMPTY_INPUT') "
     "AND rows_read != rows_accepted + rows_rejected"),
    ("run_counts_reconcile (accepted = inserted + already_loaded)",
     "SELECT COUNT(*) FROM pipeline_run WHERE status IN ('SUCCESS','EMPTY_INPUT') "
     "AND rows_accepted != rows_inserted + rows_already_loaded"),
    ("rejects_traceable_to_run_and_source",
     "SELECT COUNT(*) FROM rejected_record r LEFT JOIN pipeline_run p USING(run_id) "
     "WHERE p.run_id IS NULL OR r.source_file IS NULL OR r.raw_record IS NULL"),
    ("curated_legs_equal_sum_of_inserted_rows",
     "SELECT ABS((SELECT COUNT(*) FROM fact_txn_leg) - "
     "COALESCE((SELECT SUM(rows_inserted) FROM pipeline_run WHERE status IN ('SUCCESS','EMPTY_INPUT')),0))"),
]


def reconcile(conn):
    """Each check returns a violation count; 0 = pass."""
    return [{"check": name, "violations": conn.execute(sql).fetchone()[0],
             "passed": conn.execute(sql).fetchone()[0] == 0} for name, sql in CHECKS]


def _table(rows, headers):
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def write_report(conn, reports_dir):
    os.makedirs(reports_dir, exist_ok=True)
    checks = reconcile(conn)
    runs = conn.execute(
        "SELECT run_id, source_file, status, rows_read, rows_accepted, rows_inserted, "
        "rows_already_loaded, rows_rejected, txns_accepted, txns_rejected, late_arriving_rows, "
        "out_of_order_rows, duration_seconds FROM pipeline_run ORDER BY started_at").fetchall()
    reasons = conn.execute("SELECT reason_code, COUNT(*) FROM rejected_record GROUP BY 1 ORDER BY 2 DESC").fetchall()
    daily = conn.execute("SELECT substr(first_event_ts,1,10) d, COUNT(*), SUM(total_amount_minor) "
                         "FROM fact_txn GROUP BY d ORDER BY d LIMIT 7").fetchall()
    bal = conn.execute("SELECT account_id, account_currency, closing_balance_minor FROM vw_account_balance "
                       "ORDER BY closing_balance_minor DESC LIMIT 5").fetchall()
    totals = conn.execute("SELECT (SELECT COUNT() FROM fact_txn), (SELECT COUNT() FROM fact_txn_leg), "
                          "(SELECT COUNT(*) FROM rejected_record)").fetchone()
    md = ["# Validation report", "",
          f"Curated: **{totals[0]} transactions, {totals[1]} legs**; quarantined rows: **{totals[2]}**.", "",
          "## Reconciliation checks (0 violations = pass)", "",
          _table([(c["check"], c["violations"], "PASS" if c["passed"] else "FAIL") for c in checks],
                 ["check", "violations", "result"]), "",
          "## Pipeline runs", "",
          _table(runs, ["run_id", "source", "status", "read", "accepted", "inserted", "already_loaded",
                        "rejected", "txns_ok", "txns_rejected", "late", "out_of_order", "secs"]), "",
          "## Rejects by reason", "", _table(reasons, ["reason_code", "rows"]), "",
          "## Sample query: daily volume (first 7 days, minor units)", "",
          _table(daily, ["date", "txns", "total_amount_minor"]), "",
          "## Sample query: top 5 closing balances", "", _table(bal, ["account_id", "currency", "closing_balance_minor"]), ""]
    with open(os.path.join(reports_dir, "validation_report.md"), "w") as f:
        f.write("\n".join(md))
    with open(os.path.join(reports_dir, "validation_report.json"), "w") as f:
        json.dump({"checks": checks, "reject_reasons": dict(reasons)}, f, indent=2)
 # Generate a detailed execution report for the latest pipeline run
    latest = conn.execute(
        "SELECT run_id, source_file, input_sha256, status, rows_read, rows_accepted, "
        "rows_inserted, rows_already_loaded, rows_rejected, "
        "txns_accepted, txns_rejected, duration_seconds "
        "FROM pipeline_run ORDER BY started_at DESC LIMIT 1"
    ).fetchone()

    if latest:
        (run_id, source_file, input_sha256, status, rows_read, rows_accepted,
         rows_inserted, already_loaded, rows_rejected,
         txns_accepted, txns_rejected, duration) = latest

        curated_txns = conn.execute(
            "SELECT COUNT(*) FROM fact_txn"
        ).fetchone()[0]

        curated_legs = conn.execute(
            "SELECT COUNT(*) FROM fact_txn_leg"
        ).fetchone()[0]

        # Reject rows are inserted only once because reject_id is deterministic.
        # On an exact rerun, rejects_inserted is therefore 0 and the original
        # rejected_record rows still point to the first run. Match by input hash
        # so the latest run report shows the decisions for this same input batch.
        reject_reasons = conn.execute(
            "SELECT r.reason_code, COUNT(*) FROM rejected_record r "
            "JOIN pipeline_run p ON p.run_id = r.run_id "
            "WHERE p.input_sha256 = ? "
            "GROUP BY r.reason_code ORDER BY COUNT(*) DESC",
            (input_sha256,)
        ).fetchall()

        reconciliation = rows_read == rows_accepted + rows_rejected
        load_reconciliation = rows_accepted == rows_inserted + already_loaded

        accepted_pct = (rows_accepted / rows_read * 100) if rows_read else 0
        rejected_pct = (rows_rejected / rows_read * 100) if rows_read else 0

        execution = [
            "# Transaction Pipeline - Execution Report",
            "",
            "## Execution Details",
            "",
            f"- Run ID: {run_id}",
            f"- Source file: {source_file}",
            f"- Status: {status}",
            f"- Execution time: {duration:.2f} seconds",
            "",
            "## Record Summary",
            "",
            f"- Total input records: {rows_read}",
            f"- Accepted records: {rows_accepted}",
            f"- Rejected records: {rows_rejected}",
            f"- Accepted percentage: {accepted_pct:.2f}%",
            f"- Rejected percentage: {rejected_pct:.2f}%",
            "",
            "## Curated Database Summary",
            "",
            f"- Total curated transactions in database: {curated_txns}",
            f"- Total curated transaction legs in database: {curated_legs}",
            f"- Newly inserted records in latest run: {rows_inserted}",
            f"- Already loaded records in latest run: {already_loaded}",
            "",
            "## Rejected Records by Reason",
            "",
            "Counts below are the persisted rejection decisions for this input file hash. "
            "Exact reruns reuse the original quarantine rows, so the latest run may insert zero new rejects.",
            "",
            "| Rejection reason | Record count |",
            "|---|---:|",
        ]

        if reject_reasons:
            for reason, count in reject_reasons:
                execution.append(f"| {reason} | {count} |")
        else:
            execution.append("| No rejected records | 0 |")

        execution += [
            "",
            "## Reconciliation Checks",
            "",
            "| Check | Result |",
            "|---|---|",
            f"| Input = Accepted + Rejected | {'PASS' if reconciliation else 'FAIL'} |",
            f"| Accepted = Inserted + Already Loaded | {'PASS' if load_reconciliation else 'FAIL'} |",
            "",
            "## Final Result",
            "",
            f"*{'PASS' if status in ('SUCCESS', 'EMPTY_INPUT') and reconciliation and load_reconciliation else 'FAIL'}*",
            "",
        ]

        with open(
            os.path.join(reports_dir, "execution_report.md"),
            "w",
            encoding="utf-8"
        ) as f:
            f.write("\n".join(execution))
    return checks


def export_csv(conn, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    for name, sql in [("fact_txn", "SELECT * FROM fact_txn ORDER BY txn_id"),
                      ("fact_txn_leg", "SELECT * FROM fact_txn_leg ORDER BY txn_id, leg_no"),
                      ("rejected_record", "SELECT * FROM rejected_record ORDER BY source_row_no"),
                      ("account_balance", "SELECT * FROM vw_account_balance ORDER BY account_id"),
                      ("pipeline_run", "SELECT * FROM pipeline_run ORDER BY started_at")]:
        cur = conn.execute(sql)
        with open(os.path.join(out_dir, f"{name}.csv"), "w", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow([d[0] for d in cur.description])
            w.writerows(cur)