import csv
import os
import sqlite3
import tempfile
import unittest

from txn_pipeline.config import Config
from txn_pipeline.generator import generate
from txn_pipeline.pipeline import run_pipeline
from txn_pipeline.reports import reconcile
from txn_pipeline.validators import REQUIRED_COLUMNS, RowError, parse_amount_minor

HEADER = ",".join(REQUIRED_COLUMNS)
ACCOUNTS = """account_id,account_currency,status,opening_balance_minor
acc_1,USD,ACTIVE,1000
acc_2,USD,ACTIVE,2000
acc_3,EUR,ACTIVE,3000
acc_4,USD,CLOSED,0
acc_j1,JPY,ACTIVE,0
acc_j2,JPY,ACTIVE,0
"""
T = "2026-01-01T10:00:00Z"
T1 = "2026-01-01T10:00:05Z"


def leg(txn="t1", n=1, acc="acc_1", d="DEBIT", amt="10.00", cur="USD", ev=T, ing=T1, typ="TRANSFER"):
    return f"{txn},{n},{acc},{d},{amt},{cur},{ev},{ing},{typ}"


def pair(txn="t1", amt="10.00", a="acc_1", b="acc_2", cur="USD", **kw):
    return [leg(txn, 1, a, "DEBIT", amt, cur, **kw), leg(txn, 2, b, "CREDIT", amt, cur, **kw)]


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = self.tmp.name
        self.cfg = Config(db_path=os.path.join(self.dir, "t.db"))
        with open(os.path.join(self.dir, "accounts.csv"), "w") as f:
            f.write(ACCOUNTS)

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, lines, header=HEADER, name="in.csv"):
        path = os.path.join(self.dir, name)
        with open(path, "w", newline="") as f:
            f.write("\n".join(([header] if header is not None else []) + lines) + "\n")
        return path

    def run_(self, lines=None, path=None, **kw):
        path = path or self.write(lines)
        return run_pipeline(path, os.path.join(self.dir, "accounts.csv"), self.cfg, **kw)

    def q(self, sql, *a):
        c = sqlite3.connect(self.cfg.db_path)
        try:
            return c.execute(sql, a).fetchall()
        finally:
            c.close()

    def reasons(self):
        return {r[0] for r in self.q("SELECT DISTINCT reason_code FROM rejected_record")}

    def assert_reconciles(self):
        conn = sqlite3.connect(self.cfg.db_path)
        bad = [c for c in reconcile(conn) if not c["passed"]]
        conn.close()
        self.assertEqual(bad, [])


class TestHappyAndEmpty(Base):
    def test_happy_path(self):
        m = self.run_(pair("t1") + pair("t2", "5.50", "acc_2", "acc_1"))
        self.assertEqual(m["status"], "SUCCESS")
        self.assertEqual((m["rows_read"], m["rows_accepted"], m["rows_inserted"], m["rows_rejected"]), (4, 4, 4, 0))
        self.assertEqual(self.q("SELECT COUNT(*) FROM fact_txn")[0][0], 2)
        # t1: acc_1 -10.00 ; t2: acc_1 +5.50 => closing 1000 - 1000 + 550
        self.assertEqual(self.q("SELECT closing_balance_minor FROM vw_account_balance WHERE account_id='acc_1'")[0][0], 550)
        self.assert_reconciles()

    def test_empty_and_header_only(self):
        zero_byte = os.path.join(self.dir, "zero.csv")
        open(zero_byte, "w").close()
        for path in (self.write([], header=HEADER), zero_byte):
            m = self.run_(path=path)
            self.assertEqual(m["status"], "EMPTY_INPUT")
            self.assertEqual(m["rows_read"], 0)
        self.assertEqual(self.q("SELECT COUNT(*) FROM fact_txn_leg")[0][0], 0)


class TestMalformedAndNulls(Base):
    def test_malformed_rows_quarantined_with_peer(self):
        lines = pair("t1") + ["t2,1,acc_1,DEBIT,10.00,USD," + T + "," + T1 + ",TRANSFER,EXTRA",  # extra field
                              "t2,2,acc_2"]                                                      # short row
        m = self.run_(lines)
        self.assertEqual(m["status"], "SUCCESS")
        self.assertEqual(m["rows_accepted"], 2)
        self.assertEqual(m["reject_breakdown"], {"MALFORMED_ROW": 2})

    def test_missing_required_field(self):
        m = self.run_([leg("t1", 1, acc=""), leg("t1", 2, "acc_2", "CREDIT")])
        self.assertEqual(m["reject_breakdown"], {"MISSING_REQUIRED_FIELD": 1, "TXN_REJECTED_PEER_LEG": 1})
        self.assertEqual(self.q("SELECT COUNT(*) FROM fact_txn_leg")[0][0], 0)

    def test_bad_values(self):
        cases = {"INVALID_AMOUNT": leg(amt="abc"), "INVALID_DIRECTION": leg(d="SIDEWAYS"),
                 "INVALID_TIMESTAMP": leg(ev="yesterday"), "INVALID_LEG_NO": leg(n="x"),
                 "UNSUPPORTED_CURRENCY": leg(cur="XYZ")}
        for code, line in cases.items():
            self.assertEqual(self.run_([line])["reject_breakdown"].get(code), 1, code)


class TestAccountMasterValidation(Base):
    def test_duplicate_account_id_fails_before_publish(self):
        with open(os.path.join(self.dir, "accounts.csv"), "a") as f:
            f.write("acc_1,USD,ACTIVE,999\n")
        m = self.run_(pair("t1"))
        self.assertEqual(m["status"], "FAILED")
        self.assertIn("duplicate account_id", m["error"])
        self.assertEqual(self.q("SELECT COUNT(*) FROM fact_txn_leg")[0][0], 0)

    def test_invalid_account_currency_or_status_fails(self):
        path = os.path.join(self.dir, "accounts.csv")
        with open(path, "w") as f:
            f.write(ACCOUNTS.replace("acc_1,USD,ACTIVE", "acc_1,XYZ,ACTIVE"))
        m = self.run_(pair("t1"))
        self.assertEqual(m["status"], "FAILED")
        self.assertIn("unsupported account currency", m["error"])
        self.assertEqual(self.q("SELECT COUNT(*) FROM fact_txn_leg")[0][0], 0)


class TestDuplicates(Base):
    def test_exact_duplicate_kept_once(self):
        m = self.run_(pair("t1") + [leg("t1", 1)])
        self.assertEqual((m["rows_accepted"], m["rows_rejected"]), (2, 1))
        self.assertEqual(m["reject_breakdown"], {"DUPLICATE_LEG": 1})
        self.assertEqual(m["txns_accepted"], 1)

    def test_conflicting_duplicate_rejects_whole_txn(self):
        m = self.run_(pair("t1") + [leg("t1", 1, amt="99.00")])
        self.assertEqual(m["rows_accepted"], 0)
        self.assertIn("DUPLICATE_CONFLICT", m["reject_breakdown"])

    def test_conflict_with_curated_data(self):
        self.run_(pair("t1"))
        m = self.run_(pair("t1", amt="11.00"))
        self.assertEqual(m["rows_inserted"], 0)
        self.assertIn("CONFLICTS_WITH_CURATED", m["reject_breakdown"])
        self.assertEqual(self.q("SELECT SUM(amount_minor) FROM fact_txn_leg")[0][0], 2000)  # unchanged

    def test_changed_curated_txn_rejects_every_incoming_leg(self):
        self.run_(pair("t1"))
        # A changed existing leg plus two individually balancing new legs must not
        # append to the already-published transaction.
        lines = [leg("t1", 1, "acc_1", "DEBIT", "11.00"),
                 leg("t1", 3, "acc_1", "DEBIT", "5.00"),
                 leg("t1", 4, "acc_2", "CREDIT", "5.00")]
        m = self.run_(lines)
        self.assertEqual(m["rows_inserted"], 0)
        self.assertEqual(self.q("SELECT COUNT(*) FROM fact_txn_leg WHERE txn_id='t1'")[0][0], 2)
        self.assertIn("CONFLICTS_WITH_CURATED", m["reject_breakdown"])
        self.assertIn("INCOMPLETE_OR_CHANGED_CURATED_TXN", m["reject_breakdown"])

    def test_partial_new_legs_cannot_extend_curated_transaction(self):
        self.run_(pair("t1"))
        m = self.run_([leg("t1", 3, "acc_1", "DEBIT", "5.00"),
                       leg("t1", 4, "acc_2", "CREDIT", "5.00")])
        self.assertEqual(m["rows_inserted"], 0)
        self.assertEqual(self.q("SELECT COUNT(*) FROM fact_txn_leg WHERE txn_id='t1'")[0][0], 2)
        self.assertEqual(m["reject_breakdown"], {"INCOMPLETE_OR_CHANGED_CURATED_TXN": 2})


class TestDomainRules(Base):
    def test_imbalance(self):
        m = self.run_([leg("t1", 1, "acc_1", "DEBIT", "10.00"), leg("t1", 2, "acc_2", "CREDIT", "9.99")])
        self.assertEqual(m["reject_breakdown"], {"TXN_IMBALANCE": 2})
        self.assertEqual(self.q("SELECT COUNT(*) FROM fact_txn")[0][0], 0)

    def test_split_payment_balances(self):
        m = self.run_([leg("t1", 1, "acc_1", "DEBIT", "10.00"), leg("t1", 2, "acc_2", "CREDIT", "9.90"),
                       leg("t1", 3, "acc_2", "CREDIT", "0.10")])
        self.assertEqual(m["rows_accepted"], 3)

    def test_single_leg_txn(self):
        self.assertEqual(self.run_([leg()])["reject_breakdown"], {"TXN_TOO_FEW_LEGS": 1})

    def test_invalid_reference_closed_and_currency_mismatch(self):
        m = self.run_(pair("t1", a="acc_404") + pair("t2", b="acc_4") + pair("t3", b="acc_3"))
        self.assertEqual({k for k in m["reject_breakdown"]},
                         {"UNKNOWN_ACCOUNT", "ACCOUNT_CLOSED", "CURRENCY_MISMATCH", "TXN_REJECTED_PEER_LEG"})
        self.assertEqual(m["rows_accepted"], 0)

    def test_event_ordering_ingested_before_event(self):
        m = self.run_(pair("t1", ev=T1, ing=T))
        self.assertEqual(m["reject_breakdown"], {"INGESTED_BEFORE_EVENT": 2})

    def test_leg_timestamp_skew(self):
        m = self.run_([leg("t1", 1, ev=T, ing=T1),
                       leg("t1", 2, "acc_2", "CREDIT", ev="2026-01-01T10:30:00Z", ing="2026-01-01T10:30:05Z")])
        self.assertEqual(m["reject_breakdown"], {"TXN_LEG_TS_SKEW": 2})

    def test_late_arriving_loaded_and_flagged_out_of_order_counted(self):
        late = pair("t_late", ev="2026-01-01T09:00:00Z", ing="2026-01-05T09:00:00Z")
        early = pair("t_early", ev="2025-12-31T09:00:00Z", ing="2025-12-31T09:00:05Z")  # after => out-of-order
        m = self.run_(late + early)
        self.assertEqual(m["rows_accepted"], 4)
        self.assertEqual(m["late_arriving_rows"], 2)
        self.assertGreaterEqual(m["out_of_order_rows"], 1)
        self.assertEqual(self.q("SELECT is_late_arriving FROM fact_txn WHERE txn_id='t_late'")[0][0], 1)

    def test_currency_precision_boundaries(self):
        self.assertEqual(parse_amount_minor("0.01", "USD", self.cfg.currency_exponents), 1)
        self.assertEqual(parse_amount_minor("1000", "JPY", self.cfg.currency_exponents), 1000)
        for bad, cur in [("0.001", "USD"), ("10.5", "JPY"), ("0", "USD"), ("-1", "USD"), ("NaN", "USD")]:
            with self.assertRaises(RowError):
                parse_amount_minor(bad, cur, self.cfg.currency_exponents)
        m = self.run_(pair("t1", "1000", "acc_j1", "acc_j2", "JPY") + pair("t2", "0.01"))
        self.assertEqual(m["rows_accepted"], 4)


class TestSchemaAndRecovery(Base):
    def test_missing_column_fails_run_and_loads_nothing(self):
        path = self.write(["t1,1,acc_1,DEBIT,10.00,USD," + T + "," + T1], header=",".join(REQUIRED_COLUMNS[:-1]))
        m = self.run_(path=path)
        self.assertEqual(m["status"], "FAILED")
        self.assertIn("SchemaError", m["error"])
        self.assertEqual(self.q("SELECT status FROM pipeline_run")[0][0], "FAILED")
        self.assertEqual(self.q("SELECT COUNT(*) FROM fact_txn_leg")[0][0], 0)

    def test_extra_column_is_warning_not_failure(self):
        lines = [l + ",x" for l in pair("t1")]
        m = self.run_(path=self.write(lines, header=HEADER + ",extra_col"))
        self.assertEqual(m["status"], "SUCCESS")
        self.assertEqual(m["rows_accepted"], 2)
        self.assertTrue(m["warnings"])

    def test_column_order_change_is_tolerated(self):
        cols = list(reversed(REQUIRED_COLUMNS))
        lines = [",".join(dict(zip(REQUIRED_COLUMNS, l.split(",")))[c] for c in cols) for l in pair("t1")]
        self.assertEqual(self.run_(path=self.write(lines, header=",".join(cols)))["rows_accepted"], 2)

    def test_missing_input_file_is_recorded_as_failed(self):
        m = run_pipeline(os.path.join(self.dir, "nope.csv"), os.path.join(self.dir, "accounts.csv"), self.cfg)
        self.assertEqual(m["status"], "FAILED")

    def test_rerun_same_input_is_idempotent(self):
        lines = pair("t1") + pair("t2", amt="1.00", b="acc_4") + [leg("t3", 1, amt="abc")]
        path = self.write(lines)
        first, second = self.run_(path=path), self.run_(path=path)
        self.assertEqual((first["rows_inserted"], first["rejects_inserted"]), (2, 3))
        self.assertEqual((second["rows_inserted"], second["rows_already_loaded"], second["rejects_inserted"]), (0, 2, 0))
        self.assertEqual(self.q("SELECT COUNT(*) FROM fact_txn_leg")[0][0], 2)
        self.assertEqual(self.q("SELECT COUNT(*) FROM rejected_record")[0][0], 3)
        self.assertEqual(self.q("SELECT COUNT(*) FROM pipeline_run")[0][0], 2)
        self.assert_reconciles()

    def test_partial_failure_rolls_back_and_retry_succeeds(self):
        path = self.write(pair("t1") + pair("t2", "2.00") + [leg("t3", 1, amt="abc")])

        def boom():
            raise RuntimeError("disk full")
        m = self.run_(path=path, _fail_hook=boom)
        self.assertEqual(m["status"], "FAILED")
        for table in ("fact_txn", "fact_txn_leg", "rejected_record", "dim_account"):
            self.assertEqual(self.q(f"SELECT COUNT(*) FROM {table}")[0][0], 0, table)
        self.assertEqual(self.q("SELECT status, error FROM pipeline_run")[0][0], "FAILED")
        retry = self.run_(path=path)                       # retry with same input
        self.assertEqual((retry["status"], retry["rows_inserted"]), ("SUCCESS", 4))
        self.assert_reconciles()

    def test_backfill_older_file_after_newer(self):
        self.run_(pair("t_new", ev="2026-02-01T00:00:00Z", ing="2026-02-01T00:00:05Z"), )
        m = self.run_(pair("t_old", ev="2026-01-01T00:00:00Z", ing="2026-01-01T00:00:05Z"),
                      path=self.write(pair("t_old", ev="2026-01-01T00:00:00Z", ing="2026-01-01T00:00:05Z"), name="old.csv"))
        self.assertEqual(m["rows_inserted"], 2)
        self.assertEqual(self.q("SELECT COUNT(*) FROM fact_txn")[0][0], 2)

    def test_account_change_keeps_history(self):
        self.run_(pair("t1"))
        with open(os.path.join(self.dir, "accounts.csv"), "w") as f:
            f.write(ACCOUNTS.replace("acc_2,USD,ACTIVE", "acc_2,USD,CLOSED"))
        self.run_(pair("t2"))
        self.assertEqual(self.q("SELECT status FROM dim_account_history WHERE account_id='acc_2'")[0][0], "ACTIVE")
        self.assertEqual(self.q("SELECT status FROM dim_account WHERE account_id='acc_2'")[0][0], "CLOSED")


class TestGeneratorAgainstLabels(Base):
    def test_deterministic_generation(self):
        a, b = os.path.join(self.dir, "a"), os.path.join(self.dir, "b")
        generate(300, 1, out_dir=a)
        generate(300, 1, out_dir=b)
        for f in ("transactions.csv", "accounts.csv", "expected_labels.csv"):
            with open(os.path.join(a, f)) as fa, open(os.path.join(b, f)) as fb:
                self.assertEqual(fa.read(), fb.read())

    def test_pipeline_matches_expected_labels(self):
        d = os.path.join(self.dir, "gen")
        generate(3000, 42, anomaly_rate=0.2, out_dir=d)
        m = run_pipeline(os.path.join(d, "transactions.csv"), os.path.join(d, "accounts.csv"), self.cfg)
        self.assertEqual(m["status"], "SUCCESS")
        self.assertEqual(m["rows_read"], m["rows_accepted"] + m["rows_rejected"])
        accepted = {r[0] for r in self.q("SELECT txn_id FROM fact_txn")}
        late = {r[0] for r in self.q("SELECT txn_id FROM fact_txn WHERE is_late_arriving=1")}
        reasons = {}
        for t, c in self.q("SELECT txn_id, reason_code FROM rejected_record"):
            reasons.setdefault(t, set()).add(c)
        seen_kinds = set()
        with open(os.path.join(d, "expected_labels.csv")) as f:
            for row in csv.DictReader(f):
                t, st, why, kind = row["txn_id"], row["expected_status"], row["expected_reason"], row["anomaly_kind"]
                seen_kinds.add(kind)
                if st == "REJECT":
                    self.assertNotIn(t, accepted, (t, kind))
                    self.assertIn(why, reasons.get(t, set()), (t, kind))
                else:
                    self.assertIn(t, accepted, (t, kind))
                    self.assertEqual(t in late, st == "ACCEPT_LATE", (t, kind))
                    if st == "ACCEPT_DUP_ROW":
                        self.assertIn("DUPLICATE_LEG", reasons.get(t, set()))
        self.assertGreaterEqual(len(seen_kinds), 14)  # 13 anomalies + clean
        self.assert_reconciles()

    def test_batch_and_naive_load_give_same_result(self):
        d = os.path.join(self.dir, "gen")
        generate(500, 3, out_dir=d)
        out = []
        for batch in (True, False):
            cfg = Config(db_path=os.path.join(self.dir, f"x{batch}.db"), batch_insert=batch)
            m = run_pipeline(os.path.join(d, "transactions.csv"), os.path.join(d, "accounts.csv"), cfg)
            out.append((m["rows_accepted"], m["rows_rejected"], m["rows_inserted"]))
        self.assertEqual(out[0], out[1])


if __name__ == "__main__":
    unittest.main()
