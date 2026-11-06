"""Deterministic synthetic data generator. Labels are written to a SEPARATE file."""
import csv
import os
import random
from datetime import datetime, timedelta, timezone

from .validators import REQUIRED_COLUMNS, iso_z

CURRENCIES = ["USD", "EUR", "INR", "JPY"]
EXP = {"USD": 2, "EUR": 2, "INR": 2, "JPY": 0}
BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
ANOMALIES = ["IMBALANCE", "UNKNOWN_ACCOUNT", "CURRENCY_MISMATCH", "NEGATIVE_AMOUNT",
             "NON_NUMERIC_AMOUNT", "MISSING_FIELD", "DUPLICATE_ROW", "CONFLICTING_DUPLICATE",
             "INGEST_BEFORE_EVENT", "LEG_TS_SKEW", "CLOSED_ACCOUNT", "LATE_ARRIVAL",
             "MALFORMED_ROW"]
# anomaly -> (expected_status, expected_reason)
EXPECTED = {
    "IMBALANCE": ("REJECT", "TXN_IMBALANCE"), "UNKNOWN_ACCOUNT": ("REJECT", "UNKNOWN_ACCOUNT"),
    "CURRENCY_MISMATCH": ("REJECT", "CURRENCY_MISMATCH"),
    "NEGATIVE_AMOUNT": ("REJECT", "INVALID_AMOUNT"), "NON_NUMERIC_AMOUNT": ("REJECT", "INVALID_AMOUNT"),
    "MISSING_FIELD": ("REJECT", "MISSING_REQUIRED_FIELD"),
    "DUPLICATE_ROW": ("ACCEPT_DUP_ROW", "DUPLICATE_LEG"),
    "CONFLICTING_DUPLICATE": ("REJECT", "DUPLICATE_CONFLICT"),
    "INGEST_BEFORE_EVENT": ("REJECT", "INGESTED_BEFORE_EVENT"),
    "LEG_TS_SKEW": ("REJECT", "TXN_LEG_TS_SKEW"), "CLOSED_ACCOUNT": ("REJECT", "ACCOUNT_CLOSED"),
    "LATE_ARRIVAL": ("ACCEPT_LATE", ""), "MALFORMED_ROW": ("REJECT", "MALFORMED_ROW"),
    "NONE": ("ACCEPT", ""),
}


def fmt_amount(minor, cur):
    return str(minor) if EXP[cur] == 0 else f"{minor // 100}.{minor % 100:02d}"


def generate(n_txns, seed=42, anomaly_rate=0.08, out_dir="data/sample"):
    rnd = random.Random(seed)
    os.makedirs(out_dir, exist_ok=True)
    n_acc = max(40, n_txns // 50)
    accounts, active, closed = [], {c: [] for c in CURRENCIES}, {c: [] for c in CURRENCIES}
    for i in range(n_acc):
        cur, is_closed = CURRENCIES[i % 4], (i % 40) >= 36
        acc_id = f"acc_{i:05d}"
        accounts.append([acc_id, cur, "CLOSED" if is_closed else "ACTIVE", rnd.randint(0, 100_000_000)])
        (closed if is_closed else active)[cur].append(acc_id)

    keyed, labels = [], []
    for i in range(n_txns):
        cur = rnd.choice(CURRENCIES)
        a, b, c = rnd.sample(active[cur], 3)
        amt = rnd.choice([1, rnd.randint(1, 500_000), rnd.randint(1, 500_000), 10**11])  # boundaries
        fee = max(1, amt // 100)
        txn_id = f"txn_{i:08d}"
        txn_type = rnd.choice(["TRANSFER", "PAYMENT_WITH_FEE"])
        spec = [("DEBIT", a, amt), ("CREDIT", b, amt)] if txn_type == "TRANSFER" else \
               [("DEBIT", a, amt + fee), ("CREDIT", b, amt), ("CREDIT", c, fee)]
        ev = BASE + timedelta(seconds=rnd.randint(0, 30 * 86400))
        legs = []
        for n, (d, acc, minor) in enumerate(spec, 1):
            lev = ev + timedelta(seconds=rnd.randint(0, 5))
            legs.append({"txn_id": txn_id, "leg_no": str(n), "account_id": acc, "direction": d,
                         "_minor": minor, "currency": cur, "_ev": lev, "txn_type": txn_type})
        kind = rnd.choice(ANOMALIES) if rnd.random() < anomaly_rate else "NONE"
        if n_txns >= 50 and i < len(ANOMALIES):      # guarantee every anomaly type appears once
            kind = ANOMALIES[i]
        extra, malformed = [], None
        if kind == "IMBALANCE":
            legs[1]["_minor"] += 1
        elif kind == "UNKNOWN_ACCOUNT":
            legs[0]["account_id"] = "acc_99999"
        elif kind == "CURRENCY_MISMATCH":
            legs[1]["currency"] = rnd.choice([x for x in CURRENCIES if x != cur])
        elif kind == "NEGATIVE_AMOUNT":
            legs[0]["_neg"] = True
        elif kind == "NON_NUMERIC_AMOUNT":
            legs[0]["_bad_amount"] = "12,5O"
        elif kind == "MISSING_FIELD":
            legs[0]["account_id"] = ""
        elif kind == "DUPLICATE_ROW":
            extra.append(dict(legs[0]))
        elif kind == "CONFLICTING_DUPLICATE":
            d = dict(legs[0]); d["_minor"] += 1; extra.append(d)
        elif kind == "INGEST_BEFORE_EVENT":
            legs[0]["_ing"] = legs[0]["_ev"] - timedelta(seconds=10)
        elif kind == "LEG_TS_SKEW":
            legs[1]["_ev"] += timedelta(minutes=10)
        elif kind == "CLOSED_ACCOUNT":
            legs[0]["account_id"] = rnd.choice(closed[cur])
        elif kind == "LATE_ARRIVAL":
            for l in legs:
                l["_ing"] = l["_ev"] + timedelta(days=2)
        elif kind == "MALFORMED_ROW":
            malformed = rnd.choice(["extra", "short"])
        for idx, l in enumerate(legs + extra):
            amount = fmt_amount(l["_minor"], l["currency"] if l["currency"] in EXP else cur)
            if l.get("_neg"):
                amount = "-" + amount
            amount = l.get("_bad_amount", amount)
            ing = l.get("_ing", l["_ev"] + timedelta(seconds=rnd.randint(1, 300)))
            row = [l["txn_id"], l["leg_no"], l["account_id"], l["direction"], amount,
                   l["currency"], iso_z(l["_ev"]), iso_z(ing), l["txn_type"]]
            if malformed and idx == 0:
                row = row + ["unexpected"] if malformed == "extra" else row[:-1]
            keyed.append((iso_z(l["_ev"]), row))
        status, reason = EXPECTED[kind]
        labels.append([txn_id, status, reason, kind])

    keyed.sort(key=lambda x: x[0])                       # then perturb => out-of-order data
    for i in range(len(keyed)):
        if rnd.random() < 0.05:
            j = min(len(keyed) - 1, i + rnd.randint(1, 50))
            keyed[i], keyed[j] = keyed[j], keyed[i]

    def write(name, header, rows):
        with open(os.path.join(out_dir, name), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(header)
            w.writerows(rows)

    write("accounts.csv", ["account_id", "account_currency", "status", "opening_balance_minor"], accounts)
    write("transactions.csv", REQUIRED_COLUMNS, [r for _, r in keyed])
    write("expected_labels.csv", ["txn_id", "expected_status", "expected_reason", "anomaly_kind"], labels)
    return {"accounts": len(accounts), "txns": n_txns, "rows": len(keyed), "out_dir": out_dir}
