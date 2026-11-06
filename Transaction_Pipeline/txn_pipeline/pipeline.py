"""Ingest -> validate -> transform -> publish. One call to run_pipeline = one run."""
import csv
import hashlib
import json
import os
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone

from .config import Config
from .db import connect
from .validators import REQUIRED_COLUMNS, RowError, check_txn, validate_row

ACCOUNT_COLUMNS = ["account_id", "account_currency", "status", "opening_balance_minor"]


class SchemaError(Exception):
    """Input header does not contain the required columns -> whole run fails."""


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_accounts(path, cfg=None):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing = set(ACCOUNT_COLUMNS) - set(reader.fieldnames or [])
        if missing:
            raise SchemaError(f"accounts file missing columns: {sorted(missing)}")
        accounts = {}
        cfg = cfg or Config()
        for row_no, r in enumerate(reader, start=2):
            try:
                account_id = (r["account_id"] or "").strip()
                currency = (r["account_currency"] or "").strip().upper()
                status = (r["status"] or "").strip().upper()
                opening_balance = int(r["opening_balance_minor"])
                if not account_id:
                    raise ValueError("account_id is required")
                if account_id in accounts:
                    raise ValueError(f"duplicate account_id {account_id!r}")
                if currency not in cfg.currency_exponents:
                    raise ValueError(f"unsupported account currency {currency!r}")
                if status not in {"ACTIVE", "CLOSED"}:
                    raise ValueError(f"invalid account status {status!r}")
                accounts[account_id] = {
                    "account_currency": currency,
                    "status": status,
                    "opening_balance_minor": opening_balance,
                }
            except (ValueError, TypeError, AttributeError, KeyError) as e:
                raise SchemaError(f"bad accounts row {row_no}: {e}")
    return accounts


def read_input(path):
    """Return (records, extra_columns). Each record: row_no, row (dict) or error + raw."""
    records, extra = [], []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:                           # 0-byte / blank file => empty batch
            return records, extra
        header = [h.strip() for h in header]
        missing = [c for c in REQUIRED_COLUMNS if c not in header]
        if missing:
            raise SchemaError(f"input missing required columns: {missing}")
        extra = [h for h in header if h not in REQUIRED_COLUMNS]
        n = 0
        while True:
            try:
                row = next(reader)
            except StopIteration:
                break
            except csv.Error as e:
                n += 1
                records.append({"row_no": n, "error": f"csv parse error: {e}", "raw": []})
                continue
            if not row:                          # blank line: ignored, not counted
                continue
            n += 1
            if len(row) != len(header):
                records.append({"row_no": n, "raw": row,
                                "error": f"expected {len(header)} fields, got {len(row)}"})
            else:
                records.append({"row_no": n, "row": dict(zip(header, row))})
    return records, extra


def run_pipeline(input_path, accounts_path, cfg=None, run_id=None, _fail_hook=None):
    cfg = cfg or Config()
    t0 = time.perf_counter()
    started = now_iso()
    run_id = run_id or f"run_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"
    source_file = os.path.basename(input_path)
    conn = connect(cfg.db_path)
    conn.execute("INSERT INTO pipeline_run(run_id, source_file, started_at, status) "
                 "VALUES (?,?,?,'RUNNING')", (run_id, source_file, started))
    m = dict(run_id=run_id, source_file=source_file, input_sha256=None, status="FAILED",
             rows_read=0, rows_accepted=0, rows_inserted=0, rows_already_loaded=0,
             rows_rejected=0, rejects_inserted=0, txns_accepted=0, txns_rejected=0,
             late_arriving_rows=0, out_of_order_rows=0, warnings=[], error=None,
             reject_breakdown={}, validate_seconds=0.0, load_seconds=0.0)
    try:
        m["input_sha256"] = sha256_file(input_path)
        accounts = read_accounts(accounts_path, cfg)
        records, extra_cols = read_input(input_path)
        if extra_cols:
            m["warnings"].append(f"ignored unexpected columns: {extra_cols}")
        m["rows_read"] = len(records)

        # ---- row-level validation ------------------------------------------------
        tv = time.perf_counter()
        rejects, poisoned = [], {}               # poisoned: txn_id -> first row-level reason

        def reject(rec, code, detail, txn_id=None, leg_no=None, poison=True):
            raw = rec.get("row") or rec.get("raw") or {}
            if txn_id is None:
                txn_id = (raw.get("txn_id") if isinstance(raw, dict)
                          else (raw[0] if raw else None)) or None
            rejects.append({"row_no": rec["row_no"], "txn_id": txn_id, "leg_no": leg_no,
                            "code": code, "detail": detail, "raw": raw})
            if poison and txn_id:
                poisoned.setdefault(txn_id, f"{code} (row {rec['row_no']})")

        legs, prev_dt = [], None
        for rec in records:
            if "error" in rec:
                reject(rec, "MALFORMED_ROW", rec["error"])
                continue
            try:
                leg = validate_row(rec["row"], accounts, cfg)
            except RowError as e:
                reject(rec, e.code, e.detail, leg_no=rec["row"].get("leg_no"))
                continue
            if prev_dt is not None and leg["_event_dt"] < prev_dt:   # earlier than previous row
                m["out_of_order_rows"] += 1
            prev_dt = leg["_event_dt"]
            leg["source_row_no"], leg["_rec"] = rec["row_no"], rec
            legs.append(leg)

        # ---- duplicates inside the batch ------------------------------------------
        groups = defaultdict(list)
        for leg in legs:
            groups[leg["leg_id"]].append(leg)
        unique = []
        for group in groups.values():
            group.sort(key=lambda g: g["source_row_no"])
            if len({g["content_hash"] for g in group}) > 1:      # same key, different data
                for g in group:
                    reject(g["_rec"], "DUPLICATE_CONFLICT",
                           f"{len(group)} rows share txn_id+leg_no with different content",
                           g["txn_id"], g["leg_no"])
                continue
            unique.append(group[0])
            for g in group[1:]:                                  # exact replay: keep first
                reject(g["_rec"], "DUPLICATE_LEG", f"exact duplicate of row {group[0]['source_row_no']}",
                       g["txn_id"], g["leg_no"], poison=False)

        # ---- compare whole transactions with already-curated data ------------------
        # A transaction is immutable and atomic after publication. A replay is valid
        # only when its complete leg set and content hashes match the curated version.
        existing_rows = conn.execute(
            "SELECT txn_id, leg_id, content_hash FROM fact_txn_leg"
        ).fetchall()
        existing_by_txn = defaultdict(dict)
        existing = {}
        for txn_id, leg_id, content_hash in existing_rows:
            existing_by_txn[txn_id][leg_id] = content_hash
            existing[leg_id] = content_hash

        unique_by_txn = defaultdict(list)
        for leg in unique:
            unique_by_txn[leg["txn_id"]].append(leg)

        survivors = []
        for txn_id, txn_legs in unique_by_txn.items():
            curated_legs = existing_by_txn.get(txn_id)
            if curated_legs is None:
                survivors.extend(txn_legs)
                continue
            incoming = {leg["leg_id"]: leg["content_hash"] for leg in txn_legs}
            if incoming == curated_legs and txn_id not in poisoned:
                # Exact whole-transaction replay: count as already loaded.
                survivors.extend(txn_legs)
                continue
            for leg in txn_legs:
                if (leg["leg_id"] in curated_legs and
                        curated_legs[leg["leg_id"]] != leg["content_hash"]):
                    code = "CONFLICTS_WITH_CURATED"
                    detail = "transaction contains a leg whose content differs from curated data"
                else:
                    code = "INCOMPLETE_OR_CHANGED_CURATED_TXN"
                    detail = "incoming leg set does not exactly match the already-curated transaction"
                reject(leg["_rec"], code, detail, txn_id, leg["leg_no"], poison=False)

        # ---- transaction-level validation -----------------------------------------
        by_txn = defaultdict(list)
        for leg in survivors:
            by_txn[leg["txn_id"]].append(leg)
        accepted_txns = {}
        for txn_id, tl in by_txn.items():
            if txn_id in poisoned:
                code, detail = "TXN_REJECTED_PEER_LEG", f"another leg failed: {poisoned[txn_id]}"
            else:
                err = check_txn(tl, cfg)
                code, detail = err if err else (None, None)
            if code:
                for l in tl:
                    reject(l["_rec"], code, detail, l["txn_id"], l["leg_no"], poison=False)
            else:
                accepted_txns[txn_id] = tl
        # txns whose every row failed at row level never reach by_txn
        all_txn_ids = {r["txn_id"] for r in rejects if r["txn_id"]} | set(by_txn)
        m["txns_accepted"] = len(accepted_txns)
        m["txns_rejected"] = len(all_txn_ids - set(accepted_txns))

        accepted_legs = sorted((l for tl in accepted_txns.values() for l in tl),
                               key=lambda l: (l["event_ts"], l["txn_id"], l["leg_no"]))
        new_legs = [l for l in accepted_legs if l["leg_id"] not in existing]
        m["rows_accepted"] = len(accepted_legs)
        m["rows_inserted_planned"] = len(new_legs)
        m["rows_already_loaded"] = len(accepted_legs) - len(new_legs)
        m["rows_rejected"] = len(rejects)
        m["late_arriving_rows"] = sum(l["is_late_arriving"] for l in accepted_legs)
        m["reject_breakdown"] = dict(Counter(r["code"] for r in rejects))
        rejects.sort(key=lambda r: (r["row_no"], r["code"]))
        m["validate_seconds"] = time.perf_counter() - tv

        # ---- atomic publish ------------------------------------------------------------
        tl_ = time.perf_counter()
        txn_rows = []
        for txn_id, tl in accepted_txns.items():
            if all(l["leg_id"] in existing for l in tl):
                continue
            ts = sorted(l["event_ts"] for l in tl)
            txn_rows.append((txn_id, tl[0]["txn_type"], tl[0]["currency"], len(tl),
                             sum(l["amount_minor"] for l in tl if l["direction"] == "DEBIT"),
                             ts[0], ts[-1], max(l["is_late_arriving"] for l in tl),
                             source_file, run_id))
        leg_rows = [(l["leg_id"], l["txn_id"], l["leg_no"], l["account_id"], l["direction"],
                     l["amount_minor"], l["signed_amount_minor"], l["currency"], l["event_ts"],
                     l["ingested_at"], l["txn_type"], l["is_late_arriving"], l["content_hash"],
                     source_file, l["source_row_no"], run_id) for l in new_legs]
        ts_now = now_iso()
        reject_rows = [(hashlib.sha1(f"{m['input_sha256']}|{r['row_no']}|{r['code']}".encode())
                        .hexdigest()[:16], run_id, source_file, r["row_no"], r["txn_id"],
                        r["leg_no"], r["code"], r["detail"],
                        json.dumps(r["raw"], ensure_ascii=False), ts_now) for r in rejects]

        conn.execute("BEGIN")
        try:
            _upsert_accounts(conn, accounts, run_id)
            conn.executemany("INSERT OR IGNORE INTO fact_txn VALUES (?,?,?,?,?,?,?,?,?,?)", txn_rows)
            sql = "INSERT OR IGNORE INTO fact_txn_leg VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            if cfg.batch_insert:
                cur = conn.executemany(sql, leg_rows)
                m["rows_inserted"] = max(cur.rowcount, 0)
            else:
                m["rows_inserted"] = sum(conn.execute(sql, r).rowcount for r in leg_rows)
            if _fail_hook:
                _fail_hook()                      # test seam: simulate crash mid-load
            cur = conn.executemany("INSERT OR IGNORE INTO rejected_record VALUES (?,?,?,?,?,?,?,?,?,?)",
                                   reject_rows)
            m["rejects_inserted"] = cur.rowcount if reject_rows else 0
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        m["load_seconds"] = time.perf_counter() - tl_
        m["status"] = "SUCCESS" if m["rows_read"] else "EMPTY_INPUT"
    except Exception as e:                        # run failure is data, not a crash
        m["status"], m["error"] = "FAILED", f"{type(e).__name__}: {e}"
        m["rows_inserted"] = m["rejects_inserted"] = 0     # load was rolled back
    finally:
        m["duration_seconds"] = round(time.perf_counter() - t0, 4)
        conn.execute(
            """UPDATE pipeline_run SET input_sha256=?, ended_at=?, status=?, rows_read=?,
               rows_accepted=?, rows_inserted=?, rows_already_loaded=?, rows_rejected=?,
               rejects_inserted=?, txns_accepted=?, txns_rejected=?, late_arriving_rows=?,
               out_of_order_rows=?, duration_seconds=?, warnings=?, error=? WHERE run_id=?""",
            (m["input_sha256"], now_iso(), m["status"], m["rows_read"], m["rows_accepted"],
             m["rows_inserted"], m["rows_already_loaded"], m["rows_rejected"],
             m["rejects_inserted"], m["txns_accepted"], m["txns_rejected"],
             m["late_arriving_rows"], m["out_of_order_rows"], m["duration_seconds"],
             json.dumps(m["warnings"]), m["error"], run_id))
        conn.close()
    return m


def _upsert_accounts(conn, accounts, run_id):
    """Upsert dimension; if attributes changed, keep the old version in dim_account_history."""
    current = {r[0]: r[1:] for r in conn.execute(
        "SELECT account_id, account_currency, status, opening_balance_minor FROM dim_account")}
    for acc_id, a in accounts.items():
        new = (a["account_currency"], a["status"], a["opening_balance_minor"])
        old = current.get(acc_id)
        if old == new:
            continue
        if old is not None:
            conn.execute("INSERT INTO dim_account_history(account_id, account_currency, status, "
                         "opening_balance_minor, superseded_by_run_id) VALUES (?,?,?,?,?)",
                         (acc_id, *old, run_id))
        conn.execute("INSERT OR REPLACE INTO dim_account VALUES (?,?,?,?,?)", (acc_id, *new, run_id))
