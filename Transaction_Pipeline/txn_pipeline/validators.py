"""Pure validation functions: no I/O, easy to unit test."""
import hashlib
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

REQUIRED_COLUMNS = ["txn_id", "leg_no", "account_id", "direction", "amount",
                    "currency", "event_ts", "ingested_at", "txn_type"]
DIRECTIONS = {"DEBIT", "CREDIT"}


class RowError(Exception):
    def __init__(self, code, detail=""):
        super().__init__(f"{code}: {detail}")
        self.code, self.detail = code, detail


def iso_z(dt):
    return dt.isoformat().replace("+00:00", "Z")


def parse_ts(value, field):
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise RowError("INVALID_TIMESTAMP", f"{field}={value!r}")
    if dt.tzinfo is None:
        raise RowError("INVALID_TIMESTAMP", f"{field} has no timezone: {value!r}")
    return dt.astimezone(timezone.utc)


def parse_amount_minor(value, currency, exponents):
    """Decimal string -> integer minor units. Rejects <=0, NaN, too many decimals."""
    try:
        d = Decimal(value)
    except InvalidOperation:
        raise RowError("INVALID_AMOUNT", f"not numeric: {value!r}")
    if not d.is_finite() or d <= 0:
        raise RowError("INVALID_AMOUNT", f"must be a positive number: {value!r}")
    scaled = d.scaleb(exponents[currency])
    if scaled != scaled.to_integral_value():
        raise RowError("INVALID_AMOUNT", f"too many decimals for {currency}: {value!r}")
    return int(scaled)


def make_leg_id(txn_id, leg_no):
    return hashlib.sha1(f"{txn_id}|{leg_no}".encode()).hexdigest()[:16]


def validate_row(row, accounts, cfg):
    """Return a typed leg dict or raise RowError (one reason per row, first failing check)."""
    missing = [c for c in REQUIRED_COLUMNS if not (row.get(c) or "").strip()]
    if missing:
        raise RowError("MISSING_REQUIRED_FIELD", ",".join(missing))
    v = {c: row[c].strip() for c in REQUIRED_COLUMNS}

    direction = v["direction"].upper()
    if direction not in DIRECTIONS:
        raise RowError("INVALID_DIRECTION", v["direction"])
    try:
        leg_no = int(v["leg_no"])
    except ValueError:
        raise RowError("INVALID_LEG_NO", v["leg_no"])
    if leg_no <= 0:
        raise RowError("INVALID_LEG_NO", v["leg_no"])
    currency = v["currency"].upper()
    if currency not in cfg.currency_exponents:
        raise RowError("UNSUPPORTED_CURRENCY", currency)

    # reference checks before value checks
    acct = accounts.get(v["account_id"])
    if acct is None:
        raise RowError("UNKNOWN_ACCOUNT", v["account_id"])
    if acct["status"] != "ACTIVE":
        raise RowError("ACCOUNT_CLOSED", v["account_id"])
    if acct["account_currency"] != currency:
        raise RowError("CURRENCY_MISMATCH",
                       f"leg={currency} account={acct['account_currency']}")

    amount_minor = parse_amount_minor(v["amount"], currency, cfg.currency_exponents)
    event_dt = parse_ts(v["event_ts"], "event_ts")
    ingested_dt = parse_ts(v["ingested_at"], "ingested_at")
    if ingested_dt < event_dt:
        raise RowError("INGESTED_BEFORE_EVENT", f"{v['ingested_at']} < {v['event_ts']}")

    event_iso = iso_z(event_dt)
    content = "|".join([v["account_id"], direction, str(amount_minor), currency,
                        event_iso, v["txn_type"]])  # ingested_at excluded: replays differ
    return {
        "leg_id": make_leg_id(v["txn_id"], leg_no),
        "txn_id": v["txn_id"], "leg_no": leg_no, "account_id": v["account_id"],
        "direction": direction, "amount_minor": amount_minor,
        "signed_amount_minor": amount_minor if direction == "CREDIT" else -amount_minor,
        "currency": currency, "event_ts": event_iso, "ingested_at": iso_z(ingested_dt),
        "txn_type": v["txn_type"],
        "is_late_arriving": int((ingested_dt - event_dt).total_seconds()
                                > cfg.late_threshold_hours * 3600),
        "content_hash": hashlib.sha1(content.encode()).hexdigest(),
        "_event_dt": event_dt,
    }


def check_txn(legs, cfg):
    """Transaction-level rules. Returns (code, detail) or None."""
    if len(legs) < 2:
        return "TXN_TOO_FEW_LEGS", f"{len(legs)} leg(s)"
    currencies = {l["currency"] for l in legs}
    if len(currencies) > 1:
        return "TXN_MIXED_CURRENCY", ",".join(sorted(currencies))
    debits = sum(l["amount_minor"] for l in legs if l["direction"] == "DEBIT")
    credits = sum(l["amount_minor"] for l in legs if l["direction"] == "CREDIT")
    if debits != credits:
        return "TXN_IMBALANCE", f"debits={debits} credits={credits}"
    times = [l["_event_dt"] for l in legs]
    skew = (max(times) - min(times)).total_seconds()
    if skew > cfg.max_leg_skew_seconds:
        return "TXN_LEG_TS_SKEW", f"{skew:.0f}s > {cfg.max_leg_skew_seconds}s"
    return None
