from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    db_path: str = "output/curated.db"
    reports_dir: str = "output/reports"
    max_leg_skew_seconds: int = 60       # legs of one txn must occur within this window
    late_threshold_hours: int = 24       # ingested_at - event_ts above this => flagged late
    currency_exponents: dict = field(
        default_factory=lambda: {"USD": 2, "EUR": 2, "INR": 2, "JPY": 0}
    )
    batch_insert: bool = True            # False = naive row-by-row insert (benchmark baseline)
