import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from dataclasses import replace

from .config import Config
from .db import connect
from .generator import generate
from .pipeline import run_pipeline
from .reports import export_csv, write_report


def main(argv=None):
    p = argparse.ArgumentParser(prog="txn_pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate", help="create synthetic accounts/transactions/labels")
    g.add_argument("--txns", type=int, default=200)
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--anomaly-rate", type=float, default=0.08)
    g.add_argument("--out-dir", default="data/sample")
    r = sub.add_parser("run", help="run the pipeline on one input file")
    r.add_argument("--input", default="data/sample/transactions.csv")
    r.add_argument("--accounts", default="data/sample/accounts.csv")
    r.add_argument("--db", default="output/curated.db")
    r.add_argument("--run-id")
    rp = sub.add_parser("report", help="reconciliation + validation report + CSV export")
    rp.add_argument("--db", default="output/curated.db")
    rp.add_argument("--out-dir", default="output")
    b = sub.add_parser("benchmark", help="time a larger run, naive vs batched load")
    b.add_argument("--txns", type=int, default=40000)
    a = p.parse_args(argv)

    if a.cmd == "generate":
        print(json.dumps(generate(a.txns, a.seed, a.anomaly_rate, a.out_dir)))
    elif a.cmd == "run":
        m = run_pipeline(a.input, a.accounts, Config(db_path=a.db), run_id=a.run_id)
        m.pop("input_sha256", None)
        print(json.dumps(m, indent=2))
        return 0 if m["status"] != "FAILED" else 1
    elif a.cmd == "report":
        conn = connect(a.db)
        checks = write_report(conn, os.path.join(a.out_dir, "reports"))
        export_csv(conn, os.path.join(a.out_dir, "curated"))
        for c in checks:
            print(("PASS " if c["passed"] else "FAIL ") + c["check"], c["violations"])
        return 0 if all(c["passed"] for c in checks) else 1
    elif a.cmd == "benchmark":
        tmp = tempfile.mkdtemp()
        t = time.perf_counter()
        info = generate(a.txns, 7, 0.08, tmp)
        print(f"generated {info['rows']} rows in {time.perf_counter() - t:.2f}s")
        for batch in (False, True):
            cfg = Config(db_path=os.path.join(tmp, f"bench_{batch}.db"), batch_insert=batch)
            m = run_pipeline(os.path.join(tmp, "transactions.csv"), os.path.join(tmp, "accounts.csv"), cfg)
            print(f"batch_insert={batch!s:5} total={m['duration_seconds']:.2f}s validate={m['validate_seconds']:.2f}s "
                  f"load={m['load_seconds']:.2f}s rows/s={m['rows_read'] / m['duration_seconds']:,.0f} "
                  f"accepted={m['rows_accepted']} rejected={m['rows_rejected']}")
        shutil.rmtree(tmp)
    return 0


if __name__ == "__main__":
    sys.exit(main())
