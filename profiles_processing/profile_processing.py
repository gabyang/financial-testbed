#!/usr/bin/env python3
"""
ingest_profiles.py
------------------
• Loads company‑profile JSON (Financial Modeling Prep style)  
• Normalises bad / blank dates to NULL  
• Bulk‑inserts into the `profiles` table, skipping duplicates  
• Mirrors *everything* that appears on the terminal (stdout + stderr)
  into a log file for easy troubleshooting
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch   # faster than row‑by‑row

# ──────────────────────────────────────────────────────────────────────
# 0.  Simple "tee" so stdout & stderr go to console **and** log file
# ──────────────────────────────────────────────────────────────────────
LOG_FILE = Path("profiles_ingest.log").resolve()
log_fh   = LOG_FILE.open("w")              # use "a" to append across runs


class Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for s in self.streams:
            s.write(data)

    def flush(self):
        for s in self.streams:
            s.flush()


sys.stdout = Tee(sys.__stdout__, log_fh)
sys.stderr = Tee(sys.__stderr__, log_fh)

print(f"[INFO] Terminal output is mirrored to {LOG_FILE}\n")

# ──────────────────────────────────────────────────────────────────────
# 1.  DB settings  (⚠️ edit to match your environment)
# ──────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "financial_db",
    "user": "postgres",
    "password": "postgres",
}

# ──────────────────────────────────────────────────────────────────────
# 2.  Helpers
# ──────────────────────────────────────────────────────────────────────
def safe_date(val):
    """
    Convert YYYY‑MM‑DD (string) → datetime.date.
    Returns None for '', null, or unparsable input.
    """
    if not val:  # None or ''
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%d").date()
    except ValueError:
        print(f"[WARN] bad date '{val}' – storing NULL")
        return None


# ──────────────────────────────────────────────────────────────────────
# 3.  Ingestion core
# ──────────────────────────────────────────────────────────────────────
INSERT_SQL = """
    INSERT INTO profiles (
        symbol, price, beta, volAvg, mktCap, lastDiv, "range", changes,
        companyName, currency, cik, isin, cusip, exchange, exchangeShortName,
        industry, website, description, ceo, sector, country, fullTimeEmployees,
        phone, address, city, state, zip, dcfDiff, dcf, image, ipoDate,
        defaultImage, isEtf, isActivelyTrading, isAdr, isFund
    ) VALUES (
        %(symbol)s, %(price)s, %(beta)s, %(volAvg)s, %(mktCap)s, %(lastDiv)s, %(range)s, %(changes)s,
        %(companyName)s, %(currency)s, %(cik)s, %(isin)s, %(cusip)s, %(exchange)s, %(exchangeShortName)s,
        %(industry)s, %(website)s, %(description)s, %(ceo)s, %(sector)s, %(country)s, %(fullTimeEmployees)s,
        %(phone)s, %(address)s, %(city)s, %(state)s, %(zip)s, %(dcfDiff)s, %(dcf)s, %(image)s, %(ipoDate)s,
        %(defaultImage)s, %(isEtf)s, %(isActivelyTrading)s, %(isAdr)s, %(isFund)s
    )
    ON CONFLICT (symbol) DO NOTHING;
"""


def insert_profiles(json_path: str | Path):
    json_path = Path(json_path).expanduser().resolve()
    print(f"[INFO] Loading JSON from {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        profiles_data = json.load(f)

    rows_to_insert = []
    total_records = 0
    for symbol, records in profiles_data.items():
        for rec in records:
            total_records += 1
            rec["ipoDate"] = safe_date(rec.get("ipoDate"))
            rows_to_insert.append(rec)

    print(f"[INFO] Parsed {total_records:,} JSON objects")

    start = time.perf_counter()
    try:
        with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
            execute_batch(cur, INSERT_SQL, rows_to_insert, page_size=1000)
            conn.commit()
            inserted = cur.rowcount  # number of *attempted* inserts
    except Exception as exc:
        print(f"[FATAL] Database error: {exc}")
        raise

    elapsed = time.perf_counter() - start

    print("\n=== Ingestion summary ===")
    print(f"JSON rows processed : {total_records:,}")
    print(f"New rows inserted   : {inserted:,}")
    print(f"Skipped (duplicates): {total_records - inserted:,}")
    print(f"Insert runtime      : {elapsed:.3f} seconds")
    print("=========================\n")


# ──────────────────────────────────────────────────────────────────────
# 4.  CLI entry‑point
# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        # Default path can be overridden via CLI:  python ingest_profiles.py /path/to/file.json
        arg_path = sys.argv[1] if len(sys.argv) > 1 else "test_data/profile_estimate/profile.json"
        insert_profiles(arg_path)
    finally:
        log_fh.close()
