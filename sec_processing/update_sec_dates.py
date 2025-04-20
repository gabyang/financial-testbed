import psycopg2
from pathlib import Path

DB_CONFIG = {
    "dbname": "financial_db",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

def update_filing_dates(root_dir: str):
    """
    Update the `filing_date` in the sec_filings table using filing-date.txt
    files located in: {symbol}/10-Q/{filing_id}/filing-date.txt
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    updated, skipped = 0, 0

    root_path = Path(root_dir)
    for symbol_dir in root_path.iterdir():
        if not symbol_dir.is_dir():
            continue

        quarterly_dir = symbol_dir / "10-Q"
        if not quarterly_dir.exists():
            continue

        for filing_dir in quarterly_dir.iterdir():
            if not filing_dir.is_dir():
                continue

            filing_id = filing_dir.name
            filing_date_file = filing_dir / "filing-date.txt"

            if not filing_date_file.exists():
                print(f"✗ Missing filing-date.txt for {filing_id}")
                skipped += 1
                continue

            with open(filing_date_file, "r", encoding="utf-8") as f:
                date_str = f.read().strip()

            # Convert YYYYMMDD to YYYY-MM-DD
            try:
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            except Exception as e:
                print(f"✗ Failed to format date for {filing_id}: {e}")
                skipped += 1
                continue

            try:
                cur.execute(
                    """
                    UPDATE sec_filings
                    SET filing_date = %s
                    WHERE filing_id = %s
                    """,
                    (formatted_date, filing_id)
                )
                print(f"✓ Updated {filing_id} to {formatted_date}")
                updated += 1
            except Exception as e:
                print(f"✗ DB update failed for {filing_id}: {e}")
                skipped += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nDone. {updated} updated, {skipped} skipped.")

if __name__ == "__main__":
    update_filing_dates("./test_data/SEC-filings")
