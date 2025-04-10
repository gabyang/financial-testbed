import psycopg2
import itertools
import pandas as pd
from datetime import datetime

DB_CONFIG = {
    'dbname': 'financial_db',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

# Define parameters
RATIOS = [0.05, 0.10, 0.15]
WINDOWS = ['1 week', '2 weeks']
SYMBOLS = [
    'AAPL'
]

def generate_query(symbol, ratio, window):

    if window == '1 week':
        group_expr = "date_trunc('week', time)"
    elif window == '2 weeks':
        group_expr = "date_trunc('week', time) + interval '1 week' * ((EXTRACT(WEEK FROM time)::int % 2) * -1)"
    else:
        raise ValueError(f"Unsupported window: {window}")

    return f"""
    WITH window_avg AS (
        SELECT
            symbol,
            {group_expr} AS window_start,
            AVG(close) AS avg_close
        FROM stock_ticks
        WHERE symbol = '{symbol}' AND time >= '2013-01-01'
        GROUP BY symbol, window_start
    ),
    deltas AS (
        SELECT
            symbol,
            window_start,
            avg_close,
            LAG(avg_close) OVER (PARTITION BY symbol ORDER BY window_start) AS prev_avg_close
        FROM window_avg
    ),
    changes AS (
        SELECT *,
            ROUND(100.0 * (avg_close - prev_avg_close) / prev_avg_close, 2) AS pct_change
        FROM deltas
        WHERE prev_avg_close IS NOT NULL
          AND ABS((avg_close - prev_avg_close) / prev_avg_close) >= {ratio}
    )
    SELECT * FROM changes;
    """


def run_batch():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    results = []

    for symbol, ratio, window in itertools.product(SYMBOLS, RATIOS, WINDOWS):
        try:
            query = generate_query(symbol, ratio, window)
            print(f"Running query for SYMBOL={symbol}, RATIO={ratio}, WINDOW={window}")
            cur.execute(query)
            rows = cur.fetchall()
            for row in rows:
                results.append({
                    'symbol': symbol,
                    'window': window,
                    'ratio': ratio,
                    'window_start': row[1],
                    'avg_close': row[2],
                    'prev_avg_close': row[3],
                    'pct_change': row[4]
                })
        except Exception as e:
            print(f"Error running query for {symbol}, {ratio}, {window}: {e}")
            continue

    cur.close()
    conn.close()

    df = pd.DataFrame(results)
    df.to_csv("significant_changes.csv", index=False)
    print("Saved results to significant_changes.csv")

if __name__ == "__main__":
    run_batch()
