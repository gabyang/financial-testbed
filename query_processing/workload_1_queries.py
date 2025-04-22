import psycopg2
import itertools
import pandas as pd
from datetime import datetime
import time  # ⏱️

DB_CONFIG = {
    'dbname': 'financial_db',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

RATIOS = [0.05, 0.10, 0.15]
WINDOWS = ['1 week', '2 weeks']
GROUP_BY_OPTION = 'industry'  # or 'symbol'
SHOW_QUERY_TIMING = True  # ⏱️ Toggle this to show/hide individual query timing
SYMBOLS = ["AAPL"]

def get_symbols(conn):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT symbol FROM stock_ticks;")
    symbols = [row[0] for row in cur.fetchall()]
    cur.close()
    return symbols

def get_industries(conn):
    """
    Given a Python list of symbols, return the distinct industries
    for those symbols from the profiles table.
    """
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT industry FROM stock_ticks;")
    industries = [row[0] for row in cur.fetchall()]
    cur.close()
    return industries

def generate_query(filter_value, ratio, window, grouping='symbol'):
    # 1) build the time‐bucketing expression
    if window == '1 week' and grouping == 'symbol':
        group_expr = "time_bucket('1 week', time, '2013-12-25'::timestamptz)"
        base_filter = f"symbol = '{filter_value}'"
        aggregate_table = 'ca_weekly_avg'
    elif window == '2 weeks' and grouping == 'symbol':
        group_expr = "time_bucket('2 weeks', time, '2013-12-18'::timestamptz)"
        base_filter = f"symbol = '{filter_value}'"
        aggregate_table = 'ca_biweekly_avg'
    elif window == '1 week' and grouping == 'industry':
        group_expr = "time_bucket('1 week', time, '2013-12-25'::timestamptz)"
        base_filter = f"industry = '{filter_value}'"
        aggregate_table = 'industry_weekly_ca'
    elif window == '2 weeks' and grouping == 'industry':
        group_expr = "time_bucket('2 weeks', time, '2013-12-18'::timestamptz)"
        base_filter = f"industry = '{filter_value}'"
        aggregate_table = 'industry_biweekly_ca'   
    else:
        raise ValueError(f"Unsupported window: {window} or grouping")

    # 2) common CTEs
    base_query = f"""
    WITH window_avg AS (
      SELECT
        {grouping},
        time_window as window_start,
         avg_close
      FROM {aggregate_table}
      WHERE {base_filter}
    ),
    deltas AS (
      SELECT
        {grouping},
        window_start,
        avg_close,
        LAG(avg_close) OVER (
          PARTITION BY {grouping}
          ORDER BY window_start
        ) AS prev_avg_close
      FROM window_avg
    ),
    changes AS (
      SELECT
        {grouping},
        window_start,
        {ratio}            AS ratio,
        avg_close,
        prev_avg_close,
        ROUND(
          100.0 * (avg_close - prev_avg_close) / prev_avg_close
        , 2)               AS pct_change
      FROM deltas
      WHERE prev_avg_close IS NOT NULL
        AND ABS((avg_close - prev_avg_close)/prev_avg_close) >= {ratio}
    )
    """

    # 3) final projection
    if grouping == 'symbol':
        query = base_query + """
        SELECT
          symbol,
          window_start,
          avg_close,
          prev_avg_close,
          pct_change
        FROM changes
        ORDER BY symbol, window_start;
        """
    else:  # industry‐level rollup
        query = base_query + """
        SELECT
          industry,
          window_start,
          ratio,
          ROUND(AVG(avg_close),2) AS avg_close,
          ROUND(AVG(prev_avg_close),2) AS prev_avg_close,
          ROUND(AVG(pct_change),2) AS avg_pct_change
        FROM changes
        GROUP BY industry, window_start, ratio
        ORDER BY industry, window_start;
        """

    return query

def run_batch():
    start_time = time.time()  # ⏱️ start timing the entire run

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    results_symbol = []
    results_industry = []

    if GROUP_BY_OPTION == 'symbol':
        symbols = get_symbols(conn)
        for symbol, ratio, window in itertools.product(symbols, RATIOS, WINDOWS):
            try:
                query = generate_query(symbol, ratio, window, grouping='symbol')
                print(f"Running query for SYMBOL={symbol}, RATIO={ratio}, WINDOW={window}")
                t0 = time.time()  # ⏱️
                cur.execute(query)
                rows = cur.fetchall()
                if SHOW_QUERY_TIMING:
                    print(f"⏱️ Query took {time.time() - t0:.2f} seconds")  # ⏱️
                for row in rows:
                    results_symbol.append({
                        'symbol': row[0],
                        'window': window,
                        'ratio': ratio,
                        'window_start': row[1],
                        'avg_close': row[2],
                        'prev_avg_close': row[3],
                        'pct_change': row[4]
                    })
            except Exception as e:
                print(f"❌ Error running query for {symbol}, {ratio}, {window}: {e}")
                continue
    elif GROUP_BY_OPTION == 'industry':
        industries = get_industries(conn)
        for industry, ratio, window in itertools.product(industries, RATIOS, WINDOWS):
            try:
                query = generate_query(industry, ratio, window, grouping='industry')
                print(f"Running query for INDUSTRY={industry}, RATIO={ratio}, WINDOW={window}")
                t0 = time.time()  # ⏱️
                cur.execute(query)
                rows = cur.fetchall()
                if SHOW_QUERY_TIMING:
                    print(f"⏱️ Query took {time.time() - t0:.2f} seconds")  # ⏱️
                for row in rows:
                    results_industry.append({
                        'industry': row[0],
                        'window': window,
                        'ratio': ratio,
                        'window_start': row[1],
                        'avg_close': row[3],
                        'prev_avg_close': row[4],
                        'pct_change': row[5]
                    })
            except Exception as e:
                print(f"❌ Error running query for {industry}, {ratio}, {window}: {e}")
                continue

    cur.close()
    conn.close()

    if results_symbol:
        df_sym = pd.DataFrame(results_symbol)
        df_sym.to_csv("significant_changes_symbol.csv", index=False)
        print("✅ Saved symbol-level results to significant_changes_symbol.csv")

    if results_industry:
        df_ind = pd.DataFrame(results_industry)
        df_ind.to_csv("significant_changes_industry.csv", index=False)
        print("✅ Saved industry-level results to significant_changes_industry.csv")

    end_time = time.time()  # ⏱️
    print(f"\n⏱️ Total processing time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    run_batch()