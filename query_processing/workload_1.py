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

# Define parameters for significant price movement threshold ratios and window options.
RATIOS = [0.05, 0.10, 0.15]
WINDOWS = ['1 week', '2 weeks']

# Set grouping option. Choose 'symbol' to compute results per symbol,
# or 'industry' to aggregate significant changes by industry.
GROUP_BY_OPTION = 'industry'  # or change to 'industry'

def get_symbols(conn):
    """Retrieve a list of symbols from the profiles table."""
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM profiles;")
    symbols = [row[0] for row in cur.fetchall()]
    cur.close()
    return symbols

def get_industries(conn):
    """Retrieve a list of distinct industries from the profiles table."""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT industry FROM profiles;")
    industries = [row[0] for row in cur.fetchall()]
    cur.close()
    return industries

def generate_query(filter_value, ratio, window, grouping='symbol'):
    """
    Generate the SQL query for computing significant price movements.
    
    When grouping by symbol:
      - filter_value is a single symbol (e.g., 'AAPL').
      - The query filters stock data for that symbol.
      
    When grouping by industry:
      - filter_value is an industry name.
      - The query filters stock data to include only symbols belonging to that industry.
      
    WINDOW is either '1 week' or '2 weeks' and determines how the time series is windowed.
    """
    if window == '1 week':
        group_expr = "date_trunc('week', time)"
    elif window == '2 weeks':
        group_expr = ("date_trunc('week', time) + interval '1 week' * "
                      "((EXTRACT(WEEK FROM time)::int % 2) * -1)")
    else:
        raise ValueError(f"Unsupported window: {window}")

    # Build the WHERE clause based on grouping method.
    if grouping == 'symbol':
        base_filter = f"symbol = '{filter_value}' AND time >= '2013-01-01'"
    elif grouping == 'industry':
        base_filter = (f"symbol IN (SELECT symbol FROM profiles WHERE industry = '{filter_value}') "
                       f"AND time >= '2013-01-01'")
    else:
        raise ValueError("Grouping must be either 'symbol' or 'industry'.")

    base_query = f"""
    WITH window_avg AS (
        SELECT
            symbol,
            {group_expr} AS window_start,
            AVG(close) AS avg_close
        FROM stock_ticks
        WHERE {base_filter}
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
        SELECT
            symbol,
            window_start,
            avg_close,
            prev_avg_close,
            ROUND(100.0 * (avg_close - prev_avg_close) / prev_avg_close, 2) AS pct_change
        FROM deltas
        WHERE prev_avg_close IS NOT NULL
          AND ABS((avg_close - prev_avg_close) / prev_avg_close) >= {ratio}
    )
    """
    
    if grouping == 'symbol':
        # Join each change with the company's profile info.
        query = base_query + """
        SELECT ch.symbol,
               ch.window_start,
               ch.avg_close,
               ch.prev_avg_close,
               ch.pct_change,
               p.industry,
               p.companyName
        FROM changes ch
        LEFT JOIN profiles p ON ch.symbol = p.symbol;
        """
    elif grouping == 'industry':
        # Group by industry and aggregate the significant changes.
        query = base_query + """
        SELECT p.industry,
               array_agg(ROW(ch.symbol, ch.window_start, ch.avg_close, ch.prev_avg_close, ch.pct_change)::text) AS changes,
               MIN(p.companyName) AS representative_company
        FROM changes ch
        LEFT JOIN profiles p ON ch.symbol = p.symbol
        GROUP BY p.industry;
        """
    return query

def run_batch():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    results = []
    
    if GROUP_BY_OPTION == 'symbol':
        # Retrieve all symbols from the profiles table.
        symbols = get_symbols(conn)
        for symbol, ratio, window in itertools.product(symbols, RATIOS, WINDOWS):
            try:
                query = generate_query(symbol, ratio, window, grouping='symbol')
                print(f"Running query for SYMBOL={symbol}, RATIO={ratio}, WINDOW={window}")
                cur.execute(query)
                rows = cur.fetchall()
                for row in rows:
                    results.append({
                        'symbol': row[0],
                        'window': window,
                        'ratio': ratio,
                        'window_start': row[1],
                        'avg_close': row[2],
                        'prev_avg_close': row[3],
                        'pct_change': row[4],
                        'industry': row[5],
                        'companyName': row[6]
                    })
            except Exception as e:
                print(f"Error running query for {symbol}, {ratio}, {window}: {e}")
                continue
    elif GROUP_BY_OPTION == 'industry':
        # Retrieve all distinct industries from the profiles table.
        industries = get_industries(conn)
        for industry, ratio, window in itertools.product(industries, RATIOS, WINDOWS):
            try:
                query = generate_query(industry, ratio, window, grouping='industry')
                print(f"Running query for INDUSTRY={industry}, RATIO={ratio}, WINDOW={window}")
                cur.execute(query)
                rows = cur.fetchall()
                for row in rows:
                    changes_array = row[1] if row[1] is not None else []
                    # Join aggregated change records for display.
                    changes_str = " | ".join(changes_array)
                    results.append({
                        'industry': row[0],
                        'ratio': ratio,
                        'window': window,
                        'representative_company': row[2],
                        'aggregated_changes': changes_str
                    })
            except Exception as e:
                print(f"Error running grouped query for {industry}, {ratio}, {window}: {e}")
                continue
    
    cur.close()
    conn.close()
    
    output_file = "significant_changes_grouped.csv"
    df = pd.DataFrame(results)
    df.to_csv(output_file, index=False)
    print(f"Saved results to {output_file}")

if __name__ == "__main__":
    run_batch()
