import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import json
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LinearRegression
import numpy as np
import time
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import os
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

BASE_PATH = './test_data'

# --- Stream Simulation Parameters ---
stream_start = pd.to_datetime("2020-01-01")
stream_end = pd.to_datetime("2023-12-31")
window_size = pd.Timedelta(days=7)

# Connect to DB
conn = psycopg2.connect(
    dbname="financial_db", user="postgres", password="postgres", host="localhost", port="5432"
)
cur = conn.cursor()

# Load SEC CSV and insert into temp
df_sec_eps = pd.read_csv("sec_extracts.csv", parse_dates=["start_date"])
df_sec_eps.replace("-", None, inplace=True)
df_sec_eps.replace({"": None, "NA": None, "N/A": None}, inplace=True)
numeric_cols = ['eps_basic', 'eps_diluted', 'eps_basic9', 'eps_diluted9']
df_sec_eps[numeric_cols] = df_sec_eps[numeric_cols].apply(pd.to_numeric, errors='coerce')
cur.execute("""
    DROP TABLE IF EXISTS sec_eps_temp;
    CREATE TEMP TABLE sec_eps_temp (
        symbol VARCHAR(10),
        start_date DATE,
        filing_period DATE,
        eps_basic NUMERIC,
        eps_diluted NUMERIC,
        eps_basic9 NUMERIC,
        eps_diluted9 NUMERIC
    );
""")
conn.commit()
execute_values(cur, "INSERT INTO sec_eps_temp VALUES %s", df_sec_eps.values.tolist())
conn.commit()

def clean_text(text):
    """Clean and normalize text for analysis with improved filtering"""
    # Convert to lowercase
    text = text.lower()
    
    # More thorough HTML/XML tag removal
    text = re.sub(r'<[^>]*>', ' ', text)
    
    # Remove CSS and JavaScript blocks entirely
    text = re.sub(r'<style.*?</style>', ' ', text, flags=re.DOTALL)
    text = re.sub(r'<script.*?</script>', ' ', text, flags=re.DOTALL)
    
    # Remove special characters but keep $ and % as they may be important for financial text
    text = re.sub(r'[^\w\s$%]', ' ', text)
    
    # Remove numbers that aren't part of words
    text = re.sub(r'\b\d+\b', ' ', text)
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Tokenize
    tokens = word_tokenize(text)
    
    # Get enhanced stopwords list
    stop_words = set(stopwords.words('english'))
    
    # Filter tokens with more strict criteria
    filtered_tokens = []
    for word in tokens:
        # Minimum 3 characters for most tokens (unless it's a meaningful financial symbol)
        if word not in stop_words and len(word) >= 3:
            # Additional check - reject tokens that look like programming variables or CSS classes
            if not re.match(r'^[a-z][a-z]$', word) and not re.match(r'^[a-z][0-9]$', word):
                filtered_tokens.append(word)
    
    return ' '.join(filtered_tokens)

def get_sec_texts(symbol, start_date, end_date):
    """Extract text from SEC filings for a specific symbol and date range"""
    texts = []
    
    try:
        # Path to 10-Q filings
        sec_path = os.path.join(BASE_PATH, "SEC-Filings", symbol, "10-Q")
        
        # Check if path exists
        if not os.path.exists(sec_path):
            return texts
        
        # List all filing folders
        filing_folders = [d for d in os.listdir(sec_path) if os.path.isdir(os.path.join(sec_path, d))]
        
        # For each filing folder, check the submission file
        for folder in filing_folders:
            submission_path = os.path.join(sec_path, folder, "full-submission.txt")
            
            if os.path.exists(submission_path):
                # Use file modification date as a proxy for filing date
                file_date = datetime.fromtimestamp(os.path.getmtime(submission_path))
                
                if start_date <= file_date <= end_date:
                    # Process SEC filings in chunks rather than loading the entire file
                    cleaned_chunks = []
                    chunk_size = 1024 * 1024  # 1MB chunks
                    
                    with open(submission_path, 'r', encoding='utf-8', errors='ignore') as f:
                        while True:
                            chunk = f.read(chunk_size)
                            if not chunk:
                                break
                            cleaned_chunks.append(clean_text(chunk))
                    
                    # Join chunks
                    if cleaned_chunks:
                        texts.append(" ".join(cleaned_chunks))
    
    except Exception as e:
        print(f"Error extracting SEC texts for {symbol}: {e}")
    
    return " ".join(texts)
    

def get_news_texts(symbol, start_date, end_date):
    """Extract text from news articles for a specific symbol and date range"""
    texts = []
    
    try:
        # Path to news.json for this symbol
        news_json_path = os.path.join(BASE_PATH, "News", symbol, "news.json")
        news_folder_path = os.path.join(BASE_PATH, "News", symbol, "news")
        
        # Check if paths exist
        if not os.path.exists(news_json_path) or not os.path.exists(news_folder_path):
            return texts
        
        # Load news JSON
        with open(news_json_path, 'r') as f:
            news_data = json.load(f)
        
        # Convert start_date and end_date to Unix timestamp
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())
        
        # Filter news by date range
        filtered_news = [item for item in news_data if start_ts <= item.get('datetime', 0) <= end_ts]
        
        # For each news item, get the text from the corresponding HTML file
        for news_item in filtered_news:
            news_id = news_item.get('id')
            if news_id:
                html_path = os.path.join(news_folder_path, f"{news_id}.html")
                
                if os.path.exists(html_path):
                    # Extract text from HTML
                    with open(html_path, 'r', encoding='utf-8', errors='ignore') as f:
                        html_content = f.read()
                    
                    # Use BeautifulSoup to extract text
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # Get text and clean it
                    text = soup.get_text(separator=' ', strip=True)
                    
                    # Add headline and summary from JSON as they might not be in the HTML
                    headline = news_item.get('headline', '')
                    summary = news_item.get('summary', '')
                    
                    full_text = f"{headline} {summary} {text}"
                    texts.append(clean_text(full_text))
                    
                    # Clear memory
                    del soup
                    del text
                    del html_content
                else:
                    # If HTML doesn't exist, use headline and summary
                    headline = news_item.get('headline', '')
                    summary = news_item.get('summary', '')
                    
                    if headline or summary:
                        texts.append(clean_text(f"{headline} {summary}"))
    
    except Exception as e:
        print(f"Error extracting news texts for {symbol}: {e}")
    
    return " ".join(texts)

# --- Streaming Loop ---
current = stream_start
batch_logs = []

while current < stream_end:
    window_start = current
    window_end = current + window_size
    print(f"🌀 Streaming window: {window_start.date()} to {window_end.date()}")

    query = """
WITH estimate_vs_actual AS (
    SELECT 
        e.symbol,
        e.fiscal_date_ending,
        s.start_date,
        e.eps_estimated,
        s.eps_diluted
    FROM historic_estimates e
    JOIN sec_eps_temp s 
        ON e.symbol = s.symbol 
        AND e.fiscal_date_ending = s.filing_period
    WHERE abs(e.eps_estimated - s.eps_diluted) > 0.01
),
daily_prices AS (
    SELECT 
        symbol,
        time::date AS date,
        FIRST_VALUE(open) OVER (PARTITION BY symbol, time::date ORDER BY time) AS day_open,
        LAST_VALUE(close) OVER (PARTITION BY symbol, time::date ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS day_close
    FROM stock_ticks
    WHERE time >= %s AND time < %s
),
distinct_daily_prices AS (
    SELECT DISTINCT symbol, date, day_open, day_close
    FROM daily_prices
),
price_movements AS (
    SELECT 
        ev.symbol,
        ev.fiscal_date_ending AS filing_period,
        ev.start_date,
        dp1.day_open AS same_day_open,
        dp1.day_close AS same_day_close,
        dp2.day_close AS next_day_close,
        ROUND(100.0 * (dp1.day_close - dp1.day_open) / dp1.day_open, 2) AS same_day_change,
        ROUND(100.0 * (dp2.day_close - dp1.day_close) / dp1.day_close, 2) AS next_day_change
    FROM estimate_vs_actual ev
    JOIN distinct_daily_prices dp1 
        ON ev.symbol = dp1.symbol 
        AND ev.start_date = dp1.date
    LEFT JOIN distinct_daily_prices dp2 
        ON dp1.symbol = dp2.symbol 
        AND dp2.date = dp1.date + INTERVAL '1 day'
),
filtered AS (
    SELECT *
    FROM price_movements
    WHERE abs(same_day_change) > 3 OR abs(next_day_change) > 3
)
SELECT DISTINCT 
    f.symbol, 
    f.filing_period, 
    p.industry, 
    f.same_day_change,
    f.next_day_change
FROM filtered f
JOIN profiles p ON f.symbol = p.symbol;
    """

    df = pd.read_sql_query(query, conn, params=(window_start, window_end))
    batch_start_time = time.time()
    industry_keywords = {}

    for industry, group in df.groupby("industry"):
        texts, changes = [], []
        for row in group.itertuples():
            # Calculate the combined change (using same_day_change as default)
            pct_change = row.same_day_change
            if pd.notna(row.next_day_change):
                pct_change += row.next_day_change
            
            # Get text from SEC filings
            sec_text = get_sec_texts(row.symbol, window_start, window_end)
            # Get text from news articles
            news_text = get_news_texts(row.symbol, window_start, window_end)
            # Combine the texts
            full_text = f"{sec_text} {news_text}".strip()
            
            if full_text:  # Only add if there's actual text
                texts.append(full_text)
                changes.append(pct_change)

        if len(texts) >= 2:  # Need at least 5 samples to build a model
            # TF-IDF vectorization
            tfidf = TfidfVectorizer(max_features=1000, stop_words='english')
            X = tfidf.fit_transform(texts).toarray()
            y = np.array(changes)
            
            # Linear regression model
            model = LinearRegression().fit(X, y)
            terms = tfidf.get_feature_names_out()
            coefs = model.coef_
            
            # Get top positive and negative terms
            top_pos = sorted([(t, c) for t, c in zip(terms, coefs) if c > 0], key=lambda x: -x[1])[:10]
            top_neg = sorted([(t, c) for t, c in zip(terms, coefs) if c < 0], key=lambda x: x[1])[:10]
            
            industry_keywords[industry] = {
                "top_positive": top_pos,
                "top_negative": top_neg
            }

    if industry_keywords:
        # Save the results to a JSON file
        with open(f"nlp_keywords_{window_start.date()}.json", "w") as f:
            json.dump(industry_keywords, f, indent=2)

    # Log the batch statistics
    batch_logs.append({
        "window_start": str(window_start.date()),
        "records": len(df),
        "industries": df['industry'].nunique(),
        "batch_runtime": round(time.time() - batch_start_time, 2)
    })

    # Move to the next window
    current += window_size

print("✅ Stream-based mini-batch NLP processing complete.")
with open("stream_batch_logs.json", "w") as f:
    json.dump(batch_logs, f, indent=2)