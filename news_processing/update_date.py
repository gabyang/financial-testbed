import json
import os
import psycopg2
import glob
from datetime import datetime
import os.path

# Database connection (update with your actual PostgreSQL details)
def get_db_connection():
    # Replace these parameters with your actual PostgreSQL connection details
    conn = psycopg2.connect(
        host="localhost",
        database="financial_db",
        user="postgres",
        password="postgres",
        port="5432"
    )
    return conn

# Process each news.json file in ./test_data/News/{symbol}/news.json
def update_articles_dates():
    # Get all symbol folders
    base_path = "./test_data/News"
    
    if not os.path.exists(base_path):
        print(f"Base path {base_path} does not exist!")
        return
    
    # Connect to the database
    conn = get_db_connection()
    cursor = conn.cursor()
    
    total_updated = 0
    
    # Process each symbol folder
    for symbol_dir in os.listdir(base_path):
        symbol_path = os.path.join(base_path, symbol_dir)
        
        # Skip if not a directory
        if not os.path.isdir(symbol_path):
            continue
        
        # Get the symbol from the folder name
        symbol = symbol_dir
        
        # Path to the news.json file for this symbol
        news_file = os.path.join(symbol_path, "news.json")
        
        if not os.path.exists(news_file):
            print(f"No news.json found for symbol {symbol}")
            continue
        
        print(f"Processing {news_file} for symbol {symbol}")
        
        try:
            with open(news_file, 'r', encoding='utf-8') as f:
                news_data = json.load(f)
                
            # news_data could be an array or a single object, handle both cases
            if not isinstance(news_data, list):
                news_data = [news_data]
                
            for item in news_data:
                if 'id' in item and 'datetime' in item:
                    news_id = item['id']
                    
                    # The source field in articles contains {id}.html
                    source_pattern = f"{news_id}.html"
                    
                    # Convert timestamp to a proper date format
                    # The datetime in your example appears to be a Unix timestamp
                    date_value = datetime.fromtimestamp(item['datetime']).strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Update the articles table where:
                    # 1. Source field contains the id.html
                    # 2. Symbol field matches the symbol from the folder name
                    query = """
                    UPDATE articles 
                    SET date = %s 
                    WHERE source LIKE %s AND symbol = %s
                    """
                    
                    cursor.execute(query, (date_value, f"%{source_pattern}%", symbol))
                    updated = cursor.rowcount
                    total_updated += updated
                    
                    if updated > 0:
                        print(f"Updated {updated} records for news ID {news_id}, symbol {symbol}")
        
        except Exception as e:
            print(f"Error processing {news_file} for symbol {symbol}: {e}")
    
    # Commit changes and close connection
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Total records updated: {total_updated}")

if __name__ == "__main__":
    update_articles_dates()