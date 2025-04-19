import json
import psycopg2
from psycopg2.extras import execute_values

# Database connection parameters
DB_PARAMS = {
    'dbname': 'financial_db',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

def create_schema_and_tables(conn):
    """Create the necessary schema and tables if they don't exist."""
    with conn.cursor() as cur:
        # Create historic_estimates table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS historic_estimates (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            date DATE,
            eps_estimated NUMERIC,
            time VARCHAR(10),
            revenue_estimated NUMERIC,
            updated_from_date DATE,
            fiscal_date_ending DATE
        );
        """)
        
        conn.commit()
        print("Schema and tables created successfully")

def parse_date(date_str):
    """Parse date string, return None if empty or invalid."""
    if not date_str:
        return None
    return date_str

def ingest_json_data(file_path, conn):
    """Ingest JSON data into PostgreSQL database."""
    try:
        # Load the JSON data
        with open(file_path, 'r') as file:
            data = json.load(file)
        
        # Prepare the data for insertion
        all_earnings = []
        
        # Iterate through each stock symbol
        for symbol, earnings_list in data.items():
            for earning in earnings_list:
                # Extract and format data for insertion, handling potential None values
                record = (
                    earning['symbol'],
                    parse_date(earning.get('date')),
                    earning.get('epsEstimated'),
                    earning.get('time'),
                    earning.get('revenueEstimated'),
                    parse_date(earning.get('updatedFromDate')),
                    parse_date(earning.get('fiscalDateEnding'))
                )
                all_earnings.append(record)
        
        # Insert data into the database
        with conn.cursor() as cur:
            insert_query = """
            INSERT INTO earnings_data 
            (symbol, date, eps_estimated, time, revenue_estimated, updated_from_date, fiscal_date_ending)
            VALUES %s
            """
            execute_values(cur, insert_query, all_earnings)
            conn.commit()
            
        print(f"Successfully inserted {len(all_earnings)} earnings records")
        
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except json.JSONDecodeError:
        print(f"Error: File '{file_path}' contains invalid JSON.")
    except Exception as e:
        print(f"Error: An unexpected error occurred: {e}")
        conn.rollback()

def main():
    file_path = "test_data/profile_estimate/historical_earning_estimates.json"  # Update this to your actual file path
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        
        # Create schema and tables
        create_schema_and_tables(conn)
        
        # Ingest the data
        ingest_json_data(file_path, conn)
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    main()