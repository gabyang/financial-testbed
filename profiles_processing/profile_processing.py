import json
import psycopg2
from datetime import datetime

# Database configuration settings
DB_CONFIG = {
    'dbname': 'financial_db',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

def insert_profiles(json_file):
    # Connect to your PostgreSQL database
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Open and load the JSON file containing profiles
    with open(json_file, 'r') as f:
        profiles_data = json.load(f)
    
    # Iterate over each symbol in the JSON
    for symbol, records in profiles_data.items():
        # Each symbol typically contains a list of profiles (here we expect a single profile per symbol)
        for record in records:
            # Convert date strings into a proper date object if needed
            if 'ipoDate' in record and record['ipoDate']:
                try:
                    record['ipoDate'] = datetime.strptime(record['ipoDate'], '%Y-%m-%d').date()
                except ValueError:
                    record['ipoDate'] = None

            # Define the SQL INSERT statement.
            # Note: "range" is a reserved keyword so we wrap it in double quotes.
            sql = """
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
            try:
                cur.execute(sql, record)
            except Exception as e:
                print(f"Error inserting record for symbol {symbol}: {e}")
    
    # Commit the transaction and close the connection
    conn.commit()
    cur.close()
    conn.close()
    print("Profiles ingestion completed.")

if __name__ == "__main__":
    insert_profiles('test_data/profile_estimate/profile.json')