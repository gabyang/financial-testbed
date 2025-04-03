import os
import pandas as pd
import numpy as np
from datetime import datetime

def preprocess_ohlc_data(file_path):
    """Preprocess OHLC data from CSV file and save back to the same file."""
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        
        # Scale price columns and convert to integers
        price_columns = ['open', 'high', 'low', 'close']
        for col in price_columns:
            df[col] = np.floor(df[col] * 100).astype(int)
        
        # Ensure volume is integer
        df['volume'] = df['volume'].astype(int)
        
        # Save back to the same file
        df.to_csv(file_path, index=False)
        print(f"Successfully updated {file_path}")
        
    except Exception as e:
        print(f"Error preprocessing file {file_path}: {e}")

def process_ohlc_files():
    """Process all OHLC CSV files in the directory."""
    ohlc_dir = 'test_data/ohlc'
    
    # Process each directory (each representing a symbol)
    for symbol_dir in os.listdir(ohlc_dir):
        print
        file_path = os.path.join(ohlc_dir, symbol_dir)
            
        print(f"Processing file: {file_path}")
        preprocess_ohlc_data(file_path)
            

if __name__ == "__main__":
    print("Starting script...")
    process_ohlc_files() 
