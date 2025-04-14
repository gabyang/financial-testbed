#!/bin/bash

# Set the PostgreSQL connection string.
export TARGET=postgres://postgres:@localhost:5432/financial_db

# Loop over each symbol directory under OHLC/.
# (Assuming your document files are in directories like OHLC/AAPL, OHLC/MSFT, etc.)
for symbol_dir in test_data/OHLC/*; do
    # Retrieve the symbol from the directory name.
    symbol=$(basename "$symbol_dir")
    echo "Processing files for symbol: $symbol"

    # Loop over each file in the symbol directory.
    for file in "$symbol_dir"/*; do
        echo "Processing file: $file for symbol: $symbol"
        
        # Create a temporary file to hold the modified document.
        tmpfile=$(mktemp)

        # Process the document:
        # - Assume the document has a header row: "timestamp,open,high,low,close,volume".
        # - For the header, insert "symbol" as the second field.
        # - For data rows, insert the symbol (retrieved from the directory) as the second field.
        awk -v sym="$symbol" 'BEGIN { FS=","; OFS="," }
            NR==1 { 
                print "timestamp,symbol,open,high,low,close,volume"; 
                next 
            }
            { 
                print $1, sym, $2, $3, $4, $5, $6 
            }' "$file" > "$tmpfile"

        # Ingest the modified file into the stock_ticks table.
        # We skip the header row here with --skip-header.
        timescaledb-parallel-copy \
            --skip-header \
            --connection "$TARGET" \
            --table stock_ticks \
            --file "$tmpfile" \
            --workers 8 \
            --reporting-period 30s

        # Remove the temporary file.
        rm "$tmpfile"
    done
done
