#!/bin/bash

export TARGET="postgres://postgres@localhost:5432/financial_db"
cd ~/Downloads/cs4221/4221_final_project/financial-testbed/test_data/ohlc

for file in *; do
    awk -v sym='AAPL' 'BEGIN { FS=OFS="," } NR==1 { print "time,symbol,open,high,low,close,volume" } NR>1 { print $1, sym, $2, $3, $4, $5, $6 }' "$file" > "/tmp/$file.with_symbol.csv"

    timescaledb-parallel-copy \
        --skip-header \
        --connection "$TARGET" \
        --table stock_ticks \
        --file "/tmp/$file.with_symbol.csv" \
        --workers 8 \
        --reporting-period 30s
done
