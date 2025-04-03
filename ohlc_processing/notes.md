Optimisation will follow from timeseries project or timescaledb:
1) Schema
2) Tuning Chunk Interval Size
3) Tuning PostgreSQL Configurations
4) Indexing
```
CREATE INDEX idx_symbol_time ON stock_ticks (symbol, time DESC);
```

5) Applying Data Compression
```
ALTER TABLE stock_ticks SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'time DESC',
  timescaledb.compress_segmentby = 'symbol'
);
```
6) Using Continuous Aggregate or Materialised View


Optimisations using other databases:
1) Mongodb
Use one document per symbol per day. This avoids exceeding MongoDB's 16MB document limit for tick data.
Example structure:
```
{
  "symbol": "AAPL",
  "date": "2025-04-03",
  "ticks": [
    { "time": "09:30:00", "price": 150.25, "volume": 100 },
    { "time": "09:30:01", "price": 150.30, "volume": 200 }
  ]
}
```

2) InfluxDB
Use a single measurement for stock prices with tags for ticker and exchange, and fields for bid, ask, and value.
Example line protocol:
```
stock_price,ticker=AAPL,exchange=NASDAQ bid=150.25,ask=150.30,value=150.27 1680505800000000000
```

3) 
