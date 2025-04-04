## Setup

### Data preparation

#### Unzip ohlc data by running and run the pre-processing script to convert all the data to relevant schema
```
gunzip *.gz
python3 preprocess_ohlc.py

```
#### Prepare news data by running process_news.py
```
python3 process_news.py
```



### Naive Implementation #1
#### Postgresql setup

Add Extensions
```
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;
```

Stock ticks
```
CREATE TABLE IF NOT EXISTS stock_metadata (
    symbol VARCHAR(5) PRIMARY KEY,
    name TEXT
);
CREATE TABLE IF NOT EXISTS stock_ticks (
                time TIMESTAMPZ PRIMARY KEY,
                symbol VARCHAR(5) NOT NULL REFERENCES stock_metadata(symbol),
                open INT NOT NULL,
                high INT NOT NULL,
                low INT NOT NULL,
                close INT NOT NULL,
                volume INT NOT NULL
            );
SELECT create_hypertable('stock_ticks', 'time');
```

Because this is a naive implementation, there will not be any optimisations.

Ingest ohlc data
```
export TARGET=postgres://USERNAME:PASSWORD@localhost:5432/DATABASE_NAME
# e.g., using the default username of 'postgres', the default password as empty, and the default database name as 'postgres':
# export TARGET=postgres://postgres:@localhost:5432/postgres

for file in *; do
    timescaledb-parallel-copy \
        --skip-header \
        --connection $TARGET \
        --table stock_ticks \
        --file "$file" \
        --workers 8 \
        --reporting-period 30s
done
```


Add schema for news articles. Change vector dimensions based on the sentence transformer
```
CREATE TABLE IF NOT EXISTS articles (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(5) NOT NULL REFERENCES stock_metadata(symbol),
                title TEXT,
                content TEXT,
                author TEXT, 
                date TIMESTAMP, 
                url TEXT,
                source TEXT,
                embedding vector(4096)
            );
```
Decide on what metadata we want to keep by elminating the above fields. should at least keep date, symbol and id

