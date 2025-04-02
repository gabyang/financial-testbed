## Setup

### Data preparation

#### Unzip ohlc data by running
```
gunzip *.gz
```

### Naive Implementation #1
#### Postgresql setup

Add Extensions
```
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;
```

Add schema
```
CREATE TABLE IF NOT EXISTS articles (
                id SERIAL PRIMARY KEY,
                title TEXT,
                content TEXT,
                author TEXT,
                date TIMESTAMP,
                url TEXT,
                source TEXT,
                embedding vector(4096)
            );
```
