CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE profiles (
    symbol TEXT PRIMARY KEY,
    companyName TEXT,
    exchangeShortName VARCHAR(10),
    industry TEXT,
    description TEXT,
    ceo TEXT,
    sector TEXT,
    country VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS stock_ticks (
    time TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL REFERENCES profiles(symbol),
    industry TEXT,
    open INT NOT NULL,
    high INT NOT NULL,
    low INT NOT NULL,
    close INT NOT NULL,
    volume INT NOT NULL,
    PRIMARY KEY (time, symbol)
);

SELECT create_hypertable(
       'stock_ticks',
       'time',
       chunk_time_interval => INTERVAL '7 days',
       partitioning_column  => 'symbol',
       number_partitions    => 8 -- chose this based on laptop’s CPU cores
);

ALTER TABLE stock_ticks SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol',
  timescaledb.compress_orderby   = 'time'
);

CREATE TABLE IF NOT EXISTS articles (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(5) NOT NULL REFERENCES profiles(symbol),
                industry TEXT,
                title TEXT,
                content TEXT,
                author TEXT, 
                date TIMESTAMP, 
                url TEXT,
                source TEXT
            );

CREATE TABLE IF NOT EXISTS article_chunks (
    article_id SERIAL NOT NULL REFERENCES articles(id),
    chunk_text TEXT,
    embedding vector(384)
)

CREATE INDEX IF NOT EXISTS idx_article_embedding_hnsw
ON article_chunks
USING hnsw (embedding vector_cosine_ops);


CREATE TABLE IF NOT EXISTS sec_filings (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    INDUSTRY TEXT,
    filing_type VARCHAR(20) NOT NULL,
    filing_date DATE,
    filing_id VARCHAR(100) NOT NULL,
    content TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sec_filing_chunks (
    id SERIAL PRIMARY KEY,
    filing_id INTEGER REFERENCES sec_filings(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL,
    token_count INTEGER,
    embedding vector({})
);

CREATE INDEX IF NOT EXISTS idx_embedding_hnsw 
ON sec_filing_chunks 
USING hnsw (embedding vector_cosine_ops);

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
