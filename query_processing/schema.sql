CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE profiles (
    symbol VARCHAR(5) PRIMARY KEY,
    price NUMERIC,
    beta NUMERIC,
    volAvg BIGINT,
    mktCap NUMERIC,
    lastDiv NUMERIC,
    "range" TEXT,
    changes NUMERIC,
    companyName TEXT,
    currency VARCHAR(10),
    cik TEXT,
    isin VARCHAR(15),
    cusip VARCHAR(15),
    exchange TEXT,
    exchangeShortName VARCHAR(50),
    industry TEXT,
    website TEXT,
    description TEXT,
    ceo TEXT,
    sector TEXT,
    country VARCHAR(5),
    fullTimeEmployees TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    state VARCHAR(10),
    zip VARCHAR(10),
    dcfDiff NUMERIC,
    dcf NUMERIC,
    image TEXT,
    ipoDate DATE,
    defaultImage BOOLEAN,
    isEtf BOOLEAN,
    isActivelyTrading BOOLEAN,
    isAdr BOOLEAN,
    isFund BOOLEAN
);

CREATE TABLE IF NOT EXISTS stock_ticks (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(5) NOT NULL REFERENCES profiles(symbol),
    open INT NOT NULL,
    high INT NOT NULL,
    low INT NOT NULL,
    close INT NOT NULL,
    volume INT NOT NULL,
    PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('stock_ticks', 'time');


CREATE TABLE IF NOT EXISTS articles (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(5) NOT NULL REFERENCES profiles(symbol),
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

-- Speed up symbol/date filtering
CREATE INDEX IF NOT EXISTS idx_articles_symbol_date
ON articles(symbol, date);


-- Speed up similarity search (HNSW is ideal for pgvector)
CREATE INDEX IF NOT EXISTS idx_article_embedding_hnsw
ON article_chunks
USING hnsw (embedding vector_cosine_ops);

-- Speed up joins with articles
CREATE INDEX IF NOT EXISTS idx_article_chunks_article_id
ON article_chunks(article_id);

CREATE INDEX IF NOT EXISTS idx_embedding_hnsw 
ON sec_filing_chunks 
USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS sec_filings (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    filing_type VARCHAR(20) NOT NULL,
    filing_date DATE,
    filing_id VARCHAR(100) NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS idx_filings_symbol_type 
ON sec_filings(symbol, filing_type);

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

-- Optimize estimates
CREATE INDEX IF NOT EXISTS idx_estimates_symbol_fiscal ON historic_estimates(symbol, fiscal_date_ending);

-- Optimize stock price lookups
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_time ON stock_ticks(symbol, time);

-- Optional: If you’re joining to a filings table
CREATE INDEX IF NOT EXISTS idx_sec_symbol_period ON sec_filings(symbol, filing_period);