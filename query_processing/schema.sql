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



