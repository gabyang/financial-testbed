CREATE TABLE IF NOT EXISTS stock_metadata (
    symbol VARCHAR(5) PRIMARY KEY,
    name TEXT
);

INSERT INTO stock_metadata ('AAPL', 'APPLE')


CREATE TABLE IF NOT EXISTS stock_ticks (
                time TIMESTAMPTZ PRIMARY KEY,
                symbol VARCHAR(5) NOT NULL REFERENCES stock_metadata(symbol),
                open INT NOT NULL,
                high INT NOT NULL,
                low INT NOT NULL,
                close INT NOT NULL,
                volume INT NOT NULL
            );
            
SELECT create_hypertable('stock_ticks', 'time');


CREATE TABLE IF NOT EXISTS articles (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(5) NOT NULL REFERENCES stock_metadata(symbol),
                title TEXT,
                content TEXT,
                author TEXT, 
                date TIMESTAMP, 
                url TEXT,
                source TEXT
            );

CREATE TABLE IF NOT EXISTS article_chunks (
    article_id VARCHAR(5) NOT NULL REFERENCES articles(id),
    chunk_text TEXT,
    embedding vector(384)
)