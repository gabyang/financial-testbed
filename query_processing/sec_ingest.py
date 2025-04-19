import re
import os
import time
import psycopg2
import numpy as np
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path
from datetime import datetime
from psycopg2.extras import execute_values, Json

# For generating embeddings - using sentence-transformers
from sentence_transformers import SentenceTransformer

# Database configuration
DB_CONFIG = {
    "dbname": "financial_db",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

# Chunking configuration
CHUNK_SIZE = 1000  # Characters per chunk
CHUNK_OVERLAP = 200  # Character overlap between chunks

# Model for embeddings
EMBEDDING_MODEL = "all-MiniLM-L6-v2"  # A smaller model, good balance of speed and quality
EMBEDDING_DIMENSION = 384  # Dimension for the all-MiniLM-L6-v2 model

class SECFilingsProcessor:
    def __init__(self, db_config=None):
        """Initialize the SEC filings processor with database configuration."""
        self.db_config = db_config or DB_CONFIG
        self.model = SentenceTransformer(EMBEDDING_MODEL)
        self.conn = None
        self.cursor = None
    
    def connect_to_db(self):
        """Establish connection to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("Successfully connected to PostgreSQL database")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
    
    def setup_database(self):
        """Create the necessary tables and extensions if they don't exist."""
        if not self.conn:
            self.connect_to_db()
        
        try:
            # Enable pgvector extension
            self.cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            
            # Create table for storing complete SEC filings
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS sec_filings (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    filing_type VARCHAR(20) NOT NULL,
                    filing_date DATE,
                    filing_id VARCHAR(100) NOT NULL,
                    content TEXT NOT NULL,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Create index on symbol and filing_type
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_filings_symbol_type 
                ON sec_filings(symbol, filing_type);
            """)
            
            # Create table for storing chunks of SEC filings
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS sec_filing_chunks (
                    id SERIAL PRIMARY KEY,
                    filing_id INTEGER REFERENCES sec_filings(id) ON DELETE CASCADE,
                    chunk_index INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    token_count INTEGER,
                    embedding vector({}),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """.format(EMBEDDING_DIMENSION))
            
            # Create vector index for similarity search
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_embedding_hnsw 
                ON sec_filing_chunks 
                USING hnsw (embedding vector_cosine_ops);
            """)
            
            self.conn.commit()
            print("Database setup completed successfully")
        except Exception as e:
            self.conn.rollback()
            print(f"Error setting up database: {e}")
            raise
    
    def chunk_text(self, text: str) -> List[str]:
        """
        Split text into chunks with overlap.
        
        Args:
            text: The text to be chunked
            
        Returns:
            List of text chunks
        """
        chunks = []
        start = 0
        text_length = len(text)
        
        # Skip empty or very small texts
        if text_length < 50:
            return [text] if text.strip() else []
            
        while start < text_length:
            end = min(start + CHUNK_SIZE, text_length)
            
            # If we're not at the end, try to find a good breaking point
            if end < text_length:
                # Look for the last period or newline within the last 20% of the chunk
                search_range_start = max(start + int(CHUNK_SIZE * 0.8), start)
                last_period = text.rfind('.', search_range_start, end)
                last_newline = text.rfind('\n', search_range_start, end)
                
                # Use the latest good breaking point
                if last_period > search_range_start:
                    end = last_period + 1
                elif last_newline > search_range_start:
                    end = last_newline + 1
            
            # Extract the chunk
            chunk = text[start:end].strip()
            if chunk:  # Only add non-empty chunks
                chunks.append(chunk)
            
            # Move to the next chunk with overlap
            start = end - CHUNK_OVERLAP if end < text_length else text_length
        
        return chunks
    
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate embedding vector for a text chunk.
        
        Args:
            text: The text to generate embedding for
            
        Returns:
            Embedding vector as a list of floats
        """
        try:
            # Generate embedding and convert to Python list
            embedding = self.model.encode(text)
            return embedding.tolist()
        except Exception as e:
            print(f"Error generating embedding: {e}")
            # Return a zero vector as fallback
            return [0.0] * EMBEDDING_DIMENSION
    
    def store_filing(self, symbol: str, filing_type: str, filing_id: str, content: str, 
                    filing_date: Optional[datetime] = None, metadata: Optional[Dict] = None) -> int:
        """
        Store a complete SEC filing in the database.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            filing_type: Type of filing (e.g., '10-Q')
            filing_id: Unique identifier for the filing
            content: Full text content of the filing
            filing_date: Date of the filing
            metadata: Additional metadata as a dictionary
            
        Returns:
            ID of the inserted filing record
        """
        if not self.conn:
            self.connect_to_db()
        
        try:
            self.cursor.execute("""
                INSERT INTO sec_filings 
                (symbol, filing_type, filing_date, filing_id, content, metadata) 
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (symbol, filing_type, filing_date, filing_id, content, Json(metadata) if metadata else None))
            
            filing_record_id = self.cursor.fetchone()[0]
            self.conn.commit()
            return filing_record_id
        except Exception as e:
            self.conn.rollback()
            print(f"Error storing filing: {e}")
            raise
    
    def process_and_store_chunks(self, filing_record_id: int, content: str) -> int:
        """
        Process a filing into chunks, generate embeddings, and store in the database.
        
        Args:
            filing_record_id: ID of the filing record in the database
            content: Full text content to chunk and embed
            
        Returns:
            Number of chunks stored
        """
        if not self.conn:
            self.connect_to_db()
        
        try:
            # Chunk the content
            chunks = self.chunk_text(content)
            
            if not chunks:
                print(f"Warning: No valid chunks generated for filing ID {filing_record_id}")
                return 0
            
            # Prepare data for batch insertion
            chunk_data = []
            for i, chunk in enumerate(chunks):
                # Generate embedding
                embedding = self.generate_embedding(chunk)
                # Approximate token count (rough estimate)
                token_count = len(chunk.split())
                
                chunk_data.append((filing_record_id, i, chunk, token_count, embedding))
            
            # Batch insert chunks with embeddings
            execute_values(
                self.cursor,
                """
                INSERT INTO sec_filing_chunks 
                (filing_id, chunk_index, content, token_count, embedding) 
                VALUES %s
                """,
                chunk_data,
                template="(%s, %s, %s, %s, %s)",
                page_size=100
            )
            
            self.conn.commit()
            return len(chunks)
        except Exception as e:
            self.conn.rollback()
            print(f"Error processing and storing chunks: {e}")
            raise
    
    def search_similar_chunks(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Search for chunks similar to the query.
        
        Args:
            query: Search query text
            limit: Maximum number of results to return
            
        Returns:
            List of dictionaries containing search results
        """
        if not self.conn:
            self.connect_to_db()
        
        try:
            # Generate embedding for the query
            query_embedding = self.generate_embedding(query)
            
            # Perform similarity search
            self.cursor.execute("""
                SELECT 
                    c.id, 
                    c.content, 
                    c.chunk_index,
                    f.symbol, 
                    f.filing_type, 
                    f.filing_date,
                    f.filing_id,
                    1 - (c.embedding <=> %s) as similarity
                FROM 
                    sec_filing_chunks c
                JOIN 
                    sec_filings f ON c.filing_id = f.id
                ORDER BY 
                    c.embedding <=> %s
                LIMIT %s;
            """, (query_embedding, query_embedding, limit))
            
            results = []
            for row in self.cursor.fetchall():
                results.append({
                    'chunk_id': row[0],
                    'content': row[1],
                    'chunk_index': row[2],
                    'symbol': row[3],
                    'filing_type': row[4],
                    'filing_date': row[5],
                    'filing_id': row[6],
                    'similarity': row[7]
                })
            
            return results
        except Exception as e:
            print(f"Error during similarity search: {e}")
            return []
    
    def process_directory(self, root_dir: str):
        """
        Process all preprocessed full-submission.txt files and store them in the database.
        Directory structure: /{symbol}/10-Q/{long_string}/full-submission.txt
        
        Args:
            root_dir: Path to the root directory containing SEC filings
        """
        start_time = time.time()
        root_path = Path(root_dir)
        processed_count = 0
        error_count = 0
        
        try:
            # Traverse directory
            for symbol_dir in root_path.iterdir():
                if not symbol_dir.is_dir():
                    continue
                    
                symbol = symbol_dir.name
                
                for filing_type_dir in symbol_dir.iterdir():
                    if not filing_type_dir.is_dir():
                        continue
                    
                    filing_type = filing_type_dir.name  # e.g., "10-Q"
                    
                    for filing_dir in filing_type_dir.iterdir():
                        if not filing_dir.is_dir():
                            continue
                        
                        filing_id = filing_dir.name
                        submission_file = filing_dir / "full-submission.txt"
                        
                        if not submission_file.exists():
                            continue
                        
                        file_start_time = time.time()
                        print(f"\nProcessing: Symbol={symbol}, FilingType={filing_type}, FilingID={filing_id}")
                        
                        try:
                            # Read the preprocessed content
                            with open(str(submission_file), 'r', encoding='utf-8') as f:
                                content = f.read()
                            
                            if not content or len(content) < 100:
                                print(f"Warning: Content for {submission_file} is too short or empty. Skipping.")
                                error_count += 1
                                continue
                            
                            # Try to extract filing date from the directory name or use current date
                            try:
                                # Extract date if filing_id contains a date pattern like YYYY-MM-DD
                                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filing_id)
                                filing_date = datetime.strptime(date_match.group(1), '%Y-%m-%d') if date_match else None
                            except:
                                filing_date = None
                            
                            # Store the complete filing
                            metadata = {
                                'file_path': str(submission_file),
                                'processing_date': datetime.now().isoformat()
                            }
                            
                            filing_record_id = self.store_filing(
                                symbol=symbol,
                                filing_type=filing_type,
                                filing_id=filing_id,
                                content=content,
                                filing_date=filing_date,
                                metadata=metadata
                            )
                            
                            # Process and store chunks with embeddings
                            chunk_count = self.process_and_store_chunks(filing_record_id, content)
                            
                            file_elapsed_time = time.time() - file_start_time
                            print(f"✓ Successfully processed {symbol} {filing_type} {filing_id}")
                            print(f"  - Created {chunk_count} chunks with embeddings")
                            print(f"  - Total processing time: {file_elapsed_time:.2f}s")
                            
                            processed_count += 1
                            
                        except Exception as e:
                            print(f"✗ Error processing {submission_file}: {e}")
                            error_count += 1
                
        except Exception as e:
            print(f"Error during directory processing: {e}")
        
        finally:
            total_time = time.time() - start_time
            print("\n=== Processing Summary ===")
            print(f"Total files processed successfully: {processed_count}")
            print(f"Total files failed: {error_count}")
            print(f"Total processing time: {total_time:.2f}s")
            if processed_count > 0:
                print(f"Average time per successful file: {(total_time/processed_count):.2f}s")
            print(f"Started at: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Finished at: {datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}")
    
    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("Database connection closed")


def main():
    # Database setup
    processor = SECFilingsProcessor()
    try:
        # Setup database tables and extensions
        processor.setup_database()
        
        # Process directory of SEC filings
        input_directory = "./test_data/SEC-filings"
        processor.process_directory(input_directory)
        
        # Example: Search for related chunks
        print("\n=== Example Search ===")
        query = "AAPL in 2013"
        results = processor.search_similar_chunks(query, limit=3)
        
        print(f"Top results for query: '{query}'")
        for i, result in enumerate(results, 1):
            print(f"\n{i}. {result['symbol']} {result['filing_type']} (Similarity: {result['similarity']:.4f})")
            print(f"   Date: {result['filing_date']}")
            print(f"   Chunk {result['chunk_index'] + 1}:")
            print(f"   {result['content'][:200]}...")
    
    finally:
        processor.close()


if __name__ == "__main__":
    main()