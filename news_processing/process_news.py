import os
import trafilatura
from mteb import MTEB
from sentence_transformers import SentenceTransformer
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from datetime import datetime
import json
from langchain.text_splitter import RecursiveCharacterTextSplitter

# PostgreSQL configuration
DB_CONFIG = {
    'dbname': 'financial_db',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}

def extract_content(html_file):
    """Extract content from HTML file using Trafilatura."""
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Extract main content
    content = trafilatura.extract(html_content)
    if not content:
        print(f"Warning: Could not extract content from {html_file}")
        return None
    
    # Extract metadata
    metadata = trafilatura.extract_metadata(html_content)
    
    return {
        'content': content,
        'title': metadata.title if metadata else None,
        'author': metadata.author if metadata else None,
        'date': metadata.date if metadata else None,
        'url': metadata.url if metadata else None,
        'source': os.path.basename(html_file)
    }

def chunk_text(text):
    """Split text into chunks using RecursiveCharacterTextSplitter."""
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        length_function=len,
        separators=["\n\n", "\n", " ", ""]
    )
    
    chunks = text_splitter.split_text(text)
    return chunks

def create_embedding(text, model):
    """Create embedding using the specified MTEB model."""
    try:
        # Create embedding using the model
        embedding = model.encode(text, convert_to_numpy=True)
        return embedding.tolist()
    except Exception as e:
        print(f"Error creating embedding: {e}")
        return None

def store_in_postgres(symbol, article_data, chunks, embeddings):
    """Store article data and embeddings in PostgreSQL."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # Insert article data and get article_id
        cur.execute("""
            INSERT INTO articles (symbol, title, content, author, date, url, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            symbol,
            article_data['title'],
            article_data['content'],
            article_data['author'],
            article_data['date'],
            article_data['url'],
            article_data['source']
        ))
        
        article_id = cur.fetchone()[0]
        
        # Insert chunks and their embeddings
        for chunk, embedding in zip(chunks, embeddings):
            cur.execute("""
                INSERT INTO article_chunks (article_id, chunk_text, embedding)
                VALUES (%s, %s, %s)
            """, (article_id, chunk, embedding))
        
        conn.commit()
        print(f"Successfully stored article {article_id} with {len(chunks)} chunks under symbol {symbol}")
        
    except Exception as e:
        print(f"Error storing in database: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def process_news_files():
    """Process all news HTML files in the directory. 
       Each stock symbol folder contains a 'news' subfolder where all HTML files reside.
    """
    news_dir = 'test_data/News'
    
    # Initialize the MTEB model
    print("Loading MTEB model...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    
    # Iterate over each stock symbol folder
    for symbol in os.listdir(news_dir):
        symbol_path = os.path.join(news_dir, symbol)
        if os.path.isdir(symbol_path):
            # Look for the 'news' subfolder inside the symbol folder
            news_folder = os.path.join(symbol_path, 'news')
            if os.path.isdir(news_folder):
                print(f"Processing news for symbol: {symbol}")
                # Process each HTML file in the 'news' folder
                for filename in os.listdir(news_folder):
                    if filename.lower().endswith('.html'):
                        file_path = os.path.join(news_folder, filename)
                        print(f"Processing {filename} for symbol {symbol}...")
                        
                        # Extract content
                        article_data = extract_content(file_path)
                        if not article_data:
                            continue
                        
                        # Split content into chunks
                        chunks = chunk_text(article_data['content'])
                        print(f"Split article into {len(chunks)} chunks")
                        
                        # Create embeddings for each chunk
                        embeddings = []
                        for chunk in chunks:
                            embedding = create_embedding(chunk, model)
                            if embedding:
                                embeddings.append(embedding)
                        
                        if not embeddings:
                            print("Failed to create embeddings for any chunks")
                            continue
                        
                        # Store in database, including the stock symbol
                        store_in_postgres(symbol, article_data, chunks, embeddings)
            else:
                print(f"Warning: No 'news' folder found in {symbol_path}")
        else:
            print(f"Skipping non-directory entry: {symbol_path}")

if __name__ == "__main__":
    print("Starting script...")
    process_news_files()
