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

def store_in_postgres(article_data, chunks, embeddings):
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
            'AAPL',
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
        print(f"Successfully stored article {article_id} with {len(chunks)} chunks")
        
    except Exception as e:
        print(f"Error storing in database: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def process_news_files():
    """Process all news HTML files in the directory."""
    news_dir = 'test_data/news'
    
    # Initialize the MTEB model
    print("Loading MTEB model...")

    # It is receommended for the laptop to have a 16GB ram for this model GG
    model = SentenceTransformer("all-MiniLM-L6-v2")
    
    # Ensure the articles table exists with pgvector extension
    # conn = psycopg2.connect(**DB_CONFIG)
    # cur = conn.cursor()
    
    # try:
    #     conn.commit()
    # finally:
    #     cur.close()
    #     conn.close()
    
    # Process each HTML file
    for filename in os.listdir(news_dir):
        # if filename.endswith('.html'):

        file_path = os.path.join(news_dir, filename)
        print(f"Processing {filename}...")
        
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
        
        # print("embeddings: ", embeddings)
        print("chunks: ", chunks)
        print("\n\n\n")
        # Store in database
        store_in_postgres(article_data, chunks, embeddings)

if __name__ == "__main__":
    print("Starting script...")
    process_news_files()
