import pandas as pd
import psycopg2
from psycopg2 import pool
import datetime
import json
import logging
import os
import argparse
import concurrent.futures
from tqdm import tqdm
import time
import yaml
from datetime import timedelta
import numpy as np
from sentence_transformers import SentenceTransformer
from langchain_community.chat_models import ChatOllama
from langchain_core.messages import HumanMessage
from sentence_transformers import CrossEncoder

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stock_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StockAnalyzer:
    def __init__(self, config_path="config.yaml"):
        """Initialize the StockAnalyzer with configuration"""
        self.load_config(config_path)
        self.setup_connection_pool()
        self.setup_models()
        self.processed_count = 0
        self.failed_items = []
        self.cross_encoder = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')
        
    def load_config(self, config_path):
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            # Default configuration if file not found
            self.config = {
                "database": {
                    "host": "localhost",
                    "database": "financial_db",
                    "user": "postgres",
                    "password": "postgres",
                    "min_connections": 5,
                    "max_connections": 20
                },
                "models": {
                    "embedding": "all-MiniLM-L6-v2",
                    "llm": "llama3.2"
                },
                "processing": {
                    "max_workers": 4,
                    "batch_size": 50,
                    "top_n_chunks": 5,
                    "top_n_articles": 5
                },
                "output": {
                    "directory": "summaries",
                    "use_database": False
                }
            }
            # Save default config for future reference
            os.makedirs(os.path.dirname(config_path) or '.', exist_ok=True)
            with open(config_path, 'w') as file:
                yaml.dump(self.config, file)
            logger.info(f"Created default configuration at {config_path}")
    
    def setup_connection_pool(self):
        """Set up a connection pool for database access"""
        db_config = self.config["database"]
        try:
            self.conn_pool = pool.ThreadedConnectionPool(
                db_config["min_connections"],
                db_config["max_connections"],
                host=db_config["host"],
                database=db_config["database"],
                user=db_config["user"],
                password=db_config["password"]
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to create connection pool: {e}")
            raise
    
    def setup_models(self):
        """Initialize and cache the embedding and LLM models"""
        try:
            logger.info("Loading embedding model...")
            self.embedding_model = SentenceTransformer(self.config["models"]["embedding"])
            
            logger.info("Setting up LLM model...")
            self.llm = ChatOllama(model=self.config["models"]["llm"], temperature=0)
            
            logger.info("Models loaded successfully")
        except Exception as e:
            logger.error(f"Error loading models: {e}")
            raise
    
    def get_embedding(self, text):
        """Generate embedding using the embedding model"""
        return self.embedding_model.encode(text)
    
    def get_db_connection(self):
        """Get a connection from the pool"""
        return self.conn_pool.getconn()
    
    def release_connection(self, conn):
        """Return a connection to the pool"""
        self.conn_pool.putconn(conn)
    
    def get_company_profile(self, symbol, conn):
        """Retrieve company profile information from the profiles table"""
        cur = conn.cursor()
        try:
            query = """
            SELECT 
                symbol, companyName, sector, industry, description, 
                ceo
            FROM profiles
            WHERE symbol = %s
            """
            cur.execute(query, (symbol,))
            profile = cur.fetchone()
            
            if profile:
                profile_dict = {
                    "symbol": profile[0],
                    "companyName": profile[1],
                    "sector": profile[2],
                    "industry": profile[3],
                    "description": profile[4],
                    "ceo": profile[5]
                }
                logger.debug(f"Retrieved profile for {symbol}")
                return profile_dict
            else:
                logger.warning(f"No profile found for {symbol}")
                return None
        except Exception as e:
            logger.error(f"Error retrieving company profile for {symbol}: {e}")
            return None
        finally:
            cur.close()
    
    def get_relevant_articles(self, symbol, window_start, conn, top_n=None):
        """
        Retrieve the top article chunks relevant to the stock's movement within a 30-day window.
        """
        if top_n is None:
            top_n = self.config["processing"]["top_n_articles"]
            
        cur = conn.cursor()
        
        try:
            # Convert the query window to datetime
            window_start = pd.to_datetime(window_start) - pd.Timedelta(days=14)
            window_end = pd.to_datetime(window_start)  + pd.Timedelta(days=14)

            query_text = f"news related to {symbol} stock price movement, earnings, outlook, analyst reactions"

            # Step 1: Vector search using pgvector
            embedding = self.get_embedding(query_text)
            embedding_str = "[" + ",".join(map(str, embedding)) + "]"

            cur.execute("""
                SELECT a.industry, a.title, a.date, a.source, c.chunk_text,
                    1 - (c.embedding <=> %s::vector) as similarity
                FROM article_chunks c
                JOIN articles a ON c.article_id = a.id
                WHERE a.industry = %s
                AND a.date BETWEEN %s AND %s
                ORDER BY similarity DESC
                LIMIT %s
            """, (embedding_str, symbol, window_start, window_end, 20))  # fetch more for re-ranking

            candidates = cur.fetchall()
            if not candidates:
                return []

            # Step 2: Re-rank using CrossEncoder
            texts_to_score = [(query_text, c[4]) for c in candidates]
            rerank_scores = self.cross_encoder.predict(texts_to_score)

            # Sort and return top-N
            scored_candidates = sorted(zip(candidates, rerank_scores), key=lambda x: -x[1])
            return [item[0] for item in scored_candidates[:top_n]]
        except Exception as e:
            logger.error(f"Error getting relevant articles for {symbol}: {e}")
            return []
        finally:
            cur.close()
    
    def get_relevant_filings(self, symbol, date_str, conn):
        """Find relevant 10-Q filings for the current and previous quarters"""
        cur = conn.cursor()
        
        try:
            # Parse the date from the input
            target_date = pd.to_datetime(date_str) + pd.Timedelta(days=100)
            
            date_range_start = pd.to_datetime(date_str)
            
            # Query to get the most recent 10-Q filings before the target date
            query = """
            SELECT id, industry, filing_type, filing_date, filing_id
            FROM sec_filings
            WHERE industry = %s
              AND filing_type = '10-Q'
              AND filing_date BETWEEN %s AND %s
            ORDER BY filing_date ASC
            """
            
            cur.execute(query, (symbol, date_range_start, target_date))
            filings = cur.fetchall()
            
            logger.debug(f"Found {len(filings)} 10-Q filings for {symbol}")
            return filings
        except Exception as e:
            logger.error(f"Error getting filings for {symbol}: {e}")
            return []
        finally:
            cur.close()

    def get_top_chunks(self, filing_ids, pct_change, conn, top_n=None):
        if top_n is None:
            top_n = self.config["processing"]["top_n_chunks"]
        if not filing_ids:
            return []

        cur = conn.cursor()
        try:
            movement_type = "decrease" if pct_change < 0 else "increase"
            query_text = f"industry price {movement_type} of {abs(pct_change):.2f}% factors explanation reasons"

            embedding = self.get_embedding(query_text)
            embedding_str = "[" + ",".join(map(str, embedding)) + "]"

            # Step 1: Vector search - fetch more for re-ranking
            cur.execute("""
                SELECT c.id, c.content, f.filing_date, f.filing_type,
                    1 - (c.embedding <=> %s::vector) as similarity
                FROM sec_filing_chunks c
                JOIN sec_filings f ON c.filing_id = f.id
                WHERE f.id IN %s
                ORDER BY similarity DESC
                LIMIT %s
            """, (embedding_str, tuple(filing_ids), 20))

            candidates = cur.fetchall()
            if not candidates:
                return []

            # Step 2: Cross-encoder re-ranking
            texts_to_score = [(query_text, c[1]) for c in candidates]
            rerank_scores = self.cross_encoder.predict(texts_to_score)

            # Sort and return top-N
            scored_chunks = sorted(zip(candidates, rerank_scores), key=lambda x: -x[1])
            return [item[0] for item in scored_chunks[:top_n]]
        except Exception as e:
            logger.error(f"Error getting top SEC chunks: {e}")
            return []
        finally:
            cur.close()


    def generate_llm_summary(self, chunks, article_chunks, stock_data, company_profile):
        """Generate a summary using LLM model with company profile information"""
        # Prepare the prompt with context and chunks
        industry = stock_data['industry']
        pct_change = stock_data['pct_change']
        window = stock_data['window']
        date = stock_data['window_start']
        
        # Include average closing prices if available - divide by 100 for correct price representation
        prev_avg_close = stock_data.get('prev_avg_close')
        avg_close = stock_data.get('avg_close')
        
        
        prev_avg_close = float(prev_avg_close) / 100
        avg_close = float(avg_close) / 100
        
        
        prompt = f"""
        You are analyzing SEC filings & news articles for {industry}.
            
        INDUSTRY MOVEMENT:
        - Previous Window Avg Close: ${stock_data.get('prev_avg_close', 'N/A')}
        - Current Window Avg Close: ${stock_data.get('avg_close', 'N/A')}
        - Percent Change: {stock_data['pct_change']:.2f}% {'decrease' if stock_data['pct_change'] < 0 else 'increase'}
        - Window: {stock_data['window']} starting {stock_data['window_start']}
        
        Analyze the following SEC filing & news chunks and explain the factors that likely contributed to this industry price movement:
        
        SEC FILING CHUNKS:
        """
        
        for i, chunk in enumerate(chunks):
            prompt += f"\n--- CHUNK {i+1} (Filing Date: {chunk[2]}, Type: {chunk[3]}) ---\n{chunk[1]}\n"
        
        prompt += "\n\nNEWS ARTICLES:\n"
        for i, art in enumerate(article_chunks):
            prompt += f"\n--- ARTICLE {i+1} ({art[2].strftime('%Y-%m-%d')}, {art[3]}) ---\nTitle: {art[1]}\n{art[4]}\n"
        
        prompt += "\n Based on industry price data, SEC filing excerpts, and news articles, provide a comprehensive analysis of:"
        prompt += "\n1. News articles that reflect the change in industry price"
        prompt += "\n2. Financial results and metrics that impacted investor sentiment. Provide the date and statistic."
        prompt += "\n3. Market conditions or industry trends that affected the industry"
        prompt += "\n4. Forward guidance or outlook changes"
        prompt += "\n5. Any other material information that could explain the price change"
        
        # Use LLM to generate summary
        try:
            max_retries = 3
            retry_delay = 2  # seconds
            
            for attempt in range(max_retries):
                try:
                    response = self.llm.invoke([HumanMessage(content=prompt)])
                    return response.content
                except Exception as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"LLM request failed, retrying in {retry_delay} seconds: {e}")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        raise
        except Exception as e:
            error_msg = f"Error generating summary after {max_retries} attempts: {str(e)}"
            logger.error(error_msg)
            return f"ERROR: {error_msg}"

    def save_summary(self, stock_data, summary, company_profile=None):
        """Save the generated summary either to a file or database"""
        industry = stock_data['industry']
        date_str = pd.to_datetime(stock_data['window_start']).strftime('%Y%m%d')
        
        if self.config["output"]["use_database"]:
            # Implementation for saving to database would go here
            conn = self.get_db_connection()
            try:
                cur = conn.cursor()
                # Example query - adjust as needed for your schema
                query = """
                INSERT INTO stock_summaries (
                    symbol, analysis_date, pct_change, prev_avg_close, avg_close,
                    company_name, sector, industry, summary, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (symbol, analysis_date)
                DO UPDATE SET 
                    summary = %s, 
                    prev_avg_close = %s,
                    avg_close = %s,
                    updated_at = NOW()
                """
                
                company_name = company_profile['companyName'] if company_profile else stock_data.get('companyName', industry)
                sector = company_profile['sector'] if company_profile else stock_data.get('sector', 'N/A')
                industry = company_profile['industry'] if company_profile else stock_data.get('industry', 'N/A')
                prev_avg_close = stock_data.get('prev_avg_close')
                avg_close = stock_data.get('avg_close')
                
                cur.execute(query, (
                    industry, stock_data['window_start'], stock_data['pct_change'], 
                    prev_avg_close, avg_close, company_name, sector, industry, summary,
                    summary, prev_avg_close, avg_close
                ))
                conn.commit()
                logger.info(f"Summary for {industry} saved to database")
            except Exception as e:
                logger.error(f"Error saving summary to database: {e}")
            finally:
                cur.close()
                self.release_connection(conn)
        else:
            # Save to file
            output_dir = self.config["output"]["directory"]
            os.makedirs(output_dir, exist_ok=True)
            output_file = os.path.join(output_dir, f"summary_{industry}_{date_str}.txt")
            
            # Create a more comprehensive header for the summary file
            header = f"INDUSTRY ANALYSIS: {industry}\n"
            header += f"Date Range: {stock_data['window_start']} ({stock_data['window']})\n"
            header += f"Price Change: {stock_data['pct_change']:.2f}%\n"
            
            if 'prev_avg_close' in stock_data and 'avg_close' in stock_data:
                header += f"Previous Avg Close: ${stock_data['prev_avg_close']/100}\n"
                header += f"Current Avg Close: ${stock_data['avg_close']/100}\n"
            
            if company_profile:
                header += f"\nCOMPANY: {company_profile['companyName']}\n"
                header += f"Sector: {company_profile['sector']}\n"
                header += f"Industry: {company_profile['industry']}\n"
                if company_profile.get('marketCap'):
                    header += f"Market Cap: {company_profile['marketCap']}\n"
                if company_profile.get('ceo'):
                    header += f"CEO: {company_profile['ceo']}\n"
            
            header += "\n" + "="*50 + "\n\nANALYSIS:\n\n"
            full_content = header + summary
            
            try:
                with open(output_file, 'w') as f:
                    f.write(full_content)
                logger.info(f"Summary saved to {output_file}")
            except Exception as e:
                logger.error(f"Error saving summary to file: {e}")

    def process_stock(self, stock_data):
        """Process a single stock data row"""
        industry = stock_data['industry']
        logger.info(f"Processing {industry}" )
        
        conn = None
        try:
            conn = self.get_db_connection()
            
            # Get company profile
            company_profile = self.get_company_profile(industry, conn)
            if company_profile:
                logger.info(f"Retrieved company profile for {industry}")
            else:
                logger.warning(f"No company profile found for {industry}")
            
            # Get relevant SEC filings
            filings = self.get_relevant_filings(industry, stock_data['window_start'], conn)
            
            if not filings:
                logger.warning(f"No relevant 10-Q filings found for {industry}")
                return {"industry": industry, "status": "no_filings"}
            
            # Extract filing IDs
            filing_ids = [filing[0] for filing in filings]
            
            # Get the most relevant chunks
            top_chunks = self.get_top_chunks(filing_ids, stock_data['pct_change'], conn)
            
            if not top_chunks:
                logger.warning(f"No relevant chunks found for {industry}")
                return {"symbol": industry, "status": "no_chunks"}
            
            # Get top relevant article chunks
            article_chunks = self.get_relevant_articles(industry, stock_data['window_start'], conn)
            
            # Generate summary with company profile information
            summary = self.generate_llm_summary(top_chunks, article_chunks, stock_data, company_profile)
            
            # Save the summary
            self.save_summary(stock_data, summary, company_profile)
            
            return {"industry": industry, "status": "success"}
            
        except Exception as e:
            logger.error(f"Error processing {industry}: {e}")
            return {"industry": industry, "status": "error", "error": str(e)}
        finally:
            if conn:
                self.release_connection(conn)

    def process_batch(self, batch):
        """Process a batch of stocks sequentially"""
        results = []
        for stock_data in batch:
            result = self.process_stock(stock_data)
            results.append(result)
        return results

    def process_csv(self, csv_file, start_index=0, end_index=None, max_workers=None):
        """Process stocks from a CSV file with parallel execution"""
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file)
            logger.info(f"Loaded {len(df)} records from {csv_file}")
            
            # Apply row filtering if specified
            if end_index is None:
                end_index = len(df)
            
            df_subset = df.iloc[start_index:end_index]
            logger.info(f"Processing rows {start_index} to {end_index-1} ({len(df_subset)} rows)")
            
            if max_workers is None:
                max_workers = self.config["processing"]["max_workers"]
                
            batch_size = self.config["processing"]["batch_size"]
            
            # Create batches
            batches = [df_subset.iloc[i:i+batch_size].to_dict('records') 
                      for i in range(0, len(df_subset), batch_size)]
            
            # Process batches in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(self.process_batch, batch) for batch in batches]
                
                # Show progress bar
                for future in tqdm(concurrent.futures.as_completed(futures), 
                                  total=len(futures), 
                                  desc="Processing batches"):
                    batch_results = future.result()
                    for result in batch_results:
                        if result["status"] != "success":
                            self.failed_items.append(result)
                        self.processed_count += 1
            
            # Report results
            logger.info(f"Processed {self.processed_count} stocks")
            logger.info(f"Failed items: {len(self.failed_items)}")
            
            # Save failed items for retry
            if self.failed_items:
                failed_file = "failed_items.json"
                with open(failed_file, 'w') as f:
                    json.dump(self.failed_items, f)
                logger.info(f"Failed items saved to {failed_file}")
                
        except Exception as e:
            logger.error(f"Error processing CSV: {e}")
            raise
        finally:
            # Close the connection pool
            if hasattr(self, 'conn_pool'):
                self.conn_pool.closeall()
                logger.info("Closed all database connections")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Process stock data and generate summaries')
    parser.add_argument('csv_file', help='Path to the CSV file containing stock data')
    parser.add_argument('--config', default='config.yaml', help='Path to configuration file')
    parser.add_argument('--start', type=int, default=0, help='Starting row index to process')
    parser.add_argument('--end', type=int, help='Ending row index to process')
    parser.add_argument('--workers', type=int, help='Number of worker threads')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Set logging level based on args
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize analyzer
    analyzer = StockAnalyzer(args.config)
    
    # Process the CSV file
    analyzer.process_csv(
        args.csv_file,
        start_index=args.start,
        end_index=args.end,
        max_workers=args.workers
    )

if __name__ == "__main__":
    main()