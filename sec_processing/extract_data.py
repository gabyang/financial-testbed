from llama_cloud_services import LlamaExtract
import csv
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
import os
from dotenv import load_dotenv
from models import SecExtract
import time

load_dotenv()

# Initialize client
extractor = LlamaExtract(api_key=os.getenv('LLAMA_CLOUD_API_KEY'))
sec_agent = extractor.get_agent(name="sec-extractor")

def process_file(file_path: str, symbol: str) -> Optional[dict]:
    try:
        start_time = time.time()
        
        # Extract data
        extraction_start = time.time()
        extraction_run = sec_agent.extract(str(file_path))
        result = extraction_run.data
        extraction_time = time.time() - extraction_start
        print(f"Data extraction completed (took {extraction_time:.2f}s)")
        
        # Process the result
        processing_start = time.time()
        new_result = {}
        for key, value in result.items():
            if key == 'filing_period':
                filing_date = datetime.strptime(value, '%Y-%m-%d')
                start_date = (filing_date - timedelta(days=90)).strftime('%Y-%m-%d')
                new_result['start_date'] = start_date
            new_result[key] = value
        
        processing_time = time.time() - processing_start
        total_time = time.time() - start_time
        print(f"Data processing completed (took {processing_time:.2f}s)")
        print(f"Total file processing time: {total_time:.2f}s")
        
        return new_result
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None


def store_to_csv(root_dir: str, csv_file: str = "sec_extracts.csv"):
    """
    Traverse directory and process all full-submission.txt files
    Directory structure: /{symbol}/10-Q/{long_string}/full-submission.txt
    """
    start_time = time.time()
    root_path = Path(root_dir)
    processed_count = 0
    error_count = 0
    
    file_exists = os.path.isfile(csv_file)
    
    try:
        with open(csv_file, mode='a', newline='') as csvfile:
            writer = None
            
            for symbol_dir in root_path.iterdir():
                if not symbol_dir.is_dir():
                    continue
                    
                symbol = symbol_dir.name
                quarterly_dir = symbol_dir / "10-Q"
                
                if not quarterly_dir.exists():
                    continue
                
                for filing_dir in quarterly_dir.iterdir():
                    if not filing_dir.is_dir():
                        continue
                        
                    submission_file = filing_dir / "full-submission.txt"
                    if not submission_file.exists():
                        continue
                    
                    file_start_time = time.time()
                    print(f"\nProcessing: Symbol={symbol}, Filing={filing_dir.name}")
                    
                    result = process_file(submission_file, symbol)
                    
                    if result:
                        if writer is None:
                            writer = csv.DictWriter(csvfile, fieldnames=result.keys())
                            if not file_exists:
                                writer.writeheader()
                                file_exists = True
                        
                        writer.writerow(result)
                        processed_count += 1
                        file_elapsed_time = time.time() - file_start_time
                        print(f"✓ Successfully processed {submission_file}")
                        print(f"Total time for this file (including CSV write): {file_elapsed_time:.2f}s")
                    else:
                        error_count += 1
                        print(f"✗ Failed to process {submission_file}")
    
    except Exception as e:
        print(f"Error during processing: {e}")
    
    finally:
        total_time = time.time() - start_time
        print("\n=== Processing Summary ===")
        print(f"Total files processed successfully: {processed_count}")
        print(f"Total files failed: {error_count}")
        print(f"Results saved to: {csv_file}")
        print(f"Total processing time: {total_time:.2f}s")
        if processed_count > 0:
            print(f"Average time per successful file: {(total_time/processed_count):.2f}s")
        print(f"Started at: {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Finished at: {datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    input_directory = "/Users/gabriel.yang/test/financial-testbed/test_data/sec"
    store_to_csv(input_directory)

