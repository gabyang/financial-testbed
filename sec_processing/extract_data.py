from llama_cloud_services import LlamaExtract
import csv
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
import os
from dotenv import load_dotenv
from models import SecExtract

load_dotenv()

# Initialize client
extractor = LlamaExtract(api_key=os.getenv('LLAMA_CLOUD_API_KEY'))
sec_agent = extractor.get_agent(name="sec-extractor")

def process_file(file_path: str, symbol: str) -> Optional[dict]:
    try:
        extraction_run = sec_agent.extract(str(file_path))
        result = extraction_run.data
        
        new_result = {}
        for key, value in result.items():
            if key == 'filing_period':
                filing_date = datetime.strptime(value, '%Y-%m-%d')
                start_date = (filing_date - timedelta(days=90)).strftime('%Y-%m-%d')
                new_result['start_date'] = start_date
            new_result[key] = value
        
        return new_result
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None


def store_to_csv(root_dir: str, csv_file: str = "sec_extracts.csv"):
    """
    Traverse directory and process all full-submission.txt files
    Directory structure: /{symbol}/10-Q/{long_string}/full-submission.txt
    """
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
                    
                    print(f"Processing: Symbol={symbol}, Filing={filing_dir.name}")
                    
                    result = process_file(submission_file, symbol)
                    
                    if result:
                        if writer is None:
                            writer = csv.DictWriter(csvfile, fieldnames=result.keys())
                            if not file_exists:
                                writer.writeheader()
                                file_exists = True
                        
                        writer.writerow(result)
                        processed_count += 1
                        print(f"✓ Successfully processed {submission_file}")
                    else:
                        error_count += 1
                        print(f"✗ Failed to process {submission_file}")
    
    except Exception as e:
        print(f"Error during processing: {e}")
    
    finally:
        print("\n=== Processing Summary ===")
        print(f"Total files processed successfully: {processed_count}")
        print(f"Total files failed: {error_count}")
        print(f"Results saved to: {csv_file}")


input_directory = "/path/to/parent/directory"
store_to_csv(input_directory)

