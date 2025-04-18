import re
import os
from typing import Optional
import sec_parser as sp
from pathlib import Path
import time
from datetime import datetime, timedelta


def extract_first_document_html(file_path: str) -> Optional[str]:
    """
    Extracts the content within the first <DOCUMENT>...</DOCUMENT> tag pair
    from a text file.

    Args:
        file_path: The path to the input text file.

    Returns:
        The content string between the first <DOCUMENT> and </DOCUMENT> tags,
        or None if the tags are not found or the file cannot be read.
    """
    print(f"Attempting to process file: {file_path}")
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None

    try:
        start_time = time.time()
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        match = re.search(r'<DOCUMENT>(.*?)</DOCUMENT>', content, re.IGNORECASE | re.DOTALL)
        
        if match:
            extracted_html = match.group(1).strip()
            elapsed_time = time.time() - start_time
            print(f"Successfully extracted the first <DOCUMENT> block (took {elapsed_time:.2f}s)")
            return extracted_html
        else:
            print("Error: Could not find a <DOCUMENT>...</DOCUMENT> block in the file.")
            return None
            
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

def overwrite_file_with_content(file_path: str, content: str):
    """
    Overwrites the specified file with the given content.

    Args:
        file_path: The path to the file to overwrite.
        content: The string content to write into the file.
    """
    try:
        start_time = time.time()
        with open(file_path, 'w', encoding='utf-8') as f_out:
            f_out.write(content)
        elapsed_time = time.time() - start_time
        print(f"\nSuccessfully overwrote '{os.path.basename(file_path)}' with extracted content (took {elapsed_time:.2f}s)")
        return True
    except Exception as e:
        print(f"\nError overwriting file {file_path}: {e}")

def parse_sec_content(content: str):
    print('Starting parse...')
    start_time = time.time()
    
    elements = sp.Edgar10QParser().parse(content)
    parsed_arr = []

    for elem in elements:
        parsed_arr.append(elem.text + '\n')

    result = ''.join(parsed_arr)
    elapsed_time = time.time() - start_time
    print(f"Parsing completed (took {elapsed_time:.2f}s)")
    return result

def process_directory(root_dir: str):
    """
    Process all full-submission.txt files in the directory structure
    Directory structure: /{symbol}/10-Q/{long_string}/full-submission.txt
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
                
                # Extract HTML content
                extracted_content = extract_first_document_html(str(submission_file))
                if not extracted_content:
                    print(f"✗ Failed to extract HTML from {submission_file}")
                    error_count += 1
                    continue

                # Parse the content
                parsed_content = parse_sec_content(extracted_content)
                if not parsed_content:
                    print(f"✗ Failed to parse content from {submission_file}")
                    error_count += 1
                    continue

                # Overwrite the file
                if overwrite_file_with_content(str(submission_file), parsed_content):
                    processed_count += 1
                    file_elapsed_time = time.time() - file_start_time
                    print(f"Total processing time for this file: {file_elapsed_time:.2f}s")
                else:
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


if __name__ == "__main__":
    # Replace with your actual root directory
    input_directory = "/Users/gabriel.yang/test/financial-testbed/test_data/sec"
    process_directory(input_directory)
