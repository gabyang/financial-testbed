import re
import os
from typing import Optional
import sec_parser as sp


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
        # Read the original content first
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None

    # Use regex to find the first <DOCUMENT>...</DOCUMENT> block
    # re.DOTALL makes '.' match newline characters as well
    # The non-greedy '.*?' ensures we match the *first* closing tag
    match = re.search(r'<DOCUMENT>(.*?)</DOCUMENT>', content, re.IGNORECASE | re.DOTALL)

    if match:
        # group(1) returns the content captured by the parentheses (.*?)
        extracted_html = match.group(1).strip()
        print(f"Successfully extracted the first <DOCUMENT> block.")
        return extracted_html
    else:
        print("Error: Could not find a <DOCUMENT>...</DOCUMENT> block in the file.")
        return None

def overwrite_file_with_content(file_path: str, content: str):
    """
    Overwrites the specified file with the given content.

    Args:
        file_path: The path to the file to overwrite.
        content: The string content to write into the file.
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f_out:
            f_out.write(content)
        print(f"\nSuccessfully overwrote '{os.path.basename(file_path)}' with extracted content.")
    except Exception as e:
        print(f"\nError overwriting file {file_path}: {e}")

def parse_sec_content(content: str):
    print('starting parse')
    # with open(file_path, 'r', encoding='utf-8') as f:
    #     filing_content = f.read()

    elements = sp.Edgar10QParser().parse(content)
    parsed_arr = []

    for elem in elements:
        parsed_arr.append(elem.text + '\n')

    return ''.join(parsed_arr)
    # print(''.join(parsed_arr))

if __name__ == "__main__":
    input_file = '/test_data/sec/full-submission.txt'

    extracted_content = extract_first_document_html(input_file)
    # overwrite_file_with_content(input_file, extracted_content)

    parsed_content = parse_sec_content(extracted_content)
    overwrite_file_with_content(input_file, parsed_content)

    if not parsed_content:    
        print("\nNo content was extracted. Original file remains unchanged.")
