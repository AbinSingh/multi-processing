import os
import time
import requests
import logging
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
import pandas as pd
import json
from pathlib import Path

# Set up logging configuration
logging.basicConfig(
    filename='/dbfs/mnt/logs/gpt_processing.log',  # Save logs to DBFS for persistence
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("ProcessFilesWithGPT").getOrCreate()

# Function to extract text from file with error handling
def extract_text(file_path):
    try:
        if file_path.endswith('.pdf'):
            # Code to extract text from PDF
            text = "Extracted text from PDF"  # Replace with actual extraction code
        elif file_path.endswith('.docx'):
            # Code to extract text from DOCX
            text = "Extracted text from DOCX"  # Replace with actual extraction code
        else:
            raise ValueError("Unsupported file format")
        return text
    except Exception as e:
        logger.error(f"Failed to extract text from {file_path}: {e}")
        return None

# Function to make the API call with rate limit, retry logic, and error handling
def call_gpt_model(extracted_text):
    api_url = "https://api.example.com/gpt"  # Replace with your actual API endpoint
    api_key = "your_api_key"  # Replace with your actual API key

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    max_retries = 5
    backoff_factor = 1.5  # Exponential backoff factor
    retry_count = 0

    while retry_count < max_retries:
        try:
            response = requests.post(api_url, json={"text": extracted_text}, headers=headers)
            if response.status_code == 200:
                return response.json()  # Return the successful response
            elif response.status_code == 429:  # Rate limit exceeded
                retry_count += 1
                wait_time = backoff_factor ** retry_count  # Exponential backoff
                logger.warning(f"Rate limit exceeded. Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"API call failed with status code {response.status_code}: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            retry_count += 1
            logger.error(f"API request failed: {e}. Retrying {retry_count}/{max_retries}...")
            time.sleep(backoff_factor ** retry_count)  # Apply exponential backoff

    logger.error("Max retries reached. API call failed.")
    return None  # Return None if all retries fail

# Directory to save successful API responses
response_dir = "/dbfs/mnt/gpt_responses"

# Function to load previously processed files
def load_processed_files():
    processed_files = set()
    if not os.path.exists(response_dir):
        os.makedirs(response_dir, exist_ok=True)
    else:
        for filename in os.listdir(response_dir):
            if filename.endswith(".json"):
                with open(os.path.join(response_dir, filename), 'r') as f:
                    data = json.load(f)
                    if data.get("status") == "success":
                        processed_files.add(data["filepath"])
    return processed_files

# Function to save the response to DBFS
def save_response(filepath, response, status="success"):
    response_data = {
        "filepath": filepath,
        "type": response.get("type", "Unknown Type"),
        "jurisdiction": response.get("jurisdiction", "Unknown Jurisdiction"),
        "status": status
    }
    filename = Path(filepath).stem + ".json"
    with open(os.path.join(response_dir, filename), 'w') as f:
        json.dump(response_data, f)

# Function to process a single file
# Modified process_file function with logging and saving responses
def process_file(file_path):
    structured_response = {
        'filepath': file_path,
        'type': 'Unknown Type',  # Default value if API call fails
        'jurisdiction': 'Unknown Jurisdiction',  # Default value if API call fails
        'status': 'error'
    }

    try:
        # Attempt to extract text from the file
        extracted_text = extract_text(file_path)
        if extracted_text is None:
            logger.warning(f"Extraction failed for file: {file_path}")
            structured_response['status'] = 'extraction_failure'
            save_response(file_path, structured_response, status="error")
            return structured_response

        # Attempt to call the GPT model API
        api_response = call_gpt_model(extracted_text)
        if api_response is None:
            logger.warning(f"API call failed for file: {file_path}")
            save_response(file_path, structured_response, status="api_failure")
            return structured_response

        # Update structured response with successful API response details
        structured_response.update({
            'type': api_response.get('type', 'Unknown Type'),
            'jurisdiction': api_response.get('jurisdiction', 'Unknown Jurisdiction'),
            'status': 'success'
        })
        save_response(file_path, structured_response, status="success")

    except Exception as e:
        logger.error(f"Unexpected error processing file {file_path}: {e}")
        structured_response['status'] = 'error'
        save_response(file_path, structured_response, status="error")

    return structured_response
# Function to process files within each partition and return a DataFrame
def process_files_in_partition(file_paths):
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_file, file_path) for file_path in file_paths]
        for future in futures:
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                logger.error(f"Error processing file in partition: {e}")
    return pd.DataFrame(results)



# Partition the DataFrame based on the number of worker nodes
num_partitions = 200  # Adjust as necessary
file_paths_df = file_paths_df.repartition(num_partitions)

# Collect file paths into partitions and process each in parallel
def distributed_file_processing(partition):
    file_paths = [row.file_path for row in partition]
    return process_files_in_partition(file_paths)

# Apply the distributed processing using mapPartitions
responses_rdd = file_paths_df.rdd.mapPartitions(distributed_file_processing)

