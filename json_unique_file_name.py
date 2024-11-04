import os
import time
import requests
import logging
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
import pandas as pd
import json
from pathlib import Path
import hashlib

# Set up logging configuration
logging.basicConfig(
    filename='/dbfs/mnt/logs/gpt_processing.log',  # Save logs to DBFS for persistence
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("ProcessFilesWithGPT").getOrCreate()

# Directory to save successful API responses
response_dir = "/dbfs/mnt/gpt_responses"


# Function to generate a unique filename for each file based on the file path
def generate_unique_filename(filepath):
    hash_object = hashlib.md5(filepath.encode())
    unique_filename = hash_object.hexdigest() + ".json"
    return unique_filename


# Function to extract text from file with error handling
def extract_text(file_path):
    try:
        if file_path.endswith('.pdf'):
            text = "Extracted text from PDF"  # Replace with actual extraction code
        elif file_path.endswith('.docx'):
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
    backoff_factor = 1.5
    retry_count = 0

    while retry_count < max_retries:
        try:
            response = requests.post(api_url, json={"text": extracted_text}, headers=headers)
            if response.status_code == 200:
                return response.json()  # Return the successful response
            elif response.status_code == 429:  # Rate limit exceeded
                retry_count += 1
                wait_time = backoff_factor ** retry_count
                logger.warning(f"Rate limit exceeded. Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"API call failed with status code {response.status_code}: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            retry_count += 1
            logger.error(f"API request failed: {e}. Retrying {retry_count}/{max_retries}...")
            time.sleep(backoff_factor ** retry_count)

    logger.error("Max retries reached. API call failed.")
    return None


# Function to load previously processed files
def load_processed_files():
    processed_files = set()
    if not os.path.exists(response_dir):
        os.makedirs(response_dir)
    else:
        for filename in os.listdir(response_dir):
            if filename.endswith(".json"):
                file_path = os.path.join(response_dir, filename)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        if data.get("status") == "success":
                            processed_files.add(data["filepath"]) #processed_files.add(filename)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decoding error for file {file_path}: {e}")
                except Exception as e:
                    logger.error(f"Error loading processed file {file_path}: {e}")
    return processed_files


# Function to save the response to DBFS
def save_response(filepath, response, status="success"):
    response_data = {
        "filepath": filepath,
        "type": response.get("type", "Unknown Type"),
        "jurisdiction": response.get("jurisdiction", "Unknown Jurisdiction"),
        "status": status
    }

    # Generate a unique filename based on the filepath
    filename = generate_unique_filename(filepath)
    file_path = os.path.join(response_dir, filename)

    try:
        with open(file_path, 'w') as f:
            json.dump(response_data, f)
    except (OSError, IOError) as e:
        logger.error(f"Failed to save JSON response for file {filepath}: {e}")


# Function to process a single file
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

# Remaining functions are unchanged for brevity
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

# Load all file paths from "first_set" folder and subfolders (countries)
root_folder = "/dbfs/mnt/first_set"  # DBFS path to your root folder
file_paths = []
for dirpath, _, filenames in os.walk(root_folder):
    for filename in filenames:
        if filename.endswith(".pdf") or filename.endswith(".docx"):
            file_paths.append(os.path.join(dirpath, filename))

# Convert file paths to a Spark DataFrame for partitioning
file_paths_df = spark.createDataFrame([(fp,) for fp in file_paths], ["file_path"])

# Partition the DataFrame based on the number of worker nodes
num_partitions = 200  # Adjust as necessary based on cluster capacity
file_paths_df = file_paths_df.repartition(num_partitions)

# Function to process each partition of files distributedly
def distributed_file_processing(partition):
    file_paths = [row.file_path for row in partition]
    processed_files = load_processed_files()  # Load processed files to skip duplicates

    # Filter file paths to process only unprocessed files
    unprocessed_file_paths = [fp for fp in file_paths if fp not in processed_files]
    # Generate unique filename for each file path and filter only unprocessed files
    '''
    unprocessed_file_paths = [
        fp for fp in file_paths if generate_unique_filename(fp) not in processed_files
    ]
    '''
    return process_files_in_partition(unprocessed_file_paths)

# Apply the distributed processing using mapPartitions
responses_rdd = file_paths_df.rdd.mapPartitions(distributed_file_processing)

# Collect results and convert to a Spark DataFrame if needed
responses_df = responses_rdd.toDF()

# Show or save the results as needed
responses_df.show()
