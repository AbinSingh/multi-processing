import os
import time
import requests
import logging
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
import pandas as pd

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


# Function to process a single file
def process_file(file_path):
    structured_response = {
    'filepath': file_path,
    'type': 'Unknown Type',  # Default value if API call fails
    'Jurisdiction': 'Unknown Jurisdiction',  # Default value if API call fails
    'extraction_failure': 'no',  # Default to 'no'
    'api_failure': 'no'  # Default to 'no'
}

    try:
        # Attempt to extract text from the file
        extracted_text = extract_text(file_path)
        if extracted_text is None:
            logger.warning(f"Extraction failed for file: {file_path}")
            structured_response['extraction_failure'] = 'yes'
            return structured_response  # Return with extraction failure noted

        # Attempt to call the GPT model API
        api_response = call_gpt_model(extracted_text)
        if api_response is None:
            logger.warning(f"API call failed for file: {file_path}")
            structured_response['api_failure'] = 'yes'
            return structured_response  # Return with API failure noted

        # Update structured response with successful API response details
        structured_response.update({
            'type': api_response.get('type', 'Unknown Type'),
            'Jurisdiction': api_response.get('jurisdiction', 'Unknown Jurisdiction')
        })

    except Exception as e:
        logger.error(f"Unexpected error processing file {file_path}: {e}")
        structured_response['extraction_failure'] = 'yes'
        structured_response['api_failure'] = 'yes'  # Mark both as failed in case of unknown error

    return structured_response

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

# Function to process files within each partition and return a DataFrame
def process_files_in_partition(file_paths):
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_file, file_path) for file_path in file_paths]
        for future in concurrent.futures.as_completed(futures):
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
num_partitions = 200  # Adjust as necessary
file_paths_df = file_paths_df.repartition(num_partitions)

# Collect file paths into partitions and process each in parallel
all_responses = []
for partition in file_paths_df.rdd.glom().collect():
    partition_file_paths = [row.file_path for row in partition]
    partition_responses = process_files_in_partition(partition_file_paths)
    all_responses.append(partition_responses)

# Combine all results into a single DataFrame
combined_df = pd.concat(all_responses, ignore_index=True)

# Convert Pandas DataFrame to Spark DataFrame for saving
spark_df = spark.createDataFrame(combined_df)

# Define CSV output path in the Databricks catalog (Delta format recommended for Databricks)
output_path = "/mnt/output/api_responses.csv"  # Adjust the path as necessary

# Save the DataFrame as CSV
try:
    spark_df.write.format("csv").mode("overwrite").option("header", "true").save(output_path)
    logger.info("Successfully saved API responses to CSV")
except Exception as e:
    logger.error(f"Failed to save CSV file: {e}")
