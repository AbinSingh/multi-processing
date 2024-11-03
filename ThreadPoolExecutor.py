import os
import requests
from concurrent.futures import ThreadPoolExecutor
import pdfplumber
from docx import Document

# Function to extract text from PDF
def extract_text_from_pdf(file_path):
    # Store all content in a list to preserve order
    extracted_content = []

    with pdfplumber.open(file_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            # Extract text from the page
            text = page.extract_text()
            if text:
                extracted_content.append(f"Page {page_num + 1} - Text Section:\n\n{text}\n\n")

    # Join all extracted content to a single output
    full_content = "\n".join(extracted_content)
    return full_content

# Function to extract text from DOCX
def extract_text_from_docx(file_path):
    text = ""
    doc = Document(file_path)
    for para in doc.paragraphs:
        text += para.text + "\n"
    return text

# Function to make an API call to GPT
def call_gpt_api(text):
    api_url = "YOUR_API_ENDPOINT"
    headers = {
        "Authorization": "Bearer YOUR_API_KEY",
        "Content-Type": "application/json"
    }
    data = {"input": text}
    response = requests.post(api_url, json=data, headers=headers)
    return response.json()  # Assuming the API returns JSON


from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("FileProcessing").getOrCreate()

# List files in the directory
file_directory = "/dbfs/path/to/your/files/"  # Change to your path
file_paths = [os.path.join(file_directory, file) for file in dbutils.fs.ls(file_directory) if
              file.endswith(('.pdf', '.docx'))]


# Function to process each file
def process_file(file_path):
    if file_path.endswith('.pdf'):
        text = extract_text_from_pdf(file_path)
    elif file_path.endswith('.docx'):
        text = extract_text_from_docx(file_path)
    else:
        return None

    # Call GPT API
    response = call_gpt_api(text)
    return response  # Collect the API response


# Process files in parallel using Spark and threading
def process_files_in_partition(file_paths):
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_file, file_path): file_path for file_path in file_paths}
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                print(f"Error processing {futures[future]}: {e}")
    return results


# Create an RDD from the file paths
rdd = spark.sparkContext.parallelize(file_paths, numSlices=4)  # Adjust numSlices based on your workers
results = rdd.mapPartitions(process_files_in_partition).collect()

# Print results
for result in results:
    print(result)
