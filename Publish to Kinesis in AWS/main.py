import datetime
import json
import boto3
import time
import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

STREAM_NAME = os.environ["kinesis_stream_name"]
REGION = os.environ["AWS_REGION_NAME"]
AWS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET = os.environ["AWS_SECRET_ACCESS_KEY"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
S3_FILE_KEY = os.environ["S3_FILE_KEY"]

# Update the state directory and file paths
STATE_DIR = './state'  # Directory for shared state
LOCK_FILE_PATH = os.path.join(STATE_DIR, 'lockfile.lock')  # Path to the lock file
PARQUET_FILE_PATH = os.path.join(STATE_DIR, 'all_stock_data.parquet')  # Path to the Parquet file

# Existing function to download the Parquet file from S3
def download_parquet_from_s3(bucket_name, file_key, download_path):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=REGION,
    )
    print("Downloading parquet file...")
    s3_client.download_file(bucket_name, file_key, download_path)
    print("Finished downloading parquet file.")

# Existing function to read data from a Parquet file
def read_parquet_data(file_path):
    df = pd.read_parquet(file_path)
    return df.to_dict(orient='records')

# New function to download the Parquet file with a lock
def download_parquet_with_lock(bucket_name, file_key, download_path):
    while os.path.exists(LOCK_FILE_PATH):
        print("Waiting for lock to be released...")
        time.sleep(5)  # Wait for 5 seconds before checking again

    try:
        # Create a lock file
        with open(LOCK_FILE_PATH, 'w') as lock_file:
            lock_file.write('locked')

        # Download the Parquet file
        download_parquet_from_s3(bucket_name, file_key, download_path)

    finally:
        # Remove the lock file
        if os.path.exists(LOCK_FILE_PATH):
            os.remove(LOCK_FILE_PATH)

# Existing function to generate data
def generate(stream_name, kinesis_client, data):
    for record in data:
        try:
            # Prepare the data to be sent
            data_to_send = {
                "date": record['Date'].isoformat(),
                "ticker": record['Ticker'],
                "open": record['Open'],
                "high": record['High'],
                "low": record['Low'],
                "close": record['Close'],
                "volume": record['Volume'],
                "dividends": record['Dividends'],
                "stock_splits": record['Stock Splits']
            }
            partition_key = data_to_send['ticker']

            print(f"Sending data: {data_to_send}")
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(data_to_send),
                PartitionKey=partition_key
            )
            print(f"Sent record to shard: {response['ShardId']} using partition key: {partition_key}")
            # time.sleep(0.01)  # Add a delay between records

        except Exception as e:
            print(f"Error sending record to Kinesis: {str(e)}")

            time.sleep(0.1)  # Wait before retrying
    print("All records from the Parquet file have been processed.")

if __name__ == "__main__":
    kinesis_client = boto3.client(
        'kinesis',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=REGION,
    )
    print(f"Starting producer for stream: {STREAM_NAME}")

    # Ensure the state directory exists
    os.makedirs(STATE_DIR, exist_ok=True)
    
    # Download the Parquet file from S3 with locking
    download_parquet_with_lock(S3_BUCKET_NAME, S3_FILE_KEY, PARQUET_FILE_PATH)

    data = read_parquet_data(PARQUET_FILE_PATH)

    generate(STREAM_NAME, kinesis_client, data)