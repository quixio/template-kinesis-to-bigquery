import datetime
import json
import random
import boto3
import time
import os
# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

STREAM_NAME = os.environ["kinesis_stream_name"]
REGION = os.environ["AWS_REGION_NAME"]  # Replace with your region
AWS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET = os.environ["AWS_SECRET_ACCESS_KEY"]


# Predefined list of tickers
TICKERS = ["NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "TSLA", "GOOG", "BRK.B", "UNH", "XOM", "LLY", "JPM", "JNJ", "V", "PG", "MA", "AVGO", "HD", "CVX"]

# Function to generate synthetic stock data
def generate_synthetic_data(num_records):
    synthetic_data = []
    for _ in range(num_records):
        record = {
            "Date": datetime.datetime.now().isoformat(),
            "Ticker": random.choice(TICKERS),  # Pick from predefined list
            "Open": round(random.uniform(100, 500), 2),
            "High": round(random.uniform(100, 500), 2),
            "Low": round(random.uniform(100, 500), 2),
            "Close": round(random.uniform(100, 500), 2),
            "Volume": random.randint(1000, 10000),
            "Dividends": round(random.uniform(0, 5), 2),
            "Stock Splits": random.randint(0, 2)
        }
        synthetic_data.append(record)
    return synthetic_data

# Updated generate function to track data size
def generate(stream_name, kinesis_client, data):
    total_data_size = 0
    for record in data:
        try:
            # Prepare the data to be sent
            data_to_send = {
                "date": record['Date'],
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

            # Calculate the size of the data to be sent
            data_size = len(json.dumps(data_to_send).encode('utf-8'))
            total_data_size += data_size

            # Stop if approximately 1.1GB of data has been sent
            if total_data_size >= 1.1e9:  # 1.1GB in bytes
                print("Reached 1.1GB data limit. Stopping data generation.")
                break

            print(f"Sending data: {data_to_send}")
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(data_to_send),
                PartitionKey=partition_key
            )
            print(f"Sent record to shard: {response['ShardId']} using partition key: {partition_key}")

        except Exception as e:
            print(f"Error sending record to Kinesis: {str(e)}")
            time.sleep(0.1)  # Wait before retrying

    print("All records have been processed.")

if __name__ == "__main__":
    kinesis_client = boto3.client(
        'kinesis',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=REGION,
    )
    print(f"Starting producer for stream: {STREAM_NAME}")

    # Generate synthetic data
    synthetic_data = generate_synthetic_data(1000000)  # Adjust the number of records as needed

    generate(STREAM_NAME, kinesis_client, synthetic_data)