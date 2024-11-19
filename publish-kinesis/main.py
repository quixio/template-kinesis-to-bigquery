import boto3
import os
import time
from datetime import datetime
import random

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

stream_name = "stream_1"

try:
    kinesis_client = boto3.client(
        'kinesis',
        aws_access_key_id=os.environ["aws_access_key_id"],
        aws_secret_access_key=os.environ["aws_secret_access_key"],
        region_name=os.environ["aws_region_name"],
        endpoint_url="http://localhost:4566"
    )
except Exception as e:
    print(f"Failed to connect to AWS: {e}")


# Check if the stream exists
try:
    response = kinesis_client.describe_stream(StreamName=stream_name)
    stream_status = response['StreamDescription']['StreamStatus']

    # Wait until the stream is active
    while stream_status != 'ACTIVE':
        time.sleep(1)
        response = kinesis_client.describe_stream(StreamName=stream_name)
        stream_status = response['StreamDescription']['StreamStatus']

except kinesis_client.exceptions.ResourceNotFoundException:
    # Create the stream if it doesn't exist
    kinesis_client.create_stream(StreamName=stream_name, ShardCount=1)

    # Wait until the stream is active
    while True:
        response = kinesis_client.describe_stream(StreamName=stream_name)
        stream_status = response['StreamDescription']['StreamStatus']
        if stream_status == 'ACTIVE':
            break
        time.sleep(1)

while True:
    response = kinesis_client.describe_stream(StreamName=stream_name)
    if response['StreamDescription']['StreamStatus'] == 'ACTIVE':
        break
    time.sleep(1)

while True:
    time = datetime.now()
    host_name = str(f'host_{random.randint(1, 10)}')
    used_pct = str(random.randint(1, 100))

    kinesis_client.put_record(
        StreamName=stream_name,
        Data=b'{"m": "mem", "host": "' + host_name + '", "used_percent": "' + used_pct + '", "time": "' + time + '"},',
        PartitionKey='partition_key'
    )
    