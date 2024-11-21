from quixstreams.sources.base import Source
import boto3
import time

class KinesisSource(Source):
    def __init__(self, name, stream_name, aws_access_key_id, aws_secret_access_key, region_name, endpoint_url=None):
        super().__init__(name=name)
        self.stream_name = stream_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        
    def run(self):
        kinesis_client = boto3.client(
            'kinesis',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
            endpoint_url=self.endpoint_url  # Use this if running Kinesis locally
        )

        # Check if the stream exists
        try:
            response = kinesis_client.describe_stream(StreamName=self.stream_name)
            stream_status = response['StreamDescription']['StreamStatus']

            # Wait until the stream is active
            while stream_status != 'ACTIVE':
                time.sleep(1)
                response = kinesis_client.describe_stream(StreamName=self.stream_name)
                stream_status = response['StreamDescription']['StreamStatus']

        except kinesis_client.exceptions.ResourceNotFoundException:
            print(f"Stream {self.stream_name} not found.")
            return

        # Get the shard ID
        shard_id = response['StreamDescription']['Shards'][0]['ShardId']

        # Get shard iterator
        shard_iterator = kinesis_client.get_shard_iterator(
            StreamName=self.stream_name,
            ShardId=shard_id,
            ShardIteratorType='LATEST'
        )['ShardIterator']

        while self.running:
            record_response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=100)
            shard_iterator = record_response['NextShardIterator']
            records = record_response['Records']

            for record in records:

                data = record['Data']
                # Publish the data to the Quix Streams topic
                self.produce(key=self.stream_name, value=data)
                print("Data published to Kinesis")

            time.sleep(1)
