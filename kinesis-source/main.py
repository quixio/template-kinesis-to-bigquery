import os
from dotenv import load_dotenv
from quixstreams import Application
from kinesis_source import KinesisSource

# Load environment variables
load_dotenv()

quix_sdk_token = os.environ["quix_sdk_token"]
kinesis_stream = os.environ["kinesis_stream"]

# Create the Kinesis source
kinesis_source = KinesisSource(
    name="kinesis_source",
    stream_name=kinesis_stream,
    aws_access_key_id=os.environ["aws_access_key_id"],
    aws_secret_access_key=os.environ["aws_secret_access_key"],
    region_name=os.environ["aws_region_name"],
    endpoint_url="http://localhost:4567"  # Use None if connecting to AWS Kinesis
)

# Initialize the application
app = Application(quix_sdk_token=quix_sdk_token)

# Create a Streaming DataFrame from the source
sdf = app.dataframe(source=kinesis_source)

# Process the data as needed (e.g., print the data)
sdf = sdf.print()

if __name__ == "__main__":
    # Run the application
    app.run()