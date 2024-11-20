from quixstreams import Application
from kinesis_source import KinesisSource

import os
from dotenv import load_dotenv

load_dotenv() # load environment variables
kinesis_stream = os.environ["kinesis_stream"]

kinesis_source = KinesisSource(
    name=kinesis_stream,
    stream_name=kinesis_stream,
    aws_access_key_id=os.environ["aws_access_key_id"],
    aws_secret_access_key=os.environ["aws_secret_access_key"],
    region_name=os.environ["aws_region_name"],
    endpoint_url="http://kinesis:4566"  

)

app = Application() # Quix Streams App class

sdf = app.dataframe(source=kinesis_source)

sdf.print()

if __name__ == "__main__":
    app.run()

