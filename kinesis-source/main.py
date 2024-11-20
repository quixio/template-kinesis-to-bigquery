from quixstreams import Application
from kinesis_source import KinesisSource

import os
from dotenv import load_dotenv

load_dotenv() # load environment variables

kinesis_source = KinesisSource()

app = Application() # Quix Streams App class

sdf = app.dataframe(source=kinesis_source)

sdf.print()

if __name__ == "__main__":
    app.run()

