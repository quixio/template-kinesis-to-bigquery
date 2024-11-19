import os
import json
from quixstreams import Application

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application(consumer_group="transformation-v1", auto_offset_reset="earliest")

input_topic = app.topic(os.environ["input"])
output_topic = app.topic(os.environ["output"])

sdf = app.dataframe(input_topic)

def schema_transformation(data):
    ...

sdf.apply(schema_transformation)

sdf.print()
sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()