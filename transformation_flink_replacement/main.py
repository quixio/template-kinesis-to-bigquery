import os
import json
from quixstreams import Application

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application(consumer_group="transformation-v1", auto_offset_reset="earliest")

input_topic = app.topic(os.environ["input"], value_deserializer="string")
output_topic = app.topic(os.environ["output"], value_serializer="json")

sdf = app.dataframe(input_topic)

def schema_transformation(original_data):
    # Parse the JSON string into a Python dictionary
    data = json.loads(original_data)

    # Change 'm' to 'field' and 'mem' to 'memory'
    if 'm' in data:
        if data['m'] == 'mem':
            data['field'] = 'memory'
        else:
            data['field'] = data['m']
        del data['m']

    # Convert the dictionary back to a JSON string
    return data

sdf = sdf.apply(schema_transformation)

sdf.print()
sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run()