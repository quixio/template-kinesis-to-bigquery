import os

from quixstreams import Application
from quixstreams.sinks.community.bigquery import BigQuerySink

app = Application(
    broker_address="localhost:9092",
    auto_offset_reset="earliest",
    consumer_group="consumer-group",
)

topic = app.topic("topic-name")

# Read the service account credentials in JSON format from some environment variable.
service_account_json = os.environ['BIGQUERY_SERVICE_ACCOUNT_JSON']

TABLE_NAME = os.environ["TABLE_NAME"]
PROJECT_ID = os.environ["PROJECT_ID"]
DATASET_ID = os.environ["DATASET_ID"]
DATASET_LOCATION = os.environ["DATASET_LOCATION"]
SERVICE_ACCOUNT_JSON = os.environ["SERVICE_ACCOUNT_JSON"]

# Initialize a sink
bigquery_sink = BigQuerySink(
    project_id=PROJECT_ID,
    location=DATASET_LOCATION,
    dataset_id=DATASET_ID,
    table_name=TABLE_NAME,
    service_account_json=service_account_json,
    schema_auto_update=True,
    ddl_timeout=10.0,
    insert_timeout=10.0,
    retry_timeout=30.0,
)

sdf = app.dataframe(topic)
sdf.sink(bigquery_sink)

if __name__ == '__main__':
    app.run()