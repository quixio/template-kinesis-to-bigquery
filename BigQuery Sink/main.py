import os

from quixstreams import Application
from quixstreams.sinks.community.bigquery import BigQuerySink

app = Application(auto_offset_reset="earliest",
    consumer_group="consumer-group",
)

topic = app.topic(os.environ["input"])

TABLE_NAME = os.environ["TABLE_NAME"]
PROJECT_ID = os.environ["PROJECT_ID"]
DATASET_ID = os.environ["DATASET_ID"]

table = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}'

DATASET_LOCATION = os.environ["DATASET_LOCATION"]
SERVICE_ACCOUNT_JSON = os.environ["BIGQUERY_SERVICE_ACCOUNT_JSON"]

# Initialize a sink
bigquery_sink = BigQuerySink(
    project_id=PROJECT_ID,
    location=DATASET_LOCATION,
    dataset_id=DATASET_ID,
    table_name=table,
    service_account_json=SERVICE_ACCOUNT_JSON,
    schema_auto_update=True,
    ddl_timeout=10.0,
    insert_timeout=10.0,
    retry_timeout=30.0,
)

sdf = app.dataframe(topic)
sdf.sink(bigquery_sink)

if __name__ == '__main__':
    app.run()