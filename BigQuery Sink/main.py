import os

from quixstreams import Application
from quixstreams.sinks.community.bigquery import BigQuerySink

app = Application(auto_offset_reset="earliest",
    consumer_group="consumer-group",
)

topic = app.topic(os.environ["input"])

# Initialize a sink
bigquery_sink = BigQuerySink(
    project_id=os.environ["PROJECT_ID"],
    location=os.environ["DATASET_LOCATION"],
    dataset_id=os.environ["DATASET_ID"],
    table_name=os.environ["TABLE_NAME"],
    service_account_json=os.environ["BIGQUERY_SERVICE_ACCOUNT_JSON"],
    schema_auto_update=True,
    ddl_timeout=10.0,
    insert_timeout=10.0,
    retry_timeout=30.0,
)

sdf = app.dataframe(topic)
sdf.sink(bigquery_sink)
sdf.print()

if __name__ == '__main__':
    app.run()