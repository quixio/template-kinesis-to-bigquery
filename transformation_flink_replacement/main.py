import os
from datetime import timedelta
from quixstreams import Application
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Quix Streams Application
app = Application(
    consumer_group="category_stats_processor",
    auto_offset_reset="earliest"  # Match Kinesis LATEST behavior if desired
)

# Define input and output topics
orders_topic = app.topic(
    os.environ["input"],
    value_deserializer="json",
)

stats_topic = app.topic(
    os.environ["output"],
    value_serializer="json",
)

# Create StreamingDataFrame from input topic
sdf = app.dataframe(topic=orders_topic)


def format_window_stats(window_result):
    """Format window aggregation results to match desired output schema"""
    return {
        "window_end": window_result["end"],
        "product_category": window_result["key"],  # Group by key is category
        "total_quantity": window_result["value"]
    }


# Process the stream with hopping window aggregation
sdf = (
    # Extract quantity for aggregation
    sdf.apply(lambda value: value["quantity"])

    # Create hopping window matching Flink's HOP(proc_time, INTERVAL '1' MINUTE, INTERVAL '5' MINUTES)
    .hopping_window(
        duration_ms=timedelta(minutes=5),  # Window size
        step_ms=timedelta(minutes=1)  # Hop size
    )
    .sum()  # SUM(quantity)
    .current()  # Get current window results

    # Format results to match desired output schema
    .apply(format_window_stats)
)

# Output results
sdf = sdf.to_topic(stats_topic)

if __name__ == "__main__":
    print("Starting category stats processor...")
    print(f"Reading from topic: {orders_topic.name}")
    print(f"Writing to topic: {stats_topic.name}")
    app.run()