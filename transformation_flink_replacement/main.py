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
    value_deserializer="json"
)

stats_topic = app.topic(
    os.environ["output"],
    value_serializer="json"
)

# Create StreamingDataFrame from input topic
sdf = app.dataframe(topic=orders_topic)

# Process the stream with hopping window aggregation
sdf = (
    # Group by product category
    sdf.group_by(lambda value: value["product_category"], name="category_group")
    
    # Extract just the quantity for summing
    .apply(lambda value: value["quantity"])
    
    # Create hopping window with 5 min duration and 1 min step
    .hopping_window(
        duration_ms=timedelta(minutes=5),
        step_ms=timedelta(minutes=1)
    )
    .sum()
    .current()
    
    # Format results to match Flink output schema
    .apply(
        lambda result, key, timestamp, headers: {
            "window_end": result["end"],
            "product_category": key,  # Get category from the groupby key
            "total_quantity": result["value"]
        },
        metadata=True  # Important: needed to access the key
    )
)

# Output results
sdf = sdf.to_topic(stats_topic)

if __name__ == "__main__":
    print("Starting category stats processor...")
    print(f"Reading from topic: {orders_topic.name}")
    print(f"Writing to topic: {stats_topic.name}")
    app.run()

# Version 5 of 5