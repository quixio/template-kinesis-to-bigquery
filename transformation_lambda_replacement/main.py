import os
from quixstreams import Application
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Quix Streams Application
app = Application(
    consumer_group="order_normalizer",
    auto_offset_reset="earliest"
)

# Define input and output topics
input_topic = app.topic(os.environ["input"], value_deserializer="json")
output_topic = app.topic(os.environ["output"], value_serializer="json")


def normalize_order(order_data):
    """
    Flatten nested order JSON into individual item records
    """
    try:
        order = order_data["order"]

        # Extract common fields
        common_fields = {
            "order_id": order["id"],
            "order_date": order["date"],
            "customer_id": order["customer"]["id"],
            "customer_name": order["customer"]["name"],
            "shipping_method": order["shipping"]["method"]
        }

        # Normalize into individual item records
        for item in order["items"]:
            normalized_record = {
                **common_fields,
                "product_id": item["product"]["id"],
                "product_name": item["product"]["name"],
                "product_category": item["product"]["category"],
                "quantity": item["quantity"],
                "price": item["price"]
            }
            yield {
                "key": str(normalized_record["product_id"]),
                "value": normalized_record
            }

    except Exception as e:
        print(f"Error normalizing order: {str(e)}")
        return None


# Create StreamingDataFrame and process
sdf = app.dataframe(topic=input_topic)

# Apply normalization and flatten the results
sdf = sdf.apply(normalize_order, expand=True)

# Output normalized records
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    print("Starting order normalizer...")
    print(f"Reading nested orders from: {input_topic.name}")
    print(f"Writing normalized records to: {output_topic.name}")
    app.run()