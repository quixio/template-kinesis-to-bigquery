import os
import time
from quixstreams import Application
from dotenv import load_dotenv
from order_generator import OrderGenerator

# For local dev, load env vars from a .env file
load_dotenv()

# Initialize Quix Streams Application
app = Application(
    consumer_group="order_producer"
)

# Define the orders topic
orders_topic = app.topic(os.environ['output_topic'], value_serializer="json")
order_generator = OrderGenerator()

if __name__ == "__main__":
    print(f"Starting order producer for topic: {orders_topic.name}")

    # Use the Quix producer context manager
    with app.get_producer() as producer:
        while event := order_generator.generate_order():
            try:
                # Serialize and produce the order
                message = orders_topic.serialize(**event)
                print(f"Producing order {message.key}")

                producer.produce(
                    topic=orders_topic.name,
                    key=message.key,
                    value=message.value
                )

                # Small delay between messages
                time.sleep(0.2)  # Adjust as needed

            except Exception as e:
                print(f"Error producing order: {str(e)}")
                time.sleep(0.1)  # Brief pause before retry

    print("Finished producing orders")