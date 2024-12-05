import os
import time
from quixstreams import Application
from dotenv import load_dotenv
from orderlines_generator import OrderLinesGenerator

# For local dev, load env vars from a .env file
load_dotenv()

# Initialize Quix Streams Application
app = Application(
    consumer_group="orderlines_producer"
)

# Define the orders topic
orderlines_topic = app.topic(os.environ["orderlines"], value_serializer="json")
orderlines_generator = OrderLinesGenerator()

if __name__ == "__main__":
    print(f"Starting orderlines producer for topic: {orderlines_topic.name}")

    # Use the Quix producer context manager
    with app.get_producer() as producer:
        while not orderlines_generator.stop:
            try:
                # Each order generates two item records
                for event in orderlines_generator.generate_orderline():
                    if event is None:  # Check if we've hit the data limit
                        break

                    # Serialize and produce the order item
                    message = orderlines_topic.serialize(**event)
                    print(f"Producing order line {message.key}: {event['value']['product_name']}")

                    producer.produce(
                        topic=orderlines_topic.name,
                        key=message.key,
                        value=message.value
                    )

                # Small delay between orders
                time.sleep(0.2)  # Adjust as needed

            except Exception as e:
                print(f"Error producing order line: {str(e)}")
                time.sleep(0.1)  # Brief pause before retry

    print(f"Finished producing order lines. Generated {orderlines_generator.orderlines_generated} order lines.")