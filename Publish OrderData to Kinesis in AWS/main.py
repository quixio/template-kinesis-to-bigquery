import datetime
import json
import random
import boto3
import time
import os
# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

STREAM_NAME = os.environ["kinesis_stream_name_lambda"]
REGION = os.environ["aws_region_name"]  # Replace with your region
AWS_KEY = os.environ["aws_access_key_id"]
AWS_SECRET = os.environ["aws_secret_access_key"]


# Predefined lists for random data generation
USER_NAMES = ["Diana Prince", "Clark Kent", "Bruce Wayne", "Barry Allen"]
EMAILS = ["diana@example.com", "clark@example.com", "bruce@example.com", "barry@example.com"]
PHONES = ["555-1234", "555-5678", "555-8765", "555-4321"]
CITIES = ["Metropolis", "Gotham", "Central City", "Star City"]
COUNTRIES = ["USA", "Canada", "UK"]
PRODUCTS = [
    {"id": "A123", "name": "Golden Lasso", "category": "Accessories"},
    {"id": "B456", "name": "Shield of Athena", "category": "Defence"},
    {"id": "C789", "name": "Batmobile", "category": "Vehicles"},
    {"id": "D012", "name": "Flash Suit", "category": "Apparel"}
]
SHIPPING_METHODS = ["Standard", "Express", "Overnight"]

# Function to generate synthetic order-centric data
def generate_synthetic_data(num_records):
    synthetic_data = []
    for _ in range(num_records):
        customer_id = random.randint(1000, 9999)
        order = {
            "order": {
                "id": random.randint(100, 999),
                "date": datetime.datetime.now().strftime("%Y-%m-%d"),
                "customer": {
                    "id": customer_id,
                    "name": random.choice(USER_NAMES),
                    "contact": {
                        "email": random.choice(EMAILS),
                        "phone": random.choice(PHONES)
                    }
                },
                "shipping": {
                    "address": {
                        "street": f"{random.randint(1, 999)} Maple Avenue",
                        "city": random.choice(CITIES),
                        "postal_code": f"{random.randint(10000, 99999)}",
                        "country": random.choice(COUNTRIES)
                    },
                    "method": random.choice(SHIPPING_METHODS),
                    "cost": round(random.uniform(5.0, 20.0), 2)
                },
                "items": [
                    {
                        "product": random.choice(PRODUCTS),
                        "quantity": random.randint(1, 3),
                        "price": round(random.uniform(10.0, 100.0), 2)
                    },
                    {
                        "product": random.choice(PRODUCTS),
                        "quantity": random.randint(1, 3),
                        "price": round(random.uniform(10.0, 100.0), 2)
                    }
                ],
                "total_cost": 0  # This will be calculated below
            }
        }
        
        # Calculate total cost
        total_cost = sum(item["quantity"] * item["price"] for item in order["order"]["items"])
        total_cost += order["order"]["shipping"]["cost"]
        order["order"]["total_cost"] = round(total_cost, 2)
        
        synthetic_data.append(order)
    return synthetic_data

# Updated generate function to track data size
def generate(stream_name, kinesis_client, data):
    total_data_size = 0
    for record in data:
        try:
            # Prepare the data to be sent
            data_to_send = {
                "order": {
                    "id": record["order"]["id"],
                    "date": record["order"]["date"],
                    "customer": {
                        "id": record["order"]["customer"]["id"],
                        "name": record["order"]["customer"]["name"],
                        "contact": {
                            "email": record["order"]["customer"]["contact"]["email"],
                            "phone": record["order"]["customer"]["contact"]["phone"]
                        }
                    },
                    "shipping": {
                        "address": {
                            "street": record["order"]["shipping"]["address"]["street"],
                            "city": record["order"]["shipping"]["address"]["city"],
                            "postal_code": record["order"]["shipping"]["address"]["postal_code"],
                            "country": record["order"]["shipping"]["address"]["country"]
                        },
                        "method": record["order"]["shipping"]["method"],
                        "cost": record["order"]["shipping"]["cost"]
                    },
                    "items": record["order"]["items"],
                    "total_cost": record["order"]["total_cost"]
                }
            }
            partition_key = str(data_to_send["order"]["id"])

            # Calculate the size of the data to be sent
            data_size = len(json.dumps(data_to_send).encode('utf-8'))
            total_data_size += data_size

            # Stop if approximately 1.1GB of data has been sent
            if total_data_size >= 1.1e9:  # 1.1GB in bytes
                print("Reached 1.1GB data limit. Stopping data generation.")
                break

            print(f"Sending data: {data_to_send}")
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(data_to_send),
                PartitionKey=partition_key
            )
            print(f"Sent record to shard: {response['ShardId']} using partition key: {partition_key}")

        except Exception as e:
            print(f"Error sending record to Kinesis: {str(e)}")
            time.sleep(0.1)  # Wait before retrying

    print("All records have been processed.")

if __name__ == "__main__":
    kinesis_client = boto3.client(
        'kinesis',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name=REGION,
    )
    print(f"Starting producer for stream: {STREAM_NAME}")

    # Generate synthetic data
    synthetic_data = generate_synthetic_data(1000000)  # Adjust the number of records as needed

    generate(STREAM_NAME, kinesis_client, synthetic_data)