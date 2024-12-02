import datetime
import json
import random
import boto3
import time
import os
# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

STREAM_NAME = os.environ["kinesis_stream_name_flink"]
REGION = os.environ["AWS_REGION_NAME"]  # Replace with your region
AWS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET = os.environ["AWS_SECRET_ACCESS_KEY"]


# Predefined lists for random data generation
USER_NAMES = ["Diana Prince", "Clark Kent", "Bruce Wayne", "Barry Allen"]
EMAILS = ["diana@example.com", "clark@example.com", "bruce@example.com", "barry@example.com"]
PHONES = ["555-1234", "555-5678", "555-8765", "555-4321"]
CITIES = ["Metropolis", "Gotham", "Central City", "Star City"]
COUNTRIES = ["USA", "Canada", "UK"]
PRODUCTS = [
    {"id": "A001", "name": "Golden Lasso", "category": "Accessories"},
    {"id": "A002", "name": "Shield of Athena", "category": "Defence"},
    {"id": "A003", "name": "Batmobile", "category": "Vehicles"},
    {"id": "A004", "name": "Flash Suit", "category": "Apparel"},
    {"id": "A005", "name": "Invisibility Cloak", "category": "Apparel"},
    {"id": "A006", "name": "Magic Wand", "category": "Magic"},
    {"id": "A007", "name": "Potion of Healing", "category": "Potions"},
    {"id": "A008", "name": "Dragon Shield", "category": "Defence"},
    {"id": "A009", "name": "Elven Bow", "category": "Weapons"},
    {"id": "A010", "name": "Dwarven Axe", "category": "Weapons"},
    {"id": "A011", "name": "Phoenix Feather", "category": "Magic"},
    {"id": "A012", "name": "Crystal Ball", "category": "Magic"},
    {"id": "A013", "name": "Flying Carpet", "category": "Vehicles"},
    {"id": "A014", "name": "Teleportation Ring", "category": "Accessories"},
    {"id": "A015", "name": "Time Turner", "category": "Accessories"},
    {"id": "A016", "name": "Gryphon Saddle", "category": "Accessories"},
    {"id": "A017", "name": "Vampire Cape", "category": "Apparel"},
    {"id": "A018", "name": "Werewolf Claw", "category": "Weapons"},
    {"id": "A019", "name": "Mermaid Scale", "category": "Magic"},
    {"id": "A020", "name": "Unicorn Horn", "category": "Magic"},
    {"id": "A021", "name": "Goblin Dagger", "category": "Weapons"},
    {"id": "A022", "name": "Troll Club", "category": "Weapons"},
    {"id": "A023", "name": "Wizard Hat", "category": "Apparel"},
    {"id": "A024", "name": "Sorcerer's Stone", "category": "Magic"},
    {"id": "A025", "name": "Enchanted Mirror", "category": "Magic"},
    {"id": "A026", "name": "Mystic Amulet", "category": "Accessories"},
    {"id": "A027", "name": "Cursed Necklace", "category": "Accessories"},
    {"id": "A028", "name": "Pirate's Cutlass", "category": "Weapons"},
    {"id": "A029", "name": "Knight's Armor", "category": "Defence"},
    {"id": "A030", "name": "Samurai Sword", "category": "Weapons"},
    {"id": "A031", "name": "Ninja Stars", "category": "Weapons"},
    {"id": "A032", "name": "Viking Helmet", "category": "Historical"},
    {"id": "A033", "name": "Roman Shield", "category": "Historical"},
    {"id": "A034", "name": "Spartan Spear", "category": "Historical"},
    {"id": "A035", "name": "Egyptian Ankh", "category": "Magic"},
    {"id": "A036", "name": "Aztec Calendar", "category": "Cultural"},
    {"id": "A037", "name": "Mayan Mask", "category": "Cultural"},
    {"id": "A038", "name": "Incan Idol", "category": "Cultural"},
    {"id": "A039", "name": "Celtic Knot", "category": "Cultural"},
    {"id": "A040", "name": "Voodoo Doll", "category": "Mythical"},
    {"id": "A041", "name": "Shaman's Staff", "category": "Mythical"},
    {"id": "A042", "name": "Alchemist's Flask", "category": "Potions"},
    {"id": "A043", "name": "Herbal Remedy", "category": "Potions"},
    {"id": "A044", "name": "Elixir of Life", "category": "Potions"},
    {"id": "A045", "name": "Potion of Strength", "category": "Potions"},
    {"id": "A046", "name": "Potion of Speed", "category": "Potions"},
    {"id": "A047", "name": "Potion of Invisibility", "category": "Potions"},
    {"id": "A048", "name": "Potion of Wisdom", "category": "Potions"},
    {"id": "A049", "name": "Potion of Luck", "category": "Potions"},
    {"id": "A050", "name": "Potion of Courage", "category": "Potions"}
]
SHIPPING_METHODS = ["Standard", "Express", "Overnight"]

# Function to generate synthetic order-centric data
def generate_synthetic_data(num_records):
    synthetic_data = []
    for _ in range(num_records):
        customer_id = random.randint(1000, 9999)
        order_id = random.randint(100, 999)
        order_date = datetime.datetime.now().strftime("%Y-%m-%d")
        customer_name = random.choice(USER_NAMES)
        shipping_method = random.choice(SHIPPING_METHODS)
        
        # Generate items for the order
        items = [
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
        ]
        
        # Create a record for each item
        for item in items:
            record = {
                "order_id": order_id,
                "order_date": order_date,
                "customer_id": customer_id,
                "customer_name": customer_name,
                "shipping_method": shipping_method,
                "product_id": item["product"]["id"],
                "product_name": item["product"]["name"],
                "product_category": item["product"]["category"],
                "quantity": item["quantity"],
                "price": item["price"]
            }
            synthetic_data.append(record)
    return synthetic_data

# Updated generate function to track data size
def generate(stream_name, kinesis_client, data):
    total_data_size = 0
    for record in data:
        try:
            # Prepare the data to be sent
            data_to_send = {
                "order_id": record["order_id"],
                "order_date": record["order_date"],
                "customer_id": record["customer_id"],
                "customer_name": record["customer_name"],
                "shipping_method": record["shipping_method"],
                "product_id": record["product_id"],
                "product_name": record["product_name"],
                "product_category": record["product_category"],
                "quantity": record["quantity"],
                "price": record["price"]
            }
            partition_key = str(data_to_send["product_id"])

            # Calculate the size of the data to be sent
            data_size = len(json.dumps(data_to_send).encode('utf-8'))
            total_data_size += data_size

            # Stop if approximately 2GB of data has been sent
            if total_data_size >= 2e9:  # 2GB in bytes
                print("Reached 2GB data limit. Stopping data generation.")
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