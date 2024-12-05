import datetime
import random
from rocksdict import Rdict
import os

class OrderGenerator:
    """
    Generates synthetic order data for streaming to Kafka with RocksDB state persistence
    """

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

    def __init__(self):
        self.stop = False
        self.max_data_size = 2e9  # 2GB limit
        
        # Initialize RocksDB state
        state_dir = os.environ.get("Quix__State__Path", "state")
        state_path = os.path.join(state_dir, "order_generator")
        os.makedirs(state_path, exist_ok=True)
        self.state_db = Rdict(state_path)
        
        # Initialize total_data_size from state or default to 0
        self.total_data_size = self.state_db.get(b"total_data_size", 0)

    def generate_order(self):
        """
        Generate a single order event
        """
        if self.stop:
            return None

        customer_id = random.randint(1000, 9999)
        order_id = random.randint(100, 999)

        order = {
            "order": {
                "id": order_id,
                "date": datetime.datetime.now().strftime("%Y-%m-%d"),
                "customer": {
                    "id": customer_id,
                    "name": random.choice(self.USER_NAMES),
                    "contact": {
                        "email": random.choice(self.EMAILS),
                        "phone": random.choice(self.PHONES)
                    }
                },
                "shipping": {
                    "address": {
                        "street": f"{random.randint(1, 999)} Maple Avenue",
                        "city": random.choice(self.CITIES),
                        "postal_code": f"{random.randint(10000, 99999)}",
                        "country": random.choice(self.COUNTRIES)
                    },
                    "method": random.choice(self.SHIPPING_METHODS),
                    "cost": round(random.uniform(5.0, 20.0), 2)
                },
                "items": [
                    {
                        "product": random.choice(self.PRODUCTS),
                        "quantity": random.randint(1, 3),
                        "price": round(random.uniform(10.0, 100.0), 2)
                    },
                    {
                        "product": random.choice(self.PRODUCTS),
                        "quantity": random.randint(1, 3),
                        "price": round(random.uniform(10.0, 100.0), 2)
                    }
                ]
            }
        }

        # Calculate total cost
        total_cost = sum(item["quantity"] * item["price"] for item in order["order"]["items"])
        total_cost += order["order"]["shipping"]["cost"]
        order["order"]["total_cost"] = round(total_cost, 2)

        # Track data size
        data_size = len(str(order))
        self.total_data_size += data_size
        
        # Update state
        self.state_db[b"total_data_size"] = self.total_data_size
        self.state_db.flush()  # Ensure it's written to disk

        if self.total_data_size >= self.max_data_size:
            print("Reached 2GB data limit. Stopping data generation.")
            self.stop = True
            return None

        return {
            "key": str(order_id),
            "value": order
        }

    def __del__(self):
        """Cleanup RocksDB on object destruction"""
        if hasattr(self, 'state_db'):
            self.state_db.close()