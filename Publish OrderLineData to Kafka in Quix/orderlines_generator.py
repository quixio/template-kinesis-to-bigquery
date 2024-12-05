import datetime
import random
from rocksdict import Rdict
import os

class OrderLinesGenerator:
    """
    Generates synthetic order data for streaming to Kafka.
    Creates individual records for each order item rather than nested items.
    """

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

    def __init__(self):
        self.stop = False
        self.max_data_size = 2e9  # 2GB limit
        
        # Initialize RocksDB state
        state_dir = os.environ.get("Quix__State__Path", "state")
        state_path = os.path.join(state_dir, "orderlines_generator")
        os.makedirs(state_path, exist_ok=True)
        self.state_db = Rdict(state_path)
        
        # Initialize state counters from persistent storage or defaults
        self.total_data_size = self.state_db.get(b"total_data_size", 0)
        self.orderlines_generated = self.state_db.get(b"orderlines_generated", 0)

    def generate_orderline(self):
        """
        Generate individual item records for an order
        """
        if self.stop:
            return None

        # Generate order-level data
        order_id = random.randint(100, 999)
        order_date = datetime.datetime.now().strftime("%Y-%m-%d")
        customer_id = random.randint(1000, 9999)
        customer_name = random.choice(self.USER_NAMES)
        shipping_method = random.choice(self.SHIPPING_METHODS)

        # Generate 2 items for this order
        items = []
        for _ in range(2):
            product = random.choice(self.PRODUCTS)
            item = {
                "order_id": order_id,
                "order_date": order_date,
                "customer_id": customer_id,
                "customer_name": customer_name,
                "shipping_method": shipping_method,
                "product_id": product["id"],
                "product_name": product["name"],
                "product_category": product["category"],
                "quantity": random.randint(1, 3),
                "price": round(random.uniform(10.0, 100.0), 2)
            }
            items.append(item)

        # Track data size and return items
        for item in items:
            data_size = len(str(item))
            self.total_data_size += data_size

            # Update state storage
            self.state_db[b"total_data_size"] = self.total_data_size

            if self.total_data_size >= self.max_data_size:
                print("Reached 2GB data limit. Stopping data generation.")
                self.stop = True
                return None

            # Use product_id as partition key for category-based partitioning
            yield {
                "key": str(item["product_id"]),
                "value": item
            }

        self.orderlines_generated += 1
        self.state_db[b"orderlines_generated"] = self.orderlines_generated
        self.state_db.flush()  # Ensure state is persisted

    def __del__(self):
        """Cleanup RocksDB on object destruction"""
        if hasattr(self, 'state_db'):
            self.state_db.close()