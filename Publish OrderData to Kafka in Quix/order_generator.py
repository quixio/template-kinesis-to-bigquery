import datetime
import random


class OrderGenerator:
    """
    Generates synthetic order data for streaming to Kafka
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
        self.total_data_size = 0
        self.max_data_size = 2e9  # 2GB limit

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

        if self.total_data_size >= self.max_data_size:
            print("Reached 2GB data limit. Stopping data generation.")
            self.stop = True
            return None

        return {
            "key": str(order_id),
            "value": order
        }