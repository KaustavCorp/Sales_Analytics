import os
import random
import pandas as pd
from faker import Faker
from dotenv import load_dotenv

# --- Load .env from project root ---
# Go up three levels from utils/DataGenerator/SalesDataFaker.py to reach root.
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env")
load_dotenv(dotenv_path=env_path)

# --- Fetch paths from .env ---
BASE_PATH = os.getenv("SALES_DATA_SOURCE")


# --- Ensure directories exist ---
for path in [BASE_PATH]:
    if path:
        os.makedirs(path, exist_ok=True)

fake = Faker()

# --- Customers ---
customers = [
    {
        "customer_id": i,
        "name": fake.name(),
        "email": fake.email(),
        "region": random.choice(["North", "South", "East", "West"])
    }
    for i in range(1, 1001)
]
pd.DataFrame(customers).to_csv(os.path.join(BASE_PATH, "Customers.csv"), index=False)

# --- Products ---
products = [
    {
        "product_id": i,
        "name": fake.word().capitalize(),
        "category": random.choice(["Electronics", "Furniture", "Clothing", "Sports"]),
        "price": round(random.uniform(10, 1000), 2)
    }
    for i in range(1, 1001)
]
pd.DataFrame(products).to_csv(os.path.join(BASE_PATH, "Products.csv"), index=False)

# --- Stores ---
stores = [
    {
        "store_id": i,
        "location": fake.city(),
        "manager": fake.name()
    }
    for i in range(1, 1001)
]
pd.DataFrame(stores).to_csv(os.path.join(BASE_PATH, "Stores.csv"), index=False)

# --- Orders ---
orders = [
    {
        "order_id": i,
        "customer_id": random.randint(1, 1000),
        "store_id": random.randint(1, 1000),
        "order_date": fake.date_between(start_date="-1y", end_date="today"),
        "status": random.choice(["Completed", "Pending", "Cancelled"])
    }
    for i in range(1, 1001)
]
pd.DataFrame(orders).to_csv(os.path.join(BASE_PATH, "Orders.csv"), index=False)

# --- OrderLines ---
orderlines = [
    {
        "orderline_id": i,
        "order_id": random.randint(1, 1000),
        "product_id": random.randint(1, 1000),
        "quantity": random.randint(1, 10),
        "unit_price": round(random.uniform(10, 1000), 2),
        "discount": round(random.uniform(0, 0.3), 2)  # up to 30% discount
    }
    for i in range(1, 1001)
]
pd.DataFrame(orderlines).to_csv(os.path.join(BASE_PATH, "OrderLines.csv"), index=False)

print(f"Data generated successfully in: {BASE_PATH}")