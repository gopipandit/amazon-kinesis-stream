import boto3
import json
import time
import random
from datetime import datetime

# AWS Kinesis stream config
STREAM_NAME = "e-commerce-log"
REGION_NAME = "ap-south-1"

# Create Kinesis client
kinesis_client = boto3.client("kinesis", region_name=REGION_NAME)

# Sample data generators
def generate_activity():
    activities = ["view_product", "add_to_cart", "remove_from_cart", "search", "checkout"]
    products = ["Laptop", "Smartphone", "Headphones", "Keyboard", "Monitor"]
    categories = ["Electronics", "Accessories", "Office", "Gaming"]

    return {
        "event_type": "activity",
        "timestamp": datetime.utcnow().isoformat(),
        "activity": random.choice(activities),
        "product_name": random.choice(products),
        "category": random.choice(categories),
        "price": round(random.uniform(10, 2000), 2)
    }

def generate_order():
    products = ["Laptop", "Smartphone", "Headphones", "Keyboard", "Monitor"]
    cart_items = [
        {
            "product_name": random.choice(products),
            "quantity": random.randint(1, 3),
            "price": round(random.uniform(10, 2000), 2)
        }
        for _ in range(random.randint(1, 3))
    ]

    return {
        "event_type": "order",
        "timestamp": datetime.utcnow().isoformat(),
        "order_id": random.randint(1000, 9999),
        "cart_items": cart_items,
        "total_price": sum(item["price"] * item["quantity"] for item in cart_items)
    }

# Send events to Kinesis
def send_to_kinesis(data):
    try:
        response = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(data),
            PartitionKey=str(random.randint(1, 1000))
        )
        print(f"✅ Sent to Kinesis: {data['event_type']} at {data['timestamp']}")
    except Exception as e:
        print(f"❌ Failed to send record: {e}")

if __name__ == "__main__":
    print("🚀 Starting Kinesis Producer... Press Ctrl+C to stop.")
    while True:
        # Randomly pick between activity or order
        event = generate_activity() if random.random() < 0.7 else generate_order()
        send_to_kinesis(event)
        time.sleep(1)  # 1 second delay between records
