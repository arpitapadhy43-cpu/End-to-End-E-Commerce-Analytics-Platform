import json
import os
import time
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

CHECKPOINT_FILE = os.environ["CHECKPOINT_FILE"]

def read_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return int(f.read().strip())
    return 0

def save_checkpoint(index):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(index))

producer = KafkaProducer(
    bootstrap_servers=os.environ["BOOTSTRAP_SERVER"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",
    retries=5,
)

ORDER_TOPIC = os.environ["ORDER_TOPIC"]
CUSTOMER_TOPIC = os.environ["CUSTOMER_TOPIC"]

df = pd.read_excel(os.environ["SOURCE_DATA_CSV_PATH"])
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
df = df.sort_values("InvoiceDate").reset_index(drop=True)

start_index = read_checkpoint()

seen_customers = set()
previous_time = None

for i in range(start_index, len(df)):
    row = df.iloc[i]

    current_time = row["InvoiceDate"]

    if previous_time:
        delta = (current_time - previous_time).total_seconds()
        time.sleep(min(delta * 0.01, 1))

    previous_time = current_time

    invoice_id = str(row["Invoice"])
    customer_id = row["Customer ID"]
    country = row["Country"]

    if pd.notna(customer_id) and customer_id not in seen_customers:
        customer_event = {
            "event_type": "customer_activity",
            "customer_id": int(customer_id),
            "country": country,
            "first_seen_time": str(current_time),
            "ingestion_time": str(datetime.utcnow())
        }
        producer.send(CUSTOMER_TOPIC, key=str(customer_id), value=customer_event)
        seen_customers.add(customer_id)

    if row["Quantity"] > 0:
        event = {
            "event_type": "order_item_created",
            "invoice_id": invoice_id,
            "stock_code": row["StockCode"],
            "description": row["Description"],
            "quantity": int(row["Quantity"]),
            "unit_price": float(row["Price"]),
            "country": country,
            "customer_id": int(customer_id) if pd.notna(customer_id) else None,
            "event_time": str(current_time),
            "total_amount": float(row["Quantity"] * row["Price"]),
            "ingestion_time": str(datetime.utcnow())
        }
        producer.send(ORDER_TOPIC, key=invoice_id, value=event)

    elif row["Quantity"] < 0:
        event = {
            "event_type": "order_item_cancelled",
            "invoice_id": invoice_id,
            "stock_code": row["StockCode"],
            "description": row["Description"],
            "quantity": int(abs(row["Quantity"])),
            "unit_price": float(row["Price"]),
            "country": country,
            "customer_id": int(customer_id) if pd.notna(customer_id) else None,
            "event_time": str(current_time),
            "total_amount": float(abs(row["Quantity"]) * row["Price"]),
            "ingestion_time": str(datetime.utcnow())
        }
        producer.send(ORDER_TOPIC, key=invoice_id, value=event)

    if i % 1000 == 0:
        producer.flush()
        save_checkpoint(i)

producer.flush()
save_checkpoint(len(df))
producer.close()