from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

transactions = {}

for message in consumer:
    transaction = message.value

    user_id = transaction["user_id"]
    timestamp = datetime.fromisoformat(transaction["timestamp"])

    if user_id not in transactions:
        transactions[user_id] = []

    transactions[user_id].append(timestamp)

    recent = []

    for t in transactions[user_id]:
        if t >= timestamp - timedelta(seconds=60):
            recent.append(t)

    transactions[user_id] = recent

    if len(recent) > 3:
        print(f"ALERT: {user_id}")
