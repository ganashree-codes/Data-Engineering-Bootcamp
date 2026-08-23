import json
import os
from kafka import KafkaConsumer

# 1. Define local storage folder and file path
output_folder = r"C:\Kafka\Output\sqlite_data"
os.makedirs(output_folder, exist_ok=True)
local_file_path = os.path.join(output_folder, 'sqlite_data.json')

# 2. Set up Kafka Consumer with a timeout
consumer = KafkaConsumer(
    'mysql-topic',                                 # Topic name matching producer
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='sqlite-to-local-group',              # Consumer group ID
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    consumer_timeout_ms=5000                       # Stops after 5s of no new messages
)

print("Reading from Kafka and saving to local storage...")

# 3. Read messages from Kafka and write JSON lines to local file
record_count = 0
with open(local_file_path, 'w', encoding='utf-8') as writer:
    for message in consumer:
        json_record = json.dumps(message.value)
        writer.write(json_record + '\n')
        record_count += 1

print(f"Successfully wrote {record_count} SQLite records to local file: {local_file_path}")