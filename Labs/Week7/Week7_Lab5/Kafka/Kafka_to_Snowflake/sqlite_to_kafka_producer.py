import sqlite3
from kafka import KafkaProducer
import json

# 1. Connect to your local SQLite database file
# Replace 'your_database.db' with the actual path to your SQLite database file
conn = sqlite3.connect('mydatabase.db')

# 2. Configure row_factory so query results are returned as dictionaries
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# 3. Fetch data from your SQLite table
cursor.execute("SELECT * FROM employees")
# Convert each Row object to a standard Python dict for JSON serialization
rows = [dict(row) for row in cursor.fetchall()]

# 4. Set up Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 5. Send rows to Kafka topic
for row in rows:
    producer.send('mysql-topic', value=row)

producer.flush()
conn.close()

print("Data sent to Kafka successfully.")