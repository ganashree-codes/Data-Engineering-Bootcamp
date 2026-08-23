import snowflake.connector
from kafka import KafkaConsumer

# 1. Connect to Snowflake
conn = snowflake.connector.connect(
    user='***********',
    password='***************',
    account='***************',
    warehouse='LAB_WH_GMD',
    database='RETAIL_DB_GMD',
    schema='RAW'
)
cursor = conn.cursor()

# Ensure target table exists with VARIANT column
cursor.execute("""
    CREATE TABLE IF NOT EXISTS sqlite_kafka_records (
        record_data VARIANT,
        ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
""")

# 2. Configure Kafka Consumer
consumer = KafkaConsumer(
    'mysql-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='snowflake-ingest-v4',  # Fresh consumer group ID
    consumer_timeout_ms=5000
)

# 3. Read messages from Kafka and insert into Snowflake
total_records = 0
for message in consumer:
    json_str = message.value.decode('utf-8')
    
    # PARSE_JSON works reliably with single cursor.execute parameter binding
    cursor.execute(
        "INSERT INTO sqlite_kafka_records (record_data) SELECT PARSE_JSON(%s)",
        (json_str,)
    )
    total_records += 1
    print(f"Ingested record {total_records}: {json_str}")

# Commit all inserted records together
conn.commit()
cursor.close()
conn.close()

if total_records > 0:
    print(f"\nDone! Successfully ingested {total_records} records into Snowflake.")
else:
    print("No new records found in Kafka topic.")