from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, count, avg, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Initialize Spark Session with Kafka Connector
spark = SparkSession.builder \
    .appName("EuropeATMStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Exact Schema Matching Your Kafka Payload
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("atm_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True)
])

# 3. Read Stream from Kafka Topic
kafka_raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "europe_atm_transactions") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse JSON Payload & Cast Amount to Numeric Format
parsed_stream = kafka_raw_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("amount", col("amount").cast(DoubleType()))

# 5. Fraud Detection: Flag Withdrawals > €3,000
fraud_stream = parsed_stream \
    .filter((col("transaction_type") == "Withdrawal") & (col("amount") > 3000)) \
    .select("transaction_id", "country", "city", "amount", "transaction_type") \
    .withColumn("alert_status", expr("'SUSPICIOUS WITHDRAWAL (> €3,000)'"))

# Output Fraud Alerts to Console
fraud_query = fraud_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .queryName("FraudAlerts") \
    .start()

# 6. Real-Time Analytics (Total Txns, Total Amount, Avg Amount)
analytics_stream = parsed_stream.groupBy() \
    .agg(
        count("transaction_id").alias("Total_Transactions"),
        _sum("amount").alias("Total_Withdrawal_Amount"),
        avg("amount").alias("Average_Withdrawal_Amount")
    )

# Output Aggregations to Console
analytics_query = analytics_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .queryName("RealTimeAnalytics") \
    .start()

# Keep streams running
spark.streams.awaitAnyTermination()