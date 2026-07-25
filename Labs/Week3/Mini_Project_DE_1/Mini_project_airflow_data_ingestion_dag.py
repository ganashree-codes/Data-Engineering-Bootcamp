from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pandas as pd
from collections import Counter


raw_data_customers = "/opt/airflow/data/raw/customers.csv"
raw_data_orders = "/opt/airflow/data/raw/orders.csv"
raw_data_products = "/opt/airflow/data/raw/products.csv"
raw_data_clickstream = "/opt/airflow/data/raw/clickstream.csv"



PROCESSED_FILE_customers = "/opt/airflow/data/processed/cleaned_customers.csv"
PROCESSED_FILE_orders = "/opt/airflow/data/processed/cleaned_orders.csv"
PROCESSED_FILE_products = "/opt/airflow/data/processed/cleaned_products.csv"
PROCESSED_FILE_clickstream = "/opt/airflow/data/processed/cleaned_clickstream.csv"

default_args = {
    "owner": "customer_orders_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

def validate_sales_date():
    df_customers= pd.read_csv(raw_data_customers)
    df_orders= pd.read_csv(raw_data_orders)
    df_products= pd.read_csv(raw_data_products)
    df_clickstream= pd.read_csv(raw_data_clickstream)

    if df_customers.isnull().values.any():
        print("Warning: Null values Found in customers data. Filling with 0")

    if df_orders.isnull().values.any():
        print("Warning: Null values Found in orders data. Filling with 0")

    if df_products.isnull().values.any():
        print("Warning: Null values Found in products data. Filling with 0")

    if df_clickstream.isnull().values.any():
        print("Warning: Null values Found in clickstream data. Filling with 0")

    

    df_customers=df_customers.fillna(0)
    df_orders=df_orders.fillna(0)
    df_products=df_products.fillna(0)
    df_clickstream=df_clickstream.fillna(0)

    if ((df_orders["quantity"] < 0) | (df_orders["unit_price"] < 0)).any():
        print("Warning : Negative prices found.Dropping those rows")

    if ((df_products["cost_price"] < 0) | (df_products["selling_price"] < 0)).any():
        print("Warning : Negative prices found.Dropping those rows")

    df_orders = df_orders[(df_orders["quantity"] >= 0) & (df_orders["unit_price"] >= 0)]
    df_products = df_products[(df_products["cost_price"] >= 0) & (df_products["selling_price"] >= 0)]

    df_customers=df_customers.drop_duplicates(keep="first")
    df_orders=df_orders.drop_duplicates(keep="first")
    df_products=df_products.drop_duplicates(keep="first")
    df_clickstream=df_clickstream.drop_duplicates(keep="first")

    print("Validation successful!")
    df_customers.to_csv(PROCESSED_FILE_customers, index=False)
    df_orders.to_csv(PROCESSED_FILE_orders, index=False)
    df_products.to_csv(PROCESSED_FILE_products, index=False)
    df_clickstream.to_csv(PROCESSED_FILE_clickstream, index=False)


def transform_sales_data():
    df_orders = pd.read_csv(PROCESSED_FILE_orders)
    df_products = pd.read_csv(PROCESSED_FILE_products)
    df_clickstream = pd.read_csv(PROCESSED_FILE_clickstream)

    df_orders["Total_Sales"] = df_orders["quantity"] * df_orders["unit_price"]
    total_revenue=df_orders["Total_Sales"].sum()
    total_items_sold=df_orders["quantity"].sum()
    print(f"Total revenue from the current day sale is:{total_revenue}")
    print(f"Total items sold in the current day sale is:{total_items_sold}")


    unpaid_orders = df_orders[(df_orders["payment_status"] == "Unpaid") | (df_orders["payment_status"] == "Failed")]
    unpaid_customers = unpaid_orders["customer_id"].unique()
    print("Customers with unpaid orders:", unpaid_customers)


    df_products["profit_margin"] = df_products["selling_price"] - df_products["cost_price"]
    total_profit_margin = df_products["profit_margin"].sum()
    print(f"Total profit margin is:{total_profit_margin}")

    used_device = set(df_clickstream['device_type'])
    print(used_device)
    count_device = Counter(df_clickstream['device_type'])
    print(count_device)
    most_used_device = count_device.most_common(1)[0]
    print(f"The most used device is: {most_used_device[0]} with count: {most_used_device[1]}")


    df_orders.to_csv(PROCESSED_FILE_orders, index=False)
    df_products.to_csv(PROCESSED_FILE_products, index=False)
    df_clickstream.to_csv(PROCESSED_FILE_clickstream, index=False)


    print("Transformation completed!")
#dag runs daily at 2AM
with DAG(
    dag_id="customer_sales_ingestion_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *", #runs daily at 2AM
    catchup=False,
    default_args=default_args,
)as dag:

    wait_for_file_customers = FileSensor(
        task_id="wait_for_customers_file",
        filepath=raw_data_customers,
        poke_interval=30,
        timeout=300,
        mode="poke"
    )

    wait_for_file_orders = FileSensor(
        task_id="wait_for_orders_file",
        filepath=raw_data_orders,
        poke_interval=30,
        timeout=300,
        mode="poke"
    )

    wait_for_file_products = FileSensor(
        task_id="wait_for_products_file",
        filepath=raw_data_products,
        poke_interval=30,
        timeout=300,
        mode="poke"
    )

    wait_for_file_clickstream = FileSensor(
        task_id="wait_for_clickstream_file",
        filepath=raw_data_clickstream,
        poke_interval=30,
        timeout=300,
        mode="poke"
    )

    validate_task = PythonOperator(
        task_id="validate_sales_data",
        python_callable=validate_sales_date
    )

    transform_task = PythonOperator(
        task_id="transform_sales_data",
        python_callable=transform_sales_data
    )

    wait_for_file_customers >> wait_for_file_orders >> wait_for_file_products >> wait_for_file_clickstream >> validate_task >> transform_task
