from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd

PROCESSED_FILE_customers = "/opt/airflow/data/processed/cleaned_customers.csv"
PROCESSED_FILE_orders = "/opt/airflow/data/processed/cleaned_orders.csv"
PROCESSED_FILE_products = "/opt/airflow/data/processed/cleaned_products.csv"
PROCESSED_FILE_clickstream = "/opt/airflow/data/processed/cleaned_clickstream.csv"


default_args = {
    "owner": "analytics_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

def check_data_quality(**context):
    df_customers = pd.read_csv(PROCESSED_FILE_customers)
    df_orders = pd.read_csv(PROCESSED_FILE_orders)
    df_products = pd.read_csv(PROCESSED_FILE_products)
    df_clickstream = pd.read_csv(PROCESSED_FILE_clickstream)

    total_profit_margin = df_products["profit_margin"].sum()
    if total_profit_margin < 100000:
        context["ti"].xcom_push(
            key="alert_message",
            value=f"Total profit margin is less than expected: {total_profit_margin}"
        )

    unpaid_orders = df_orders[(df_orders["payment_status"] == "Unpaid") | (df_orders["payment_status"] == "Failed")]
    if not unpaid_orders.empty:
        context["ti"].xcom_push(
            key="alert_message",
            value=f"There are {len(unpaid_orders)} unpaid or failed orders."
        )

with DAG(
    dag_id="customer_sales_quality_alert_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
) as dag:

    data_quality_check_task = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality
    )

    send_email = EmailOperator(
        task_id="send_email_alert",
        to="gana.drk@gmail.com",
        from_email="gana.drk@gmail.com",
        subject="Customer orders Sales Alert",
        html_content="""
        <h3>Sales Alert Triggered</h3>
        <p>Please check today's sales report.</p>
        """,
        retries=3,
        )

    data_quality_check_task >> send_email