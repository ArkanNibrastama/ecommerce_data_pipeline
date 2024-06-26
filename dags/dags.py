from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow import DAG
import pendulum.day
from ingest_data import main as data_ingestion
from datetime import datetime, timedelta, timezone
import pendulum

dateyesterday = (datetime.now().astimezone(timezone(timedelta(hours=7)))-timedelta(days=1)).strftime("%Y-%m-%d")

args = {
    "owner" : "arkan"
}

with DAG(
    dag_id='ecommerce-data-pipeline',
    default_args=args,
    schedule_interval="1 0 * * *", #running dag every 00:01 AM every day
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Bangkok")
) as dag:
    
    ingest_data = PythonOperator(
        task_id='data_ingestion',
        python_callable=data_ingestion
    )

    transform_data = SparkSubmitOperator(
        application='/opt/airflow/dags/transform_data.py',
        conn_id="spark_default",
        task_id="data_transformation"
    )

    data_quality_check = BashOperator(
        task_id = "data_quality_check",
        bash_command="cd /opt/airflow/dags && pytest"
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id = "load_to_bq",
        gcp_conn_id="gcp_conn",
        bucket="arkan-ecommerce-data-pipeline-transformed",
        source_objects=f"{dateyesterday}/*.csv",
        source_format="csv",
        destination_project_dataset_table="web.orders",
        write_disposition="WRITE_APPEND"
    )

    ingest_data >> transform_data >> data_quality_check >> load_to_bq