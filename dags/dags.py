from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from ingest_data import main as data_ingestion
import os

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/service_acc_key.json"

dateyesterday = "2024-05-25"

args = {
    "owner" : "arkan"
}

with DAG(
    dag_id='ecommerce-data-pipeline',
    default_args=args,
    schedule_interval="1 0 * * *", #running dag every 00:01 AM every day
    start_date=days_ago(1)
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
