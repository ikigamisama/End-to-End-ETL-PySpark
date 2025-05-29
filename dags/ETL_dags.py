import os

from dotenv import load_dotenv
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

from functions.retail.extract.load_data import data_ingestion
from functions.retail.load.duck_db import execute_duckdb_etl

load_dotenv()

default_args = {
    'owner': 'ikigami',
    'depends_on_past': False,
}

s3_config = {
    "spark.master": "local[*]",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ACCESS_KEY", "minioLocalAccessKey"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_SECRET_KEY", "minioLocalSecretKey123"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    # Additional S3A configurations for better performance and reliability
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.attempts.maximum": "3",
    "spark.hadoop.fs.s3a.connection.establish.timeout": "5000",
    "spark.hadoop.fs.s3a.connection.timeout": "200000",
    # Buffer configurations for better performance
    "spark.hadoop.fs.s3a.buffer.dir": "/tmp",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.fast.upload.buffer": "disk",
    "spark.hadoop.fs.s3a.multipart.size": "104857600",  # 100MB
    "spark.hadoop.fs.s3a.multipart.threshold": "2147483647",  # 2GB
}

with DAG(
    "etl_spark",
    description="A DAG to execute ETL with Spark",
    schedule=None,
    default_args=default_args,
    start_date=datetime(2025, 5, 1),
    tags=['ETL', 'pyspark'],
    catchup=False,
) as dag:
    analytics_bucket = "etl-dag"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=analytics_bucket, aws_conn_id="aws_default"
    )

    extract_init = EmptyOperator(task_id="extract_method")

    extract_data_ingestion = PythonOperator(
        task_id='extract_data_ingestion',
        python_callable=data_ingestion
    )

    merge_data_ingestion = SparkSubmitOperator(
        task_id="merge_data_ingestion",
        application="/opt/airflow/dags/functions/retail/extract/merge_data_extraction.py",
        conn_id="spark_default",
        verbose=True,
        name="merge-data-ingestion",
        conf=s3_config,
        driver_memory="2g",
        deploy_mode="client"
    )

    transform_init = EmptyOperator(task_id="transform_method")

    lookup_data_task = SparkSubmitOperator(
        task_id="lookup_data_task",
        application="/opt/airflow/dags/functions/retail/transform/lookup_data/transform_lookup_data.py",
        conn_id="spark_default",
        verbose=True,
        name="transform_lookup_data",
        conf=s3_config,
        driver_memory="2g",
        deploy_mode="client"
    )

    with TaskGroup("core_entity_transform_task", tooltip="Spliting the Data into a Core Entity RDBMS Type") as core_entity_transform_task:
        transform_address = SparkSubmitOperator(
            task_id="transform_address",
            application="/opt/airflow/dags/functions/retail/transform/core_entity_data/transform_address_core.py",
            conn_id="spark_default",
            verbose=True,
            name="transform_core_address_data",
            conf=s3_config,
            driver_memory="2g",
            deploy_mode="client"
        )

        transform_customers = SparkSubmitOperator(
            task_id="transform_customers",
            application="/opt/airflow/dags/functions/retail/transform/core_entity_data/transform_customers_core.py",
            conn_id="spark_default",
            verbose=True,
            name="transform_core_customers_data",
            conf=s3_config,
            driver_memory="2g",
            deploy_mode="client"
        )

        transform_products = SparkSubmitOperator(
            task_id="transform_products",
            application="/opt/airflow/dags/functions/retail/transform/core_entity_data/transform_products_core.py",
            conn_id="spark_default",
            verbose=True,
            name="transform_core_products_data",
            conf=s3_config,
            driver_memory="2g",
            deploy_mode="client"
        )

        transform_transactions = SparkSubmitOperator(
            task_id="transform_transactions",
            application="/opt/airflow/dags/functions/retail/transform/core_entity_data/transform_transactions_core.py",
            conn_id="spark_default",
            verbose=True,
            name="transform_core_transactions_data",
            conf=s3_config,
            driver_memory="2g",
            deploy_mode="client"
        )

        transform_address >> transform_customers >> transform_products >> transform_transactions

    load_init = EmptyOperator(task_id="load_method")

    duck_db_load = PythonOperator(
        task_id='duck_db_load',
        python_callable=execute_duckdb_etl
    )

    create_s3_bucket >> extract_init
    extract_init >> extract_data_ingestion >> merge_data_ingestion >> transform_init
    transform_init >> lookup_data_task >> core_entity_transform_task >> load_init
    load_init >> duck_db_load
