import os

from dotenv import load_dotenv
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

from functions.retail.extract import data_ingestion

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

    with TaskGroup("data_transform_extract_split", tooltip="Splitting the retail data into customer, transaction, and product datasets") as data_transform_extract_split:
        split_customer_task = SparkSubmitOperator(
            task_id="split_customer",
            application="/opt/airflow/dags/spark_jobs/split_customer.py",
            conn_id="spark_default",
            verbose=True,
            name="split-customer-local",
            conf=s3_config,
            driver_memory="2g",
            deploy_mode="client"
        )

        split_transaction_task = SparkSubmitOperator(
            task_id="split_transaction",
            application="/opt/airflow/dags/spark_jobs/split_transaction.py",
            conn_id="spark_default",
            verbose=True,
            name="split-transaction-local",
            conf=s3_config,
            driver_memory="2g",
            deploy_mode="client"
        )

        split_product_task = SparkSubmitOperator(
            task_id="split_product",
            application="/opt/airflow/dags/spark_jobs/split_product.py",
            conn_id="spark_default",
            verbose=True,
            name="split-product-local",
            conf=s3_config,
            driver_memory="2g",
            deploy_mode="client",
        )

        split_customer_task >> split_transaction_task >> split_product_task

    create_s3_bucket >> extract_init >> extract_data_ingestion >> data_transform_extract_split
