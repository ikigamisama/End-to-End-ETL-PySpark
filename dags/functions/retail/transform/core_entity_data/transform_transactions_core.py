import sys
from pyspark.sql import SparkSession

from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col, monotonically_increasing_id, row_number, current_timestamp, concat_ws, to_timestamp


def main():
    spark = None
    try:
        spark = SparkSession.builder \
            .appName('transform_core_transactions_data') \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioLocalAccessKey") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioLocalSecretKey123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.multipart.size", "104857600") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        spark._jsc.hadoopConfiguration().set(
            "spark.app.name", "transform_core_transactions_data")

        print("Reading data from S3...")
        raw_data = spark.read.parquet(
            "s3a://etl-dag/bronze/data/retail_data_parquet")

        print("Start Create for Transactions Core Data . . . .")

        shippings_df = spark.read.parquet(
            "s3a://etl-dag/silver/data/lookup/shipping_methods/")

        payments_df = spark.read.parquet(
            "s3a://etl-dag/silver/data/lookup/payment_methods/")

        orders_df = spark.read.parquet(
            "s3a://etl-dag/silver/data/lookup/order_status/")

        transactions_base = raw_data

        transactions_base = transactions_base.join(
            shippings_df, transactions_base['Shipping_Method'] == shippings_df['Method_Name'], how='left')
        transactions_base = transactions_base.join(
            payments_df, transactions_base['Payment_Method'] == payments_df['Method_Name'], how='left')
        transactions_base = transactions_base.join(
            orders_df, transactions_base['Order_Status'] == orders_df['Status_Name'], how='left')

        transactions_df = transactions_base.select(
            col('Transaction_ID').cast("int"),
            col("Customer_ID").cast("int"),
            col('Time').alias("Transaction_DateTime"),
            col('Total_Amount'),
            col("Total_Purchases").cast("int"),
            col("Shipping_Method_ID"),
            col("Payment_Method_ID"),
            col("Status_ID").alias("Order_Status_ID"),
            current_timestamp().alias("Created_Date"),
            current_timestamp().alias("Updated_Date")
        )

        transactions_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/core_entity/transactions/")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
