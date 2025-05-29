import sys
from pyspark.sql import SparkSession

from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col, monotonically_increasing_id, row_number, current_timestamp, concat_ws, to_timestamp


def main():
    spark = None
    try:
        spark = SparkSession.builder \
            .appName('transform_core_customers_data') \
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
            "spark.app.name", "transform_core_customers_data")

        print("Reading data from S3...")
        raw_data = spark.read.parquet(
            "s3a://etl-dag/bronze/data/retail_data_parquet")

        print("Start Create for Customers Core Data . . . .")

        segments_df = spark.read.parquet(
            "s3a://etl-dag/silver/data/lookup/segment/")

        incomes_df = spark.read.parquet(
            "s3a://etl-dag/silver/data/lookup/income_level/")

        customers_base = raw_data.select(
            'Customer_ID', 'Name', 'Email', 'Phone', 'Age', 'Gender', 'Income', 'Customer_Segment')
        customers_base = customers_base.join(
            incomes_df, customers_base["Income"] == incomes_df["Income_Level"], how="left")
        customers_base = customers_base.join(
            segments_df, customers_base["Customer_Segment"] == segments_df["Segment_Name"], how="left")

        customers_base.select('Customer_ID').dropna(
        ).rdd.flatMap(lambda row: row).collect()

        customers_df = customers_base.select(
            col("Customer_ID").cast("int"),
            col("Name"),
            col("Email"),
            col("Phone").cast("string"),
            col("Age").cast("int"),
            col("Gender"),
            col("Income_Level_ID"),
            col("Segment_ID").alias("Customer_Segment_ID"),
            current_timestamp().alias("Created_Date"),
            current_timestamp().alias("Updated_Date")
        )
        customers_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/core_entity/customers/")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
