import sys
from pyspark.sql import SparkSession

from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col, monotonically_increasing_id, row_number, current_timestamp, concat_ws, to_timestamp


def main():
    spark = None
    try:
        spark = SparkSession.builder \
            .appName('split_customer') \
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
            "spark.app.name", "transform_core_address_data")

        print("Reading data from S3...")
        raw_data = spark.read.parquet(
            "s3a://etl-dag/bronze/data/retail_data_parquet")

        print("Start Create for Address Core Data . . . .")

        cities_df = spark.read.parquet(
            "s3a://etl-dag/silver/data/lookup/city/")
        addresses_base = raw_data.select(
            'Customer_ID', 'Address', 'City', 'State/Province', 'Zipcode', 'Country')
        addresses_base = addresses_base.join(
            cities_df, addresses_base['City'] == cities_df['City_Name'], how='left')

        addresses_df = addresses_base.select(
            col("Customer_ID").cast("int"),
            col("Address").alias("Street_Address"),
            col("City_ID"),
            col("Zipcode").cast("int"),
            current_timestamp().alias("Created_Date"),
            current_timestamp().alias("Updated_Date")
        )

        addresses_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/core_entity/address/")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
