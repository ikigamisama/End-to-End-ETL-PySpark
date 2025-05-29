import sys
from pyspark.sql import SparkSession


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
        spark._jsc.hadoopConfiguration().set("spark.app.name", "split_transactions")

        print("Reading data from S3...")
        s3_input_path = "s3a://etl-dag/bronze/data/raw/retail_data_*.csv"

        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(s3_input_path)

        print(f"Total records: {df.count()}")

        s3_output_path = "s3a://etl-dag/bronze/data/retail_data_parquet/"

        print("Writing merged data to S3...")
        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .parquet(s3_output_path)

        print("Data merge completed successfully!")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
