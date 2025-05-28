import sys
from pyspark.sql.functions import col, avg, sum, countDistinct

from pyspark.sql import SparkSession


def main():
    spark = None
    try:
        spark = SparkSession.builder \
            .appName('split_product') \
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

        # Read data
        df = spark.read.csv("s3a://etl-dag/bronze/data/retail_data.csv",
                            header=True, inferSchema=True)

        # Static product info
        product_static = df.select(
            col("products").alias("product_name"),
            "Product_Category",
            "Product_Brand",
            "Product_Type"
        ).dropDuplicates()

        # Aggregated metrics
        product_agg = df.groupBy("products").agg(
            countDistinct("Transaction_ID").alias("total_transactions"),
            countDistinct("Customer_ID").alias("unique_customers"),
            sum("Total_Amount").alias("total_revenue"),
            avg("Ratings").alias("avg_rating")
        ).withColumnRenamed("products", "product_name")

        # Write outputs
        product_static.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/products/static/")
        product_agg.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/products/aggregated/")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
