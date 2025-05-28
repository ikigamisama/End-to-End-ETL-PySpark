import sys
from pyspark.sql.functions import col, count, sum, avg

from pyspark.sql import SparkSession


def main():
    spark = None
    try:
        spark = SparkSession.builder \
            .appName('split_transaction') \
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
        df = spark.read.csv("s3a://etl-dag/bronze/data/retail_data.csv",
                            header=True, inferSchema=True)

        print(f"Total records: {df.count()}")

        # Cast necessary columns
        df = df.withColumn("Transaction_ID", col("Transaction_ID").cast("string")) \
            .withColumn("Customer_ID", col("Customer_ID").cast("string")) \
            .withColumn("Amount", col("Amount").cast("double")) \
            .withColumn("Total_Amount", col("Total_Amount").cast("double")) \
            .withColumn("Ratings", col("Ratings").cast("double"))

        # Select transaction static data (assuming Transaction_ID is unique)
        transaction_static = df.select(
            "Transaction_ID", "Customer_ID", "Date", "Year", "Month", "Time",
            "Total_Purchases", "Amount", "Total_Amount", "Product_Category",
            "Product_Brand", "Product_Type", "Feedback", "Shipping_Method",
            "Payment_Method", "Order_Status", "Ratings", "products"
        ).dropDuplicates(["Transaction_ID"])

        print(f"Unique transactions: {transaction_static.count()}")

        # Transaction aggregation by Year and Month
        transaction_agg = df.groupBy("Year", "Month").agg(
            count("Transaction_ID").alias("total_transactions"),
            sum("Amount").alias("total_revenue"),
            avg("Ratings").alias("avg_ratings")
        )

        print(f"Monthly aggregations: {transaction_agg.count()}")

        # Write static transaction data
        print("Writing static transaction data to S3...")
        transaction_static.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/transactions/static/")

        # Write aggregated transaction data
        print("Writing aggregated transaction data to S3...")
        transaction_agg.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/transactions/aggregated/")

        print("Transaction processing completed successfully!")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
