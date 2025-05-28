import sys
from pyspark.sql.functions import col, count, sum, avg

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
        df = spark.read.csv("s3a://etl-dag/bronze/data/retail_data.csv",
                            header=True, inferSchema=True)

        print(f"Total records: {df.count()}")

        # Clean and cast data types
        df = df.withColumn("Customer_ID", col("Customer_ID").cast("string")) \
            .withColumn("Transaction_ID", col("Transaction_ID").cast("string")) \
            .withColumn("Phone", col("Phone").cast("string")) \
            .withColumn("Zipcode", col("Zipcode").cast("string")) \
            .withColumn("Year", col("Year").cast("int")) \
            .withColumn("Age", col("Age").cast("int")) \
            .withColumn("Amount", col("Amount").cast("double")) \
            .withColumn("Ratings", col("Ratings").cast("double"))

        # Extract customer static data
        customer_static = df.select(
            "Customer_ID", "Name", "Email", "Phone", "Address", "City", "State", "Zipcode",
            "Country", "Age", "Gender", "Income", "Customer_Segment"
        ).dropDuplicates(["Customer_ID"])

        print(f"Unique customers: {customer_static.count()}")

        # Calculate customer behavior
        customer_behavior = df.groupBy("Customer_ID").agg(
            count("*").alias("total_orders"),
            sum("Amount").alias("total_spent"),
            avg("Ratings").alias("avg_rating")
        )

        # Join static and behavior data
        customer_df = customer_static.join(
            customer_behavior, on="Customer_ID", how="left")

        print("Writing customer data to S3...")
        customer_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/customers/")

        print("Customer processing completed successfully!")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
