import sys
from pyspark.sql import SparkSession

from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col, monotonically_increasing_id, row_number, current_timestamp, concat_ws, to_timestamp


def main():
    spark = None
    try:
        spark = SparkSession.builder \
            .appName('transform_core_products_data') \
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
            "spark.app.name", "transform_core_products_data")

        print("Reading data from S3...")
        raw_data = spark.read.parquet(
            "s3a://etl-dag/bronze/data/retail_data_parquet")

        print("Start Create for Products Core Data . . . .")

        categories_df = spark.read.parquet(
            "s3a://etl-dag/silver/data/lookup/cateogry/")

        brands_df = spark.read.parquet(
            "s3a://etl-dag/silver/data/lookup/brand/")

        product_type_df = spark.read.parquet(
            "s3a://etl-dag/silver/data/lookup/product_type/")

        products_base = raw_data.select(
            'products', 'Product_Type', 'Product_Brand', 'Product_Category')
        products_base = products_base.join(
            product_type_df, products_base['Product_Type'] == product_type_df['Product_Type_Name'], how='left')
        products_base = products_base.join(
            brands_df, products_base['Product_Brand'] == brands_df['Brand_Name'], how='left')
        products_base = products_base.join(categories_df.select(
            "Category_Name"), products_base['Product_Category'] == categories_df['Category_Name'], how='left')

        products_df = products_base.select(
            col("products").alias("Product_Name"),
            col("Product_Type_ID"),
            col("Brand_ID"),
            col("Category_ID"),
            current_timestamp().alias("Created_Date"),
            current_timestamp().alias("Updated_Date")
        ).withColumn("Product_ID", row_number().over(Window.orderBy("Product_Name")))

        products_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/core_entity/products/")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
