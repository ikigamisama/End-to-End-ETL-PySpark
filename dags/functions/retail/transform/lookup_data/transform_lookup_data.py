import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number


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
        spark._jsc.hadoopConfiguration().set("spark.app.name", "transform_lookup_data")

        print("Reading data from S3...")
        raw_data = spark.read.parquet(
            "s3a://etl-dag/bronze/data/retail_data_parquet")

        print("Start Create for Lookup Data . . . .")

        print("Creating Dataframe for Country")
        countries = raw_data.select("Country").where(
            col("Country").isNotNull()).distinct().rdd.flatMap(lambda row: row).collect()
        countries_with_id = [Row(Country_ID=i+1, Country_Name=country)
                             for i, country in enumerate(countries)]

        countries_df = spark.createDataFrame(countries_with_id)
        countries_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/lookup/country/")

        print("Creating Dataframe for States/Province Table")
        states_data = raw_data.select(
            "State/Province", "Country").dropna().distinct()
        states_data = states_data.join(
            countries_df,
            states_data["Country"] == countries_df["Country_Name"],
            how="inner"
        )

        states_provinces_df = states_data.select(
            row_number().over(Window.orderBy("State/Province")).alias("State_ID"),
            col("State/Province").alias("State_Name"),
            col("Country_ID")
        )

        states_provinces_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/lookup/state_provinces/")

        print("Creating Dataframe for Cities Table")
        cities_data = raw_data.select(
            "City", "State/Province").dropna().distinct()
        cities_data = cities_data.join(
            states_provinces_df,
            cities_data['State/Province'] == states_provinces_df['State_Name'],
            how="inner"
        )

        cities_df = cities_data.select(
            row_number().over(Window.orderBy("City")).alias("City_ID"),
            col("City").alias("City_Name"),
            col("State_ID")
        )

        cities_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/lookup/city/")

        print("Creating Dataframe for Categories Table")
        categories = raw_data.select("Product_Category").dropna(
        ).distinct().rdd.flatMap(lambda row: row).collect()
        categories_with_id = [Row(Category_ID=i+1, Category_Name=category)
                              for i, category in enumerate(categories)]

        categories_df = spark.createDataFrame(categories_with_id)
        categories_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/lookup/cateogry/")

        print("Creating Dataframe for Brands Table")
        brands = raw_data.select("Product_Brand").dropna(
        ).distinct().rdd.flatMap(lambda row: row).collect()
        brands_with_id = [Row(Brand_ID=i+1, Brand_Name=brand)
                          for i, brand in enumerate(brands)]

        brands_df = spark.createDataFrame(brands_with_id)
        brands_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/lookup/brand/")

        print("Creating Dataframe for Product Type Table")
        products_data = raw_data.select(
            "Product_Type", "Product_Category").dropna().distinct()
        products_data = products_data.join(
            categories_df, products_data['Product_Category'] == categories_df['Category_Name'], how="inner")

        products_df = products_data.select(
            row_number().over(Window.orderBy("Product_Type")).alias("Product_Type_ID"),
            col("Product_Type").alias("Product_Type_Name"),
            col("Category_ID")
        )

        products_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/lookup/product_type/")

        print("Creating Dataframe for Segments Table")
        segments = raw_data.select("Customer_Segment").dropna(
        ).distinct().rdd.flatMap(lambda row: row).collect()
        segments_with_id = [Row(Segment_ID=i+1, Segment_Name=segment)
                            for i, segment in enumerate(segments)]

        segments_df = spark.createDataFrame(segments_with_id)
        segments_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/lookup/segment/")

        print("Creating Dataframe for Income Levels Table")
        incomes = raw_data.select("Income").dropna(
        ).distinct().rdd.flatMap(lambda row: row).collect()
        incomes_with_id = [Row(Income_Level_ID=i+1, Income_Level=income)
                           for i, income in enumerate(incomes)]

        incomes_df = spark.createDataFrame(incomes_with_id)
        incomes_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/lookup/income_level/")

        print("Creating Dataframe for Income Shipping Methods Table")
        shippings = raw_data.select("Shipping_Method").dropna(
        ).distinct().rdd.flatMap(lambda row: row).collect()
        shippings_with_id = [Row(Shipping_Method_ID=i+1, Method_Name=shipping)
                             for i, shipping in enumerate(shippings)]

        shippings_df = spark.createDataFrame(shippings_with_id)
        shippings_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/lookup/shipping_methods/")

        print("Creating Dataframe for Payment Methods Table")
        payments = raw_data.select("Payment_Method").dropna(
        ).distinct().rdd.flatMap(lambda row: row).collect()
        payments_with_id = [Row(Payment_Method_ID=i+1, Method_Name=payment)
                            for i, payment in enumerate(payments)]

        payments_df = spark.createDataFrame(payments_with_id)
        payments_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/lookup/payment_methods/")

        print("Creating Dataframe for Order Status Table")

        orders = raw_data.select("Order_Status").dropna(
        ).distinct().rdd.flatMap(lambda row: row).collect()
        orders_with_id = [Row(Status_ID=i+1, Status_Name=order)
                          for i, order in enumerate(orders)]

        orders_df = spark.createDataFrame(orders_with_id)
        orders_df.write.mode("overwrite").parquet(
            "s3a://etl-dag/silver/data/lookup/order_status/")

    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
