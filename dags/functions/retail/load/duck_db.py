import duckdb
import logging
from typing import Dict, List


def setup_duckdb_s3_connection(db_filename: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB connection with S3 configuration"""
    conn = duckdb.connect(db_filename)

    # Configure S3 settings
    conn.execute("""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_region='us-east-1';
        SET s3_endpoint='minio:9000';
        SET s3_access_key_id='minioLocalAccessKey';
        SET s3_secret_access_key='minioLocalSecretKey123';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    logging.info("DuckDB S3 connection configured successfully")
    return conn


def create_table_from_s3(conn: duckdb.DuckDBPyConnection, table_name: str, s3_path: str, columns: List[str] = None) -> None:
    """Create DuckDB table from S3 parquet files"""
    try:
        if columns:
            columns_str = ", ".join(columns)
            query = f"""
                CREATE OR REPLACE TABLE {table_name} AS 
                SELECT {columns_str} 
                FROM read_parquet('{s3_path}*.parquet')
            """
        else:
            query = f"""
                CREATE OR REPLACE TABLE {table_name} AS 
                SELECT * 
                FROM read_parquet('{s3_path}*.parquet')
            """

        conn.execute(query)
        logging.info(f"Table '{table_name}' created from {s3_path}")

    except Exception as e:
        logging.error(f"Failed to create table {table_name}: {str(e)}")
        raise


def load_lookup_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Load all lookup tables from S3"""
    lookup_tables = {
        'countries': {
            'path': 's3://etl-dag/silver/data/lookup/country/',
            'columns': ['Country_ID', 'Country_Name']
        },
        'states_provinces': {
            'path': 's3://etl-dag/silver/data/lookup/state_provinces/',
            'columns': ['State_ID', 'State_Name', 'Country_ID']
        },
        'cities': {
            'path': 's3://etl-dag/silver/data/lookup/city/',
            'columns': ['City_ID', 'City_Name', 'State_ID']
        },
        'categories': {
            'path': 's3://etl-dag/silver/data/lookup/category/',
            'columns': ['Category_ID', 'Category_Name']
        },
        'brands': {
            'path': 's3://etl-dag/silver/data/lookup/brand/',
            'columns': ['Brand_ID', 'Brand_Name']
        },
        'product_types': {
            'path': 's3://etl-dag/silver/data/lookup/product_type/',
            'columns': ['Product_Type_ID', 'Product_Type_Name', 'Category_ID']
        },
        'segments': {
            'path': 's3://etl-dag/silver/data/lookup/segment/',
            'columns': ['Segment_ID', 'Segment_Name']
        },
        'income_levels': {
            'path': 's3://etl-dag/silver/data/lookup/income_level/',
            'columns': ['Income_Level_ID', 'Income_Level']
        },
        'shipping_methods': {
            'path': 's3://etl-dag/silver/data/lookup/shipping_methods/',
            'columns': ['Shipping_Method_ID', 'Method_Name']
        },
        'payment_methods': {
            'path': 's3://etl-dag/silver/data/lookup/payment_methods/',
            'columns': ['Payment_Method_ID', 'Method_Name']
        },
        'order_status': {
            'path': 's3://etl-dag/silver/data/lookup/order_status/',
            'columns': ['Status_ID', 'Status_Name']
        }
    }

    for table_name, config in lookup_tables.items():
        create_table_from_s3(
            conn, table_name, config['path'], config['columns'])


def load_core_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Load all core entity tables from S3"""
    core_tables = {
        'customers': {
            'path': 's3://etl-dag/silver/data/core_entity/customers/',
            'columns': ['Customer_ID', 'Name', 'Email', 'Phone', 'Age', 'Gender',
                        'Income_Level_ID', 'Customer_Segment_ID', 'Created_Date', 'Updated_Date']
        },
        'products': {
            'path': 's3://etl-dag/silver/data/core_entity/products/',
            'columns': ['Product_ID', 'Product_Name', 'Product_Type_ID', 'Brand_ID',
                        'Category_ID', 'Created_Date', 'Updated_Date']
        },
        'addresses': {
            'path': 's3://etl-dag/silver/data/core_entity/address/',
            'columns': ['Customer_ID', 'Street_Address', 'City_ID', 'Zipcode',
                        'Created_Date', 'Updated_Date']
        },
        'transactions': {
            'path': 's3://etl-dag/silver/data/core_entity/transactions/',
            'columns': ['Transaction_ID', 'Customer_ID', 'Transaction_DateTime',
                        'Total_Amount', 'Total_Purchases', 'Shipping_Method_ID',
                        'Payment_Method_ID', 'Order_Status_ID', 'Created_Date', 'Updated_Date']
        }
    }

    for table_name, config in core_tables.items():
        create_table_from_s3(
            conn, table_name, config['path'], config['columns'])


def save_tables_to_s3(conn: duckdb.DuckDBPyConnection, output_path: str = 's3://etl-dag/gold/data/duckdb/') -> None:
    """Save all DuckDB tables as Parquet files to S3"""
    try:
        tables = conn.execute("SHOW TABLES").fetchall()
        for table in tables:
            table_name = table[0]
            output_file = f"{output_path}{table_name}.parquet"
            conn.execute(f"""
                COPY {table_name} TO '{output_file}' (FORMAT PARQUET, OVERWRITE 1)
            """)
            logging.info(f"Saved table '{table_name}' to {output_file}")
    except Exception as e:
        logging.error(f"Failed to save tables to S3: {str(e)}")
        raise


def execute_duckdb_etl():
    """Main ETL function for Airflow PythonOperator"""
    try:
        # Setup connection
        conn = setup_duckdb_s3_connection()

        # Load all tables
        logging.info("Loading lookup tables...")
        load_lookup_tables(conn)

        logging.info("Loading core entity tables...")
        load_core_tables(conn)

        # Verify tables were created
        tables = conn.execute("SHOW TABLES").fetchall()
        logging.info(f"Created {len(tables)} tables: {[t[0] for t in tables]}")

        # Optional: Run some validation queries
        for table_name in ['customers', 'products', 'transactions']:
            count = conn.execute(
                f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            logging.info(f"Table {table_name}: {count} rows loaded")

        # Save to S3 (Gold Layer)
        logging.info("Saving tables to Gold S3 path...")
        save_tables_to_s3(conn, output_path='s3://etl-dag/gold/data/duckdb/')

        conn.close()
        logging.info("ETL process completed successfully")

    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
        raise
