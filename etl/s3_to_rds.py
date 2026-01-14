import os
import boto3
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from io import BytesIO

# Load environment variables from .env
load_dotenv('/opt/airflow/.env')

# Database connection setup
DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:"
    f"{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)

# AWS S3 client setup
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
s3_client = boto3.client('s3')


def s3_to_rds(file_name, table_name):
    """
    Load a CSV file from S3 to an RDS table.
    """
    try:
        print(f">> Task Started: {file_name}")
        
        # Download file from S3
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"raw/{file_name}")
        df = pd.read_csv(BytesIO(obj['Body'].read()))
        
        # Load data into RDS
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f">> Success: {table_name} ({len(df)} rows loaded).")
    except Exception as e:
        print(f"!! Critical Error on {file_name}: {e}")


def run_load():
    """
    Load multiple CSV files from S3 to RDS tables.
    """
    files_to_load = {
        "olist_customers_dataset.csv": "customers",
        "olist_orders_dataset.csv": "orders",
        "olist_order_items_dataset.csv": "order_items",
        "olist_products_dataset.csv": "products",
        "product_category_name_translation.csv": "product_category_name_translation"
    }
    
    print("=== RDS Loading Started ===")
    for file_name, table_name in files_to_load.items():
        s3_to_rds(file_name, table_name)
    print("=== RDS Loading Finished ===")


if __name__ == "__main__":
    run_load()
