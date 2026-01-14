import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# --------------------------
# 1. Environment Configuration
# --------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(current_dir, "..", ".env")
load_dotenv(dotenv_path)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")

# --------------------------
# 2. Database Engine Setup
# --------------------------
db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def load_local_to_rds(file_name, table_name):
    """
    Load a local CSV file into a PostgreSQL table using SQLAlchemy.
    Replaces existing data if table already exists.
    """
    data_path = os.path.join(current_dir, "..", "data", file_name)
    
    if not os.path.exists(data_path):
        print(f"Error: File not found at {data_path}")
        return

    try:
        print(f">> Reading local file: {file_name}")
        df = pd.read_csv(data_path)
        print(f">> Transferring {len(df)} rows to RDS table '{table_name}'")
        
        # Data ingestion
        connection = engine.raw_connection()
        try:
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f">> Success: Table '{table_name}' updated.")
        finally:
            connection.close()
            
    except Exception as e:
        print(f"!! Fail: Data ingestion failed for {file_name}. Reason: {str(e)}")


if __name__ == "__main__":
    # --------------------------
    # 3. Target CSV to Table Mapping
    # --------------------------
    target_files = {
        "olist_customers_dataset.csv": "customers",
        "olist_orders_dataset.csv": "orders",
        "olist_order_items_dataset.csv": "order_items",
        "olist_products_dataset.csv": "products",
        "product_category_name_translation.csv": "product_category_name_translation"
    }
    
    print("=== Starting raw data ingestion to RDS ===")
    for file, table in target_files.items():
        load_local_to_rds(file, table)
    
    print("=== Core data ingestion process completed ===")
    print("All tasks finished successfully.")
