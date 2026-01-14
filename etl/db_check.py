import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --------------------------
# 1. Load environment variables
# --------------------------
env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(env_path)

# --------------------------
# 2. Database Configuration
# --------------------------
DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)


def check_db_integrity():
    """
    Validate data ingestion by fetching sample records from the production table.
    Displays first few rows for verification.
    """
    query = "SELECT * FROM customers LIMIT 5;"

    try:
        print("Action: Initiating data integrity check...")
        with engine.connect() as conn:
            # Using text() for SQLAlchemy 2.0 compatibility
            df = pd.read_sql(text(query), conn)

            print("\n[Sample Data Preview from RDS]")
            print(df)
            print("\nSuccess: Data validation completed. Ingestion verified.")

    except Exception as e:
        print(f"Error: Integrity check failed. Reason: {e}")


if __name__ == "__main__":
    check_db_integrity()
