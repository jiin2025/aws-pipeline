import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# --------------------------
# 1. Load environment variables
# --------------------------
load_dotenv()

# Database connection setup
DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)


def export_mart_to_csv():
    """
    Export the final analytical data mart from RDS to a local CSV file.
    Output is saved under the 'data' directory.
    """
    query = "SELECT * FROM mart_daily_category_sales"

    # Define output directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(current_dir, "data")

    # Create directory if it does not exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"System: Directory created at {output_dir}")

    output_path = os.path.join(output_dir, "daily_sales_mart.csv")

    try:
        print("Action: Fetching data from RDS...")

        # Load data into DataFrame
        df = pd.read_sql(query, engine)

        # Save to CSV (utf-8-sig for Excel compatibility)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')

        print(f"Success: Mart exported to {output_path}")
        print(f"Log: Total records exported: {len(df)}")

    except Exception as e:
        print(f"Error: Export failed. Reason: {str(e)}")


if __name__ == "__main__":
    export_mart_to_csv()
