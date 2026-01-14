import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --------------------------
# 1. Configuration Setup
# --------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(current_dir, "..", ".env")
load_dotenv(dotenv_path)

# Database Connection
DB_URL = (
    f"postgresql://{os.getenv('DB_USER')}:"
    f"{os.getenv('DB_PASSWORD')}@"
    f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
)
engine = create_engine(DB_URL)


def create_daily_sales_mart():
    """
    Transform staging data into an analytical data mart (Gold Layer).
    Aggregates daily sales by category for delivered orders.
    """
    sql = """
    -- Drop existing table to ensure clean refresh
    DROP TABLE IF EXISTS mart_daily_category_sales;

    -- Aggregate raw data into a structured business view
    CREATE TABLE mart_daily_category_sales AS
    SELECT 
        LEFT(t1.order_purchase_timestamp, 10) AS order_date,
        t4.product_category_name_english AS category_name,
        TRUNC(SUM(t2.price)::numeric, 0) AS total_sales_amt
    FROM orders t1 
    LEFT JOIN order_items t2 ON t1.order_id = t2.order_id
    LEFT JOIN products t3 ON t2.product_id = t3.product_id
    LEFT JOIN product_category_name_translation t4 ON t3.product_category_name = t4.product_category_name
    WHERE t1.order_status = 'delivered'
      AND t4.product_category_name_english IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 1 ASC;
    """

    try:
        # Using engine.begin() for automatic transaction management
        with engine.begin() as conn:
            print("Action: Creating Gold Layer mart (Daily Category Sales)...")
            conn.execute(text(sql))
            print("Success: 'mart_daily_category_sales' table created.")

            # Preview first 5 rows for validation
            result = conn.execute(text("SELECT * FROM mart_daily_category_sales LIMIT 5;"))
            print("\n[Data Mart Preview]")
            for row in result:
                print(row)

    except Exception as e:
        print(f"Error: Data mart creation failed. Details: {str(e)}")
        print("Check: Ensure 'products' and 'product_category_name_translation' tables are loaded.")


if __name__ == "__main__":
    create_daily_sales_mart()
